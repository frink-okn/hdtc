//! Generic external merge sort implementation.

use anyhow::{Context, Result};
use rayon::slice::ParallelSliceMut;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use zstd::{Decoder, Encoder};

use super::parallel_merge::{
    DedupIterator, MergeSource, MergeTreeConfig, MergeTreeIterator, build_merge_tree,
};

/// Threshold: use parallel merge tree when chunk count exceeds this.
/// Below this, the single-threaded BinaryHeap merge is faster (no thread overhead).
const PARALLEL_MERGE_THRESHOLD: usize = 16;
static NEXT_SORTER_ID: AtomicU64 = AtomicU64::new(1);

/// Trait for items that can be externally sorted.
/// Items must be serializable to/from bytes for disk storage.
pub trait Sortable: Ord + Sized + Send + Clone {
    /// Write this item to a writer. Must be self-delimiting.
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()>;

    /// Read an item from a reader. Returns None at EOF.
    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>>;

    /// Approximate memory size of this item in bytes.
    fn mem_size(&self) -> usize;
}

/// External merge sort that spills to disk when memory budget is exceeded.
pub struct ExternalSorter {
    temp_dir: PathBuf,
    memory_budget: usize,
    sorter_id: u64,
    chunk_files: Vec<PathBuf>,
    chunk_counter: usize,
}

impl ExternalSorter {
    /// Create a new ExternalSorter.
    ///
    /// - `temp_dir`: directory for temporary chunk files
    /// - `memory_budget`: approximate max bytes to hold in memory before flushing
    pub fn new(temp_dir: impl AsRef<Path>, memory_budget: usize) -> Self {
        Self {
            temp_dir: temp_dir.as_ref().to_path_buf(),
            memory_budget,
            sorter_id: NEXT_SORTER_ID.fetch_add(1, AtomicOrdering::Relaxed),
            chunk_files: Vec::new(),
            chunk_counter: 0,
        }
    }

    /// Sort items from an iterator, writing sorted chunks to disk as needed.
    /// Returns a merge iterator over all chunks in sorted order.
    #[cfg(test)]
    pub fn sort<T: Sortable + PartialEq + 'static>(
        &mut self,
        items: impl Iterator<Item = T>,
    ) -> Result<MergeIterator<T>> {
        let mut buffer: Vec<T> = Vec::new();
        let mut mem_used: usize = 0;

        for item in items {
            mem_used += item.mem_size();
            buffer.push(item);

            if mem_used >= self.memory_budget {
                self.flush_chunk(&mut buffer)?;
                mem_used = 0;
            }
        }

        // Flush remaining items
        if !buffer.is_empty() {
            self.flush_chunk(&mut buffer)?;
        }

        self.merge()
    }

    /// Sort items pushed incrementally. Call `push` repeatedly, then `finish`.
    pub fn push<T: Sortable>(
        &mut self,
        item: T,
        buffer: &mut Vec<T>,
        mem_used: &mut usize,
    ) -> Result<()> {
        *mem_used += item.mem_size();
        buffer.push(item);

        if *mem_used >= self.memory_budget {
            self.flush_chunk(buffer)?;
            *mem_used = 0;
        }
        Ok(())
    }

    /// Flush remaining buffer and create the merge iterator.
    pub fn finish<T: Sortable + PartialEq + 'static>(
        &mut self,
        buffer: &mut Vec<T>,
    ) -> Result<MergeIterator<T>> {
        if !buffer.is_empty() {
            self.flush_chunk(buffer)?;
        }
        self.merge()
    }

    /// Sort and write a chunk to a temporary file with zstd compression.
    fn flush_chunk<T: Sortable>(&mut self, buffer: &mut Vec<T>) -> Result<()> {
        buffer.par_sort_unstable();

        let chunk_path = self.temp_dir.join(format!(
            "sort_{}_chunk_{:06}.tmp.zst",
            self.sorter_id, self.chunk_counter
        ));
        self.chunk_counter += 1;

        let file = File::create(&chunk_path)
            .with_context(|| format!("Failed to create chunk file {}", chunk_path.display()))?;
        let buf_writer = BufWriter::with_capacity(256 * 1024, file);
        let mut encoder = Encoder::new(buf_writer, 1)?; // zstd level 1 for speed

        for item in buffer.drain(..) {
            item.write_to(&mut encoder)?;
        }
        encoder.finish()?;

        tracing::debug!("Wrote compressed chunk: {}", chunk_path.display());
        self.chunk_files.push(chunk_path);
        Ok(())
    }

    /// Create a k-way merge iterator over all sorted chunks.
    ///
    /// For small chunk counts (≤16), uses a single-threaded BinaryHeap merge.
    /// For larger counts, uses a parallel binary merge tree with bounded fan-in
    /// to avoid exhausting file descriptors and to pipeline zstd decompression.
    fn merge<T: Sortable + PartialEq + 'static>(&self) -> Result<MergeIterator<T>> {
        if self.chunk_files.len() <= PARALLEL_MERGE_THRESHOLD {
            // Fast path: single-threaded heap merge (no thread overhead)
            let mut readers = Vec::with_capacity(self.chunk_files.len());
            for path in &self.chunk_files {
                let file = File::open(path)
                    .with_context(|| format!("Failed to open chunk file {}", path.display()))?;
                let decoder = Decoder::new(file)?;
                readers.push(decoder);
            }
            MergeIterator::from_heap(readers)
        } else {
            // Parallel path: binary merge tree with bounded fan-in
            tracing::info!(
                "Using parallel merge tree for {} sort chunks",
                self.chunk_files.len()
            );
            let sources: Vec<MergeSource<T>> = self
                .chunk_files
                .iter()
                .map(|p| MergeSource::File(p.clone()))
                .collect();
            let channel_capacity = calculate_merge_channel_capacity(self.memory_budget);
            let config =
                MergeTreeConfig::new(&self.temp_dir).with_channel_capacity(channel_capacity);
            let handle = build_merge_tree(sources, &config)?;
            MergeIterator::from_tree(handle)
        }
    }

    /// Number of chunk files currently produced.
    pub fn chunk_file_count(&self) -> usize {
        self.chunk_files.len()
    }

    /// Clean up temporary chunk files.
    pub fn cleanup(&self) {
        for path in &self.chunk_files {
            if let Err(e) = std::fs::remove_file(path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!("Failed to remove temp file {}: {}", path.display(), e);
            }
        }
    }
}

impl Drop for ExternalSorter {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Calculate bounded channel capacity for the parallel merge tree.
///
/// Uses ~5% of the sort memory budget for channel buffers, clamped to
/// reasonable bounds. This mirrors the vocab merger's approach.
fn calculate_merge_channel_capacity(memory_budget: usize) -> usize {
    const MIB: usize = 1024 * 1024;
    // IdTriple is ~24 bytes; estimate ~32 bytes per item with overhead.
    let estimated_entry_bytes = 32usize;
    let queue_budget = (memory_budget / 20).clamp(2 * MIB, 64 * MIB);
    (queue_budget / estimated_entry_bytes).clamp(64, 4096)
}

/// K-way merge iterator over sorted chunk files with deduplication.
///
/// Supports two backends:
/// - **Heap**: Single-threaded BinaryHeap merge (used for small chunk counts, ≤16)
/// - **Tree**: Parallel binary merge tree with bounded fan-in (used for larger counts)
pub enum MergeIterator<T: Sortable + 'static> {
    Heap(HeapMergeIterator<T>),
    Tree(DedupIterator<MergeTreeIterator<T>, T>),
}

impl<T: Sortable + PartialEq + 'static> MergeIterator<T> {
    /// Create from a set of zstd decoders (single-threaded heap merge).
    fn from_heap(readers: Vec<Decoder<'static, BufReader<File>>>) -> Result<Self> {
        Ok(MergeIterator::Heap(HeapMergeIterator::new(readers)?))
    }

    /// Create from a parallel merge tree handle (with deduplication).
    fn from_tree(handle: super::parallel_merge::MergeTreeHandle<T>) -> Result<Self> {
        let tree_iter = MergeTreeIterator::new(handle);
        Ok(MergeIterator::Tree(DedupIterator::new(tree_iter)))
    }
}

impl<T: Sortable + PartialEq + 'static> Iterator for MergeIterator<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MergeIterator::Heap(heap) => heap.next(),
            MergeIterator::Tree(tree) => tree.next(),
        }
    }
}

/// Single-threaded BinaryHeap k-way merge with deduplication.
pub struct HeapMergeIterator<T: Sortable> {
    heap: BinaryHeap<HeapEntry<T>>,
    readers: Vec<Option<Decoder<'static, BufReader<File>>>>,
    last_item: Option<T>,
}

/// Entry in the merge heap. Wraps an item with its source chunk index.
/// Reversed ordering so BinaryHeap (max-heap) acts as a min-heap.
struct HeapEntry<T: Sortable> {
    item: T,
    source: usize,
}

impl<T: Sortable> PartialEq for HeapEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.item == other.item
    }
}

impl<T: Sortable> Eq for HeapEntry<T> {}

impl<T: Sortable> PartialOrd for HeapEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Sortable> Ord for HeapEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap behavior
        other.item.cmp(&self.item)
    }
}

impl<T: Sortable> HeapMergeIterator<T> {
    fn new(mut readers: Vec<Decoder<'static, BufReader<File>>>) -> Result<Self> {
        let mut heap = BinaryHeap::with_capacity(readers.len());
        let mut opt_readers: Vec<Option<Decoder<'static, BufReader<File>>>> =
            Vec::with_capacity(readers.len());

        for (i, mut reader) in readers.drain(..).enumerate() {
            match T::read_from(&mut reader) {
                Ok(Some(item)) => {
                    heap.push(HeapEntry { item, source: i });
                    opt_readers.push(Some(reader));
                }
                Ok(None) => {
                    opt_readers.push(None); // empty chunk
                }
                Err(e) => return Err(e),
            }
        }

        Ok(Self {
            heap,
            readers: opt_readers,
            last_item: None,
        })
    }
}

impl<T: Sortable> Iterator for HeapMergeIterator<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = self.heap.pop()?;
            let source = entry.source;

            // Try to read the next item from the same source
            if let Some(reader) = self.readers[source].as_mut() {
                match T::read_from(reader) {
                    Ok(Some(next_item)) => {
                        self.heap.push(HeapEntry {
                            item: next_item,
                            source,
                        });
                    }
                    Ok(None) => {
                        self.readers[source] = None; // exhausted
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Skip duplicates: if this item equals the last emitted item, continue
            if let Some(ref last) = self.last_item
                && &entry.item == last
            {
                continue; // Skip duplicate
            }

            // Emit this item and remember it for next comparison
            self.last_item = Some(entry.item.clone());
            return Some(Ok(entry.item));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple test type: a u64 value.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestItem(u64);

    impl Sortable for TestItem {
        fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
            writer.write_all(&self.0.to_le_bytes())?;
            Ok(())
        }

        fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
            let mut buf = [0u8; 8];
            match reader.read_exact(&mut buf) {
                Ok(()) => Ok(Some(TestItem(u64::from_le_bytes(buf)))),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
                Err(e) => Err(e.into()),
            }
        }

        fn mem_size(&self) -> usize {
            8
        }
    }

    #[test]
    fn test_external_sort_small() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut sorter = ExternalSorter::new(temp_dir.path(), 1024);

        let items = vec![5u64, 3, 8, 1, 9, 2, 7, 4, 6, 0]
            .into_iter()
            .map(TestItem);

        let merged: Vec<TestItem> = sorter
            .sort(items)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let expected: Vec<TestItem> = (0..10).map(TestItem).collect();
        assert_eq!(merged, expected);
    }

    #[test]
    fn test_external_sort_multiple_chunks() {
        let temp_dir = tempfile::tempdir().unwrap();
        // Budget of 24 bytes = 3 items per chunk
        let mut sorter = ExternalSorter::new(temp_dir.path(), 24);

        let items = vec![9u64, 7, 5, 3, 1, 8, 6, 4, 2, 0]
            .into_iter()
            .map(TestItem);

        let merged: Vec<TestItem> = sorter
            .sort(items)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let expected: Vec<TestItem> = (0..10).map(TestItem).collect();
        assert_eq!(merged, expected);
    }

    #[test]
    fn test_external_sort_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut sorter = ExternalSorter::new(temp_dir.path(), 1024);

        let items = std::iter::empty::<TestItem>();
        let merged: Vec<TestItem> = sorter
            .sort(items)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert!(merged.is_empty());
    }

    #[test]
    fn test_external_sort_single_item() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut sorter = ExternalSorter::new(temp_dir.path(), 1024);

        let items = std::iter::once(TestItem(42));
        let merged: Vec<TestItem> = sorter
            .sort(items)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(merged, vec![TestItem(42)]);
    }

    #[test]
    fn test_external_sort_already_sorted() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut sorter = ExternalSorter::new(temp_dir.path(), 24);

        let items = (0..10).map(TestItem);
        let merged: Vec<TestItem> = sorter
            .sort(items)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let expected: Vec<TestItem> = (0..10).map(TestItem).collect();
        assert_eq!(merged, expected);
    }

    #[test]
    fn test_external_sort_duplicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut sorter = ExternalSorter::new(temp_dir.path(), 24);

        let items = vec![3u64, 1, 3, 2, 1, 2].into_iter().map(TestItem);
        let merged: Vec<TestItem> = sorter
            .sort(items)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // Duplicates are automatically deduplicated
        assert_eq!(merged, vec![TestItem(1), TestItem(2), TestItem(3)]);
    }

    #[test]
    fn test_push_api() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut sorter = ExternalSorter::new(temp_dir.path(), 24);

        let mut buffer = Vec::new();
        let mut mem_used = 0;

        for v in [5u64, 3, 8, 1, 9, 2, 7, 4, 6, 0] {
            sorter
                .push(TestItem(v), &mut buffer, &mut mem_used)
                .unwrap();
        }

        let merged: Vec<TestItem> = sorter
            .finish(&mut buffer)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let expected: Vec<TestItem> = (0..10).map(TestItem).collect();
        assert_eq!(merged, expected);
    }

    #[test]
    fn test_two_sorters_same_temp_dir_do_not_clobber_chunks() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Tiny budget to force multiple chunk flushes for both sorters.
        let mut sorter_a = ExternalSorter::new(temp_dir.path(), 24);
        let mut sorter_b = ExternalSorter::new(temp_dir.path(), 24);

        let mut buffer_a = Vec::new();
        let mut mem_a = 0usize;
        let mut buffer_b = Vec::new();
        let mut mem_b = 0usize;

        // Interleave pushes so chunk files from both sorters are created
        // in the same directory over the same timeline.
        for i in 0..100u64 {
            sorter_a
                .push(TestItem(10_000 + (99 - i)), &mut buffer_a, &mut mem_a)
                .unwrap();
            sorter_b
                .push(TestItem(20_000 + (99 - i)), &mut buffer_b, &mut mem_b)
                .unwrap();
        }

        let out_a: Vec<TestItem> = sorter_a
            .finish(&mut buffer_a)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let out_b: Vec<TestItem> = sorter_b
            .finish(&mut buffer_b)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let expected_a: Vec<TestItem> = (0..100).map(|i| TestItem(10_000 + i)).collect();
        let expected_b: Vec<TestItem> = (0..100).map(|i| TestItem(20_000 + i)).collect();

        assert_eq!(out_a, expected_a);
        assert_eq!(out_b, expected_b);
    }
}
