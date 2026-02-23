//! Reusable parallel binary merge tree with bounded fan-in.
//!
//! Merges N sorted input streams into a single sorted output stream using a
//! recursive binary merge tree where each internal node runs in its own thread.
//! When N exceeds MAX_FANIN, the merge proceeds in rounds with intermediate
//! temp files to avoid exhausting file descriptors.
//!
//! Used by both the vocabulary merger (Stage 4) and the external sort (Stage 6).

use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::cmp::Ordering;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;

/// Default maximum fan-in per merge round. With a binary tree over 512 leaves,
/// this spawns at most ~1023 threads — well within OS limits.
pub const DEFAULT_MAX_FANIN: usize = 512;

/// Default channel capacity between merge tree nodes.
const DEFAULT_CHANNEL_CAPACITY: usize = 256;

// ---------------------------------------------------------------------------
// Mergeable trait
// ---------------------------------------------------------------------------

/// Item that can participate in a parallel merge tree.
///
/// Provides ordering for merge, plus serialization for intermediate temp files
/// when multi-round merging is needed.
pub trait Mergeable: Send + Sized + 'static {
    /// Compare two items for merge ordering (determines output order).
    fn merge_cmp(&self, other: &Self) -> Ordering;

    /// Serialize to a writer. Must be self-delimiting.
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()>;

    /// Deserialize from a reader. Returns `None` at EOF.
    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>>;
}

/// Blanket implementation: every `Sortable` is automatically `Mergeable`.
impl<T: super::Sortable + 'static> Mergeable for T {
    fn merge_cmp(&self, other: &Self) -> Ordering {
        Ord::cmp(self, other)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        super::Sortable::write_to(self, writer)
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        <Self as super::Sortable>::read_from(reader)
    }
}

// ---------------------------------------------------------------------------
// MergeSource
// ---------------------------------------------------------------------------

/// Type alias for the factory closure used in `MergeSource::Factory`.
type SourceFactory<T> =
    Box<dyn FnOnce() -> Result<Box<dyn Iterator<Item = Result<T>> + Send>> + Send>;

/// A source of sorted items for a merge tree leaf node.
pub enum MergeSource<T: Mergeable> {
    /// A file in generic Mergeable format (zstd-compressed, written via `T::write_to`).
    /// Used for intermediate temp files and external sort chunks.
    File(PathBuf),

    /// A closure that produces an iterator of items.
    /// Captures context (e.g., batch_id) and opens custom file formats.
    Factory(SourceFactory<T>),
}

// ---------------------------------------------------------------------------
// MergeTreeConfig
// ---------------------------------------------------------------------------

/// Configuration for building a parallel merge tree.
pub struct MergeTreeConfig {
    /// Maximum number of inputs merged in a single round.
    /// If more inputs are provided, multi-round merging with intermediate
    /// temp files is used. Default: 512.
    pub max_fanin: usize,

    /// Bounded channel capacity between merge tree nodes.
    pub channel_capacity: usize,

    /// Directory for intermediate temp files (multi-round merges).
    pub temp_dir: PathBuf,
}

impl MergeTreeConfig {
    pub fn new(temp_dir: impl Into<PathBuf>) -> Self {
        Self {
            max_fanin: DEFAULT_MAX_FANIN,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            temp_dir: temp_dir.into(),
        }
    }

    #[cfg(test)]
    pub fn with_max_fanin(mut self, max_fanin: usize) -> Self {
        self.max_fanin = max_fanin;
        self
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }
}

// ---------------------------------------------------------------------------
// MergeTreeHandle
// ---------------------------------------------------------------------------

/// Handle to a running parallel merge tree.
///
/// Owns all worker threads and intermediate temp files. The merged output
/// is available via the `rx` receiver. Call `join()` after consuming all
/// items to wait for workers and propagate any panics.
pub struct MergeTreeHandle<T: Mergeable> {
    /// Receiver for the merged, sorted output stream.
    pub rx: Receiver<Result<T>>,

    /// Worker thread handles (leaf readers + interior merge nodes).
    handles: Vec<JoinHandle<()>>,

    /// Intermediate temp files to clean up on drop.
    temp_files: Vec<PathBuf>,
}

impl<T: Mergeable> MergeTreeHandle<T> {
    /// Wait for all worker threads to complete. Returns an error if any
    /// worker thread panicked.
    pub fn join(mut self) -> Result<()> {
        for handle in self.handles.drain(..) {
            if handle.join().is_err() {
                anyhow::bail!("Merge tree worker thread panicked");
            }
        }
        Ok(())
    }
}

impl<T: Mergeable> Drop for MergeTreeHandle<T> {
    fn drop(&mut self) {
        // Clean up intermediate temp files
        for path in &self.temp_files {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!(
                    "Failed to remove intermediate merge file {}: {}",
                    path.display(),
                    e
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Core: build_merge_tree
// ---------------------------------------------------------------------------

/// Build a parallel merge tree over the given sorted sources.
///
/// If the number of sources exceeds `config.max_fanin`, the merge proceeds
/// in multiple rounds: each round merges groups of up to `max_fanin` sources
/// into intermediate temp files, which become inputs for the next round.
///
/// Returns a handle whose `rx` field yields items in sorted order.
pub fn build_merge_tree<T: Mergeable>(
    sources: Vec<MergeSource<T>>,
    config: &MergeTreeConfig,
) -> Result<MergeTreeHandle<T>> {
    if sources.is_empty() {
        let (_tx, rx) = bounded(1);
        return Ok(MergeTreeHandle {
            rx,
            handles: Vec::new(),
            temp_files: Vec::new(),
        });
    }

    if sources.len() <= config.max_fanin {
        // Single round: build binary tree directly
        build_single_round(sources, config)
    } else {
        // Multi-round: merge groups into intermediate files, then merge those
        build_multi_round(sources, config)
    }
}

/// Build a single-round parallel binary merge tree (N <= max_fanin).
fn build_single_round<T: Mergeable>(
    sources: Vec<MergeSource<T>>,
    config: &MergeTreeConfig,
) -> Result<MergeTreeHandle<T>> {
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    // Spawn leaf threads, one per source
    let leaf_receivers: Vec<Receiver<Result<T>>> = sources
        .into_iter()
        .map(|source| spawn_leaf_thread(source, config.channel_capacity, &mut handles))
        .collect::<Result<_>>()?;

    // Build binary merge tree over the leaf receivers
    let root_rx = build_binary_tree(leaf_receivers, config.channel_capacity, &mut handles);

    Ok(MergeTreeHandle {
        rx: root_rx,
        handles,
        temp_files: Vec::new(),
    })
}

/// Multi-round merge: group sources, merge each group to an intermediate file,
/// then recursively merge the intermediate files.
fn build_multi_round<T: Mergeable>(
    sources: Vec<MergeSource<T>>,
    config: &MergeTreeConfig,
) -> Result<MergeTreeHandle<T>> {
    let mut all_temp_files: Vec<PathBuf> = Vec::new();
    let mut current_sources = sources;

    // Keep merging in rounds until we're within max_fanin
    let mut round = 0u32;
    while current_sources.len() > config.max_fanin {
        tracing::info!(
            "Merge round {}: {} sources → {} groups of ≤{}",
            round,
            current_sources.len(),
            current_sources.len().div_ceil(config.max_fanin),
            config.max_fanin,
        );

        let mut next_sources: Vec<MergeSource<T>> = Vec::new();

        // Process groups of max_fanin
        let mut source_vec = current_sources;
        let mut groups: Vec<Vec<MergeSource<T>>> = Vec::new();
        while !source_vec.is_empty() {
            let take = source_vec.len().min(config.max_fanin);
            groups.push(source_vec.drain(..take).collect());
        }

        for (group_idx, group) in groups.into_iter().enumerate() {
            let intermediate_path = config.temp_dir.join(format!(
                "merge_intermediate_r{:02}_g{:04}.tmp.zst",
                round, group_idx
            ));

            // Build a merge tree for this group, drain to intermediate file
            let handle = build_single_round(group, config)?;
            write_merged_to_file(&handle.rx, &intermediate_path)?;
            handle.join()?;

            all_temp_files.push(intermediate_path.clone());
            next_sources.push(MergeSource::File(intermediate_path));
        }

        current_sources = next_sources;
        round += 1;
    }

    // Final round: build the tree directly (no more intermediate files)
    tracing::info!(
        "Merge final round: {} sources → parallel binary tree",
        current_sources.len()
    );
    let mut handle = build_single_round(current_sources, config)?;
    handle.temp_files = all_temp_files;
    Ok(handle)
}

// ---------------------------------------------------------------------------
// Leaf thread spawning
// ---------------------------------------------------------------------------

/// Spawn a leaf thread that reads from a `MergeSource` and sends items
/// into a bounded channel.
fn spawn_leaf_thread<T: Mergeable>(
    source: MergeSource<T>,
    channel_capacity: usize,
    handles: &mut Vec<JoinHandle<()>>,
) -> Result<Receiver<Result<T>>> {
    let (tx, rx) = bounded(channel_capacity);

    let handle = std::thread::spawn(move || {
        let result = match source {
            MergeSource::File(path) => read_mergeable_file(&path, &tx),
            MergeSource::Factory(factory) => read_from_factory(factory, &tx),
        };
        if let Err(e) = result {
            let _ = tx.send(Err(e));
        }
    });

    handles.push(handle);
    Ok(rx)
}

/// Read items from a generic Mergeable-format file (zstd-compressed).
fn read_mergeable_file<T: Mergeable>(path: &Path, tx: &Sender<Result<T>>) -> Result<()> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open merge file: {}", path.display()))?;
    let buf_reader = BufReader::new(file);
    let mut decoder = zstd::Decoder::with_buffer(buf_reader)
        .with_context(|| format!("Failed to create zstd decoder for: {}", path.display()))?;

    loop {
        match T::read_from(&mut decoder) {
            Ok(Some(item)) => {
                if tx.send(Ok(item)).is_err() {
                    return Ok(()); // Consumer dropped
                }
            }
            Ok(None) => return Ok(()), // EOF
            Err(e) => return Err(e),
        }
    }
}

/// Read items from a factory-produced iterator.
fn read_from_factory<T: Mergeable>(
    factory: SourceFactory<T>,
    tx: &Sender<Result<T>>,
) -> Result<()> {
    let iter = factory()?;
    for item_result in iter {
        match item_result {
            Ok(item) => {
                if tx.send(Ok(item)).is_err() {
                    return Ok(()); // Consumer dropped
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Binary merge tree construction
// ---------------------------------------------------------------------------

/// Build a recursive binary merge tree over the given leaf receivers.
///
/// Each internal node is a thread that performs a 2-way merge of its two
/// children, forwarding items through a bounded channel. The tree has
/// ceil(log2(N)) levels and N-1 internal nodes.
///
/// Returns the root receiver that yields all items in sorted order.
fn build_binary_tree<T: Mergeable>(
    mut layer: Vec<Receiver<Result<T>>>,
    channel_capacity: usize,
    handles: &mut Vec<JoinHandle<()>>,
) -> Receiver<Result<T>> {
    if layer.is_empty() {
        let (_tx, rx) = bounded(1);
        return rx;
    }

    // Reduce pairwise until a single receiver remains
    while layer.len() > 1 {
        let mut next_layer: Vec<Receiver<Result<T>>> = Vec::new();

        // Drain pairs from the layer
        let mut drain = layer.into_iter();
        loop {
            let left = match drain.next() {
                Some(rx) => rx,
                None => break,
            };
            let right = match drain.next() {
                Some(rx) => rx,
                None => {
                    // Odd one out — pass through to next layer
                    next_layer.push(left);
                    break;
                }
            };

            // Spawn a 2-way merge thread for this pair
            let (tx, rx) = bounded(channel_capacity);
            let handle = std::thread::spawn(move || {
                two_way_merge::<T>(left, right, tx);
            });
            handles.push(handle);
            next_layer.push(rx);
        }

        layer = next_layer;
    }

    layer.pop().expect("non-empty layer after tree construction")
}

/// Perform a 2-way merge of two sorted input channels into a single output.
fn two_way_merge<T: Mergeable>(
    left_rx: Receiver<Result<T>>,
    right_rx: Receiver<Result<T>>,
    tx: Sender<Result<T>>,
) {
    let mut left_next = match recv_item(&left_rx) {
        Ok(item) => item,
        Err(e) => {
            let _ = tx.send(Err(e));
            return;
        }
    };
    let mut right_next = match recv_item(&right_rx) {
        Ok(item) => item,
        Err(e) => {
            let _ = tx.send(Err(e));
            return;
        }
    };

    while left_next.is_some() || right_next.is_some() {
        let send_item = match (&left_next, &right_next) {
            (Some(left_item), Some(right_item)) => {
                if left_item.merge_cmp(right_item) != Ordering::Greater {
                    left_next.take()
                } else {
                    right_next.take()
                }
            }
            (Some(_), None) => left_next.take(),
            (None, Some(_)) => right_next.take(),
            (None, None) => None,
        };

        if let Some(item) = send_item
            && tx.send(Ok(item)).is_err()
        {
            return; // Consumer dropped
        }

        if left_next.is_none() {
            left_next = match recv_item(&left_rx) {
                Ok(item) => item,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };
        }
        if right_next.is_none() {
            right_next = match recv_item(&right_rx) {
                Ok(item) => item,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };
        }
    }
}

/// Receive the next item from a channel, translating channel-closed to None.
fn recv_item<T>(rx: &Receiver<Result<T>>) -> Result<Option<T>> {
    match rx.recv() {
        Ok(Ok(item)) => Ok(Some(item)),
        Ok(Err(e)) => Err(e),
        Err(_) => Ok(None), // Channel closed = source exhausted
    }
}

// ---------------------------------------------------------------------------
// Intermediate file I/O (for multi-round merges)
// ---------------------------------------------------------------------------

/// Drain a merge tree's output into a zstd-compressed intermediate file.
fn write_merged_to_file<T: Mergeable>(rx: &Receiver<Result<T>>, path: &Path) -> Result<()> {
    let file = std::fs::File::create(path)
        .with_context(|| format!("Failed to create intermediate merge file: {}", path.display()))?;
    let buf_writer = BufWriter::with_capacity(256 * 1024, file);
    let mut encoder = zstd::Encoder::new(buf_writer, 1)?; // level 1 for speed

    let mut count = 0u64;
    loop {
        match rx.recv() {
            Ok(Ok(item)) => {
                item.write_to(&mut encoder)?;
                count += 1;
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => break, // Channel closed
        }
    }

    encoder.finish()?;
    tracing::debug!(
        "Wrote {} items to intermediate merge file: {}",
        count,
        path.display()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Iterator adapters
// ---------------------------------------------------------------------------

/// Wraps a `MergeTreeHandle` as an `Iterator<Item = Result<T>>`.
///
/// On drop, joins all worker threads and cleans up temp files.
pub struct MergeTreeIterator<T: Mergeable> {
    handle: Option<MergeTreeHandle<T>>,
}

impl<T: Mergeable> MergeTreeIterator<T> {
    pub fn new(handle: MergeTreeHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }
}

impl<T: Mergeable> Iterator for MergeTreeIterator<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let handle = self.handle.as_ref()?;
        handle.rx.recv().ok() // None when channel closed
    }
}

impl<T: Mergeable> Drop for MergeTreeIterator<T> {
    fn drop(&mut self) {
        if let Some(mut handle) = self.handle.take() {
            // Drain any remaining items to unblock workers
            while handle.rx.recv().is_ok() {}

            for h in handle.handles.drain(..) {
                let _ = h.join();
            }
            // temp_files cleaned up by MergeTreeHandle's Drop
        }
    }
}

/// Deduplication iterator adapter.
///
/// Wraps any iterator of `Result<T>` and skips consecutive duplicate items
/// (where duplicates are determined by `T: PartialEq`).
pub struct DedupIterator<I, T> {
    inner: I,
    last: Option<T>,
}

impl<I, T> DedupIterator<I, T>
where
    I: Iterator<Item = Result<T>>,
    T: PartialEq + Clone,
{
    pub fn new(inner: I) -> Self {
        Self { inner, last: None }
    }
}

impl<I, T> Iterator for DedupIterator<I, T>
where
    I: Iterator<Item = Result<T>>,
    T: PartialEq + Clone,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.inner.next()?;
            match item {
                Ok(value) => {
                    if let Some(ref last) = self.last
                        && &value == last
                    {
                        continue; // Skip duplicate
                    }
                    self.last = Some(value.clone());
                    return Some(Ok(value));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Simple test item: a u64 value.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct TestItem(u64);

    impl Mergeable for TestItem {
        fn merge_cmp(&self, other: &Self) -> Ordering {
            self.0.cmp(&other.0)
        }

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
    }

    fn factory_source(items: Vec<u64>) -> MergeSource<TestItem> {
        MergeSource::Factory(Box::new(move || {
            Ok(Box::new(items.into_iter().map(|v| Ok(TestItem(v)))))
        }))
    }

    #[test]
    fn test_empty_sources() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = MergeTreeConfig::new(temp_dir.path());
        let handle = build_merge_tree::<TestItem>(vec![], &config)?;
        let items: Vec<TestItem> = MergeTreeIterator::new(handle)
            .collect::<Result<Vec<_>>>()?;
        assert!(items.is_empty());
        Ok(())
    }

    #[test]
    fn test_single_source() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = MergeTreeConfig::new(temp_dir.path());
        let sources = vec![factory_source(vec![1, 3, 5])];
        let handle = build_merge_tree(sources, &config)?;
        let items: Vec<TestItem> = MergeTreeIterator::new(handle)
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(items, vec![TestItem(1), TestItem(3), TestItem(5)]);
        Ok(())
    }

    #[test]
    fn test_two_sources_interleaved() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = MergeTreeConfig::new(temp_dir.path());
        let sources = vec![
            factory_source(vec![1, 3, 5, 7]),
            factory_source(vec![2, 4, 6, 8]),
        ];
        let handle = build_merge_tree(sources, &config)?;
        let items: Vec<TestItem> = MergeTreeIterator::new(handle)
            .collect::<Result<Vec<_>>>()?;
        let expected: Vec<TestItem> = (1..=8).map(TestItem).collect();
        assert_eq!(items, expected);
        Ok(())
    }

    #[test]
    fn test_many_sources() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = MergeTreeConfig::new(temp_dir.path())
            .with_channel_capacity(2); // Small capacity for backpressure testing
        let sources: Vec<MergeSource<TestItem>> = (0..10)
            .map(|i| factory_source(vec![i * 3, i * 3 + 1, i * 3 + 2]))
            .collect();
        let handle = build_merge_tree(sources, &config)?;
        let items: Vec<TestItem> = MergeTreeIterator::new(handle)
            .collect::<Result<Vec<_>>>()?;
        // Should contain 0..30 in sorted order (with duplicates since ranges overlap)
        let mut expected: Vec<u64> = (0..10).flat_map(|i| vec![i * 3, i * 3 + 1, i * 3 + 2]).collect();
        expected.sort();
        let expected: Vec<TestItem> = expected.into_iter().map(TestItem).collect();
        assert_eq!(items, expected);
        Ok(())
    }

    #[test]
    fn test_multi_round_merge() -> Result<()> {
        let temp_dir = TempDir::new()?;
        // Use tiny max_fanin to force multi-round
        let config = MergeTreeConfig::new(temp_dir.path())
            .with_max_fanin(3)
            .with_channel_capacity(2);

        // 10 sources, max_fanin=3 → round 0 produces 4 intermediates, round 1 produces 2, final round merges 2
        let sources: Vec<MergeSource<TestItem>> = (0..10)
            .map(|i| factory_source(vec![i * 2, i * 2 + 1]))
            .collect();
        let handle = build_merge_tree(sources, &config)?;
        let items: Vec<TestItem> = MergeTreeIterator::new(handle)
            .collect::<Result<Vec<_>>>()?;

        let mut expected: Vec<u64> = (0..10).flat_map(|i| vec![i * 2, i * 2 + 1]).collect();
        expected.sort();
        let expected: Vec<TestItem> = expected.into_iter().map(TestItem).collect();
        assert_eq!(items, expected);

        // Verify intermediate files were cleaned up
        let remaining_files: Vec<_> = std::fs::read_dir(temp_dir.path())?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("merge_intermediate"))
            .collect();
        assert!(remaining_files.is_empty(), "Intermediate files should be cleaned up");

        Ok(())
    }

    #[test]
    fn test_file_source_roundtrip() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test_source.zst");

        // Write a sorted file in Mergeable format
        {
            let file = std::fs::File::create(&file_path)?;
            let buf_writer = BufWriter::new(file);
            let mut encoder = zstd::Encoder::new(buf_writer, 1)?;
            for v in [10u64, 20, 30] {
                TestItem(v).write_to(&mut encoder)?;
            }
            encoder.finish()?;
        }

        let config = MergeTreeConfig::new(temp_dir.path());
        let sources = vec![
            MergeSource::File(file_path),
            factory_source(vec![5, 15, 25, 35]),
        ];
        let handle = build_merge_tree(sources, &config)?;
        let items: Vec<TestItem> = MergeTreeIterator::new(handle)
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            items,
            vec![
                TestItem(5), TestItem(10), TestItem(15), TestItem(20),
                TestItem(25), TestItem(30), TestItem(35)
            ]
        );
        Ok(())
    }

    #[test]
    fn test_error_propagation() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = MergeTreeConfig::new(temp_dir.path());
        let sources = vec![
            MergeSource::<TestItem>::Factory(Box::new(|| {
                Err(anyhow::anyhow!("intentional test error"))
            })),
        ];
        let handle = build_merge_tree(sources, &config)?;
        let result: Result<Vec<TestItem>> = MergeTreeIterator::new(handle)
            .collect::<Result<Vec<_>>>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("intentional test error"));
        Ok(())
    }

    #[test]
    fn test_dedup_iterator() {
        let items = vec![Ok(1u64), Ok(1), Ok(2), Ok(2), Ok(2), Ok(3), Ok(4), Ok(4)];
        let deduped: Vec<u64> = DedupIterator::new(items.into_iter())
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(deduped, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_dedup_iterator_with_errors() {
        let items: Vec<Result<u64>> = vec![
            Ok(1),
            Ok(2),
            Err(anyhow::anyhow!("error")),
            Ok(3),
        ];
        let mut iter = DedupIterator::new(items.into_iter());
        assert_eq!(iter.next().unwrap().unwrap(), 1);
        assert_eq!(iter.next().unwrap().unwrap(), 2);
        assert!(iter.next().unwrap().is_err());
        // After error, iteration stops (the error consumed the next())
    }

    #[test]
    fn test_duplicate_values_across_sources() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = MergeTreeConfig::new(temp_dir.path());
        // Same values in multiple sources — merge tree does NOT deduplicate
        let sources = vec![
            factory_source(vec![1, 2, 3]),
            factory_source(vec![1, 2, 3]),
        ];
        let handle = build_merge_tree(sources, &config)?;
        let items: Vec<TestItem> = MergeTreeIterator::new(handle)
            .collect::<Result<Vec<_>>>()?;
        // All 6 items present (no dedup in merge tree)
        assert_eq!(
            items,
            vec![
                TestItem(1), TestItem(1), TestItem(2), TestItem(2),
                TestItem(3), TestItem(3)
            ]
        );
        Ok(())
    }
}
