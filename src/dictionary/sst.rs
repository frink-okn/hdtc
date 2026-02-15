//! Sorted String Table (SST) for disk-backed term-to-ID lookup.
//!
//! A write-once, read-many data structure optimized for our access pattern:
//! - Built during dictionary construction from already-sorted terms
//! - Queried during Pass 2 to resolve terms to their dictionary IDs
//!
//! Layout:
//! - Sequential records: u32(key_len) | key_bytes | u8(section) | u64(id)
//! - Sparse block index (every Nth record): stored separately in memory

use anyhow::{bail, Context, Result};
use memmap2::Mmap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// How many records per block in the sparse index.
const BLOCK_SIZE: usize = 1024;

/// Dictionary section identifiers for term-to-ID mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DictSection {
    Shared = 0,
    Subjects = 1,
    Predicates = 2,
    Objects = 3,
    Graphs = 4,
}

impl DictSection {
    fn from_byte(b: u8) -> Result<Self> {
        match b {
            0 => Ok(Self::Shared),
            1 => Ok(Self::Subjects),
            2 => Ok(Self::Predicates),
            3 => Ok(Self::Objects),
            4 => Ok(Self::Graphs),
            _ => bail!("Invalid DictSection byte: {b}"),
        }
    }
}

/// Result of looking up a term in the SST.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TermId {
    pub section: DictSection,
    pub local_id: u64,
}

/// Builder for writing an SST file from sorted entries.
pub struct SstWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    index: Vec<IndexEntry>,
    record_count: usize,
    bytes_written: u64,
}

/// A sparse index entry: the key at the start of each block and its file offset.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
}

impl SstWriter {
    /// Create a new SST writer at the given path.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)
            .with_context(|| format!("Failed to create SST file {}", path.display()))?;
        let writer = BufWriter::with_capacity(256 * 1024, file);
        Ok(Self {
            writer,
            path,
            index: Vec::new(),
            record_count: 0,
            bytes_written: 0,
        })
    }

    /// Write a record to the SST. Entries MUST be written in sorted key order.
    pub fn write_entry(&mut self, key: &[u8], section: DictSection, id: u64) -> Result<()> {
        // Record sparse index entry at block boundaries
        if self.record_count % BLOCK_SIZE == 0 {
            self.index.push(IndexEntry {
                key: key.to_vec(),
                offset: self.bytes_written,
            });
        }

        // Write: u32(key_len) | key_bytes | u8(section) | u64(id)
        let key_len = key.len() as u32;
        self.writer.write_all(&key_len.to_le_bytes())?;
        self.writer.write_all(key)?;
        self.writer.write_all(&[section as u8])?;
        self.writer.write_all(&id.to_le_bytes())?;

        self.bytes_written += 4 + key.len() as u64 + 1 + 8;
        self.record_count += 1;

        Ok(())
    }

    /// Finish writing and return the path and index for creating an SstReader.
    pub fn finish(mut self) -> Result<(PathBuf, Vec<IndexEntry>)> {
        self.writer.flush()?;
        tracing::info!(
            "SST written: {} records, {} bytes, {} index entries",
            self.record_count,
            self.bytes_written,
            self.index.len()
        );
        Ok((self.path, self.index))
    }
}

/// Reader for looking up terms in an SST file.
pub struct SstReader {
    mmap: Mmap,
    index: Vec<IndexEntry>,
    path: PathBuf,
}

impl SstReader {
    /// Open an SST file for reading.
    pub fn open(path: PathBuf, index: Vec<IndexEntry>) -> Result<Self> {
        let file = File::open(&path)
            .with_context(|| format!("Failed to open SST file {}", path.display()))?;
        let mmap = unsafe { Mmap::map(&file) }
            .with_context(|| format!("Failed to mmap SST file {}", path.display()))?;
        Ok(Self { mmap, index, path })
    }

    /// Look up a term by key. Returns the section and local ID if found.
    pub fn get(&self, key: &[u8]) -> Option<TermId> {
        if self.index.is_empty() {
            return None;
        }

        // Binary search the sparse index to find the block
        let block_idx = match self
            .index
            .binary_search_by(|entry| entry.key.as_slice().cmp(key))
        {
            Ok(i) => i,             // Exact match on a block boundary
            Err(0) => return None,   // Key is before all entries
            Err(i) => i - 1,        // Key is in the block starting at i-1
        };

        // Linear scan within the block
        let start_offset = self.index[block_idx].offset as usize;
        let end_offset = if block_idx + 1 < self.index.len() {
            self.index[block_idx + 1].offset as usize
        } else {
            self.mmap.len()
        };

        let mut pos = start_offset;
        while pos < end_offset {
            // Read key_len
            if pos + 4 > self.mmap.len() {
                break;
            }
            let key_len =
                u32::from_le_bytes(self.mmap[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Read key
            if pos + key_len > self.mmap.len() {
                break;
            }
            let record_key = &self.mmap[pos..pos + key_len];
            pos += key_len;

            // Read section + id
            if pos + 9 > self.mmap.len() {
                break;
            }
            let section_byte = self.mmap[pos];
            pos += 1;
            let id = u64::from_le_bytes(self.mmap[pos..pos + 8].try_into().unwrap());
            pos += 8;

            match record_key.cmp(key) {
                std::cmp::Ordering::Equal => {
                    return Some(TermId {
                        section: DictSection::from_byte(section_byte).ok()?,
                        local_id: id,
                    });
                }
                std::cmp::Ordering::Greater => {
                    // Past the key, not found
                    return None;
                }
                std::cmp::Ordering::Less => {
                    // Keep scanning
                }
            }
        }

        None
    }

    /// Clean up the SST file.
    pub fn cleanup(&self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            tracing::warn!("Failed to remove SST file {}: {}", self.path.display(), e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_test_sst(entries: &[(&str, DictSection, u64)]) -> (tempfile::TempDir, SstReader) {
        let temp_dir = tempfile::tempdir().unwrap();
        let sst_path = temp_dir.path().join("test.sst");

        let mut writer = SstWriter::new(&sst_path).unwrap();
        for &(key, section, id) in entries {
            writer.write_entry(key.as_bytes(), section, id).unwrap();
        }
        let (path, index) = writer.finish().unwrap();
        let reader = SstReader::open(path, index).unwrap();
        (temp_dir, reader)
    }

    #[test]
    fn test_single_entry() {
        let (_dir, reader) = build_test_sst(&[("<http://example.org/a>", DictSection::Shared, 1)]);

        let result = reader.get(b"<http://example.org/a>").unwrap();
        assert_eq!(result.section, DictSection::Shared);
        assert_eq!(result.local_id, 1);
    }

    #[test]
    fn test_not_found() {
        let (_dir, reader) = build_test_sst(&[("<http://example.org/a>", DictSection::Shared, 1)]);

        assert!(reader.get(b"<http://example.org/b>").is_none());
        assert!(reader.get(b"<http://example.org/>").is_none());
    }

    #[test]
    fn test_multiple_entries() {
        let entries = vec![
            ("<http://example.org/a>", DictSection::Shared, 1),
            ("<http://example.org/b>", DictSection::Subjects, 2),
            ("<http://example.org/c>", DictSection::Objects, 3),
            ("<http://example.org/d>", DictSection::Predicates, 1),
        ];
        let (_dir, reader) = build_test_sst(&entries);

        for &(key, section, id) in &entries {
            let result = reader.get(key.as_bytes()).unwrap();
            assert_eq!(result.section, section, "wrong section for {key}");
            assert_eq!(result.local_id, id, "wrong id for {key}");
        }
    }

    #[test]
    fn test_many_entries_across_blocks() {
        // Create more entries than BLOCK_SIZE to test multi-block lookup
        let entries: Vec<(String, DictSection, u64)> = (0..BLOCK_SIZE * 3 + 50)
            .map(|i| {
                (
                    format!("<http://example.org/resource{i:06}>"),
                    DictSection::Shared,
                    i as u64 + 1,
                )
            })
            .collect();

        let entry_refs: Vec<(&str, DictSection, u64)> = entries
            .iter()
            .map(|(k, s, i)| (k.as_str(), *s, *i))
            .collect();

        let (_dir, reader) = build_test_sst(&entry_refs);

        // Check a few entries from different blocks
        let result = reader.get(entries[0].0.as_bytes()).unwrap();
        assert_eq!(result.local_id, 1);

        let mid = BLOCK_SIZE + 500;
        let result = reader.get(entries[mid].0.as_bytes()).unwrap();
        assert_eq!(result.local_id, mid as u64 + 1);

        let last = entries.len() - 1;
        let result = reader.get(entries[last].0.as_bytes()).unwrap();
        assert_eq!(result.local_id, last as u64 + 1);

        // Check non-existent
        assert!(reader.get(b"<http://example.org/zzz>").is_none());
    }
}
