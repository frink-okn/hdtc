//! Partial vocabulary file format for batched processing.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Magic number for partial vocabulary files: "PVOC" (0x50564F43)
const MAGIC: u32 = 0x50564F43;

/// Partial vocabulary entry (term + role flags + local IDs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialVocabEntry {
    pub term: Vec<u8>,
    pub roles: u8,
    pub so_local_id: Option<u32>, // Local ID in subject/object space (if used as S or O)
    pub p_local_id: Option<u32>,  // Local ID in predicate space (if used as P)
}

impl PartialVocabEntry {
    pub fn new(term: Vec<u8>, roles: u8, so_local_id: Option<u32>, p_local_id: Option<u32>) -> Self {
        Self { term, roles, so_local_id, p_local_id }
    }
}

/// Writer for partial vocabulary files.
pub struct PartialVocabWriter {
    encoder: zstd::Encoder<'static, BufWriter<File>>,
    count: u32,
}

impl PartialVocabWriter {
    /// Create a new partial vocabulary writer.
    pub fn create(path: &Path) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("Failed to create partial vocab file: {}", path.display()))?;
        let buf_writer = BufWriter::new(file);
        let encoder = zstd::Encoder::new(buf_writer, 3)?; // Level 3 compression

        Ok(Self { encoder, count: 0 })
    }

    /// Write header (magic + count + max local IDs).
    pub fn write_header(&mut self, entry_count: u32, max_so_id: u32, max_p_id: u32) -> Result<()> {
        // Write magic number
        self.encoder.write_all(&MAGIC.to_le_bytes())?;
        // Write entry count
        self.encoder.write_all(&entry_count.to_le_bytes())?;
        // Write max local IDs for pre-allocating mappings
        self.encoder.write_all(&max_so_id.to_le_bytes())?;
        self.encoder.write_all(&max_p_id.to_le_bytes())?;
        Ok(())
    }

    /// Write a single entry.
    pub fn write_entry(&mut self, entry: &PartialVocabEntry) -> Result<()> {
        use super::batch_vocab::{ROLE_SUBJECT, ROLE_OBJECT, ROLE_PREDICATE};

        // Write term length
        let term_len = entry.term.len() as u32;
        self.encoder.write_all(&term_len.to_le_bytes())?;
        // Write term bytes
        self.encoder.write_all(&entry.term)?;
        // Write roles
        self.encoder.write_all(&[entry.roles])?;

        // Write SO local ID if term is used as subject/object
        let is_so = (entry.roles & (ROLE_SUBJECT | ROLE_OBJECT)) != 0;
        if is_so {
            let so_id = entry.so_local_id.expect("SO local ID must be present when roles include S/O");
            self.encoder.write_all(&so_id.to_le_bytes())?;
        }

        // Write P local ID if term is used as predicate
        let is_p = (entry.roles & ROLE_PREDICATE) != 0;
        if is_p {
            let p_id = entry.p_local_id.expect("P local ID must be present when roles include P");
            self.encoder.write_all(&p_id.to_le_bytes())?;
        }

        self.count += 1;
        Ok(())
    }

    /// Finish writing and close the file.
    pub fn finish(mut self) -> Result<()> {
        self.encoder.finish()?;
        Ok(())
    }

    /// Get number of entries written.
    pub fn count(&self) -> u32 {
        self.count
    }
}

/// Reader for partial vocabulary files.
pub struct PartialVocabReader {
    decoder: zstd::Decoder<'static, BufReader<File>>,
    count: u32,
    max_so_id: u32,
    max_p_id: u32,
    entries_read: u32,
}

impl PartialVocabReader {
    /// Open a partial vocabulary file for reading.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("Failed to open partial vocab file: {}", path.display()))?;
        let buf_reader = BufReader::new(file);
        let mut decoder = zstd::Decoder::with_buffer(buf_reader)?;

        // Read header
        let mut magic_bytes = [0u8; 4];
        decoder.read_exact(&mut magic_bytes)?;
        let magic = u32::from_le_bytes(magic_bytes);
        if magic != MAGIC {
            anyhow::bail!("Invalid partial vocab file magic: expected {:#x}, got {:#x}", MAGIC, magic);
        }

        let mut count_bytes = [0u8; 4];
        decoder.read_exact(&mut count_bytes)?;
        let count = u32::from_le_bytes(count_bytes);

        let mut max_so_id_bytes = [0u8; 4];
        decoder.read_exact(&mut max_so_id_bytes)?;
        let max_so_id = u32::from_le_bytes(max_so_id_bytes);

        let mut max_p_id_bytes = [0u8; 4];
        decoder.read_exact(&mut max_p_id_bytes)?;
        let max_p_id = u32::from_le_bytes(max_p_id_bytes);

        Ok(Self {
            decoder,
            count,
            max_so_id,
            max_p_id,
            entries_read: 0,
        })
    }

    /// Get max SO local ID in this batch.
    pub fn max_so_id(&self) -> u32 {
        self.max_so_id
    }

    /// Get max P local ID in this batch.
    pub fn max_p_id(&self) -> u32 {
        self.max_p_id
    }

    /// Read the next entry.
    pub fn read_entry(&mut self) -> Result<Option<PartialVocabEntry>> {
        use super::batch_vocab::{ROLE_SUBJECT, ROLE_OBJECT, ROLE_PREDICATE};

        if self.entries_read >= self.count {
            return Ok(None);
        }

        // Read term length
        let mut term_len_bytes = [0u8; 4];
        if self.decoder.read_exact(&mut term_len_bytes).is_err() {
            return Ok(None); // End of file
        }
        let term_len = u32::from_le_bytes(term_len_bytes) as usize;

        // Read term bytes
        let mut term = vec![0u8; term_len];
        self.decoder.read_exact(&mut term)?;

        // Read roles
        let mut roles_byte = [0u8; 1];
        self.decoder.read_exact(&mut roles_byte)?;
        let roles = roles_byte[0];

        // Read SO local ID if present
        let is_so = (roles & (ROLE_SUBJECT | ROLE_OBJECT)) != 0;
        let so_local_id = if is_so {
            let mut id_bytes = [0u8; 4];
            self.decoder.read_exact(&mut id_bytes)?;
            Some(u32::from_le_bytes(id_bytes))
        } else {
            None
        };

        // Read P local ID if present
        let is_p = (roles & ROLE_PREDICATE) != 0;
        let p_local_id = if is_p {
            let mut id_bytes = [0u8; 4];
            self.decoder.read_exact(&mut id_bytes)?;
            Some(u32::from_le_bytes(id_bytes))
        } else {
            None
        };

        self.entries_read += 1;

        Ok(Some(PartialVocabEntry { term, roles, so_local_id, p_local_id }))
    }

    /// Get total number of entries in the file.
    pub fn total_entries(&self) -> u32 {
        self.count
    }
}

impl Iterator for PartialVocabReader {
    type Item = Result<PartialVocabEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_entry() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_partial_vocab_roundtrip() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test.pvoc.zst");

        // Write some entries
        let mut writer = PartialVocabWriter::create(&path)?;
        writer.write_header(3, 2, 1)?; // 3 entries, max_so_id=2, max_p_id=1
        writer.write_entry(&PartialVocabEntry::new(b"term1".to_vec(), 0x01, Some(0), None))?; // subject only
        writer.write_entry(&PartialVocabEntry::new(b"term2".to_vec(), 0x02, None, Some(0)))?; // predicate only
        writer.write_entry(&PartialVocabEntry::new(b"term3".to_vec(), 0x04, Some(1), None))?; // object only
        let count = writer.count();
        writer.finish()?;

        assert_eq!(count, 3);

        // Read them back
        let mut reader = PartialVocabReader::open(&path)?;
        assert_eq!(reader.total_entries(), 3);
        assert_eq!(reader.max_so_id(), 2);
        assert_eq!(reader.max_p_id(), 1);

        let entries: Vec<_> = reader.collect::<Result<Vec<_>>>()?;
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].term, b"term1");
        assert_eq!(entries[0].roles, 0x01);
        assert_eq!(entries[0].so_local_id, Some(0));
        assert_eq!(entries[0].p_local_id, None);
        assert_eq!(entries[1].term, b"term2");
        assert_eq!(entries[1].roles, 0x02);
        assert_eq!(entries[1].so_local_id, None);
        assert_eq!(entries[1].p_local_id, Some(0));
        assert_eq!(entries[2].term, b"term3");

        Ok(())
    }
}
