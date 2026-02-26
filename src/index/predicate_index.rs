//! PredicateIndex builder - creates inverted index structures for fast predicate queries.
//!
//! Implements the hdt-java PredicateIndexArray algorithm for building:
//! 1. Bitmap marking predicate group boundaries
//! 2. Sequence mapping predicate occurrences to their positions

use crate::io::log_array::bits_for;
use crate::io::{LogArrayWriter, StreamingBitmapEncoder, StreamingLogArrayEncoder};
use crate::sort::Sortable;
use crate::triples::{StreamingBitmapResult, StreamingLogArrayResult};
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Sort entry for streaming predicate index construction.
/// Sorted by (predicate, pos_y) to group entries by predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredicateEntry {
    pub predicate: u64,
    pub pos_y: u64,
}

impl Ord for PredicateEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.predicate
            .cmp(&other.predicate)
            .then(self.pos_y.cmp(&other.pos_y))
    }
}

impl PartialOrd for PredicateEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Sortable for PredicateEntry {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.predicate.to_le_bytes())?;
        writer.write_all(&self.pos_y.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; 16];
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        Ok(Some(Self {
            predicate: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            pos_y: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        }))
    }

    fn mem_size(&self) -> usize {
        16
    }
}

/// Result of streaming predicate index construction.
pub struct StreamingPredicateIndexResult {
    pub bitmap: StreamingBitmapResult,
    pub sequence: StreamingLogArrayResult,
    pub predicate_count: Vec<u8>,
}

impl StreamingPredicateIndexResult {
    pub fn cleanup(&self) {
        for path in [&self.bitmap.path, &self.sequence.path] {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!(
                    "Failed to delete predicate index temp file {}: {}",
                    path.display(),
                    e
                );
            }
        }
    }
}

/// Build PredicateIndex from a sorted iterator of PredicateEntry values.
///
/// The entries MUST be sorted by (predicate, pos_y). This function streams
/// the sorted output into temp files using O(1) memory (plus O(max_predicate)
/// for counts).
pub fn build_predicate_index_streaming(
    sorted_entries: impl Iterator<Item = Result<PredicateEntry>>,
    num_sp_pairs: u64,
    max_predicate: u64,
    temp_dir: &Path,
) -> Result<StreamingPredicateIndexResult> {
    let bitmap_path = temp_dir.join("idx_pred_bitmap.tmp");
    let sequence_path = temp_dir.join("idx_pred_sequence.tmp");

    if num_sp_pairs == 0 {
        // Write empty structures
        File::create(&bitmap_path).context("create pred bitmap temp")?;
        File::create(&sequence_path).context("create pred sequence temp")?;

        let pred_count_buf = build_predicate_count_from_counts(&[], 0)?;

        return Ok(StreamingPredicateIndexResult {
            bitmap: StreamingBitmapResult {
                path: bitmap_path,
                num_bits: 0,
            },
            sequence: StreamingLogArrayResult {
                path: sequence_path,
                bits_per_entry: 1,
                num_entries: 0,
            },
            predicate_count: pred_count_buf,
        });
    }

    let max_pos_y = num_sp_pairs.saturating_sub(1);
    let seq_bits = bits_for(max_pos_y);

    let bitmap_file = File::create(&bitmap_path).context("create pred bitmap temp")?;
    let mut bitmap_encoder =
        StreamingBitmapEncoder::new(BufWriter::with_capacity(256 * 1024, bitmap_file));

    let seq_file = File::create(&sequence_path).context("create pred sequence temp")?;
    let mut seq_encoder =
        StreamingLogArrayEncoder::new(seq_bits, BufWriter::with_capacity(256 * 1024, seq_file));

    let mut counts = vec![0u64; max_predicate as usize];
    let mut current_predicate: Option<u64> = None;
    let mut emitted = 0u64;

    for entry_result in sorted_entries {
        let entry = entry_result?;

        // Mark boundary when predicate changes
        if let Some(prev) = current_predicate
            && entry.predicate != prev
        {
            bitmap_encoder.set_last(true);
        }
        current_predicate = Some(entry.predicate);

        bitmap_encoder.push(false)?;
        seq_encoder.push(entry.pos_y)?;
        emitted += 1;

        // Track counts (predicate IDs are 1-based)
        if entry.predicate > 0 && (entry.predicate as usize) <= counts.len() {
            counts[(entry.predicate - 1) as usize] += 1;
        }

        if emitted.is_multiple_of(5_000_000) {
            tracing::debug!(
                "Predicate index: serialized {} / {} entries ({:.1}%)",
                emitted,
                num_sp_pairs,
                (emitted as f64 / num_sp_pairs as f64) * 100.0
            );
        }
    }

    // Mark last boundary
    if current_predicate.is_some() {
        bitmap_encoder.set_last(true);
    }

    let (bitmap_num_bits, mut bitmap_writer) = bitmap_encoder.finish()?;
    bitmap_writer.flush()?;

    let (seq_num_entries, seq_bpe, mut seq_writer) = seq_encoder.finish()?;
    seq_writer.flush()?;

    let pred_count_buf = build_predicate_count_from_counts(&counts, num_sp_pairs)?;

    Ok(StreamingPredicateIndexResult {
        bitmap: StreamingBitmapResult {
            path: bitmap_path,
            num_bits: bitmap_num_bits,
        },
        sequence: StreamingLogArrayResult {
            path: sequence_path,
            bits_per_entry: seq_bpe,
            num_entries: seq_num_entries,
        },
        predicate_count: pred_count_buf,
    })
}

/// Build predicate count sequence from pre-computed counts.
fn build_predicate_count_from_counts(counts: &[u64], num_sp_pairs: u64) -> Result<Vec<u8>> {
    let bits = if num_sp_pairs == 0 {
        1
    } else {
        (64 - num_sp_pairs.leading_zeros()) as u8
    };
    let mut writer = LogArrayWriter::new(bits);

    for &count in counts {
        writer.push(count);
    }

    let mut buf = Vec::new();
    writer.write_to(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{BitmapReader, LogArrayReader};
    use std::io::Cursor;
    use tempfile::TempDir;

    #[test]
    fn test_predicate_count_from_counts() -> Result<()> {
        let counts_buf = build_predicate_count_from_counts(&[3, 1, 1], 5)?;
        let reader = LogArrayReader::read_from(&mut Cursor::new(counts_buf))?;
        assert_eq!(reader.len(), 3);
        assert_eq!(reader.get(0), 3);
        assert_eq!(reader.get(1), 1);
        assert_eq!(reader.get(2), 1);

        Ok(())
    }

    #[test]
    fn test_build_predicate_index_streaming_empty() -> Result<()> {
        let tmp = TempDir::new()?;
        let result = build_predicate_index_streaming(std::iter::empty(), 0, 0, tmp.path())?;

        let counts_reader = LogArrayReader::read_from(&mut Cursor::new(&result.predicate_count))?;
        assert_eq!(counts_reader.len(), 0);

        result.cleanup();
        Ok(())
    }

    #[test]
    fn test_build_predicate_index_streaming_groups_and_sequence() -> Result<()> {
        let tmp = TempDir::new()?;
        let entries = vec![
            PredicateEntry {
                predicate: 1,
                pos_y: 0,
            },
            PredicateEntry {
                predicate: 1,
                pos_y: 2,
            },
            PredicateEntry {
                predicate: 2,
                pos_y: 1,
            },
            PredicateEntry {
                predicate: 3,
                pos_y: 3,
            },
            PredicateEntry {
                predicate: 3,
                pos_y: 4,
            },
        ];

        let result =
            build_predicate_index_streaming(entries.into_iter().map(Ok), 5, 3, tmp.path())?;

        let mut bitmap_section = Vec::new();
        crate::hdt::writer::write_bitmap_from_file(
            &mut bitmap_section,
            &result.bitmap.path,
            result.bitmap.num_bits,
        )?;
        let bitmap = BitmapReader::read_from(&mut Cursor::new(bitmap_section))?;
        assert_eq!(bitmap.len(), 5);
        assert!(!bitmap.get(0));
        assert!(bitmap.get(1));
        assert!(bitmap.get(2));
        assert!(!bitmap.get(3));
        assert!(bitmap.get(4));

        let mut sequence_section = Vec::new();
        crate::hdt::writer::write_log_array_from_file(
            &mut sequence_section,
            &result.sequence.path,
            result.sequence.bits_per_entry,
            result.sequence.num_entries,
        )?;
        let sequence = LogArrayReader::read_from(&mut Cursor::new(sequence_section))?;
        assert_eq!(sequence.len(), 5);
        assert_eq!(sequence.get(0), 0);
        assert_eq!(sequence.get(1), 2);
        assert_eq!(sequence.get(2), 1);
        assert_eq!(sequence.get(3), 3);
        assert_eq!(sequence.get(4), 4);

        let counts_reader = LogArrayReader::read_from(&mut Cursor::new(&result.predicate_count))?;
        assert_eq!(counts_reader.len(), 3);
        assert_eq!(counts_reader.get(0), 2);
        assert_eq!(counts_reader.get(1), 1);
        assert_eq!(counts_reader.get(2), 2);

        result.cleanup();
        Ok(())
    }
}
