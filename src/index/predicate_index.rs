//! PredicateIndex builder - creates inverted index structures for fast predicate queries.
//!
//! Implements the hdt-java PredicateIndexArray algorithm for building:
//! 1. Bitmap marking predicate group boundaries
//! 2. Sequence mapping predicate occurrences to their positions

use crate::io::{BitmapWriter, LogArrayWriter, StreamingBitmapEncoder, StreamingLogArrayEncoder};
use crate::io::log_array::bits_for;
use crate::sort::Sortable;
use crate::triples::{StreamingBitmapResult, StreamingLogArrayResult};
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// PredicateIndex auxiliary structures for fast predicate lookups.
#[allow(dead_code)]
pub struct PredicateIndex {
    /// Serialized bitmap marking predicate group boundaries.
    pub bitmap: Vec<u8>,
    /// Serialized sequence mapping predicate occurrences to positions.
    pub sequence: Vec<u8>,
}

/// Build PredicateIndex from a predicate sequence (seqY).
///
/// Algorithm (from hdt-java PredicateIndexArray):
/// 1. Count occurrences of each predicate value
/// 2. Build bitmap marking group boundaries (accumulated counts)
/// 3. Build sequence mapping occurrences to original positions
///
/// # Arguments
/// - `predicates`: seqY from BitmapTriples (predicate IDs in permuted order)
/// - `max_predicate`: Maximum predicate ID value
#[allow(dead_code)]
pub fn build_predicate_index(predicates: &[u64], max_predicate: u64) -> Result<PredicateIndex> {
    let num_predicates = predicates.len() as u64;

    if num_predicates == 0 {
        // Edge case: no predicates
        let bitmap = BitmapWriter::new();
        let mut bitmap_buf = Vec::new();
        bitmap.write_to(&mut bitmap_buf)?;

        let array = LogArrayWriter::for_max_value(0);
        let mut array_buf = Vec::new();
        array.write_to(&mut array_buf)?;

        return Ok(PredicateIndex {
            bitmap: bitmap_buf,
            sequence: array_buf,
        });
    }

    // Phase 1: Count predicate occurrences
    let mut pred_count = vec![0u64; (max_predicate + 1) as usize];
    for &p in predicates {
        if (p as usize) < pred_count.len() {
            pred_count[p as usize] += 1;
        }
    }

    // Phase 2: Build bitmap marking predicate group boundaries
    // Bitmap marks the position of the last occurrence of each predicate group
    let mut bitmap = BitmapWriter::new();
    let mut accumulated = 0u64;

    for count in pred_count[1..].iter() {
        accumulated += count;
        if accumulated > num_predicates {
            break;
        }

        // We need to set bit at position (accumulated - 1) to mark group boundary
        // But BitmapWriter only supports push() and set_last()
        // So we build incrementally by pushing and then adjust
        while bitmap.len() < accumulated {
            bitmap.push(false);
        }
        if bitmap.len() == accumulated {
            bitmap.set_last(true);
        }
    }

    // Ensure we have exactly num_predicates bits
    while bitmap.len() < num_predicates {
        bitmap.push(false);
    }

    // Make sure last bit is set (marks end of last group)
    if !bitmap.is_empty() {
        bitmap.set_last(true);
    }

    // Phase 3: Build sequence mapping occurrences to positions
    // Calculate base positions for each predicate using accumulated counts
    let mut bases = vec![0u64; (max_predicate + 1) as usize];
    let mut accumulated = 0u64;
    for p in 1..=max_predicate {
        bases[p as usize] = accumulated;
        if (p as usize) < pred_count.len() {
            accumulated += pred_count[p as usize];
        }
    }

    // Build inverted sequence - maps predicate occurrences to positions
    let mut temp_array = vec![0u64; num_predicates as usize];
    let mut insert_positions = vec![0u64; (max_predicate + 1) as usize];

    for (pos, &pred) in predicates.iter().enumerate() {
        if (pred as usize) < bases.len() {
            let base = bases[pred as usize];
            let offset = insert_positions[pred as usize];
            let idx = (base + offset) as usize;
            if idx < temp_array.len() {
                temp_array[idx] = pos as u64;
            }
            insert_positions[pred as usize] += 1;
        }
    }

    // Serialize bitmap
    let mut bitmap_buf = Vec::new();
    bitmap.write_to(&mut bitmap_buf)?;

    // Serialize sequence
    let max_pos = if num_predicates > 0 {
        num_predicates - 1
    } else {
        0
    };
    let mut array = LogArrayWriter::for_max_value(max_pos);
    for val in temp_array {
        array.push(val);
    }

    let mut array_buf = Vec::new();
    array.write_to(&mut array_buf)?;

    Ok(PredicateIndex {
        bitmap: bitmap_buf,
        sequence: array_buf,
    })
}

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
                tracing::warn!("Failed to delete predicate index temp file {}: {}", path.display(), e);
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
            bitmap: StreamingBitmapResult { path: bitmap_path, num_bits: 0 },
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

/// Build predicate count sequence (for index file).
///
/// This stores the count of occurrences for each predicate ID.
///
/// # Arguments
/// - `predicates`: seqY from BitmapTriples
/// - `max_predicate`: Maximum predicate ID value
#[allow(dead_code)]
pub fn build_predicate_count(predicates: &[u64], max_predicate: u64) -> Result<Vec<u8>> {
    let mut counts = vec![0u64; max_predicate as usize];
    for &p in predicates {
        if p > 0 && (p as usize) <= counts.len() {
            counts[(p - 1) as usize] += 1;
        }
    }

    let bits = if predicates.is_empty() {
        1
    } else {
        (64 - (predicates.len() as u64).leading_zeros()) as u8
    };
    let mut writer = LogArrayWriter::new(bits);

    for &count in &counts {
        writer.push(count);
    }

    let mut buf = Vec::new();
    writer.write_to(&mut buf)?;
    Ok(buf)
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use crate::io::LogArrayReader;

    #[test]
    fn test_predicate_count() -> Result<()> {
        // Test predicates: [1, 1, 2, 1, 3]
        let predicates = vec![1u64, 1, 2, 1, 3];

        let counts_buf = build_predicate_count(&predicates, 3)?;

        // Parse back to verify
        let reader = LogArrayReader::read_from(&mut Cursor::new(counts_buf))?;
        assert_eq!(reader.len(), 3);
        assert_eq!(reader.get(0), 3); // pred 1 has 3 occurrences
        assert_eq!(reader.get(1), 1); // pred 2 has 1 occurrence
        assert_eq!(reader.get(2), 1); // pred 3 has 1 occurrence

        Ok(())
    }

    #[test]
    fn test_single_predicate() -> Result<()> {
        let predicates = vec![1u64, 1, 1];

        let pred_index = build_predicate_index(&predicates, 1)?;

        // Should have bitmap and sequence
        assert!(!pred_index.bitmap.is_empty());
        assert!(!pred_index.sequence.is_empty());

        // Verify count
        let counts_buf = build_predicate_count(&predicates, 1)?;
        let reader = LogArrayReader::read_from(&mut Cursor::new(counts_buf))?;
        assert_eq!(reader.len(), 1);
        assert_eq!(reader.get(0), 3);

        Ok(())
    }

    #[test]
    fn test_multiple_predicates() -> Result<()> {
        // Test with mixed predicates
        let predicates = vec![1u64, 2, 1, 3, 2, 1];
        let max_pred = 3;

        let pred_index = build_predicate_index(&predicates, max_pred)?;
        assert!(!pred_index.bitmap.is_empty());
        assert!(!pred_index.sequence.is_empty());

        // Verify counts
        let counts_buf = build_predicate_count(&predicates, max_pred)?;
        let reader = LogArrayReader::read_from(&mut Cursor::new(counts_buf))?;
        assert_eq!(reader.len(), 3);
        assert_eq!(reader.get(0), 3); // pred 1 appears 3 times
        assert_eq!(reader.get(1), 2); // pred 2 appears 2 times
        assert_eq!(reader.get(2), 1); // pred 3 appears 1 time

        Ok(())
    }
}
