//! PredicateIndex builder - creates inverted index structures for fast predicate queries.
//!
//! Implements the hdt-java PredicateIndexArray algorithm for building:
//! 1. Bitmap marking predicate group boundaries
//! 2. Sequence mapping predicate occurrences to their positions

use crate::io::{BitmapWriter, LogArrayWriter};
use anyhow::Result;

/// PredicateIndex auxiliary structures for fast predicate lookups.
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

/// Build predicate count sequence (for index file).
///
/// This stores the count of occurrences for each predicate ID.
///
/// # Arguments
/// - `predicates`: seqY from BitmapTriples
/// - `max_predicate`: Maximum predicate ID value
pub fn build_predicate_count(predicates: &[u64], max_predicate: u64) -> Result<Vec<u8>> {
    let mut counts = vec![0u64; (max_predicate + 1) as usize];
    for &p in predicates {
        counts[p as usize] += 1;
    }

    let max_count = *counts.iter().max().unwrap_or(&0);
    let mut writer = LogArrayWriter::for_max_value(max_count);

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
        assert_eq!(reader.get(0), 0); // pred 0 has 0 occurrences
        assert_eq!(reader.get(1), 3); // pred 1 has 3 occurrences
        assert_eq!(reader.get(2), 1); // pred 2 has 1 occurrence
        assert_eq!(reader.get(3), 1); // pred 3 has 1 occurrence

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
        assert_eq!(reader.get(1), 3);

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
        assert_eq!(reader.get(1), 3); // pred 1 appears 3 times
        assert_eq!(reader.get(2), 2); // pred 2 appears 2 times
        assert_eq!(reader.get(3), 1); // pred 3 appears 1 time

        Ok(())
    }
}
