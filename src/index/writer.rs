//! Index file writer - creates .hdt.index.v1-1 files.
//!
//! Writes the 5-section index file format used by hdt-java.

use crate::io::ControlInfo;
use crate::index::predicate_index::PredicateIndex;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Write HDT index file (.hdt.index.v1-1) in OPS order.
///
/// The index file contains five binary sections:
/// 1. bitmapIndexZ - Bitmap from OPS BitmapTriples Z component (marks object boundaries)
/// 2. indexZ - Sequence from OPS BitmapTriples Z component (subject IDs)
/// 3. predicateIndex.bitmap - Inverted index boundaries for predicates
/// 4. predicateIndex.sequence - Position mappings for predicate occurrences
/// 5. predicateCount - Per-predicate occurrence counts
///
/// # Arguments
/// - `output_path`: Path where the index file will be created
/// - `num_triples`: Number of triples in the dataset
/// - `triples_order`: Triple order ordinal from main HDT triples control info
/// - `bitmap_index_z`: Serialized bitmapIndexZ component bytes
/// - `index_z`: Serialized indexZ component bytes
/// - `predicate_index`: PredicateIndex structures
/// - `predicate_count`: Serialized predicate count sequence
pub fn write_index(
    output_path: &Path,
    num_triples: u64,
    triples_order: u64,
    bitmap_index_z: &[u8],
    index_z: &[u8],
    predicate_index: &PredicateIndex,
    predicate_count: &[u8],
) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("Failed to create index file {}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);

    // Write Control Information
    // The index file uses indexFoQ format and INDEX control type (per hdt-java)
    let mut ci = ControlInfo::new(
        crate::io::ControlType::Index,
        "<http://purl.org/HDT/hdt#indexFoQ>",
    );
    ci.set_property("numTriples", num_triples.to_string());
    ci.set_property("order", triples_order.to_string());
    ci.write_to(&mut writer)
        .context("Failed to write control info to index file")?;

    // Write five sections in order

    // Section 1: bitmapIndexZ (BitmapZ from OPS BitmapTriples)
    // This marks object boundaries in the permuted order
    writer
        .write_all(bitmap_index_z)
        .context("Failed to write bitmapIndexZ section")?;

    // Section 2: indexZ (ArrayZ from OPS BitmapTriples)
    // This is the subject ID sequence in OPS order
    writer
        .write_all(index_z)
        .context("Failed to write indexZ section")?;

    // Section 3: predicateIndex.bitmap
    // Inverted index boundaries for predicates
    writer
        .write_all(&predicate_index.bitmap)
        .context("Failed to write predicateIndex bitmap section")?;

    // Section 4: predicateIndex.sequence
    // Position mappings for predicate occurrences
    writer
        .write_all(&predicate_index.sequence)
        .context("Failed to write predicateIndex sequence section")?;

    // Section 5: predicateCount
    // Per-predicate occurrence counts
    writer
        .write_all(predicate_count)
        .context("Failed to write predicateCount section")?;

    writer.flush()?;

    tracing::info!("Index file written: {}", output_path.display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{BitmapReader, BitmapWriter, ControlType, LogArrayReader, LogArrayWriter};
    use std::io::Cursor;

    fn build_bitmap(bits: &[bool]) -> Vec<u8> {
        let mut writer = BitmapWriter::new();
        for &bit in bits {
            writer.push(bit);
        }
        let mut buf = Vec::new();
        writer.write_to(&mut buf).expect("serialize bitmap");
        buf
    }

    fn build_logarray(values: &[u64], max_value: u64) -> Vec<u8> {
        let mut writer = LogArrayWriter::for_max_value(max_value);
        for &value in values {
            writer.push(value);
        }
        let mut buf = Vec::new();
        writer.write_to(&mut buf).expect("serialize logarray");
        buf
    }

    #[test]
    fn test_index_file_created() -> Result<()> {
        // Create a minimal test index in memory
        let output_path = std::env::temp_dir().join("test_index.hdt.index.v1-1");

        let predicate_index = PredicateIndex {
            bitmap: vec![],
            sequence: vec![],
        };

        write_index(&output_path, 0, 1, &[], &[], &predicate_index, &[])?;

        // Verify file was created
        assert!(output_path.exists());

        // Clean up
        let _ = std::fs::remove_file(&output_path);

        Ok(())
    }

    #[test]
    fn test_index_control_info_type_and_properties() -> Result<()> {
        let output_path = std::env::temp_dir().join("test_index_control_info.hdt.index.v1-1");

        let bitmap_index_z = build_bitmap(&[true, false, true]);
        let index_z = build_logarray(&[0, 1, 2], 2);
        let pred_bitmap = build_bitmap(&[true, true]);
        let pred_seq = build_logarray(&[0, 1], 1);
        let pred_count = build_logarray(&[2, 1], 2);

        let predicate_index = PredicateIndex {
            bitmap: pred_bitmap,
            sequence: pred_seq,
        };

        write_index(
            &output_path,
            3,
            1,
            &bitmap_index_z,
            &index_z,
            &predicate_index,
            &pred_count,
        )?;

        let bytes = std::fs::read(&output_path)?;
        assert!(bytes.len() > 6, "index file should include control info");
        assert_eq!(bytes[4], 5, "control type byte must be INDEX (5)");

        let mut cursor = Cursor::new(bytes);
        let ci = crate::io::ControlInfo::read_from(&mut cursor)?;
        assert_eq!(ci.control_type, ControlType::Index);
        assert_eq!(ci.format, "<http://purl.org/HDT/hdt#indexFoQ>");
        assert_eq!(ci.get_property("numTriples"), Some("3"));
        assert_eq!(ci.get_property("order"), Some("1"));

        let _ = std::fs::remove_file(&output_path);
        Ok(())
    }

    #[test]
    fn test_index_components_roundtrip_and_boundaries() -> Result<()> {
        let output_path = std::env::temp_dir().join("test_index_components.hdt.index.v1-1");

        let bitmap_index_z = build_bitmap(&[false, true, false, true]);
        let index_z = build_logarray(&[0, 2, 1, 3], 3);
        let pred_bitmap = build_bitmap(&[true, false, true]);
        let pred_seq = build_logarray(&[0, 2, 1], 2);
        let pred_count = build_logarray(&[2, 1, 1], 2);

        let predicate_index = PredicateIndex {
            bitmap: pred_bitmap.clone(),
            sequence: pred_seq.clone(),
        };

        write_index(
            &output_path,
            4,
            1,
            &bitmap_index_z,
            &index_z,
            &predicate_index,
            &pred_count,
        )?;

        let bytes = std::fs::read(&output_path)?;
        let mut cursor = Cursor::new(bytes.clone());

        let _ci = crate::io::ControlInfo::read_from(&mut cursor)?;
        let c1 = BitmapReader::read_from(&mut cursor)?;
        let c2 = LogArrayReader::read_from(&mut cursor)?;
        let c3 = BitmapReader::read_from(&mut cursor)?;
        let c4 = LogArrayReader::read_from(&mut cursor)?;
        let c5 = LogArrayReader::read_from(&mut cursor)?;

        assert_eq!(cursor.position() as usize, bytes.len(), "all index components should consume file exactly");
        assert_eq!(c1.len(), 4);
        assert_eq!(c2.len(), 4);
        assert_eq!(c3.len(), 3);
        assert_eq!(c4.len(), 3);
        assert_eq!(c5.len(), 3);

        let _ = std::fs::remove_file(&output_path);
        Ok(())
    }
}
