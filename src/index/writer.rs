//! Index file writer - creates .hdt.index.v1-1 files.
//!
//! Writes the 5-section index file format used by hdt-java.

use crate::hdt::writer::{write_bitmap_from_file, write_log_array_from_file};
use crate::index::predicate_index::PredicateIndex;
use crate::io::ControlInfo;
use crate::triples::{StreamingBitmapResult, StreamingLogArrayResult};
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
/// Sections 1-2 are streamed from temp files to avoid holding them in memory.
/// Sections 3-5 are small (proportional to predicate count) and written from memory.
pub fn write_index(
    output_path: &Path,
    num_triples: u64,
    triples_order: u64,
    bitmap_index_z: &StreamingBitmapResult,
    index_z: &StreamingLogArrayResult,
    predicate_index: &PredicateIndex,
    predicate_count: &[u8],
) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("Failed to create index file {}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);

    // Write Control Information
    let mut ci = ControlInfo::new(
        crate::io::ControlType::Index,
        "<http://purl.org/HDT/hdt#indexFoQ>",
    );
    ci.set_property("numTriples", num_triples.to_string());
    ci.set_property("order", triples_order.to_string());
    ci.write_to(&mut writer)
        .context("Failed to write control info to index file")?;

    // Section 1: bitmapIndexZ — streamed from temp file
    write_bitmap_from_file(&mut writer, &bitmap_index_z.path, bitmap_index_z.num_bits)
        .context("Failed to write bitmapIndexZ section")?;

    // Section 2: indexZ — streamed from temp file
    write_log_array_from_file(
        &mut writer,
        &index_z.path,
        index_z.bits_per_entry,
        index_z.num_entries,
    )
    .context("Failed to write indexZ section")?;

    // Section 3: predicateIndex.bitmap (small, from memory)
    writer
        .write_all(&predicate_index.bitmap)
        .context("Failed to write predicateIndex bitmap section")?;

    // Section 4: predicateIndex.sequence (small, from memory)
    writer
        .write_all(&predicate_index.sequence)
        .context("Failed to write predicateIndex sequence section")?;

    // Section 5: predicateCount (small, from memory)
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
    use crate::io::{
        BitmapReader, BitmapWriter, ControlType, LogArrayReader, LogArrayWriter,
        StreamingBitmapEncoder, StreamingLogArrayEncoder,
    };
    use std::io::Cursor;
    use tempfile::TempDir;

    /// Write a bitmap to a temp file using the streaming encoder, returning a StreamingBitmapResult.
    fn build_streaming_bitmap(bits: &[bool], dir: &Path, name: &str) -> StreamingBitmapResult {
        let path = dir.join(name);
        let file = File::create(&path).unwrap();
        let mut encoder = StreamingBitmapEncoder::new(BufWriter::new(file));

        for &bit in bits {
            encoder.push(bit).unwrap();
        }

        let (num_bits, mut writer) = encoder.finish().unwrap();
        writer.flush().unwrap();

        StreamingBitmapResult { path, num_bits }
    }

    /// Write a log array to a temp file using the streaming encoder, returning a StreamingLogArrayResult.
    fn build_streaming_logarray(
        values: &[u64],
        max_value: u64,
        dir: &Path,
        name: &str,
    ) -> StreamingLogArrayResult {
        let bits_per_entry = if max_value == 0 {
            1
        } else {
            64 - max_value.leading_zeros() as u8
        };

        let path = dir.join(name);
        let file = File::create(&path).unwrap();
        let mut encoder = StreamingLogArrayEncoder::new(bits_per_entry, BufWriter::new(file));

        for &value in values {
            encoder.push(value).unwrap();
        }

        let (num_entries, bits_per_entry, mut writer) = encoder.finish().unwrap();
        writer.flush().unwrap();

        StreamingLogArrayResult {
            path,
            bits_per_entry,
            num_entries,
        }
    }

    fn build_inmem_bitmap(bits: &[bool]) -> Vec<u8> {
        let mut writer = BitmapWriter::new();
        for &bit in bits {
            writer.push(bit);
        }
        let mut buf = Vec::new();
        writer.write_to(&mut buf).expect("serialize bitmap");
        buf
    }

    fn build_inmem_logarray(values: &[u64], max_value: u64) -> Vec<u8> {
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
        let tmp = TempDir::new()?;
        let output_path = tmp.path().join("test_index.hdt.index.v1-1");

        let bitmap_index_z = build_streaming_bitmap(&[], tmp.path(), "biz.tmp");
        let index_z = build_streaming_logarray(&[], 0, tmp.path(), "iz.tmp");

        let predicate_index = PredicateIndex {
            bitmap: vec![],
            sequence: vec![],
        };

        write_index(
            &output_path,
            0,
            1,
            &bitmap_index_z,
            &index_z,
            &predicate_index,
            &[],
        )?;

        assert!(output_path.exists());
        Ok(())
    }

    #[test]
    fn test_index_control_info_type_and_properties() -> Result<()> {
        let tmp = TempDir::new()?;
        let output_path = tmp.path().join("test_index_ci.hdt.index.v1-1");

        let bitmap_index_z = build_streaming_bitmap(&[true, false, true], tmp.path(), "biz.tmp");
        let index_z = build_streaming_logarray(&[0, 1, 2], 2, tmp.path(), "iz.tmp");
        let pred_bitmap = build_inmem_bitmap(&[true, true]);
        let pred_seq = build_inmem_logarray(&[0, 1], 1);
        let pred_count = build_inmem_logarray(&[2, 1], 2);

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

        Ok(())
    }

    #[test]
    fn test_index_components_roundtrip_and_boundaries() -> Result<()> {
        let tmp = TempDir::new()?;
        let output_path = tmp.path().join("test_index_components.hdt.index.v1-1");

        let bitmap_index_z =
            build_streaming_bitmap(&[false, true, false, true], tmp.path(), "biz.tmp");
        let index_z = build_streaming_logarray(&[0, 2, 1, 3], 3, tmp.path(), "iz.tmp");
        let pred_bitmap = build_inmem_bitmap(&[true, false, true]);
        let pred_seq = build_inmem_logarray(&[0, 2, 1], 2);
        let pred_count = build_inmem_logarray(&[2, 1, 1], 2);

        let predicate_index = PredicateIndex {
            bitmap: pred_bitmap,
            sequence: pred_seq,
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

        assert_eq!(
            cursor.position() as usize,
            bytes.len(),
            "all index components should consume file exactly"
        );
        assert_eq!(c1.len(), 4);
        assert_eq!(c2.len(), 4);
        assert_eq!(c3.len(), 3);
        assert_eq!(c4.len(), 3);
        assert_eq!(c5.len(), 3);

        Ok(())
    }
}
