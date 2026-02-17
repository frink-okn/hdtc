//! Index file writer - creates .hdt.index.v1-1 files.
//!
//! Writes the 5-section index file format used by hdt-java.

use crate::io::ControlInfo;
use crate::triples::builder::BitmapTriplesData;
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
/// - `ops_triples`: OPS-ordered BitmapTriples structures
/// - `predicate_index`: PredicateIndex structures
/// - `predicate_count`: Serialized predicate count sequence
pub fn write_index(
    output_path: &Path,
    num_triples: u64,
    ops_triples: &BitmapTriplesData,
    predicate_index: &PredicateIndex,
    predicate_count: &[u8],
) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("Failed to create index file {}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);

    // Write Control Information
    // The index file uses indexFoQ format with order=1 for OPS (per hdt-java)
    let mut ci = ControlInfo::new(
        crate::io::ControlType::Triples,
        "<http://purl.org/HDT/hdt#indexFoQ>",
    );
    ci.set_property("numTriples", num_triples.to_string());
    ci.set_property("order", "1"); // OPS order
    ci.write_to(&mut writer)
        .context("Failed to write control info to index file")?;

    // Write five sections in order

    // Section 1: bitmapIndexZ (BitmapZ from OPS BitmapTriples)
    // This marks object boundaries in the permuted order
    writer
        .write_all(&ops_triples.bitmap_z)
        .context("Failed to write bitmapIndexZ section")?;

    // Section 2: indexZ (ArrayZ from OPS BitmapTriples)
    // This is the subject ID sequence in OPS order
    writer
        .write_all(&ops_triples.array_z)
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

    #[test]
    fn test_index_file_created() -> Result<()> {
        // Create a minimal test index in memory
        let output_path = std::env::temp_dir().join("test_index.hdt.index.v1-1");

        // Create empty BitmapTriplesData
        let triples = BitmapTriplesData {
            bitmap_y: vec![],
            bitmap_z: vec![],
            array_y: vec![],
            array_z: vec![],
            num_triples: 0,
            max_subject: 0,
            max_predicate: 0,
            max_object: 0,
        };

        let predicate_index = PredicateIndex {
            bitmap: vec![],
            sequence: vec![],
        };

        write_index(&output_path, 0, &triples, &predicate_index, &[])?;

        // Verify file was created
        assert!(output_path.exists());

        // Clean up
        let _ = std::fs::remove_file(&output_path);

        Ok(())
    }
}
