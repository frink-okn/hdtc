//! HDT index file creation (.hdt.index.v1-1).
//!
//! Creates an OPS-ordered sidecar index file from an existing HDT file.

mod decoder;
mod ops_triple;
mod predicate_index;
mod writer;

pub use decoder::BitmapTriplesDecoder;
pub use ops_triple::OpsTriple;
pub use predicate_index::{build_predicate_index, build_predicate_count};
pub use writer::write_index;

use crate::io::{BitmapReader, LogArrayReader, ControlInfo, ControlType};
use crate::sort::ExternalSorter;
use crate::triples::builder::build_bitmap_triples;
use crate::triples::id_triple::IdTriple;
use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Helper function to read null-terminated string from a reader
fn read_null_terminated_string<R: Read>(reader: &mut R) -> Result<String> {
    let mut result = Vec::new();
    let mut buf = [0u8; 1];
    loop {
        reader.read_exact(&mut buf)?;
        if buf[0] == 0 {
            break;
        }
        result.push(buf[0]);
    }
    Ok(String::from_utf8(result)?)
}

/// Create HDT index file (.hdt.index.v1-1) from an existing HDT file.
///
/// The index file contains triples in OPS (Object-Predicate-Subject) order
/// plus auxiliary inverted index structures for fast predicate-based queries.
///
/// # Arguments
/// - `hdt_path`: Path to the main HDT file
/// - `memory_budget`: Memory budget in bytes for sorting operations
/// - `temp_dir`: Directory for temporary sort files
///
/// # Returns
/// Path to the created index file (same as HDT with .hdt.index.v1-1 suffix)
pub fn create_index(
    hdt_path: &Path,
    memory_budget: usize,
    temp_dir: &Path,
) -> Result<PathBuf> {
    tracing::info!("Creating index for {}", hdt_path.display());

    // Read HDT file and locate triples section
    let file = File::open(hdt_path)
        .with_context(|| format!("Failed to open HDT file {}", hdt_path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);

    // Skip global control info
    let global_ci = ControlInfo::read_from(&mut reader)
        .context("Failed to read global control info")?;
    if global_ci.control_type != ControlType::Global {
        bail!("Expected global control info at start of HDT file");
    }

    // Skip header section
    let header_ci = ControlInfo::read_from(&mut reader)
        .context("Failed to read header control info")?;
    if header_ci.control_type != ControlType::Header {
        bail!("Expected header control info");
    }
    let header_len: usize = header_ci
        .get_property("length")
        .and_then(|s| s.parse().ok())
        .context("Missing or invalid header length in control info")?;
    reader
        .seek(SeekFrom::Current(header_len as i64))
        .context("Failed to skip header section")?;

    // Skip dictionary section (without knowing its exact length)
    let dict_ci = ControlInfo::read_from(&mut reader)
        .context("Failed to read dictionary control info")?;
    if dict_ci.control_type != ControlType::Dictionary {
        bail!("Expected dictionary control info");
    }

    // The dictionary contains four PFC-encoded sub-sections without explicit length headers.
    // Scan forward to find the triples section marker ($HDT magic bytes).
    let mut buf = [0u8; 4];
    let mut byte = [0u8; 1];
    let mut num_triples = 0u64;

    loop {
        reader
            .read_exact(&mut byte)
            .context("Failed to read while scanning for triples section")?;

        if byte[0] == b'$' {
            buf[0] = byte[0];
            reader
                .read_exact(&mut buf[1..])
                .context("Failed to read potential magic bytes")?;

            if &buf == b"$HDT" {
                reader
                    .read_exact(&mut byte)
                    .context("Failed to read section type")?;

                if byte[0] == 4 {
                    // Found the triples section (type=4)
                    let _format = read_null_terminated_string(&mut reader)
                        .context("Failed to read triples format")?;
                    let props_str = read_null_terminated_string(&mut reader)
                        .context("Failed to read triples properties")?;

                    // Parse properties
                    let mut properties = std::collections::BTreeMap::new();
                    if !props_str.is_empty() {
                        for prop in props_str.split(';') {
                            if let Some((key, value)) = prop.split_once('=') {
                                properties.insert(key.to_string(), value.to_string());
                            }
                        }
                    }

                    // Skip CRC16
                    reader
                        .read_exact(&mut [0u8; 2])
                        .context("Failed to skip CRC16")?;

                    num_triples = properties
                        .get("numTriples")
                        .and_then(|s| s.parse().ok())
                        .context("Missing or invalid numTriples")?;

                    tracing::info!("Decoded main HDT: {} triples", num_triples);
                    break;
                }
            }
        }
    }

    // Read BitmapTriples structures (order: Y, Z, Y, Z)
    let bitmap_y = BitmapReader::read_from(&mut reader)
        .context("Failed to read BitmapY")?;
    let bitmap_z = BitmapReader::read_from(&mut reader)
        .context("Failed to read BitmapZ")?;
    let array_y = LogArrayReader::read_from(&mut reader)
        .context("Failed to read ArrayY")?;
    let array_z = LogArrayReader::read_from(&mut reader)
        .context("Failed to read ArrayZ")?;

    // Extract seqY for later PredicateIndex building
    let mut seq_y = Vec::new();
    for i in 0..array_y.len() {
        seq_y.push(array_y.get(i));
    }

    // Create decoder and convert to OPS order
    let decoder = BitmapTriplesDecoder::new(bitmap_y, array_y, bitmap_z, array_z, num_triples);

    tracing::info!("Sorting triples in OPS order...");

    // Convert to OPS and sort
    let mut sorter = ExternalSorter::new(temp_dir, memory_budget);

    // Track max IDs while converting
    let mut max_object = 0u64;
    let mut max_predicate = 0u64;
    let mut max_subject = 0u64;
    let mut sort_buffer: Vec<OpsTriple> = Vec::new();
    let mut mem_used: usize = 0;

    for result in decoder {
        let triple = result.context("Failed to decode triple")?;
        max_object = max_object.max(triple.object);
        max_predicate = max_predicate.max(triple.predicate);
        max_subject = max_subject.max(triple.subject);

        let ops = OpsTriple::from(triple);
        sorter.push(ops, &mut sort_buffer, &mut mem_used)?;
    }

    // Finish sorting to get sorted iterator
    let sorted_ops = sorter.finish(&mut sort_buffer)?;

    // Collect sorted OPS triples for processing
    let mut sorted_triples = Vec::new();
    for result in sorted_ops {
        let ops = result.context("Failed to read sorted OPS triple")?;
        sorted_triples.push(ops);
    }

    tracing::info!(
        "Building OPS BitmapTriples structures (max S={}, max P={}, max O={})",
        max_subject,
        max_predicate,
        max_object
    );

    // Build OPS BitmapTriples
    let ops_iter = sorted_triples.iter().map(|ops| Ok(IdTriple {
        subject: ops.subject,
        predicate: ops.predicate,
        object: ops.object,
    }));

    let ops_bitmap = build_bitmap_triples(
        ops_iter,
        max_object,    // X in OPS
        max_predicate, // Y in OPS
        max_subject,   // Z in OPS
    )
    .context("Failed to build OPS BitmapTriples")?;

    // Build PredicateIndex
    tracing::info!("Building PredicateIndex structures...");

    let pred_index =
        build_predicate_index(&seq_y, max_predicate).context("Failed to build predicate index")?;
    let pred_count = build_predicate_count(&seq_y, max_predicate)
        .context("Failed to build predicate count")?;

    // Write index file
    let index_path = hdt_path.with_extension("hdt.index.v1-1");

    write_index(&index_path, num_triples, &ops_bitmap, &pred_index, &pred_count)
        .context("Failed to write index file")?;

    tracing::info!("Index creation complete: {}", index_path.display());

    Ok(index_path)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_index_module_compiles() {
        // Placeholder - actual testing requires HDT files
        // Real tests are in integration tests
    }
}
