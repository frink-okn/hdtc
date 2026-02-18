//! HDT index file creation (.hdt.index.v1-1).
//!
//! Creates a Java-compatible sidecar index file from an existing HDT file.

mod predicate_index;
mod writer;

pub use predicate_index::{build_predicate_index, build_predicate_count};
pub use writer::write_index;

use crate::io::{BitmapReader, BitmapWriter, ControlInfo, ControlType, LogArrayReader, LogArrayWriter};
use anyhow::{bail, Context, Result};
use oxrdf::Term;
use std::fs::File;
use std::io::Cursor;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

fn parse_num_triples_from_header(header: &str) -> Result<u64> {
    const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";
    const HDT_TRIPLES_NUM: &str = "http://purl.org/HDT/hdt#triplesnumTriples";

    let mut value_from_void: Option<u64> = None;
    let mut value_from_hdt: Option<u64> = None;

    let parser = oxrdfio::RdfParser::from_format(oxrdfio::RdfFormat::NTriples)
        .for_reader(Cursor::new(header.as_bytes()));

    for quad_result in parser {
        let quad = quad_result.context("Invalid N-Triples in HDT header metadata")?;
        let predicate = quad.predicate.as_str();

        if predicate != VOID_TRIPLES && predicate != HDT_TRIPLES_NUM {
            continue;
        }

        let Term::Literal(literal) = quad.object else {
            continue;
        };
        let parsed = literal
            .value()
            .parse::<u64>()
            .with_context(|| format!("Invalid numeric triple-count literal: {}", literal.value()))?;

        if predicate == VOID_TRIPLES {
            value_from_void = Some(parsed);
        }
        if predicate == HDT_TRIPLES_NUM {
            value_from_hdt = Some(parsed);
        }
    }

    match (value_from_void, value_from_hdt) {
        (Some(v), Some(h)) if v != h => {
            bail!(
                "Header triple-count mismatch between void:triples ({v}) and hdt:triplesnumTriples ({h})"
            )
        }
        (Some(v), Some(_)) => Ok(v),
        (Some(v), None) => Ok(v),
        (None, Some(h)) => Ok(h),
        (None, None) => bail!("Header metadata missing triple-count predicate"),
    }
}

/// Create HDT index file (.hdt.index.v1-1) from an existing HDT file.
///
/// The index file contains object index and predicate index structures
/// compatible with hdt-java's `indexFoQ` format.
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
    _memory_budget: usize,
    _temp_dir: &Path,
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

    let mut header_buf = vec![0u8; header_len];
    reader
        .read_exact(&mut header_buf)
        .context("Failed to read header section")?;
    let header_text = String::from_utf8(header_buf)
        .context("Header content is not valid UTF-8")?;
    let num_triples_from_header = parse_num_triples_from_header(&header_text)
        .context("Failed to parse triple count from header metadata")?;

    // Skip dictionary section (without knowing its exact length)
    let dict_ci = ControlInfo::read_from(&mut reader)
        .context("Failed to read dictionary control info")?;
    if dict_ci.control_type != ControlType::Dictionary {
        bail!("Expected dictionary control info");
    }

    // The dictionary contains PFC-encoded sub-sections without an explicit total length.
    // Scan forward for a *valid* triples ControlInfo block instead of trusting raw "$HDT" bytes,
    // which may appear in dictionary payloads.
    let triples_order = loop {
        let candidate_start = reader
            .stream_position()
            .context("Failed to get stream position while scanning for triples section")?;

        let mut marker = [0u8; 5];
        if reader.read_exact(&mut marker).is_err() {
            bail!("Failed to locate triples control info marker");
        }

        if &marker == b"$HDT\x04" {
            reader
                .seek(SeekFrom::Start(candidate_start))
                .context("Failed to rewind to triples control info candidate")?;

            match ControlInfo::read_from(&mut reader) {
                Ok(ci) if ci.control_type == ControlType::Triples => {
                    let order = ci
                        .get_property("order")
                        .and_then(|s| s.parse().ok())
                        .context("Missing or invalid triples order")?;

                    tracing::info!("Decoded main HDT: {} triples", num_triples_from_header);
                    break order;
                }
                _ => {
                    reader
                        .seek(SeekFrom::Start(candidate_start + 1))
                        .context("Failed to advance after invalid triples control info candidate")?;
                }
            }
        } else {
            reader
                .seek(SeekFrom::Start(candidate_start + 1))
                .context("Failed to advance scan position")?;
        }
    };

    // Read BitmapTriples structures (order: Y, Z, Y, Z)
    let _bitmap_y = BitmapReader::read_from(&mut reader)
        .context("Failed to read BitmapY")?;
    let bitmap_z = BitmapReader::read_from(&mut reader)
        .context("Failed to read BitmapZ")?;
    let array_y = LogArrayReader::read_from(&mut reader)
        .context("Failed to read ArrayY")?;
    let array_z = LogArrayReader::read_from(&mut reader)
        .context("Failed to read ArrayZ")?;

    let num_triples = num_triples_from_header;

    // Extract seqY for later PredicateIndex building
    let mut seq_y = Vec::new();
    for i in 0..array_y.len() {
        seq_y.push(array_y.get(i));
    }

    // Build bitmapIndexZ + indexZ exactly as hdt-java does for indexFoQ:
    // for each triple position i in SPO stream, compute (object=seqZ[i], posY=rank1(bitmapZ, i-1)),
    // group by object, sort each object-group by (predicate at posY, posY), then serialize posY values.
    let (bitmap_index_z, index_z, max_predicate) =
        build_object_index(&seq_y, &bitmap_z, &array_z, num_triples)
            .context("Failed to build bitmapIndexZ/indexZ")?;

    // Build PredicateIndex
    tracing::info!("Building PredicateIndex structures...");

    let pred_index =
        build_predicate_index(&seq_y, max_predicate).context("Failed to build predicate index")?;
    let pred_count = build_predicate_count(&seq_y, max_predicate)
        .context("Failed to build predicate count")?;

    // Write index file
    let index_path = hdt_path.with_extension("hdt.index.v1-1");

    write_index(
        &index_path,
        num_triples,
        triples_order,
        &bitmap_index_z,
        &index_z,
        &pred_index,
        &pred_count,
    )
        .context("Failed to write index file")?;

    tracing::info!("Index creation complete: {}", index_path.display());

    Ok(index_path)
}

fn build_object_index(
    seq_y: &[u64],
    bitmap_z: &BitmapReader,
    array_z: &LogArrayReader,
    num_triples: u64,
) -> Result<(Vec<u8>, Vec<u8>, u64)> {
    if num_triples == 0 {
        let mut bitmap_buf = Vec::new();
        BitmapWriter::new().write_to(&mut bitmap_buf)?;

        let mut index_buf = Vec::new();
        LogArrayWriter::for_max_value(0).write_to(&mut index_buf)?;

        return Ok((bitmap_buf, index_buf, 0));
    }

    let mut max_object = 0u64;
    let mut max_predicate = 0u64;
    let mut object_lists: Vec<Vec<(u64, u64)>> = vec![Vec::new()];

    for i in 0..num_triples {
        let object = array_z.get(i);
        max_object = max_object.max(object);

        let object_idx = object as usize;
        if object_idx >= object_lists.len() {
            object_lists.resize_with(object_idx + 1, Vec::new);
        }

        let pos_y = if i == 0 { 0 } else { bitmap_z.rank1(i - 1) };
        let predicate = seq_y
            .get(pos_y as usize)
            .copied()
            .context("posY out of bounds while building object index")?;
        max_predicate = max_predicate.max(predicate);

        object_lists[object_idx].push((predicate, pos_y));
    }

    let max_pos_y = (seq_y.len() as u64).saturating_sub(1);
    let mut index_z_writer = LogArrayWriter::for_max_value(max_pos_y);
    let mut bitmap_index_z_writer = BitmapWriter::new();

    for entries in object_lists
        .iter_mut()
        .take(max_object as usize + 1)
        .skip(1)
    {
        if entries.is_empty() {
            continue;
        }

        entries.sort_unstable_by(|(pred_a, pos_a), (pred_b, pos_b)| {
            pred_a.cmp(pred_b).then(pos_a.cmp(pos_b))
        });

        for &(_, pos_y) in entries.iter() {
            index_z_writer.push(pos_y);
            bitmap_index_z_writer.push(false);
        }
        bitmap_index_z_writer.set_last(true);
    }

    if bitmap_index_z_writer.len() != num_triples {
        bail!(
            "bitmapIndexZ length mismatch: got {}, expected {}",
            bitmap_index_z_writer.len(),
            num_triples
        );
    }

    let mut bitmap_buf = Vec::new();
    bitmap_index_z_writer.write_to(&mut bitmap_buf)?;

    let mut index_buf = Vec::new();
    index_z_writer.write_to(&mut index_buf)?;

    Ok((bitmap_buf, index_buf, max_predicate))
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_index_module_compiles() {
        // Placeholder - actual testing requires HDT files
        // Real tests are in integration tests
    }
}
