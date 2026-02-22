//! HDT index file creation (.hdt.index.v1-1).
//!
//! Creates a Java-compatible sidecar index file from an existing HDT file.

mod predicate_index;
mod writer;

pub use predicate_index::{build_predicate_index, build_predicate_count};
pub use writer::write_index;

use crate::io::{
    BitmapReader, ControlInfo, ControlType, LogArrayReader,
    StreamingBitmapEncoder, StreamingLogArrayEncoder,
};
use crate::sort::{ExternalSorter, Sortable};
use crate::triples::{StreamingBitmapResult, StreamingLogArrayResult};
use anyhow::{bail, Context, Result};
use oxrdf::Term;
use std::fs::File;
use std::io::Cursor;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

const PFC_SECTION_TYPE: u8 = 0x02;
const DICTIONARY_FOUR_FORMAT: &str = "<http://purl.org/HDT/hdt#dictionaryFour>";

fn read_vbyte_from_reader<R: Read>(reader: &mut R) -> Result<u64> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut byte_buf = [0u8; 1];

    loop {
        reader.read_exact(&mut byte_buf)?;
        let byte = byte_buf[0];
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 != 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            bail!("Invalid VByte: value exceeds u64 range");
        }
    }
}

fn skip_pfc_section<R: Read + Seek>(reader: &mut R) -> Result<()> {
    let mut section_type = [0u8; 1];
    reader.read_exact(&mut section_type)?;
    if section_type[0] != PFC_SECTION_TYPE {
        bail!(
            "Invalid dictionary section type: expected 0x{PFC_SECTION_TYPE:02x}, got 0x{:02x}",
            section_type[0]
        );
    }

    let _string_count = read_vbyte_from_reader(reader)?;
    let buffer_length = read_vbyte_from_reader(reader)?;
    let _block_size = read_vbyte_from_reader(reader)?;

    let mut crc8 = [0u8; 1];
    reader.read_exact(&mut crc8)?;

    let _block_offsets = LogArrayReader::read_from(reader)?;

    reader
        .seek(SeekFrom::Current(buffer_length as i64))
        .context("Failed to skip PFC string buffer")?;

    let mut crc32 = [0u8; 4];
    reader.read_exact(&mut crc32)?;

    Ok(())
}

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
    memory_budget: usize,
    temp_dir: &Path,
) -> Result<PathBuf> {
    tracing::info!("Creating index for {}", hdt_path.display());
    tracing::info!(
        "Index settings: memory budget={} MiB, temp dir={}",
        memory_budget / 1024 / 1024,
        temp_dir.display()
    );

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

    // Validate dictionary metadata and skip the four PFC dictionary sections deterministically.
    let dict_ci = ControlInfo::read_from(&mut reader)
        .context("Failed to read dictionary control info")?;
    if dict_ci.control_type != ControlType::Dictionary {
        bail!("Expected dictionary control info");
    }
    if dict_ci.format != DICTIONARY_FOUR_FORMAT {
        bail!(
            "Unsupported dictionary format for fast index creation: {} (expected {})",
            dict_ci.format,
            DICTIONARY_FOUR_FORMAT
        );
    }
    let _dict_elements: u64 = dict_ci
        .get_property("elements")
        .context("Dictionary control info missing required 'elements' property")?
        .parse()
        .context("Dictionary control info has invalid non-numeric 'elements' property")?;

    tracing::info!("Skipping 4 PFC dictionary sections to reach triples section");
    let dict_skip_start = Instant::now();
    for section_idx in 0..4 {
        skip_pfc_section(&mut reader)
            .with_context(|| format!("Failed to skip dictionary section {}", section_idx + 1))?;
    }
    tracing::info!(
        "Dictionary sections skipped in {:.3}s",
        dict_skip_start.elapsed().as_secs_f64()
    );

    let triples_ci = ControlInfo::read_from(&mut reader)
        .context("Failed to read triples control info after dictionary")?;
    if triples_ci.control_type != ControlType::Triples {
        bail!(
            "Expected triples control info after dictionary, found {:?}",
            triples_ci.control_type
        );
    }

    let triples_order = triples_ci
        .get_property("order")
        .and_then(|s| s.parse().ok())
        .context("Missing or invalid triples order")?;

    tracing::info!("Decoded main HDT: {} triples", num_triples_from_header);

    // Read BitmapTriples structures (order: BitmapY, BitmapZ, ArrayY, ArrayZ)
    // BitmapY is not needed for index creation — drop it immediately
    drop(BitmapReader::read_from(&mut reader).context("Failed to read BitmapY")?);

    let bitmap_z = BitmapReader::read_from(&mut reader)
        .context("Failed to read BitmapZ")?;
    let array_y = LogArrayReader::read_from(&mut reader)
        .context("Failed to read ArrayY")?;
    let array_z = LogArrayReader::read_from(&mut reader)
        .context("Failed to read ArrayZ")?;

    let num_triples = num_triples_from_header;

    // Extract seqY from ArrayY, then drop the LogArrayReader to free its packed words
    tracing::info!("Extracting seqY from ArrayY ({} entries)", array_y.len());
    let seq_y_start = Instant::now();
    let mut seq_y = Vec::with_capacity(array_y.len() as usize);
    for i in 0..array_y.len() {
        seq_y.push(array_y.get(i));
        if (i + 1).is_multiple_of(5_000_000) {
            tracing::info!(
                "Extracted seqY entries: {} / {} ({:.1}%)",
                i + 1,
                array_y.len(),
                ((i + 1) as f64 / array_y.len() as f64) * 100.0
            );
        }
    }
    let array_y_freed = array_y.heap_size();
    drop(array_y);
    tracing::info!(
        "Extracted seqY in {:.3}s (freed ArrayY: {} MiB)",
        seq_y_start.elapsed().as_secs_f64(),
        array_y_freed / 1024 / 1024
    );

    // Compute reader memory that coexists with the sort buffer so we can subtract
    // it from the sort budget. bitmap_z, array_z, and seq_y are alive during sorting.
    let reader_memory = bitmap_z.heap_size() + array_z.heap_size()
        + seq_y.len() * std::mem::size_of::<u64>();
    tracing::info!(
        "Reader memory during sort: {} MiB (bitmap_z={} MiB, array_z={} MiB, seq_y={} MiB)",
        reader_memory / 1024 / 1024,
        bitmap_z.heap_size() / 1024 / 1024,
        array_z.heap_size() / 1024 / 1024,
        (seq_y.len() * std::mem::size_of::<u64>()) / 1024 / 1024
    );

    // Build bitmapIndexZ + indexZ exactly as hdt-java does for indexFoQ:
    // for each triple position i in SPO stream, compute (object=seqZ[i], posY=rank1(bitmapZ, i-1)),
    // group by object, sort each object-group by (predicate at posY, posY), then serialize posY values.
    let sort_budget = memory_budget.saturating_sub(reader_memory);
    let obj_index =
        build_object_index(&seq_y, &bitmap_z, &array_z, num_triples, sort_budget, temp_dir)
            .context("Failed to build bitmapIndexZ/indexZ")?;

    // Drop readers — they're no longer needed after the object index is built
    let freed = bitmap_z.heap_size() + array_z.heap_size();
    drop(bitmap_z);
    drop(array_z);
    tracing::debug!("Dropped bitmap_z + array_z (freed {} MiB)", freed / 1024 / 1024);

    // Build PredicateIndex
    tracing::info!("Building PredicateIndex structures...");

    let pred_index =
        build_predicate_index(&seq_y, obj_index.max_predicate)
            .context("Failed to build predicate index")?;
    let pred_count = build_predicate_count(&seq_y, obj_index.max_predicate)
        .context("Failed to build predicate count")?;

    // Write index file — streams bitmap_index_z and index_z from temp files
    let index_path = hdt_path.with_extension("hdt.index.v1-1");

    write_index(
        &index_path,
        num_triples,
        triples_order,
        &obj_index.bitmap_index_z,
        &obj_index.index_z,
        &pred_index,
        &pred_count,
    )
        .context("Failed to write index file")?;

    // Clean up temp files
    obj_index.cleanup();

    tracing::info!("Index creation complete: {}", index_path.display());

    Ok(index_path)
}

/// Result of building the object index with streaming encoders.
struct ObjectIndexResult {
    bitmap_index_z: StreamingBitmapResult,
    index_z: StreamingLogArrayResult,
    max_predicate: u64,
}

impl ObjectIndexResult {
    fn cleanup(&self) {
        for path in [&self.bitmap_index_z.path, &self.index_z.path] {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!("Failed to delete index temp file {}: {}", path.display(), e);
            }
        }
    }
}

fn build_object_index(
    seq_y: &[u64],
    bitmap_z: &BitmapReader,
    array_z: &LogArrayReader,
    num_triples: u64,
    memory_budget: usize,
    temp_dir: &Path,
) -> Result<ObjectIndexResult> {
    // Set up streaming encoders writing to temp files
    let bitmap_path = temp_dir.join("idx_bitmap_index_z.tmp");
    let index_path = temp_dir.join("idx_index_z.tmp");

    if num_triples == 0 {
        // Write empty structures to temp files
        let bitmap_file = File::create(&bitmap_path)
            .context("Failed to create bitmap_index_z temp file")?;
        let index_file = File::create(&index_path)
            .context("Failed to create index_z temp file")?;
        drop(bitmap_file);
        drop(index_file);

        return Ok(ObjectIndexResult {
            bitmap_index_z: StreamingBitmapResult {
                path: bitmap_path,
                num_bits: 0,
            },
            index_z: StreamingLogArrayResult {
                path: index_path,
                bits_per_entry: 1,
                num_entries: 0,
            },
            max_predicate: 0,
        });
    }

    tracing::info!(
        "Building object index with external-sort path: {} triples, memory budget={} MiB",
        num_triples,
        memory_budget / 1024 / 1024
    );

    let effective_memory_budget = memory_budget.max(64 * 1024 * 1024);
    let mut sorter = ExternalSorter::new(temp_dir, effective_memory_budget);
    let mut sort_buffer: Vec<ObjectPosEntry> = Vec::new();
    let mut sort_mem_used = 0usize;

    let mut max_predicate = 0u64;
    let stage_start = Instant::now();

    for i in 0..num_triples {
        let object = array_z.get(i);

        let pos_y = if i == 0 { 0 } else { bitmap_z.rank1(i - 1) };
        let predicate = seq_y
            .get(pos_y as usize)
            .copied()
            .context("posY out of bounds while building object index")?;
        max_predicate = max_predicate.max(predicate);

        sorter.push(
            ObjectPosEntry {
                object,
                predicate,
                pos_y,
                seq_no: i,
            },
            &mut sort_buffer,
            &mut sort_mem_used,
        )?;

        if (i + 1).is_multiple_of(5_000_000) {
            tracing::info!(
                "Object-index pass 1: {} / {} triples ({:.1}%), chunks={}, elapsed {:.1}s",
                i + 1,
                num_triples,
                ((i + 1) as f64 / num_triples as f64) * 100.0,
                sorter.chunk_file_count(),
                stage_start.elapsed().as_secs_f64()
            );
        }
    }

    tracing::info!(
        "Object-index pass 1 complete in {:.3}s (chunks so far: {})",
        stage_start.elapsed().as_secs_f64(),
        sorter.chunk_file_count()
    );

    tracing::info!("Object-index pass 2: sorting/merging entries");
    let sort_start = Instant::now();
    let sorted_entries = sorter.finish(&mut sort_buffer)?;
    tracing::info!(
        "Object-index pass 2 complete in {:.3}s (total chunks: {})",
        sort_start.elapsed().as_secs_f64(),
        sorter.chunk_file_count()
    );

    // Create streaming encoders backed by temp files
    let max_pos_y = (seq_y.len() as u64).saturating_sub(1);
    let bits_per_entry = if max_pos_y == 0 { 1 } else { 64 - max_pos_y.leading_zeros() as u8 };

    let bitmap_file = File::create(&bitmap_path)
        .context("Failed to create bitmap_index_z temp file")?;
    let mut bitmap_encoder =
        StreamingBitmapEncoder::new(BufWriter::with_capacity(256 * 1024, bitmap_file));

    let index_file = File::create(&index_path)
        .context("Failed to create index_z temp file")?;
    let mut index_encoder =
        StreamingLogArrayEncoder::new(bits_per_entry, BufWriter::with_capacity(256 * 1024, index_file));

    let mut current_object: Option<u64> = None;
    let mut emitted = 0u64;
    let emit_start = Instant::now();

    for entry in sorted_entries {
        let entry = entry?;

        if entry.object == 0 {
            continue;
        }

        if let Some(prev_object) = current_object
            && entry.object != prev_object
        {
            bitmap_encoder.set_last(true);
        }
        current_object = Some(entry.object);

        index_encoder.push(entry.pos_y)?;
        bitmap_encoder.push(false)?;
        emitted += 1;

        if emitted.is_multiple_of(5_000_000) {
            tracing::info!(
                "Object-index pass 3: serialized {} entries, elapsed {:.1}s",
                emitted,
                emit_start.elapsed().as_secs_f64()
            );
        }
    }

    if current_object.is_some() {
        bitmap_encoder.set_last(true);
    }

    // Finish streaming encoders
    let (bitmap_num_bits, mut bitmap_writer) = bitmap_encoder.finish()?;
    bitmap_writer.flush()?;

    if bitmap_num_bits != num_triples {
        bail!(
            "bitmapIndexZ length mismatch: got {}, expected {}",
            bitmap_num_bits,
            num_triples
        );
    }

    let (index_num_entries, index_bpe, mut index_writer) = index_encoder.finish()?;
    index_writer.flush()?;

    tracing::info!(
        "Object-index pass 3 complete in {:.3}s ({} entries streamed to temp files)",
        emit_start.elapsed().as_secs_f64(),
        emitted
    );

    Ok(ObjectIndexResult {
        bitmap_index_z: StreamingBitmapResult {
            path: bitmap_path,
            num_bits: bitmap_num_bits,
        },
        index_z: StreamingLogArrayResult {
            path: index_path,
            bits_per_entry: index_bpe,
            num_entries: index_num_entries,
        },
        max_predicate,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObjectPosEntry {
    object: u64,
    predicate: u64,
    pos_y: u64,
    seq_no: u64,
}

impl Ord for ObjectPosEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.object
            .cmp(&other.object)
            .then(self.predicate.cmp(&other.predicate))
            .then(self.pos_y.cmp(&other.pos_y))
            .then(self.seq_no.cmp(&other.seq_no))
    }
}

impl PartialOrd for ObjectPosEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Sortable for ObjectPosEntry {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.object.to_le_bytes())?;
        writer.write_all(&self.predicate.to_le_bytes())?;
        writer.write_all(&self.pos_y.to_le_bytes())?;
        writer.write_all(&self.seq_no.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; 32];
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        Ok(Some(Self {
            object: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            predicate: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            pos_y: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            seq_no: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        }))
    }

    fn mem_size(&self) -> usize {
        32
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_index_module_compiles() {
        // Placeholder - actual testing requires HDT files
        // Real tests are in integration tests
    }
}
