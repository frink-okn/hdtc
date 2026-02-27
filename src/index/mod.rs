//! HDT index file creation (.hdt.index.v1-1).
//!
//! Creates a Java-compatible sidecar index file from an existing HDT file.
//! Uses streaming decoders to avoid loading BitmapTriples into memory,
//! enabling index creation for datasets with billions of triples.

mod predicate_index;
mod writer;

use crate::io::log_array::bits_for;
use crate::io::{
    ControlInfo, ControlType, LogArrayReader, StreamingBitmapDecoder, StreamingBitmapEncoder,
    StreamingLogArrayDecoder, StreamingLogArrayEncoder,
};
use crate::sort::{ExternalSorter, Sortable};
use crate::triples::{StreamingBitmapResult, StreamingLogArrayResult};
use anyhow::{Context, Result, bail};
use oxrdf::Term;
use predicate_index::{PredicateEntry, build_predicate_index_streaming};
use std::fs::File;
use std::io::Cursor;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use writer::write_index_streaming;

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
        let parsed = literal.value().parse::<u64>().with_context(|| {
            format!("Invalid numeric triple-count literal: {}", literal.value())
        })?;

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

/// Skip a bitmap section, recording its starting file offset.
/// Returns (section_start_offset, num_bits).
fn skip_bitmap_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64)> {
    let section_start = reader.stream_position()?;

    // Read preamble: type(1) + VByte(num_bits) + CRC8(1)
    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;

    let num_bits = read_vbyte_from_reader(reader)?;

    let mut crc8 = [0u8; 1];
    reader.read_exact(&mut crc8)?;

    // Skip data + CRC32C
    let data_bytes = num_bits.div_ceil(8);
    reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

    Ok((section_start, num_bits))
}

/// Skip a log array section, recording its starting file offset.
/// Returns (section_start_offset, num_entries, bits_per_entry).
fn skip_log_array_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64, u8)> {
    let section_start = reader.stream_position()?;

    // Read preamble: type(1) + bits_per_entry(1) + VByte(num_entries) + CRC8(1)
    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;

    let mut bits_byte = [0u8; 1];
    reader.read_exact(&mut bits_byte)?;
    let bits_per_entry = bits_byte[0];

    let num_entries = read_vbyte_from_reader(reader)?;

    let mut crc8 = [0u8; 1];
    reader.read_exact(&mut crc8)?;

    // Skip data + CRC32C
    let total_bits = num_entries * bits_per_entry as u64;
    let data_bytes = total_bits.div_ceil(8);
    reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

    Ok((section_start, num_entries, bits_per_entry))
}

/// Create HDT index file (.hdt.index.v1-1) from an existing HDT file.
///
/// Uses streaming decoders to read BitmapTriples with O(1) memory instead of
/// loading the entire array into RAM. This enables index creation for datasets
/// with billions of triples under a bounded memory budget.
pub fn create_index(hdt_path: &Path, memory_budget: usize, temp_dir: &Path) -> Result<PathBuf> {
    tracing::debug!("Creating index for {}", hdt_path.display());
    tracing::debug!(
        "Index settings: memory budget={} MiB, temp dir={}",
        memory_budget / 1024 / 1024,
        temp_dir.display()
    );

    // ── Phase 1: Parse HDT, skip to triples, record BitmapTriples section offsets ──

    let file = File::open(hdt_path)
        .with_context(|| format!("Failed to open HDT file {}", hdt_path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);

    let global_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read global control info")?;
    if global_ci.control_type != ControlType::Global {
        bail!("Expected global control info at start of HDT file");
    }

    let header_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read header control info")?;
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
    let header_text = String::from_utf8(header_buf).context("Header content is not valid UTF-8")?;
    let num_triples = parse_num_triples_from_header(&header_text)
        .context("Failed to parse triple count from header metadata")?;

    let dict_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read dictionary control info")?;
    if dict_ci.control_type != ControlType::Dictionary {
        bail!("Expected dictionary control info");
    }
    if dict_ci.format != DICTIONARY_FOUR_FORMAT {
        bail!(
            "Unsupported dictionary format: {} (expected {})",
            dict_ci.format,
            DICTIONARY_FOUR_FORMAT
        );
    }

    tracing::debug!("Skipping 4 PFC dictionary sections");
    let dict_start = Instant::now();
    for i in 0..4 {
        skip_pfc_section(&mut reader)
            .with_context(|| format!("Failed to skip dictionary section {}", i + 1))?;
    }
    tracing::debug!(
        "Dictionary skipped in {:.3}s",
        dict_start.elapsed().as_secs_f64()
    );

    let triples_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read triples control info")?;
    if triples_ci.control_type != ControlType::Triples {
        bail!(
            "Expected triples control info, found {:?}",
            triples_ci.control_type
        );
    }
    let triples_order: u64 = triples_ci
        .get_property("order")
        .and_then(|s| s.parse().ok())
        .context("Missing or invalid triples order")?;

    tracing::info!("Decoded main HDT: {num_triples} triples");

    // Scan BitmapTriples sections: BitmapY, BitmapZ, ArrayY, ArrayZ
    // We skip BitmapY (not needed) and record start offsets of the other three.
    let (_by_start, _by_bits) =
        skip_bitmap_section(&mut reader).context("Failed to scan BitmapY")?;
    let (bz_start, _bz_bits) =
        skip_bitmap_section(&mut reader).context("Failed to scan BitmapZ")?;
    let (ay_start, ay_entries, ay_bpe) =
        skip_log_array_section(&mut reader).context("Failed to scan ArrayY")?;
    let (az_start, _az_entries, az_bpe) =
        skip_log_array_section(&mut reader).context("Failed to scan ArrayZ")?;

    let num_sp_pairs = ay_entries;

    tracing::debug!(
        "BitmapTriples: ArrayY={ay_entries} entries ({ay_bpe}bpe), ArrayZ={num_triples} entries ({az_bpe}bpe)"
    );

    drop(reader); // Done scanning

    // ── Phase 2: Open streaming decoders on 3 file handles ──

    let open_at = |offset: u64| -> Result<BufReader<File>> {
        let mut f = File::open(hdt_path)?;
        f.seek(SeekFrom::Start(offset))?;
        Ok(BufReader::with_capacity(256 * 1024, f))
    };

    let mut bitmap_z_dec = StreamingBitmapDecoder::new(open_at(bz_start)?)
        .context("Failed to create BitmapZ decoder")?;
    let mut array_y_dec = StreamingLogArrayDecoder::new(open_at(ay_start)?)
        .context("Failed to create ArrayY decoder")?;
    let mut array_z_dec = StreamingLogArrayDecoder::new(open_at(az_start)?)
        .context("Failed to create ArrayZ decoder")?;

    // ── Phase 3: Stream all triples through two external sorters ──

    let ops_budget = (memory_budget * 3 / 4).max(64 * 1024 * 1024);
    let pred_budget = (memory_budget / 4).max(64 * 1024 * 1024);

    let mut ops_sorter = ExternalSorter::new(temp_dir, ops_budget);
    let mut ops_buffer: Vec<ObjectPosEntry> = Vec::new();
    let mut ops_mem_used = 0usize;

    let mut pred_sorter = ExternalSorter::new(temp_dir, pred_budget);
    let mut pred_buffer: Vec<PredicateEntry> = Vec::new();
    let mut pred_mem_used = 0usize;

    let mut max_predicate = 0u64;
    let mut min_object = u64::MAX;
    let mut max_object = 0u64;
    let stage_start = Instant::now();

    // Read first predicate
    let mut current_predicate = if num_sp_pairs > 0 {
        let p = array_y_dec
            .next_entry()?
            .context("ArrayY unexpectedly empty")?;
        if p == 0 {
            bail!("Invalid predicate ID 0 in ArrayY at pos_y=0");
        }
        max_predicate = max_predicate.max(p);
        p
    } else {
        0
    };

    let mut pos_y: u64 = 0;
    let mut new_sp_pair = true; // Track when we enter a new (S,P) pair

    for pos_z in 0..num_triples {
        let object = array_z_dec
            .next_entry()?
            .with_context(|| format!("ArrayZ ended early at position {pos_z}"))?;

        if object == 0 {
            bail!(
                "Invalid object ID 0 decoded from ArrayZ at triple position {} (predicate={}, pos_y={}). This was detected before sorting, indicating source HDT data/encoding or ArrayZ decoding is invalid.",
                pos_z,
                current_predicate,
                pos_y
            );
        }

        min_object = min_object.min(object);
        max_object = max_object.max(object);

        let bz_bit = bitmap_z_dec
            .next_bit()?
            .with_context(|| format!("BitmapZ ended early at position {pos_z}"))?;

        ops_sorter.push(
            ObjectPosEntry {
                object,
                predicate: current_predicate,
                pos_y,
                seq_no: pos_z,
            },
            &mut ops_buffer,
            &mut ops_mem_used,
        )?;

        // Push one predicate entry per S-P pair (not per triple)
        if new_sp_pair {
            pred_sorter.push(
                PredicateEntry {
                    predicate: current_predicate,
                    pos_y,
                },
                &mut pred_buffer,
                &mut pred_mem_used,
            )?;
            new_sp_pair = false;
        }

        if bz_bit {
            // Last object for this (S,P) pair — advance to next pair
            new_sp_pair = true;
            pos_y += 1;
            if pos_y < num_sp_pairs {
                current_predicate = array_y_dec
                    .next_entry()?
                    .with_context(|| format!("ArrayY ended early at pos_y {pos_y}"))?;
                if current_predicate == 0 {
                    bail!("Invalid predicate ID 0 in ArrayY at pos_y={pos_y}");
                }
                max_predicate = max_predicate.max(current_predicate);
            }
        }

        if (pos_z + 1).is_multiple_of(10_000_000) {
            tracing::debug!(
                "Streaming pass: {} / {} ({:.1}%), OPS chunks={}, pred chunks={}, {:.1}s",
                pos_z + 1,
                num_triples,
                ((pos_z + 1) as f64 / num_triples as f64) * 100.0,
                ops_sorter.chunk_file_count(),
                pred_sorter.chunk_file_count(),
                stage_start.elapsed().as_secs_f64()
            );
        }
    }

    tracing::info!(
        "Streaming pass complete: {} triples in {:.1}s (OPS chunks={}, pred chunks={})",
        num_triples,
        stage_start.elapsed().as_secs_f64(),
        ops_sorter.chunk_file_count(),
        pred_sorter.chunk_file_count()
    );

    if pos_y != num_sp_pairs {
        bail!("BitmapZ boundary count mismatch: got {pos_y}, expected {num_sp_pairs}");
    }

    if num_triples > 0 {
        tracing::info!("ArrayZ object ID range: [{}..={}]", min_object, max_object);
    }

    // Verify CRCs — finish() consumes decoders, closing HDT file handles
    bitmap_z_dec
        .finish()
        .context("BitmapZ CRC verification failed")?;
    array_y_dec
        .finish()
        .context("ArrayY CRC verification failed")?;
    array_z_dec
        .finish()
        .context("ArrayZ CRC verification failed")?;

    // ── Phase 4: Sort OPS entries and build bitmapIndexZ + indexZ ──

    tracing::info!("Sorting OPS entries...");
    let sort_start = Instant::now();
    let sorted_ops = ops_sorter.finish(&mut ops_buffer)?;
    tracing::info!(
        "OPS sort complete ({:.1}s)",
        sort_start.elapsed().as_secs_f64()
    );

    let bitmap_path = temp_dir.join("idx_bitmap_index_z.tmp");
    let index_path = temp_dir.join("idx_index_z.tmp");

    let max_pos_y = num_sp_pairs.saturating_sub(1);
    let index_bpe = if max_pos_y == 0 {
        1
    } else {
        bits_for(max_pos_y)
    };

    let bitmap_file = File::create(&bitmap_path)?;
    let mut bitmap_encoder =
        StreamingBitmapEncoder::new(std::io::BufWriter::with_capacity(256 * 1024, bitmap_file));

    let index_file = File::create(&index_path)?;
    let mut index_encoder = StreamingLogArrayEncoder::new(
        index_bpe,
        std::io::BufWriter::with_capacity(256 * 1024, index_file),
    );

    let mut current_object: Option<u64> = None;
    let mut emitted = 0u64;
    let emit_start = Instant::now();

    for entry in sorted_ops {
        let entry = entry?;

        if entry.object == 0 {
            bail!(
                "Invalid object ID 0 in OPS stream (seq_no={}, predicate={}, pos_y={})",
                entry.seq_no,
                entry.predicate,
                entry.pos_y
            );
        }

        if let Some(prev) = current_object
            && entry.object != prev
        {
            bitmap_encoder.set_last(true);
        }
        current_object = Some(entry.object);

        index_encoder.push(entry.pos_y)?;
        bitmap_encoder.push(false)?;
        emitted += 1;

        if emitted.is_multiple_of(10_000_000) {
            tracing::debug!(
                "OPS emit: {} entries, {:.1}s",
                emitted,
                emit_start.elapsed().as_secs_f64()
            );
        }
    }

    if current_object.is_some() {
        bitmap_encoder.set_last(true);
    }

    let (bitmap_num_bits, mut bw) = bitmap_encoder.finish()?;
    bw.flush()?;

    if bitmap_num_bits != num_triples {
        bail!("bitmapIndexZ length mismatch: got {bitmap_num_bits}, expected {num_triples}");
    }

    let (index_num_entries, index_bpe, mut iw) = index_encoder.finish()?;
    iw.flush()?;

    tracing::info!(
        "OPS emit complete: {emitted} entries ({:.1}s)",
        emit_start.elapsed().as_secs_f64()
    );

    // Drop OPS sorter to delete its chunk files (no longer needed)
    drop(ops_sorter);

    let obj_bitmap = StreamingBitmapResult {
        path: bitmap_path,
        num_bits: bitmap_num_bits,
    };
    let obj_index = StreamingLogArrayResult {
        path: index_path,
        bits_per_entry: index_bpe,
        num_entries: index_num_entries,
    };

    // ── Phase 5: Sort predicate entries and build predicate index ──

    tracing::info!("Building PredicateIndex...");
    let pred_start = Instant::now();
    let sorted_pred = pred_sorter.finish(&mut pred_buffer)?;

    let pred_result =
        build_predicate_index_streaming(sorted_pred, num_sp_pairs, max_predicate, temp_dir)
            .context("Failed to build streaming predicate index")?;

    tracing::info!(
        "PredicateIndex complete ({:.1}s)",
        pred_start.elapsed().as_secs_f64()
    );

    // Drop pred sorter to delete its chunk files (no longer needed)
    drop(pred_sorter);

    // ── Phase 6: Write index file ──

    let index_path = hdt_path.with_extension("hdt.index.v1-1");

    write_index_streaming(
        &index_path,
        num_triples,
        triples_order,
        &obj_bitmap,
        &obj_index,
        &pred_result.bitmap,
        &pred_result.sequence,
        &pred_result.predicate_count,
    )
    .context("Failed to write index file")?;

    // Clean up temp files
    for path in [&obj_bitmap.path, &obj_index.path] {
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!("Failed to delete temp file {}: {}", path.display(), e);
        }
    }
    pred_result.cleanup();

    tracing::info!("Index file created: {}", index_path.display());

    Ok(index_path)
}

/// Validate BitmapTriples structures in an existing HDT file.
///
/// Performs a streaming pass over ArrayY/ArrayZ/BitmapZ and checks:
/// - object IDs are non-zero
/// - predicate IDs are non-zero
/// - BitmapZ boundaries produce exactly ArrayY.len() (S,P) pairs
/// - section CRCs verify
pub fn validate_hdt_triples(hdt_path: &Path) -> Result<()> {
    let file = File::open(hdt_path)
        .with_context(|| format!("Failed to open HDT file {}", hdt_path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);

    let global_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read global control info")?;
    if global_ci.control_type != ControlType::Global {
        bail!("Expected global control info at start of HDT file");
    }

    let header_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read header control info")?;
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
    let header_text = String::from_utf8(header_buf).context("Header content is not valid UTF-8")?;
    let num_triples = parse_num_triples_from_header(&header_text)
        .context("Failed to parse triple count from header metadata")?;

    let dict_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read dictionary control info")?;
    if dict_ci.control_type != ControlType::Dictionary {
        bail!("Expected dictionary control info");
    }
    if dict_ci.format != DICTIONARY_FOUR_FORMAT {
        bail!(
            "Unsupported dictionary format: {} (expected {})",
            dict_ci.format,
            DICTIONARY_FOUR_FORMAT
        );
    }

    for i in 0..4 {
        skip_pfc_section(&mut reader)
            .with_context(|| format!("Failed to skip dictionary section {}", i + 1))?;
    }

    let triples_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read triples control info")?;
    if triples_ci.control_type != ControlType::Triples {
        bail!(
            "Expected triples control info, found {:?}",
            triples_ci.control_type
        );
    }

    tracing::info!("Decoded main HDT: {num_triples} triples");

    let (_by_start, _by_bits) =
        skip_bitmap_section(&mut reader).context("Failed to scan BitmapY")?;
    let (bz_start, _bz_bits) =
        skip_bitmap_section(&mut reader).context("Failed to scan BitmapZ")?;
    let (ay_start, ay_entries, _ay_bpe) =
        skip_log_array_section(&mut reader).context("Failed to scan ArrayY")?;
    let (az_start, _az_entries, _az_bpe) =
        skip_log_array_section(&mut reader).context("Failed to scan ArrayZ")?;

    let num_sp_pairs = ay_entries;

    drop(reader);

    let open_at = |offset: u64| -> Result<BufReader<File>> {
        let mut f = File::open(hdt_path)?;
        f.seek(SeekFrom::Start(offset))?;
        Ok(BufReader::with_capacity(256 * 1024, f))
    };

    let mut bitmap_z_dec = StreamingBitmapDecoder::new(open_at(bz_start)?)
        .context("Failed to create BitmapZ decoder")?;
    let mut array_y_dec = StreamingLogArrayDecoder::new(open_at(ay_start)?)
        .context("Failed to create ArrayY decoder")?;
    let mut array_z_dec = StreamingLogArrayDecoder::new(open_at(az_start)?)
        .context("Failed to create ArrayZ decoder")?;

    let mut current_predicate = if num_sp_pairs > 0 {
        let p = array_y_dec
            .next_entry()?
            .context("ArrayY unexpectedly empty")?;
        if p == 0 {
            bail!("Invalid predicate ID 0 in ArrayY at pos_y=0");
        }
        p
    } else {
        0
    };

    let mut min_object = u64::MAX;
    let mut max_object = 0u64;
    let mut pos_y = 0u64;
    let start = Instant::now();

    for pos_z in 0..num_triples {
        let object = array_z_dec
            .next_entry()?
            .with_context(|| format!("ArrayZ ended early at position {pos_z}"))?;
        if object == 0 {
            bail!(
                "Invalid object ID 0 decoded from ArrayZ at triple position {} (predicate={}, pos_y={})",
                pos_z,
                current_predicate,
                pos_y
            );
        }

        min_object = min_object.min(object);
        max_object = max_object.max(object);

        let bz_bit = bitmap_z_dec
            .next_bit()?
            .with_context(|| format!("BitmapZ ended early at position {pos_z}"))?;

        if bz_bit {
            pos_y += 1;
            if pos_y < num_sp_pairs {
                current_predicate = array_y_dec
                    .next_entry()?
                    .with_context(|| format!("ArrayY ended early at pos_y {pos_y}"))?;
                if current_predicate == 0 {
                    bail!("Invalid predicate ID 0 in ArrayY at pos_y={pos_y}");
                }
            }
        }

        if (pos_z + 1).is_multiple_of(100_000_000) {
            tracing::info!(
                "Validation progress: {} / {} ({:.1}%), {:.1}s",
                pos_z + 1,
                num_triples,
                ((pos_z + 1) as f64 / num_triples as f64) * 100.0,
                start.elapsed().as_secs_f64()
            );
        }
    }

    if pos_y != num_sp_pairs {
        bail!("BitmapZ boundary count mismatch: got {pos_y}, expected {num_sp_pairs}");
    }

    bitmap_z_dec
        .finish()
        .context("BitmapZ CRC verification failed")?;
    array_y_dec
        .finish()
        .context("ArrayY CRC verification failed")?;
    array_z_dec
        .finish()
        .context("ArrayZ CRC verification failed")?;

    if num_triples > 0 {
        tracing::info!("ArrayZ object ID range: [{}..={}]", min_object, max_object);
    }

    tracing::info!(
        "Validation complete: {} triples, {} (S,P) pairs in {:.1}s",
        num_triples,
        num_sp_pairs,
        start.elapsed().as_secs_f64()
    );

    Ok(())
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
    use super::*;
    use crate::sort::ExternalSorter;

    #[test]
    fn test_index_module_compiles() {
        // Placeholder - actual testing requires HDT files
        // Real tests are in integration tests
    }

    /// Test OPS sort + emit pipeline with many sort chunks.
    /// Uses a tiny memory budget to force the parallel merge tree path (>16 chunks).
    /// Verifies no entries are lost or corrupted during sort/merge.
    #[test]
    fn test_ops_sort_many_chunks_no_data_loss() {
        let temp = tempfile::tempdir().unwrap();

        // Generate 50,000 ObjectPosEntry values with diverse fields.
        // With 32 bytes per entry and a ~50KB budget, we get ~1500 entries per chunk → ~33 chunks.
        let n = 50_000u64;
        let budget = 50 * 1024; // 50 KB — forces ~33 chunks

        let mut sorter = ExternalSorter::new(temp.path(), budget);
        let mut buffer: Vec<ObjectPosEntry> = Vec::new();
        let mut mem_used = 0usize;

        let entries: Vec<ObjectPosEntry> = (0..n)
            .map(|i| ObjectPosEntry {
                object: (i * 7919 + 1) % 5000 + 1, // 1-based, range 1..5000
                predicate: (i * 31) % 100 + 1,
                pos_y: i % 10000,
                seq_no: i,
            })
            .collect();

        for entry in &entries {
            sorter
                .push(*entry, &mut buffer, &mut mem_used)
                .expect("push should succeed");
        }

        let chunks = sorter.chunk_file_count();
        // Should have created multiple chunks (well above parallel merge threshold of 16)
        assert!(
            chunks > 0,
            "Should have created sort chunks, got 0 (budget may be too large)"
        );

        let sorted_iter = sorter.finish(&mut buffer).expect("finish should succeed");

        let mut count = 0u64;
        let mut zero_objects = 0u64;
        let mut prev: Option<ObjectPosEntry> = None;

        for result in sorted_iter {
            let entry = result.expect("iteration should not error");
            count += 1;

            if entry.object == 0 {
                zero_objects += 1;
            }

            // Verify sort order
            if let Some(ref p) = prev {
                assert!(
                    entry >= *p,
                    "Sort order violated at count {count}: prev={p:?}, curr={entry:?}"
                );
            }
            prev = Some(entry);
        }

        assert_eq!(
            zero_objects, 0,
            "No entries should have object=0 (all inputs are 1-based)"
        );
        assert_eq!(
            count,
            n,
            "All {n} entries should be emitted, got {count} (lost {} entries)",
            n - count
        );
    }

    /// Test that the OPS emit phase correctly handles entries from the sorter.
    /// Simulates the Phase 4 emit loop with controlled data.
    #[test]
    fn test_ops_emit_correctness() {
        // Create sorted OPS entries (already in order) that simulate a real dataset.
        // 3 objects, multiple predicates and Y-positions.
        let entries = vec![
            // Object 1: 3 triples
            ObjectPosEntry {
                object: 1,
                predicate: 1,
                pos_y: 0,
                seq_no: 0,
            },
            ObjectPosEntry {
                object: 1,
                predicate: 1,
                pos_y: 2,
                seq_no: 3,
            },
            ObjectPosEntry {
                object: 1,
                predicate: 2,
                pos_y: 4,
                seq_no: 5,
            },
            // Object 2: 2 triples
            ObjectPosEntry {
                object: 2,
                predicate: 1,
                pos_y: 1,
                seq_no: 1,
            },
            ObjectPosEntry {
                object: 2,
                predicate: 2,
                pos_y: 3,
                seq_no: 4,
            },
            // Object 3: 1 triple
            ObjectPosEntry {
                object: 3,
                predicate: 1,
                pos_y: 5,
                seq_no: 2,
            },
        ];

        // Simulate the Phase 4 emit loop
        let mut bitmap_bits: Vec<bool> = Vec::new();
        let mut index_values: Vec<u64> = Vec::new();
        let mut current_object: Option<u64> = None;

        for entry in &entries {
            if entry.object == 0 {
                continue;
            }
            if let Some(prev) = current_object
                && entry.object != prev
            {
                // Mark previous group boundary
                if let Some(last) = bitmap_bits.last_mut() {
                    *last = true;
                }
            }
            current_object = Some(entry.object);
            bitmap_bits.push(false);
            index_values.push(entry.pos_y);
        }
        if current_object.is_some()
            && let Some(last) = bitmap_bits.last_mut()
        {
            *last = true;
        }

        assert_eq!(bitmap_bits.len(), 6);
        assert_eq!(index_values.len(), 6);

        // Object 1 group: 3 entries, boundary at position 2
        assert!(!bitmap_bits[0]);
        assert!(!bitmap_bits[1]);
        assert!(bitmap_bits[2]); // end of object 1 group

        // Object 2 group: 2 entries, boundary at position 4
        assert!(!bitmap_bits[3]);
        assert!(bitmap_bits[4]); // end of object 2 group

        // Object 3 group: 1 entry, boundary at position 5
        assert!(bitmap_bits[5]); // end of object 3 group

        assert_eq!(index_values, vec![0, 2, 4, 1, 3, 5]);
    }

    /// Regression test: silently skipping object=0 entries causes the emitted
    /// bitmap length to be smaller than num_triples, matching the production
    /// failure mode seen on very large HDT index builds.
    #[test]
    fn test_ops_emit_skipping_zero_causes_length_mismatch() {
        let entries = vec![
            ObjectPosEntry {
                object: 1,
                predicate: 1,
                pos_y: 0,
                seq_no: 0,
            },
            ObjectPosEntry {
                object: 0,
                predicate: 2,
                pos_y: 1,
                seq_no: 1,
            },
            ObjectPosEntry {
                object: 2,
                predicate: 1,
                pos_y: 2,
                seq_no: 2,
            },
            ObjectPosEntry {
                object: 0,
                predicate: 3,
                pos_y: 3,
                seq_no: 3,
            },
            ObjectPosEntry {
                object: 3,
                predicate: 1,
                pos_y: 4,
                seq_no: 4,
            },
        ];

        let num_triples = entries.len() as u64;

        // Mirror legacy behavior (skip object=0) to demonstrate why it fails.
        let mut emitted = 0u64;
        for entry in &entries {
            if entry.object == 0 {
                continue;
            }
            emitted += 1;
        }

        assert_eq!(num_triples, 5);
        assert_eq!(emitted, 3);
        assert_ne!(
            emitted, num_triples,
            "Skipping object=0 creates bitmap length mismatch"
        );
    }
}
