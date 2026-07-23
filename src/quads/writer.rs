//! Streaming writer for the packed HDT graphs sidecar, version 1.

use crate::io::ControlInfo;
use crate::io::crc_utils::{CRC32C_ALGO, crc32c};
use crate::quads::GraphMembership;
use crate::sort::Sortable;
use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const HEADER_SIZE: u64 = 256;
const FOOTER_SIZE: u64 = 64;
const DIRECTORY_ENTRY_SIZE: u64 = 96;
const CHUNK_ENTRY_SIZE: u64 = 48;
const CHUNK_SHIFT: u32 = 16;
const ARRAY_LIMIT: u32 = 4096;

const ENCODING_DENSE: u32 = 1;
const ENCODING_SPARSE: u32 = 2;
const ENCODING_ELIAS_FANO: u32 = 3;

#[derive(Debug, Clone, Copy)]
struct LayerEntry {
    primary_offset: u64,
    primary_length: u64,
    secondary_offset: u64,
    secondary_length: u64,
    item_count_a: u64,
    item_count_b: u64,
    member_count: u64,
    minimum_position: u64,
    maximum_position_exclusive: u64,
    encoding: u32,
    primary_crc: u32,
    secondary_crc: u32,
    parameter: u64,
}

impl LayerEntry {
    fn empty(universe: u64) -> Self {
        Self {
            primary_offset: 0,
            primary_length: 0,
            secondary_offset: 0,
            secondary_length: 0,
            item_count_a: 0,
            item_count_b: 0,
            member_count: 0,
            minimum_position: universe,
            maximum_position_exclusive: 0,
            encoding: ENCODING_SPARSE,
            primary_crc: 0,
            secondary_crc: 0,
            parameter: 0,
        }
    }

    fn bytes(self) -> [u8; DIRECTORY_ENTRY_SIZE as usize] {
        let mut out = [0u8; DIRECTORY_ENTRY_SIZE as usize];
        put_u64(&mut out, 0, self.primary_offset);
        put_u64(&mut out, 8, self.primary_length);
        put_u64(&mut out, 16, self.secondary_offset);
        put_u64(&mut out, 24, self.secondary_length);
        put_u64(&mut out, 32, self.item_count_a);
        put_u64(&mut out, 40, self.item_count_b);
        put_u64(&mut out, 48, self.member_count);
        put_u64(&mut out, 56, self.minimum_position);
        put_u64(&mut out, 64, self.maximum_position_exclusive);
        put_u32(&mut out, 72, self.encoding);
        put_u32(&mut out, 76, 0);
        put_u32(&mut out, 80, self.primary_crc);
        put_u32(&mut out, 84, self.secondary_crc);
        put_u64(&mut out, 88, self.parameter);
        out
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct LayerStats {
    member_count: u64,
    minimum: u64,
    maximum_exclusive: u64,
    non_empty_chunks: u64,
    payload_span: u64,
}

#[derive(Debug, Clone, Copy)]
struct ChunkMeta {
    key: u64,
    cardinality: u32,
    payload_relative_offset: u64,
    payload_length: u32,
    payload_crc: u32,
}

impl ChunkMeta {
    const SIZE: usize = 32;

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.key.to_le_bytes())?;
        writer.write_all(&self.cardinality.to_le_bytes())?;
        writer.write_all(&self.payload_length.to_le_bytes())?;
        writer.write_all(&self.payload_relative_offset.to_le_bytes())?;
        writer.write_all(&self.payload_crc.to_le_bytes())?;
        writer.write_all(&0u32.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut bytes = [0u8; Self::SIZE];
        match reader.read_exact(&mut bytes) {
            Ok(()) => Ok(Some(Self {
                key: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                cardinality: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                payload_length: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
                payload_relative_offset: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                payload_crc: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
            })),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ChosenEncoding {
    DenseChunks,
    SparseChunks { hash_capacity: u64 },
    EliasFano,
}

/// Return the automatically discovered sidecar path by appending `.graphs` to
/// the complete HDT path.
pub fn canonical_sidecar_path(hdt_path: &Path) -> PathBuf {
    let mut value = hdt_path.as_os_str().to_os_string();
    value.push(".graphs");
    PathBuf::from(value)
}

/// Assemble a version-1 graph sidecar from a graph-major zstd membership file.
#[allow(clippy::too_many_arguments)]
pub fn write_graph_sidecar(
    output_path: &Path,
    hdt_path: &Path,
    graph_dictionary_path: &Path,
    graph_dictionary_length: u64,
    named_graph_count: u64,
    triple_count: u64,
    expected_membership_count: u64,
    membership_path: &Path,
    has_blank_graph_names: bool,
    temp_dir: &Path,
) -> Result<()> {
    let started = std::time::Instant::now();
    tracing::info!(
        triple_count,
        named_graph_count,
        expected_membership_count,
        "Encoding graph sidecar"
    );
    let directory_length = named_graph_count
        .checked_add(1)
        .and_then(|count| count.checked_mul(DIRECTORY_ENTRY_SIZE))
        .and_then(|bytes| bytes.checked_add(4))
        .context("layer-directory size overflow")?;
    let dictionary_offset = HEADER_SIZE;
    let directory_offset = align_up(
        dictionary_offset
            .checked_add(graph_dictionary_length)
            .context("dictionary end overflow")?,
        64,
    )?;
    let layers_offset = align_up(
        directory_offset
            .checked_add(directory_length)
            .context("layer-directory end overflow")?,
        64,
    )?;

    let mut layers_file = tempfile::tempfile_in(temp_dir)?;
    let mut directory_file = tempfile::tempfile_in(temp_dir)?;
    let mut positions_file = tempfile::tempfile_in(temp_dir)?;

    let membership_file = File::open(membership_path)
        .with_context(|| format!("Failed to open {}", membership_path.display()))?;
    let decoder = zstd::Decoder::with_buffer(BufReader::new(membership_file))?;
    let mut memberships = MembershipStream::new(decoder)?;
    let mut observed_membership_count = 0u64;

    let mut directory_crc = CRC32C_ALGO.digest();
    for graph_id in 0..=named_graph_count {
        let layer_started = std::time::Instant::now();
        let stats = spool_layer(
            graph_id,
            triple_count,
            &mut memberships,
            &mut positions_file,
        )?;
        observed_membership_count = observed_membership_count
            .checked_add(stats.member_count)
            .context("membership count overflow")?;

        let entry = if stats.member_count == 0 {
            LayerEntry::empty(triple_count)
        } else {
            let encoding = choose_encoding(triple_count, stats)?;
            match encoding {
                ChosenEncoding::DenseChunks => encode_chunked_layer(
                    &mut layers_file,
                    layers_offset,
                    &mut positions_file,
                    triple_count,
                    stats,
                    true,
                    0,
                    temp_dir,
                )?,
                ChosenEncoding::SparseChunks { hash_capacity } => encode_chunked_layer(
                    &mut layers_file,
                    layers_offset,
                    &mut positions_file,
                    triple_count,
                    stats,
                    false,
                    hash_capacity,
                    temp_dir,
                )?,
                ChosenEncoding::EliasFano => encode_elias_fano_layer(
                    &mut layers_file,
                    layers_offset,
                    &mut positions_file,
                    triple_count,
                    stats,
                    temp_dir,
                )?,
            }
        };

        let bytes = entry.bytes();
        directory_file.write_all(&bytes)?;
        directory_crc.update(&bytes);
        let layer_elapsed = layer_started.elapsed();
        if stats.member_count > 0 && layer_elapsed.as_secs() >= 1 {
            tracing::info!(
                graph_id,
                memberships = stats.member_count,
                elapsed_seconds = layer_elapsed.as_secs_f64(),
                "Encoded graph layer"
            );
        } else {
            tracing::debug!(
                graph_id,
                memberships = stats.member_count,
                elapsed_seconds = layer_elapsed.as_secs_f64(),
                "Encoded graph layer"
            );
        }
    }

    ensure!(
        memberships.next.is_none(),
        "membership stream contains graph ID greater than named graph count"
    );
    ensure!(
        observed_membership_count == expected_membership_count,
        "membership count mismatch: stream has {observed_membership_count}, expected {expected_membership_count}"
    );
    ensure!(
        observed_membership_count >= triple_count,
        "non-exhaustive membership stream: {observed_membership_count} memberships for {triple_count} triples"
    );

    let directory_crc = directory_crc.finalize();
    directory_file.write_all(&directory_crc.to_le_bytes())?;
    let layers_length = layers_file.stream_position()?;
    let footer_offset = align_up(
        layers_offset
            .checked_add(layers_length)
            .context("layers end overflow")?,
        64,
    )?;
    let sidecar_size = footer_offset
        .checked_add(FOOTER_SIZE)
        .context("sidecar size overflow")?;

    let (source_data_length, source_digest) = hdt_position_identity(hdt_path)?;
    let mut flags = 1u64; // EXHAUSTIVE
    if observed_membership_count == triple_count {
        flags |= 1 << 1; // DISJOINT follows from exhaustive and M == N.
    }
    if has_blank_graph_names {
        flags |= 1 << 2;
    }

    let header = build_header(HeaderFields {
        flags,
        triple_count,
        named_graph_count,
        membership_count: observed_membership_count,
        source_data_length,
        sidecar_size,
        dictionary_offset,
        dictionary_length: graph_dictionary_length,
        directory_offset,
        directory_length,
        layers_offset: if layers_length == 0 {
            footer_offset
        } else {
            layers_offset
        },
        layers_length,
        footer_offset,
        source_digest,
    });
    let header_crc = u32::from_le_bytes(header[252..256].try_into().unwrap());
    let footer = build_footer(sidecar_size, directory_offset, directory_length, header_crc);

    let output = File::create(output_path)
        .with_context(|| format!("Failed to create {}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, output);
    writer.write_all(&header)?;
    copy_exact_file(graph_dictionary_path, graph_dictionary_length, &mut writer)?;
    pad_to_absolute(&mut writer, directory_offset)?;
    rewind(&mut directory_file)?;
    std::io::copy(&mut directory_file, &mut writer)?;
    pad_to_absolute(&mut writer, layers_offset)?;
    rewind(&mut layers_file)?;
    std::io::copy(&mut layers_file, &mut writer)?;
    pad_to_absolute(&mut writer, footer_offset)?;
    writer.write_all(&footer)?;
    writer.flush()?;

    let actual_size = std::fs::metadata(output_path)?.len();
    ensure!(
        actual_size == sidecar_size,
        "sidecar size mismatch after assembly"
    );
    tracing::info!(
        bytes = actual_size,
        elapsed_seconds = started.elapsed().as_secs_f64(),
        "Graph sidecar encoding complete"
    );
    Ok(())
}

struct MembershipStream<R: Read> {
    reader: R,
    next: Option<GraphMembership>,
}

impl<R: Read> MembershipStream<R> {
    fn new(mut reader: R) -> Result<Self> {
        let next = GraphMembership::read_from(&mut reader)?;
        Ok(Self { reader, next })
    }

    fn advance(&mut self) -> Result<()> {
        self.next = GraphMembership::read_from(&mut self.reader)?;
        Ok(())
    }
}

fn spool_layer<R: Read>(
    graph_id: u64,
    universe: u64,
    memberships: &mut MembershipStream<R>,
    positions: &mut File,
) -> Result<LayerStats> {
    // Reuse the allocated extent from the largest preceding layer. Readers
    // consume exactly `member_count` positions, so stale trailing bytes are
    // irrelevant. Truncating here can spend seconds freeing a multi-GB extent
    // before a tiny following layer.
    positions.seek(SeekFrom::Start(0))?;
    let mut positions_writer = BufWriter::with_capacity(1024 * 1024, &mut *positions);
    let mut stats = LayerStats {
        minimum: universe,
        ..LayerStats::default()
    };
    let mut previous = None;
    let mut current_chunk = None;
    let mut current_cardinality = 0u32;

    while let Some(item) = memberships.next {
        if item.graph < graph_id {
            bail!("membership stream is not graph-major");
        }
        if item.graph != graph_id {
            break;
        }
        ensure!(
            item.position < universe,
            "membership position outside HDT universe"
        );
        ensure!(
            previous.is_none_or(|value| item.position > value),
            "duplicate or unsorted graph membership"
        );

        positions_writer.write_all(&item.position.to_le_bytes())?;
        stats.minimum = stats.minimum.min(item.position);
        stats.maximum_exclusive = item
            .position
            .checked_add(1)
            .context("membership maximum overflow")?;
        stats.member_count = stats
            .member_count
            .checked_add(1)
            .context("layer count overflow")?;

        let chunk = item.position >> CHUNK_SHIFT;
        if current_chunk != Some(chunk) {
            if current_chunk.is_some() {
                finish_chunk_stats(&mut stats, current_cardinality)?;
            }
            current_chunk = Some(chunk);
            current_cardinality = 0;
            stats.non_empty_chunks = stats
                .non_empty_chunks
                .checked_add(1)
                .context("chunk count overflow")?;
        }
        current_cardinality += 1;
        previous = Some(item.position);
        memberships.advance()?;
    }

    if current_chunk.is_some() {
        finish_chunk_stats(&mut stats, current_cardinality)?;
    }
    positions_writer.flush()?;
    drop(positions_writer);
    positions.seek(SeekFrom::Start(0))?;
    Ok(stats)
}

fn finish_chunk_stats(stats: &mut LayerStats, cardinality: u32) -> Result<()> {
    stats.payload_span = align_up(stats.payload_span, 8)?;
    let bytes = if cardinality <= ARRAY_LIMIT {
        u64::from(cardinality) * 2
    } else {
        8_448
    };
    stats.payload_span = stats
        .payload_span
        .checked_add(bytes)
        .context("chunk payload size overflow")?;
    Ok(())
}

fn choose_encoding(universe: u64, stats: LayerStats) -> Result<ChosenEncoding> {
    let universe_chunks = universe_chunk_count(universe);
    let sparse_capacity = sparse_hash_capacity(stats.non_empty_chunks)?;
    let dense_index = universe_chunks
        .checked_mul(CHUNK_ENTRY_SIZE)
        .context("dense chunk directory overflow")?;
    let sparse_index = stats
        .non_empty_chunks
        .checked_mul(CHUNK_ENTRY_SIZE)
        .and_then(|value| value.checked_add(sparse_capacity.checked_mul(8)?))
        .context("sparse chunk index overflow")?;
    let (chunk_encoding, chunk_index) = if dense_index <= sparse_index {
        (ChosenEncoding::DenseChunks, dense_index)
    } else {
        (
            ChosenEncoding::SparseChunks {
                hash_capacity: sparse_capacity,
            },
            sparse_index,
        )
    };
    let chunk_size = chunk_index
        .checked_add(stats.payload_span)
        .context("chunked layer size overflow")?;

    if stats.member_count >= universe.div_ceil(64) {
        return Ok(chunk_encoding);
    }

    let ef_size = elias_fano_estimated_size(universe, stats.member_count)?;
    if ef_size < chunk_size {
        let tie_limit = ef_size
            .checked_add(ef_size.div_ceil(8))
            .context("encoding tie calculation overflow")?;
        if chunk_size <= tie_limit {
            Ok(chunk_encoding)
        } else {
            Ok(ChosenEncoding::EliasFano)
        }
    } else {
        Ok(chunk_encoding)
    }
}

fn elias_fano_estimated_size(universe: u64, members: u64) -> Result<u64> {
    let params = EliasFanoParams::new(universe, members)?;
    let mut size = 160u64;
    for length in [
        params.lower_length,
        params.superrank_length,
        params.subrank_length,
        params.upper_length,
    ] {
        if length != 0 {
            size = align_up(size, 8)?;
            size = size
                .checked_add(length)
                .context("Elias-Fano size overflow")?;
        }
    }
    Ok(size)
}

#[allow(clippy::too_many_arguments)]
fn encode_chunked_layer(
    layers: &mut File,
    layers_base: u64,
    positions: &mut File,
    universe: u64,
    stats: LayerStats,
    dense: bool,
    hash_capacity: u64,
    temp_dir: &Path,
) -> Result<LayerEntry> {
    let mut metadata = tempfile::tempfile_in(temp_dir)?;
    let mut payloads = tempfile::tempfile_in(temp_dir)?;
    build_chunk_payloads(positions, stats.member_count, &mut metadata, &mut payloads)?;

    let universe_chunks = universe_chunk_count(universe);
    let stored_chunks = if dense {
        universe_chunks
    } else {
        stats.non_empty_chunks
    };
    let primary_length = stored_chunks
        .checked_mul(CHUNK_ENTRY_SIZE)
        .context("chunk directory length overflow")?;

    align_layer_file(layers, layers_base, 64)?;
    let primary_offset = layer_absolute_position(layers, layers_base)?;
    let primary_end = primary_offset
        .checked_add(primary_length)
        .context("chunk directory end overflow")?;
    let secondary_offset = if dense { 0 } else { align_up(primary_end, 8)? };
    let secondary_length = if dense {
        0
    } else {
        hash_capacity
            .checked_mul(8)
            .context("sparse hash length overflow")?
    };
    let payload_base = align_up(
        if dense {
            primary_end
        } else {
            secondary_offset
                .checked_add(secondary_length)
                .context("sparse hash end overflow")?
        },
        8,
    )?;

    rewind(&mut metadata)?;
    let mut metadata_reader = BufReader::new(&mut metadata);
    let mut next_meta = ChunkMeta::read_from(&mut metadata_reader)?;
    let mut primary_digest = CRC32C_ALGO.digest();
    let mut rank_before = 0u64;

    for entry_index in 0..stored_chunks {
        let key = if dense {
            entry_index
        } else {
            next_meta.context("sparse chunk metadata ended early")?.key
        };
        let meta = if next_meta.is_some_and(|value| value.key == key) {
            let value = next_meta.take().unwrap();
            next_meta = ChunkMeta::read_from(&mut metadata_reader)?;
            Some(value)
        } else {
            None
        };

        let mut bytes = [0u8; CHUNK_ENTRY_SIZE as usize];
        put_u64(&mut bytes, 0, key);
        put_u64(&mut bytes, 8, rank_before);
        if let Some(meta) = meta {
            let payload_offset = payload_base
                .checked_add(meta.payload_relative_offset)
                .context("payload offset overflow")?;
            put_u64(&mut bytes, 16, payload_offset);
            put_u32(&mut bytes, 24, meta.payload_length);
            put_u32(&mut bytes, 28, meta.cardinality);
            bytes[32] = if meta.cardinality <= ARRAY_LIMIT {
                1
            } else {
                2
            };
            put_u32(&mut bytes, 36, meta.payload_crc);
            rank_before = rank_before
                .checked_add(u64::from(meta.cardinality))
                .context("chunk rank overflow")?;
        }
        layers.write_all(&bytes)?;
        primary_digest.update(&bytes);
    }
    ensure!(
        next_meta.is_none(),
        "unused chunk metadata after directory encoding"
    );
    ensure!(
        rank_before == stats.member_count,
        "chunk rank/count mismatch"
    );
    let primary_crc = primary_digest.finalize();

    let (secondary_crc, parameter) = if dense {
        (0, 0)
    } else {
        pad_layer_to_absolute(layers, layers_base, secondary_offset)?;
        let mut hash = tempfile::tempfile_in(temp_dir)?;
        build_sparse_hash(&mut hash, &mut metadata, hash_capacity)?;
        rewind(&mut hash)?;
        let crc = copy_with_crc(&mut hash, layers)?;
        (crc, hash_capacity)
    };

    pad_layer_to_absolute(layers, layers_base, payload_base)?;
    rewind(&mut payloads)?;
    std::io::copy(&mut payloads, layers)?;

    Ok(LayerEntry {
        primary_offset,
        primary_length,
        secondary_offset,
        secondary_length,
        item_count_a: stored_chunks,
        item_count_b: stats.non_empty_chunks,
        member_count: stats.member_count,
        minimum_position: stats.minimum,
        maximum_position_exclusive: stats.maximum_exclusive,
        encoding: if dense {
            ENCODING_DENSE
        } else {
            ENCODING_SPARSE
        },
        primary_crc,
        secondary_crc,
        parameter,
    })
}

fn build_chunk_payloads(
    positions: &mut File,
    member_count: u64,
    metadata: &mut File,
    payloads: &mut File,
) -> Result<()> {
    positions.seek(SeekFrom::Start(0))?;
    metadata.set_len(0)?;
    metadata.seek(SeekFrom::Start(0))?;
    payloads.set_len(0)?;
    payloads.seek(SeekFrom::Start(0))?;

    let mut reader = BufReader::new(positions);
    let mut remaining = member_count;
    let mut pending = if remaining > 0 {
        remaining -= 1;
        Some(read_u64(&mut reader)?)
    } else {
        None
    };

    while let Some(first) = pending.take() {
        let key = first >> CHUNK_SHIFT;
        let mut offsets = Vec::with_capacity(ARRAY_LIMIT as usize + 1);
        offsets.push((first & 0xffff) as u16);

        while remaining > 0 {
            let position = read_u64(&mut reader)?;
            remaining -= 1;
            if position >> CHUNK_SHIFT == key {
                offsets.push((position & 0xffff) as u16);
            } else {
                pending = Some(position);
                break;
            }
        }

        let relative_offset = align_up(payloads.stream_position()?, 8)?;
        pad_to_absolute(payloads, relative_offset)?;
        let payload = encode_chunk_payload(&offsets)?;
        let meta = ChunkMeta {
            key,
            cardinality: u32::try_from(offsets.len()).context("chunk cardinality overflow")?,
            payload_relative_offset: relative_offset,
            payload_length: u32::try_from(payload.len()).context("chunk payload too large")?,
            payload_crc: crc32c(&payload),
        };
        payloads.write_all(&payload)?;
        meta.write_to(metadata)?;
    }

    ensure!(remaining == 0, "position scratch ended inconsistently");
    metadata.flush()?;
    payloads.flush()?;
    Ok(())
}

fn encode_chunk_payload(offsets: &[u16]) -> Result<Vec<u8>> {
    ensure!(!offsets.is_empty(), "empty chunks have no payload");
    if offsets.len() <= ARRAY_LIMIT as usize {
        let mut payload = Vec::with_capacity(offsets.len() * 2);
        for &offset in offsets {
            payload.extend_from_slice(&offset.to_le_bytes());
        }
        return Ok(payload);
    }

    let mut payload = vec![0u8; 8_448];
    let mut index = 0usize;
    for subblock in 0..128usize {
        while index < offsets.len() && usize::from(offsets[index]) < subblock * 512 {
            index += 1;
        }
        let rank = u16::try_from(index).context("bitmap subrank overflow")?;
        payload[subblock * 2..subblock * 2 + 2].copy_from_slice(&rank.to_le_bytes());
    }
    for &offset in offsets {
        let offset = usize::from(offset);
        payload[256 + offset / 8] |= 1 << (offset % 8);
    }
    Ok(payload)
}

fn build_sparse_hash(hash: &mut File, metadata: &mut File, capacity: u64) -> Result<()> {
    ensure!(
        capacity >= 2 && capacity.is_power_of_two(),
        "invalid sparse hash capacity"
    );
    hash.set_len(capacity.checked_mul(8).context("hash size overflow")?)?;
    metadata.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(metadata);
    let mut entry_index = 0u64;

    while let Some(meta) = ChunkMeta::read_from(&mut reader)? {
        let mut slot = mix64(meta.key) & (capacity - 1);
        loop {
            hash.seek(SeekFrom::Start(slot * 8))?;
            let mut bytes = [0u8; 8];
            hash.read_exact(&mut bytes)?;
            if u64::from_le_bytes(bytes) == 0 {
                hash.seek(SeekFrom::Start(slot * 8))?;
                hash.write_all(&(entry_index + 1).to_le_bytes())?;
                break;
            }
            slot = (slot + 1) & (capacity - 1);
        }
        entry_index += 1;
    }
    hash.flush()?;
    Ok(())
}

fn mix64(chunk_key: u64) -> u64 {
    let mut value = chunk_key.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Debug, Clone, Copy)]
struct EliasFanoParams {
    low_bits: u32,
    high_buckets: u64,
    upper_bits: u64,
    lower_length: u64,
    superrank_count: u64,
    superrank_length: u64,
    subrank_count: u64,
    subrank_length: u64,
    upper_length: u64,
}

impl EliasFanoParams {
    fn new(universe: u64, members: u64) -> Result<Self> {
        ensure!(
            members > 0 && members <= universe,
            "invalid Elias-Fano universe/count"
        );
        let ratio = universe / members;
        let low_bits = if ratio <= 1 {
            0
        } else {
            63 - ratio.leading_zeros()
        };
        let high_buckets = 1 + ((universe - 1) >> low_bits);
        let upper_bits = high_buckets
            .checked_add(members)
            .context("Elias-Fano upper length overflow")?;
        let lower_length = checked_bits_to_bytes(members, u64::from(low_bits))?;
        let superrank_count = upper_bits
            .div_ceil(4096)
            .checked_add(1)
            .context("superrank count overflow")?;
        let superrank_length = superrank_count
            .checked_mul(8)
            .context("superrank length overflow")?;
        let subrank_count = upper_bits.div_ceil(512);
        let subrank_length = subrank_count
            .checked_mul(2)
            .context("subrank length overflow")?;
        let upper_length = upper_bits.div_ceil(8);
        Ok(Self {
            low_bits,
            high_buckets,
            upper_bits,
            lower_length,
            superrank_count,
            superrank_length,
            subrank_count,
            subrank_length,
            upper_length,
        })
    }
}

fn encode_elias_fano_layer(
    layers: &mut File,
    layers_base: u64,
    positions: &mut File,
    universe: u64,
    stats: LayerStats,
    temp_dir: &Path,
) -> Result<LayerEntry> {
    let params = EliasFanoParams::new(universe, stats.member_count)?;
    let mut lower = tempfile::tempfile_in(temp_dir)?;
    let mut superranks = tempfile::tempfile_in(temp_dir)?;
    let mut subranks = tempfile::tempfile_in(temp_dir)?;
    let mut upper = tempfile::tempfile_in(temp_dir)?;

    encode_lower_parts(positions, stats.member_count, params.low_bits, &mut lower)?;
    encode_upper_parts(
        positions,
        stats.member_count,
        params,
        &mut superranks,
        &mut subranks,
        &mut upper,
    )?;
    ensure!(
        lower.metadata()?.len() == params.lower_length,
        "lower length mismatch"
    );
    ensure!(
        superranks.metadata()?.len() == params.superrank_length,
        "superrank length mismatch"
    );
    ensure!(
        subranks.metadata()?.len() == params.subrank_length,
        "subrank length mismatch"
    );
    ensure!(
        upper.metadata()?.len() == params.upper_length,
        "upper length mismatch"
    );

    let lower_crc = file_crc(&mut lower)?;
    let superrank_crc = file_crc(&mut superranks)?;
    let subrank_crc = file_crc(&mut subranks)?;
    let upper_crc = file_crc(&mut upper)?;

    align_layer_file(layers, layers_base, 64)?;
    let primary_offset = layer_absolute_position(layers, layers_base)?;
    let mut cursor = primary_offset
        .checked_add(160)
        .context("EF header end overflow")?;
    let lower_offset = if params.lower_length == 0 {
        0
    } else {
        cursor = align_up(cursor, 8)?;
        let offset = cursor;
        cursor = cursor
            .checked_add(params.lower_length)
            .context("lower end overflow")?;
        offset
    };
    cursor = align_up(cursor, 8)?;
    let superrank_offset = cursor;
    cursor = cursor
        .checked_add(params.superrank_length)
        .context("superrank end overflow")?;
    cursor = align_up(cursor, 8)?;
    let subrank_offset = cursor;
    cursor = cursor
        .checked_add(params.subrank_length)
        .context("subrank end overflow")?;
    cursor = align_up(cursor, 8)?;
    let upper_offset = cursor;

    let mut header = [0u8; 160];
    header[0..8].copy_from_slice(b"$HDTEF01");
    put_u32(&mut header, 8, 160);
    put_u32(&mut header, 12, params.low_bits);
    put_u64(&mut header, 16, universe);
    put_u64(&mut header, 24, stats.member_count);
    put_u64(&mut header, 32, params.upper_bits);
    put_u64(&mut header, 40, params.high_buckets);
    put_u64(&mut header, 48, lower_offset);
    put_u64(&mut header, 56, params.lower_length);
    put_u64(&mut header, 64, superrank_offset);
    put_u64(&mut header, 72, params.superrank_length);
    put_u64(&mut header, 80, subrank_offset);
    put_u64(&mut header, 88, params.subrank_length);
    put_u64(&mut header, 96, upper_offset);
    put_u64(&mut header, 104, params.upper_length);
    put_u64(&mut header, 112, params.superrank_count);
    put_u64(&mut header, 120, params.subrank_count);
    put_u32(&mut header, 128, lower_crc);
    put_u32(&mut header, 132, superrank_crc);
    put_u32(&mut header, 136, subrank_crc);
    put_u32(&mut header, 140, upper_crc);
    let internal_crc = crc32c(&header[..156]);
    put_u32(&mut header, 156, internal_crc);
    let primary_crc = crc32c(&header);
    layers.write_all(&header)?;

    if params.lower_length != 0 {
        pad_layer_to_absolute(layers, layers_base, lower_offset)?;
        copy_file(&mut lower, layers)?;
    }
    pad_layer_to_absolute(layers, layers_base, superrank_offset)?;
    copy_file(&mut superranks, layers)?;
    pad_layer_to_absolute(layers, layers_base, subrank_offset)?;
    copy_file(&mut subranks, layers)?;
    pad_layer_to_absolute(layers, layers_base, upper_offset)?;
    copy_file(&mut upper, layers)?;

    Ok(LayerEntry {
        primary_offset,
        primary_length: 160,
        secondary_offset: 0,
        secondary_length: 0,
        item_count_a: 0,
        item_count_b: 0,
        member_count: stats.member_count,
        minimum_position: stats.minimum,
        maximum_position_exclusive: stats.maximum_exclusive,
        encoding: ENCODING_ELIAS_FANO,
        primary_crc,
        secondary_crc: 0,
        parameter: 0,
    })
}

fn encode_lower_parts(
    positions: &mut File,
    members: u64,
    low_bits: u32,
    output: &mut File,
) -> Result<()> {
    output.set_len(0)?;
    output.seek(SeekFrom::Start(0))?;
    if low_bits == 0 {
        return Ok(());
    }
    positions.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(positions);
    let mut writer = BufWriter::with_capacity(1024 * 1024, &mut *output);
    let mask = (1u64 << low_bits) - 1;
    let mut current = 0u64;
    let mut used = 0u32;
    for _ in 0..members {
        let value = read_u64(&mut reader)? & mask;
        current |= value << used;
        if used + low_bits >= 64 {
            writer.write_all(&current.to_le_bytes())?;
            let overflow = used + low_bits - 64;
            current = if overflow == 0 {
                0
            } else {
                value >> (low_bits - overflow)
            };
            used = overflow;
        } else {
            used += low_bits;
        }
    }
    if used > 0 {
        let bytes = used.div_ceil(8) as usize;
        writer.write_all(&current.to_le_bytes()[..bytes])?;
    }
    writer.flush()?;
    Ok(())
}

fn encode_upper_parts(
    positions: &mut File,
    members: u64,
    params: EliasFanoParams,
    superranks: &mut File,
    subranks: &mut File,
    upper: &mut File,
) -> Result<()> {
    for file in [&mut *superranks, &mut *subranks, &mut *upper] {
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
    }
    positions.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(positions);
    let mut superrank_writer = BufWriter::with_capacity(256 * 1024, &mut *superranks);
    let mut subrank_writer = BufWriter::with_capacity(256 * 1024, &mut *subranks);
    let mut upper_writer = BufWriter::with_capacity(1024 * 1024, &mut *upper);
    let mut member_index = 0u64;
    let mut next_one = if members > 0 {
        let value = read_u64(&mut reader)?;
        Some((value >> params.low_bits) + member_index)
    } else {
        None
    };
    let word_count = params.upper_bits.div_ceil(64);
    let mut rank = 0u64;
    let mut super_base = 0u64;

    for word_index in 0..word_count {
        if word_index.is_multiple_of(64) {
            superrank_writer.write_all(&rank.to_le_bytes())?;
            super_base = rank;
        }
        if word_index.is_multiple_of(8) {
            let relative = u16::try_from(rank - super_base).context("EF subrank overflow")?;
            subrank_writer.write_all(&relative.to_le_bytes())?;
        }

        let start = word_index * 64;
        let end = start + 64;
        let mut word = 0u64;
        while let Some(bit) = next_one {
            if bit >= end {
                break;
            }
            word |= 1u64 << (bit - start);
            member_index += 1;
            next_one = if member_index < members {
                let value = read_u64(&mut reader)?;
                Some((value >> params.low_bits) + member_index)
            } else {
                None
            };
        }
        rank += u64::from(word.count_ones());
        let remaining = params.upper_length - word_index * 8;
        let bytes = remaining.min(8) as usize;
        upper_writer.write_all(&word.to_le_bytes()[..bytes])?;
    }
    superrank_writer.write_all(&rank.to_le_bytes())?;
    ensure!(
        rank == members && next_one.is_none(),
        "EF upper cardinality mismatch"
    );
    superrank_writer.flush()?;
    subrank_writer.flush()?;
    upper_writer.flush()?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct HeaderFields {
    flags: u64,
    triple_count: u64,
    named_graph_count: u64,
    membership_count: u64,
    source_data_length: u64,
    sidecar_size: u64,
    dictionary_offset: u64,
    dictionary_length: u64,
    directory_offset: u64,
    directory_length: u64,
    layers_offset: u64,
    layers_length: u64,
    footer_offset: u64,
    source_digest: [u8; 32],
}

fn build_header(fields: HeaderFields) -> [u8; HEADER_SIZE as usize] {
    let mut header = [0u8; HEADER_SIZE as usize];
    header[0..8].copy_from_slice(b"$HDTGRPH");
    put_u16(&mut header, 8, 1);
    put_u16(&mut header, 10, 0);
    put_u32(&mut header, 12, HEADER_SIZE as u32);
    put_u64(&mut header, 16, fields.flags);
    put_u64(&mut header, 24, fields.triple_count);
    put_u64(&mut header, 32, fields.named_graph_count);
    put_u64(&mut header, 40, fields.membership_count);
    put_u64(&mut header, 48, fields.source_data_length);
    put_u64(&mut header, 56, fields.sidecar_size);
    put_u64(&mut header, 64, fields.dictionary_offset);
    put_u64(&mut header, 72, fields.dictionary_length);
    put_u64(&mut header, 80, fields.directory_offset);
    put_u64(&mut header, 88, fields.directory_length);
    put_u64(&mut header, 96, fields.layers_offset);
    put_u64(&mut header, 104, fields.layers_length);
    put_u64(&mut header, 112, fields.footer_offset);
    put_u32(&mut header, 120, 1);
    put_u32(&mut header, 124, CHUNK_SHIFT);
    header[128..160].copy_from_slice(&fields.source_digest);
    let checksum = crc32c(&header[..252]);
    put_u32(&mut header, 252, checksum);
    header
}

fn build_footer(
    file_size: u64,
    directory_offset: u64,
    directory_length: u64,
    header_crc: u32,
) -> [u8; FOOTER_SIZE as usize] {
    let mut footer = [0u8; FOOTER_SIZE as usize];
    footer[0..8].copy_from_slice(b"$HDTGEND");
    put_u16(&mut footer, 8, 1);
    put_u16(&mut footer, 10, 0);
    put_u32(&mut footer, 12, FOOTER_SIZE as u32);
    put_u64(&mut footer, 16, file_size);
    put_u64(&mut footer, 24, 0);
    put_u64(&mut footer, 32, directory_offset);
    put_u64(&mut footer, 40, directory_length);
    put_u64(&mut footer, 48, 0);
    put_u32(&mut footer, 56, header_crc);
    let checksum = crc32c(&footer[..60]);
    put_u32(&mut footer, 60, checksum);
    footer
}

fn hdt_position_identity(hdt_path: &Path) -> Result<(u64, [u8; 32])> {
    let file = File::open(hdt_path)
        .with_context(|| format!("Failed to open HDT file {}", hdt_path.display()))?;
    let file_length = file.metadata()?.len();
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    ControlInfo::read_from(&mut reader).context("Failed to read HDT global control info")?;
    let header =
        ControlInfo::read_from(&mut reader).context("Failed to read HDT header control info")?;
    let header_length: u64 = header
        .get_property("length")
        .context("HDT header is missing length")?
        .parse()
        .context("Invalid HDT header length")?;
    reader.seek(SeekFrom::Current(
        i64::try_from(header_length).context("HDT header is too large to seek")?,
    ))?;
    let data_offset = reader.stream_position()?;
    ensure!(
        data_offset <= file_length,
        "HDT dictionary offset exceeds file length"
    );

    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 256 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok((file_length - data_offset, hasher.finalize().into()))
}

fn universe_chunk_count(universe: u64) -> u64 {
    if universe == 0 {
        0
    } else {
        1 + ((universe - 1) >> CHUNK_SHIFT)
    }
}

fn sparse_hash_capacity(non_empty_chunks: u64) -> Result<u64> {
    ensure!(non_empty_chunks > 0, "non-empty layer requires a chunk");
    let minimum = non_empty_chunks
        .checked_mul(2)
        .context("sparse hash capacity overflow")?;
    minimum
        .checked_next_power_of_two()
        .context("sparse hash capacity overflow")
}

fn checked_bits_to_bytes(count: u64, bits: u64) -> Result<u64> {
    count
        .checked_mul(bits)
        .map(|value| value.div_ceil(8))
        .context("packed bit length overflow")
}

fn align_up(value: u64, alignment: u64) -> Result<u64> {
    ensure!(
        alignment.is_power_of_two(),
        "alignment must be a power of two"
    );
    value
        .checked_add(alignment - 1)
        .map(|rounded| rounded & !(alignment - 1))
        .context("alignment overflow")
}

fn layer_absolute_position(file: &mut File, base: u64) -> Result<u64> {
    base.checked_add(file.stream_position()?)
        .context("layer absolute offset overflow")
}

fn align_layer_file(file: &mut File, base: u64, alignment: u64) -> Result<()> {
    let absolute = layer_absolute_position(file, base)?;
    let target = align_up(absolute, alignment)?;
    pad_layer_to_absolute(file, base, target)
}

fn pad_layer_to_absolute(file: &mut File, base: u64, absolute: u64) -> Result<()> {
    ensure!(absolute >= base, "layer target precedes layer base");
    pad_to_absolute(file, absolute - base)
}

fn pad_to_absolute<W: Write + Seek>(writer: &mut W, target: u64) -> Result<()> {
    let position = writer.stream_position()?;
    ensure!(position <= target, "writer advanced past required offset");
    write_zeros(writer, target - position)
}

fn write_zeros<W: Write>(writer: &mut W, mut length: u64) -> Result<()> {
    const ZEROES: [u8; 8192] = [0; 8192];
    while length > 0 {
        let amount = usize::try_from(length.min(ZEROES.len() as u64)).unwrap();
        writer.write_all(&ZEROES[..amount])?;
        length -= amount as u64;
    }
    Ok(())
}

fn copy_exact_file<W: Write>(path: &Path, expected_length: u64, writer: &mut W) -> Result<()> {
    let file = File::open(path)?;
    ensure!(
        file.metadata()?.len() == expected_length,
        "temporary section length changed"
    );
    let copied = std::io::copy(&mut BufReader::new(file), writer)?;
    ensure!(copied == expected_length, "short copy of temporary section");
    Ok(())
}

fn copy_file(source: &mut File, destination: &mut File) -> Result<u64> {
    rewind(source)?;
    Ok(std::io::copy(source, destination)?)
}

fn copy_with_crc<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<u32> {
    let mut digest = CRC32C_ALGO.digest();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        writer.write_all(&buffer[..read])?;
    }
    Ok(digest.finalize())
}

fn file_crc(file: &mut File) -> Result<u32> {
    rewind(file)?;
    let mut digest = CRC32C_ALGO.digest();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize())
}

fn rewind(file: &mut File) -> Result<()> {
    file.seek(SeekFrom::Start(0))?;
    Ok(())
}

fn read_u64<R: Read>(reader: &mut R) -> Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn put_u16<const N: usize>(bytes: &mut [u8; N], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32<const N: usize>(bytes: &mut [u8; N], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64<const N: usize>(bytes: &mut [u8; N], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_path_appends_suffix() {
        assert_eq!(
            canonical_sidecar_path(Path::new("data.hdt")),
            PathBuf::from("data.hdt.graphs")
        );
    }

    #[test]
    fn array_and_bitmap_container_boundaries() -> Result<()> {
        let array = (0..4096u16).collect::<Vec<_>>();
        assert_eq!(encode_chunk_payload(&array)?.len(), 8192);
        let bitmap = (0..4097u16).collect::<Vec<_>>();
        assert_eq!(encode_chunk_payload(&bitmap)?.len(), 8448);
        Ok(())
    }

    #[test]
    fn sparse_hash_mix_is_stable() {
        assert_eq!(mix64(0), 0xe220_a839_7b1d_cdaf);
        assert_ne!(mix64(1), mix64(2));
    }

    #[test]
    fn elias_fano_parameter_edges() -> Result<()> {
        let dense = EliasFanoParams::new(4, 4)?;
        assert_eq!(dense.low_bits, 0);
        let sparse = EliasFanoParams::new(1_000_000, 2)?;
        assert!(sparse.low_bits > 0);
        Ok(())
    }

    #[test]
    fn dense_and_sparse_chunk_table_selection() -> Result<()> {
        let universe = 65_536u64 * 1024;
        let member_count = universe.div_ceil(64);
        let dense = choose_encoding(
            universe,
            LayerStats {
                member_count,
                minimum: 0,
                maximum_exclusive: universe,
                non_empty_chunks: 1024,
                payload_span: 1024 * 8448,
            },
        )?;
        assert!(matches!(dense, ChosenEncoding::DenseChunks));

        let sparse = choose_encoding(
            universe,
            LayerStats {
                member_count,
                minimum: 0,
                maximum_exclusive: member_count,
                non_empty_chunks: 16,
                payload_span: 16 * 8448,
            },
        )?;
        assert!(matches!(sparse, ChosenEncoding::SparseChunks { .. }));
        Ok(())
    }
}
