//! File-backed reader for the packed HDT graphs sidecar, version 1.

// hdtc is currently a binary crate, so rustc cannot see callers of the full
// reader API even though these operations are part of the sidecar contract.
#![allow(dead_code)]

use crate::hdt::reader::sha256_to_end;
use crate::io::crc_utils::{CRC32C_ALGO, crc8, crc32c};
use crate::io::{ControlInfo, decode_vbyte, encode_vbyte, read_vbyte};
use crate::quads::writer::canonical_sidecar_path;
use crate::sort::{ExternalSorter, Sortable};
use anyhow::{Context, Result, bail, ensure};
use oxrdf::NamedNode;
use oxrdfio::{RdfFormat, RdfParser};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const HEADER_SIZE: u64 = 256;
const FOOTER_SIZE: u64 = 64;
const DIRECTORY_ENTRY_SIZE: u64 = 96;
const CHUNK_ENTRY_SIZE: u64 = 48;
const CHUNK_SHIFT: u32 = 16;
const ENCODING_DENSE: u32 = 1;
const ENCODING_SPARSE: u32 = 2;
const ENCODING_ELIAS_FANO: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphTerm {
    DefaultGraph,
    Named(String),
}

#[derive(Debug, Clone, Copy)]
struct Header {
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
    header_crc: u32,
}

#[derive(Debug, Clone, Copy)]
struct DiskLogArray {
    bits: u8,
    count: u64,
    data_offset: u64,
    data_length: u64,
    stored_crc: u32,
}

#[derive(Debug, Clone, Copy)]
struct GraphDictionary {
    string_count: u64,
    block_size: u64,
    buffer_offset: u64,
    buffer_length: u64,
    buffer_crc: u32,
    offsets: DiskLogArray,
}

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
    flags: u32,
    primary_crc: u32,
    secondary_crc: u32,
    parameter: u64,
}

impl LayerEntry {
    fn parse(bytes: &[u8; DIRECTORY_ENTRY_SIZE as usize]) -> Self {
        Self {
            primary_offset: get_u64(bytes, 0),
            primary_length: get_u64(bytes, 8),
            secondary_offset: get_u64(bytes, 16),
            secondary_length: get_u64(bytes, 24),
            item_count_a: get_u64(bytes, 32),
            item_count_b: get_u64(bytes, 40),
            member_count: get_u64(bytes, 48),
            minimum_position: get_u64(bytes, 56),
            maximum_position_exclusive: get_u64(bytes, 64),
            encoding: get_u32(bytes, 72),
            flags: get_u32(bytes, 76),
            primary_crc: get_u32(bytes, 80),
            secondary_crc: get_u32(bytes, 84),
            parameter: get_u64(bytes, 88),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ChunkEntry {
    key: u64,
    rank_before: u64,
    payload_offset: u64,
    payload_length: u32,
    cardinality: u32,
    encoding: u8,
    flags: u8,
    reserved16: u16,
    payload_crc: u32,
    reserved64: u64,
}

impl ChunkEntry {
    fn parse(bytes: &[u8; CHUNK_ENTRY_SIZE as usize]) -> Self {
        Self {
            key: get_u64(bytes, 0),
            rank_before: get_u64(bytes, 8),
            payload_offset: get_u64(bytes, 16),
            payload_length: get_u32(bytes, 24),
            cardinality: get_u32(bytes, 28),
            encoding: bytes[32],
            flags: bytes[33],
            reserved16: u16::from_le_bytes(bytes[34..36].try_into().unwrap()),
            payload_crc: get_u32(bytes, 36),
            reserved64: get_u64(bytes, 40),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct EfHeader {
    low_bits: u32,
    universe: u64,
    members: u64,
    upper_bits: u64,
    high_buckets: u64,
    lower_offset: u64,
    lower_length: u64,
    superrank_offset: u64,
    superrank_length: u64,
    subrank_offset: u64,
    subrank_length: u64,
    upper_offset: u64,
    upper_length: u64,
    superrank_count: u64,
    subrank_count: u64,
    lower_crc: u32,
    superrank_crc: u32,
    subrank_crc: u32,
    upper_crc: u32,
}

/// A file-backed sidecar reader. Opening reads fixed metadata only; the graph
/// dictionary, directory, indexes, and payloads remain on disk.
pub struct GraphSidecarReader {
    hdt_path: PathBuf,
    file: File,
    header: Header,
    dictionary: GraphDictionary,
    hdt_data_offset: u64,
    /// Last Elias-Fano header parsed, keyed by its layer primary offset.
    ef_header_cache: Option<(u64, EfHeader)>,
}

impl GraphSidecarReader {
    pub fn open_for_hdt(hdt_path: &Path) -> Result<Self> {
        Self::open(&canonical_sidecar_path(hdt_path), hdt_path)
    }

    pub fn open(sidecar_path: &Path, hdt_path: &Path) -> Result<Self> {
        let mut file = File::open(sidecar_path)
            .with_context(|| format!("Failed to open graph sidecar {}", sidecar_path.display()))?;
        let file_size = file.metadata()?.len();
        let header = read_header(&mut file, file_size)?;
        let (hdt_data_offset, hdt_data_length, hdt_triples) = hdt_metadata(hdt_path)?;
        ensure!(
            hdt_data_length == header.source_data_length,
            "sidecar/HDT data length mismatch"
        );
        ensure!(
            hdt_triples == header.triple_count,
            "sidecar/HDT triple count mismatch"
        );
        let dictionary = read_graph_dictionary(&mut file, header)?;

        Ok(Self {
            hdt_path: hdt_path.to_path_buf(),
            file,
            header,
            dictionary,
            hdt_data_offset,
            ef_header_cache: None,
        })
    }

    pub fn triple_count(&self) -> u64 {
        self.header.triple_count
    }

    pub fn named_graph_count(&self) -> u64 {
        self.header.named_graph_count
    }

    pub fn membership_count(&self) -> u64 {
        self.header.membership_count
    }

    pub fn graph(&mut self, graph_id: u64) -> Result<GraphTerm> {
        if graph_id == 0 {
            return Ok(GraphTerm::DefaultGraph);
        }
        ensure!(
            graph_id <= self.header.named_graph_count,
            "graph ID out of range"
        );
        Ok(GraphTerm::Named(self.dictionary_term(graph_id)?))
    }

    pub fn graph_id(&mut self, term: &GraphTerm) -> Result<Option<u64>> {
        match term {
            GraphTerm::DefaultGraph => Ok(Some(0)),
            GraphTerm::Named(value) => self.graph_id_str(value),
        }
    }

    pub fn graph_id_str(&mut self, term: &str) -> Result<Option<u64>> {
        if self.dictionary.string_count == 0 {
            return Ok(None);
        }
        let block_count = self
            .dictionary
            .string_count
            .div_ceil(self.dictionary.block_size);
        let mut low = 0u64;
        let mut high = block_count;
        while low < high {
            let mid = low + (high - low) / 2;
            let first = self.dictionary_block_first(mid)?;
            if first.as_bytes() <= term.as_bytes() {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        if low == 0 {
            return Ok(None);
        }
        let block = low - 1;
        let terms = self.dictionary_block(block)?;
        match terms.binary_search_by(|candidate| candidate.as_bytes().cmp(term.as_bytes())) {
            Ok(index) => Ok(Some(block * self.dictionary.block_size + index as u64 + 1)),
            Err(_) => Ok(None),
        }
    }

    pub fn count(&mut self, graph_id: u64) -> Result<u64> {
        Ok(self.layer_entry(graph_id)?.member_count)
    }

    pub fn access(&mut self, graph_id: u64, position: u64) -> Result<bool> {
        ensure!(position < self.header.triple_count, "position out of range");
        let layer = self.layer_entry(graph_id)?;
        if layer.member_count == 0
            || position < layer.minimum_position
            || position >= layer.maximum_position_exclusive
        {
            return Ok(false);
        }
        match layer.encoding {
            ENCODING_DENSE | ENCODING_SPARSE => {
                let Some(chunk) = self.find_chunk_for_access(layer, position >> CHUNK_SHIFT)?
                else {
                    return Ok(false);
                };
                self.container_access(chunk, (position & 0xffff) as u16)
            }
            ENCODING_ELIAS_FANO => {
                let before = self.ef_rank(layer, position)?;
                Ok(self.ef_rank(layer, position + 1)? != before)
            }
            other => bail!("unsupported layer encoding {other}"),
        }
    }

    pub fn rank(&mut self, graph_id: u64, position: u64) -> Result<u64> {
        ensure!(
            position <= self.header.triple_count,
            "rank position out of range"
        );
        let layer = self.layer_entry(graph_id)?;
        if position == 0 || layer.member_count == 0 {
            return Ok(0);
        }
        if position == self.header.triple_count || position >= layer.maximum_position_exclusive {
            return Ok(layer.member_count);
        }
        if position <= layer.minimum_position {
            return Ok(0);
        }
        match layer.encoding {
            ENCODING_DENSE | ENCODING_SPARSE => self.chunked_rank(layer, position),
            ENCODING_ELIAS_FANO => self.ef_rank(layer, position),
            other => bail!("unsupported layer encoding {other}"),
        }
    }

    pub fn select(&mut self, graph_id: u64, ordinal: u64) -> Result<u64> {
        let layer = self.layer_entry(graph_id)?;
        ensure!(ordinal < layer.member_count, "select ordinal out of range");
        match layer.encoding {
            ENCODING_DENSE | ENCODING_SPARSE => self.chunked_select(layer, ordinal),
            ENCODING_ELIAS_FANO => self.ef_select(layer, ordinal),
            other => bail!("unsupported layer encoding {other}"),
        }
    }

    pub fn next_member(&mut self, graph_id: u64, position: u64) -> Result<Option<u64>> {
        ensure!(position < self.header.triple_count, "position out of range");
        let rank = self.rank(graph_id, position)?;
        let count = self.count(graph_id)?;
        if rank == count {
            Ok(None)
        } else {
            self.select(graph_id, rank).map(Some)
        }
    }

    pub fn graphs_of(&mut self, position: u64) -> Result<Vec<u64>> {
        ensure!(position < self.header.triple_count, "position out of range");
        let mut graphs = Vec::new();
        for graph_id in 0..=self.header.named_graph_count {
            if self.access(graph_id, position)? {
                graphs.push(graph_id);
            }
        }
        Ok(graphs)
    }

    pub fn layer_iter(&mut self, graph_id: u64) -> Result<LayerMemberIter> {
        let layer = self.layer_entry(graph_id)?;
        LayerMemberIter::new(self.file.try_clone()?, layer, self.header.triple_count)
    }

    /// Perform full identity, checksum, encoding, dictionary, and exhaustive
    /// membership validation with an externally sorted position sweep.
    ///
    /// The sweep already sorts every membership into `(position, graph)` order.
    /// When `transposed_output` is given, that sorted stream is also written
    /// there as a zstd-compressed `PositionGraphMembership` file, so a caller
    /// that needs the transposition (a graph-preserving merge) does not have to
    /// repeat the sort.
    pub fn validate_strict(
        &mut self,
        temp_dir: &Path,
        memory_limit: usize,
        transposed_output: Option<&Path>,
    ) -> Result<()> {
        self.validate_identity_digest()?;
        ensure!(
            range_crc(
                &mut self.file,
                self.dictionary.offsets.data_offset,
                self.dictionary.offsets.data_length
            )? == self.dictionary.offsets.stored_crc,
            "graph dictionary LogArray CRC mismatch"
        );
        let mut previous_offset = 0u64;
        for index in 0..self.dictionary.offsets.count {
            let offset = disk_log_get(&mut self.file, self.dictionary.offsets, index)?;
            ensure!(
                offset >= previous_offset,
                "graph dictionary block offsets are not monotone"
            );
            ensure!(
                offset <= self.dictionary.buffer_length,
                "graph dictionary block offset is out of range"
            );
            previous_offset = offset;
        }
        ensure!(
            previous_offset == self.dictionary.buffer_length,
            "graph dictionary sentinel mismatch"
        );
        validate_packed_tail(&mut self.file, self.dictionary.offsets)?;
        ensure!(
            range_crc(
                &mut self.file,
                self.dictionary.buffer_offset,
                self.dictionary.buffer_length
            )? == self.dictionary.buffer_crc,
            "graph dictionary buffer CRC mismatch"
        );

        let mut previous_term: Option<String> = None;
        let mut blank_graph_seen = false;
        for graph_id in 1..=self.header.named_graph_count {
            let GraphTerm::Named(term) = self.graph(graph_id)? else {
                unreachable!()
            };
            ensure!(
                previous_term
                    .as_ref()
                    .is_none_or(|previous| previous.as_bytes() < term.as_bytes()),
                "graph dictionary is not strictly sorted"
            );
            if term.starts_with("_:") {
                ensure!(term.len() > 2, "empty blank-node graph label");
                blank_graph_seen = true;
            } else {
                NamedNode::new(&term).context("graph dictionary term is not an absolute IRI")?;
            }
            previous_term = Some(term);
        }
        ensure!(
            blank_graph_seen == (self.header.flags & (1 << 2) != 0),
            "HAS_BLANK_GRAPH_NAMES flag mismatch"
        );

        let entry_bytes = self
            .header
            .directory_length
            .checked_sub(4)
            .context("invalid directory length")?;
        let stored_directory_crc =
            read_u32_at(&mut self.file, self.header.directory_offset + entry_bytes)?;
        ensure!(
            range_crc(&mut self.file, self.header.directory_offset, entry_bytes)?
                == stored_directory_crc,
            "layer-directory CRC mismatch"
        );

        let sort_budget = memory_limit.max(1024 * 1024);
        let mut sorter = ExternalSorter::new(temp_dir, sort_budget);
        let mut buffer = Vec::<PositionMembership>::new();
        let mut memory_used = 0usize;
        let mut total_members = 0u64;
        let mut first_layer_start = None;
        let mut previous_layer_end = self.header.layers_offset;

        for graph_id in 0..=self.header.named_graph_count {
            let layer = self.layer_entry(graph_id)?;
            if let Some((start, end)) = self.validate_layer_metadata(layer)? {
                ensure!(
                    start >= previous_layer_end,
                    "layer regions overlap or are out of graph-ID order"
                );
                first_layer_start.get_or_insert(start);
                previous_layer_end = end;
            }
            let mut count = 0u64;
            let mut minimum = self.header.triple_count;
            let mut maximum_exclusive = 0u64;
            let mut previous = None;
            let iterator = self.layer_iter(graph_id)?;
            for position in iterator {
                let position = position?;
                ensure!(
                    previous.is_none_or(|value| position > value),
                    "layer positions are not strictly increasing"
                );
                previous = Some(position);
                minimum = minimum.min(position);
                maximum_exclusive = position + 1;
                count += 1;
                total_members += 1;
                sorter.push(
                    PositionMembership {
                        position,
                        graph: graph_id,
                    },
                    &mut buffer,
                    &mut memory_used,
                )?;
            }
            ensure!(count == layer.member_count, "layer member count mismatch");
            ensure!(minimum == layer.minimum_position, "layer minimum mismatch");
            ensure!(
                maximum_exclusive == layer.maximum_position_exclusive,
                "layer maximum mismatch"
            );
        }
        ensure!(
            total_members == self.header.membership_count,
            "global membership count mismatch"
        );
        let expected_layers_end = self
            .header
            .layers_offset
            .checked_add(self.header.layers_length)
            .context("layers span overflow")?;
        if self.header.layers_length == 0 {
            ensure!(
                first_layer_start.is_none(),
                "empty layers span contains a layer"
            );
        } else {
            ensure!(
                first_layer_start == Some(self.header.layers_offset),
                "layers offset is not the first non-empty layer"
            );
            ensure!(
                previous_layer_end == expected_layers_end,
                "layers length does not end at the final payload"
            );
        }

        let mut transposed = transposed_output
            .map(|path| -> Result<_> {
                let file = File::create(path)
                    .with_context(|| format!("Failed to create {}", path.display()))?;
                Ok(zstd::Encoder::new(BufWriter::new(file), 3)?)
            })
            .transpose()?;

        let mut expected_position = 0u64;
        let mut current_position = None;
        let mut graph_count_at_position = 0u64;
        for item in sorter.finish(&mut buffer)? {
            let item = item?;
            if let Some(encoder) = transposed.as_mut() {
                // Same field order and 16-byte layout as
                // `PositionGraphMembership`, which is what consumers read back.
                item.write_to(encoder)?;
            }
            if current_position != Some(item.position) {
                if let Some(previous) = current_position {
                    ensure!(
                        previous == expected_position,
                        "non-exhaustive graph memberships"
                    );
                    if self.header.flags & (1 << 1) != 0 {
                        ensure!(graph_count_at_position == 1, "DISJOINT flag violated");
                    }
                    expected_position += 1;
                }
                current_position = Some(item.position);
                graph_count_at_position = 0;
            }
            graph_count_at_position += 1;
        }
        if let Some(previous) = current_position {
            ensure!(
                previous == expected_position,
                "non-exhaustive graph memberships"
            );
            if self.header.flags & (1 << 1) != 0 {
                ensure!(graph_count_at_position == 1, "DISJOINT flag violated");
            }
            expected_position += 1;
        }
        ensure!(
            expected_position == self.header.triple_count,
            "non-exhaustive graph memberships"
        );
        if let Some(encoder) = transposed {
            encoder.finish()?.flush()?;
        }
        Ok(())
    }

    fn validate_identity_digest(&mut self) -> Result<()> {
        let mut file = File::open(&self.hdt_path)?;
        file.seek(SeekFrom::Start(self.hdt_data_offset))?;
        let digest = sha256_to_end(&mut BufReader::with_capacity(256 * 1024, file))?;
        ensure!(
            digest == self.header.source_digest,
            "graph sidecar is bound to a different HDT"
        );
        Ok(())
    }

    fn validate_layer_metadata(&mut self, layer: LayerEntry) -> Result<Option<(u64, u64)>> {
        ensure!(layer.flags == 0, "nonzero layer flags");
        if layer.member_count == 0 {
            ensure!(
                layer.encoding == ENCODING_SPARSE,
                "empty layer must use sparse encoding"
            );
            ensure!(
                layer.primary_offset == 0
                    && layer.primary_length == 0
                    && layer.secondary_offset == 0
                    && layer.secondary_length == 0
                    && layer.item_count_a == 0
                    && layer.item_count_b == 0
                    && layer.primary_crc == 0
                    && layer.secondary_crc == 0
                    && layer.parameter == 0,
                "invalid empty layer fields"
            );
            ensure!(
                layer.minimum_position == self.header.triple_count,
                "invalid empty layer minimum"
            );
            ensure!(
                layer.maximum_position_exclusive == 0,
                "invalid empty layer maximum"
            );
            return Ok(None);
        }
        ensure!(
            layer.minimum_position < layer.maximum_position_exclusive,
            "invalid layer range"
        );
        ensure!(
            layer.maximum_position_exclusive <= self.header.triple_count,
            "layer range outside universe"
        );
        ensure!(
            layer.primary_offset.is_multiple_of(64),
            "unaligned layer primary"
        );
        self.ensure_layer_range(layer.primary_offset, layer.primary_length)?;
        ensure!(
            range_crc(&mut self.file, layer.primary_offset, layer.primary_length)?
                == layer.primary_crc,
            "layer primary CRC mismatch"
        );

        match layer.encoding {
            ENCODING_DENSE | ENCODING_SPARSE => {
                ensure!(
                    layer.primary_length
                        == layer
                            .item_count_a
                            .checked_mul(CHUNK_ENTRY_SIZE)
                            .context("chunk directory length overflow")?,
                    "chunk directory length mismatch"
                );
                if layer.encoding == ENCODING_DENSE {
                    let expected = if self.header.triple_count == 0 {
                        0
                    } else {
                        1 + ((self.header.triple_count - 1) >> CHUNK_SHIFT)
                    };
                    ensure!(
                        layer.item_count_a == expected,
                        "dense chunk table is incomplete"
                    );
                    ensure!(
                        layer.secondary_offset == 0 && layer.secondary_length == 0,
                        "dense layer has secondary index"
                    );
                    ensure!(
                        layer.parameter == 0 && layer.secondary_crc == 0,
                        "invalid dense layer parameter"
                    );
                } else {
                    ensure!(
                        layer.item_count_a == layer.item_count_b,
                        "sparse layer contains empty chunks"
                    );
                    ensure!(
                        layer.parameter >= 2 && layer.parameter.is_power_of_two(),
                        "invalid sparse hash capacity"
                    );
                    ensure!(
                        layer.parameter
                            >= layer
                                .item_count_a
                                .checked_mul(2)
                                .context("sparse chunk count overflow")?,
                        "sparse hash load factor too high"
                    );
                    ensure!(
                        layer.secondary_offset.is_multiple_of(8),
                        "unaligned sparse hash"
                    );
                    ensure!(
                        layer.secondary_length
                            == layer
                                .parameter
                                .checked_mul(8)
                                .context("sparse hash length overflow")?,
                        "sparse hash length mismatch"
                    );
                    self.ensure_layer_range(layer.secondary_offset, layer.secondary_length)?;
                    ensure!(
                        range_crc(
                            &mut self.file,
                            layer.secondary_offset,
                            layer.secondary_length
                        )? == layer.secondary_crc,
                        "sparse hash CRC mismatch"
                    );
                }
                let end = self.validate_chunk_directory(layer)?;
                Ok(Some((layer.primary_offset, end)))
            }
            ENCODING_ELIAS_FANO => {
                let end = self.validate_ef_metadata(layer)?;
                Ok(Some((layer.primary_offset, end)))
            }
            other => bail!("unsupported layer encoding {other}"),
        }
    }

    fn validate_chunk_directory(&mut self, layer: LayerEntry) -> Result<u64> {
        let mut rank = 0u64;
        let mut previous_key = None;
        let mut non_empty = 0u64;
        let primary_end = layer
            .primary_offset
            .checked_add(layer.primary_length)
            .context("chunk directory end overflow")?;
        let mut payload_end = if layer.encoding == ENCODING_SPARSE {
            ensure!(
                layer.secondary_offset >= primary_end,
                "sparse hash overlaps chunk directory"
            );
            layer
                .secondary_offset
                .checked_add(layer.secondary_length)
                .context("sparse hash end overflow")?
        } else {
            primary_end
        };
        for index in 0..layer.item_count_a {
            let chunk = self.read_chunk(layer, index)?;
            ensure!(
                chunk.flags == 0 && chunk.reserved16 == 0 && chunk.reserved64 == 0,
                "nonzero chunk reserved fields"
            );
            ensure!(chunk.rank_before == rank, "chunk rank recurrence mismatch");
            if layer.encoding == ENCODING_DENSE {
                ensure!(chunk.key == index, "dense chunk key mismatch");
            } else {
                ensure!(chunk.cardinality > 0, "empty sparse chunk");
                ensure!(
                    previous_key.is_none_or(|key| chunk.key > key),
                    "sparse keys not increasing"
                );
            }
            ensure!(
                chunk.key < self.header.triple_count.div_ceil(1 << CHUNK_SHIFT),
                "chunk key outside universe"
            );
            if chunk.cardinality == 0 {
                ensure!(
                    chunk.encoding == 0
                        && chunk.payload_offset == 0
                        && chunk.payload_length == 0
                        && chunk.payload_crc == 0,
                    "invalid empty chunk"
                );
            } else {
                non_empty += 1;
                ensure!(
                    chunk.payload_offset.is_multiple_of(8),
                    "unaligned chunk payload"
                );
                ensure!(
                    chunk.payload_offset >= payload_end,
                    "chunk payloads overlap or are out of key order"
                );
                self.ensure_layer_range(chunk.payload_offset, u64::from(chunk.payload_length))?;
                payload_end = chunk
                    .payload_offset
                    .checked_add(u64::from(chunk.payload_length))
                    .context("chunk payload end overflow")?;
                if chunk.cardinality <= 4096 {
                    ensure!(
                        chunk.encoding == 1 && chunk.payload_length == chunk.cardinality * 2,
                        "invalid array container"
                    );
                } else {
                    ensure!(
                        chunk.encoding == 2 && chunk.payload_length == 8_448,
                        "invalid bitmap container"
                    );
                }
                ensure!(
                    range_crc(
                        &mut self.file,
                        chunk.payload_offset,
                        u64::from(chunk.payload_length)
                    )? == chunk.payload_crc,
                    "chunk payload CRC mismatch"
                );
                if chunk.encoding == 2 {
                    self.validate_bitmap_subranks(chunk)?;
                }
            }
            rank += u64::from(chunk.cardinality);
            previous_key = Some(chunk.key);
        }
        ensure!(
            rank == layer.member_count,
            "chunk directory member count mismatch"
        );
        ensure!(
            non_empty == layer.item_count_b,
            "non-empty chunk count mismatch"
        );
        if layer.encoding == ENCODING_SPARSE {
            for index in 0..layer.item_count_a {
                let chunk = self.read_chunk(layer, index)?;
                let found = self.find_chunk_for_access(layer, chunk.key)?;
                ensure!(
                    found.is_some_and(|entry| entry.key == chunk.key),
                    "sparse hash lookup failed"
                );
            }
        }
        Ok(payload_end)
    }

    fn validate_bitmap_subranks(&mut self, chunk: ChunkEntry) -> Result<()> {
        let mut bitmap = [0u8; 8192];
        read_exact_at(&mut self.file, chunk.payload_offset + 256, &mut bitmap)?;
        let mut rank = 0u32;
        for block in 0..128u64 {
            let stored = u32::from(read_u16_at(
                &mut self.file,
                chunk.payload_offset + block * 2,
            )?);
            ensure!(stored == rank, "bitmap subrank recurrence mismatch");
            let start = block as usize * 64;
            rank += bitmap[start..start + 64]
                .iter()
                .map(|byte| byte.count_ones())
                .sum::<u32>();
        }
        ensure!(
            rank == chunk.cardinality,
            "bitmap subrank cardinality mismatch"
        );
        Ok(())
    }

    fn validate_ef_metadata(&mut self, layer: LayerEntry) -> Result<u64> {
        ensure!(
            layer.secondary_offset == 0
                && layer.secondary_length == 0
                && layer.secondary_crc == 0
                && layer.item_count_a == 0
                && layer.item_count_b == 0
                && layer.parameter == 0,
            "invalid Elias-Fano directory fields"
        );
        let header = self.ef_header(layer)?;
        ensure!(
            header.universe == self.header.triple_count,
            "Elias-Fano universe mismatch"
        );
        ensure!(
            header.members == layer.member_count,
            "Elias-Fano member count mismatch"
        );
        ensure!(header.low_bits < 64, "invalid Elias-Fano low-bit width");
        let ratio = header.universe / header.members;
        let expected_low_bits = if ratio <= 1 {
            0
        } else {
            63 - ratio.leading_zeros()
        };
        ensure!(
            header.low_bits == expected_low_bits,
            "noncanonical Elias-Fano low-bit width"
        );
        ensure!(
            header.high_buckets == 1 + ((header.universe - 1) >> header.low_bits),
            "Elias-Fano high-bucket count mismatch"
        );
        let expected_upper_bits = header
            .high_buckets
            .checked_add(header.members)
            .context("Elias-Fano upper length overflow")?;
        ensure!(
            header.upper_bits == expected_upper_bits,
            "Elias-Fano upper length mismatch"
        );
        ensure!(
            header.lower_length
                == header
                    .members
                    .checked_mul(u64::from(header.low_bits))
                    .context("Elias-Fano lower length overflow")?
                    .div_ceil(8),
            "Elias-Fano lower length mismatch"
        );
        ensure!(
            header.upper_length == header.upper_bits.div_ceil(8),
            "Elias-Fano upper byte length mismatch"
        );
        ensure!(
            header.superrank_count == header.upper_bits.div_ceil(4096) + 1,
            "Elias-Fano superrank count mismatch"
        );
        ensure!(
            header.subrank_count == header.upper_bits.div_ceil(512),
            "Elias-Fano subrank count mismatch"
        );
        ensure!(
            header.superrank_length == header.superrank_count * 8,
            "Elias-Fano superrank length mismatch"
        );
        ensure!(
            header.subrank_length == header.subrank_count * 2,
            "Elias-Fano subrank length mismatch"
        );
        ensure!(
            range_crc(&mut self.file, header.lower_offset, header.lower_length)?
                == header.lower_crc,
            "Elias-Fano lower CRC mismatch"
        );
        ensure!(
            range_crc(
                &mut self.file,
                header.superrank_offset,
                header.superrank_length
            )? == header.superrank_crc,
            "Elias-Fano superrank CRC mismatch"
        );
        ensure!(
            range_crc(&mut self.file, header.subrank_offset, header.subrank_length)?
                == header.subrank_crc,
            "Elias-Fano subrank CRC mismatch"
        );
        ensure!(
            range_crc(&mut self.file, header.upper_offset, header.upper_length)?
                == header.upper_crc,
            "Elias-Fano upper CRC mismatch"
        );
        if header.lower_length == 0 {
            ensure!(
                header.lower_offset == 0 && header.lower_crc == 0,
                "invalid empty Elias-Fano lower region"
            );
        } else {
            ensure!(
                header.lower_offset.is_multiple_of(8),
                "unaligned Elias-Fano lower region"
            );
            self.ensure_layer_range(header.lower_offset, header.lower_length)?;
            validate_tail_bits(
                &mut self.file,
                header.lower_offset,
                header.lower_length,
                header.members * u64::from(header.low_bits),
            )?;
        }
        let mut previous_end = layer
            .primary_offset
            .checked_add(layer.primary_length)
            .context("Elias-Fano header end overflow")?;
        if header.lower_length != 0 {
            ensure!(
                header.lower_offset >= previous_end,
                "Elias-Fano lower region overlaps header"
            );
            previous_end = header
                .lower_offset
                .checked_add(header.lower_length)
                .context("Elias-Fano lower end overflow")?;
        }
        for (offset, length) in [
            (header.superrank_offset, header.superrank_length),
            (header.subrank_offset, header.subrank_length),
            (header.upper_offset, header.upper_length),
        ] {
            ensure!(offset.is_multiple_of(8), "unaligned Elias-Fano region");
            ensure!(
                offset >= previous_end,
                "Elias-Fano regions overlap or are out of canonical order"
            );
            self.ensure_layer_range(offset, length)?;
            previous_end = offset
                .checked_add(length)
                .context("Elias-Fano region end overflow")?;
        }
        validate_tail_bits(
            &mut self.file,
            header.upper_offset,
            header.upper_length,
            header.upper_bits,
        )?;
        self.validate_ef_ranks(header)?;
        Ok(previous_end)
    }

    fn validate_ef_ranks(&mut self, header: EfHeader) -> Result<()> {
        let word_count = header.upper_bits.div_ceil(64);
        let mut rank = 0u64;
        let mut super_base = 0u64;
        for word_index in 0..word_count {
            if word_index.is_multiple_of(64) {
                let stored = read_u64_at(
                    &mut self.file,
                    header.superrank_offset + (word_index / 64) * 8,
                )?;
                ensure!(stored == rank, "Elias-Fano superrank recurrence mismatch");
                super_base = rank;
            }
            if word_index.is_multiple_of(8) {
                let stored = u64::from(read_u16_at(
                    &mut self.file,
                    header.subrank_offset + (word_index / 8) * 2,
                )?);
                ensure!(
                    stored == rank - super_base,
                    "Elias-Fano subrank recurrence mismatch"
                );
            }
            let byte_offset = word_index * 8;
            let bytes_to_read = (header.upper_length - byte_offset).min(8) as usize;
            let mut bytes = [0u8; 8];
            read_exact_at(
                &mut self.file,
                header.upper_offset + byte_offset,
                &mut bytes[..bytes_to_read],
            )?;
            rank += u64::from(u64::from_le_bytes(bytes).count_ones());
        }
        let final_rank = read_u64_at(
            &mut self.file,
            header.superrank_offset + (header.superrank_count - 1) * 8,
        )?;
        ensure!(
            final_rank == rank && rank == header.members,
            "Elias-Fano upper cardinality mismatch"
        );
        Ok(())
    }

    fn ensure_layer_range(&self, offset: u64, length: u64) -> Result<()> {
        let layers_end = self
            .header
            .layers_offset
            .checked_add(self.header.layers_length)
            .context("layers range overflow")?;
        let end = offset.checked_add(length).context("layer range overflow")?;
        ensure!(
            offset >= self.header.layers_offset && end <= layers_end,
            "layer region outside layers span"
        );
        Ok(())
    }

    fn layer_entry(&mut self, graph_id: u64) -> Result<LayerEntry> {
        ensure!(
            graph_id <= self.header.named_graph_count,
            "graph ID out of range"
        );
        let offset = self
            .header
            .directory_offset
            .checked_add(
                graph_id
                    .checked_mul(DIRECTORY_ENTRY_SIZE)
                    .context("directory offset overflow")?,
            )
            .context("directory offset overflow")?;
        let mut bytes = [0u8; DIRECTORY_ENTRY_SIZE as usize];
        read_exact_at(&mut self.file, offset, &mut bytes)?;
        Ok(LayerEntry::parse(&bytes))
    }

    fn read_chunk(&mut self, layer: LayerEntry, index: u64) -> Result<ChunkEntry> {
        ensure!(index < layer.item_count_a, "chunk index out of range");
        let offset = layer
            .primary_offset
            .checked_add(
                index
                    .checked_mul(CHUNK_ENTRY_SIZE)
                    .context("chunk offset overflow")?,
            )
            .context("chunk offset overflow")?;
        let mut bytes = [0u8; CHUNK_ENTRY_SIZE as usize];
        read_exact_at(&mut self.file, offset, &mut bytes)?;
        Ok(ChunkEntry::parse(&bytes))
    }

    fn find_chunk_for_access(&mut self, layer: LayerEntry, key: u64) -> Result<Option<ChunkEntry>> {
        if layer.encoding == ENCODING_DENSE {
            if key >= layer.item_count_a {
                return Ok(None);
            }
            let entry = self.read_chunk(layer, key)?;
            return Ok((entry.cardinality != 0).then_some(entry));
        }
        ensure!(
            layer.parameter >= 2 && layer.parameter.is_power_of_two(),
            "invalid sparse hash capacity"
        );
        let mut slot = mix64(key) & (layer.parameter - 1);
        for _ in 0..layer.parameter {
            let value = read_u64_at(&mut self.file, layer.secondary_offset + slot * 8)?;
            if value == 0 {
                return Ok(None);
            }
            let index = value - 1;
            ensure!(index < layer.item_count_a, "sparse hash index out of range");
            let entry = self.read_chunk(layer, index)?;
            if entry.key == key {
                return Ok(Some(entry));
            }
            slot = (slot + 1) & (layer.parameter - 1);
        }
        bail!("sparse hash probe did not terminate")
    }

    fn find_chunk_sorted(
        &mut self,
        layer: LayerEntry,
        key: u64,
    ) -> Result<(u64, Option<ChunkEntry>)> {
        let mut low = 0u64;
        let mut high = layer.item_count_a;
        while low < high {
            let mid = low + (high - low) / 2;
            let entry = self.read_chunk(layer, mid)?;
            if entry.key < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        if low < layer.item_count_a {
            let entry = self.read_chunk(layer, low)?;
            if entry.key == key {
                return Ok((low, Some(entry)));
            }
        }
        Ok((low, None))
    }

    fn container_access(&mut self, chunk: ChunkEntry, offset: u16) -> Result<bool> {
        match chunk.encoding {
            1 => {
                let mut low = 0u32;
                let mut high = chunk.cardinality;
                while low < high {
                    let mid = low + (high - low) / 2;
                    let value =
                        read_u16_at(&mut self.file, chunk.payload_offset + u64::from(mid) * 2)?;
                    if value < offset {
                        low = mid + 1;
                    } else {
                        high = mid;
                    }
                }
                Ok(low < chunk.cardinality
                    && read_u16_at(&mut self.file, chunk.payload_offset + u64::from(low) * 2)?
                        == offset)
            }
            2 => {
                let byte = read_u8_at(
                    &mut self.file,
                    chunk.payload_offset + 256 + u64::from(offset) / 8,
                )?;
                Ok(byte & (1 << (offset % 8)) != 0)
            }
            0 => Ok(false),
            other => bail!("unsupported chunk container encoding {other}"),
        }
    }

    fn container_rank(&mut self, chunk: ChunkEntry, offset: u16) -> Result<u64> {
        match chunk.encoding {
            1 => {
                let mut low = 0u32;
                let mut high = chunk.cardinality;
                while low < high {
                    let mid = low + (high - low) / 2;
                    let value =
                        read_u16_at(&mut self.file, chunk.payload_offset + u64::from(mid) * 2)?;
                    if value < offset {
                        low = mid + 1;
                    } else {
                        high = mid;
                    }
                }
                Ok(u64::from(low))
            }
            2 => {
                let subblock = u64::from(offset) / 512;
                let base = u64::from(read_u16_at(
                    &mut self.file,
                    chunk.payload_offset + subblock * 2,
                )?);
                let start = subblock * 512;
                let bit_count = u64::from(offset) - start;
                let byte_count = bit_count.div_ceil(8) as usize;
                let mut bytes = [0u8; 64];
                if byte_count > 0 {
                    read_exact_at(
                        &mut self.file,
                        chunk.payload_offset + 256 + start / 8,
                        &mut bytes[..byte_count],
                    )?;
                    if !bit_count.is_multiple_of(8) {
                        bytes[byte_count - 1] &= (1 << (bit_count % 8)) - 1;
                    }
                }
                Ok(base
                    + bytes[..byte_count]
                        .iter()
                        .map(|byte| u64::from(byte.count_ones()))
                        .sum::<u64>())
            }
            other => bail!("unsupported non-empty container encoding {other}"),
        }
    }

    fn container_select(&mut self, chunk: ChunkEntry, ordinal: u64) -> Result<u16> {
        ensure!(
            ordinal < u64::from(chunk.cardinality),
            "container select out of range"
        );
        match chunk.encoding {
            1 => read_u16_at(&mut self.file, chunk.payload_offset + ordinal * 2),
            2 => {
                let mut low = 0u64;
                let mut high = 128u64;
                while low < high {
                    let mid = low + (high - low) / 2;
                    let end_rank = if mid == 127 {
                        u64::from(chunk.cardinality)
                    } else {
                        u64::from(read_u16_at(
                            &mut self.file,
                            chunk.payload_offset + (mid + 1) * 2,
                        )?)
                    };
                    if end_rank > ordinal {
                        high = mid;
                    } else {
                        low = mid + 1;
                    }
                }
                let subblock = low;
                let start_rank = u64::from(read_u16_at(
                    &mut self.file,
                    chunk.payload_offset + subblock * 2,
                )?);
                let mut local = ordinal - start_rank;
                for word_index in 0..8u64 {
                    let word = read_u64_at(
                        &mut self.file,
                        chunk.payload_offset + 256 + subblock * 64 + word_index * 8,
                    )?;
                    let ones = u64::from(word.count_ones());
                    if local < ones {
                        let bit = select_in_word(word, local)?;
                        return u16::try_from(subblock * 512 + word_index * 64 + u64::from(bit))
                            .context("bitmap select offset overflow");
                    }
                    local -= ones;
                }
                bail!("bitmap select cardinality mismatch")
            }
            other => bail!("unsupported container encoding {other}"),
        }
    }

    fn chunked_rank(&mut self, layer: LayerEntry, position: u64) -> Result<u64> {
        let key = position >> CHUNK_SHIFT;
        let offset = (position & 0xffff) as u16;
        let (insertion, entry) = if layer.encoding == ENCODING_DENSE {
            let entry = self.read_chunk(layer, key)?;
            (key, Some(entry))
        } else {
            self.find_chunk_sorted(layer, key)?
        };
        if let Some(entry) = entry {
            if entry.cardinality == 0 {
                Ok(entry.rank_before)
            } else {
                Ok(entry.rank_before + self.container_rank(entry, offset)?)
            }
        } else if insertion == layer.item_count_a {
            Ok(layer.member_count)
        } else {
            Ok(self.read_chunk(layer, insertion)?.rank_before)
        }
    }

    fn chunked_select(&mut self, layer: LayerEntry, ordinal: u64) -> Result<u64> {
        let mut low = 0u64;
        let mut high = layer.item_count_a;
        while low < high {
            let mid = low + (high - low) / 2;
            let entry = self.read_chunk(layer, mid)?;
            if entry.rank_before + u64::from(entry.cardinality) > ordinal {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        ensure!(low < layer.item_count_a, "chunk select directory mismatch");
        let entry = self.read_chunk(layer, low)?;
        let local = self.container_select(entry, ordinal - entry.rank_before)?;
        Ok((entry.key << CHUNK_SHIFT) | u64::from(local))
    }

    /// Read a layer's Elias-Fano header, reusing the last one parsed. The
    /// header is immutable file data, so a single-slot cache removes the
    /// repeated 160-byte read and CRC32C from every rank/select/access.
    fn ef_header(&mut self, layer: LayerEntry) -> Result<EfHeader> {
        if let Some((offset, header)) = self.ef_header_cache
            && offset == layer.primary_offset
        {
            return Ok(header);
        }
        let header = read_ef_header_from(&mut self.file, layer)?;
        self.ef_header_cache = Some((layer.primary_offset, header));
        Ok(header)
    }

    fn ef_lower(&mut self, header: EfHeader, index: u64) -> Result<u64> {
        if header.low_bits == 0 {
            return Ok(0);
        }
        let bit = index
            .checked_mul(u64::from(header.low_bits))
            .context("lower-part bit offset overflow")?;
        let byte_offset = bit / 8;
        let shift = (bit % 8) as u32;
        ensure!(
            byte_offset < header.lower_length,
            "lower-part offset out of range"
        );
        let available = (header.lower_length - byte_offset).min(16) as usize;
        let mut bytes = [0u8; 16];
        read_exact_at(
            &mut self.file,
            header.lower_offset + byte_offset,
            &mut bytes[..available],
        )?;
        let packed = u128::from_le_bytes(bytes);
        let mask = (1u128 << header.low_bits) - 1;
        Ok(((packed >> shift) & mask) as u64)
    }

    fn ef_rank1(&mut self, header: EfHeader, position: u64) -> Result<u64> {
        ensure!(
            position <= header.upper_bits,
            "upper rank position out of range"
        );
        if position == header.upper_bits {
            return Ok(header.members);
        }
        let superblock = position / 4096;
        let subblock = position / 512;
        let superrank = read_u64_at(&mut self.file, header.superrank_offset + superblock * 8)?;
        let subrank = u64::from(read_u16_at(
            &mut self.file,
            header.subrank_offset + subblock * 2,
        )?);
        let start = subblock * 512;
        let bit_count = position - start;
        let byte_count = bit_count.div_ceil(8) as usize;
        let mut bytes = [0u8; 64];
        if byte_count > 0 {
            read_exact_at(
                &mut self.file,
                header.upper_offset + start / 8,
                &mut bytes[..byte_count],
            )?;
            if !bit_count.is_multiple_of(8) {
                bytes[byte_count - 1] &= (1 << (bit_count % 8)) - 1;
            }
        }
        Ok(superrank
            + subrank
            + bytes[..byte_count]
                .iter()
                .map(|byte| u64::from(byte.count_ones()))
                .sum::<u64>())
    }

    /// Directory-guided select over the upper bitmap: binary-search the
    /// superblock end ranks, then the containing superblock's eight
    /// subblocks, then at most eight 64-bit words. Reads stay bounded
    /// instead of scaling with the bitmap length.
    fn ef_select_bit(&mut self, header: EfHeader, ordinal: u64, one: bool) -> Result<u64> {
        let total = if one {
            header.members
        } else {
            header.upper_bits - header.members
        };
        ensure!(ordinal < total, "Elias-Fano select ordinal out of range");

        // Rank of the requested bit value in `[0, min(bit, upper_bits))`,
        // given the number of one bits in that same prefix.
        let upper_bits = header.upper_bits;
        let value_rank = move |ones: u64, bit: u64| -> u64 {
            if one { ones } else { bit.min(upper_bits) - ones }
        };

        // Last 4096-bit superblock whose start rank is at or below `ordinal`.
        let superblocks = header
            .superrank_count
            .checked_sub(1)
            .filter(|count| *count > 0)
            .context("Elias-Fano superrank directory is empty")?;
        let mut low = 0u64;
        let mut high = superblocks - 1;
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            let ones = read_u64_at(&mut self.file, header.superrank_offset + mid * 8)?;
            if value_rank(ones, mid * 4096) <= ordinal {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        let superblock = low;
        let superblock_ones =
            read_u64_at(&mut self.file, header.superrank_offset + superblock * 8)?;

        // Last 512-bit subblock of that superblock whose start rank fits.
        let mut low = superblock * 8;
        let mut high = (low + 7).min(header.subrank_count - 1);
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            let relative = u64::from(read_u16_at(&mut self.file, header.subrank_offset + mid * 2)?);
            if value_rank(superblock_ones + relative, mid * 512) <= ordinal {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        let subblock = low;
        let relative =
            u64::from(read_u16_at(&mut self.file, header.subrank_offset + subblock * 2)?);
        let mut rank = value_rank(superblock_ones + relative, subblock * 512);

        // At most eight words inside the subblock.
        let start = subblock * 512;
        for word_index in 0..8u64 {
            let bit_base = start + word_index * 64;
            if bit_base >= header.upper_bits {
                break;
            }
            let byte_offset = bit_base / 8;
            let bytes_to_read = (header.upper_length - byte_offset).min(8) as usize;
            let mut bytes = [0u8; 8];
            read_exact_at(
                &mut self.file,
                header.upper_offset + byte_offset,
                &mut bytes[..bytes_to_read],
            )?;
            let mut word = u64::from_le_bytes(bytes);
            if !one {
                word = !word;
            }
            // Bits at or beyond `upper_bits` are not part of the bitmap and
            // must not be selected, especially after inverting for select0.
            let valid = (header.upper_bits - bit_base).min(64);
            if valid < 64 {
                word &= (1u64 << valid) - 1;
            }
            let ones = u64::from(word.count_ones());
            if rank + ones > ordinal {
                let bit = select_in_word(word, ordinal - rank)?;
                return Ok(bit_base + u64::from(bit));
            }
            rank += ones;
        }
        bail!("Elias-Fano rank directory disagrees with the upper bitmap")
    }

    fn ef_select(&mut self, layer: LayerEntry, ordinal: u64) -> Result<u64> {
        let header = self.ef_header(layer)?;
        let upper = self.ef_select_bit(header, ordinal, true)?;
        let high = upper - ordinal;
        let value = (high << header.low_bits) | self.ef_lower(header, ordinal)?;
        ensure!(
            value < header.universe,
            "decoded Elias-Fano value out of range"
        );
        Ok(value)
    }

    fn ef_rank(&mut self, layer: LayerEntry, position: u64) -> Result<u64> {
        let header = self.ef_header(layer)?;
        if position == 0 {
            return Ok(0);
        }
        if position == header.universe {
            return Ok(header.members);
        }
        let high = position >> header.low_bits;
        let low_mask = if header.low_bits == 0 {
            0
        } else {
            (1u64 << header.low_bits) - 1
        };
        let low_part = position & low_mask;
        if high >= header.high_buckets {
            return Ok(header.members);
        }
        // Exactly `j` zeros precede the zero-based `j`th zero bit, so
        // `rank1(select0(j)) == select0(j) - j` and no rank probe is needed.
        let start = if high == 0 {
            0
        } else {
            self.ef_select_bit(header, high - 1, false)? - (high - 1)
        };
        let end = self.ef_select_bit(header, high, false)? - high;
        let mut left = start;
        let mut right = end;
        while left < right {
            let mid = left + (right - left) / 2;
            if self.ef_lower(header, mid)? < low_part {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        Ok(left)
    }

    fn dictionary_term(&mut self, graph_id: u64) -> Result<String> {
        let zero_based = graph_id - 1;
        let block = zero_based / self.dictionary.block_size;
        let index = (zero_based % self.dictionary.block_size) as usize;
        self.dictionary_block(block)?
            .into_iter()
            .nth(index)
            .context("graph dictionary ID missing")
    }

    fn dictionary_block_first(&mut self, block: u64) -> Result<String> {
        self.dictionary_block(block)?
            .into_iter()
            .next()
            .context("empty graph dictionary block")
    }

    fn dictionary_block(&mut self, block: u64) -> Result<Vec<String>> {
        let start = disk_log_get(&mut self.file, self.dictionary.offsets, block)?;
        let end = disk_log_get(&mut self.file, self.dictionary.offsets, block + 1)?;
        ensure!(
            end >= start && end <= self.dictionary.buffer_length,
            "invalid graph dictionary block offsets"
        );
        let mut bytes =
            vec![0u8; usize::try_from(end - start).context("graph dictionary block too large")?];
        read_exact_at(
            &mut self.file,
            self.dictionary.buffer_offset + start,
            &mut bytes,
        )?;
        let base_id = block * self.dictionary.block_size;
        let count = (self.dictionary.string_count - base_id).min(self.dictionary.block_size);
        decode_pfc_block(&bytes, count)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PositionMembership {
    position: u64,
    graph: u64,
}

impl Sortable for PositionMembership {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.position.to_le_bytes())?;
        writer.write_all(&self.graph.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut bytes = [0u8; 16];
        match reader.read_exact(&mut bytes) {
            Ok(()) => Ok(Some(Self {
                position: get_u64(&bytes, 0),
                graph: get_u64(&bytes, 8),
            })),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn mem_size(&self) -> usize {
        16
    }
}

/// Forward layer iterator. It keeps one chunk container or the current
/// Elias-Fano cursor, so continued iteration is O(1) amortized and bounded.
pub enum LayerMemberIter {
    Empty,
    Chunked(ChunkLayerIter),
    EliasFano(EliasFanoLayerIter),
}

impl LayerMemberIter {
    fn new(file: File, layer: LayerEntry, universe: u64) -> Result<Self> {
        if layer.member_count == 0 {
            return Ok(Self::Empty);
        }
        match layer.encoding {
            ENCODING_DENSE | ENCODING_SPARSE => Ok(Self::Chunked(ChunkLayerIter {
                file,
                layer,
                universe,
                entry_index: 0,
                current_positions: Vec::new(),
                current_index: 0,
                yielded: 0,
            })),
            ENCODING_ELIAS_FANO => Ok(Self::EliasFano(EliasFanoLayerIter::new(
                file, layer, universe,
            )?)),
            other => bail!("unsupported layer encoding {other}"),
        }
    }
}

impl Iterator for LayerMemberIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::Chunked(iterator) => iterator.next(),
            Self::EliasFano(iterator) => iterator.next(),
        }
    }
}

pub struct ChunkLayerIter {
    file: File,
    layer: LayerEntry,
    universe: u64,
    entry_index: u64,
    current_positions: Vec<u64>,
    current_index: usize,
    yielded: u64,
}

impl ChunkLayerIter {
    fn load_next_container(&mut self) -> Result<bool> {
        self.current_positions.clear();
        self.current_index = 0;
        while self.entry_index < self.layer.item_count_a {
            let offset = self.layer.primary_offset + self.entry_index * CHUNK_ENTRY_SIZE;
            self.entry_index += 1;
            let mut bytes = [0u8; CHUNK_ENTRY_SIZE as usize];
            read_exact_at(&mut self.file, offset, &mut bytes)?;
            let chunk = ChunkEntry::parse(&bytes);
            if chunk.cardinality == 0 {
                continue;
            }
            let mut payload = vec![0u8; chunk.payload_length as usize];
            read_exact_at(&mut self.file, chunk.payload_offset, &mut payload)?;
            ensure!(
                crc32c(&payload) == chunk.payload_crc,
                "chunk payload CRC mismatch"
            );
            match chunk.encoding {
                1 => {
                    ensure!(
                        chunk.payload_length == chunk.cardinality * 2,
                        "invalid array payload length"
                    );
                    let mut previous = None;
                    for pair in payload.chunks_exact(2) {
                        let local = u16::from_le_bytes(pair.try_into().unwrap());
                        ensure!(
                            previous.is_none_or(|value| local > value),
                            "unsorted array container"
                        );
                        let position = (chunk.key << CHUNK_SHIFT) | u64::from(local);
                        ensure!(position < self.universe, "array position outside universe");
                        self.current_positions.push(position);
                        previous = Some(local);
                    }
                }
                2 => {
                    ensure!(
                        chunk.payload_length == 8_448,
                        "invalid bitmap payload length"
                    );
                    let mut cardinality = 0u32;
                    for local in 0..65_536u64 {
                        if payload[256 + local as usize / 8] & (1 << (local % 8)) != 0 {
                            let position = (chunk.key << CHUNK_SHIFT) | local;
                            ensure!(position < self.universe, "bitmap tail bit outside universe");
                            self.current_positions.push(position);
                            cardinality += 1;
                        }
                    }
                    ensure!(
                        cardinality == chunk.cardinality,
                        "bitmap cardinality mismatch"
                    );
                }
                other => bail!("invalid non-empty container encoding {other}"),
            }
            ensure!(
                self.current_positions.len() == chunk.cardinality as usize,
                "container count mismatch"
            );
            return Ok(true);
        }
        ensure!(
            self.yielded == self.layer.member_count,
            "layer iterator count mismatch"
        );
        Ok(false)
    }
}

impl Iterator for ChunkLayerIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.current_positions.len() {
            match self.load_next_container() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(error) => return Some(Err(error)),
            }
        }
        let value = self.current_positions[self.current_index];
        self.current_index += 1;
        self.yielded += 1;
        Some(Ok(value))
    }
}

pub struct EliasFanoLayerIter {
    lower_file: File,
    upper_reader: BufReader<File>,
    header: EfHeader,
    universe: u64,
    ordinal: u64,
    bit_position: u64,
    current_byte: u8,
    bits_left: u8,
}

impl EliasFanoLayerIter {
    fn new(mut file: File, layer: LayerEntry, universe: u64) -> Result<Self> {
        let header = read_ef_header_from(&mut file, layer)?;
        ensure!(header.universe == universe, "Elias-Fano universe mismatch");
        let mut upper_file = file.try_clone()?;
        upper_file.seek(SeekFrom::Start(header.upper_offset))?;
        Ok(Self {
            lower_file: file,
            upper_reader: BufReader::new(upper_file),
            header,
            universe,
            ordinal: 0,
            bit_position: 0,
            current_byte: 0,
            bits_left: 0,
        })
    }

    fn next_one(&mut self) -> Result<u64> {
        while self.bit_position < self.header.upper_bits {
            if self.bits_left == 0 {
                let mut byte = [0u8; 1];
                self.upper_reader.read_exact(&mut byte)?;
                self.current_byte = byte[0];
                self.bits_left = 8;
            }
            let position = self.bit_position;
            let set = self.current_byte & 1 != 0;
            self.current_byte >>= 1;
            self.bits_left -= 1;
            self.bit_position += 1;
            if set {
                return Ok(position);
            }
        }
        bail!("Elias-Fano upper bitmap ended before all members")
    }

    fn lower(&mut self, index: u64) -> Result<u64> {
        if self.header.low_bits == 0 {
            return Ok(0);
        }
        let bit = index * u64::from(self.header.low_bits);
        let byte = bit / 8;
        let shift = (bit % 8) as u32;
        let available = (self.header.lower_length - byte).min(16) as usize;
        let mut bytes = [0u8; 16];
        read_exact_at(
            &mut self.lower_file,
            self.header.lower_offset + byte,
            &mut bytes[..available],
        )?;
        let mask = (1u128 << self.header.low_bits) - 1;
        Ok(((u128::from_le_bytes(bytes) >> shift) & mask) as u64)
    }
}

impl Iterator for EliasFanoLayerIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ordinal >= self.header.members {
            return None;
        }
        let result = (|| -> Result<u64> {
            let upper = self.next_one()?;
            let high = upper - self.ordinal;
            let value = (high << self.header.low_bits) | self.lower(self.ordinal)?;
            ensure!(
                value < self.universe,
                "Elias-Fano iterator value outside universe"
            );
            self.ordinal += 1;
            Ok(value)
        })();
        Some(result)
    }
}

fn read_header(file: &mut File, file_size: u64) -> Result<Header> {
    ensure!(
        file_size >= HEADER_SIZE + FOOTER_SIZE,
        "graph sidecar is truncated"
    );
    let mut bytes = [0u8; HEADER_SIZE as usize];
    read_exact_at(file, 0, &mut bytes)?;
    ensure!(&bytes[0..8] == b"$HDTGRPH", "invalid graph sidecar magic");
    ensure!(
        get_u16(&bytes, 8) == 1,
        "unsupported graph sidecar major version"
    );
    ensure!(
        get_u16(&bytes, 10) == 0,
        "unsupported graph sidecar minor version"
    );
    ensure!(
        get_u32(&bytes, 12) == HEADER_SIZE as u32,
        "invalid graph sidecar header size"
    );
    ensure!(
        crc32c(&bytes[..252]) == get_u32(&bytes, 252),
        "graph sidecar header CRC mismatch"
    );
    ensure!(
        bytes[160..252].iter().all(|byte| *byte == 0),
        "nonzero graph sidecar header reserved bytes"
    );
    let flags = get_u64(&bytes, 16);
    ensure!(flags & !0b111 == 0, "unknown graph sidecar flags");
    ensure!(flags & 1 != 0, "version 1 graph sidecar is not exhaustive");
    ensure!(
        get_u32(&bytes, 120) == 1,
        "unsupported HDT identity algorithm"
    );
    ensure!(
        get_u32(&bytes, 124) == CHUNK_SHIFT,
        "unsupported graph position chunk shift"
    );

    let named_graph_count = get_u64(&bytes, 32);
    let directory_length = get_u64(&bytes, 88);
    let expected_directory_length = named_graph_count
        .checked_add(1)
        .and_then(|count| count.checked_mul(DIRECTORY_ENTRY_SIZE))
        .and_then(|length| length.checked_add(4))
        .context("layer-directory length overflow")?;
    ensure!(
        directory_length == expected_directory_length,
        "invalid layer-directory length"
    );

    let header = Header {
        flags,
        triple_count: get_u64(&bytes, 24),
        named_graph_count,
        membership_count: get_u64(&bytes, 40),
        source_data_length: get_u64(&bytes, 48),
        sidecar_size: get_u64(&bytes, 56),
        dictionary_offset: get_u64(&bytes, 64),
        dictionary_length: get_u64(&bytes, 72),
        directory_offset: get_u64(&bytes, 80),
        directory_length,
        layers_offset: get_u64(&bytes, 96),
        layers_length: get_u64(&bytes, 104),
        footer_offset: get_u64(&bytes, 112),
        source_digest: bytes[128..160].try_into().unwrap(),
        header_crc: get_u32(&bytes, 252),
    };
    ensure!(
        header.sidecar_size == file_size,
        "graph sidecar file size mismatch"
    );
    ensure!(
        header.footer_offset == file_size - FOOTER_SIZE,
        "invalid footer offset"
    );
    ensure!(
        header.dictionary_offset.is_multiple_of(64),
        "unaligned graph dictionary"
    );
    ensure!(
        header.directory_offset.is_multiple_of(64),
        "unaligned layer directory"
    );
    ensure!(
        header.footer_offset.is_multiple_of(64),
        "unaligned graph sidecar footer"
    );
    ensure!(
        header.membership_count >= header.triple_count,
        "graph sidecar membership count is below triple count"
    );
    checked_range(
        header.dictionary_offset,
        header.dictionary_length,
        file_size,
    )?;
    checked_range(header.directory_offset, header.directory_length, file_size)?;
    let dictionary_end = header
        .dictionary_offset
        .checked_add(header.dictionary_length)
        .context("graph dictionary range overflow")?;
    let directory_end = header
        .directory_offset
        .checked_add(header.directory_length)
        .context("layer directory range overflow")?;
    ensure!(
        header.dictionary_offset >= HEADER_SIZE,
        "graph dictionary overlaps header"
    );
    ensure!(
        dictionary_end <= header.directory_offset,
        "graph dictionary overlaps layer directory"
    );
    ensure!(
        directory_end <= header.footer_offset,
        "layer directory overlaps footer"
    );
    if header.layers_length != 0 {
        ensure!(
            header.layers_offset.is_multiple_of(64),
            "unaligned layers offset"
        );
        ensure!(
            directory_end <= header.layers_offset,
            "layer data overlaps directory"
        );
        checked_range(
            header.layers_offset,
            header.layers_length,
            header.footer_offset,
        )?;
    } else {
        ensure!(
            header.layers_offset == header.footer_offset,
            "empty layers offset must point to footer"
        );
    }

    let mut footer = [0u8; FOOTER_SIZE as usize];
    read_exact_at(file, header.footer_offset, &mut footer)?;
    ensure!(
        &footer[0..8] == b"$HDTGEND",
        "invalid graph sidecar footer magic"
    );
    ensure!(
        get_u16(&footer, 8) == 1 && get_u16(&footer, 10) == 0,
        "footer version mismatch"
    );
    ensure!(
        get_u32(&footer, 12) == FOOTER_SIZE as u32,
        "invalid footer size"
    );
    ensure!(
        get_u64(&footer, 16) == file_size,
        "footer file size mismatch"
    );
    ensure!(get_u64(&footer, 24) == 0, "invalid footer header offset");
    ensure!(
        get_u64(&footer, 32) == header.directory_offset,
        "footer directory offset mismatch"
    );
    ensure!(
        get_u64(&footer, 40) == header.directory_length,
        "footer directory length mismatch"
    );
    ensure!(get_u64(&footer, 48) == 0, "nonzero footer reserved bytes");
    ensure!(
        get_u32(&footer, 56) == header.header_crc,
        "footer header CRC copy mismatch"
    );
    ensure!(
        crc32c(&footer[..60]) == get_u32(&footer, 60),
        "footer CRC mismatch"
    );
    Ok(header)
}

fn validate_packed_tail(file: &mut File, array: DiskLogArray) -> Result<()> {
    let used_bits = array
        .count
        .checked_mul(u64::from(array.bits))
        .context("LogArray bit length overflow")?;
    validate_tail_bits(file, array.data_offset, array.data_length, used_bits)
}

fn validate_tail_bits(file: &mut File, offset: u64, length: u64, used_bits: u64) -> Result<()> {
    ensure!(
        used_bits
            <= length
                .checked_mul(8)
                .context("packed region size overflow")?,
        "packed region is too short"
    );
    if length == 0 || used_bits.is_multiple_of(8) {
        return Ok(());
    }
    let last = read_u8_at(file, offset + length - 1)?;
    let used_in_last = (used_bits % 8) as u8;
    let tail_mask = !((1u8 << used_in_last) - 1);
    ensure!(last & tail_mask == 0, "nonzero packed tail bits");
    Ok(())
}

fn read_graph_dictionary(file: &mut File, header: Header) -> Result<GraphDictionary> {
    file.seek(SeekFrom::Start(header.dictionary_offset))?;
    let mut preamble = Vec::new();
    let mut section_type = [0u8; 1];
    file.read_exact(&mut section_type)?;
    ensure!(section_type[0] == 0x02, "invalid graph PFC section type");
    preamble.push(section_type[0]);
    let string_count = read_vbyte(file)?;
    preamble.extend_from_slice(&encode_vbyte(string_count));
    let buffer_length = read_vbyte(file)?;
    preamble.extend_from_slice(&encode_vbyte(buffer_length));
    let block_size = read_vbyte(file)?;
    preamble.extend_from_slice(&encode_vbyte(block_size));
    ensure!(
        block_size == 16,
        "version 1 graph PFC block size must be 16"
    );
    let mut checksum = [0u8; 1];
    file.read_exact(&mut checksum)?;
    ensure!(
        checksum[0] == crc8(&preamble),
        "graph PFC preamble CRC mismatch"
    );
    ensure!(
        string_count == header.named_graph_count,
        "graph dictionary count mismatch"
    );

    let offsets = read_disk_log_array(file)?;
    let expected_blocks = if string_count == 0 {
        0
    } else {
        string_count.div_ceil(block_size)
    };
    ensure!(
        offsets.count == expected_blocks + 1,
        "graph PFC offset count mismatch"
    );
    let buffer_offset = file.stream_position()?;
    let section_end = buffer_offset
        .checked_add(buffer_length)
        .and_then(|value| value.checked_add(4))
        .context("graph dictionary end overflow")?;
    ensure!(
        section_end == header.dictionary_offset + header.dictionary_length,
        "graph dictionary length mismatch"
    );
    let buffer_crc = read_u32_at(file, buffer_offset + buffer_length)?;
    let dictionary = GraphDictionary {
        string_count,
        block_size,
        buffer_offset,
        buffer_length,
        buffer_crc,
        offsets,
    };
    ensure!(
        disk_log_get(file, offsets, offsets.count - 1)? == buffer_length,
        "graph PFC sentinel mismatch"
    );
    Ok(dictionary)
}

fn read_disk_log_array(file: &mut File) -> Result<DiskLogArray> {
    let mut preamble = Vec::new();
    let mut fixed = [0u8; 2];
    file.read_exact(&mut fixed)?;
    ensure!(fixed[0] == 1, "invalid graph PFC LogArray type");
    ensure!(fixed[1] <= 64, "invalid graph PFC LogArray bit width");
    preamble.extend_from_slice(&fixed);
    let count = read_vbyte(file)?;
    preamble.extend_from_slice(&encode_vbyte(count));
    let mut stored_crc8 = [0u8; 1];
    file.read_exact(&mut stored_crc8)?;
    ensure!(
        stored_crc8[0] == crc8(&preamble),
        "graph PFC LogArray preamble CRC mismatch"
    );
    let data_offset = file.stream_position()?;
    let data_length = count
        .checked_mul(u64::from(fixed[1]))
        .map(|bits| bits.div_ceil(8))
        .context("graph PFC LogArray size overflow")?;
    file.seek(SeekFrom::Start(data_offset + data_length))?;
    let mut crc = [0u8; 4];
    file.read_exact(&mut crc)?;
    Ok(DiskLogArray {
        bits: fixed[1],
        count,
        data_offset,
        data_length,
        stored_crc: u32::from_le_bytes(crc),
    })
}

fn disk_log_get(file: &mut File, array: DiskLogArray, index: u64) -> Result<u64> {
    ensure!(index < array.count, "LogArray index out of range");
    if array.bits == 0 {
        return Ok(0);
    }
    let bit = index * u64::from(array.bits);
    let byte = bit / 8;
    let shift = (bit % 8) as u32;
    let available = (array.data_length - byte).min(16) as usize;
    let mut bytes = [0u8; 16];
    read_exact_at(file, array.data_offset + byte, &mut bytes[..available])?;
    let mask = if array.bits == 64 {
        u128::from(u64::MAX)
    } else {
        (1u128 << array.bits) - 1
    };
    Ok(((u128::from_le_bytes(bytes) >> shift) & mask) as u64)
}

fn decode_pfc_block(bytes: &[u8], count: u64) -> Result<Vec<String>> {
    let mut output = Vec::with_capacity(count as usize);
    let mut position = 0usize;
    let mut previous = Vec::new();
    for index in 0..count {
        let value = if index == 0 {
            let end = bytes[position..]
                .iter()
                .position(|byte| *byte == 0)
                .context("unterminated graph PFC base string")?
                + position;
            let value = bytes[position..end].to_vec();
            position = end + 1;
            value
        } else {
            let (shared, consumed) = decode_vbyte(&bytes[position..])?;
            position += consumed;
            let shared = usize::try_from(shared).context("graph PFC prefix too large")?;
            ensure!(shared <= previous.len(), "invalid graph PFC shared prefix");
            let end = bytes[position..]
                .iter()
                .position(|byte| *byte == 0)
                .context("unterminated graph PFC suffix")?
                + position;
            let mut value = previous[..shared].to_vec();
            value.extend_from_slice(&bytes[position..end]);
            position = end + 1;
            value
        };
        let text =
            String::from_utf8(value.clone()).context("graph dictionary term is not UTF-8")?;
        previous = value;
        output.push(text);
    }
    ensure!(position == bytes.len(), "trailing bytes in graph PFC block");
    Ok(output)
}

fn hdt_metadata(path: &Path) -> Result<(u64, u64, u64)> {
    let file =
        File::open(path).with_context(|| format!("Failed to open HDT {}", path.display()))?;
    let file_length = file.metadata()?.len();
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    ControlInfo::read_from(&mut reader).context("Failed to read HDT global control info")?;
    let header =
        ControlInfo::read_from(&mut reader).context("Failed to read HDT header control info")?;
    let header_length: usize = header
        .get_property("length")
        .context("HDT header missing length")?
        .parse()
        .context("Invalid HDT header length")?;
    let mut content = vec![0u8; header_length];
    reader.read_exact(&mut content)?;
    let triple_count = parse_hdt_triple_count(&content)?;
    let data_offset = reader.stream_position()?;
    ensure!(data_offset <= file_length, "HDT data offset outside file");
    Ok((data_offset, file_length - data_offset, triple_count))
}

fn parse_hdt_triple_count(header: &[u8]) -> Result<u64> {
    const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";
    const HDT_TRIPLES: &str = "http://purl.org/HDT/hdt#triplesnumTriples";
    let parser = RdfParser::from_format(RdfFormat::NTriples).for_reader(Cursor::new(header));
    let mut value = None;
    for result in parser {
        let quad = result.context("Invalid HDT header N-Triples")?;
        if quad.predicate.as_str() != VOID_TRIPLES && quad.predicate.as_str() != HDT_TRIPLES {
            continue;
        }
        let oxrdf::Term::Literal(literal) = quad.object else {
            continue;
        };
        let parsed = literal
            .value()
            .parse::<u64>()
            .context("Invalid HDT triple count")?;
        if let Some(previous) = value {
            ensure!(previous == parsed, "Conflicting HDT header triple counts");
        }
        value = Some(parsed);
    }
    value.context("HDT header has no triple count")
}

fn read_ef_header_from(file: &mut File, layer: LayerEntry) -> Result<EfHeader> {
    ensure!(
        layer.primary_length == 160,
        "invalid Elias-Fano primary length"
    );
    let mut bytes = [0u8; 160];
    read_exact_at(file, layer.primary_offset, &mut bytes)?;
    ensure!(&bytes[0..8] == b"$HDTEF01", "invalid Elias-Fano magic");
    ensure!(get_u32(&bytes, 8) == 160, "invalid Elias-Fano header size");
    ensure!(
        crc32c(&bytes[..156]) == get_u32(&bytes, 156),
        "Elias-Fano header CRC mismatch"
    );
    ensure!(
        bytes[144..156].iter().all(|byte| *byte == 0),
        "nonzero Elias-Fano header reserved bytes"
    );
    Ok(EfHeader {
        low_bits: get_u32(&bytes, 12),
        universe: get_u64(&bytes, 16),
        members: get_u64(&bytes, 24),
        upper_bits: get_u64(&bytes, 32),
        high_buckets: get_u64(&bytes, 40),
        lower_offset: get_u64(&bytes, 48),
        lower_length: get_u64(&bytes, 56),
        superrank_offset: get_u64(&bytes, 64),
        superrank_length: get_u64(&bytes, 72),
        subrank_offset: get_u64(&bytes, 80),
        subrank_length: get_u64(&bytes, 88),
        upper_offset: get_u64(&bytes, 96),
        upper_length: get_u64(&bytes, 104),
        superrank_count: get_u64(&bytes, 112),
        subrank_count: get_u64(&bytes, 120),
        lower_crc: get_u32(&bytes, 128),
        superrank_crc: get_u32(&bytes, 132),
        subrank_crc: get_u32(&bytes, 136),
        upper_crc: get_u32(&bytes, 140),
    })
}

fn read_exact_at(file: &mut File, offset: u64, bytes: &mut [u8]) -> Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(bytes)?;
    Ok(())
}

fn read_u8_at(file: &mut File, offset: u64) -> Result<u8> {
    let mut bytes = [0u8; 1];
    read_exact_at(file, offset, &mut bytes)?;
    Ok(bytes[0])
}

fn read_u16_at(file: &mut File, offset: u64) -> Result<u16> {
    let mut bytes = [0u8; 2];
    read_exact_at(file, offset, &mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32_at(file: &mut File, offset: u64) -> Result<u32> {
    let mut bytes = [0u8; 4];
    read_exact_at(file, offset, &mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64_at(file: &mut File, offset: u64) -> Result<u64> {
    let mut bytes = [0u8; 8];
    read_exact_at(file, offset, &mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn checked_range(offset: u64, length: u64, bound: u64) -> Result<()> {
    let end = offset
        .checked_add(length)
        .context("section range overflow")?;
    ensure!(end <= bound, "sidecar section exceeds file bounds");
    Ok(())
}

fn range_crc(file: &mut File, offset: u64, length: u64) -> Result<u32> {
    file.seek(SeekFrom::Start(offset))?;
    let mut remaining = length;
    let mut digest = CRC32C_ALGO.digest();
    let mut bytes = [0u8; 64 * 1024];
    while remaining > 0 {
        let amount = usize::try_from(remaining.min(bytes.len() as u64)).unwrap();
        file.read_exact(&mut bytes[..amount])?;
        digest.update(&bytes[..amount]);
        remaining -= amount as u64;
    }
    Ok(digest.finalize())
}

fn select_in_word(mut word: u64, mut ordinal: u64) -> Result<u32> {
    ensure!(
        ordinal < u64::from(word.count_ones()),
        "word select out of range"
    );
    loop {
        let bit = word.trailing_zeros();
        if ordinal == 0 {
            return Ok(bit);
        }
        word &= word - 1;
        ordinal -= 1;
    }
}

fn mix64(chunk_key: u64) -> u64 {
    let mut value = chunk_key.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dictionary::pfc::StreamingPfcEncoder;
    use crate::io::{ControlInfo, ControlType};
    use crate::quads::GraphMembership;
    use crate::quads::writer::write_graph_sidecar;
    use crate::sort::Sortable;
    use std::io::BufWriter;

    fn fake_hdt(path: &Path, triples: u64) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        ControlInfo::new(ControlType::Global, "<http://purl.org/HDT/hdt#HDTv1>")
            .write_to(&mut writer)?;
        let header_text = format!(
            "<urn:dataset> <http://rdfs.org/ns/void#triples> \"{triples}\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n"
        );
        let mut header = ControlInfo::new(
            ControlType::Header,
            "<http://purl.org/HDT/hdt#headerNtriples>",
        );
        header.set_property("length", header_text.len().to_string());
        header.write_to(&mut writer)?;
        writer.write_all(header_text.as_bytes())?;
        writer.write_all(b"dictionary-and-triples-test-suffix")?;
        writer.flush()?;
        Ok(())
    }

    #[test]
    fn reader_operations_cover_array_and_bitmap_boundaries() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let hdt = temp.path().join("data.hdt");
        let sidecar = canonical_sidecar_path(&hdt);
        let memberships = temp.path().join("memberships.zst");
        const N: u64 = 65_536;
        fake_hdt(&hdt, N)?;

        let mut graph_dictionary = StreamingPfcEncoder::new(temp.path(), "reader-graphs")?;
        graph_dictionary.push("urn:g1")?;
        graph_dictionary.push("urn:g2")?;
        let graph_dictionary = graph_dictionary.finish()?;

        let membership_file = File::create(&memberships)?;
        let mut encoder = zstd::Encoder::new(BufWriter::new(membership_file), 1)?;
        for position in 0..4096 {
            GraphMembership { graph: 0, position }.write_to(&mut encoder)?;
        }
        for position in 0..4097 {
            GraphMembership { graph: 1, position }.write_to(&mut encoder)?;
        }
        for position in 0..N {
            GraphMembership { graph: 2, position }.write_to(&mut encoder)?;
        }
        encoder.finish()?;

        write_graph_sidecar(
            &sidecar,
            &hdt,
            &graph_dictionary.path,
            graph_dictionary.size,
            2,
            N,
            N + 4096 + 4097,
            &memberships,
            false,
            temp.path(),
        )?;

        let mut reader = GraphSidecarReader::open_for_hdt(&hdt)?;
        assert_eq!(reader.graph(0)?, GraphTerm::DefaultGraph);
        assert_eq!(reader.graph(1)?, GraphTerm::Named("urn:g1".into()));
        assert_eq!(reader.graph_id_str("urn:g2")?, Some(2));
        assert_eq!(reader.graph_id(&GraphTerm::DefaultGraph)?, Some(0));
        assert_eq!(reader.count(0)?, 4096);
        assert!(reader.access(0, 4095)?);
        assert!(!reader.access(0, 4096)?);
        assert_eq!(reader.rank(1, 4096)?, 4096);
        assert_eq!(reader.rank(1, N)?, 4097);
        assert_eq!(reader.select(1, 4096)?, 4096);
        assert_eq!(reader.next_member(0, 4095)?, Some(4095));
        assert_eq!(reader.next_member(0, 4096)?, None);
        assert_eq!(reader.graphs_of(4096)?, vec![1, 2]);
        assert_eq!(reader.graphs_of(5000)?, vec![2]);
        reader.validate_strict(temp.path(), 1024 * 1024, None)?;
        Ok(())
    }

    /// Exercise the Elias-Fano and sparse-chunk layer encodings, which the
    /// dense fixture above never selects.
    ///
    /// `choose_encoding` only reaches Elias-Fano below 1/64 density, and only
    /// reaches sparse chunks when the non-empty chunks plus access hash are
    /// smaller than a full dense directory, so both layers are shaped to land
    /// on those branches.
    #[test]
    fn reader_operations_cover_elias_fano_and_sparse_layers() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let hdt = temp.path().join("data.hdt");
        let sidecar = canonical_sidecar_path(&hdt);
        let memberships = temp.path().join("memberships.zst");
        const N: u64 = 1_000_000;
        fake_hdt(&hdt, N)?;

        // Scattered across several 65,536-position chunks and both sides of a
        // chunk boundary, including the last position in the universe.
        let sparse_positions: Vec<u64> = vec![
            0,
            1,
            65_535,
            65_536,
            65_537,
            131_072,
            200_000,
            499_999,
            500_000,
            999_999,
        ];
        // One dense run inside a single chunk: sparse chunk table + bitmap
        // container, far above the Elias-Fano density threshold.
        let clustered: Vec<u64> = (0..20_000u64).collect();

        let mut graph_dictionary = StreamingPfcEncoder::new(temp.path(), "ef-graphs")?;
        graph_dictionary.push("urn:ef")?;
        graph_dictionary.push("urn:sparse")?;
        let graph_dictionary = graph_dictionary.finish()?;

        let membership_file = File::create(&memberships)?;
        let mut encoder = zstd::Encoder::new(BufWriter::new(membership_file), 1)?;
        for position in 0..N {
            GraphMembership { graph: 0, position }.write_to(&mut encoder)?;
        }
        for &position in &sparse_positions {
            GraphMembership { graph: 1, position }.write_to(&mut encoder)?;
        }
        for &position in &clustered {
            GraphMembership { graph: 2, position }.write_to(&mut encoder)?;
        }
        encoder.finish()?;

        let expected_memberships = N + sparse_positions.len() as u64 + clustered.len() as u64;
        write_graph_sidecar(
            &sidecar,
            &hdt,
            &graph_dictionary.path,
            graph_dictionary.size,
            2,
            N,
            expected_memberships,
            &memberships,
            false,
            temp.path(),
        )?;

        let mut reader = GraphSidecarReader::open_for_hdt(&hdt)?;
        assert_eq!(reader.layer_entry(1)?.encoding, ENCODING_ELIAS_FANO);
        assert_eq!(reader.layer_entry(2)?.encoding, ENCODING_SPARSE);
        assert_eq!(reader.layer_entry(0)?.encoding, ENCODING_DENSE);

        // Elias-Fano: iteration, select, rank and access must agree.
        let iterated = reader.layer_iter(1)?.collect::<Result<Vec<_>>>()?;
        assert_eq!(iterated, sparse_positions);
        assert_eq!(reader.count(1)?, sparse_positions.len() as u64);
        for (ordinal, &position) in sparse_positions.iter().enumerate() {
            assert_eq!(reader.select(1, ordinal as u64)?, position);
            assert_eq!(reader.rank(1, position)?, ordinal as u64);
            assert_eq!(reader.rank(1, position + 1)?, ordinal as u64 + 1);
            assert!(reader.access(1, position)?);
            assert_eq!(reader.next_member(1, position)?, Some(position));
        }
        assert_eq!(reader.rank(1, 0)?, 0);
        assert_eq!(reader.rank(1, N)?, sparse_positions.len() as u64);
        for probe in [2u64, 65_538, 300_000, 999_998] {
            assert!(!reader.access(1, probe)?, "unexpected member at {probe}");
        }
        assert_eq!(reader.next_member(1, 2)?, Some(65_535));
        assert_eq!(reader.next_member(1, 500_001)?, Some(999_999));
        assert!(reader.select(1, sparse_positions.len() as u64).is_err());

        // Sparse chunk table: access probes go through the on-disk hash.
        assert_eq!(reader.count(2)?, clustered.len() as u64);
        assert!(reader.access(2, 0)?);
        assert!(reader.access(2, 19_999)?);
        assert!(!reader.access(2, 20_000)?);
        assert!(!reader.access(2, 65_536)?);
        assert_eq!(reader.rank(2, 20_000)?, 20_000);
        assert_eq!(reader.select(2, 19_999)?, 19_999);
        assert_eq!(reader.layer_iter(2)?.collect::<Result<Vec<_>>>()?, clustered);

        assert_eq!(reader.graphs_of(0)?, vec![0, 1, 2]);
        assert_eq!(reader.graphs_of(65_535)?, vec![0, 1]);
        assert_eq!(reader.graphs_of(300_000)?, vec![0]);

        reader.validate_strict(temp.path(), 4 * 1024 * 1024, None)?;
        Ok(())
    }
}
