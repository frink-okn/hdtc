use crate::dictionary::DictCounts;
use crate::hdt::artifacts::SourceIdentity;
use crate::hdt::reader::{BitmapTriplesScanner, sha256_to_end};
use crate::io::crc_utils::{CRC32C_ALGO, crc32c};
use crate::io::{StreamingBitmapEncoder, StreamingLogArrayEncoder};
use crate::permutation::{
    PermEntry, PermutationCollector, PositionMaps, PreparedPermutationAssembler, scan_hdt,
};
use crate::quads::writer::encode_layer_set;
use crate::quads::{
    GraphMembership, GraphSidecarReader, LayerMemberIter, PositionGraphMembership,
    canonical_sidecar_path,
};
use crate::sort::{ExternalSorter, MergeIterator, Sortable};
use crate::triples::BitmapTriplesFiles;
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result, ensure};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tempfile::TempPath;

const HEADER_SIZE: u64 = 256;
const FOOTER_SIZE: u64 = 64;
const DIRECTORY_ENTRY_SIZE: u64 = 64;
const SUPERBLOCK_BITS: u32 = 4096;
const SUBBLOCK_BITS: u32 = 512;
const POSITION_CHUNK_SHIFT: u32 = 16;
const REQUIRED: u32 = 1;

const POS_DIRECTORY: u32 = 0x0101;
const POS_REGION: u32 = 0x0102;
const OPS_DIRECTORY: u32 = 0x0201;
const OPS_REGION: u32 = 0x0202;
const TRANSPOSE_ARRAY: u32 = 0x0301;
const TRANSPOSE_BITMAP: u32 = 0x0302;
const TRANSPOSE_SUPER: u32 = 0x0303;
const TRANSPOSE_SUB: u32 = 0x0304;

const HAS_POS_LAYERS: u64 = 1 << 0;
const HAS_OPS_LAYERS: u64 = 1 << 1;
const HAS_MEMBERSHIP_RANKS: u64 = 1 << 2;
const HAS_MEMBERSHIP_IDS: u64 = 1 << 3;
const DIRECT_PREPARED_LAYER_LIMIT: u64 = 128;
const DIRECT_PREPARED_LAYER_BUFFER: usize = 64 * 1024;
const DIRECT_PREPARED_LAYER_ESTIMATED_BYTES: usize = 2 * 1024 * 1024;
const LAYER_MERGE_LIMIT: u64 = 128;
// A chunked layer decodes one 2^POSITION_CHUNK_SHIFT container at a time, so a
// fully dense container is the resident cost of holding one layer iterator.
const LAYER_MERGE_ESTIMATED_BYTES: usize = (1 << POSITION_CHUNK_SHIFT) * 8;
// Memberships per triple past which repeating a triple's key once per
// membership costs more than the extra sorts the mapping strategy needs.
const DECORATED_MULTIPLICITY_LIMIT: u64 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphIndexOptions {
    pub pos_layers: bool,
    pub ops_layers: bool,
    pub membership_ranks: bool,
    pub membership_ids: bool,
}

impl Default for GraphIndexOptions {
    fn default() -> Self {
        Self {
            pos_layers: true,
            ops_layers: true,
            membership_ranks: false,
            membership_ids: false,
        }
    }
}

pub fn canonical_path(hdt_path: &Path) -> PathBuf {
    let mut name: OsString = canonical_sidecar_path(hdt_path).into_os_string();
    name.push(".idx");
    PathBuf::from(name)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SpoPositionMap {
    spo_position: u64,
    permuted_position: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedGraphEntry {
    first: u64,
    second: u64,
    third: u64,
    graph: u64,
    spo_position: u64,
}

impl Ord for PreparedGraphEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.first
            .cmp(&other.first)
            .then(self.second.cmp(&other.second))
            .then(self.third.cmp(&other.third))
            .then(self.graph.cmp(&other.graph))
            .then(self.spo_position.cmp(&other.spo_position))
    }
}

impl PartialOrd for PreparedGraphEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Sortable for PreparedGraphEntry {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        for value in [
            self.first,
            self.second,
            self.third,
            self.graph,
            self.spo_position,
        ] {
            writer.write_all(&value.to_le_bytes())?;
        }
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut bytes = [0u8; 40];
        match reader.read_exact(&mut bytes) {
            Ok(()) => Ok(Some(Self {
                first: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                second: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                third: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                graph: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
                spo_position: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            })),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn mem_size(&self) -> usize {
        40
    }
}

/// POS/OPS sort runs decorated with graph IDs while the create pipeline still
/// has each unique SPO triple and all of its memberships together.
pub struct PreparedGraphIndexCollector {
    pos_sorter: ExternalSorter,
    ops_sorter: ExternalSorter,
    pos_buffer: Vec<PreparedGraphEntry>,
    ops_buffer: Vec<PreparedGraphEntry>,
    pos_memory: usize,
    ops_memory: usize,
    membership_count: u64,
    triple_count: u64,
    last_position: Option<u64>,
    last_graph: Option<u64>,
    last_triple: Option<IdTriple>,
    collect_pos: bool,
    collect_ops: bool,
}

impl PreparedGraphIndexCollector {
    pub fn new(temp_dir: &Path, memory_budget: usize) -> Self {
        Self::for_spaces(temp_dir, memory_budget, true, true)
    }

    /// Collect only the requested position spaces. A space that is not
    /// collected never sees a record, so a single-space caller neither spills
    /// runs it will not merge nor gives up half its sort budget to them.
    pub(crate) fn for_spaces(
        temp_dir: &Path,
        memory_budget: usize,
        collect_pos: bool,
        collect_ops: bool,
    ) -> Self {
        let (pos_budget, ops_budget) = match (collect_pos, collect_ops) {
            (true, true) => {
                let pos_budget = (memory_budget / 2).max(1);
                (pos_budget, memory_budget.saturating_sub(pos_budget).max(1))
            }
            _ => (memory_budget.max(1), memory_budget.max(1)),
        };
        Self {
            pos_sorter: ExternalSorter::new(temp_dir, pos_budget),
            ops_sorter: ExternalSorter::new(temp_dir, ops_budget),
            pos_buffer: Vec::new(),
            ops_buffer: Vec::new(),
            pos_memory: 0,
            ops_memory: 0,
            membership_count: 0,
            triple_count: 0,
            last_position: None,
            last_graph: None,
            last_triple: None,
            collect_pos,
            collect_ops,
        }
    }

    pub fn push(&mut self, triple: IdTriple, membership: GraphMembership) -> Result<()> {
        if self.last_position != Some(membership.position) {
            ensure!(
                self.last_position
                    .map_or(membership.position == 0, |before| {
                        before.checked_add(1) == Some(membership.position)
                    }),
                "prepared graph memberships skip an SPO position"
            );
            self.triple_count = self
                .triple_count
                .checked_add(1)
                .context("prepared graph-index triple count overflow")?;
            self.last_position = Some(membership.position);
            self.last_graph = None;
            self.last_triple = Some(triple);
        } else {
            ensure!(
                self.last_triple == Some(triple),
                "one SPO position contains different triples"
            );
        }
        ensure!(
            self.last_graph
                .is_none_or(|before| membership.graph > before),
            "prepared graph IDs are not strictly increasing within an SPO position"
        );
        self.last_graph = Some(membership.graph);

        if self.collect_pos {
            self.pos_sorter.push(
                PreparedGraphEntry {
                    first: triple.predicate,
                    second: triple.object,
                    third: triple.subject,
                    graph: membership.graph,
                    spo_position: membership.position,
                },
                &mut self.pos_buffer,
                &mut self.pos_memory,
            )?;
        }
        if self.collect_ops {
            self.ops_sorter.push(
                PreparedGraphEntry {
                    first: triple.object,
                    second: triple.predicate,
                    third: triple.subject,
                    graph: membership.graph,
                    spo_position: membership.position,
                },
                &mut self.ops_buffer,
                &mut self.ops_memory,
            )?;
        }
        self.membership_count = self
            .membership_count
            .checked_add(1)
            .context("prepared graph-index membership count overflow")?;
        Ok(())
    }
}

/// Position-major memberships produced by merging the sidecar's layers.
///
/// Every layer is already strictly increasing in position, so transposing the
/// sidecar is a k-way merge rather than a sort of all memberships: nothing
/// spills, and each layer contributes one buffered iterator.
struct LayerMergeIter {
    layers: Vec<LayerMemberIter>,
    heads: BinaryHeap<Reverse<(u64, usize)>>,
}

impl LayerMergeIter {
    fn new(sidecar: &mut GraphSidecarReader, named_graphs: u64) -> Result<Self> {
        let count = usize::try_from(
            named_graphs
                .checked_add(1)
                .context("layer-merge layer count overflow")?,
        )
        .context("layer-merge layer count overflow")?;
        let mut layers = Vec::with_capacity(count);
        let mut heads = BinaryHeap::with_capacity(count);
        for graph in 0..=named_graphs {
            let mut layer = sidecar.layer_iter(graph)?;
            if let Some(position) = layer.next().transpose()? {
                heads.push(Reverse((position, layers.len())));
            }
            layers.push(layer);
        }
        Ok(Self { layers, heads })
    }
}

impl Iterator for LayerMergeIter {
    type Item = Result<PositionGraphMembership>;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse((position, index)) = self.heads.pop()?;
        match self.layers[index].next().transpose() {
            Ok(Some(next)) => self.heads.push(Reverse((next, index))),
            Ok(None) => {}
            Err(error) => return Some(Err(error)),
        }
        Some(Ok(PositionGraphMembership {
            position,
            graph: index as u64,
        }))
    }
}

/// The sidecar's memberships in `(position, graph)` order.
///
/// A bounded graph dictionary merges its layers directly. Past that the layers
/// no longer fit as concurrent iterators, so the memberships go through the
/// bounded external sort instead.
enum PositionMajorMemberships {
    Merged(LayerMergeIter),
    // The sorter owns the chunk files its iterator reads, so it has to outlive
    // the merge it handed out.
    Sorted {
        // Owns the chunk files its iterator reads, so it outlives the merge.
        _sorter: ExternalSorter,
        sorted: MergeIterator<PositionGraphMembership>,
    },
}

/// Bytes the k-way layer merge holds resident, or `None` if it does not apply.
///
/// The merge keeps one buffered iterator per layer, so its cost is known up
/// front rather than budgeted as a fraction. Reporting it lets the caller hand
/// the rest of the budget to the POS/OPS sorts, which is where it does work.
fn layer_merge_reserve(layer_count: u64, memory_budget: usize) -> Option<usize> {
    if layer_count > LAYER_MERGE_LIMIT {
        return None;
    }
    let reserve = layer_count
        .checked_mul(LAYER_MERGE_ESTIMATED_BYTES as u64)
        .and_then(|bytes| usize::try_from(bytes).ok())?;
    (reserve <= memory_budget / 2).then_some(reserve)
}

impl PositionMajorMemberships {
    fn open(
        sidecar: &mut GraphSidecarReader,
        named_graphs: u64,
        temp_dir: &Path,
        memory_budget: usize,
        merge: bool,
    ) -> Result<Self> {
        let layer_count = named_graphs
            .checked_add(1)
            .context("layer-merge layer count overflow")?;
        if merge {
            tracing::info!(layer_count, "Transposing graph layers by k-way merge");
            return Ok(Self::Merged(LayerMergeIter::new(sidecar, named_graphs)?));
        }
        tracing::info!(
            layer_count,
            "Graph count exceeds layer-merge resource limit; transposing by external sort"
        );
        let mut sorter = ExternalSorter::new(temp_dir, memory_budget.max(1));
        let mut buffer = Vec::new();
        let mut memory = 0usize;
        for graph in 0..=named_graphs {
            for position in sidecar.layer_iter(graph)? {
                sorter.push(
                    PositionGraphMembership {
                        position: position?,
                        graph,
                    },
                    &mut buffer,
                    &mut memory,
                )?;
            }
        }
        let sorted = sorter.finish(&mut buffer)?;
        Ok(Self::Sorted {
            _sorter: sorter,
            sorted,
        })
    }
}

impl Iterator for PositionMajorMemberships {
    type Item = Result<PositionGraphMembership>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Merged(inner) => inner.next(),
            Self::Sorted { sorted, .. } => sorted.next(),
        }
    }
}

type PreparedMembershipEncoder = zstd::Encoder<'static, BufWriter<File>>;

struct DirectPreparedLayerSpool {
    temp_dir: PathBuf,
    encoders: Vec<Option<PreparedMembershipEncoder>>,
    last_positions: Vec<Option<u64>>,
    count: u64,
}

impl DirectPreparedLayerSpool {
    fn new(temp_dir: &Path, layer_count: u64) -> Result<Self> {
        let layer_count = usize::try_from(layer_count).context("graph layer count overflow")?;
        Ok(Self {
            temp_dir: temp_dir.to_path_buf(),
            encoders: std::iter::repeat_with(|| None).take(layer_count).collect(),
            last_positions: vec![None; layer_count],
            count: 0,
        })
    }

    fn push(&mut self, membership: GraphMembership) -> Result<()> {
        let graph = usize::try_from(membership.graph).context("graph ID overflow")?;
        let previous = self
            .last_positions
            .get_mut(graph)
            .context("graph ID exceeds prepared layer count")?;
        ensure!(
            previous.is_none_or(|value| membership.position > value),
            "prepared layer positions are not strictly increasing"
        );
        *previous = Some(membership.position);
        if self.encoders[graph].is_none() {
            let file = tempfile::tempfile_in(&self.temp_dir)?;
            self.encoders[graph] = Some(zstd::Encoder::new(
                BufWriter::with_capacity(DIRECT_PREPARED_LAYER_BUFFER, file),
                1,
            )?);
        }
        membership.write_to(
            self.encoders[graph]
                .as_mut()
                .context("prepared layer encoder is absent")?,
        )?;
        self.count = self
            .count
            .checked_add(1)
            .context("prepared layer membership count overflow")?;
        Ok(())
    }

    fn finish(self, temp_dir: &Path) -> Result<TempPath> {
        let output = tempfile::Builder::new()
            .prefix(".hdtc-prepared-graph-memberships-")
            .tempfile_in(temp_dir)?
            .into_temp_path();
        let file = File::create(&output)?;
        if self.count == 0 {
            zstd::Encoder::new(BufWriter::new(file), 1)?.finish()?;
            return Ok(output);
        }
        let mut writer = BufWriter::with_capacity(256 * 1024, file);
        for encoder in self.encoders.into_iter().flatten() {
            let mut frame = encoder.finish()?;
            frame.flush()?;
            let mut frame = frame.into_inner().map_err(|error| error.into_error())?;
            frame.seek(SeekFrom::Start(0))?;
            std::io::copy(&mut frame, &mut writer)?;
        }
        writer.flush()?;
        Ok(output)
    }
}

struct SortedPreparedLayerSpool {
    sorter: ExternalSorter,
    buffer: Vec<GraphMembership>,
    memory: usize,
    count: u64,
}

enum PreparedLayerSpool {
    Direct(DirectPreparedLayerSpool),
    Sorted(SortedPreparedLayerSpool),
}

impl PreparedLayerSpool {
    fn new(temp_dir: &Path, layer_count: u64, memory_budget: usize) -> Result<Self> {
        let memory_limit =
            u64::try_from((memory_budget / DIRECT_PREPARED_LAYER_ESTIMATED_BYTES).max(1))
                .unwrap_or(u64::MAX);
        let direct_layer_limit = DIRECT_PREPARED_LAYER_LIMIT.min(memory_limit);
        if layer_count <= direct_layer_limit {
            tracing::info!(
                layer_count,
                "Spooling integrated graph-index memberships directly by layer"
            );
            Ok(Self::Direct(DirectPreparedLayerSpool::new(
                temp_dir,
                layer_count,
            )?))
        } else {
            tracing::info!(
                layer_count,
                direct_layer_limit,
                "Graph count exceeds integrated direct-spool resource limit; using external membership sort"
            );
            Ok(Self::Sorted(SortedPreparedLayerSpool {
                sorter: ExternalSorter::new(temp_dir, memory_budget.max(1)),
                buffer: Vec::new(),
                memory: 0,
                count: 0,
            }))
        }
    }

    fn push(&mut self, membership: GraphMembership) -> Result<()> {
        match self {
            Self::Direct(spool) => spool.push(membership),
            Self::Sorted(spool) => {
                spool
                    .sorter
                    .push(membership, &mut spool.buffer, &mut spool.memory)?;
                spool.count = spool
                    .count
                    .checked_add(1)
                    .context("prepared layer membership count overflow")?;
                Ok(())
            }
        }
    }

    fn finish(self, temp_dir: &Path) -> Result<TempPath> {
        match self {
            Self::Direct(spool) => spool.finish(temp_dir),
            Self::Sorted(mut spool) => {
                let output = tempfile::Builder::new()
                    .prefix(".hdtc-prepared-graph-memberships-")
                    .tempfile_in(temp_dir)?
                    .into_temp_path();
                let file = File::create(&output)?;
                let mut encoder = zstd::Encoder::new(BufWriter::new(file), 3)?;
                let mut observed = 0u64;
                for membership in spool.sorter.finish(&mut spool.buffer)? {
                    membership?.write_to(&mut encoder)?;
                    observed += 1;
                }
                ensure!(
                    observed == spool.count,
                    "prepared layer sort count mismatch"
                );
                encoder.finish()?.flush()?;
                Ok(output)
            }
        }
    }
}

struct PreparedGraphGroups<'a, I> {
    inner: I,
    pending: Option<PreparedGraphEntry>,
    spool: &'a mut PreparedLayerSpool,
    permuted_position: u64,
}

impl<'a, I> PreparedGraphGroups<'a, I>
where
    I: Iterator<Item = Result<PreparedGraphEntry>>,
{
    fn new(inner: I, spool: &'a mut PreparedLayerSpool) -> Self {
        Self {
            inner,
            pending: None,
            spool,
            permuted_position: 0,
        }
    }

    fn positions_emitted(&self) -> u64 {
        self.permuted_position
    }
}

impl<I> Iterator for PreparedGraphGroups<'_, I>
where
    I: Iterator<Item = Result<PreparedGraphEntry>>,
{
    type Item = Result<PermEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = (|| -> Result<Option<PermEntry>> {
            let first = match self.pending.take() {
                Some(entry) => entry,
                None => match self.inner.next().transpose()? {
                    Some(entry) => entry,
                    None => return Ok(None),
                },
            };
            let key = (first.first, first.second, first.third);
            let spo_position = first.spo_position;
            let mut previous_graph = None;
            let mut current = first;
            loop {
                ensure!(
                    current.spo_position == spo_position,
                    "one prepared permutation triple has multiple SPO positions"
                );
                ensure!(
                    previous_graph.is_none_or(|before| current.graph > before),
                    "prepared permutation graph IDs are not strictly increasing"
                );
                self.spool.push(GraphMembership {
                    graph: current.graph,
                    position: self.permuted_position,
                })?;
                previous_graph = Some(current.graph);
                match self.inner.next().transpose()? {
                    Some(next) if (next.first, next.second, next.third) == key => current = next,
                    Some(next) => {
                        self.pending = Some(next);
                        break;
                    }
                    None => break,
                }
            }
            self.permuted_position = self
                .permuted_position
                .checked_add(1)
                .context("prepared permutation position overflow")?;
            Ok(Some(PermEntry {
                first: first.first,
                second: first.second,
                third: first.third,
                spo_position,
            }))
        })();
        match result {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

impl Sortable for SpoPositionMap {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.spo_position.to_le_bytes())?;
        writer.write_all(&self.permuted_position.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut bytes = [0u8; 16];
        match reader.read_exact(&mut bytes) {
            Ok(()) => Ok(Some(Self {
                spo_position: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                permuted_position: u64::from_le_bytes(bytes[8..].try_into().unwrap()),
            })),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn mem_size(&self) -> usize {
        16
    }
}

struct RawSection {
    section_type: u32,
    flags: u32,
    file: File,
    offset: u64,
    length: u64,
    entry_count: u64,
    bits_per_entry: u8,
    crc: u32,
    parameter: u64,
    indexed_bits: u64,
}

fn whole_file_digest(path: &Path) -> Result<[u8; 32]> {
    let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    sha256_to_end(&mut BufReader::with_capacity(256 * 1024, file))
}

fn file_crc(file: &mut File) -> Result<(u64, u32)> {
    let length = file.metadata()?.len();
    if length == 0 {
        return Ok((0, 0));
    }
    file.seek(SeekFrom::Start(0))?;
    let mut digest = CRC32C_ALGO.digest();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    file.seek(SeekFrom::Start(0))?;
    Ok((length, digest.finalize()))
}

fn temp_file(temp_dir: &Path) -> Result<File> {
    tempfile::tempfile_in(temp_dir).with_context(|| {
        format!(
            "Failed to create graph-index scratch file in {}",
            temp_dir.display()
        )
    })
}

fn finish_bitmap(file: &mut File, encoder: StreamingBitmapEncoder<BufWriter<File>>) -> Result<u64> {
    let (bits, mut writer) = encoder.finish()?;
    writer.flush()?;
    drop(writer);
    file.sync_data()?;
    file.seek(SeekFrom::Start(0))?;
    Ok(bits)
}

fn finish_array(
    file: &mut File,
    encoder: StreamingLogArrayEncoder<BufWriter<File>>,
) -> Result<(u64, u8)> {
    let (entries, width, mut writer) = encoder.finish()?;
    writer.flush()?;
    drop(writer);
    file.sync_data()?;
    file.seek(SeekFrom::Start(0))?;
    Ok((entries, width))
}

struct Transpose {
    array: Option<File>,
    bitmap: File,
    superranks: File,
    subranks: File,
    width: u8,
}

fn graph_id_width(named_graphs: u64) -> u8 {
    if named_graphs == 0 {
        0
    } else {
        (64 - named_graphs.leading_zeros()) as u8
    }
}

fn build_transpose(
    memberships_path: &Path,
    triples: u64,
    memberships: u64,
    named_graphs: u64,
    include_ids: bool,
    temp_dir: &Path,
) -> Result<Transpose> {
    ensure!(memberships > 0, "an empty dataset has no transpose");
    let width = graph_id_width(named_graphs);
    let mut bitmap_file = temp_file(temp_dir)?;
    let mut bitmap = StreamingBitmapEncoder::new(BufWriter::with_capacity(
        256 * 1024,
        bitmap_file.try_clone()?,
    ));
    let mut array_file = include_ids.then(|| temp_file(temp_dir)).transpose()?;
    let mut array = array_file
        .as_ref()
        .map(|file| -> Result<_> {
            Ok(StreamingLogArrayEncoder::new(
                width,
                BufWriter::with_capacity(256 * 1024, file.try_clone()?),
            ))
        })
        .transpose()?;

    let file = File::open(memberships_path)?;
    let decoder = zstd::Decoder::with_buffer(BufReader::new(file))?;
    let mut reader = decoder;
    let mut count = 0u64;
    let mut positions = 0u64;
    let mut previous: Option<PositionGraphMembership> = None;
    while let Some(item) = PositionGraphMembership::read_from(&mut reader)? {
        ensure!(item.position < triples, "transpose position out of range");
        if let Some(before) = previous {
            ensure!(
                item.position > before.position
                    || (item.position == before.position && item.graph > before.graph),
                "transpose input is not ordered by position and graph"
            );
            if item.position != before.position {
                ensure!(
                    before.position.checked_add(1) == Some(item.position),
                    "transpose input is not exhaustive"
                );
                bitmap.set_last(true);
                positions += 1;
            }
        } else {
            ensure!(item.position == 0, "transpose input has no position zero");
        }
        ensure!(
            item.graph <= named_graphs,
            "transpose graph ID out of range"
        );
        bitmap.push(false)?;
        if let Some(array) = array.as_mut() {
            array.push(item.graph)?;
        }
        previous = Some(item);
        count = count.checked_add(1).context("transpose count overflow")?;
    }
    ensure!(count == memberships, "transpose membership-count mismatch");
    ensure!(previous.is_some(), "nonempty transpose has no memberships");
    bitmap.set_last(true);
    positions += 1;
    ensure!(positions == triples, "transpose position-count mismatch");
    let bits = finish_bitmap(&mut bitmap_file, bitmap)?;
    ensure!(bits == memberships, "transpose bitmap length mismatch");
    if let (Some(file), Some(array)) = (array_file.as_mut(), array) {
        let (entries, actual_width) = finish_array(file, array)?;
        ensure!(entries == memberships, "transpose array length mismatch");
        ensure!(actual_width == width, "transpose array width mismatch");
    }

    let (superranks, subranks) = build_rank_directories(&mut bitmap_file, memberships, temp_dir)?;
    Ok(Transpose {
        array: array_file,
        bitmap: bitmap_file,
        superranks,
        subranks,
        width,
    })
}

fn build_rank_directories(bitmap: &mut File, bits: u64, temp_dir: &Path) -> Result<(File, File)> {
    let mut super_file = temp_file(temp_dir)?;
    let mut sub_file = temp_file(temp_dir)?;
    bitmap.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(256 * 1024, bitmap.try_clone()?);
    let mut super_writer = BufWriter::with_capacity(64 * 1024, super_file.try_clone()?);
    let mut sub_writer = BufWriter::with_capacity(64 * 1024, sub_file.try_clone()?);
    let blocks = bits.div_ceil(u64::from(SUBBLOCK_BITS));
    let mut total = 0u64;
    let mut within = 0u16;
    for block in 0..blocks {
        if block.is_multiple_of(8) {
            super_writer.write_all(&total.to_le_bytes())?;
            within = 0;
        }
        sub_writer.write_all(&within.to_le_bytes())?;
        let start_bit = block * u64::from(SUBBLOCK_BITS);
        let bits_here = (bits - start_bit).min(u64::from(SUBBLOCK_BITS));
        let mut bytes = [0u8; 64];
        reader.read_exact(&mut bytes[..bits_here.div_ceil(8) as usize])?;
        let complete = (bits_here / 8) as usize;
        let mut count = bytes[..complete]
            .iter()
            .map(|value| value.count_ones())
            .sum::<u32>();
        if !bits_here.is_multiple_of(8) {
            let mask = (1u8 << (bits_here % 8)) - 1;
            count += (bytes[complete] & mask).count_ones();
        }
        total = total
            .checked_add(u64::from(count))
            .context("transpose bitmap population overflow")?;
        within = within
            .checked_add(u16::try_from(count).context("transpose block population overflow")?)
            .context("transpose subrank overflow")?;
    }
    super_writer.write_all(&total.to_le_bytes())?;
    super_writer.flush()?;
    sub_writer.flush()?;
    drop(super_writer);
    drop(sub_writer);
    super_file.sync_data()?;
    sub_file.sync_data()?;
    super_file.seek(SeekFrom::Start(0))?;
    sub_file.seek(SeekFrom::Start(0))?;
    bitmap.seek(SeekFrom::Start(0))?;
    Ok((super_file, sub_file))
}

fn build_layer_memberships(
    permutation: impl Iterator<Item = Result<crate::permutation::PermEntry>>,
    memberships_path: &Path,
    expected_triples: u64,
    expected_memberships: u64,
    temp_dir: &Path,
    memory_budget: usize,
) -> Result<TempPath> {
    let sort_budget = memory_budget.max(1);
    let mut map_sorter = ExternalSorter::new(temp_dir, sort_budget);
    let mut map_buffer = Vec::<SpoPositionMap>::new();
    let mut map_memory = 0usize;
    let mut permuted_position = 0u64;
    for entry in permutation {
        let entry = entry?;
        map_sorter.push(
            SpoPositionMap {
                spo_position: entry.spo_position,
                permuted_position,
            },
            &mut map_buffer,
            &mut map_memory,
        )?;
        permuted_position = permuted_position
            .checked_add(1)
            .context("permuted position overflow")?;
    }
    ensure!(
        permuted_position == expected_triples,
        "permutation position-map length mismatch"
    );
    let mut mapping = map_sorter.finish(&mut map_buffer)?;
    let mut current_map = mapping.next().transpose()?;

    let mut layer_sorter = ExternalSorter::new(temp_dir, sort_budget);
    let mut layer_buffer = Vec::<GraphMembership>::new();
    let mut layer_memory = 0usize;
    let source = File::open(memberships_path)?;
    let mut source = zstd::Decoder::with_buffer(BufReader::new(source))?;
    let mut observed = 0u64;
    while let Some(item) = PositionGraphMembership::read_from(&mut source)? {
        while current_map.is_some_and(|map| map.spo_position < item.position) {
            current_map = mapping.next().transpose()?;
        }
        let map = current_map.context("position map ended before memberships")?;
        ensure!(
            map.spo_position == item.position,
            "membership position is absent from permutation map"
        );
        layer_sorter.push(
            GraphMembership {
                graph: item.graph,
                position: map.permuted_position,
            },
            &mut layer_buffer,
            &mut layer_memory,
        )?;
        observed = observed
            .checked_add(1)
            .context("layer membership-count overflow")?;
    }
    ensure!(
        observed == expected_memberships,
        "layer membership-count mismatch"
    );

    let output = tempfile::Builder::new()
        .prefix(".hdtc-graph-index-memberships-")
        .tempfile_in(temp_dir)?;
    let output_path = output.into_temp_path();
    let file = File::create(&output_path)?;
    let mut encoder = zstd::Encoder::new(BufWriter::with_capacity(256 * 1024, file), 3)?;
    for item in layer_sorter.finish(&mut layer_buffer)? {
        item?.write_to(&mut encoder)?;
    }
    encoder.finish()?.flush()?;
    Ok(output_path)
}

fn align64(value: u64) -> Result<u64> {
    value
        .checked_add(63)
        .map(|value| value & !63)
        .context("graph-index alignment overflow")
}

#[allow(clippy::too_many_arguments)]
fn append_section(
    sections: &mut Vec<RawSection>,
    cursor: &mut u64,
    section_type: u32,
    mut file: File,
    entry_count: u64,
    bits_per_entry: u8,
    parameter: u64,
    indexed_bits: u64,
    span: bool,
) -> Result<()> {
    let (length, payload_crc) = file_crc(&mut file)?;
    let offset = if length == 0 { 0 } else { *cursor };
    let crc = if span { 0 } else { payload_crc };
    if length != 0 {
        *cursor = align64(
            cursor
                .checked_add(length)
                .context("graph-index section end overflow")?,
        )?;
    }
    sections.push(RawSection {
        section_type,
        flags: REQUIRED,
        file,
        offset,
        length,
        entry_count,
        bits_per_entry,
        crc,
        parameter,
        indexed_bits,
    });
    Ok(())
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn write_zeros_to<W: Write + Seek>(writer: &mut W, target: u64) -> Result<()> {
    let position = writer.stream_position()?;
    ensure!(position <= target, "graph-index regions overlap");
    let mut remaining = target - position;
    let zeroes = [0u8; 8192];
    while remaining > 0 {
        let count = usize::try_from(remaining.min(zeroes.len() as u64)).unwrap();
        writer.write_all(&zeroes[..count])?;
        remaining -= count as u64;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn assemble(
    output: &Path,
    flags: u64,
    triples: u64,
    named_graphs: u64,
    memberships: u64,
    source_data_length: u64,
    source_digest: &[u8; 32],
    sidecar_digest: &[u8; 32],
    mut sections: Vec<RawSection>,
    directory_offset: u64,
    directory_length: u64,
    footer_offset: u64,
) -> Result<()> {
    sections.sort_unstable_by_key(|section| section.section_type);
    let section_count = u32::try_from(sections.len()).context("too many graph-index sections")?;
    let file_size = footer_offset
        .checked_add(FOOTER_SIZE)
        .context("graph-index size overflow")?;

    let mut directory = Vec::with_capacity(sections.len() * DIRECTORY_ENTRY_SIZE as usize);
    for section in &sections {
        let mut entry = [0u8; DIRECTORY_ENTRY_SIZE as usize];
        put_u32(&mut entry, 0, section.section_type);
        put_u32(&mut entry, 4, section.flags);
        put_u64(&mut entry, 8, section.offset);
        put_u64(&mut entry, 16, section.length);
        put_u64(&mut entry, 24, section.entry_count);
        entry[32] = section.bits_per_entry;
        put_u32(&mut entry, 36, section.crc);
        put_u64(&mut entry, 40, section.parameter);
        put_u64(&mut entry, 48, section.indexed_bits);
        directory.extend_from_slice(&entry);
    }
    ensure!(
        u64::try_from(directory.len())
            .ok()
            .and_then(|length| length.checked_add(4))
            == Some(directory_length),
        "graph-index directory length mismatch"
    );

    let mut header = [0u8; HEADER_SIZE as usize];
    header[..8].copy_from_slice(b"$HDTGIDX");
    put_u16(&mut header, 8, 1);
    put_u16(&mut header, 10, 0);
    put_u32(&mut header, 12, HEADER_SIZE as u32);
    put_u64(&mut header, 16, flags);
    put_u64(&mut header, 24, triples);
    put_u64(&mut header, 32, named_graphs);
    put_u64(&mut header, 40, memberships);
    put_u64(&mut header, 48, source_data_length);
    put_u64(&mut header, 56, file_size);
    put_u64(&mut header, 64, directory_offset);
    put_u64(&mut header, 72, directory_length);
    put_u32(&mut header, 80, section_count);
    put_u32(&mut header, 84, 1);
    put_u64(&mut header, 88, footer_offset);
    put_u32(&mut header, 96, SUPERBLOCK_BITS);
    put_u32(&mut header, 100, SUBBLOCK_BITS);
    put_u32(&mut header, 104, POSITION_CHUNK_SHIFT);
    header[112..144].copy_from_slice(source_digest);
    header[144..176].copy_from_slice(sidecar_digest);
    let header_crc = crc32c(&header[..252]);
    put_u32(&mut header, 252, header_crc);

    let mut footer = [0u8; FOOTER_SIZE as usize];
    footer[..8].copy_from_slice(b"$HDTGXND");
    put_u16(&mut footer, 8, 1);
    put_u16(&mut footer, 10, 0);
    put_u32(&mut footer, 12, FOOTER_SIZE as u32);
    put_u64(&mut footer, 16, file_size);
    put_u64(&mut footer, 24, 0);
    put_u64(&mut footer, 32, directory_offset);
    put_u64(&mut footer, 40, directory_length);
    put_u64(&mut footer, 48, 0);
    put_u32(&mut footer, 56, header_crc);
    let footer_crc = crc32c(&footer[..60]);
    put_u32(&mut footer, 60, footer_crc);

    let file = File::create(output)
        .with_context(|| format!("Failed to create graph index {}", output.display()))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);
    writer.write_all(&header)?;
    write_zeros_to(&mut writer, directory_offset)?;
    writer.write_all(&directory)?;
    writer.write_all(&crc32c(&directory).to_le_bytes())?;
    for section in &mut sections {
        if section.length == 0 {
            continue;
        }
        write_zeros_to(&mut writer, section.offset)?;
        section.file.seek(SeekFrom::Start(0))?;
        let copied = std::io::copy(&mut section.file, &mut writer)?;
        ensure!(copied == section.length, "short graph-index section copy");
    }
    write_zeros_to(&mut writer, footer_offset)?;
    writer.write_all(&footer)?;
    writer.flush()?;
    // The caller renames this file into the canonical `.graphs.idx` name, and a
    // normal open verifies metadata but not payload checksums. Reaching disk
    // before the rename is what keeps a crash from publishing a well-formed
    // header over payload bytes that never landed.
    writer
        .get_ref()
        .sync_all()
        .with_context(|| format!("Failed to flush graph index {}", output.display()))?;
    ensure!(
        std::fs::metadata(output)?.len() == file_size,
        "graph-index size mismatch after assembly"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn assemble_from_memberships(
    output: &Path,
    options: GraphIndexOptions,
    pos_memberships: Option<&Path>,
    ops_memberships: Option<&Path>,
    transpose: Option<Transpose>,
    triples: u64,
    named_graphs: u64,
    memberships: u64,
    source_data_length: u64,
    source_digest: &[u8; 32],
    sidecar_digest: &[u8; 32],
    temp_dir: &Path,
) -> Result<()> {
    let mut flags = 0u64;
    let mut section_count = 0u32;
    if options.pos_layers {
        flags |= HAS_POS_LAYERS;
        section_count += 2;
    }
    if options.ops_layers {
        flags |= HAS_OPS_LAYERS;
        section_count += 2;
    }
    if options.membership_ranks {
        flags |= HAS_MEMBERSHIP_RANKS;
        section_count += 3;
    }
    if options.membership_ids {
        flags |= HAS_MEMBERSHIP_IDS;
        section_count += 1;
    }
    ensure!(flags != 0, "a graph index must contain a structure");

    let directory_offset = HEADER_SIZE;
    let directory_length = u64::from(section_count)
        .checked_mul(DIRECTORY_ENTRY_SIZE)
        .and_then(|length| length.checked_add(4))
        .context("graph-index directory length overflow")?;
    let mut cursor = align64(
        directory_offset
            .checked_add(directory_length)
            .context("graph-index directory end overflow")?,
    )?;
    let mut sections = Vec::with_capacity(section_count as usize);

    if let Some(path) = pos_memberships {
        let layer_count = named_graphs
            .checked_add(1)
            .context("POS layer count overflow")?;
        let layer_directory_length = layer_count
            .checked_mul(96)
            .and_then(|length| length.checked_add(4))
            .context("POS layer-directory length overflow")?;
        let region_base = align64(
            cursor
                .checked_add(layer_directory_length)
                .context("POS layer-directory end overflow")?,
        )?;
        let set = encode_layer_set(
            path,
            named_graphs,
            triples,
            memberships,
            region_base,
            temp_dir,
        )?;
        append_section(
            &mut sections,
            &mut cursor,
            POS_DIRECTORY,
            set.directory,
            layer_count,
            0,
            0,
            0,
            false,
        )?;
        ensure!(cursor == region_base, "POS layer region base mismatch");
        append_section(
            &mut sections,
            &mut cursor,
            POS_REGION,
            set.region,
            set.non_empty_layers,
            0,
            0,
            0,
            true,
        )?;
    }
    if let Some(path) = ops_memberships {
        let layer_count = named_graphs
            .checked_add(1)
            .context("OPS layer count overflow")?;
        let layer_directory_length = layer_count
            .checked_mul(96)
            .and_then(|length| length.checked_add(4))
            .context("OPS layer-directory length overflow")?;
        let region_base = align64(
            cursor
                .checked_add(layer_directory_length)
                .context("OPS layer-directory end overflow")?,
        )?;
        let set = encode_layer_set(
            path,
            named_graphs,
            triples,
            memberships,
            region_base,
            temp_dir,
        )?;
        append_section(
            &mut sections,
            &mut cursor,
            OPS_DIRECTORY,
            set.directory,
            layer_count,
            0,
            0,
            0,
            false,
        )?;
        ensure!(cursor == region_base, "OPS layer region base mismatch");
        append_section(
            &mut sections,
            &mut cursor,
            OPS_REGION,
            set.region,
            set.non_empty_layers,
            0,
            0,
            0,
            true,
        )?;
    }
    if let Some(mut transpose) = transpose {
        if let Some(array) = transpose.array.take() {
            append_section(
                &mut sections,
                &mut cursor,
                TRANSPOSE_ARRAY,
                array,
                memberships,
                transpose.width,
                0,
                0,
                false,
            )?;
        }
        append_section(
            &mut sections,
            &mut cursor,
            TRANSPOSE_BITMAP,
            transpose.bitmap,
            memberships,
            1,
            0,
            0,
            false,
        )?;
        append_section(
            &mut sections,
            &mut cursor,
            TRANSPOSE_SUPER,
            transpose.superranks,
            memberships
                .div_ceil(u64::from(SUPERBLOCK_BITS))
                .checked_add(1)
                .context("transpose superrank count overflow")?,
            64,
            u64::from(SUPERBLOCK_BITS),
            memberships,
            false,
        )?;
        append_section(
            &mut sections,
            &mut cursor,
            TRANSPOSE_SUB,
            transpose.subranks,
            memberships.div_ceil(u64::from(SUBBLOCK_BITS)),
            16,
            u64::from(SUBBLOCK_BITS),
            memberships,
            false,
        )?;
    }

    assemble(
        output,
        flags,
        triples,
        named_graphs,
        memberships,
        source_data_length,
        source_digest,
        sidecar_digest,
        sections,
        directory_offset,
        directory_length,
        cursor,
    )
}

/// Drain one graph-decorated position space into a graph-major membership file.
///
/// The sorted stream is grouped by triple, which yields the permuted position of
/// each triple and, alongside it, that triple's graphs. `consume` sees the same
/// grouped stream, so a permutation index can be encoded from it without sorting
/// the triples a second time.
fn drain_prepared_space<F>(
    sorted: MergeIterator<PreparedGraphEntry>,
    layer_count: u64,
    expected_triples: u64,
    temp_dir: &Path,
    layer_budget: usize,
    consume: F,
) -> Result<TempPath>
where
    F: FnOnce(&mut PreparedGraphGroups<'_, MergeIterator<PreparedGraphEntry>>) -> Result<()>,
{
    let mut spool = PreparedLayerSpool::new(temp_dir, layer_count, layer_budget)?;
    let mut groups = PreparedGraphGroups::new(sorted, &mut spool);
    consume(&mut groups)?;
    ensure!(
        groups.positions_emitted() == expected_triples,
        "prepared layer-set triple-count mismatch"
    );
    drop(groups);
    spool.finish(temp_dir)
}

/// Drain a grouped stream when nothing else consumes it.
fn drain_groups(
    groups: &mut PreparedGraphGroups<'_, MergeIterator<PreparedGraphEntry>>,
) -> Result<()> {
    for entry in groups {
        entry?;
    }
    Ok(())
}

/// Finish the default POS/OPS graph index from sort runs populated during HDT
/// creation. Graph IDs ride through the permutation sorts, so draining each
/// sorted stream can append directly to monotonically ordered per-graph layers.
/// No sidecar transpose, inverse-position sort, HDT rescan, or membership sort
/// is needed for the common bounded-graph-count case.
#[allow(clippy::too_many_arguments)]
pub fn finish_prepared_graph_index(
    mut collector: PreparedGraphIndexCollector,
    graph_index_output: &Path,
    permutation_output: Option<&Path>,
    hdt_path: &Path,
    sidecar_path: &Path,
    counts: &DictCounts,
    triples_files: &BitmapTriplesFiles,
    expected_memberships: u64,
    temp_dir: &Path,
    memory_budget: usize,
    permutation_maps: PositionMaps,
) -> Result<()> {
    ensure!(
        collector.triple_count == triples_files.num_triples,
        "prepared graph-index triple-count mismatch"
    );
    ensure!(
        collector.membership_count == expected_memberships,
        "prepared graph-index membership-count mismatch"
    );
    let layer_count = counts
        .graphs
        .checked_add(1)
        .context("prepared graph layer count overflow")?;
    let layer_budget = (memory_budget / 2).max(1);
    let mut permutation = permutation_output
        .map(|_| {
            PreparedPermutationAssembler::new(
                hdt_path,
                counts,
                triples_files,
                temp_dir,
                permutation_maps,
            )
        })
        .transpose()?;

    let pos_sorted = collector.pos_sorter.finish(&mut collector.pos_buffer)?;
    let pos_memberships = drain_prepared_space(
        pos_sorted,
        layer_count,
        triples_files.num_triples,
        temp_dir,
        layer_budget,
        |groups| match permutation.as_mut() {
            Some(permutation) => permutation.encode_pos(groups),
            None => drain_groups(groups),
        },
    )?;
    drop(collector.pos_sorter);

    let ops_sorted = collector.ops_sorter.finish(&mut collector.ops_buffer)?;
    let ops_memberships = drain_prepared_space(
        ops_sorted,
        layer_count,
        triples_files.num_triples,
        temp_dir,
        layer_budget,
        |groups| match permutation.as_mut() {
            Some(permutation) => permutation.encode_ops(groups),
            None => drain_groups(groups),
        },
    )?;
    drop(collector.ops_sorter);

    if let (Some(permutation), Some(output)) = (permutation, permutation_output) {
        permutation.finish(output)?;
    }

    let source = SourceIdentity::capture(hdt_path)?;
    let metadata = scan_hdt(hdt_path)?;
    let sidecar = GraphSidecarReader::open(sidecar_path, hdt_path)?;
    ensure!(
        sidecar.source_digest() == *source.digest(),
        "prepared sidecar/HDT stored digest mismatch"
    );
    ensure!(
        sidecar.triple_count() == triples_files.num_triples,
        "prepared sidecar triple-count mismatch"
    );
    ensure!(
        sidecar.named_graph_count() == counts.graphs,
        "prepared sidecar graph-count mismatch"
    );
    ensure!(
        sidecar.membership_count() == expected_memberships,
        "prepared sidecar membership-count mismatch"
    );
    let sidecar_digest = sidecar.whole_file_digest()?;
    drop(sidecar);

    assemble_from_memberships(
        graph_index_output,
        GraphIndexOptions::default(),
        Some(&pos_memberships),
        Some(&ops_memberships),
        None,
        triples_files.num_triples,
        counts.graphs,
        expected_memberships,
        metadata.file_length - metadata.data_offset,
        source.digest(),
        &sidecar_digest,
        temp_dir,
    )?;
    source.ensure_unchanged(hdt_path)?;
    ensure!(
        whole_file_digest(sidecar_path)? == sidecar_digest,
        "prepared graph sidecar changed during graph-index construction"
    );
    Ok(())
}

pub fn create_graph_index(
    hdt_path: &Path,
    memory_budget: usize,
    temp_dir: &Path,
    mut options: GraphIndexOptions,
) -> Result<PathBuf> {
    if options.membership_ids {
        options.membership_ranks = true;
    }
    ensure!(
        options.pos_layers || options.ops_layers || options.membership_ranks,
        "a graph index must contain at least one structure"
    );
    ensure!(
        hdt_path.is_file(),
        "HDT file not found: {}",
        hdt_path.display()
    );
    let sidecar_path = canonical_sidecar_path(hdt_path);
    ensure!(
        sidecar_path.is_file(),
        "graph sidecar not found: {}",
        sidecar_path.display()
    );

    let source = SourceIdentity::capture(hdt_path)?;
    let metadata = scan_hdt(hdt_path)?;
    let mut sidecar = GraphSidecarReader::open(&sidecar_path, hdt_path)?;
    ensure!(
        sidecar.source_digest() == *source.digest(),
        "sidecar/HDT stored digest mismatch"
    );
    ensure!(
        sidecar.source_data_length() == metadata.file_length - metadata.data_offset,
        "sidecar/HDT stored data-length mismatch"
    );
    let triples = sidecar.triple_count();
    let named_graphs = sidecar.named_graph_count();
    let memberships = sidecar.membership_count();

    let sidecar_digest = sidecar.whole_file_digest()?;

    if memberships == 0 {
        options.membership_ranks = false;
        options.membership_ids = false;
    }

    // Transposing the sidecar is a k-way merge of its layers, each of which is
    // already position-sorted, so it costs one buffered iterator per layer
    // instead of an external sort of every membership. Both layer strategies
    // below consume that same stream.
    let layer_count = named_graphs
        .checked_add(1)
        .context("graph layer count overflow")?;
    let collect_layers = options.pos_layers || options.ops_layers;

    // Two ways to reach graph-major permuted positions, and which is cheaper
    // depends on how many graphs the average triple belongs to.
    //
    // Decorating carries a graph ID through the POS/OPS sorts, so each triple
    // is sorted once per membership but the layer spools can then be appended
    // to directly. Mapping sorts each triple once, then pays two further sorts
    // per position space to invert the permutation and regroup by graph.
    //
    // Decorating therefore moves fewer bytes until a triple averages several
    // graphs, after which repeating its key per membership dominates. Measured
    // against both, `decorate` wins comfortably through 16 memberships per
    // triple at every budget and loses beyond that under memory pressure.
    let decorate = collect_layers
        && memberships <= triples.saturating_mul(DECORATED_MULTIPLICITY_LIMIT).max(1);
    if collect_layers {
        if decorate {
            tracing::info!(
                triples,
                memberships,
                "Building graph layers from graph-decorated permutation sorts"
            );
        } else {
            tracing::info!(
                triples,
                memberships,
                "Memberships per triple exceed the decorated-sort limit; \
                 building graph layers by mapping permuted positions"
            );
        }
    }
    // `build_layer_memberships` reads the transposed stream back, so the
    // mapping strategy needs it on disk even without membership ranks.
    let need_transposed_file = options.membership_ranks || (collect_layers && !decorate);

    // The merge's cost is one buffered iterator per layer, so reserve exactly
    // that and leave the rest to the sorts. Only the external-sort fallback
    // needs a real share of the budget.
    let merge_reserve = layer_merge_reserve(layer_count, memory_budget);
    let stream_budget = match (merge_reserve, collect_layers) {
        (Some(reserve), _) => reserve,
        (None, true) => (memory_budget / 2).max(1),
        (None, false) => memory_budget.max(1),
    };
    let collector_budget = if decorate {
        memory_budget.saturating_sub(stream_budget).max(1)
    } else {
        1
    };
    let mut collector = decorate.then(|| {
        PreparedGraphIndexCollector::for_spaces(
            temp_dir,
            collector_budget,
            options.pos_layers,
            options.ops_layers,
        )
    });
    let transposed = need_transposed_file
        .then(|| -> Result<_> {
            Ok(tempfile::Builder::new()
                .prefix(".hdtc-graph-index-transposed-")
                .tempfile_in(temp_dir)?
                .into_temp_path())
        })
        .transpose()?;
    let mut transposed_encoder = transposed
        .as_deref()
        .map(|path| -> Result<_> {
            let file = File::create(path)
                .with_context(|| format!("Failed to create {}", path.display()))?;
            Ok(zstd::Encoder::new(
                BufWriter::with_capacity(256 * 1024, file),
                3,
            )?)
        })
        .transpose()?;

    let mut positioned = PositionMajorMemberships::open(
        &mut sidecar,
        named_graphs,
        temp_dir,
        stream_budget,
        merge_reserve.is_some(),
    )?;
    let mut pending = positioned.next().transpose()?;
    let mut observed_memberships = 0u64;
    if let Some(collector) = collector.as_mut() {
        let mut scanner = BitmapTriplesScanner::new(&metadata.offsets, hdt_path)?;
        let mut position = 0u64;
        while let Some((subject, predicate, object)) = scanner.next_triple()? {
            let triple = IdTriple {
                subject,
                predicate,
                object,
            };
            let mut joined = false;
            while pending.is_some_and(|item| item.position == position) {
                let item = pending.context("graph membership vanished mid-join")?;
                if let Some(encoder) = transposed_encoder.as_mut() {
                    item.write_to(encoder)?;
                }
                collector.push(
                    triple,
                    GraphMembership {
                        graph: item.graph,
                        position,
                    },
                )?;
                observed_memberships += 1;
                joined = true;
                pending = positioned.next().transpose()?;
            }
            ensure!(joined, "SPO position {position} has no graph membership");
            position += 1;
        }
        scanner.finish()?;
        ensure!(position == triples, "HDT/sidecar triple-count mismatch");
    } else {
        while let Some(item) = pending {
            if let Some(encoder) = transposed_encoder.as_mut() {
                item.write_to(encoder)?;
            }
            observed_memberships += 1;
            pending = positioned.next().transpose()?;
        }
    }
    ensure!(
        pending.is_none(),
        "graph membership position exceeds the HDT triple count"
    );
    ensure!(
        observed_memberships == memberships,
        "sidecar membership-count mismatch"
    );
    drop(positioned);
    if let Some(encoder) = transposed_encoder {
        encoder.finish()?.flush()?;
    }
    drop(sidecar);

    // Keyed off the option, not off the file: the mapping strategy stages a
    // transposed stream of its own, and building a transpose from it that the
    // flags do not declare would desync the section directory.
    let transpose = options
        .membership_ranks
        .then(|| -> Result<_> {
            build_transpose(
                transposed
                    .as_deref()
                    .context("membership ranks require a transposed stream")?,
                triples,
                memberships,
                named_graphs,
                options.membership_ids,
                temp_dir,
            )
        })
        .transpose()?;

    let layer_budget = (memory_budget / 2).max(1);
    let (pos_memberships, ops_memberships) = if decorate {
        let pos = options
            .pos_layers
            .then(|| -> Result<_> {
                let collector = collector
                    .as_mut()
                    .context("POS graph-index collector is absent")?;
                let sorted = collector.pos_sorter.finish(&mut collector.pos_buffer)?;
                drain_prepared_space(
                    sorted,
                    layer_count,
                    triples,
                    temp_dir,
                    layer_budget,
                    drain_groups,
                )
            })
            .transpose()?;
        let ops = options
            .ops_layers
            .then(|| -> Result<_> {
                let collector = collector
                    .as_mut()
                    .context("OPS graph-index collector is absent")?;
                let sorted = collector.ops_sorter.finish(&mut collector.ops_buffer)?;
                drain_prepared_space(
                    sorted,
                    layer_count,
                    triples,
                    temp_dir,
                    layer_budget,
                    drain_groups,
                )
            })
            .transpose()?;
        drop(collector);
        (pos, ops)
    } else if collect_layers {
        // High multiplicity: sort each triple once, then map its permuted
        // position onto the memberships already sitting in SPO-position order.
        let transposed = transposed
            .as_deref()
            .context("mapped layer construction requires a transposed stream")?;
        let mut permutation = PermutationCollector::for_spaces(
            temp_dir,
            memory_budget,
            PositionMaps::default(),
            options.pos_layers,
            options.ops_layers,
        );
        let mut scanner = BitmapTriplesScanner::new(&metadata.offsets, hdt_path)?;
        while let Some((subject, predicate, object)) = scanner.next_triple()? {
            permutation.push(IdTriple {
                subject,
                predicate,
                object,
            })?;
        }
        scanner.finish()?;
        let pos = options
            .pos_layers
            .then(|| -> Result<_> {
                build_layer_memberships(
                    permutation.finish_pos()?,
                    transposed,
                    triples,
                    memberships,
                    temp_dir,
                    layer_budget,
                )
            })
            .transpose()?;
        let ops = options
            .ops_layers
            .then(|| -> Result<_> {
                build_layer_memberships(
                    permutation.finish_ops()?,
                    transposed,
                    triples,
                    memberships,
                    temp_dir,
                    layer_budget,
                )
            })
            .transpose()?;
        drop(permutation);
        (pos, ops)
    } else {
        (None, None)
    };

    let output = canonical_path(hdt_path);
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let temporary = tempfile::Builder::new()
        .prefix(".hdtc-graph-index-output-")
        .tempfile_in(parent)?
        .into_temp_path();
    assemble_from_memberships(
        &temporary,
        options,
        pos_memberships.as_deref(),
        ops_memberships.as_deref(),
        transpose,
        triples,
        named_graphs,
        memberships,
        metadata.file_length - metadata.data_offset,
        source.digest(),
        &sidecar_digest,
        temp_dir,
    )?;
    source.ensure_unchanged(hdt_path)?;
    ensure!(
        whole_file_digest(&sidecar_path)? == sidecar_digest,
        "graph sidecar changed during graph-index construction"
    );
    temporary
        .persist(&output)
        .map_err(|error| error.error)
        .with_context(|| format!("Failed to publish graph index {}", output.display()))?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_name_appends_to_sidecar() {
        assert_eq!(
            canonical_path(Path::new("data.hdt")),
            PathBuf::from("data.hdt.graphs.idx")
        );
    }

    #[test]
    fn graph_width_uses_maximum_identifier() {
        assert_eq!(graph_id_width(0), 0);
        assert_eq!(graph_id_width(1), 1);
        assert_eq!(graph_id_width(7), 3);
        assert_eq!(graph_id_width(8), 4);
    }
}
