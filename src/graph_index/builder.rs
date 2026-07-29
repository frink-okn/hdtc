use crate::hdt::artifacts::SourceIdentity;
use crate::hdt::reader::{BitmapTriplesScanner, sha256_to_end};
use crate::io::crc_utils::{CRC32C_ALGO, crc32c};
use crate::io::{StreamingBitmapEncoder, StreamingLogArrayEncoder};
use crate::permutation::{PermutationCollector, scan_hdt};
use crate::quads::writer::encode_layer_set;
use crate::quads::{
    GraphMembership, GraphSidecarReader, PositionGraphMembership, canonical_sidecar_path,
};
use crate::sort::{ExternalSorter, Sortable};
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result, ensure};
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

    let transposed = tempfile::Builder::new()
        .prefix(".hdtc-graph-index-transposed-")
        .tempfile_in(temp_dir)?
        .into_temp_path();
    sidecar.validate_strict(temp_dir, memory_budget, Some(&transposed))?;
    let sidecar_digest = sidecar.whole_file_digest()?;

    if memberships == 0 {
        options.membership_ranks = false;
        options.membership_ids = false;
    }
    let transpose = if options.membership_ranks {
        Some(build_transpose(
            &transposed,
            triples,
            memberships,
            named_graphs,
            options.membership_ids,
            temp_dir,
        )?)
    } else {
        None
    };

    let mut collector = if options.pos_layers || options.ops_layers {
        let mut collector = PermutationCollector::for_spaces(
            temp_dir,
            memory_budget,
            crate::permutation::PositionMaps::default(),
            options.pos_layers,
            options.ops_layers,
        );
        let mut scanner = BitmapTriplesScanner::new(&metadata.offsets, hdt_path)?;
        while let Some((subject, predicate, object)) = scanner.next_triple()? {
            collector.push(IdTriple {
                subject,
                predicate,
                object,
            })?;
        }
        scanner.finish()?;
        Some(collector)
    } else {
        None
    };

    let pos_memberships = if options.pos_layers {
        let sorted = collector
            .as_mut()
            .context("POS permutation collector is absent")?
            .finish_pos()?;
        Some(build_layer_memberships(
            sorted,
            &transposed,
            triples,
            memberships,
            temp_dir,
            (memory_budget / 2).max(1),
        )?)
    } else {
        None
    };
    let ops_memberships = if options.ops_layers {
        let sorted = collector
            .as_mut()
            .context("OPS permutation collector is absent")?
            .finish_ops()?;
        Some(build_layer_memberships(
            sorted,
            &transposed,
            triples,
            memberships,
            temp_dir,
            (memory_budget / 2).max(1),
        )?)
    } else {
        None
    };
    drop(collector);

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
    ensure!(
        flags != 0,
        "an empty dataset still needs a layer-set structure"
    );

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

    if let Some(path) = pos_memberships.as_deref() {
        let layer_count = named_graphs
            .checked_add(1)
            .context("POS layer count overflow")?;
        let directory_length = layer_count
            .checked_mul(96)
            .and_then(|length| length.checked_add(4))
            .context("POS layer-directory length overflow")?;
        let region_base = align64(
            cursor
                .checked_add(directory_length)
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
    if let Some(path) = ops_memberships.as_deref() {
        let layer_count = named_graphs
            .checked_add(1)
            .context("OPS layer count overflow")?;
        let directory_length = layer_count
            .checked_mul(96)
            .and_then(|length| length.checked_add(4))
            .context("OPS layer-directory length overflow")?;
        let region_base = align64(
            cursor
                .checked_add(directory_length)
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

    let footer_offset = cursor;
    let output = canonical_path(hdt_path);
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let temporary = tempfile::Builder::new()
        .prefix(".hdtc-graph-index-output-")
        .tempfile_in(parent)?
        .into_temp_path();
    assemble(
        &temporary,
        flags,
        triples,
        named_graphs,
        memberships,
        metadata.file_length - metadata.data_offset,
        source.digest(),
        &sidecar_digest,
        sections,
        directory_offset,
        directory_length,
        footer_offset,
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
