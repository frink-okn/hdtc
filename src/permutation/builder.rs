use super::format::*;
use crate::dictionary::DictCounts;
use crate::hdt::artifacts::SourceIdentity;
use crate::hdt::reader::{BitmapTriplesScanner, HdtSectionOffsets};
use crate::io::crc_utils::{CRC32C_ALGO, crc8, crc32c};
use crate::io::{ControlInfo, ControlType, StreamingBitmapEncoder, StreamingLogArrayEncoder};
use crate::sort::{ExternalSorter, Sortable};
use crate::triples::BitmapTriplesFiles;
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result, ensure};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tempfile::{NamedTempFile, TempPath};

const DICTIONARY_FOUR_FORMAT: &str = "<http://purl.org/HDT/hdt#dictionaryFour>";
const TRIPLES_BITMAP_FORMAT: &str = "<http://purl.org/HDT/hdt#triplesBitmap>";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PositionMaps {
    pub pos: bool,
    pub ops: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RawBitmap {
    pub path: PathBuf,
    pub data_offset: u64,
    pub bits: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct HdtMetadata {
    pub data_offset: u64,
    pub file_length: u64,
    pub triples: u64,
    pub sp_pairs: u64,
    pub subjects: u64,
    pub predicates: u64,
    pub objects: u64,
    pub offsets: HdtSectionOffsets,
    pub bitmap_y: RawBitmap,
    pub bitmap_z: RawBitmap,
}

fn read_bitmap<R: Read + Seek>(reader: &mut R, path: &Path) -> Result<(u64, u64, u64)> {
    let section_start = reader.stream_position()?;
    let mut preamble = vec![0u8; 1];
    reader.read_exact(&mut preamble)?;
    ensure!(preamble[0] == 1, "invalid HDT bitmap section type");
    let bits = read_vbyte_recording(reader, &mut preamble)?;
    let mut stored_crc = [0u8; 1];
    reader.read_exact(&mut stored_crc)?;
    ensure!(
        crc8(&preamble) == stored_crc[0],
        "HDT bitmap preamble CRC8 mismatch"
    );
    let data_offset = reader.stream_position()?;
    let data_len = bits.div_ceil(8);
    reader.seek(SeekFrom::Current(
        i64::try_from(data_len).context("bitmap is too large to seek")?,
    ))?;
    let mut data_crc = [0u8; 4];
    reader
        .read_exact(&mut data_crc)
        .with_context(|| format!("truncated bitmap in {}", path.display()))?;
    Ok((section_start, data_offset, bits))
}

fn read_array<R: Read + Seek>(reader: &mut R, path: &Path) -> Result<(u64, u64, u8)> {
    let section_start = reader.stream_position()?;
    let mut preamble = vec![0u8; 2];
    reader.read_exact(&mut preamble)?;
    ensure!(preamble[0] == 1, "invalid HDT LogArray section type");
    let width = preamble[1];
    ensure!(width <= 64, "invalid HDT LogArray width");
    let entries = read_vbyte_recording(reader, &mut preamble)?;
    let mut stored_crc = [0u8; 1];
    reader.read_exact(&mut stored_crc)?;
    ensure!(
        crc8(&preamble) == stored_crc[0],
        "HDT LogArray preamble CRC8 mismatch"
    );
    let data_len = packed_len(entries, width)?;
    reader.seek(SeekFrom::Current(
        i64::try_from(data_len).context("LogArray is too large to seek")?,
    ))?;
    let mut data_crc = [0u8; 4];
    reader
        .read_exact(&mut data_crc)
        .with_context(|| format!("truncated LogArray in {}", path.display()))?;
    Ok((section_start, entries, width))
}

fn read_vbyte_recording<R: Read>(reader: &mut R, bytes: &mut Vec<u8>) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        bytes.push(byte[0]);
        value |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 != 0 {
            return Ok(value);
        }
        shift += 7;
        ensure!(shift < 64, "invalid VByte in HDT section preamble");
    }
}

/// Read the counts needed by the permutation format and seek past a PFC
/// section without materializing its block-offset LogArray. The normal PFC
/// reader expands those offsets for random term access; this structural scan
/// only needs the string count and the position of the following section.
fn skip_pfc_section<R: Read + Seek>(reader: &mut R, section_name: &str) -> Result<u64> {
    let mut preamble = vec![0u8; 1];
    reader.read_exact(&mut preamble)?;
    ensure!(preamble[0] == 2, "invalid {section_name} PFC section type");
    let string_count = read_vbyte_recording(reader, &mut preamble)?;
    let buffer_length = read_vbyte_recording(reader, &mut preamble)?;
    let block_size = read_vbyte_recording(reader, &mut preamble)?;
    ensure!(block_size > 0, "invalid {section_name} PFC block size");
    let mut stored_crc = [0u8; 1];
    reader.read_exact(&mut stored_crc)?;
    ensure!(
        crc8(&preamble) == stored_crc[0],
        "{section_name} PFC preamble CRC8 mismatch"
    );

    let mut offsets_preamble = vec![0u8; 2];
    reader.read_exact(&mut offsets_preamble)?;
    ensure!(
        offsets_preamble[0] == 1,
        "invalid {section_name} PFC offsets LogArray type"
    );
    let offsets_width = offsets_preamble[1];
    ensure!(
        offsets_width <= 64,
        "invalid {section_name} PFC offsets width"
    );
    let offsets_count = read_vbyte_recording(reader, &mut offsets_preamble)?;
    reader.read_exact(&mut stored_crc)?;
    ensure!(
        crc8(&offsets_preamble) == stored_crc[0],
        "{section_name} PFC offsets preamble CRC8 mismatch"
    );
    let expected_offsets = string_count
        .div_ceil(block_size)
        .checked_add(1)
        .context("PFC block-offset count overflow")?;
    ensure!(
        offsets_count == expected_offsets,
        "unexpected {section_name} PFC block-offset count"
    );

    let offsets_length = packed_len(offsets_count, offsets_width)?;
    let bytes_to_skip = offsets_length
        .checked_add(4)
        .and_then(|length| length.checked_add(buffer_length))
        .and_then(|length| length.checked_add(4))
        .context("PFC section length overflow")?;
    reader.seek(SeekFrom::Current(
        i64::try_from(bytes_to_skip).context("PFC section is too large to seek")?,
    ))?;
    Ok(string_count)
}

pub(crate) fn scan_hdt(path: &Path) -> Result<HdtMetadata> {
    let file =
        File::open(path).with_context(|| format!("Failed to open HDT file {}", path.display()))?;
    let file_length = file.metadata()?.len();
    let mut reader = BufReader::with_capacity(256 * 1024, file);

    let global =
        ControlInfo::read_from(&mut reader).context("Failed to read HDT global control info")?;
    ensure!(
        global.control_type == ControlType::Global,
        "expected HDT global control info"
    );
    let header =
        ControlInfo::read_from(&mut reader).context("Failed to read HDT header control info")?;
    ensure!(
        header.control_type == ControlType::Header,
        "expected HDT header control info"
    );
    let header_length: u64 = header
        .get_property("length")
        .context("HDT header is missing length")?
        .parse()
        .context("invalid HDT header length")?;
    reader.seek(SeekFrom::Current(
        i64::try_from(header_length).context("HDT header is too large")?,
    ))?;
    let data_offset = reader.stream_position()?;

    let dictionary = ControlInfo::read_from(&mut reader)
        .context("Failed to read HDT dictionary control info")?;
    ensure!(
        dictionary.control_type == ControlType::Dictionary,
        "expected HDT dictionary control info"
    );
    ensure!(
        dictionary.format == DICTIONARY_FOUR_FORMAT,
        "unsupported HDT dictionary format: {}",
        dictionary.format
    );
    let shared = skip_pfc_section(&mut reader, "shared")?;
    let subjects_only = skip_pfc_section(&mut reader, "subjects")?;
    let predicates = skip_pfc_section(&mut reader, "predicates")?;
    let objects_only = skip_pfc_section(&mut reader, "objects")?;

    let triples_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read HDT triples control info")?;
    ensure!(
        triples_ci.control_type == ControlType::Triples,
        "expected HDT triples control info"
    );
    ensure!(
        triples_ci.format == TRIPLES_BITMAP_FORMAT,
        "unsupported HDT triples format: {}",
        triples_ci.format
    );
    let triples_order: u64 = triples_ci
        .get_property("order")
        .context("HDT triples control info is missing order")?
        .parse()
        .context("invalid HDT triples order")?;
    ensure!(
        triples_order == 1,
        "permutation indexes require SPO-ordered HDT triples"
    );

    let (by_start, by_data, by_bits) = read_bitmap(&mut reader, path)?;
    let (bz_start, bz_data, bz_bits) = read_bitmap(&mut reader, path)?;
    let (ay_start, ay_entries, _) = read_array(&mut reader, path)?;
    let (az_start, az_entries, _) = read_array(&mut reader, path)?;
    ensure!(by_bits == ay_entries, "HDT BitmapY/ArrayY length mismatch");
    ensure!(bz_bits == az_entries, "HDT BitmapZ/ArrayZ length mismatch");
    ensure!(
        reader.stream_position()? == file_length,
        "trailing or truncated bytes after HDT triples"
    );

    let subjects = shared
        .checked_add(subjects_only)
        .context("subject count overflow")?;
    let objects = shared
        .checked_add(objects_only)
        .context("object count overflow")?;
    Ok(HdtMetadata {
        data_offset,
        file_length,
        triples: az_entries,
        sp_pairs: ay_entries,
        subjects,
        predicates,
        objects,
        offsets: HdtSectionOffsets {
            num_triples: az_entries,
            num_sp_pairs: ay_entries,
            by_start,
            bz_start,
            ay_start,
            az_start,
        },
        bitmap_y: RawBitmap {
            path: path.to_path_buf(),
            data_offset: by_data,
            bits: by_bits,
        },
        bitmap_z: RawBitmap {
            path: path.to_path_buf(),
            data_offset: bz_data,
            bits: bz_bits,
        },
    })
}

#[derive(Debug, Clone, Copy, Eq)]
pub(crate) struct PermEntry {
    pub first: u64,
    pub second: u64,
    pub third: u64,
    pub spo_position: u64,
}

impl PartialEq for PermEntry {
    fn eq(&self, other: &Self) -> bool {
        self.first == other.first
            && self.second == other.second
            && self.third == other.third
            && self.spo_position == other.spo_position
    }
}

impl Ord for PermEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.first
            .cmp(&other.first)
            .then(self.second.cmp(&other.second))
            .then(self.third.cmp(&other.third))
            .then(self.spo_position.cmp(&other.spo_position))
    }
}

impl PartialOrd for PermEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Sortable for PermEntry {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        for value in [self.first, self.second, self.third, self.spo_position] {
            writer.write_all(&value.to_le_bytes())?;
        }
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut bytes = [0u8; 32];
        match reader.read_exact(&mut bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        Ok(Some(Self {
            first: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            second: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            third: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            spo_position: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        }))
    }

    fn mem_size(&self) -> usize {
        32
    }
}

/// Two external sorts prepared while the create pipeline's unique SPO stream
/// is still available.
pub struct PermutationCollector {
    pos_sorter: ExternalSorter,
    ops_sorter: ExternalSorter,
    pos_buffer: Vec<PermEntry>,
    ops_buffer: Vec<PermEntry>,
    pos_memory: usize,
    ops_memory: usize,
    count: u64,
    last_source_subject: u64,
    maps: PositionMaps,
    temp_dir: PathBuf,
    collect_pos: bool,
    collect_ops: bool,
}

impl PermutationCollector {
    pub fn new(temp_dir: &Path, memory_budget: usize, maps: PositionMaps) -> Self {
        Self::for_spaces(temp_dir, memory_budget, maps, true, true)
    }

    /// Collect only the requested position spaces. A space that is not
    /// collected costs nothing: its sorter never sees a record, so a
    /// single-space caller neither spills run files it will never merge nor
    /// gives up half its sort budget to them.
    pub(crate) fn for_spaces(
        temp_dir: &Path,
        memory_budget: usize,
        maps: PositionMaps,
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
            count: 0,
            last_source_subject: 0,
            maps,
            temp_dir: temp_dir.to_path_buf(),
            collect_pos,
            collect_ops,
        }
    }

    pub fn push(&mut self, triple: IdTriple) -> Result<()> {
        if self.count == 0 {
            ensure!(
                triple.subject == 1,
                "SPO source has an empty leading subject group"
            );
        } else if triple.subject != self.last_source_subject {
            ensure!(
                self.last_source_subject
                    .checked_add(1)
                    .is_some_and(|next| triple.subject == next),
                "SPO source has an empty subject group"
            );
        }
        let spo_position = self.count;
        if self.collect_pos {
            self.pos_sorter.push(
                PermEntry {
                    first: triple.predicate,
                    second: triple.object,
                    third: triple.subject,
                    spo_position,
                },
                &mut self.pos_buffer,
                &mut self.pos_memory,
            )?;
        }
        if self.collect_ops {
            self.ops_sorter.push(
                PermEntry {
                    first: triple.object,
                    second: triple.predicate,
                    third: triple.subject,
                    spo_position,
                },
                &mut self.ops_buffer,
                &mut self.ops_memory,
            )?;
        }
        self.count = self
            .count
            .checked_add(1)
            .context("permutation triple-count overflow")?;
        self.last_source_subject = triple.subject;
        Ok(())
    }

    pub(crate) fn finish_pos(&mut self) -> Result<crate::sort::MergeIterator<PermEntry>> {
        ensure!(self.collect_pos, "POS permutation was not collected");
        self.pos_sorter.finish(&mut self.pos_buffer)
    }

    pub(crate) fn finish_ops(&mut self) -> Result<crate::sort::MergeIterator<PermEntry>> {
        ensure!(self.collect_ops, "OPS permutation was not collected");
        self.ops_sorter.finish(&mut self.ops_buffer)
    }
}

struct DraftSection {
    section_type: u32,
    flags: u32,
    payload: TempPath,
    entry_count: u64,
    bits_per_entry: u8,
    parameter: u64,
    indexed_bits: u64,
}

struct EncodedPermutation {
    sections: Vec<DraftSection>,
    pair_count: u64,
}

/// Incremental encoder for POS/OPS streams that were prepared while the HDT's
/// unique SPO stream was still available.
///
/// Keeping the two encoders separate lets another derived artifact consume the
/// same sorted streams as they are drained, without sorting the triples again.
pub(crate) struct PreparedPermutationAssembler {
    hdt_path: PathBuf,
    source: SourceIdentity,
    metadata: HdtMetadata,
    maps: PositionMaps,
    temp_dir: PathBuf,
    spo_y: RawBitmap,
    spo_z: RawBitmap,
    pos: Option<EncodedPermutation>,
    ops: Option<EncodedPermutation>,
}

impl PreparedPermutationAssembler {
    pub(crate) fn new(
        hdt_path: &Path,
        counts: &DictCounts,
        triples: &BitmapTriplesFiles,
        temp_dir: &Path,
        maps: PositionMaps,
    ) -> Result<Self> {
        let source = SourceIdentity::capture(hdt_path)?;
        let metadata = scan_hdt(hdt_path)?;
        ensure!(
            metadata.subjects == counts.shared + counts.subjects,
            "prepared permutation subject count mismatch"
        );
        ensure!(
            metadata.predicates == counts.predicates,
            "prepared permutation predicate count mismatch"
        );
        ensure!(
            metadata.objects == counts.shared + counts.objects,
            "prepared permutation object count mismatch"
        );
        ensure!(
            metadata.triples == triples.num_triples,
            "prepared permutation HDT triple count mismatch"
        );
        Ok(Self {
            hdt_path: hdt_path.to_path_buf(),
            source,
            metadata,
            maps,
            temp_dir: temp_dir.to_path_buf(),
            spo_y: raw_bitmap_from_temp(&triples.bitmap_y.path, triples.bitmap_y.num_bits),
            spo_z: raw_bitmap_from_temp(&triples.bitmap_z.path, triples.bitmap_z.num_bits),
            pos: None,
            ops: None,
        })
    }

    pub(crate) fn encode_pos(
        &mut self,
        sorted: impl Iterator<Item = Result<PermEntry>>,
    ) -> Result<()> {
        ensure!(self.pos.is_none(), "POS permutation was already encoded");
        self.pos = Some(encode_permutation(
            sorted,
            1,
            self.metadata.triples,
            self.metadata.predicates,
            self.metadata.objects,
            self.metadata.subjects,
            self.maps.pos,
            &self.temp_dir,
        )?);
        Ok(())
    }

    pub(crate) fn encode_ops(
        &mut self,
        sorted: impl Iterator<Item = Result<PermEntry>>,
    ) -> Result<()> {
        ensure!(self.ops.is_none(), "OPS permutation was already encoded");
        self.ops = Some(encode_permutation(
            sorted,
            2,
            self.metadata.triples,
            self.metadata.objects,
            self.metadata.predicates,
            self.metadata.subjects,
            self.maps.ops,
            &self.temp_dir,
        )?);
        Ok(())
    }

    pub(crate) fn finish(self, output: &Path) -> Result<()> {
        let pos = self.pos.context("POS permutation was not encoded")?;
        let ops = self.ops.context("OPS permutation was not encoded")?;
        let spo_y_dirs = build_rank_directories(&self.spo_y, &self.temp_dir)?;
        let spo_z_dirs = build_rank_directories(&self.spo_z, &self.temp_dir)?;
        let mut drafts = pos.sections;
        drafts.extend(ops.sections);
        drafts.push(rank_draft(
            SPO_Y_SUPER,
            spo_y_dirs.0,
            self.metadata.sp_pairs,
            true,
        ));
        drafts.push(rank_draft(
            SPO_Y_SUB,
            spo_y_dirs.1,
            self.metadata.sp_pairs,
            false,
        ));
        drafts.push(rank_draft(
            SPO_Z_SUPER,
            spo_z_dirs.0,
            self.metadata.triples,
            true,
        ));
        drafts.push(rank_draft(
            SPO_Z_SUB,
            spo_z_dirs.1,
            self.metadata.triples,
            false,
        ));
        assemble(
            output,
            &self.metadata,
            self.source.digest(),
            pos.pair_count,
            ops.pair_count,
            drafts,
        )?;
        self.source.ensure_unchanged(&self.hdt_path)
    }
}

fn temp_file(temp_dir: &Path, prefix: &str) -> Result<NamedTempFile> {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempfile_in(temp_dir)
        .with_context(|| {
            format!(
                "Failed to create permutation scratch file in {}",
                temp_dir.display()
            )
        })
}

fn finish_bitmap(
    mut file: NamedTempFile,
    encoder: StreamingBitmapEncoder<BufWriter<File>>,
) -> Result<(TempPath, u64)> {
    let (bits, mut writer) = encoder.finish()?;
    writer.flush()?;
    drop(writer);
    file.as_file_mut().sync_data()?;
    Ok((file.into_temp_path(), bits))
}

fn finish_array(
    mut file: NamedTempFile,
    encoder: StreamingLogArrayEncoder<BufWriter<File>>,
) -> Result<(TempPath, u64, u8)> {
    let (entries, width, mut writer) = encoder.finish()?;
    writer.flush()?;
    drop(writer);
    file.as_file_mut().sync_data()?;
    Ok((file.into_temp_path(), entries, width))
}

#[allow(clippy::too_many_arguments)]
fn encode_permutation(
    sorted: impl Iterator<Item = Result<PermEntry>>,
    component: u32,
    expected_triples: u64,
    first_max: u64,
    second_max: u64,
    subject_max: u64,
    include_map: bool,
    temp_dir: &Path,
) -> Result<EncodedPermutation> {
    let ay_file = temp_file(temp_dir, ".hdtc-perm-ay-")?;
    let by_file = temp_file(temp_dir, ".hdtc-perm-by-")?;
    let az_file = temp_file(temp_dir, ".hdtc-perm-az-")?;
    let bz_file = temp_file(temp_dir, ".hdtc-perm-bz-")?;
    let map_file = include_map
        .then(|| temp_file(temp_dir, ".hdtc-perm-map-"))
        .transpose()?;

    let mut ay = StreamingLogArrayEncoder::new(
        id_width(second_max),
        BufWriter::with_capacity(256 * 1024, ay_file.reopen()?),
    );
    let mut by =
        StreamingBitmapEncoder::new(BufWriter::with_capacity(256 * 1024, by_file.reopen()?));
    let mut az = StreamingLogArrayEncoder::new(
        id_width(subject_max),
        BufWriter::with_capacity(256 * 1024, az_file.reopen()?),
    );
    let mut bz =
        StreamingBitmapEncoder::new(BufWriter::with_capacity(256 * 1024, bz_file.reopen()?));
    let mut map = map_file
        .as_ref()
        .map(|file| -> Result<_> {
            Ok(StreamingLogArrayEncoder::new(
                position_width(expected_triples),
                BufWriter::with_capacity(256 * 1024, file.reopen()?),
            ))
        })
        .transpose()?;

    let mut previous: Option<PermEntry> = None;
    let mut triples = 0u64;
    let mut pairs = 0u64;
    for result in sorted {
        let entry = result?;
        ensure!(
            entry.first >= 1 && entry.first <= first_max,
            "permutation level-1 identifier out of range"
        );
        ensure!(
            entry.second >= 1 && entry.second <= second_max,
            "permutation level-2 identifier out of range"
        );
        ensure!(
            entry.third >= 1 && entry.third <= subject_max,
            "permutation subject identifier out of range"
        );
        ensure!(
            entry.spo_position < expected_triples,
            "permutation SPO position out of range"
        );

        match previous {
            None => {
                ensure!(
                    entry.first == 1,
                    "permutation has an empty leading level-1 group"
                );
                ay.push(entry.second)?;
                by.push(false)?;
                az.push(entry.third)?;
                bz.push(false)?;
                pairs = 1;
            }
            Some(prev) if entry.first != prev.first => {
                ensure!(
                    prev.first.checked_add(1) == Some(entry.first),
                    "permutation has an empty level-1 group"
                );
                by.set_last(true);
                bz.set_last(true);
                ay.push(entry.second)?;
                by.push(false)?;
                az.push(entry.third)?;
                bz.push(false)?;
                pairs = pairs
                    .checked_add(1)
                    .context("permutation pair-count overflow")?;
            }
            Some(prev) if entry.second != prev.second => {
                ensure!(
                    entry.second > prev.second,
                    "permutation level-2 values are not strictly increasing"
                );
                bz.set_last(true);
                ay.push(entry.second)?;
                by.push(false)?;
                az.push(entry.third)?;
                bz.push(false)?;
                pairs = pairs
                    .checked_add(1)
                    .context("permutation pair-count overflow")?;
            }
            Some(prev) => {
                ensure!(
                    entry.third > prev.third,
                    "permutation level-3 values are not strictly increasing"
                );
                az.push(entry.third)?;
                bz.push(false)?;
            }
        }
        if let Some(map) = map.as_mut() {
            map.push(entry.spo_position)?;
        }
        previous = Some(entry);
        triples = triples
            .checked_add(1)
            .context("permutation triple-count overflow")?;
    }
    ensure!(
        triples == expected_triples,
        "permutation triple-count mismatch"
    );
    if let Some(last) = previous {
        ensure!(
            last.first == first_max,
            "permutation has an empty trailing level-1 group"
        );
        by.set_last(true);
        bz.set_last(true);
    } else {
        ensure!(
            first_max == 0 && second_max == 0 && subject_max == 0,
            "nonempty dictionary role has no triples"
        );
    }

    let (ay_path, ay_count, ay_width) = finish_array(ay_file, ay)?;
    let (by_path, by_bits) = finish_bitmap(by_file, by)?;
    let (az_path, az_count, az_width) = finish_array(az_file, az)?;
    let (bz_path, bz_bits) = finish_bitmap(bz_file, bz)?;
    ensure!(
        ay_count == pairs && by_bits == pairs,
        "permutation pair-count encoding mismatch"
    );
    ensure!(
        az_count == triples && bz_bits == triples,
        "permutation triple-count encoding mismatch"
    );

    let base = component << 8;
    let mut sections = vec![
        DraftSection {
            section_type: base | 1,
            flags: REQUIRED,
            payload: ay_path,
            entry_count: pairs,
            bits_per_entry: ay_width,
            parameter: 0,
            indexed_bits: 0,
        },
        DraftSection {
            section_type: base | 2,
            flags: REQUIRED,
            payload: by_path,
            entry_count: pairs,
            bits_per_entry: 1,
            parameter: 0,
            indexed_bits: 0,
        },
        DraftSection {
            section_type: base | 3,
            flags: REQUIRED,
            payload: az_path,
            entry_count: triples,
            bits_per_entry: az_width,
            parameter: 0,
            indexed_bits: 0,
        },
        DraftSection {
            section_type: base | 4,
            flags: REQUIRED,
            payload: bz_path,
            entry_count: triples,
            bits_per_entry: 1,
            parameter: 0,
            indexed_bits: 0,
        },
    ];
    let by_dirs = build_rank_directories(
        &RawBitmap {
            path: sections[1].payload.to_path_buf(),
            data_offset: 0,
            bits: pairs,
        },
        temp_dir,
    )?;
    let bz_dirs = build_rank_directories(
        &RawBitmap {
            path: sections[3].payload.to_path_buf(),
            data_offset: 0,
            bits: triples,
        },
        temp_dir,
    )?;
    sections.push(rank_draft(base | 5, by_dirs.0, pairs, true));
    sections.push(rank_draft(base | 6, by_dirs.1, pairs, false));
    sections.push(rank_draft(base | 7, bz_dirs.0, triples, true));
    sections.push(rank_draft(base | 8, bz_dirs.1, triples, false));
    if let (Some(map_file), Some(map)) = (map_file, map) {
        let (path, entries, width) = finish_array(map_file, map)?;
        sections.push(DraftSection {
            section_type: base | 9,
            flags: 0,
            payload: path,
            entry_count: entries,
            bits_per_entry: width,
            parameter: 0,
            indexed_bits: 0,
        });
    }
    Ok(EncodedPermutation {
        sections,
        pair_count: pairs,
    })
}

fn rank_draft(
    section_type: u32,
    payload: TempPath,
    indexed_bits: u64,
    superrank: bool,
) -> DraftSection {
    DraftSection {
        section_type,
        flags: REQUIRED,
        payload,
        entry_count: if superrank {
            super_count(indexed_bits)
        } else {
            sub_count(indexed_bits)
        },
        bits_per_entry: if superrank { 64 } else { 16 },
        parameter: if superrank {
            u64::from(SUPERBLOCK_BITS)
        } else {
            u64::from(SUBBLOCK_BITS)
        },
        indexed_bits,
    }
}

fn build_rank_directories(bitmap: &RawBitmap, temp_dir: &Path) -> Result<(TempPath, TempPath)> {
    let mut super_file = temp_file(temp_dir, ".hdtc-perm-super-")?;
    let mut sub_file = temp_file(temp_dir, ".hdtc-perm-sub-")?;
    if bitmap.bits > 0 {
        let mut reader = BufReader::with_capacity(256 * 1024, File::open(&bitmap.path)?);
        reader.seek(SeekFrom::Start(bitmap.data_offset))?;
        let mut super_writer = BufWriter::with_capacity(64 * 1024, super_file.as_file_mut());
        let mut sub_writer = BufWriter::with_capacity(64 * 1024, sub_file.as_file_mut());
        let blocks = sub_count(bitmap.bits);
        let mut total = 0u64;
        let mut within = 0u16;
        for block in 0..blocks {
            if block.is_multiple_of(8) {
                super_writer.write_all(&total.to_le_bytes())?;
                within = 0;
            }
            sub_writer.write_all(&within.to_le_bytes())?;
            let start_bit = block * u64::from(SUBBLOCK_BITS);
            let bits_here = (bitmap.bits - start_bit).min(u64::from(SUBBLOCK_BITS));
            let bytes_here = bits_here.div_ceil(8) as usize;
            let mut bytes = [0u8; 64];
            reader.read_exact(&mut bytes[..bytes_here])?;
            let count = block_popcount(&bytes[..bytes_here], bits_here)?;
            total = total
                .checked_add(u64::from(count))
                .context("bitmap population overflow")?;
            within = within
                .checked_add(u16::try_from(count).unwrap())
                .context("subrank overflow")?;
            ensure!(
                u32::from(within) <= SUPERBLOCK_BITS,
                "subrank exceeds superblock width"
            );
        }
        super_writer.write_all(&total.to_le_bytes())?;
        super_writer.flush()?;
        sub_writer.flush()?;
    }
    super_file.as_file_mut().sync_data()?;
    sub_file.as_file_mut().sync_data()?;
    Ok((super_file.into_temp_path(), sub_file.into_temp_path()))
}

fn file_crc(path: &Path) -> Result<(u64, u32)> {
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    if length == 0 {
        return Ok((0, 0));
    }
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut digest = CRC32C_ALGO.digest();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok((length, digest.finalize()))
}

struct FinalSection {
    draft: DraftSection,
    offset: u64,
    length: u64,
    crc: u32,
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

#[allow(clippy::too_many_arguments)]
fn assemble(
    output: &Path,
    metadata: &HdtMetadata,
    source_digest: &[u8; 32],
    pos_pairs: u64,
    ops_pairs: u64,
    mut drafts: Vec<DraftSection>,
) -> Result<()> {
    drafts.sort_unstable_by_key(|section| section.section_type);
    let section_count =
        u32::try_from(drafts.len()).context("too many permutation-index sections")?;
    let directory_offset = HEADER_SIZE;
    let directory_length = u64::from(section_count)
        .checked_mul(DIRECTORY_ENTRY_SIZE)
        .and_then(|v| v.checked_add(4))
        .context("directory size overflow")?;
    let mut cursor = align64(
        directory_offset
            .checked_add(directory_length)
            .context("directory offset overflow")?,
    )?;
    let mut sections = Vec::with_capacity(drafts.len());
    for draft in drafts {
        let (length, crc) = file_crc(&draft.payload)?;
        let offset = if length == 0 {
            0
        } else {
            let at = cursor;
            cursor = align64(
                cursor
                    .checked_add(length)
                    .context("section offset overflow")?,
            )?;
            at
        };
        sections.push(FinalSection {
            draft,
            offset,
            length,
            crc,
        });
    }
    let footer_offset = cursor;
    let file_size = footer_offset
        .checked_add(FOOTER_SIZE)
        .context("permutation-index size overflow")?;
    let source_length = metadata
        .file_length
        .checked_sub(metadata.data_offset)
        .context("HDT source-data length underflow")?;

    let mut directory = Vec::with_capacity(sections.len() * 64);
    for section in &sections {
        let mut entry = [0u8; 64];
        put_u32(&mut entry, 0, section.draft.section_type);
        put_u32(&mut entry, 4, section.draft.flags);
        put_u64(&mut entry, 8, section.offset);
        put_u64(&mut entry, 16, section.length);
        put_u64(&mut entry, 24, section.draft.entry_count);
        entry[32] = section.draft.bits_per_entry;
        put_u32(&mut entry, 36, section.crc);
        put_u64(&mut entry, 40, section.draft.parameter);
        put_u64(
            &mut entry,
            48,
            if section.length == 0 {
                0
            } else {
                section.draft.indexed_bits
            },
        );
        directory.extend_from_slice(&entry);
    }

    let mut header = [0u8; 256];
    header[0..8].copy_from_slice(b"$HDTPERM");
    put_u16(&mut header, 8, 1);
    put_u16(&mut header, 10, 0);
    put_u32(&mut header, 12, 256);
    put_u64(&mut header, 16, 0b111);
    put_u64(&mut header, 24, metadata.triples);
    put_u64(&mut header, 32, metadata.subjects);
    put_u64(&mut header, 40, metadata.predicates);
    put_u64(&mut header, 48, metadata.objects);
    put_u64(&mut header, 56, pos_pairs);
    put_u64(&mut header, 64, ops_pairs);
    put_u64(&mut header, 72, source_length);
    put_u64(&mut header, 80, file_size);
    put_u64(&mut header, 88, directory_offset);
    put_u64(&mut header, 96, directory_length);
    put_u32(&mut header, 104, section_count);
    put_u32(&mut header, 108, 1);
    put_u64(&mut header, 112, footer_offset);
    put_u32(&mut header, 120, SUPERBLOCK_BITS);
    put_u32(&mut header, 124, SUBBLOCK_BITS);
    header[128..160].copy_from_slice(source_digest);
    let header_crc = crc32c(&header[..252]);
    put_u32(&mut header, 252, header_crc);

    let mut footer = [0u8; 64];
    footer[0..8].copy_from_slice(b"$HDTPEND");
    put_u16(&mut footer, 8, 1);
    put_u16(&mut footer, 10, 0);
    put_u32(&mut footer, 12, 64);
    put_u64(&mut footer, 16, file_size);
    put_u64(&mut footer, 24, 0);
    put_u64(&mut footer, 32, directory_offset);
    put_u64(&mut footer, 40, directory_length);
    put_u32(&mut footer, 48, section_count);
    put_u32(&mut footer, 56, header_crc);
    let footer_crc = crc32c(&footer[..60]);
    put_u32(&mut footer, 60, footer_crc);

    let mut writer = BufWriter::with_capacity(256 * 1024, File::create(output)?);
    writer.write_all(&header)?;
    writer.write_all(&directory)?;
    writer.write_all(&crc32c(&directory).to_le_bytes())?;
    write_zeros_to(&mut writer, align64(directory_offset + directory_length)?)?;
    for section in &sections {
        if section.length == 0 {
            continue;
        }
        write_zeros_to(&mut writer, section.offset)?;
        let mut payload = BufReader::with_capacity(256 * 1024, File::open(&section.draft.payload)?);
        std::io::copy(&mut payload, &mut writer)?;
    }
    write_zeros_to(&mut writer, footer_offset)?;
    writer.write_all(&footer)?;
    writer.flush()?;
    // The caller renames this file into the canonical `.perm` name, and a
    // normal open verifies metadata but not payload checksums. Reaching disk
    // before the rename is what keeps a crash from publishing a well-formed
    // header over payload bytes that never landed.
    writer
        .get_ref()
        .sync_all()
        .with_context(|| format!("Failed to flush permutation index {}", output.display()))?;
    ensure!(
        std::fs::metadata(output)?.len() == file_size,
        "written permutation-index size mismatch"
    );
    Ok(())
}

fn write_zeros_to(writer: &mut BufWriter<File>, offset: u64) -> Result<()> {
    let current = writer.stream_position()?;
    ensure!(current <= offset, "permutation-index regions overlap");
    let mut remaining = offset - current;
    let zeros = [0u8; 64];
    while remaining > 0 {
        let count = remaining.min(64) as usize;
        writer.write_all(&zeros[..count])?;
        remaining -= count as u64;
    }
    Ok(())
}

fn raw_bitmap_from_temp(path: &Path, bits: u64) -> RawBitmap {
    RawBitmap {
        path: path.to_path_buf(),
        data_offset: 0,
        bits,
    }
}

fn finish_collector(
    mut collector: PermutationCollector,
    output: &Path,
    metadata: &HdtMetadata,
    source_digest: &[u8; 32],
    spo_y: &RawBitmap,
    spo_z: &RawBitmap,
) -> Result<()> {
    ensure!(
        collector.count == metadata.triples,
        "prepared permutation triple-count mismatch"
    );
    ensure!(
        collector.last_source_subject == metadata.subjects,
        "SPO subject groups do not cover the dictionary subject identifier space"
    );
    let pos_sorted = collector.pos_sorter.finish(&mut collector.pos_buffer)?;
    let pos = encode_permutation(
        pos_sorted,
        1,
        metadata.triples,
        metadata.predicates,
        metadata.objects,
        metadata.subjects,
        collector.maps.pos,
        &collector.temp_dir,
    )?;
    drop(collector.pos_sorter);
    let ops_sorted = collector.ops_sorter.finish(&mut collector.ops_buffer)?;
    let ops = encode_permutation(
        ops_sorted,
        2,
        metadata.triples,
        metadata.objects,
        metadata.predicates,
        metadata.subjects,
        collector.maps.ops,
        &collector.temp_dir,
    )?;
    drop(collector.ops_sorter);

    let spo_y_dirs = build_rank_directories(spo_y, &collector.temp_dir)?;
    let spo_z_dirs = build_rank_directories(spo_z, &collector.temp_dir)?;
    let mut drafts = pos.sections;
    drafts.extend(ops.sections);
    drafts.push(rank_draft(
        SPO_Y_SUPER,
        spo_y_dirs.0,
        metadata.sp_pairs,
        true,
    ));
    drafts.push(rank_draft(
        SPO_Y_SUB,
        spo_y_dirs.1,
        metadata.sp_pairs,
        false,
    ));
    drafts.push(rank_draft(
        SPO_Z_SUPER,
        spo_z_dirs.0,
        metadata.triples,
        true,
    ));
    drafts.push(rank_draft(SPO_Z_SUB, spo_z_dirs.1, metadata.triples, false));
    assemble(
        output,
        metadata,
        source_digest,
        pos.pair_count,
        ops.pair_count,
        drafts,
    )
}

pub fn create_permutation_index(
    hdt_path: &Path,
    memory_budget: usize,
    temp_dir: &Path,
    maps: PositionMaps,
) -> Result<PathBuf> {
    let source = SourceIdentity::capture(hdt_path)?;
    let metadata = scan_hdt(hdt_path)?;
    let mut collector = PermutationCollector::new(temp_dir, memory_budget, maps);
    let mut scanner = BitmapTriplesScanner::new(&metadata.offsets, hdt_path)?;
    while let Some((subject, predicate, object)) = scanner.next_triple()? {
        collector.push(IdTriple {
            subject,
            predicate,
            object,
        })?;
    }
    scanner.finish()?;

    let output = canonical_path(hdt_path);
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let temp = tempfile::Builder::new()
        .prefix(".hdtc-perm-output-")
        .tempfile_in(parent)?
        .into_temp_path();
    finish_collector(
        collector,
        &temp,
        &metadata,
        source.digest(),
        &metadata.bitmap_y,
        &metadata.bitmap_z,
    )?;
    source.ensure_unchanged(hdt_path)?;
    temp.persist(&output)
        .map_err(|error| error.error)
        .with_context(|| format!("Failed to publish permutation index {}", output.display()))?;
    Ok(output)
}

pub fn finish_prepared_index(
    mut collector: PermutationCollector,
    output: &Path,
    hdt_path: &Path,
    counts: &DictCounts,
    triples: &BitmapTriplesFiles,
) -> Result<()> {
    ensure!(
        collector.count == triples.num_triples,
        "prepared permutation triple-count mismatch"
    );
    let mut assembler = PreparedPermutationAssembler::new(
        hdt_path,
        counts,
        triples,
        &collector.temp_dir,
        collector.maps,
    )?;
    let pos = collector.pos_sorter.finish(&mut collector.pos_buffer)?;
    assembler.encode_pos(pos)?;
    drop(collector.pos_sorter);
    let ops = collector.ops_sorter.finish(&mut collector.ops_buffer)?;
    assembler.encode_ops(ops)?;
    drop(collector.ops_sorter);
    assembler.finish(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dictionary::pfc::PfcEncoder;
    use std::io::Cursor;

    #[test]
    fn structural_pfc_skip_preserves_counts_and_next_section_position() {
        for terms in [&[][..], &["alpha", "beta", "gamma"][..]] {
            let mut encoder = PfcEncoder::with_block_size(2);
            for term in terms {
                encoder.push(*term);
            }
            let mut section = Vec::new();
            encoder.write_to(&mut section).unwrap();
            let section_length = section.len() as u64;
            section.extend_from_slice(b"next");

            let mut reader = Cursor::new(section);
            assert_eq!(
                skip_pfc_section(&mut reader, "test").unwrap(),
                terms.len() as u64
            );
            assert_eq!(reader.position(), section_length);
            let mut next = [0u8; 4];
            reader.read_exact(&mut next).unwrap();
            assert_eq!(&next, b"next");
        }
    }
}
