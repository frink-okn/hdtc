use super::builder::{HdtMetadata, PermEntry, PermutationCollector, PositionMaps, scan_hdt};
use super::format::*;
use crate::hdt::reader::{BitmapTriplesScanner, sha256_to_end};
use crate::io::crc_utils::CRC32C_ALGO;
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result, bail, ensure};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct PermutationIndex {
    path: PathBuf,
    header: Header,
    sections: Vec<Section>,
}

impl PermutationIndex {
    /// Open the canonical sidecar for query use. This validates all fixed
    /// metadata and the cheap source binding (dictionary counts, triple count,
    /// and suffix length); strict validation additionally hashes both files and
    /// checks every payload and semantic invariant.
    pub fn open(path: &Path, hdt_path: &Path) -> Result<Self> {
        let (header, sections) = read_header_and_sections(path)?;
        let hdt = scan_hdt(hdt_path)?;
        validate_source_metadata(&header, &hdt)?;
        validate_section_metadata(&header, &sections, false)?;
        validate_spo_shapes(&sections, &hdt)?;
        validate_canonical_regions(path, &header, &sections, false)?;
        Ok(Self {
            path: path.to_path_buf(),
            header,
            sections,
        })
    }

    fn section(&self, section_type: u32) -> Result<&Section> {
        self.sections
            .binary_search_by_key(&section_type, |section| section.section_type)
            .map(|index| &self.sections[index])
            .map_err(|_| anyhow::anyhow!("missing permutation-index section {section_type:#06x}"))
    }

    /// Stream an index-side triple pattern. At least one of predicate/object
    /// must be bound. POS is used whenever the predicate is known; otherwise
    /// OPS is used for the object-rooted pattern.
    pub fn triples(&self, predicate: Option<u64>, object: Option<u64>) -> Result<IndexedTriples> {
        ensure!(
            predicate.is_some() || object.is_some(),
            "permutation lookup needs a predicate or object"
        );
        let (component, first, second_filter, pair_count) = if let Some(predicate) = predicate {
            (1u32, predicate, object, self.header.pos_pairs)
        } else {
            (2u32, object.unwrap(), None, self.header.ops_pairs)
        };
        let first_max = if component == 1 {
            self.header.predicates
        } else {
            self.header.objects
        };
        if first == 0 || first > first_max || self.header.triples == 0 {
            return IndexedTriples::empty(self.path.clone(), component, first);
        }

        let base = component << 8;
        let array_y = self.section(base | 1)?.clone();
        let bitmap_y = self.section(base | 2)?;
        let array_z = self.section(base | 3)?.clone();
        let bitmap_z = self.section(base | 4)?.clone();
        let y_super = self.section(base | 5)?;
        let y_sub = self.section(base | 6)?;
        let z_super = self.section(base | 7)?;
        let z_sub = self.section(base | 8)?;
        let mut file = File::open(&self.path)?;
        let mut y_start = if first == 1 {
            0
        } else {
            select1(&mut file, bitmap_y, y_super, y_sub, first - 2)? + 1
        };
        let mut y_end = select1(&mut file, bitmap_y, y_super, y_sub, first - 1)? + 1;
        ensure!(
            y_end <= pair_count && y_start < y_end,
            "invalid permutation level-1 group"
        );

        if let Some(target) = second_filter {
            match binary_search_packed(&mut file, &array_y, y_start, y_end, target)? {
                Some(position) => {
                    y_start = position;
                    y_end = position + 1;
                }
                None => return IndexedTriples::empty(self.path.clone(), component, first),
            }
        }
        let z_start = if y_start == 0 {
            0
        } else {
            select1(&mut file, &bitmap_z, z_super, z_sub, y_start - 1)? + 1
        };
        let z_end = select1(&mut file, &bitmap_z, z_super, z_sub, y_end - 1)? + 1;
        IndexedTriples::new(
            self.path.clone(),
            component,
            first,
            array_y,
            bitmap_z,
            array_z,
            y_start,
            y_end,
            z_start,
            z_end,
        )
    }
}

fn validate_source_metadata(header: &Header, hdt: &HdtMetadata) -> Result<()> {
    ensure!(
        header.triples == hdt.triples,
        "permutation/HDT triple-count mismatch"
    );
    ensure!(
        header.subjects == hdt.subjects,
        "permutation/HDT subject-count mismatch"
    );
    ensure!(
        header.predicates == hdt.predicates,
        "permutation/HDT predicate-count mismatch"
    );
    ensure!(
        header.objects == hdt.objects,
        "permutation/HDT object-count mismatch"
    );
    ensure!(
        header.source_data_length == hdt.file_length - hdt.data_offset,
        "permutation/HDT data-length mismatch"
    );
    Ok(())
}

fn expected_core() -> [u32; 20] {
    [
        POS_ARRAY_Y,
        POS_BITMAP_Y,
        POS_ARRAY_Z,
        POS_BITMAP_Z,
        POS_Y_SUPER,
        POS_Y_SUB,
        POS_Z_SUPER,
        POS_Z_SUB,
        OPS_ARRAY_Y,
        OPS_BITMAP_Y,
        OPS_ARRAY_Z,
        OPS_BITMAP_Z,
        OPS_Y_SUPER,
        OPS_Y_SUB,
        OPS_Z_SUPER,
        OPS_Z_SUB,
        SPO_Y_SUPER,
        SPO_Y_SUB,
        SPO_Z_SUPER,
        SPO_Z_SUB,
    ]
}

fn expected_shape(header: &Header, section_type: u32) -> Result<(u64, u8, u64, u64)> {
    let n = header.triples;
    let shape = match section_type {
        POS_ARRAY_Y => (header.pos_pairs, id_width(header.objects), 0, 0),
        POS_BITMAP_Y => (header.pos_pairs, 1, 0, 0),
        POS_ARRAY_Z => (n, id_width(header.subjects), 0, 0),
        POS_BITMAP_Z => (n, 1, 0, 0),
        OPS_ARRAY_Y => (header.ops_pairs, id_width(header.predicates), 0, 0),
        OPS_BITMAP_Y => (header.ops_pairs, 1, 0, 0),
        OPS_ARRAY_Z => (n, id_width(header.subjects), 0, 0),
        OPS_BITMAP_Z => (n, 1, 0, 0),
        POS_MAP | OPS_MAP => (n, position_width(n), 0, 0),
        POS_Y_SUPER => (
            super_count(header.pos_pairs),
            64,
            u64::from(SUPERBLOCK_BITS),
            header.pos_pairs,
        ),
        POS_Y_SUB => (
            sub_count(header.pos_pairs),
            16,
            u64::from(SUBBLOCK_BITS),
            header.pos_pairs,
        ),
        POS_Z_SUPER | OPS_Z_SUPER | SPO_Z_SUPER => {
            (super_count(n), 64, u64::from(SUPERBLOCK_BITS), n)
        }
        POS_Z_SUB | OPS_Z_SUB | SPO_Z_SUB => (sub_count(n), 16, u64::from(SUBBLOCK_BITS), n),
        OPS_Y_SUPER => (
            super_count(header.ops_pairs),
            64,
            u64::from(SUPERBLOCK_BITS),
            header.ops_pairs,
        ),
        OPS_Y_SUB => (
            sub_count(header.ops_pairs),
            16,
            u64::from(SUBBLOCK_BITS),
            header.ops_pairs,
        ),
        SPO_Y_SUPER | SPO_Y_SUB => bail!("SPO-Y directory shape needs the HDT pair count"),
        _ => bail!("unknown permutation-index section {section_type:#06x}"),
    };
    Ok(shape)
}

fn validate_section_metadata(
    header: &Header,
    sections: &[Section],
    strict_unknown: bool,
) -> Result<()> {
    let present: HashSet<u32> = sections
        .iter()
        .map(|section| section.section_type)
        .collect();
    for section_type in expected_core() {
        ensure!(
            present.contains(&section_type),
            "missing core permutation-index section {section_type:#06x}"
        );
    }
    for section in sections {
        ensure!(section.flags & !REQUIRED == 0, "unknown section flags");
        if section.length == 0 {
            ensure!(
                section.offset == 0 && section.payload_crc == 0,
                "empty section has an offset or checksum"
            );
        }
        if is_core(section.section_type) {
            ensure!(
                section.flags == REQUIRED,
                "core section is not marked REQUIRED"
            );
        } else if matches!(section.section_type, POS_MAP | OPS_MAP) {
            ensure!(
                section.flags == 0,
                "optional position map is marked REQUIRED"
            );
        } else if section.flags & REQUIRED != 0 {
            bail!("unknown required section {:#06x}", section.section_type);
        } else if strict_unknown {
            bail!(
                "optional section {:#06x} is not understood and cannot be strictly validated",
                section.section_type
            );
        } else {
            continue;
        }
        if !matches!(section.section_type, SPO_Y_SUPER | SPO_Y_SUB) {
            let (count, width, parameter, indexed) = expected_shape(header, section.section_type)?;
            ensure!(
                section.entry_count == count,
                "section {:#06x} entry-count mismatch",
                section.section_type
            );
            ensure!(
                section.bits_per_entry == width,
                "section {:#06x} width mismatch",
                section.section_type
            );
            ensure!(
                section.parameter == parameter,
                "section {:#06x} parameter mismatch",
                section.section_type
            );
            let expected_indexed = if count == 0 { 0 } else { indexed };
            ensure!(
                section.indexed_bits == expected_indexed,
                "section {:#06x} indexed-length mismatch",
                section.section_type
            );
            ensure!(
                section.length == packed_len(count, width)?,
                "section {:#06x} payload-length mismatch",
                section.section_type
            );
        }
    }
    Ok(())
}

fn validate_spo_shapes(sections: &[Section], hdt: &HdtMetadata) -> Result<()> {
    for (section_type, indexed, superrank) in [
        (SPO_Y_SUPER, hdt.sp_pairs, true),
        (SPO_Y_SUB, hdt.sp_pairs, false),
        (SPO_Z_SUPER, hdt.triples, true),
        (SPO_Z_SUB, hdt.triples, false),
    ] {
        let section = sections
            .iter()
            .find(|s| s.section_type == section_type)
            .unwrap();
        let count = if superrank {
            super_count(indexed)
        } else {
            sub_count(indexed)
        };
        let width = if superrank { 64 } else { 16 };
        let parameter = if superrank {
            SUPERBLOCK_BITS
        } else {
            SUBBLOCK_BITS
        };
        ensure!(
            section.entry_count == count,
            "SPO rank entry-count mismatch"
        );
        ensure!(section.bits_per_entry == width, "SPO rank width mismatch");
        ensure!(
            section.parameter == u64::from(parameter),
            "SPO rank parameter mismatch"
        );
        ensure!(
            section.indexed_bits == if count == 0 { 0 } else { indexed },
            "SPO indexed-length mismatch"
        );
        ensure!(
            section.length == packed_len(count, width)?,
            "SPO rank payload-length mismatch"
        );
    }
    Ok(())
}

fn validate_canonical_regions(
    path: &Path,
    header: &Header,
    sections: &[Section],
    checksums: bool,
) -> Result<()> {
    let directory_end = header
        .directory_offset
        .checked_add(header.directory_length)
        .context("directory end overflow")?;
    let mut cursor = align64(directory_end)?;
    validate_zero_range(path, directory_end, cursor)?;
    for section in sections {
        if section.length == 0 {
            continue;
        }
        ensure!(
            section.offset == cursor && section.offset % 64 == 0,
            "section {:#06x} is not tightly packed and aligned",
            section.section_type
        );
        let end = section
            .offset
            .checked_add(section.length)
            .context("section end overflow")?;
        if checksums {
            ensure!(
                crc_file_range(path, section.offset, section.length)? == section.payload_crc,
                "section {:#06x} CRC32C mismatch",
                section.section_type
            );
            validate_tail_bits(path, section)?;
        }
        let aligned = align64(end)?;
        validate_zero_range(path, end, aligned)?;
        cursor = aligned;
    }
    ensure!(
        cursor == header.footer_offset,
        "footer is not tightly packed after payloads"
    );
    Ok(())
}

fn validate_zero_range(path: &Path, start: u64, end: u64) -> Result<()> {
    ensure!(start <= end, "invalid padding range");
    if start == end {
        return Ok(());
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut remaining = end - start;
    let mut buffer = [0u8; 4096];
    while remaining > 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..count])?;
        ensure!(
            buffer[..count].iter().all(|&byte| byte == 0),
            "nonzero alignment padding"
        );
        remaining -= count as u64;
    }
    Ok(())
}

fn crc_file_range(path: &Path, offset: u64, length: u64) -> Result<u32> {
    if length == 0 {
        return Ok(0);
    }
    let mut file = BufReader::with_capacity(256 * 1024, File::open(path)?);
    file.seek(SeekFrom::Start(offset))?;
    let mut limited = file.take(length);
    let mut digest = CRC32C_ALGO.digest();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = limited.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(digest.finalize())
}

fn validate_tail_bits(path: &Path, section: &Section) -> Result<()> {
    if section.length == 0 || !(is_array(section.section_type) || is_bitmap(section.section_type)) {
        return Ok(());
    }
    let used_bits = section
        .entry_count
        .checked_mul(u64::from(section.bits_per_entry))
        .context("section bit-length overflow")?;
    let remainder = (used_bits % 8) as u8;
    if remainder == 0 {
        return Ok(());
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(section.offset + section.length - 1))?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)?;
    ensure!(
        byte[0] >> remainder == 0,
        "section {:#06x} has nonzero unused high bits",
        section.section_type
    );
    Ok(())
}

fn read_packed_at(file: &mut File, section: &Section, index: u64) -> Result<u64> {
    ensure!(
        index < section.entry_count,
        "packed-array index out of range"
    );
    let width = u64::from(section.bits_per_entry);
    if width == 0 {
        return Ok(0);
    }
    let bit = index
        .checked_mul(width)
        .context("packed-array offset overflow")?;
    let byte_offset = bit / 8;
    let shift = (bit % 8) as u32;
    let bytes_needed = ((u64::from(shift) + width).div_ceil(8)) as usize;
    let mut bytes = [0u8; 9];
    file.seek(SeekFrom::Start(section.offset + byte_offset))?;
    file.read_exact(&mut bytes[..bytes_needed])?;
    let mut value = 0u128;
    for (position, byte) in bytes[..bytes_needed].iter().enumerate() {
        value |= u128::from(*byte) << (position * 8);
    }
    let shifted = value >> shift;
    let mask = if width == 64 {
        u128::from(u64::MAX)
    } else {
        (1u128 << width) - 1
    };
    Ok((shifted & mask) as u64)
}

fn binary_search_packed(
    file: &mut File,
    section: &Section,
    mut low: u64,
    mut high: u64,
    target: u64,
) -> Result<Option<u64>> {
    while low < high {
        let middle = low + (high - low) / 2;
        match read_packed_at(file, section, middle)?.cmp(&target) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Some(middle)),
        }
    }
    Ok(None)
}

fn rank1(
    file: &mut File,
    bitmap: &Section,
    superrank: &Section,
    subrank: &Section,
    position: u64,
) -> Result<u64> {
    ensure!(position <= bitmap.entry_count, "rank position out of range");
    if bitmap.entry_count == 0 {
        return Ok(0);
    }
    if position == bitmap.entry_count {
        return read_packed_at(file, superrank, superrank.entry_count - 1);
    }
    let super_index = position / u64::from(SUPERBLOCK_BITS);
    let sub_index = position / u64::from(SUBBLOCK_BITS);
    let base =
        read_packed_at(file, superrank, super_index)? + read_packed_at(file, subrank, sub_index)?;
    let start = sub_index * u64::from(SUBBLOCK_BITS);
    let bit_count = position - start;
    let byte_count = bit_count.div_ceil(8) as usize;
    if byte_count == 0 {
        return Ok(base);
    }
    let mut bytes = [0u8; 64];
    file.seek(SeekFrom::Start(bitmap.offset + start / 8))?;
    file.read_exact(&mut bytes[..byte_count])?;
    if !bit_count.is_multiple_of(8) {
        let mask = (1u8 << (bit_count % 8)) - 1;
        bytes[byte_count - 1] &= mask;
    }
    Ok(base
        + bytes[..byte_count]
            .iter()
            .map(|byte| u64::from(byte.count_ones()))
            .sum::<u64>())
}

fn select1(
    file: &mut File,
    bitmap: &Section,
    superrank: &Section,
    subrank: &Section,
    ordinal: u64,
) -> Result<u64> {
    let population = rank1(file, bitmap, superrank, subrank, bitmap.entry_count)?;
    ensure!(ordinal < population, "select ordinal out of range");
    let mut low = 0u64;
    let mut high = bitmap.entry_count;
    while low < high {
        let middle = low + (high - low) / 2;
        if rank1(file, bitmap, superrank, subrank, middle + 1)? > ordinal {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    Ok(low)
}

struct BarePackedReader {
    reader: BufReader<File>,
    remaining: u64,
    width: u8,
    buffer: u128,
    buffered_bits: u32,
}

impl BarePackedReader {
    fn new(path: &Path, section: &Section, start: u64, count: u64) -> Result<Self> {
        ensure!(
            start
                .checked_add(count)
                .is_some_and(|end| end <= section.entry_count),
            "packed-reader range out of bounds"
        );
        let bit = start
            .checked_mul(u64::from(section.bits_per_entry))
            .context("packed-reader offset overflow")?;
        let mut reader = BufReader::with_capacity(256 * 1024, File::open(path)?);
        reader.seek(SeekFrom::Start(section.offset + bit / 8))?;
        let skipped = (bit % 8) as u32;
        let mut this = Self {
            reader,
            remaining: count,
            width: section.bits_per_entry,
            buffer: 0,
            buffered_bits: 0,
        };
        if skipped > 0 {
            let mut byte = [0u8; 1];
            this.reader.read_exact(&mut byte)?;
            this.buffer = u128::from(byte[0] >> skipped);
            this.buffered_bits = 8 - skipped;
        }
        Ok(this)
    }

    fn next_value(&mut self) -> Result<Option<u64>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        if self.width == 0 {
            self.remaining -= 1;
            return Ok(Some(0));
        }
        while self.buffered_bits < u32::from(self.width) {
            let mut byte = [0u8; 1];
            self.reader.read_exact(&mut byte)?;
            self.buffer |= u128::from(byte[0]) << self.buffered_bits;
            self.buffered_bits += 8;
        }
        let mask = if self.width == 64 {
            u128::from(u64::MAX)
        } else {
            (1u128 << self.width) - 1
        };
        let value = (self.buffer & mask) as u64;
        self.buffer >>= self.width;
        self.buffered_bits -= u32::from(self.width);
        self.remaining -= 1;
        Ok(Some(value))
    }
}

struct BareBitmapReader {
    reader: BarePackedReader,
}

impl BareBitmapReader {
    fn new(path: &Path, section: &Section, start: u64, count: u64) -> Result<Self> {
        Ok(Self {
            reader: BarePackedReader::new(path, section, start, count)?,
        })
    }
    fn next_bit(&mut self) -> Result<Option<bool>> {
        Ok(self.reader.next_value()?.map(|value| value != 0))
    }
}

pub struct IndexedTriples {
    component: u32,
    first: u64,
    y_remaining: u64,
    z_remaining: u64,
    current_second: u64,
    array_y: Option<BarePackedReader>,
    array_z: Option<BarePackedReader>,
    bitmap_z: Option<BareBitmapReader>,
}

impl IndexedTriples {
    fn empty(_path: PathBuf, component: u32, first: u64) -> Result<Self> {
        Ok(Self {
            component,
            first,
            y_remaining: 0,
            z_remaining: 0,
            current_second: 0,
            array_y: None,
            array_z: None,
            bitmap_z: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        path: PathBuf,
        component: u32,
        first: u64,
        array_y: Section,
        bitmap_z: Section,
        array_z: Section,
        y_start: u64,
        y_end: u64,
        z_start: u64,
        z_end: u64,
    ) -> Result<Self> {
        let mut ay = BarePackedReader::new(&path, &array_y, y_start, y_end - y_start)?;
        let current_second = ay.next_value()?.context("empty permutation Y range")?;
        Ok(Self {
            component,
            first,
            y_remaining: y_end - y_start,
            z_remaining: z_end - z_start,
            current_second,
            array_y: Some(ay),
            array_z: Some(BarePackedReader::new(
                &path,
                &array_z,
                z_start,
                z_end - z_start,
            )?),
            bitmap_z: Some(BareBitmapReader::new(
                &path,
                &bitmap_z,
                z_start,
                z_end - z_start,
            )?),
        })
    }

    pub fn len(&self) -> u64 {
        self.z_remaining
    }
}

impl Iterator for IndexedTriples {
    type Item = Result<(u64, u64, u64)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.z_remaining == 0 {
            return None;
        }
        let result = (|| {
            let subject = self
                .array_z
                .as_mut()
                .unwrap()
                .next_value()?
                .context("permutation ArrayZ ended early")?;
            let end_group = self
                .bitmap_z
                .as_mut()
                .unwrap()
                .next_bit()?
                .context("permutation BitmapZ ended early")?;
            let triple = if self.component == 1 {
                (subject, self.first, self.current_second)
            } else {
                (subject, self.current_second, self.first)
            };
            self.z_remaining -= 1;
            if end_group {
                self.y_remaining -= 1;
                if self.y_remaining > 0 {
                    self.current_second = self
                        .array_y
                        .as_mut()
                        .unwrap()
                        .next_value()?
                        .context("permutation ArrayY ended early")?;
                }
            }
            Ok(triple)
        })();
        Some(result)
    }
}

struct FullDecoder {
    remaining: u64,
    first: u64,
    current_second: u64,
    array_y: BarePackedReader,
    bitmap_y: BareBitmapReader,
    array_z: BarePackedReader,
    bitmap_z: BareBitmapReader,
}

impl FullDecoder {
    fn new(index: &PermutationIndex, component: u32) -> Result<Self> {
        let base = component << 8;
        let ay = index.section(base | 1)?;
        let by = index.section(base | 2)?;
        let az = index.section(base | 3)?;
        let bz = index.section(base | 4)?;
        let mut array_y = BarePackedReader::new(&index.path, ay, 0, ay.entry_count)?;
        let current_second = if ay.entry_count > 0 {
            array_y.next_value()?.context("empty ArrayY")?
        } else {
            0
        };
        Ok(Self {
            remaining: az.entry_count,
            first: if az.entry_count > 0 { 1 } else { 0 },
            current_second,
            array_y,
            bitmap_y: BareBitmapReader::new(&index.path, by, 0, by.entry_count)?,
            array_z: BarePackedReader::new(&index.path, az, 0, az.entry_count)?,
            bitmap_z: BareBitmapReader::new(&index.path, bz, 0, bz.entry_count)?,
        })
    }

    fn next_entry(&mut self) -> Result<Option<(PermEntry, bool, bool)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let third = self.array_z.next_value()?.context("ArrayZ ended early")?;
        let end_z = self.bitmap_z.next_bit()?.context("BitmapZ ended early")?;
        let entry = PermEntry {
            first: self.first,
            second: self.current_second,
            third,
            spo_position: 0,
        };
        self.remaining -= 1;
        let mut end_y = false;
        if end_z {
            end_y = self.bitmap_y.next_bit()?.context("BitmapY ended early")?;
            if self.remaining > 0 {
                self.current_second = self.array_y.next_value()?.context("ArrayY ended early")?;
                if end_y {
                    self.first += 1;
                }
            }
        }
        Ok(Some((entry, end_y, end_z)))
    }
}

fn validate_rank(
    index_path: &Path,
    bitmap_path: &Path,
    bitmap_offset: u64,
    bits: u64,
    superrank: &Section,
    subrank: &Section,
) -> Result<()> {
    if bits == 0 {
        return Ok(());
    }
    let mut bitmap = BufReader::with_capacity(256 * 1024, File::open(bitmap_path)?);
    bitmap.seek(SeekFrom::Start(bitmap_offset))?;
    let mut index_file = File::open(index_path)?;
    let mut total = 0u64;
    let mut within = 0u64;
    for block in 0..sub_count(bits) {
        if block.is_multiple_of(8) {
            ensure!(
                read_packed_at(&mut index_file, superrank, block / 8)? == total,
                "superrank value mismatch"
            );
            within = 0;
        }
        ensure!(
            read_packed_at(&mut index_file, subrank, block)? == within,
            "subrank value mismatch"
        );
        let start = block * u64::from(SUBBLOCK_BITS);
        let bits_here = (bits - start).min(u64::from(SUBBLOCK_BITS));
        let bytes_here = bits_here.div_ceil(8) as usize;
        let mut bytes = [0u8; 64];
        bitmap.read_exact(&mut bytes[..bytes_here])?;
        let count = bytes[..bytes_here]
            .iter()
            .map(|byte| u64::from(byte.count_ones()))
            .sum::<u64>();
        total += count;
        within += count;
    }
    ensure!(
        read_packed_at(&mut index_file, superrank, superrank.entry_count - 1)? == total,
        "superrank sentinel mismatch"
    );
    Ok(())
}

fn validate_permutation_semantics(
    index: &PermutationIndex,
    component: u32,
    sorted: impl Iterator<Item = Result<PermEntry>>,
) -> Result<()> {
    let mut decoded = FullDecoder::new(index, component)?;
    let map_type = (component << 8) | 9;
    let mut map = index
        .sections
        .iter()
        .find(|section| section.section_type == map_type)
        .map(|section| BarePackedReader::new(&index.path, section, 0, section.entry_count))
        .transpose()?;
    let first_max = if component == 1 {
        index.header.predicates
    } else {
        index.header.objects
    };
    let second_max = if component == 1 {
        index.header.objects
    } else {
        index.header.predicates
    };
    let mut previous: Option<PermEntry> = None;
    let mut count = 0u64;
    let mut pair_boundaries = 0u64;
    let mut level1_boundaries = 0u64;
    let mut terminal_z = false;
    let mut terminal_y = false;
    for expected in sorted {
        let expected = expected?;
        let (actual, end_y, end_z) = decoded
            .next_entry()?
            .context("permutation ended before source sort")?;
        ensure!(
            (actual.first, actual.second, actual.third)
                == (expected.first, expected.second, expected.third),
            "permutation triple differs from the HDT source"
        );
        ensure!(
            actual.first >= 1 && actual.first <= first_max,
            "level-1 identifier out of range"
        );
        ensure!(
            actual.second >= 1 && actual.second <= second_max,
            "level-2 identifier out of range"
        );
        ensure!(
            actual.third >= 1 && actual.third <= index.header.subjects,
            "subject identifier out of range"
        );
        if let Some(previous) = previous {
            ensure!(
                actual.first > previous.first
                    || (actual.first == previous.first && actual.second > previous.second)
                    || (actual.first == previous.first
                        && actual.second == previous.second
                        && actual.third > previous.third),
                "permutation groups are not strictly increasing"
            );
            ensure!(
                previous
                    .first
                    .checked_add(1)
                    .is_some_and(|next| actual.first <= next),
                "permutation contains an empty level-1 group"
            );
        } else if index.header.triples > 0 {
            ensure!(
                actual.first == 1,
                "permutation contains an empty leading group"
            );
        }
        if let Some(map) = map.as_mut() {
            ensure!(
                map.next_value()?.context("position map ended early")? == expected.spo_position,
                "position map does not identify the matching SPO triple"
            );
        }
        if end_z {
            pair_boundaries += 1;
        }
        if end_y {
            level1_boundaries += 1;
        }
        terminal_z = end_z;
        terminal_y = end_y;
        previous = Some(actual);
        count += 1;
    }
    ensure!(
        decoded.next_entry()?.is_none(),
        "permutation has triples absent from source"
    );
    ensure!(
        count == index.header.triples,
        "permutation triple-count mismatch"
    );
    if count > 0 {
        ensure!(
            previous.unwrap().first == first_max,
            "permutation contains an empty trailing group"
        );
        ensure!(
            terminal_z && terminal_y,
            "permutation terminal boundary bit is clear"
        );
    }
    let expected_pairs = if component == 1 {
        index.header.pos_pairs
    } else {
        index.header.ops_pairs
    };
    ensure!(
        pair_boundaries == expected_pairs,
        "BitmapZ population count mismatch"
    );
    ensure!(
        level1_boundaries == first_max,
        "BitmapY population count mismatch"
    );
    Ok(())
}

/// Strictly validate a `.perm` sidecar against its HDT using bounded memory.
pub fn validate_permutation_index(
    path: &Path,
    hdt_path: &Path,
    temp_dir: &Path,
    memory_budget: usize,
) -> Result<()> {
    let (header, sections) = read_header_and_sections(path)?;
    let hdt = scan_hdt(hdt_path)?;
    validate_source_metadata(&header, &hdt)?;
    validate_section_metadata(&header, &sections, true)?;
    validate_spo_shapes(&sections, &hdt)?;
    validate_canonical_regions(path, &header, &sections, true)?;

    let mut source = BufReader::with_capacity(256 * 1024, File::open(hdt_path)?);
    source.seek(SeekFrom::Start(hdt.data_offset))?;
    ensure!(
        sha256_to_end(&mut source)? == header.source_digest,
        "permutation/HDT SHA-256 binding mismatch"
    );

    let index = PermutationIndex {
        path: path.to_path_buf(),
        header,
        sections,
    };
    for (bitmap_type, super_type, sub_type) in [
        (POS_BITMAP_Y, POS_Y_SUPER, POS_Y_SUB),
        (POS_BITMAP_Z, POS_Z_SUPER, POS_Z_SUB),
        (OPS_BITMAP_Y, OPS_Y_SUPER, OPS_Y_SUB),
        (OPS_BITMAP_Z, OPS_Z_SUPER, OPS_Z_SUB),
    ] {
        let bitmap = index.section(bitmap_type)?;
        validate_rank(
            &index.path,
            &index.path,
            bitmap.offset,
            bitmap.entry_count,
            index.section(super_type)?,
            index.section(sub_type)?,
        )?;
    }
    validate_rank(
        &index.path,
        &hdt.bitmap_y.path,
        hdt.bitmap_y.data_offset,
        hdt.bitmap_y.bits,
        index.section(SPO_Y_SUPER)?,
        index.section(SPO_Y_SUB)?,
    )?;
    validate_rank(
        &index.path,
        &hdt.bitmap_z.path,
        hdt.bitmap_z.data_offset,
        hdt.bitmap_z.bits,
        index.section(SPO_Z_SUPER)?,
        index.section(SPO_Z_SUB)?,
    )?;

    let mut collector = PermutationCollector::new(temp_dir, memory_budget, PositionMaps::default());
    let mut scanner = BitmapTriplesScanner::new(&hdt.offsets, hdt_path)?;
    while let Some((subject, predicate, object)) = scanner.next_triple()? {
        collector.push(IdTriple {
            subject,
            predicate,
            object,
        })?;
    }
    scanner.finish()?;
    let pos = collector.finish_pos()?;
    validate_permutation_semantics(&index, 1, pos)?;
    let ops = collector.finish_ops()?;
    validate_permutation_semantics(&index, 2, ops)?;
    Ok(())
}
