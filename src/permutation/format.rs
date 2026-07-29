use crate::io::crc_utils::crc32c;
use anyhow::{Context, Result, bail, ensure};
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub(crate) const HEADER_SIZE: u64 = 256;
pub(crate) const FOOTER_SIZE: u64 = 64;
pub(crate) const DIRECTORY_ENTRY_SIZE: u64 = 64;
pub(crate) const SUPERBLOCK_BITS: u32 = 4096;
pub(crate) const SUBBLOCK_BITS: u32 = 512;
pub(crate) const REQUIRED: u32 = 1;

pub(crate) const POS_ARRAY_Y: u32 = 0x0101;
pub(crate) const POS_BITMAP_Y: u32 = 0x0102;
pub(crate) const POS_ARRAY_Z: u32 = 0x0103;
pub(crate) const POS_BITMAP_Z: u32 = 0x0104;
pub(crate) const POS_Y_SUPER: u32 = 0x0105;
pub(crate) const POS_Y_SUB: u32 = 0x0106;
pub(crate) const POS_Z_SUPER: u32 = 0x0107;
pub(crate) const POS_Z_SUB: u32 = 0x0108;
pub(crate) const POS_MAP: u32 = 0x0109;

pub(crate) const OPS_ARRAY_Y: u32 = 0x0201;
pub(crate) const OPS_BITMAP_Y: u32 = 0x0202;
pub(crate) const OPS_ARRAY_Z: u32 = 0x0203;
pub(crate) const OPS_BITMAP_Z: u32 = 0x0204;
pub(crate) const OPS_Y_SUPER: u32 = 0x0205;
pub(crate) const OPS_Y_SUB: u32 = 0x0206;
pub(crate) const OPS_Z_SUPER: u32 = 0x0207;
pub(crate) const OPS_Z_SUB: u32 = 0x0208;
pub(crate) const OPS_MAP: u32 = 0x0209;

pub(crate) const SPO_Y_SUPER: u32 = 0x0305;
pub(crate) const SPO_Y_SUB: u32 = 0x0306;
pub(crate) const SPO_Z_SUPER: u32 = 0x0307;
pub(crate) const SPO_Z_SUB: u32 = 0x0308;

pub fn canonical_path(hdt_path: &Path) -> PathBuf {
    let mut name: OsString = hdt_path.as_os_str().to_owned();
    name.push(".perm");
    PathBuf::from(name)
}

pub(crate) fn align64(value: u64) -> Result<u64> {
    value
        .checked_add(63)
        .map(|v| v & !63)
        .context("permutation-index alignment overflow")
}

pub(crate) fn packed_len(count: u64, width: u8) -> Result<u64> {
    count
        .checked_mul(u64::from(width))
        .context("packed section bit-length overflow")
        .map(|bits| bits.div_ceil(8))
}

pub(crate) fn id_width(maximum: u64) -> u8 {
    if maximum == 0 {
        0
    } else {
        (64 - maximum.leading_zeros()) as u8
    }
}

pub(crate) fn position_width(count: u64) -> u8 {
    if count <= 1 {
        0
    } else {
        (64 - (count - 1).leading_zeros()) as u8
    }
}

pub(crate) fn super_count(bits: u64) -> u64 {
    if bits == 0 {
        0
    } else {
        bits.div_ceil(u64::from(SUPERBLOCK_BITS)) + 1
    }
}

pub(crate) fn sub_count(bits: u64) -> u64 {
    if bits == 0 {
        0
    } else {
        bits.div_ceil(u64::from(SUBBLOCK_BITS))
    }
}

pub(crate) fn is_core(section_type: u32) -> bool {
    matches!(
        section_type,
        POS_ARRAY_Y
            | POS_BITMAP_Y
            | POS_ARRAY_Z
            | POS_BITMAP_Z
            | POS_Y_SUPER
            | POS_Y_SUB
            | POS_Z_SUPER
            | POS_Z_SUB
            | OPS_ARRAY_Y
            | OPS_BITMAP_Y
            | OPS_ARRAY_Z
            | OPS_BITMAP_Z
            | OPS_Y_SUPER
            | OPS_Y_SUB
            | OPS_Z_SUPER
            | OPS_Z_SUB
            | SPO_Y_SUPER
            | SPO_Y_SUB
            | SPO_Z_SUPER
            | SPO_Z_SUB
    )
}

pub(crate) fn is_bitmap(section_type: u32) -> bool {
    matches!(
        section_type,
        POS_BITMAP_Y | POS_BITMAP_Z | OPS_BITMAP_Y | OPS_BITMAP_Z
    )
}

pub(crate) fn is_array(section_type: u32) -> bool {
    matches!(
        section_type,
        POS_ARRAY_Y | POS_ARRAY_Z | OPS_ARRAY_Y | OPS_ARRAY_Z | POS_MAP | OPS_MAP
    )
}

#[derive(Debug, Clone)]
pub(crate) struct Header {
    pub flags: u64,
    pub triples: u64,
    pub subjects: u64,
    pub predicates: u64,
    pub objects: u64,
    pub pos_pairs: u64,
    pub ops_pairs: u64,
    pub source_data_length: u64,
    pub file_size: u64,
    pub directory_offset: u64,
    pub directory_length: u64,
    pub section_count: u32,
    pub footer_offset: u64,
    pub source_digest: [u8; 32],
    pub header_crc: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct Section {
    pub section_type: u32,
    pub flags: u32,
    pub offset: u64,
    pub length: u64,
    pub entry_count: u64,
    pub bits_per_entry: u8,
    pub payload_crc: u32,
    pub parameter: u64,
    pub indexed_bits: u64,
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

pub(crate) fn parse_header(bytes: &[u8; 256]) -> Result<Header> {
    ensure!(
        &bytes[0..8] == b"$HDTPERM",
        "invalid permutation-index magic"
    );
    ensure!(
        u16_at(bytes, 8) == 1 && u16_at(bytes, 10) == 0,
        "unsupported permutation-index version"
    );
    ensure!(
        u32_at(bytes, 12) == 256,
        "invalid permutation-index header size"
    );
    ensure!(
        u32_at(bytes, 108) == 1,
        "unsupported source identity algorithm"
    );
    ensure!(
        u32_at(bytes, 120) == SUPERBLOCK_BITS,
        "unsupported superblock width"
    );
    ensure!(
        u32_at(bytes, 124) == SUBBLOCK_BITS,
        "unsupported subblock width"
    );
    ensure!(
        bytes[160..252].iter().all(|&b| b == 0),
        "nonzero reserved header bytes"
    );
    let stored_crc = u32_at(bytes, 252);
    ensure!(
        crc32c(&bytes[..252]) == stored_crc,
        "permutation-index header CRC32C mismatch"
    );
    Ok(Header {
        flags: u64_at(bytes, 16),
        triples: u64_at(bytes, 24),
        subjects: u64_at(bytes, 32),
        predicates: u64_at(bytes, 40),
        objects: u64_at(bytes, 48),
        pos_pairs: u64_at(bytes, 56),
        ops_pairs: u64_at(bytes, 64),
        source_data_length: u64_at(bytes, 72),
        file_size: u64_at(bytes, 80),
        directory_offset: u64_at(bytes, 88),
        directory_length: u64_at(bytes, 96),
        section_count: u32_at(bytes, 104),
        footer_offset: u64_at(bytes, 112),
        source_digest: bytes[128..160].try_into().unwrap(),
        header_crc: stored_crc,
    })
}

pub(crate) fn parse_section(bytes: &[u8; 64]) -> Result<Section> {
    ensure!(
        bytes[33..36].iter().all(|&b| b == 0),
        "nonzero section reserved bytes"
    );
    ensure!(
        bytes[56..64].iter().all(|&b| b == 0),
        "nonzero section reserved bytes"
    );
    Ok(Section {
        section_type: u32_at(bytes, 0),
        flags: u32_at(bytes, 4),
        offset: u64_at(bytes, 8),
        length: u64_at(bytes, 16),
        entry_count: u64_at(bytes, 24),
        bits_per_entry: bytes[32],
        payload_crc: u32_at(bytes, 36),
        parameter: u64_at(bytes, 40),
        indexed_bits: u64_at(bytes, 48),
    })
}

pub(crate) fn read_header_and_sections(path: &Path) -> Result<(Header, Vec<Section>)> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open permutation index {}", path.display()))?;
    let actual_size = file.metadata()?.len();
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut header_bytes = [0u8; 256];
    reader.read_exact(&mut header_bytes)?;
    let header = parse_header(&header_bytes)?;
    ensure!(
        header.flags == 0b111,
        "unknown or missing permutation-index flags"
    );
    ensure!(
        header.section_count >= 20,
        "permutation index has fewer than 20 core sections"
    );
    ensure!(
        header.directory_offset % 64 == 0,
        "unaligned section directory"
    );
    let entries_len = u64::from(header.section_count)
        .checked_mul(DIRECTORY_ENTRY_SIZE)
        .context("section-directory length overflow")?;
    ensure!(
        header.directory_length == entries_len + 4,
        "invalid section-directory length"
    );
    ensure!(
        header.file_size == actual_size,
        "permutation-index file-size mismatch"
    );
    ensure!(
        header.footer_offset
            == actual_size
                .checked_sub(FOOTER_SIZE)
                .context("file is shorter than footer")?,
        "invalid footer offset"
    );

    reader.seek(SeekFrom::Start(header.directory_offset))?;
    let entries_len_usize =
        usize::try_from(entries_len).context("section directory is too large")?;
    let mut directory = vec![0u8; entries_len_usize];
    reader.read_exact(&mut directory)?;
    let mut crc_bytes = [0u8; 4];
    reader.read_exact(&mut crc_bytes)?;
    ensure!(
        crc32c(&directory) == u32::from_le_bytes(crc_bytes),
        "section-directory CRC32C mismatch"
    );

    let mut sections = Vec::with_capacity(header.section_count as usize);
    for chunk in directory.chunks_exact(64) {
        sections.push(parse_section(chunk.try_into().unwrap())?);
    }

    reader.seek(SeekFrom::Start(header.footer_offset))?;
    let mut footer = [0u8; 64];
    reader.read_exact(&mut footer)?;
    ensure!(
        &footer[0..8] == b"$HDTPEND",
        "invalid permutation-index footer magic"
    );
    ensure!(
        u16_at(&footer, 8) == 1 && u16_at(&footer, 10) == 0,
        "unsupported footer version"
    );
    ensure!(u32_at(&footer, 12) == 64, "invalid footer size");
    ensure!(
        u64_at(&footer, 16) == header.file_size,
        "footer file-size mismatch"
    );
    ensure!(u64_at(&footer, 24) == 0, "footer header offset is not zero");
    ensure!(
        u64_at(&footer, 32) == header.directory_offset,
        "footer directory-offset mismatch"
    );
    ensure!(
        u64_at(&footer, 40) == header.directory_length,
        "footer directory-length mismatch"
    );
    ensure!(
        u32_at(&footer, 48) == header.section_count,
        "footer section-count mismatch"
    );
    ensure!(u32_at(&footer, 52) == 0, "nonzero footer reserved field");
    ensure!(
        u32_at(&footer, 56) == header.header_crc,
        "footer header-CRC copy mismatch"
    );
    ensure!(
        crc32c(&footer[..60]) == u32_at(&footer, 60),
        "footer CRC32C mismatch"
    );

    for pair in sections.windows(2) {
        if pair[0].section_type >= pair[1].section_type {
            bail!("section types are duplicated or out of order");
        }
    }
    Ok((header, sections))
}
