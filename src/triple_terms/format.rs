//! Byte layout of the triple-term index, `data.hdt.tt`.
//!
//! `docs/triple-terms-format.md` is normative. The container follows the
//! permutation index (`docs/permutation-index-format.md` §2–§8) exactly — a
//! 256-byte header, a CRC-protected directory of 64-byte entries, bare packed
//! payloads at 64-byte aligned offsets, a 64-byte footer — so a mapped reader
//! that already knows one knows the other.

use crate::io::crc_utils::crc32c;
use crate::permutation::format::{DIRECTORY_ENTRY_SIZE, FOOTER_SIZE, HEADER_SIZE, align64};
use anyhow::{Context, Result, bail, ensure};
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tempfile::TempPath;

pub const MAGIC: &[u8; 8] = b"$HDTTERM";
pub const FOOTER_MAGIC: &[u8; 8] = b"$HDTTEND";
pub const MAJOR_VERSION: u16 = 1;
pub const MINOR_VERSION: u16 = 0;
/// The IRI convention of [`super::iri`]: namespace, hash, and label form.
pub const IRI_CONVENTION: u32 = 1;
pub const IDENTITY_SHA256: u32 = 1;

/// Header flag: the index covers a propositions HDT.
pub const HAS_PROPOSITIONS: u64 = 1;

/// Section components, in the high byte of a section type.
pub mod component {
    /// The run of triple-term IRIs in `data.hdt`'s object dictionary.
    pub const DATA_RUN: u32 = 0x01;
    /// The run of triple-term IRIs in the propositions HDT's object dictionary.
    pub const PROPS_RUN: u32 = 0x02;
    pub const DATA_SPO: u32 = 0x11;
    pub const DATA_POS: u32 = 0x12;
    pub const DATA_OPS: u32 = 0x13;
    pub const PROPS_SPO: u32 = 0x21;
    pub const PROPS_POS: u32 = 0x22;
    pub const PROPS_OPS: u32 = 0x23;
}

/// Section kinds, in the low byte of a section type.
pub mod kind {
    /// Run component: proposition subject, `(id << 1) | unasserted`.
    pub const TARGET_SUBJECT: u32 = 0x01;
    /// Run component: proposition predicate identifier.
    pub const TARGET_PREDICATE: u32 = 0x02;
    /// Run component: proposition object identifier.
    pub const TARGET_OBJECT: u32 = 0x03;
    /// Order component: bitmap over that HDT's triples in that order.
    pub const LAYER: u32 = 0x04;
    pub const LAYER_SUPERRANK: u32 = 0x05;
    pub const LAYER_SUBRANK: u32 = 0x06;
    /// Order component: `data.hdt` object identifier of each set bit's
    /// triple-term IRI, in rank order.
    pub const REVERSE: u32 = 0x07;
    /// Order component, sparse layers (§8.1): the Elias–Fano high bits, a
    /// bitmap the rank directory indexes in place of `LAYER`.
    pub const LAYER_HIGH: u32 = 0x08;
    /// Order component, sparse layers (§8.1): the Elias–Fano low bits of each
    /// set position, in rank order.
    pub const LAYER_LOW: u32 = 0x09;
}

/// How a layer is stored (§8.1): a bitmap over its positions, or the
/// Elias–Fano encoding of its set positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerEncoding {
    Dense,
    /// Each set position split into its low `low_bits` bits, stored packed,
    /// and the rest, stored in unary in a bitmap of [`sparse_high_bits`].
    Sparse {
        low_bits: u8,
    },
}

/// The encoding a layer over `positions` positions with `marked` of them set
/// is stored in: the smaller in payload bytes, the bitmap on a tie (§8.1). A
/// function of the two counts alone, so a layer's bytes are a function of its
/// contents and a reader or validator can recompute the choice.
pub fn layer_encoding(positions: u64, marked: u64) -> LayerEncoding {
    use crate::permutation::format::{sub_count, super_count};
    let ranked = |bits: u64| bits.div_ceil(8) + super_count(bits) * 8 + sub_count(bits) * 2;
    if positions == 0 || marked > positions {
        return LayerEncoding::Dense;
    }
    let low_bits = (positions / marked.max(1)).ilog2() as u8;
    if low_bits == 0 {
        return LayerEncoding::Dense;
    }
    let sparse = ranked(sparse_high_bits(positions, marked, low_bits))
        + (marked * u64::from(low_bits)).div_ceil(8);
    if sparse < ranked(positions) {
        LayerEncoding::Sparse { low_bits }
    } else {
        LayerEncoding::Dense
    }
}

/// The length of a sparse layer's high-bits bitmap: one set bit per set
/// position, and one clear bit closing each of the `((positions - 1) >>
/// low_bits) + 1` buckets of `2^low_bits` positions.
pub fn sparse_high_bits(positions: u64, marked: u64, low_bits: u8) -> u64 {
    if positions == 0 {
        return 0;
    }
    marked + ((positions - 1) >> low_bits) + 1
}

pub const fn section_type(component: u32, kind: u32) -> u32 {
    (component << 8) | kind
}

/// The three orders a layer is kept in, and their component codes.
pub const DATA_ORDERS: [u32; 3] = [
    component::DATA_SPO,
    component::DATA_POS,
    component::DATA_OPS,
];
pub const PROPS_ORDERS: [u32; 3] = [
    component::PROPS_SPO,
    component::PROPS_POS,
    component::PROPS_OPS,
];

/// The canonical sidecar path: `data.hdt` → `data.hdt.tt`.
pub fn canonical_path(hdt_path: &Path) -> PathBuf {
    let mut name: OsString = hdt_path.as_os_str().to_owned();
    name.push(".tt");
    PathBuf::from(name)
}

/// The propositions HDT beside `data.hdt`: `data.hdt.props.hdt`.
pub fn propositions_path(hdt_path: &Path) -> PathBuf {
    let mut name: OsString = hdt_path.as_os_str().to_owned();
    name.push(".props.hdt");
    PathBuf::from(name)
}

/// What the header records about one bound HDT.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceFacts {
    pub triples: u64,
    pub objects: u64,
    pub source_length: u64,
    /// First object identifier of the triple-term IRI run, 0 when empty.
    pub run_start: u64,
    pub run_length: u64,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub flags: u64,
    pub convention: u32,
    pub section_count: u32,
    pub sidecar_size: u64,
    pub directory_offset: u64,
    pub directory_length: u64,
    pub footer_offset: u64,
    pub data: SourceFacts,
    pub props: SourceFacts,
}

impl Header {
    pub fn has_propositions(&self) -> bool {
        self.flags & HAS_PROPOSITIONS != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
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

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}
fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}
fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
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

const SOURCE_FIELDS_DATA: usize = 80;
const SOURCE_FIELDS_PROPS: usize = 120;
const DIGEST_DATA: usize = 160;
const DIGEST_PROPS: usize = 192;

fn put_source(header: &mut [u8; 256], at: usize, digest_at: usize, facts: &SourceFacts) {
    put_u64(header, at, facts.triples);
    put_u64(header, at + 8, facts.objects);
    put_u64(header, at + 16, facts.source_length);
    put_u64(header, at + 24, facts.run_start);
    put_u64(header, at + 32, facts.run_length);
    header[digest_at..digest_at + 32].copy_from_slice(&facts.digest);
}

fn get_source(header: &[u8; 256], at: usize, digest_at: usize) -> SourceFacts {
    SourceFacts {
        triples: get_u64(header, at),
        objects: get_u64(header, at + 8),
        source_length: get_u64(header, at + 16),
        run_start: get_u64(header, at + 24),
        run_length: get_u64(header, at + 32),
        digest: header[digest_at..digest_at + 32].try_into().unwrap(),
    }
}

pub fn parse_header(bytes: &[u8; 256]) -> Result<Header> {
    ensure!(&bytes[0..8] == MAGIC, "not a triple-term index (bad magic)");
    ensure!(
        get_u16(bytes, 8) == MAJOR_VERSION,
        "unsupported triple-term index major version {}",
        get_u16(bytes, 8)
    );
    ensure!(
        get_u16(bytes, 10) == MINOR_VERSION,
        "unsupported triple-term index minor version {}",
        get_u16(bytes, 10)
    );
    ensure!(get_u32(bytes, 12) == HEADER_SIZE as u32, "bad header size");
    ensure!(
        crc32c(&bytes[..252]) == get_u32(bytes, 252),
        "triple-term index header checksum mismatch"
    );
    let flags = get_u64(bytes, 16);
    ensure!(flags & !HAS_PROPOSITIONS == 0, "unknown header flags");
    let convention = get_u32(bytes, 24);
    ensure!(
        convention == IRI_CONVENTION,
        "unsupported triple-term IRI convention {convention}"
    );
    ensure!(
        get_u32(bytes, 28) == IDENTITY_SHA256,
        "unsupported identity algorithm"
    );
    ensure!(
        get_u32(bytes, 32) == crate::permutation::format::SUPERBLOCK_BITS
            && get_u32(bytes, 36) == crate::permutation::format::SUBBLOCK_BITS,
        "unsupported rank-directory sampling"
    );
    ensure!(
        bytes[44..48].iter().all(|b| *b == 0) && bytes[224..252].iter().all(|b| *b == 0),
        "reserved header bytes are not zero"
    );
    Ok(Header {
        flags,
        convention,
        section_count: get_u32(bytes, 40),
        sidecar_size: get_u64(bytes, 48),
        directory_offset: get_u64(bytes, 56),
        directory_length: get_u64(bytes, 64),
        footer_offset: get_u64(bytes, 72),
        data: get_source(bytes, SOURCE_FIELDS_DATA, DIGEST_DATA),
        props: get_source(bytes, SOURCE_FIELDS_PROPS, DIGEST_PROPS),
    })
}

pub fn parse_section(bytes: &[u8]) -> Result<Section> {
    ensure!(
        bytes.len() == DIRECTORY_ENTRY_SIZE as usize,
        "bad entry size"
    );
    ensure!(
        bytes[33..36].iter().all(|b| *b == 0) && bytes[56..64].iter().all(|b| *b == 0),
        "reserved directory bytes are not zero"
    );
    Ok(Section {
        section_type: get_u32(bytes, 0),
        flags: get_u32(bytes, 4),
        offset: get_u64(bytes, 8),
        length: get_u64(bytes, 16),
        entry_count: get_u64(bytes, 24),
        bits_per_entry: bytes[32],
        payload_crc: get_u32(bytes, 36),
        parameter: get_u64(bytes, 40),
        indexed_bits: get_u64(bytes, 48),
    })
}

/// Read and checksum the header and directory of an index file.
pub fn read_header_and_sections(path: &Path) -> Result<(Header, Vec<Section>)> {
    let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    let file_length = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut bytes = [0u8; 256];
    reader
        .read_exact(&mut bytes)
        .with_context(|| format!("{} is too short for a header", path.display()))?;
    let header = parse_header(&bytes)?;
    ensure!(
        header.sidecar_size == file_length,
        "triple-term index size does not match its header"
    );
    ensure!(
        header.directory_length == u64::from(header.section_count) * DIRECTORY_ENTRY_SIZE + 4,
        "directory length does not match the section count"
    );
    ensure!(
        header.directory_offset == HEADER_SIZE
            && header.footer_offset.checked_add(FOOTER_SIZE) == Some(header.sidecar_size)
            && header.footer_offset % 64 == 0,
        "triple-term index regions are not where the header says"
    );
    let mut footer = [0u8; 64];
    reader.seek(SeekFrom::Start(header.footer_offset))?;
    reader.read_exact(&mut footer)?;
    ensure!(
        &footer[0..8] == FOOTER_MAGIC
            && get_u16(&footer, 8) == get_u16(&bytes, 8)
            && get_u16(&footer, 10) == get_u16(&bytes, 10)
            && get_u32(&footer, 12) == FOOTER_SIZE as u32
            && get_u64(&footer, 16) == header.sidecar_size
            && get_u64(&footer, 24) == 0
            && get_u64(&footer, 32) == header.directory_offset
            && get_u64(&footer, 40) == header.directory_length
            && get_u32(&footer, 48) == header.section_count
            && get_u32(&footer, 52) == 0
            && get_u32(&footer, 56) == get_u32(&bytes, 252)
            && crc32c(&footer[..60]) == get_u32(&footer, 60),
        "triple-term index footer does not match its header"
    );
    reader.seek(SeekFrom::Start(header.directory_offset))?;
    let mut directory = vec![0u8; header.directory_length as usize];
    reader.read_exact(&mut directory)?;
    let (entries, crc) = directory.split_at(directory.len() - 4);
    ensure!(
        crc32c(entries) == u32::from_le_bytes(crc.try_into().unwrap()),
        "triple-term index directory checksum mismatch"
    );
    let sections = entries
        .chunks(DIRECTORY_ENTRY_SIZE as usize)
        .map(parse_section)
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        sections
            .windows(2)
            .all(|w| w[0].section_type < w[1].section_type),
        "directory entries are not in ascending type order"
    );
    check_section_set(&header, &sections)?;
    for section in &sections {
        if section.length > 0 {
            ensure!(
                section.offset % 64 == 0
                    && section
                        .offset
                        .checked_add(section.length)
                        .is_some_and(|end| end <= header.footer_offset),
                "section {:#06x} lies outside the payload area",
                section.section_type
            );
        }
    }
    Ok((header, sections))
}

fn is_run(component: u32) -> bool {
    matches!(component, component::DATA_RUN | component::PROPS_RUN)
}

/// Check the directory against §6: known types only, each REQUIRED, the
/// presence rules, and the shapes the rest of the format relies on.
fn check_section_set(header: &Header, sections: &[Section]) -> Result<()> {
    use crate::permutation::format::{
        REQUIRED, SUBBLOCK_BITS, SUPERBLOCK_BITS, id_width, sub_count, super_count,
    };
    let find = |section_type: u32| sections.iter().find(|s| s.section_type == section_type);
    for section in sections {
        let (component, kind_value) = (section.section_type >> 8, section.section_type & 0xFF);
        let known = if is_run(component) {
            (kind::TARGET_SUBJECT..=kind::TARGET_OBJECT).contains(&kind_value)
        } else if DATA_ORDERS.contains(&component) || PROPS_ORDERS.contains(&component) {
            (kind::LAYER..=kind::LAYER_LOW).contains(&kind_value)
        } else {
            false
        };
        ensure!(
            known,
            "unknown triple-term index section {:#06x}",
            section.section_type
        );
        ensure!(
            section.flags == REQUIRED,
            "section {:#06x} has flags {:#x}",
            section.section_type,
            section.flags
        );
        ensure!(
            section.length == crate::io::packed_len(section.entry_count, section.bits_per_entry)?,
            "section {:#06x} length does not match its entries",
            section.section_type
        );
        ensure!(
            (section.length == 0) == (section.offset == 0)
                && (section.length > 0 || section.payload_crc == 0),
            "section {:#06x} is empty but placed",
            section.section_type
        );
    }
    let mut components: Vec<u32> = vec![component::DATA_RUN, component::PROPS_RUN];
    components.extend(DATA_ORDERS);
    if header.has_propositions() {
        components.extend(PROPS_ORDERS);
    } else {
        ensure!(
            sections
                .iter()
                .all(|s| !PROPS_ORDERS.contains(&(s.section_type >> 8))),
            "propositions layers without HAS_PROPOSITIONS"
        );
        ensure!(
            header.props == SourceFacts::default(),
            "propositions HDT fields set without HAS_PROPOSITIONS"
        );
    }
    for component in components {
        if is_run(component) {
            let run_length = if component == component::DATA_RUN {
                header.data.run_length
            } else {
                header.props.run_length
            };
            for k in [
                kind::TARGET_SUBJECT,
                kind::TARGET_PREDICATE,
                kind::TARGET_OBJECT,
            ] {
                let section = find(section_type(component, k)).with_context(|| {
                    format!("missing section {:#06x}", section_type(component, k))
                })?;
                ensure!(
                    section.entry_count == run_length,
                    "run arrays do not match the run"
                );
                // §7: both runs share widths; the object width is fixed by
                // the header, the others by the HDTs (checked on open).
                let data_run = find(section_type(component::DATA_RUN, k))
                    .context("missing data run section")?;
                ensure!(
                    section.bits_per_entry == data_run.bits_per_entry,
                    "the two runs' arrays have different widths"
                );
                if k == kind::TARGET_OBJECT {
                    ensure!(
                        section.bits_per_entry
                            == id_width(header.data.objects.max(header.props.objects)),
                        "run object array has the wrong width"
                    );
                }
            }
            continue;
        }
        let triples = if DATA_ORDERS.contains(&component) {
            header.data.triples
        } else {
            header.props.triples
        };
        let get = |k: u32| {
            find(section_type(component, k))
                .with_context(|| format!("missing section {:#06x}", section_type(component, k)))
        };
        let (superrank, subrank, reverse) = (
            get(kind::LAYER_SUPERRANK)?,
            get(kind::LAYER_SUBRANK)?,
            get(kind::REVERSE)?,
        );
        // §8.1: the encoding is the one the counts choose, and its bitmap —
        // the layer itself, or the high bits — is the one indexed.
        let marked = reverse.entry_count;
        let (dense, high, low) = (
            find(section_type(component, kind::LAYER)),
            find(section_type(component, kind::LAYER_HIGH)),
            find(section_type(component, kind::LAYER_LOW)),
        );
        let indexed = match (layer_encoding(triples, marked), dense, high, low) {
            (LayerEncoding::Dense, Some(layer), None, None) => {
                ensure!(
                    layer.entry_count == triples && layer.bits_per_entry == 1,
                    "layer {component:#04x} does not cover its HDT's triples"
                );
                triples
            }
            (LayerEncoding::Sparse { low_bits }, None, Some(high), Some(low)) => {
                let bits = sparse_high_bits(triples, marked, low_bits);
                ensure!(
                    high.entry_count == bits
                        && high.bits_per_entry == 1
                        && low.entry_count == marked
                        && low.bits_per_entry == low_bits,
                    "sparse layer {component:#04x} does not cover its HDT's triples"
                );
                bits
            }
            (encoding, ..) => {
                bail!("layer {component:#04x} is not stored as its counts require ({encoding:?})")
            }
        };
        ensure!(
            superrank.bits_per_entry == 64
                && superrank.parameter == u64::from(SUPERBLOCK_BITS)
                && superrank.entry_count == super_count(indexed)
                && superrank.indexed_bits == indexed
                && subrank.bits_per_entry == 16
                && subrank.parameter == u64::from(SUBBLOCK_BITS)
                && subrank.entry_count == sub_count(indexed)
                && subrank.indexed_bits == indexed,
            "layer {component:#04x} rank directory has the wrong shape"
        );
        ensure!(
            reverse.bits_per_entry == id_width(header.data.objects),
            "layer {component:#04x} reverse array has the wrong width"
        );
    }
    Ok(())
}

/// One section waiting to be placed: its payload in a scratch file.
pub(crate) struct DraftSection {
    pub section_type: u32,
    pub payload: TempPath,
    pub entry_count: u64,
    pub bits_per_entry: u8,
    pub parameter: u64,
    pub indexed_bits: u64,
}

fn file_crc(path: &Path) -> Result<(u64, u32)> {
    use crate::io::crc_utils::CRC32C_ALGO;
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

fn write_zeros_to(writer: &mut BufWriter<File>, offset: u64) -> Result<()> {
    let current = writer.stream_position()?;
    ensure!(current <= offset, "triple-term index regions overlap");
    let mut remaining = offset - current;
    let zeros = [0u8; 64];
    while remaining > 0 {
        let count = remaining.min(64) as usize;
        writer.write_all(&zeros[..count])?;
        remaining -= count as u64;
    }
    Ok(())
}

/// Write a complete index file: tightly packed, sections in ascending type
/// order, so the bytes are a function of the contents alone.
pub(crate) fn assemble(
    output: &Path,
    flags: u64,
    data: &SourceFacts,
    props: &SourceFacts,
    mut drafts: Vec<DraftSection>,
) -> Result<()> {
    drafts.sort_unstable_by_key(|section| section.section_type);
    for pair in drafts.windows(2) {
        if pair[0].section_type == pair[1].section_type {
            bail!("duplicate section {:#06x}", pair[0].section_type);
        }
    }
    let section_count = u32::try_from(drafts.len()).context("too many sections")?;
    let directory_offset = HEADER_SIZE;
    let directory_length = u64::from(section_count) * DIRECTORY_ENTRY_SIZE + 4;
    let mut cursor = align64(directory_offset + directory_length)?;
    let mut placed = Vec::with_capacity(drafts.len());
    for draft in drafts {
        let (length, crc) = file_crc(&draft.payload)?;
        let offset = if length == 0 {
            0
        } else {
            let at = cursor;
            cursor = align64(cursor.checked_add(length).context("offset overflow")?)?;
            at
        };
        placed.push((draft, offset, length, crc));
    }
    let footer_offset = cursor;
    let file_size = footer_offset + FOOTER_SIZE;

    let mut directory = Vec::with_capacity(placed.len() * 64);
    for (draft, offset, length, crc) in &placed {
        let mut entry = [0u8; 64];
        put_u32(&mut entry, 0, draft.section_type);
        put_u32(&mut entry, 4, crate::permutation::format::REQUIRED);
        put_u64(&mut entry, 8, *offset);
        put_u64(&mut entry, 16, *length);
        put_u64(&mut entry, 24, draft.entry_count);
        entry[32] = draft.bits_per_entry;
        put_u32(&mut entry, 36, *crc);
        put_u64(&mut entry, 40, draft.parameter);
        put_u64(
            &mut entry,
            48,
            if *length == 0 { 0 } else { draft.indexed_bits },
        );
        directory.extend_from_slice(&entry);
    }

    let mut header = [0u8; 256];
    header[0..8].copy_from_slice(MAGIC);
    put_u16(&mut header, 8, MAJOR_VERSION);
    put_u16(&mut header, 10, MINOR_VERSION);
    put_u32(&mut header, 12, HEADER_SIZE as u32);
    put_u64(&mut header, 16, flags);
    put_u32(&mut header, 24, IRI_CONVENTION);
    put_u32(&mut header, 28, IDENTITY_SHA256);
    put_u32(&mut header, 32, crate::permutation::format::SUPERBLOCK_BITS);
    put_u32(&mut header, 36, crate::permutation::format::SUBBLOCK_BITS);
    put_u32(&mut header, 40, section_count);
    put_u64(&mut header, 48, file_size);
    put_u64(&mut header, 56, directory_offset);
    put_u64(&mut header, 64, directory_length);
    put_u64(&mut header, 72, footer_offset);
    put_source(&mut header, SOURCE_FIELDS_DATA, DIGEST_DATA, data);
    put_source(&mut header, SOURCE_FIELDS_PROPS, DIGEST_PROPS, props);
    let header_crc = crc32c(&header[..252]);
    put_u32(&mut header, 252, header_crc);

    let mut footer = [0u8; 64];
    footer[0..8].copy_from_slice(FOOTER_MAGIC);
    put_u16(&mut footer, 8, MAJOR_VERSION);
    put_u16(&mut footer, 10, MINOR_VERSION);
    put_u32(&mut footer, 12, FOOTER_SIZE as u32);
    put_u64(&mut footer, 16, file_size);
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
    for (draft, offset, length, _) in &placed {
        if *length == 0 {
            continue;
        }
        write_zeros_to(&mut writer, *offset)?;
        let mut payload = BufReader::with_capacity(256 * 1024, File::open(&draft.payload)?);
        std::io::copy(&mut payload, &mut writer)?;
    }
    write_zeros_to(&mut writer, footer_offset)?;
    writer.write_all(&footer)?;
    writer.flush()?;
    writer
        .get_ref()
        .sync_all()
        .with_context(|| format!("Failed to flush {}", output.display()))?;
    ensure!(
        std::fs::metadata(output)?.len() == file_size,
        "written triple-term index size mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The smaller encoding wins, with whole bits of low part: sparse at
    /// low densities, dense from about a quarter up, dense when there is
    /// nothing to cover, and never sparse with no low bits.
    #[test]
    fn a_layer_is_stored_in_the_smaller_encoding() {
        assert_eq!(layer_encoding(0, 0), LayerEncoding::Dense);
        assert_eq!(layer_encoding(100, 100), LayerEncoding::Dense);
        assert_eq!(layer_encoding(1_000_000, 300_000), LayerEncoding::Dense);
        assert_eq!(
            layer_encoding(1_000_000, 100_000),
            LayerEncoding::Sparse { low_bits: 3 }
        );
        assert_eq!(
            layer_encoding(1_000_000, 0),
            LayerEncoding::Sparse { low_bits: 19 }
        );
        // ROBOKOP's layers: 133 M marks over 1.4 G positions.
        assert_eq!(
            layer_encoding(1_402_044_173, 132_960_364),
            LayerEncoding::Sparse { low_bits: 3 }
        );
        // Small layers are dominated by the rank directory either way.
        assert_eq!(layer_encoding(20, 5), LayerEncoding::Dense);
        for positions in [1u64, 7, 100, 4096, 70_000, 1 << 33] {
            for marked in [0, 1, positions / 3, positions / 2, positions] {
                if let LayerEncoding::Sparse { low_bits } = layer_encoding(positions, marked) {
                    assert!(low_bits >= 1, "{positions} {marked}");
                    assert!(sparse_high_bits(positions, marked, low_bits) >= marked);
                }
            }
        }
    }

    /// The directory a layer of `marked` marks over `triples` positions would
    /// have, stored in `encoding`; runs empty, no propositions HDT.
    fn directory(triples: u64, marked: u64, encoding: LayerEncoding) -> (Header, Vec<Section>) {
        use crate::permutation::format::{
            REQUIRED, SUBBLOCK_BITS, SUPERBLOCK_BITS, id_width, sub_count, super_count,
        };
        let objects = 1000;
        let section = |section_type: u32, entry_count: u64, bits_per_entry: u8| Section {
            section_type,
            flags: REQUIRED,
            offset: 0,
            length: crate::io::packed_len(entry_count, bits_per_entry).unwrap(),
            entry_count,
            bits_per_entry,
            payload_crc: 0,
            parameter: 0,
            indexed_bits: 0,
        };
        let placed = |mut s: Section| {
            if s.length > 0 {
                s.offset = 64;
            }
            s
        };
        let mut sections = Vec::new();
        for run in [component::DATA_RUN, component::PROPS_RUN] {
            sections.push(section(section_type(run, kind::TARGET_SUBJECT), 0, 2));
            sections.push(section(section_type(run, kind::TARGET_PREDICATE), 0, 1));
            sections.push(section(
                section_type(run, kind::TARGET_OBJECT),
                0,
                id_width(objects),
            ));
        }
        for component in DATA_ORDERS {
            let indexed = match encoding {
                LayerEncoding::Dense => {
                    sections.push(placed(section(
                        section_type(component, kind::LAYER),
                        triples,
                        1,
                    )));
                    triples
                }
                LayerEncoding::Sparse { low_bits } => {
                    let bits = sparse_high_bits(triples, marked, low_bits);
                    sections.push(placed(section(
                        section_type(component, kind::LAYER_HIGH),
                        bits,
                        1,
                    )));
                    sections.push(placed(section(
                        section_type(component, kind::LAYER_LOW),
                        marked,
                        low_bits,
                    )));
                    bits
                }
            };
            let mut superrank = placed(section(
                section_type(component, kind::LAYER_SUPERRANK),
                super_count(indexed),
                64,
            ));
            superrank.parameter = u64::from(SUPERBLOCK_BITS);
            superrank.indexed_bits = indexed;
            let mut subrank = placed(section(
                section_type(component, kind::LAYER_SUBRANK),
                sub_count(indexed),
                16,
            ));
            subrank.parameter = u64::from(SUBBLOCK_BITS);
            subrank.indexed_bits = indexed;
            sections.push(superrank);
            sections.push(subrank);
            sections.push(placed(section(
                section_type(component, kind::REVERSE),
                marked,
                id_width(objects),
            )));
        }
        let header = Header {
            flags: 0,
            convention: IRI_CONVENTION,
            section_count: sections.len() as u32,
            sidecar_size: 0,
            directory_offset: 0,
            directory_length: 0,
            footer_offset: 0,
            data: SourceFacts {
                triples,
                objects,
                ..SourceFacts::default()
            },
            props: SourceFacts::default(),
        };
        (header, sections)
    }

    /// A layer stored in the encoding its counts choose is accepted; the same
    /// layer in the other encoding is refused, so its bytes stay canonical.
    #[test]
    fn a_layer_in_the_other_encoding_is_refused() {
        for (triples, marked) in [(1_000_000u64, 1_000u64), (1_000_000, 500_000)] {
            let chosen = layer_encoding(triples, marked);
            let other = match chosen {
                LayerEncoding::Dense => LayerEncoding::Sparse {
                    low_bits: (triples / marked).ilog2() as u8,
                },
                LayerEncoding::Sparse { .. } => LayerEncoding::Dense,
            };
            let (header, sections) = directory(triples, marked, chosen);
            check_section_set(&header, &sections).unwrap();
            let (header, sections) = directory(triples, marked, other);
            let error = check_section_set(&header, &sections).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("not stored as its counts require"),
                "{triples} {marked}: {error}"
            );
        }
    }
}
