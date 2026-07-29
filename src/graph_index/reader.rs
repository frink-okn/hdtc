#![allow(dead_code)]

use super::builder::canonical_path;
use crate::hdt::reader::{BitmapTriplesScanner, sha256_to_end};
use crate::io::crc_utils::crc32c;
use crate::permutation::{PermEntry, PermutationCollector, PositionMaps, scan_hdt};
use crate::quads::{
    EmbeddedLayerSetReader, GraphSidecarReader, PositionGraphMembership, canonical_sidecar_path,
};
use crate::sort::{ExternalSorter, Sortable};
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result, bail, ensure};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const HEADER_SIZE: u64 = 256;
const FOOTER_SIZE: u64 = 64;
const DIRECTORY_ENTRY_SIZE: u64 = 64;
const SUPERBLOCK_BITS: u32 = 4096;
const SUBBLOCK_BITS: u32 = 512;
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
const KNOWN_FLAGS: u64 =
    HAS_POS_LAYERS | HAS_OPS_LAYERS | HAS_MEMBERSHIP_RANKS | HAS_MEMBERSHIP_IDS;

#[derive(Debug, Clone)]
struct Header {
    flags: u64,
    triples: u64,
    named_graphs: u64,
    memberships: u64,
    source_data_length: u64,
    file_size: u64,
    directory_offset: u64,
    directory_length: u64,
    section_count: u32,
    footer_offset: u64,
    source_digest: [u8; 32],
    sidecar_digest: [u8; 32],
    header_crc: u32,
}

#[derive(Debug, Clone, Copy)]
struct Section {
    section_type: u32,
    flags: u32,
    offset: u64,
    length: u64,
    entry_count: u64,
    bits_per_entry: u8,
    crc: u32,
    parameter: u64,
    indexed_bits: u64,
}

pub struct GraphIndex {
    path: PathBuf,
    hdt_path: PathBuf,
    sidecar_path: PathBuf,
    header: Header,
    sections: Vec<Section>,
    file: File,
    pos_layers: Option<EmbeddedLayerSetReader>,
    ops_layers: Option<EmbeddedLayerSetReader>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphIndexSpace {
    Pos,
    Ops,
}

impl GraphIndex {
    pub fn open_for_hdt(hdt_path: &Path) -> Result<Self> {
        Self::open(&canonical_path(hdt_path), hdt_path)
    }

    pub fn open(path: &Path, hdt_path: &Path) -> Result<Self> {
        ensure!(hdt_path.is_file(), "HDT parent is absent");
        let sidecar_path = canonical_sidecar_path(hdt_path);
        ensure!(sidecar_path.is_file(), "graphs-sidecar parent is absent");
        let mut file = File::open(path)
            .with_context(|| format!("Failed to open graph index {}", path.display()))?;
        let file_size = file.metadata()?.len();
        let header = read_header(&mut file, file_size, false)?;
        let sections = read_sections(&mut file, &header, false)?;
        validate_section_set(&header, &sections)?;
        validate_regions(&header, &sections)?;
        read_footer(&mut file, &header, false)?;

        let hdt = scan_hdt(hdt_path)?;
        let sidecar = GraphSidecarReader::open(&sidecar_path, hdt_path)?;
        ensure!(
            header.source_digest == sidecar.source_digest()
                && header.source_data_length == sidecar.source_data_length(),
            "graph-index/sidecar stored HDT binding mismatch"
        );
        ensure!(
            header.source_data_length == hdt.file_length - hdt.data_offset,
            "graph-index/HDT data-length mismatch"
        );
        ensure!(
            header.triples == hdt.triples,
            "graph-index/HDT triple-count mismatch"
        );
        ensure!(
            header.triples == sidecar.triple_count()
                && header.named_graphs == sidecar.named_graph_count()
                && header.memberships == sidecar.membership_count(),
            "graph-index/sidecar count mismatch"
        );

        let pos_layers = if header.flags & HAS_POS_LAYERS != 0 {
            let directory = section_from(&sections, POS_DIRECTORY)?;
            let region = section_from(&sections, POS_REGION)?;
            Some(EmbeddedLayerSetReader::open(
                path,
                directory.offset,
                directory.length,
                region.offset,
                region.length,
                header.triples,
                header.named_graphs,
                header.memberships,
            )?)
        } else {
            None
        };
        let ops_layers = if header.flags & HAS_OPS_LAYERS != 0 {
            let directory = section_from(&sections, OPS_DIRECTORY)?;
            let region = section_from(&sections, OPS_REGION)?;
            Some(EmbeddedLayerSetReader::open(
                path,
                directory.offset,
                directory.length,
                region.offset,
                region.length,
                header.triples,
                header.named_graphs,
                header.memberships,
            )?)
        } else {
            None
        };

        Ok(Self {
            path: path.to_path_buf(),
            hdt_path: hdt_path.to_path_buf(),
            sidecar_path,
            header,
            sections,
            file,
            pos_layers,
            ops_layers,
        })
    }

    pub fn triple_count(&self) -> u64 {
        self.header.triples
    }

    pub fn named_graph_count(&self) -> u64 {
        self.header.named_graphs
    }

    pub fn membership_count(&self) -> u64 {
        self.header.memberships
    }

    pub fn has_pos_layers(&self) -> bool {
        self.header.flags & HAS_POS_LAYERS != 0
    }

    pub fn has_ops_layers(&self) -> bool {
        self.header.flags & HAS_OPS_LAYERS != 0
    }

    pub fn has_membership_ranks(&self) -> bool {
        self.header.flags & HAS_MEMBERSHIP_RANKS != 0
    }

    pub fn has_membership_ids(&self) -> bool {
        self.header.flags & HAS_MEMBERSHIP_IDS != 0
    }

    fn layers_mut(&mut self, space: GraphIndexSpace) -> Result<&mut EmbeddedLayerSetReader> {
        match space {
            GraphIndexSpace::Pos => self.pos_layers.as_mut(),
            GraphIndexSpace::Ops => self.ops_layers.as_mut(),
        }
        .context("requested graph-index layer set is absent")
    }

    pub fn count(&mut self, space: GraphIndexSpace, graph: u64) -> Result<u64> {
        self.layers_mut(space)?.count(graph)
    }

    pub fn access(&mut self, space: GraphIndexSpace, graph: u64, position: u64) -> Result<bool> {
        self.layers_mut(space)?.access(graph, position)
    }

    pub fn rank(&mut self, space: GraphIndexSpace, graph: u64, position: u64) -> Result<u64> {
        self.layers_mut(space)?.rank(graph, position)
    }

    pub fn select(&mut self, space: GraphIndexSpace, graph: u64, ordinal: u64) -> Result<u64> {
        self.layers_mut(space)?.select(graph, ordinal)
    }

    pub fn next_member(
        &mut self,
        space: GraphIndexSpace,
        graph: u64,
        position: u64,
    ) -> Result<Option<u64>> {
        self.layers_mut(space)?.next_member(graph, position)
    }

    pub fn graphs_of(&mut self, position: u64) -> Result<Vec<u64>> {
        ensure!(position < self.header.triples, "position out of range");
        ensure!(self.has_membership_ids(), "graph-index ArrayG is absent");
        let array = self.section(TRANSPOSE_ARRAY)?;
        let bitmap = self.section(TRANSPOSE_BITMAP)?;
        let superranks = self.section(TRANSPOSE_SUPER)?;
        let subranks = self.section(TRANSPOSE_SUB)?;
        let start = self.adjacency_offset(position)?;
        let end = select1(&mut self.file, bitmap, superranks, subranks, position)?
            .checked_add(1)
            .context("graph run end overflow")?;
        let capacity = usize::try_from(end - start).context("graph run is too large")?;
        let mut graphs = Vec::with_capacity(capacity);
        for ordinal in start..end {
            graphs.push(packed_value(&mut self.file, array, ordinal)?);
        }
        Ok(graphs)
    }

    pub fn memberships(&mut self, start: u64, end: u64) -> Result<u64> {
        ensure!(
            start <= end && end <= self.header.triples,
            "invalid position range"
        );
        ensure!(self.has_membership_ranks(), "graph-index BitmapG is absent");
        Ok(self.adjacency_offset(end)? - self.adjacency_offset(start)?)
    }

    fn adjacency_offset(&mut self, position: u64) -> Result<u64> {
        ensure!(position <= self.header.triples, "position out of range");
        if position == 0 {
            return Ok(0);
        }
        let bitmap = self.section(TRANSPOSE_BITMAP)?;
        let superranks = self.section(TRANSPOSE_SUPER)?;
        let subranks = self.section(TRANSPOSE_SUB)?;
        select1(&mut self.file, bitmap, superranks, subranks, position - 1)?
            .checked_add(1)
            .context("adjacency offset overflow")
    }

    fn section(&self, section_type: u32) -> Result<Section> {
        self.sections
            .binary_search_by_key(&section_type, |section| section.section_type)
            .map(|index| self.sections[index])
            .map_err(|_| anyhow::anyhow!("missing graph-index section {section_type:#06x}"))
    }
}

fn section_from(sections: &[Section], section_type: u32) -> Result<Section> {
    sections
        .binary_search_by_key(&section_type, |section| section.section_type)
        .map(|index| sections[index])
        .map_err(|_| anyhow::anyhow!("missing graph-index section {section_type:#06x}"))
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

fn read_exact_at(file: &mut File, offset: u64, bytes: &mut [u8]) -> Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(bytes)?;
    Ok(())
}

fn read_header(file: &mut File, actual_size: u64, strict_crc: bool) -> Result<Header> {
    ensure!(
        actual_size >= HEADER_SIZE + FOOTER_SIZE,
        "graph index is truncated"
    );
    let mut bytes = [0u8; HEADER_SIZE as usize];
    read_exact_at(file, 0, &mut bytes)?;
    ensure!(&bytes[..8] == b"$HDTGIDX", "invalid graph-index magic");
    ensure!(
        get_u16(&bytes, 8) == 1 && get_u16(&bytes, 10) == 0,
        "unsupported graph-index version"
    );
    ensure!(
        get_u32(&bytes, 12) == 256,
        "invalid graph-index header size"
    );
    if strict_crc {
        ensure!(
            crc32c(&bytes[..252]) == get_u32(&bytes, 252),
            "graph-index header CRC mismatch"
        );
    }
    ensure!(
        get_u64(&bytes, 16) != 0 && get_u64(&bytes, 16) & !KNOWN_FLAGS == 0,
        "unknown or empty graph-index flags"
    );
    ensure!(get_u32(&bytes, 84) == 1, "unsupported identity algorithm");
    ensure!(
        get_u32(&bytes, 96) == SUPERBLOCK_BITS && get_u32(&bytes, 100) == SUBBLOCK_BITS,
        "unsupported graph-index rank geometry"
    );
    ensure!(
        get_u32(&bytes, 104) == 16,
        "unsupported position chunk shift"
    );
    ensure!(
        get_u32(&bytes, 108) == 0,
        "nonzero graph-index reserved field"
    );
    ensure!(
        bytes[176..252].iter().all(|byte| *byte == 0),
        "nonzero graph-index header reserved bytes"
    );
    let section_count = get_u32(&bytes, 80);
    let expected_directory_length = u64::from(section_count)
        .checked_mul(DIRECTORY_ENTRY_SIZE)
        .and_then(|length| length.checked_add(4))
        .context("graph-index directory length overflow")?;
    ensure!(
        get_u64(&bytes, 64).is_multiple_of(64),
        "unaligned graph-index directory"
    );
    ensure!(
        get_u64(&bytes, 72) == expected_directory_length,
        "invalid graph-index directory length"
    );
    ensure!(
        get_u64(&bytes, 56) == actual_size,
        "graph-index size mismatch"
    );
    ensure!(
        get_u64(&bytes, 88).checked_add(FOOTER_SIZE) == Some(actual_size),
        "invalid graph-index footer offset"
    );
    ensure!(
        get_u64(&bytes, 88).is_multiple_of(64),
        "unaligned graph-index footer"
    );
    let mut source_digest = [0u8; 32];
    source_digest.copy_from_slice(&bytes[112..144]);
    let mut sidecar_digest = [0u8; 32];
    sidecar_digest.copy_from_slice(&bytes[144..176]);
    Ok(Header {
        flags: get_u64(&bytes, 16),
        triples: get_u64(&bytes, 24),
        named_graphs: get_u64(&bytes, 32),
        memberships: get_u64(&bytes, 40),
        source_data_length: get_u64(&bytes, 48),
        file_size: actual_size,
        directory_offset: get_u64(&bytes, 64),
        directory_length: get_u64(&bytes, 72),
        section_count,
        footer_offset: get_u64(&bytes, 88),
        source_digest,
        sidecar_digest,
        header_crc: get_u32(&bytes, 252),
    })
}

fn read_sections(file: &mut File, header: &Header, strict_crc: bool) -> Result<Vec<Section>> {
    let entries_length = u64::from(header.section_count) * DIRECTORY_ENTRY_SIZE;
    let mut bytes = vec![0u8; usize::try_from(entries_length).context("directory too large")?];
    read_exact_at(file, header.directory_offset, &mut bytes)?;
    if strict_crc {
        let mut stored = [0u8; 4];
        read_exact_at(
            file,
            header
                .directory_offset
                .checked_add(entries_length)
                .context("graph-index directory checksum offset overflow")?,
            &mut stored,
        )?;
        ensure!(
            crc32c(&bytes) == u32::from_le_bytes(stored),
            "graph-index directory CRC mismatch"
        );
    }
    let mut sections = Vec::with_capacity(header.section_count as usize);
    let mut previous = None;
    for entry in bytes.chunks_exact(DIRECTORY_ENTRY_SIZE as usize) {
        let section = Section {
            section_type: get_u32(entry, 0),
            flags: get_u32(entry, 4),
            offset: get_u64(entry, 8),
            length: get_u64(entry, 16),
            entry_count: get_u64(entry, 24),
            bits_per_entry: entry[32],
            crc: get_u32(entry, 36),
            parameter: get_u64(entry, 40),
            indexed_bits: get_u64(entry, 48),
        };
        ensure!(
            section.flags & !REQUIRED == 0,
            "unknown graph-index section flags"
        );
        ensure!(
            entry[33..36].iter().all(|byte| *byte == 0)
                && entry[56..64].iter().all(|byte| *byte == 0),
            "nonzero graph-index section reserved bytes"
        );
        ensure!(
            previous.is_none_or(|value| section.section_type > value),
            "duplicate or out-of-order graph-index section"
        );
        let known = matches!(
            section.section_type,
            POS_DIRECTORY
                | POS_REGION
                | OPS_DIRECTORY
                | OPS_REGION
                | TRANSPOSE_ARRAY
                | TRANSPOSE_BITMAP
                | TRANSPOSE_SUPER
                | TRANSPOSE_SUB
        );
        if !known && section.flags & REQUIRED != 0 {
            bail!(
                "unknown required graph-index section {:#06x}",
                section.section_type
            );
        }
        previous = Some(section.section_type);
        sections.push(section);
    }
    Ok(sections)
}

fn expected_packed_length(count: u64, width: u8) -> Result<u64> {
    count
        .checked_mul(u64::from(width))
        .map(|bits| bits.div_ceil(8))
        .context("graph-index packed length overflow")
}

fn id_width(maximum: u64) -> u8 {
    if maximum == 0 {
        0
    } else {
        (64 - maximum.leading_zeros()) as u8
    }
}

fn validate_section_set(header: &Header, sections: &[Section]) -> Result<()> {
    let find = |section_type| {
        sections
            .binary_search_by_key(&section_type, |section| section.section_type)
            .ok()
            .map(|index| sections[index])
    };
    let layer_count = header
        .named_graphs
        .checked_add(1)
        .context("graph-index layer count overflow")?;
    let expected_directory = layer_count
        .checked_mul(96)
        .and_then(|length| length.checked_add(4))
        .context("layer-directory length overflow")?;
    for (flag, directory_type, region_type) in [
        (HAS_POS_LAYERS, POS_DIRECTORY, POS_REGION),
        (HAS_OPS_LAYERS, OPS_DIRECTORY, OPS_REGION),
    ] {
        let present = header.flags & flag != 0;
        ensure!(
            present == find(directory_type).is_some() && present == find(region_type).is_some(),
            "graph-index layer flag/section mismatch"
        );
        if present {
            let directory = find(directory_type).context("missing layer-directory section")?;
            ensure!(
                directory.flags == REQUIRED
                    && directory.length == expected_directory
                    && directory.entry_count == layer_count
                    && directory.bits_per_entry == 0
                    && directory.parameter == 0
                    && directory.indexed_bits == 0,
                "invalid graph-index layer-directory section"
            );
            let region = find(region_type).context("missing layer-region section")?;
            ensure!(
                region.flags == REQUIRED
                    && region.bits_per_entry == 0
                    && region.parameter == 0
                    && region.indexed_bits == 0
                    && region.crc == 0,
                "invalid graph-index layer-region section"
            );
            ensure!(
                (region.length == 0 && region.offset == 0 && region.entry_count == 0)
                    || (region.length > 0
                        && region.offset > 0
                        && region.entry_count > 0
                        && region.entry_count <= layer_count),
                "invalid empty/nonempty graph-index layer region"
            );
        }
    }

    let has_ranks = header.flags & HAS_MEMBERSHIP_RANKS != 0;
    let has_ids = header.flags & HAS_MEMBERSHIP_IDS != 0;
    ensure!(!has_ids || has_ranks, "ArrayG is present without BitmapG");
    if header.memberships == 0 {
        ensure!(
            !has_ranks && !has_ids,
            "empty dataset has transpose sections"
        );
    }
    ensure!(
        has_ranks == find(TRANSPOSE_BITMAP).is_some()
            && has_ranks == find(TRANSPOSE_SUPER).is_some()
            && has_ranks == find(TRANSPOSE_SUB).is_some(),
        "graph-index transpose-rank flag/section mismatch"
    );
    ensure!(
        has_ids == find(TRANSPOSE_ARRAY).is_some(),
        "graph-index transpose-ID flag/section mismatch"
    );
    if let Some(array) = find(TRANSPOSE_ARRAY) {
        let width = id_width(header.named_graphs);
        ensure!(
            array.flags == REQUIRED
                && array.length == expected_packed_length(header.memberships, width)?
                && array.entry_count == header.memberships
                && array.bits_per_entry == width
                && array.parameter == 0
                && array.indexed_bits == 0,
            "invalid graph-index ArrayG section"
        );
    }
    if let Some(bitmap) = find(TRANSPOSE_BITMAP) {
        ensure!(
            bitmap.flags == REQUIRED
                && bitmap.length == header.memberships.div_ceil(8)
                && bitmap.entry_count == header.memberships
                && bitmap.bits_per_entry == 1
                && bitmap.parameter == 0
                && bitmap.indexed_bits == 0,
            "invalid graph-index BitmapG section"
        );
        let superranks = find(TRANSPOSE_SUPER).context("missing BitmapG superranks")?;
        let super_count = header
            .memberships
            .div_ceil(u64::from(SUPERBLOCK_BITS))
            .checked_add(1)
            .context("BitmapG superrank count overflow")?;
        let super_length = super_count
            .checked_mul(8)
            .context("BitmapG superrank length overflow")?;
        ensure!(
            superranks.flags == REQUIRED
                && superranks.length == super_length
                && superranks.entry_count == super_count
                && superranks.bits_per_entry == 64
                && superranks.parameter == u64::from(SUPERBLOCK_BITS)
                && superranks.indexed_bits == header.memberships,
            "invalid graph-index BitmapG superrank section"
        );
        let subranks = find(TRANSPOSE_SUB).context("missing BitmapG subranks")?;
        let sub_count = header.memberships.div_ceil(u64::from(SUBBLOCK_BITS));
        let sub_length = sub_count
            .checked_mul(2)
            .context("BitmapG subrank length overflow")?;
        ensure!(
            subranks.flags == REQUIRED
                && subranks.length == sub_length
                && subranks.entry_count == sub_count
                && subranks.bits_per_entry == 16
                && subranks.parameter == u64::from(SUBBLOCK_BITS)
                && subranks.indexed_bits == header.memberships,
            "invalid graph-index BitmapG subrank section"
        );
    }
    for section in sections {
        if section.length == 0 {
            ensure!(section.crc == 0, "empty graph-index section has a CRC");
        }
        if matches!(
            section.section_type,
            POS_DIRECTORY
                | POS_REGION
                | OPS_DIRECTORY
                | OPS_REGION
                | TRANSPOSE_ARRAY
                | TRANSPOSE_BITMAP
                | TRANSPOSE_SUPER
                | TRANSPOSE_SUB
        ) {
            ensure!(
                section.flags == REQUIRED,
                "version-1 section is not required"
            );
        }
    }
    Ok(())
}

fn validate_regions(header: &Header, sections: &[Section]) -> Result<()> {
    let directory_end = header
        .directory_offset
        .checked_add(header.directory_length)
        .context("graph-index directory end overflow")?;
    ensure!(
        header.directory_offset >= HEADER_SIZE && directory_end <= header.footer_offset,
        "graph-index directory is out of range"
    );
    let mut previous_end = directory_end;
    for section in sections {
        if section.length == 0 {
            ensure!(
                section.offset == 0,
                "empty graph-index section has an offset"
            );
            continue;
        }
        ensure!(
            section.offset.is_multiple_of(64),
            "unaligned graph-index section"
        );
        let end = section
            .offset
            .checked_add(section.length)
            .context("graph-index section end overflow")?;
        ensure!(
            section.offset >= previous_end && end <= header.footer_offset,
            "overlapping or out-of-range graph-index section"
        );
        previous_end = end;
    }
    ensure!(
        previous_end <= header.footer_offset,
        "graph-index payload overflow"
    );
    Ok(())
}

fn read_footer(file: &mut File, header: &Header, strict_crc: bool) -> Result<()> {
    let mut bytes = [0u8; FOOTER_SIZE as usize];
    read_exact_at(file, header.footer_offset, &mut bytes)?;
    ensure!(
        &bytes[..8] == b"$HDTGXND",
        "invalid graph-index footer magic"
    );
    ensure!(
        get_u16(&bytes, 8) == 1 && get_u16(&bytes, 10) == 0 && get_u32(&bytes, 12) == 64,
        "invalid graph-index footer version or size"
    );
    ensure!(
        get_u64(&bytes, 16) == header.file_size
            && get_u64(&bytes, 24) == 0
            && get_u64(&bytes, 32) == header.directory_offset
            && get_u64(&bytes, 40) == header.directory_length
            && get_u64(&bytes, 48) == 0
            && get_u32(&bytes, 56) == header.header_crc,
        "graph-index footer/header mismatch"
    );
    if strict_crc {
        ensure!(
            crc32c(&bytes[..60]) == get_u32(&bytes, 60),
            "graph-index footer CRC mismatch"
        );
    }
    Ok(())
}

fn range_crc(file: &mut File, offset: u64, length: u64) -> Result<u32> {
    file.seek(SeekFrom::Start(offset))?;
    let mut take = file.take(length);
    let mut digest = crate::io::crc_utils::CRC32C_ALGO.digest();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = take.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    ensure!(take.limit() == 0, "truncated graph-index section");
    Ok(digest.finalize())
}

fn whole_digest(path: &Path) -> Result<[u8; 32]> {
    let file = File::open(path)?;
    sha256_to_end(&mut BufReader::with_capacity(256 * 1024, file))
}

fn zero_range(file: &mut File, offset: u64, length: u64) -> Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    let mut remaining = length;
    let mut buffer = [0u8; 64 * 1024];
    while remaining > 0 {
        let count = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        file.read_exact(&mut buffer[..count])?;
        ensure!(
            buffer[..count].iter().all(|byte| *byte == 0),
            "nonzero graph-index padding"
        );
        remaining -= count as u64;
    }
    Ok(())
}

fn validate_outer_checksums(index: &GraphIndex) -> Result<()> {
    let mut file = File::open(&index.path)?;
    let header = read_header(&mut file, index.header.file_size, true)?;
    let sections = read_sections(&mut file, &header, true)?;
    read_footer(&mut file, &header, true)?;
    let mut cursor = HEADER_SIZE;
    zero_range(&mut file, cursor, header.directory_offset - cursor)?;
    cursor = header
        .directory_offset
        .checked_add(header.directory_length)
        .context("graph-index directory end overflow")?;
    for section in &sections {
        if section.length == 0 {
            continue;
        }
        zero_range(&mut file, cursor, section.offset - cursor)?;
        if !matches!(section.section_type, POS_REGION | OPS_REGION) {
            ensure!(
                range_crc(&mut file, section.offset, section.length)? == section.crc,
                "graph-index section CRC mismatch for {:#06x}",
                section.section_type
            );
        }
        cursor = section
            .offset
            .checked_add(section.length)
            .context("graph-index section end overflow")?;
    }
    zero_range(&mut file, cursor, header.footer_offset - cursor)?;
    Ok(())
}

fn packed_value(file: &mut File, section: Section, index: u64) -> Result<u64> {
    ensure!(
        index < section.entry_count,
        "packed-array index out of range"
    );
    if section.bits_per_entry == 0 {
        return Ok(0);
    }
    let bit = index
        .checked_mul(u64::from(section.bits_per_entry))
        .context("packed-array bit offset overflow")?;
    let byte = bit / 8;
    let shift = (bit % 8) as u32;
    let available = (section.length - byte).min(16) as usize;
    let mut bytes = [0u8; 16];
    read_exact_at(file, section.offset + byte, &mut bytes[..available])?;
    let mask = if section.bits_per_entry == 64 {
        u128::from(u64::MAX)
    } else {
        (1u128 << section.bits_per_entry) - 1
    };
    Ok(((u128::from_le_bytes(bytes) >> shift) & mask) as u64)
}

fn select1(
    file: &mut File,
    bitmap: Section,
    superranks: Section,
    subranks: Section,
    ordinal: u64,
) -> Result<u64> {
    let total = packed_value(file, superranks, superranks.entry_count - 1)?;
    ensure!(ordinal < total, "bitmap select ordinal out of range");

    // Upper-bound search chooses the last superblock whose cumulative rank is
    // at most the requested ordinal. The terminal sample is always greater.
    let mut low = 0u64;
    let mut high = superranks.entry_count;
    while low < high {
        let mid = low + (high - low) / 2;
        if packed_value(file, superranks, mid)? <= ordinal {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let superblock = low - 1;
    let super_rank = packed_value(file, superranks, superblock)?;
    let first_subblock = superblock * 8;
    let subblock_count = bitmap.entry_count.div_ceil(u64::from(SUBBLOCK_BITS));
    let mut chosen = first_subblock;
    let mut chosen_rank = super_rank;
    for subblock in first_subblock..(first_subblock + 8).min(subblock_count) {
        let rank = super_rank + packed_value(file, subranks, subblock)?;
        if rank > ordinal {
            break;
        }
        chosen = subblock;
        chosen_rank = rank;
    }

    let start_bit = chosen * u64::from(SUBBLOCK_BITS);
    let bits = (bitmap.entry_count - start_bit).min(u64::from(SUBBLOCK_BITS));
    let mut bytes = [0u8; 64];
    read_exact_at(
        file,
        bitmap.offset + start_bit / 8,
        &mut bytes[..bits.div_ceil(8) as usize],
    )?;
    let mut remaining = ordinal - chosen_rank;
    for (byte_index, &byte) in bytes[..bits.div_ceil(8) as usize].iter().enumerate() {
        let mut word = byte;
        let population = u64::from(word.count_ones());
        if remaining >= population {
            remaining -= population;
            continue;
        }
        while remaining > 0 {
            word &= word - 1;
            remaining -= 1;
        }
        let bit = u64::from(word.trailing_zeros());
        let result = start_bit + byte_index as u64 * 8 + bit;
        ensure!(
            result < bitmap.entry_count,
            "bitmap select reached a tail bit"
        );
        return Ok(result);
    }
    bail!("bitmap rank directory does not locate select ordinal")
}

fn bitmap_bit(file: &mut File, section: Section, index: u64) -> Result<bool> {
    ensure!(index < section.entry_count, "bitmap index out of range");
    let mut byte = [0u8; 1];
    read_exact_at(file, section.offset + index / 8, &mut byte)?;
    Ok(byte[0] & (1 << (index % 8)) != 0)
}

fn validate_transpose(index: &GraphIndex, memberships_path: &Path) -> Result<()> {
    if !index.has_membership_ranks() {
        return Ok(());
    }
    let bitmap = index.section(TRANSPOSE_BITMAP)?;
    let array = index
        .has_membership_ids()
        .then(|| index.section(TRANSPOSE_ARRAY))
        .transpose()?;
    let mut file = File::open(&index.path)?;
    let source = File::open(memberships_path)?;
    let mut source = zstd::Decoder::with_buffer(BufReader::new(source))?;
    let mut ordinal = 0u64;
    let mut previous = None;
    while let Some(item) = PositionGraphMembership::read_from(&mut source)? {
        if let Some(array) = array {
            ensure!(
                packed_value(&mut file, array, ordinal)? == item.graph,
                "ArrayG differs from sidecar membership"
            );
        }
        if ordinal > 0 {
            let end_of_position = previous.is_some_and(|position| position != item.position);
            ensure!(
                bitmap_bit(&mut file, bitmap, ordinal - 1)? == end_of_position,
                "BitmapG position boundary mismatch"
            );
        }
        previous = Some(item.position);
        ordinal = ordinal
            .checked_add(1)
            .context("transpose ordinal overflow")?;
    }
    ensure!(
        ordinal == index.header.memberships,
        "transpose length mismatch"
    );
    if ordinal > 0 {
        ensure!(
            bitmap_bit(&mut file, bitmap, ordinal - 1)?,
            "BitmapG terminal boundary is clear"
        );
    }
    validate_bitmap_ranks(index, &mut file, bitmap)?;
    if !index.header.memberships.is_multiple_of(8) {
        let mut byte = [0u8; 1];
        read_exact_at(&mut file, bitmap.offset + bitmap.length - 1, &mut byte)?;
        let mask = !((1u8 << (index.header.memberships % 8)) - 1);
        ensure!(byte[0] & mask == 0, "BitmapG has nonzero tail bits");
    }
    if let Some(array) = array {
        let used_bits = index
            .header
            .memberships
            .checked_mul(u64::from(array.bits_per_entry))
            .context("ArrayG bit length overflow")?;
        if array.length > 0 && !used_bits.is_multiple_of(8) {
            let mut byte = [0u8; 1];
            read_exact_at(&mut file, array.offset + array.length - 1, &mut byte)?;
            let mask = !((1u8 << (used_bits % 8)) - 1);
            ensure!(byte[0] & mask == 0, "ArrayG has nonzero tail bits");
        }
    }
    Ok(())
}

fn validate_bitmap_ranks(index: &GraphIndex, file: &mut File, bitmap: Section) -> Result<()> {
    let superranks = index.section(TRANSPOSE_SUPER)?;
    let subranks = index.section(TRANSPOSE_SUB)?;
    let blocks = index.header.memberships.div_ceil(u64::from(SUBBLOCK_BITS));
    let mut total = 0u64;
    let mut super_base = 0u64;
    for block in 0..blocks {
        if block.is_multiple_of(8) {
            let stored = packed_value(file, superranks, block / 8)?;
            ensure!(stored == total, "BitmapG superrank recurrence mismatch");
            super_base = total;
        }
        ensure!(
            packed_value(file, subranks, block)? == total - super_base,
            "BitmapG subrank recurrence mismatch"
        );
        let start_bit = block * u64::from(SUBBLOCK_BITS);
        let bits = (index.header.memberships - start_bit).min(u64::from(SUBBLOCK_BITS));
        let mut bytes = [0u8; 64];
        read_exact_at(
            file,
            bitmap.offset + start_bit / 8,
            &mut bytes[..bits.div_ceil(8) as usize],
        )?;
        let complete = (bits / 8) as usize;
        let mut count = bytes[..complete]
            .iter()
            .map(|byte| byte.count_ones())
            .sum::<u32>();
        if !bits.is_multiple_of(8) {
            let mask = (1u8 << (bits % 8)) - 1;
            count += (bytes[complete] & mask).count_ones();
        }
        total = total
            .checked_add(u64::from(count))
            .context("BitmapG population overflow")?;
    }
    ensure!(
        packed_value(file, superranks, superranks.entry_count - 1)? == total,
        "BitmapG terminal superrank mismatch"
    );
    ensure!(total == index.header.triples, "BitmapG population mismatch");
    Ok(())
}

fn open_layer_set(index: &GraphIndex, component: u32) -> Result<EmbeddedLayerSetReader> {
    let directory = index.section((component << 8) | 1)?;
    let region = index.section((component << 8) | 2)?;
    EmbeddedLayerSetReader::open(
        &index.path,
        directory.offset,
        directory.length,
        region.offset,
        region.length,
        index.header.triples,
        index.header.named_graphs,
        index.header.memberships,
    )
}

fn validate_layer_relation(
    index: &GraphIndex,
    component: u32,
    permutation: impl Iterator<Item = Result<PermEntry>>,
    sidecar_memberships: &Path,
    temp_dir: &Path,
    memory_budget: usize,
) -> Result<()> {
    let mut layers = open_layer_set(index, component)?;
    layers.validate_strict()?;
    let mut sidecar = GraphSidecarReader::open(&index.sidecar_path, &index.hdt_path)?;
    let budget = (memory_budget / 2).max(1);
    let mut by_permuted = ExternalSorter::new(temp_dir, budget);
    let mut buffer = Vec::<PositionGraphMembership>::new();
    let mut memory = 0usize;
    for graph in 0..=index.header.named_graphs {
        ensure!(
            layers.count(graph)? == sidecar.count(graph)?,
            "layer-set/sidecar graph count mismatch"
        );
        for position in layers.layer_iter(graph)? {
            by_permuted.push(
                PositionGraphMembership {
                    position: position?,
                    graph,
                },
                &mut buffer,
                &mut memory,
            )?;
        }
    }

    let mut permutation = permutation.enumerate();
    let mut current = match permutation.next() {
        Some((position, entry)) => Some((position as u64, entry?)),
        None => None,
    };
    let mut by_spo = ExternalSorter::new(temp_dir, budget);
    let mut spo_buffer = Vec::<PositionGraphMembership>::new();
    let mut spo_memory = 0usize;
    let mut observed = 0u64;
    for item in by_permuted.finish(&mut buffer)? {
        let item = item?;
        while current.is_some_and(|(position, _)| position < item.position) {
            current = match permutation.next() {
                Some((position, entry)) => Some((position as u64, entry?)),
                None => None,
            };
        }
        let (position, entry) = current.context("permutation ended before layer set")?;
        ensure!(
            position == item.position,
            "layer position missing from permutation"
        );
        by_spo.push(
            PositionGraphMembership {
                position: entry.spo_position,
                graph: item.graph,
            },
            &mut spo_buffer,
            &mut spo_memory,
        )?;
        observed = observed
            .checked_add(1)
            .context("layer relation count overflow")?;
    }
    ensure!(
        observed == index.header.memberships,
        "layer-set relation membership-count mismatch"
    );

    let source = File::open(sidecar_memberships)?;
    let mut source = zstd::Decoder::with_buffer(BufReader::new(source))?;
    let mut expected = PositionGraphMembership::read_from(&mut source)?;
    for actual in by_spo.finish(&mut spo_buffer)? {
        let actual = actual?;
        ensure!(
            Some(actual) == expected,
            "layer-set membership mapping differs from sidecar"
        );
        expected = PositionGraphMembership::read_from(&mut source)?;
    }
    ensure!(
        expected.is_none(),
        "layer-set relation ended before sidecar"
    );
    Ok(())
}

pub fn validate_graph_index(
    path: &Path,
    hdt_path: &Path,
    temp_dir: &Path,
    memory_budget: usize,
) -> Result<()> {
    let index = GraphIndex::open(path, hdt_path)?;
    validate_outer_checksums(&index)?;
    ensure!(
        whole_digest(&index.sidecar_path)? == index.header.sidecar_digest,
        "graph-index/sidecar SHA-256 binding mismatch"
    );
    let hdt = scan_hdt(hdt_path)?;
    let mut hdt_file = File::open(hdt_path)?;
    hdt_file.seek(SeekFrom::Start(hdt.data_offset))?;
    ensure!(
        sha256_to_end(&mut BufReader::with_capacity(256 * 1024, hdt_file))?
            == index.header.source_digest,
        "graph-index/HDT SHA-256 binding mismatch"
    );

    let transposed = tempfile::Builder::new()
        .prefix(".hdtc-graph-index-validation-")
        .tempfile_in(temp_dir)?
        .into_temp_path();
    let mut sidecar = GraphSidecarReader::open(&index.sidecar_path, hdt_path)?;
    sidecar.validate_strict(temp_dir, memory_budget, Some(&transposed))?;
    validate_transpose(&index, &transposed)?;

    if index.has_pos_layers() || index.has_ops_layers() {
        let mut collector =
            PermutationCollector::new(temp_dir, memory_budget, PositionMaps::default());
        let mut scanner = BitmapTriplesScanner::new(&hdt.offsets, hdt_path)?;
        while let Some((subject, predicate, object)) = scanner.next_triple()? {
            collector.push(IdTriple {
                subject,
                predicate,
                object,
            })?;
        }
        scanner.finish()?;
        if index.has_pos_layers() {
            let permutation = collector.finish_pos()?;
            validate_layer_relation(&index, 1, permutation, &transposed, temp_dir, memory_budget)?;
        }
        if index.has_ops_layers() {
            let permutation = collector.finish_ops()?;
            validate_layer_relation(&index, 2, permutation, &transposed, temp_dir, memory_budget)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn select_uses_rank_directories() -> Result<()> {
        let mut file = tempfile::tempfile()?;
        file.write_all(&[0b0011_0010])?; // set positions 1, 4, 5
        file.write_all(&[0; 7])?;
        file.write_all(&0u64.to_le_bytes())?;
        file.write_all(&3u64.to_le_bytes())?;
        file.write_all(&0u16.to_le_bytes())?;
        let bitmap = Section {
            section_type: TRANSPOSE_BITMAP,
            flags: REQUIRED,
            offset: 0,
            length: 1,
            entry_count: 6,
            bits_per_entry: 1,
            crc: 0,
            parameter: 0,
            indexed_bits: 0,
        };
        let superranks = Section {
            section_type: TRANSPOSE_SUPER,
            flags: REQUIRED,
            offset: 8,
            length: 16,
            entry_count: 2,
            bits_per_entry: 64,
            crc: 0,
            parameter: 4096,
            indexed_bits: 6,
        };
        let subranks = Section {
            section_type: TRANSPOSE_SUB,
            flags: REQUIRED,
            offset: 24,
            length: 2,
            entry_count: 1,
            bits_per_entry: 16,
            crc: 0,
            parameter: 512,
            indexed_bits: 6,
        };
        assert_eq!(select1(&mut file, bitmap, superranks, subranks, 0)?, 1);
        assert_eq!(select1(&mut file, bitmap, superranks, subranks, 1)?, 4);
        assert_eq!(select1(&mut file, bitmap, superranks, subranks, 2)?, 5);
        Ok(())
    }
}
