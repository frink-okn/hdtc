//! Shared HDT reading infrastructure.
//!
//! Provides:
//! - `PfcSectionIndex` — random-access dictionary lookup (extract + locate)
//! - `DictionaryResolver` — ID→term and term→ID resolution for all four sections
//! - `HdtSectionOffsets` — byte offsets of all BitmapTriples sections
//! - `BitmapTriplesScanner` — forward-only streaming triple iterator
//! - `open_hdt()` — scan HDT file structure, open dictionary
//! - N-Triples term-writing helpers (tab-delimited format)

use crate::io::crc_utils::crc8;
use crate::io::{
    ControlInfo, ControlType, LogArrayReader, StreamingBitmapDecoder, StreamingLogArrayDecoder,
    decode_vbyte, encode_vbyte, read_vbyte, skip_bitmap_section, skip_log_array_section,
};
use anyhow::{Context, Result, bail};
use oxrdfio::{RdfFormat, RdfParser};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

const PFC_SECTION_TYPE: u8 = 0x02;
const DICTIONARY_FOUR_FORMAT: &str = "<http://purl.org/HDT/hdt#dictionaryFour>";
const TRIPLES_BITMAP_FORMAT: &str = "<http://purl.org/HDT/hdt#triplesBitmap>";

// ---------------------------------------------------------------------------
// HDT section offsets
// ---------------------------------------------------------------------------

/// Byte offsets of each BitmapTriples section within an HDT file,
/// together with the triple and SP-pair counts.
#[derive(Debug, Clone, Copy)]
pub struct HdtSectionOffsets {
    pub num_triples: u64,
    pub num_sp_pairs: u64,
    pub by_start: u64,
    pub bz_start: u64,
    pub ay_start: u64,
    pub az_start: u64,
}

// ---------------------------------------------------------------------------
// PFC section index (extract + locate)
// ---------------------------------------------------------------------------

/// Random-access index over a PFC dictionary section.
///
/// Supports ID→term lookup (`get_bytes`) and term→ID lookup (`locate`).
/// Block-level LRU cache bounds memory usage.
pub struct PfcSectionIndex {
    pub section_name: &'static str,
    pub string_count: u64,
    block_size: u64,
    /// Block start offsets into the string buffer (n_blocks + 1 entries).
    offsets: Vec<u64>,
    /// Absolute file offset of the start of the string data buffer.
    string_buf_start: u64,
    /// Seekable reader for block data.
    reader: BufReader<File>,
    block_cache: HashMap<u64, Vec<Vec<u8>>>,
    cache_order: VecDeque<u64>,
    cache_capacity: usize,
}

impl PfcSectionIndex {
    pub fn read_from<R: Read + Seek>(
        reader: &mut R,
        hdt_path: &Path,
        section_name: &'static str,
        cache_budget: usize,
    ) -> Result<Self> {
        let mut preamble = Vec::new();

        let mut section_type = [0u8; 1];
        reader.read_exact(&mut section_type)?;
        if section_type[0] != PFC_SECTION_TYPE {
            bail!(
                "Invalid dictionary section type for {section_name}: expected 0x{PFC_SECTION_TYPE:02x}, got 0x{:02x}",
                section_type[0]
            );
        }
        preamble.push(section_type[0]);

        let string_count = read_vbyte(reader)
            .with_context(|| format!("Invalid string count VByte for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(string_count));
        let buffer_length = read_vbyte(reader)
            .with_context(|| format!("Invalid buffer length VByte for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(buffer_length));
        let block_size = read_vbyte(reader)
            .with_context(|| format!("Invalid block size VByte for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(block_size));
        if block_size == 0 {
            bail!("Invalid block size 0 in {section_name} section");
        }

        let mut crc8_buf = [0u8; 1];
        reader.read_exact(&mut crc8_buf)?;
        let expected_crc8 = crc8(&preamble);
        if crc8_buf[0] != expected_crc8 {
            bail!(
                "PFC preamble CRC8 mismatch in {section_name}: expected {expected_crc8:#04x}, got {:#04x}",
                crc8_buf[0]
            );
        }

        let offsets_reader = LogArrayReader::read_from(reader)
            .with_context(|| format!("Failed to read block offsets for {section_name}"))?;
        let offset_count = offsets_reader.len();

        let expected_blocks = if string_count == 0 {
            0
        } else {
            string_count.div_ceil(block_size)
        };
        let expected_offsets = expected_blocks + 1;
        if offset_count != expected_offsets {
            bail!(
                "Unexpected offset count in {section_name}: got {offset_count}, expected {expected_offsets}"
            );
        }

        let mut offsets = Vec::with_capacity(offset_count as usize);
        for i in 0..offset_count {
            offsets.push(offsets_reader.get(i));
        }

        if offsets.last().copied().unwrap_or(0) != buffer_length {
            bail!(
                "PFC sentinel mismatch in {section_name}: last offset {} != buffer length {buffer_length}",
                offsets.last().copied().unwrap_or(0)
            );
        }

        let string_buf_start = reader.stream_position()?;
        reader
            .seek(SeekFrom::Current(buffer_length as i64 + 4))
            .with_context(|| format!("Failed to skip string buffer for {section_name}"))?;

        // ~2KB per decoded block; at least 64 blocks so small budgets still work.
        const ESTIMATED_BLOCK_BYTES: usize = 2048;
        let cache_capacity = (cache_budget / ESTIMATED_BLOCK_BYTES).max(64);

        let file = File::open(hdt_path)?;
        Ok(Self {
            section_name,
            string_count,
            block_size,
            offsets,
            string_buf_start,
            reader: BufReader::with_capacity(64 * 1024, file),
            block_cache: HashMap::new(),
            cache_order: VecDeque::new(),
            cache_capacity,
        })
    }

    /// Retrieve the raw bytes of a 1-based dictionary ID.
    pub fn get_bytes(&mut self, id: u64, buf: &mut Vec<u8>) -> Result<()> {
        if id == 0 || id > self.string_count {
            bail!(
                "{} ID out of range: {id} (valid range: 1..={})",
                self.section_name,
                self.string_count
            );
        }

        let zero_based = id - 1;
        let block_index = zero_based / self.block_size;
        let entry_in_block = (zero_based % self.block_size) as usize;

        let section_name = self.section_name;
        let block = self.get_or_decode_block(block_index)?;
        let entry = block.get(entry_in_block).with_context(|| {
            format!(
                "Decoded block too short in {section_name} at block {block_index}, entry {entry_in_block}"
            )
        })?;
        buf.clear();
        buf.extend_from_slice(entry);
        Ok(())
    }

    /// Binary-search for a term, returning its 1-based ID in this section.
    ///
    /// Returns `None` if the term is not present.
    pub fn locate(&mut self, term: &[u8]) -> Result<Option<u64>> {
        if self.string_count == 0 {
            return Ok(None);
        }

        let n_blocks = self.offsets.len().saturating_sub(1);
        if n_blocks == 0 {
            return Ok(None);
        }

        // Binary search: find the rightmost block whose first string <= term.
        // Invariant: block[lo].first_string <= term (if it exists).
        let mut lo = 0usize;
        let mut hi = n_blocks;

        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            let first = self.read_first_string_of_block(mid)?;
            match first.as_slice().cmp(term) {
                Ordering::Less | Ordering::Equal => lo = mid,
                Ordering::Greater => hi = mid,
            }
        }

        // Block lo is the candidate. Decode it and linear-search.
        let block_size = self.block_size;
        let block = self.get_or_decode_block(lo as u64)?;
        let base_id = lo as u64 * block_size + 1; // 1-based
        for (i, entry) in block.iter().enumerate() {
            match entry.as_slice().cmp(term) {
                Ordering::Equal => return Ok(Some(base_id + i as u64)),
                Ordering::Greater => return Ok(None), // sorted — won't appear later
                Ordering::Less => continue,
            }
        }
        Ok(None)
    }

    /// Read only the first (verbatim) string of a block, seeking directly.
    fn read_first_string_of_block(&mut self, block_index: usize) -> Result<Vec<u8>> {
        let start = self.offsets[block_index];
        self.reader
            .seek(SeekFrom::Start(self.string_buf_start + start))?;
        let mut result = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            self.reader.read_exact(&mut byte)?;
            if byte[0] == 0 {
                break;
            }
            result.push(byte[0]);
        }
        Ok(result)
    }

    /// Return a decoded block, using the LRU cache.
    fn get_or_decode_block(&mut self, block_index: u64) -> Result<&Vec<Vec<u8>>> {
        if !self.block_cache.contains_key(&block_index) {
            let block = self.decode_block(block_index)?;
            self.block_cache.insert(block_index, block);
            self.cache_order.push_back(block_index);
            while self.cache_order.len() > self.cache_capacity {
                if let Some(evicted) = self.cache_order.pop_front() {
                    self.block_cache.remove(&evicted);
                }
            }
        }
        Ok(self.block_cache.get(&block_index).unwrap())
    }

    fn decode_block(&mut self, block_index: u64) -> Result<Vec<Vec<u8>>> {
        let start = self
            .offsets
            .get(block_index as usize)
            .copied()
            .with_context(|| {
                format!(
                    "Missing block offset {block_index} in {}",
                    self.section_name
                )
            })?;
        let end = self
            .offsets
            .get(block_index as usize + 1)
            .copied()
            .with_context(|| {
                format!(
                    "Missing block offset {} in {}",
                    block_index + 1,
                    self.section_name
                )
            })?;

        if end < start {
            bail!(
                "Invalid block offsets in {}: end {} < start {}",
                self.section_name,
                end,
                start
            );
        }

        let block_len = (end - start) as usize;
        let mut data = vec![0u8; block_len];
        self.reader
            .seek(SeekFrom::Start(self.string_buf_start + start))?;
        self.reader.read_exact(&mut data)?;

        let base = block_index * self.block_size;
        let max_entries = (self.string_count - base).min(self.block_size) as usize;
        let mut entries = Vec::with_capacity(max_entries);

        let mut pos = 0usize;
        let mut prev_bytes = Vec::<u8>::new();
        for i in 0..max_entries {
            if pos >= data.len() {
                bail!(
                    "Unexpected end of block in {} at entry {i}",
                    self.section_name
                );
            }

            if i == 0 {
                let rel_end = data[pos..].iter().position(|&b| b == 0).with_context(|| {
                    format!(
                        "Missing null terminator in {} block {block_index}",
                        self.section_name
                    )
                })?;
                let end_pos = pos + rel_end;
                let term_bytes = data[pos..end_pos].to_vec();
                pos = end_pos + 1;
                prev_bytes = term_bytes.clone();
                entries.push(term_bytes);
                continue;
            }

            let (shared, consumed) = decode_vbyte(&data[pos..])?;
            pos += consumed;
            let rel_end = data[pos..].iter().position(|&b| b == 0).with_context(|| {
                format!(
                    "Missing null terminator in {} block {block_index}",
                    self.section_name
                )
            })?;
            let end_pos = pos + rel_end;
            let suffix = &data[pos..end_pos];
            pos = end_pos + 1;

            let shared = shared as usize;
            if shared > prev_bytes.len() {
                bail!(
                    "Invalid shared prefix length {} in {} block {block_index} (prev len {})",
                    shared,
                    self.section_name,
                    prev_bytes.len()
                );
            }

            let mut value_bytes = Vec::with_capacity(shared + suffix.len());
            value_bytes.extend_from_slice(&prev_bytes[..shared]);
            value_bytes.extend_from_slice(suffix);
            prev_bytes = value_bytes.clone();
            entries.push(value_bytes);
        }

        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Dictionary resolver (ID→term and term→ID)
// ---------------------------------------------------------------------------

/// Resolves HDT dictionary IDs to raw term bytes and vice-versa.
///
/// ID scheme:
/// - Subjects: IDs 1..=shared_count in shared, then 1..=subjects_count → global shared_count + local
/// - Objects:  IDs 1..=shared_count in shared, then 1..=objects_count  → global shared_count + local
/// - Predicates: IDs 1..=predicates_count (own space)
pub struct DictionaryResolver {
    pub shared: PfcSectionIndex,
    pub subjects: PfcSectionIndex,
    pub predicates: PfcSectionIndex,
    pub objects: PfcSectionIndex,
}

impl DictionaryResolver {
    /// Resolve a subject ID to raw term bytes.
    pub fn subject_term(&mut self, subject_id: u64, buf: &mut Vec<u8>) -> Result<()> {
        let shared_count = self.shared.string_count;
        if subject_id == 0 {
            bail!("Invalid subject ID 0");
        }
        if subject_id <= shared_count {
            return self.shared.get_bytes(subject_id, buf);
        }
        let local = subject_id - shared_count;
        self.subjects.get_bytes(local, buf)
    }

    /// Resolve a predicate ID to raw term bytes.
    pub fn predicate_term(&mut self, predicate_id: u64, buf: &mut Vec<u8>) -> Result<()> {
        self.predicates.get_bytes(predicate_id, buf)
    }

    /// Resolve an object ID to raw term bytes.
    pub fn object_term(&mut self, object_id: u64, buf: &mut Vec<u8>) -> Result<()> {
        let shared_count = self.shared.string_count;
        if object_id == 0 {
            bail!("Invalid object ID 0");
        }
        if object_id <= shared_count {
            return self.shared.get_bytes(object_id, buf);
        }
        let local = object_id - shared_count;
        self.objects.get_bytes(local, buf)
    }

    /// Locate a subject term, returning its global ID.
    ///
    /// Searches the shared section first; if not found, searches the subject-only section
    /// and offsets by `shared_count`.
    pub fn locate_subject(&mut self, term: &[u8]) -> Result<Option<u64>> {
        if let Some(id) = self.shared.locate(term)? {
            return Ok(Some(id));
        }
        let shared_count = self.shared.string_count;
        if let Some(local) = self.subjects.locate(term)? {
            return Ok(Some(shared_count + local));
        }
        Ok(None)
    }

    /// Locate an object term, returning its global ID.
    ///
    /// Searches the shared section first; if not found, searches the object-only section
    /// and offsets by `shared_count`.
    pub fn locate_object(&mut self, term: &[u8]) -> Result<Option<u64>> {
        if let Some(id) = self.shared.locate(term)? {
            return Ok(Some(id));
        }
        let shared_count = self.shared.string_count;
        if let Some(local) = self.objects.locate(term)? {
            return Ok(Some(shared_count + local));
        }
        Ok(None)
    }

    /// Locate a predicate term, returning its ID.
    pub fn locate_predicate(&mut self, term: &[u8]) -> Result<Option<u64>> {
        self.predicates.locate(term)
    }
}

// ---------------------------------------------------------------------------
// Open HDT file: scan structure, build dictionary
// ---------------------------------------------------------------------------

/// Open an HDT file, scan its structure, and build a `DictionaryResolver`.
///
/// Returns both the section byte offsets (for streaming BitmapTriples) and
/// the dictionary resolver (for ID→term / term→ID lookups).
pub fn open_hdt(
    hdt_path: &Path,
    memory_limit: usize,
) -> Result<(HdtSectionOffsets, DictionaryResolver)> {
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
    let header_text =
        String::from_utf8(header_buf).context("Header content is not valid UTF-8")?;
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

    // Dump's memory is almost entirely the PFC block cache — the only other
    // allocations are block-offset vectors (tens of MB) and I/O buffers (~1 MB).
    // Reserve a fixed 64 MB for those, then split the rest across 4 sections.
    const RESERVED_BYTES: usize = 64 * 1024 * 1024;
    let cache_budget_per_section = memory_limit.saturating_sub(RESERVED_BYTES) / 4;

    let dictionary = DictionaryResolver {
        shared: PfcSectionIndex::read_from(
            &mut reader,
            hdt_path,
            "shared",
            cache_budget_per_section,
        )?,
        subjects: PfcSectionIndex::read_from(
            &mut reader,
            hdt_path,
            "subjects",
            cache_budget_per_section,
        )?,
        predicates: PfcSectionIndex::read_from(
            &mut reader,
            hdt_path,
            "predicates",
            cache_budget_per_section,
        )?,
        objects: PfcSectionIndex::read_from(
            &mut reader,
            hdt_path,
            "objects",
            cache_budget_per_section,
        )?,
    };

    let triples_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read triples control info")?;
    if triples_ci.control_type != ControlType::Triples {
        bail!(
            "Expected triples control info, found {:?}",
            triples_ci.control_type
        );
    }
    if triples_ci.format != TRIPLES_BITMAP_FORMAT {
        bail!(
            "Unsupported triples format: {} (expected {})",
            triples_ci.format,
            TRIPLES_BITMAP_FORMAT
        );
    }

    let (by_start, by_bits) =
        skip_bitmap_section(&mut reader).context("Failed to scan BitmapY")?;
    let (bz_start, _bz_bits) =
        skip_bitmap_section(&mut reader).context("Failed to scan BitmapZ")?;
    let (ay_start, ay_entries, _ay_bpe) =
        skip_log_array_section(&mut reader).context("Failed to scan ArrayY")?;
    let (az_start, az_entries, _az_bpe) =
        skip_log_array_section(&mut reader).context("Failed to scan ArrayZ")?;

    if az_entries != num_triples {
        bail!(
            "ArrayZ size mismatch: header has {num_triples} triples but ArrayZ has {az_entries} entries"
        );
    }
    if by_bits != ay_entries {
        bail!(
            "BitmapY/ArrayY mismatch: BitmapY has {by_bits} bits but ArrayY has {ay_entries} entries"
        );
    }

    let offsets = HdtSectionOffsets {
        num_triples,
        num_sp_pairs: ay_entries,
        by_start,
        bz_start,
        ay_start,
        az_start,
    };

    Ok((offsets, dictionary))
}

// ---------------------------------------------------------------------------
// Parse triple count from HDT header
// ---------------------------------------------------------------------------

fn parse_num_triples_from_header(header: &str) -> Result<u64> {
    const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";
    const HDT_TRIPLES_NUM: &str = "http://purl.org/HDT/hdt#triplesnumTriples";

    let mut value_from_void: Option<u64> = None;
    let mut value_from_hdt: Option<u64> = None;

    let parser =
        RdfParser::from_format(RdfFormat::NTriples).for_reader(Cursor::new(header.as_bytes()));

    for quad_result in parser {
        let quad = quad_result.context("Invalid N-Triples in HDT header metadata")?;
        let predicate = quad.predicate.as_str();

        if predicate != VOID_TRIPLES && predicate != HDT_TRIPLES_NUM {
            continue;
        }

        let oxrdf::Term::Literal(literal) = quad.object else {
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

// ---------------------------------------------------------------------------
// BitmapTriples scanner
// ---------------------------------------------------------------------------

/// Forward-only streaming iterator over all triples in BitmapTriples (SPO) order.
///
/// Emits `(subject_id, predicate_id, object_id)` tuples. Each call to
/// `next_triple()` reads one triple from the underlying file handles.
/// Call `finish()` after the last triple to verify CRCs.
pub struct BitmapTriplesScanner {
    bitmap_y: StreamingBitmapDecoder<BufReader<File>>,
    bitmap_z: StreamingBitmapDecoder<BufReader<File>>,
    array_y: StreamingLogArrayDecoder<BufReader<File>>,
    array_z: StreamingLogArrayDecoder<BufReader<File>>,
    num_triples: u64,
    num_sp_pairs: u64,
    current_subject: u64,
    current_predicate: u64,
    pos_y: u64,
    pos_z: u64,
}

impl BitmapTriplesScanner {
    /// Open streaming decoders at the given section offsets.
    pub fn new(offsets: &HdtSectionOffsets, hdt_path: &Path) -> Result<Self> {
        let open_at = |offset: u64| -> Result<BufReader<File>> {
            let mut f = File::open(hdt_path)?;
            f.seek(SeekFrom::Start(offset))?;
            Ok(BufReader::with_capacity(256 * 1024, f))
        };

        let bitmap_y = StreamingBitmapDecoder::new(open_at(offsets.by_start)?)
            .context("Failed to create BitmapY decoder")?;
        let bitmap_z = StreamingBitmapDecoder::new(open_at(offsets.bz_start)?)
            .context("Failed to create BitmapZ decoder")?;
        let mut array_y = StreamingLogArrayDecoder::new(open_at(offsets.ay_start)?)
            .context("Failed to create ArrayY decoder")?;
        let array_z = StreamingLogArrayDecoder::new(open_at(offsets.az_start)?)
            .context("Failed to create ArrayZ decoder")?;

        // Pre-load the first predicate (index 0 in ArrayY).
        let initial_predicate = if offsets.num_sp_pairs > 0 {
            array_y
                .next_entry()?
                .context("ArrayY unexpectedly empty")?
        } else {
            0
        };

        Ok(Self {
            bitmap_y,
            bitmap_z,
            array_y,
            array_z,
            num_triples: offsets.num_triples,
            num_sp_pairs: offsets.num_sp_pairs,
            current_subject: 1,
            current_predicate: initial_predicate,
            pos_y: 0,
            pos_z: 0,
        })
    }

    /// Return the next `(subject_id, predicate_id, object_id)` triple, or `None` at end.
    pub fn next_triple(&mut self) -> Result<Option<(u64, u64, u64)>> {
        if self.pos_z >= self.num_triples {
            return Ok(None);
        }

        let object = self
            .array_z
            .next_entry()?
            .with_context(|| format!("ArrayZ ended early at position {}", self.pos_z))?;

        if object == 0 {
            bail!("Invalid object ID 0 at triple position {}", self.pos_z);
        }
        if self.current_predicate == 0 {
            bail!(
                "Invalid predicate ID 0 at triple position {}",
                self.pos_z
            );
        }

        let triple = (self.current_subject, self.current_predicate, object);

        let bz_bit = self
            .bitmap_z
            .next_bit()?
            .with_context(|| format!("BitmapZ ended early at position {}", self.pos_z))?;

        if bz_bit {
            let by_bit = self
                .bitmap_y
                .next_bit()?
                .with_context(|| format!("BitmapY ended early at pos_y {}", self.pos_y))?;

            if by_bit {
                self.current_subject += 1;
            }

            self.pos_y += 1;
            if self.pos_y < self.num_sp_pairs {
                self.current_predicate = self
                    .array_y
                    .next_entry()?
                    .with_context(|| format!("ArrayY ended early at pos_y {}", self.pos_y))?;
            }
        }

        self.pos_z += 1;
        Ok(Some(triple))
    }

    /// Verify that all sections were fully consumed and that CRCs match.
    ///
    /// Call this after reading all triples. Skip if using early exit (`--limit` etc.).
    #[allow(dead_code)]
    pub fn finish(self) -> Result<()> {
        if self.pos_y != self.num_sp_pairs {
            bail!(
                "Bitmap boundary count mismatch: got {}, expected {}",
                self.pos_y,
                self.num_sp_pairs
            );
        }
        self.bitmap_y
            .finish()
            .context("BitmapY CRC verification failed")?;
        self.bitmap_z
            .finish()
            .context("BitmapZ CRC verification failed")?;
        self.array_y
            .finish()
            .context("ArrayY CRC verification failed")?;
        self.array_z
            .finish()
            .context("ArrayZ CRC verification failed")?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// N-Triples term writing (tab-delimited format: S\tP\tO\t.\n)
// ---------------------------------------------------------------------------

/// Write one triple in tab-delimited N-Triples format to `w`.
pub fn write_triple_tab(
    w: &mut impl Write,
    subject: &[u8],
    predicate: &[u8],
    object: &[u8],
) -> std::io::Result<()> {
    write_nt_subject(w, subject)?;
    w.write_all(b"\t")?;
    w.write_all(b"<")?;
    w.write_all(predicate)?;
    w.write_all(b">")?;
    w.write_all(b"\t")?;
    write_nt_object(w, object)?;
    w.write_all(b"\t.\n")
}

/// Write a subject term (IRI or blank node) in N-Triples format.
pub(crate) fn write_nt_subject(w: &mut impl Write, term: &[u8]) -> std::io::Result<()> {
    if term.starts_with(b"_:") {
        w.write_all(term)
    } else {
        w.write_all(b"<")?;
        w.write_all(term)?;
        w.write_all(b">")
    }
}

/// Write an object term (IRI, blank node, or literal) in N-Triples format.
///
/// Literal values are escaped for N-Triples (HDT stores raw unescaped UTF-8).
pub(crate) fn write_nt_object(w: &mut impl Write, term: &[u8]) -> std::io::Result<()> {
    if term.starts_with(b"\"") {
        write_nt_literal(w, term)
    } else if term.starts_with(b"_:") {
        w.write_all(term)
    } else {
        w.write_all(b"<")?;
        w.write_all(term)?;
        w.write_all(b">")
    }
}

/// Write a literal in N-Triples format with proper value escaping.
///
/// HDT stores literals as `"raw value"`, `"raw value"@lang`, or `"raw value"^^<datatype>`.
/// The value portion may contain raw `"`, `\`, newlines, etc. that must be escaped.
fn write_nt_literal(w: &mut impl Write, term: &[u8]) -> std::io::Result<()> {
    debug_assert!(term.first() == Some(&b'"'));

    let (value_end, suffix_start) = find_literal_boundary(term);
    let value = &term[1..value_end];

    w.write_all(b"\"")?;
    write_escaped_literal_value(w, value)?;
    w.write_all(b"\"")?;
    if suffix_start < term.len() {
        w.write_all(&term[suffix_start..])?;
    }
    Ok(())
}

/// Find the boundary between the raw value and the suffix in an HDT literal.
///
/// Returns `(value_end, suffix_start)` where:
/// - `value` is `term[1..value_end]`
/// - `suffix` is `term[suffix_start..]` (e.g. `^^<datatype>` or `@lang`, empty for simple)
pub(crate) fn find_literal_boundary(term: &[u8]) -> (usize, usize) {
    let len = term.len();
    if len < 2 {
        return (len, len);
    }

    match term[len - 1] {
        b'>' => {
            let mut i = len - 2;
            while i >= 4 {
                if term[i] == b'<'
                    && term[i - 1] == b'^'
                    && term[i - 2] == b'^'
                    && term[i - 3] == b'"'
                {
                    return (i - 3, i - 2);
                }
                i -= 1;
            }
        }
        b'"' => return (len - 1, len),
        b if b.is_ascii_alphanumeric() || b == b'-' => {
            let mut tag_start = len - 1;
            while tag_start > 0
                && (term[tag_start - 1].is_ascii_alphanumeric() || term[tag_start - 1] == b'-')
            {
                tag_start -= 1;
            }
            if tag_start >= 2 && term[tag_start - 1] == b'@' && term[tag_start - 2] == b'"' {
                return (tag_start - 2, tag_start - 1);
            }
        }
        _ => {}
    }

    (len, len)
}

/// Write a literal value with N-Triples escaping.
pub(crate) fn write_escaped_literal_value(w: &mut impl Write, value: &[u8]) -> std::io::Result<()> {
    let mut start = 0;
    for (i, &b) in value.iter().enumerate() {
        let escape: &[u8] = match b {
            b'\\' => b"\\\\",
            b'"' => b"\\\"",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x08 => b"\\b",
            0x0C => b"\\f",
            0x00..=0x1F => {
                if start < i {
                    w.write_all(&value[start..i])?;
                }
                write!(w, "\\u{b:04X}")?;
                start = i + 1;
                continue;
            }
            _ => continue,
        };
        if start < i {
            w.write_all(&value[start..i])?;
        }
        w.write_all(escape)?;
        start = i + 1;
    }
    if start < value.len() {
        w.write_all(&value[start..])?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// N-Triples query term writing (for debug / output of bound terms)
// ---------------------------------------------------------------------------

/// Buffered output writer — concrete enum so callers get monomorphized
/// (and inlined) `Write` calls instead of dynamic dispatch through `dyn Write`.
pub enum OutputWriter {
    File(BufWriter<File>),
    Stdout(BufWriter<std::io::Stdout>),
}

impl Write for OutputWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(w) => w.write(buf),
            Self::Stdout(w) => w.write(buf),
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::File(w) => w.write_all(buf),
            Self::Stdout(w) => w.write_all(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(w) => w.flush(),
            Self::Stdout(w) => w.flush(),
        }
    }
}

/// Create a buffered output to either stdout or a file.
pub fn make_writer(output_path: Option<&Path>) -> Result<OutputWriter> {
    match output_path {
        Some(path) => {
            let file = File::create(path)
                .with_context(|| format!("Failed to create output file {}", path.display()))?;
            Ok(OutputWriter::File(BufWriter::with_capacity(
                256 * 1024,
                file,
            )))
        }
        None => Ok(OutputWriter::Stdout(BufWriter::with_capacity(
            256 * 1024,
            std::io::stdout(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_to_string(f: impl Fn(&mut Vec<u8>) -> std::io::Result<()>) -> String {
        let mut buf = Vec::new();
        f(&mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_write_subject_iri() {
        let result = write_to_string(|w| write_nt_subject(w, b"http://example.org/s"));
        assert_eq!(result, "<http://example.org/s>");
    }

    #[test]
    fn test_write_subject_blank_node() {
        let result = write_to_string(|w| write_nt_subject(w, b"_:b0"));
        assert_eq!(result, "_:b0");
    }

    #[test]
    fn test_write_object_iri() {
        let result = write_to_string(|w| write_nt_object(w, b"http://example.org/o"));
        assert_eq!(result, "<http://example.org/o>");
    }

    #[test]
    fn test_write_literal_simple() {
        let result = write_to_string(|w| write_nt_object(w, b"\"hello\""));
        assert_eq!(result, "\"hello\"");
    }

    #[test]
    fn test_write_literal_typed() {
        let result = write_to_string(|w| {
            write_nt_object(w, b"\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>")
        });
        assert_eq!(
            result,
            "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
    }

    #[test]
    fn test_write_literal_language() {
        let result = write_to_string(|w| write_nt_object(w, b"\"bonjour\"@fr"));
        assert_eq!(result, "\"bonjour\"@fr");
    }

    #[test]
    fn test_write_literal_embedded_quote() {
        let result = write_to_string(|w| write_nt_object(w, b"\"he said \"hi\"\""));
        assert_eq!(result, r#""he said \"hi\"""#);
    }

    #[test]
    fn test_write_triple_tab() {
        let mut buf = Vec::new();
        write_triple_tab(
            &mut buf,
            b"http://example.org/s",
            b"http://example.org/p",
            b"http://example.org/o",
        )
        .unwrap();
        assert_eq!(
            buf,
            b"<http://example.org/s>\t<http://example.org/p>\t<http://example.org/o>\t.\n"
        );
    }

    #[test]
    fn test_find_boundary_typed() {
        let term = b"\"value\"^^<http://example.org/type>";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"value");
        assert_eq!(&term[ss..], b"^^<http://example.org/type>");
    }

    #[test]
    fn test_find_boundary_language() {
        let term = b"\"value\"@en";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"value");
        assert_eq!(&term[ss..], b"@en");
    }

    #[test]
    fn test_find_boundary_simple() {
        let term = b"\"value\"";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"value");
        assert_eq!(ss, term.len());
    }
}
