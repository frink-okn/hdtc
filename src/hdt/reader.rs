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
    ControlInfo, LogArrayReader, StreamingBitmapDecoder, StreamingLogArrayDecoder, decode_vbyte,
    encode_vbyte, read_vbyte,
};
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const PFC_SECTION_TYPE: u8 = 0x02;
use crate::hdt::pfc_reader::PfcSection;
use crate::hdt::sections::scan_hdt_sections;

// ---------------------------------------------------------------------------
// HDT identity digest
// ---------------------------------------------------------------------------

/// Byte offset where an HDT file's dictionary begins: past the global control
/// info, the header control info, and the header N-Triples blob.
///
/// Identity digests start here rather than at byte 0. `hdtc header` rewrites
/// copy the dictionary and triples verbatim, so a digest that skips the header
/// keeps derived artifacts (graph sidecars, sketches) bound to an HDT across
/// header edits.
pub(crate) fn hdt_data_offset<R: Read + Seek>(reader: &mut R) -> Result<u64> {
    ControlInfo::read_from(reader).context("Failed to read HDT global control info")?;
    let header =
        ControlInfo::read_from(reader).context("Failed to read HDT header control info")?;
    let header_length: u64 = header
        .get_property("length")
        .context("HDT header is missing length")?
        .parse()
        .context("Invalid HDT header length")?;
    reader.seek(SeekFrom::Current(
        i64::try_from(header_length).context("HDT header is too large to seek")?,
    ))?;
    Ok(reader.stream_position()?)
}

/// SHA-256 over every remaining byte of `reader`.
pub fn sha256_to_end<R: Read>(reader: &mut R) -> Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    std::io::copy(reader, &mut hasher)?;
    Ok(hasher.finalize().into())
}

/// SHA-256 identity digest of an HDT file's dictionary and triples, excluding
/// the mutable header. See [`hdt_data_offset`].
pub(crate) fn hdt_data_digest(path: &Path) -> Result<[u8; 32]> {
    let file =
        File::open(path).with_context(|| format!("Failed to open HDT file {}", path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    hdt_data_offset(&mut reader)?;
    sha256_to_end(&mut reader)
}

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
/// Supports ID→term lookup (`get_bytes`), term→ID lookup (`locate`), and
/// lower-bound/prefix-range searches.
/// Decoded blocks are cached first-in, first-out under a byte budget.
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
    /// Cached block indexes in insertion order; the front is evicted first.
    cache_order: VecDeque<u64>,
    /// [`decoded_block_bytes`] summed over the cached blocks.
    cache_bytes: usize,
    cache_budget: usize,
}

/// Resident bytes of one cached block: its terms, their `Vec` headers, and an
/// allowance for the allocator and the cache's own map and queue entries.
///
/// Counted per block rather than assumed, because a term's size is unbounded: a
/// block of geometry literals can be a thousand times a block of IRIs.
fn decoded_block_bytes(block: &[Vec<u8>]) -> usize {
    // An allocator chunk header, and rounding to its 16-byte granularity.
    const ALLOCATION_OVERHEAD: usize = 16;
    // The map slot, the queue slot, and the outer `Vec`'s own allocation.
    const BLOCK_OVERHEAD: usize = 64;
    let terms: usize = block
        .iter()
        .map(|term| term.capacity() + std::mem::size_of::<Vec<u8>>() + ALLOCATION_OVERHEAD)
        .sum();
    BLOCK_OVERHEAD + terms
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
            cache_bytes: 0,
            // A floor so a tiny budget still caches a few dozen blocks of IRIs.
            cache_budget: cache_budget.max(128 * 1024),
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

    /// Return the 1-based insertion position of the first entry not less than `term`.
    ///
    /// The returned position is in `1..=string_count + 1`; the final value is the
    /// past-the-end sentinel. Block heads are binary-searched before decoding the
    /// single candidate block.
    pub fn lower_bound(&mut self, term: &[u8]) -> Result<u64> {
        let past_end = self
            .string_count
            .checked_add(1)
            .context("PFC string count has no past-the-end position")?;
        if self.string_count == 0 {
            return Ok(past_end);
        }

        let n_blocks = self.offsets.len().saturating_sub(1);
        if n_blocks == 0 {
            bail!(
                "Missing block offsets in non-empty {} section",
                self.section_name
            );
        }

        // Upper-bound the block heads, then search the block immediately before
        // that point. If no block head is <= term, insertion is at the beginning.
        let mut lo = 0usize;
        let mut hi = n_blocks;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let first = self.read_first_string_of_block(mid)?;
            if first.as_slice() <= term {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return Ok(1);
        }

        let block_index = lo - 1;
        let block_size = self.block_size;
        let block = self.get_or_decode_block(block_index as u64)?;
        let entry_index = block.partition_point(|entry| entry.as_slice() < term);
        let base_id = (block_index as u64)
            .checked_mul(block_size)
            .and_then(|value| value.checked_add(1))
            .context("PFC dictionary ID overflow")?;
        base_id
            .checked_add(entry_index as u64)
            .map(|id| id.min(past_end))
            .context("PFC dictionary ID overflow")
    }

    /// Return the half-open 1-based ID range containing entries with `prefix`.
    pub fn prefix_range(&mut self, prefix: &[u8]) -> Result<std::ops::Range<u64>> {
        let start = self.lower_bound(prefix)?;
        let end = match prefix_successor(prefix) {
            Some(upper) => self.lower_bound(&upper)?,
            None => self
                .string_count
                .checked_add(1)
                .context("PFC string count has no past-the-end position")?,
        };
        Ok(start..end)
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

    /// Visit every term in ID order, reading the section front to back.
    ///
    /// A full scan never revisits a block, so it bypasses the block cache. Sent
    /// through [`get_bytes`](Self::get_bytes) instead, it would fill the cache to
    /// capacity with terms that are never read again — and capacity is counted in
    /// blocks sized for short terms, so a section of long literals would hold many
    /// times its budget.
    pub fn for_each_term(&mut self, mut visit: impl FnMut(u64, &[u8]) -> Result<()>) -> Result<()> {
        let blocks = self.offsets.len().saturating_sub(1) as u64;
        if blocks == 0 {
            return Ok(());
        }
        // Blocks are contiguous, so one seek positions the whole scan.
        let (first, _) = self.block_range(0)?;
        self.reader
            .seek(SeekFrom::Start(self.string_buf_start + first))?;

        let mut data = Vec::new();
        let mut term = Vec::new();
        let mut id = 0u64;
        for block_index in 0..blocks {
            let (start, end) = self.block_range(block_index)?;
            data.resize((end - start) as usize, 0);
            self.reader.read_exact(&mut data)?;
            self.decode_terms(block_index, &data, &mut term, |value| {
                id += 1;
                visit(id, value)
            })?;
        }
        Ok(())
    }

    /// Return a decoded block, using the block cache.
    ///
    /// The block just decoded is never evicted, since it is the one returned; a
    /// single block larger than the whole budget is the only way to exceed it.
    fn get_or_decode_block(&mut self, block_index: u64) -> Result<&Vec<Vec<u8>>> {
        if !self.block_cache.contains_key(&block_index) {
            let block = self.decode_block(block_index)?;
            self.cache_bytes += decoded_block_bytes(&block);
            self.block_cache.insert(block_index, block);
            self.cache_order.push_back(block_index);
            while self.cache_bytes > self.cache_budget && self.cache_order.len() > 1 {
                if let Some(evicted) = self.cache_order.pop_front()
                    && let Some(block) = self.block_cache.remove(&evicted)
                {
                    self.cache_bytes -= decoded_block_bytes(&block);
                }
            }
        }
        Ok(self.block_cache.get(&block_index).unwrap())
    }

    fn decode_block(&mut self, block_index: u64) -> Result<Vec<Vec<u8>>> {
        let (start, end) = self.block_range(block_index)?;
        let mut data = vec![0u8; (end - start) as usize];
        self.reader
            .seek(SeekFrom::Start(self.string_buf_start + start))?;
        self.reader.read_exact(&mut data)?;

        let mut entries = Vec::with_capacity(self.block_entries(block_index) as usize);
        let mut term = Vec::new();
        self.decode_terms(block_index, &data, &mut term, |value| {
            entries.push(value.to_vec());
            Ok(())
        })?;
        Ok(entries)
    }

    /// Terms in block `block_index`: `block_size`, except in a short final block.
    fn block_entries(&self, block_index: u64) -> u64 {
        (self.string_count - block_index * self.block_size).min(self.block_size)
    }

    /// Decode one front-coded block, handing each term to `visit` in order.
    ///
    /// Each term is decoded in place over its predecessor in `term`, so decoding
    /// allocates nothing per term.
    fn decode_terms(
        &self,
        block_index: u64,
        data: &[u8],
        term: &mut Vec<u8>,
        mut visit: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut pos = 0usize;
        for i in 0..self.block_entries(block_index) {
            if pos >= data.len() {
                bail!(
                    "Unexpected end of block in {} at entry {i}",
                    self.section_name
                );
            }

            if i == 0 {
                term.clear();
            } else {
                let (shared, consumed) = decode_vbyte(&data[pos..])?;
                pos += consumed;
                let shared = shared as usize;
                if shared > term.len() {
                    bail!(
                        "Invalid shared prefix length {} in {} block {block_index} (prev len {})",
                        shared,
                        self.section_name,
                        term.len()
                    );
                }
                term.truncate(shared);
            }

            let rel_end = data[pos..].iter().position(|&b| b == 0).with_context(|| {
                format!(
                    "Missing null terminator in {} block {block_index}",
                    self.section_name
                )
            })?;
            term.extend_from_slice(&data[pos..pos + rel_end]);
            pos += rel_end + 1;
            visit(term)?;
        }
        Ok(())
    }

    /// The byte range of block `block_index` within the string buffer.
    fn block_range(&self, block_index: u64) -> Result<(u64, u64)> {
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
        Ok((start, end))
    }
}

/// Smallest byte string greater than every string beginning with `prefix`.
/// Returns `None` when no finite upper bound exists.
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.pop() {
        if last != u8::MAX {
            upper.push(last + 1);
            return Some(upper);
        }
    }
    None
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

    // One structural walk locates every section; the dictionary indexes are
    // then built by seeking back to the four PFC sections it recorded. The
    // walk also checks the formats, the SPO ordering, and that each level's
    // bitmap and array agree on their length.
    let sections = scan_hdt_sections(&mut reader)
        .with_context(|| format!("Failed to scan HDT file {}", hdt_path.display()))?;

    let header_text = sections.read_header(&mut reader)?;
    let num_triples = parse_num_triples_from_header(&header_text)
        .context("Failed to parse triple count from header metadata")?;
    if sections.num_triples() != num_triples {
        bail!(
            "ArrayZ size mismatch: header has {num_triples} triples but ArrayZ has {} entries",
            sections.num_triples()
        );
    }

    // Dump's memory is almost entirely the PFC block cache — the only other
    // allocations are block-offset vectors (tens of MB) and I/O buffers (~1 MB).
    // Reserve a fixed 64 MB for those, then split the rest across 4 sections.
    const RESERVED_BYTES: usize = 64 * 1024 * 1024;
    let cache_budget_per_section = memory_limit.saturating_sub(RESERVED_BYTES) / 4;

    let mut index_of = |section: &PfcSection, name: &'static str| -> Result<PfcSectionIndex> {
        reader.seek(SeekFrom::Start(section.section_start))?;
        PfcSectionIndex::read_from(&mut reader, hdt_path, name, cache_budget_per_section)
    };
    let dictionary = DictionaryResolver {
        shared: index_of(&sections.shared, "shared")?,
        subjects: index_of(&sections.subjects, "subjects")?,
        predicates: index_of(&sections.predicates, "predicates")?,
        objects: index_of(&sections.objects, "objects")?,
    };

    let offsets = HdtSectionOffsets {
        num_triples,
        num_sp_pairs: sections.num_sp_pairs(),
        by_start: sections.bitmap_y.section_start,
        bz_start: sections.bitmap_z.section_start,
        ay_start: sections.array_y.section_start,
        az_start: sections.array_z.section_start,
    };

    Ok((offsets, dictionary))
}

// ---------------------------------------------------------------------------
// Parse triple count from HDT header
// ---------------------------------------------------------------------------

fn parse_num_triples_from_header(header: &str) -> Result<u64> {
    Ok(crate::rdf::header_counts(header.as_bytes())?.triples)
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
            array_y.next_entry()?.context("ArrayY unexpectedly empty")?
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
            bail!("Invalid predicate ID 0 at triple position {}", self.pos_z);
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
    use crate::dictionary::pfc::PfcEncoder;
    use std::fs::File;

    fn write_to_string(f: impl Fn(&mut Vec<u8>) -> std::io::Result<()>) -> String {
        let mut buf = Vec::new();
        f(&mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn pfc_index(
        strings: &[&str],
        block_size: usize,
    ) -> (tempfile::NamedTempFile, PfcSectionIndex) {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = PfcEncoder::with_block_size(block_size);
        for value in strings {
            encoder.push(*value);
        }
        encoder.write_to(&mut temp).unwrap();
        temp.flush().unwrap();

        let mut reader = File::open(temp.path()).unwrap();
        let index =
            PfcSectionIndex::read_from(&mut reader, temp.path(), "test", 64 * 1024).unwrap();
        (temp, index)
    }

    #[test]
    fn lower_bound_and_prefix_range_cross_blocks() {
        let (_temp, mut index) =
            pfc_index(&["alpha", "beta", "delta", "epsilon", "gamma", "zeta"], 2);

        assert_eq!(index.lower_bound(b"").unwrap(), 1);
        assert_eq!(index.lower_bound(b"alpha").unwrap(), 1);
        assert_eq!(index.lower_bound(b"bet").unwrap(), 2);
        assert_eq!(index.lower_bound(b"beta").unwrap(), 2);
        assert_eq!(index.lower_bound(b"charlie").unwrap(), 3);
        assert_eq!(index.lower_bound(b"zzzz").unwrap(), 7);
        assert_eq!(index.prefix_range(b"").unwrap(), 1..7);
        assert_eq!(index.prefix_range(b"e").unwrap(), 4..5);
        assert_eq!(index.prefix_range(b"g").unwrap(), 5..6);
        assert_eq!(index.prefix_range(b"missing").unwrap(), 6..6);
    }

    #[test]
    fn lower_bound_matches_a_sequential_search() {
        let strings = [
            "aa0", "aa1", "aa2", "ab0", "ab1", "ba0", "ba1", "ba2", "bb0", "ca0", "za0",
        ];
        let (_temp, mut index) = pfc_index(&strings, 3);
        for query in ["", "a", "aa0", "aa15", "ab", "b", "ba2", "bc", "ca0", "zz"] {
            let expected =
                strings.partition_point(|value| value.as_bytes() < query.as_bytes()) as u64 + 1;
            assert_eq!(index.lower_bound(query.as_bytes()).unwrap(), expected);
        }
    }

    #[test]
    fn for_each_term_visits_every_term_in_order_without_caching() {
        // Eleven terms in blocks of three: the last block is short, and the
        // shared prefixes exercise front-coding across each block.
        let strings = [
            "aa0", "aa1", "aa2", "ab0", "ab1", "ba0", "ba1", "ba2", "bb0", "ca0", "za0",
        ];
        let (_temp, mut index) = pfc_index(&strings, 3);

        let mut visited = Vec::new();
        index
            .for_each_term(|id, term| {
                visited.push((id, String::from_utf8(term.to_vec()).unwrap()));
                Ok(())
            })
            .unwrap();
        let expected: Vec<_> = strings
            .iter()
            .enumerate()
            .map(|(i, value)| (i as u64 + 1, value.to_string()))
            .collect();
        assert_eq!(visited, expected);
        assert!(index.block_cache.is_empty());

        // The scan leaves random access working.
        let mut buf = Vec::new();
        index.get_bytes(7, &mut buf).unwrap();
        assert_eq!(buf, b"ba1");

        let (_temp, mut empty) = pfc_index(&[], 4);
        empty
            .for_each_term(|_, _| panic!("an empty section has no terms"))
            .unwrap();
    }

    /// The cached blocks' accounted bytes, recomputed from the blocks themselves.
    fn recount_cache(index: &PfcSectionIndex) -> usize {
        index
            .block_cache
            .values()
            .map(|block| decoded_block_bytes(block))
            .sum()
    }

    #[test]
    fn block_cache_is_bounded_by_bytes_not_blocks() {
        // Ten blocks of four 20 KB terms: each block alone is over half the
        // 128 KiB floor, so no two fit together. A count-bounded cache held all
        // ten, 800 KB, under the same budget.
        let long: Vec<String> = (0..40)
            .map(|i| format!("{i:03}{}", "x".repeat(20_000)))
            .collect();
        let long: Vec<&str> = long.iter().map(String::as_str).collect();
        let (_temp, mut index) = pfc_index(&long, 4);
        let mut buf = Vec::new();
        for id in (1..=40).chain([3, 39, 17]) {
            index.get_bytes(id, &mut buf).unwrap();
            assert_eq!(buf, long[id as usize - 1].as_bytes());
            assert_eq!(index.cache_bytes, recount_cache(&index));
            assert!(index.cache_bytes <= index.cache_budget);
        }
        assert_eq!(index.block_cache.len(), 1);
        assert_eq!(index.cache_order.len(), 1);

        // Short terms still share the budget: every block stays resident.
        let short = [
            "aa0", "aa1", "aa2", "ab0", "ab1", "ba0", "ba1", "ba2", "bb0", "ca0", "za0",
        ];
        let (_temp, mut index) = pfc_index(&short, 3);
        for id in 1..=11 {
            index.get_bytes(id, &mut buf).unwrap();
            assert_eq!(buf, short[id as usize - 1].as_bytes());
        }
        assert_eq!(index.block_cache.len(), 4);
        assert_eq!(index.cache_bytes, recount_cache(&index));
    }

    #[test]
    fn a_block_larger_than_the_budget_is_still_returned() {
        let huge = "y".repeat(300_000);
        let (_temp, mut index) = pfc_index(&["a", &huge, "z"], 2);
        let mut buf = Vec::new();
        index.get_bytes(2, &mut buf).unwrap();
        assert_eq!(buf, huge.as_bytes());
        assert!(index.cache_bytes > index.cache_budget);
        // The next block evicts it rather than joining it.
        index.get_bytes(3, &mut buf).unwrap();
        assert_eq!(buf, b"z");
        assert_eq!(index.block_cache.len(), 1);
        assert_eq!(index.cache_bytes, recount_cache(&index));
    }

    #[test]
    fn for_each_term_stops_at_the_first_visitor_error() {
        let (_temp, mut index) = pfc_index(&["a", "b", "c", "d"], 2);
        let mut seen = 0;
        let error = index
            .for_each_term(|id, _| {
                seen += 1;
                anyhow::ensure!(id < 3, "stop at {id}");
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "stop at 3");
        assert_eq!(seen, 3);
    }

    #[test]
    fn prefix_ranges_handle_empty_sections_and_max_bytes() {
        let (_temp, mut empty) = pfc_index(&[], 4);
        assert_eq!(empty.lower_bound(b"anything").unwrap(), 1);
        assert_eq!(empty.prefix_range(b"").unwrap(), 1..1);
        assert_eq!(empty.prefix_range(&[u8::MAX]).unwrap(), 1..1);
        assert_eq!(prefix_successor(&[b'a', u8::MAX]), Some(vec![b'b']));
        assert_eq!(prefix_successor(&[u8::MAX]), None);
    }

    #[test]
    fn prefix_ranges_compare_unicode_as_utf8_bytes() {
        let (_temp, mut index) = pfc_index(
            &["http://x.example/a", "http://x.example/éclair", "urn:z"],
            2,
        );
        assert_eq!(
            index.prefix_range("http://x.example/é".as_bytes()).unwrap(),
            2..3
        );
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
        assert_eq!(result, "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>");
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
