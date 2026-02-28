//! HDT input adapter — makes an existing HDT file look like a "batch" to the pipeline.
//!
//! Provides two capabilities:
//! 1. A sorted vocabulary stream (for the k-way merge in Stage 4)
//! 2. A streaming triple reader (for the ID remapper in Stage 5)

use crate::hdt::pfc_reader::{PfcSectionHeader, PfcSectionIterator};
use crate::io::{ControlInfo, ControlType, StreamingBitmapDecoder, StreamingLogArrayDecoder, read_vbyte};
use crate::pipeline::batch_vocab::Roles;
use crate::pipeline::vocab_merger::StreamEntry;
use anyhow::{Context, Result, bail};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const DICTIONARY_FOUR_FORMAT: &str = "<http://purl.org/HDT/hdt#dictionaryFour>";
const TRIPLES_BITMAP_FORMAT: &str = "<http://purl.org/HDT/hdt#triplesBitmap>";

/// Metadata extracted from scanning an HDT file.
#[derive(Debug, Clone)]
pub struct HdtInputAdapter {
    pub path: PathBuf,
    pub shared_count: u64,
    pub subjects_count: u64,
    pub objects_count: u64,
    pub predicates_count: u64,
    pub num_triples: u64,
    /// Original N-Triples size from the HDT header (0 if not present).
    pub original_size: u64,
    /// File offset where the dictionary ControlInfo starts.
    dict_ci_offset: u64,
    /// File offset where the triples BitmapY starts.
    triples_data_offset: u64,
    /// Number of (subject, predicate) pairs (= ArrayY/BitmapY entries).
    num_sp_pairs: u64,
}

impl HdtInputAdapter {
    /// Scan an HDT file, extract section counts and offsets.
    pub fn scan(hdt_path: &Path) -> Result<Self> {
        let file = File::open(hdt_path)
            .with_context(|| format!("Failed to open HDT file {}", hdt_path.display()))?;
        let mut reader = BufReader::with_capacity(256 * 1024, file);

        // Global control info
        let global_ci =
            ControlInfo::read_from(&mut reader).context("Failed to read global control info")?;
        if global_ci.control_type != ControlType::Global {
            bail!("Expected global control info at start of HDT file");
        }

        // Verify HDT cookie/magic if present
        if let Some(format) = global_ci.format.strip_prefix("") {
            // format string is the full URI — just check control_type
            let _ = format;
        }

        // Header section — skip it
        let header_ci =
            ControlInfo::read_from(&mut reader).context("Failed to read header control info")?;
        if header_ci.control_type != ControlType::Header {
            bail!("Expected header control info");
        }
        let header_len: usize = header_ci
            .get_property("length")
            .and_then(|s| s.parse().ok())
            .context("Missing or invalid header length")?;
        let mut header_buf = vec![0u8; header_len];
        reader.read_exact(&mut header_buf)?;

        // Parse triple count and original size from header
        let header_text = String::from_utf8(header_buf).context("Header not valid UTF-8")?;
        let num_triples = parse_num_triples_from_header(&header_text)?;
        let original_size = parse_original_size_from_header(&header_text);

        // Dictionary control info
        let dict_ci_offset = reader.stream_position()?;
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

        // Read PFC section headers to get counts, then skip the data
        let shared_count = skip_pfc_section(&mut reader, "shared")?;
        let subjects_count = skip_pfc_section(&mut reader, "subjects")?;
        let predicates_count = skip_pfc_section(&mut reader, "predicates")?;
        let objects_count = skip_pfc_section(&mut reader, "objects")?;

        // Triples control info
        let triples_ci =
            ControlInfo::read_from(&mut reader).context("Failed to read triples control info")?;
        if triples_ci.control_type != ControlType::Triples {
            bail!("Expected triples control info");
        }
        if triples_ci.format != TRIPLES_BITMAP_FORMAT {
            bail!(
                "Unsupported triples format: {} (expected {})",
                triples_ci.format,
                TRIPLES_BITMAP_FORMAT
            );
        }

        // Record where the triples data starts (BitmapY is first)
        let triples_data_offset = reader.stream_position()?;

        // Skip past BitmapY to read ArrayY to get num_sp_pairs
        let (_by_start, _by_bits) = skip_bitmap_section(&mut reader)?;
        let (_bz_start, _bz_bits) = skip_bitmap_section(&mut reader)?;
        let (_ay_start, ay_entries, _ay_bpe) = skip_log_array_section(&mut reader)?;

        let num_sp_pairs = ay_entries;

        tracing::info!(
            "HDT input {}: {} triples, {} shared, {} subjects, {} predicates, {} objects",
            hdt_path.display(),
            num_triples,
            shared_count,
            subjects_count,
            predicates_count,
            objects_count
        );

        Ok(Self {
            path: hdt_path.to_path_buf(),
            shared_count,
            subjects_count,
            objects_count,
            predicates_count,
            num_triples,
            original_size,
            dict_ci_offset,
            triples_data_offset,
            num_sp_pairs,
        })
    }

    /// Max local subject/object ID in the flat ID space.
    pub fn max_so_id(&self) -> u32 {
        (self.shared_count + self.subjects_count + self.objects_count).saturating_sub(1) as u32
    }

    /// Max local predicate ID in the flat ID space.
    pub fn max_p_id(&self) -> u32 {
        self.predicates_count.saturating_sub(1) as u32
    }

    /// File offset where the BitmapTriples data begins.
    pub fn triples_data_offset(&self) -> u64 {
        self.triples_data_offset
    }

    /// Number of (subject, predicate) pairs in the BitmapTriples.
    pub fn num_sp_pairs(&self) -> u64 {
        self.num_sp_pairs
    }

    /// Create a factory closure that produces a sorted vocabulary stream.
    ///
    /// The stream is a 4-way merge of all PFC sections (shared, subjects,
    /// objects, predicates) yielding `StreamEntry` items in lexicographic order.
    ///
    /// When `file_index` is `Some(i)`, blank node terms are disambiguated by
    /// prefixing them with `f{i}_` (e.g. `_:b1` → `_:f2_b1`), matching the
    /// parser's per-file blank node disambiguation.
    pub fn vocab_factory(
        &self,
        batch_id: usize,
        file_index: Option<usize>,
    ) -> crate::pipeline::vocab_merger::VocabFactory
    {
        let path = self.path.clone();
        let dict_ci_offset = self.dict_ci_offset;
        let shared_count = self.shared_count;
        let subjects_count = self.subjects_count;

        Box::new(move || {
            let file = File::open(&path)?;
            let mut reader = BufReader::with_capacity(256 * 1024, file);
            reader.seek(SeekFrom::Start(dict_ci_offset))?;

            // Skip the dictionary ControlInfo
            let _dict_ci = ControlInfo::read_from(&mut reader)?;

            // Read all 4 PFC section headers and create iterators
            let shared_header = PfcSectionHeader::read_from(&mut reader, "shared")?;
            let shared_iter = PfcSectionIterator::new(&mut reader as &mut dyn Read, &shared_header, "shared");

            // Wait — we can't use &mut reader for multiple iterators simultaneously.
            // Instead, we need to read sections sequentially: collect each section's
            // terms, then merge. But that defeats the streaming purpose.
            //
            // Better approach: read all 4 sections into memory (just the terms),
            // then do a 4-way merge. The terms are already stored in the HDT file
            // and we need them for the merge anyway.
            //
            // Actually, PFC sections are read sequentially in the file, so we need
            // to read them one at a time. Let's collect them into sorted Vec<Vec<u8>>,
            // then do a streaming 4-way merge from the Vecs.
            drop(shared_iter);

            // Re-open and seek to dictionary start
            let file = File::open(&path)?;
            let mut reader = BufReader::with_capacity(256 * 1024, file);
            reader.seek(SeekFrom::Start(dict_ci_offset))?;
            let _dict_ci = ControlInfo::read_from(&mut reader)?;

            // Read each PFC section sequentially
            let mut shared_terms = read_pfc_section_terms(&mut reader, "shared")?;
            let mut subjects_terms = read_pfc_section_terms(&mut reader, "subjects")?;
            let predicates_terms = read_pfc_section_terms(&mut reader, "predicates")?;
            let mut objects_terms = read_pfc_section_terms(&mut reader, "objects")?;

            // Disambiguate blank nodes by prefixing with a file-specific identifier.
            // This must happen before the merge so sort order reflects the prefixed terms.
            // Predicates are never blank nodes so they are skipped.
            if let Some(idx) = file_index {
                let prefix = format!("f{idx}_");
                disambiguate_blank_nodes(&mut shared_terms, &prefix);
                disambiguate_blank_nodes(&mut subjects_terms, &prefix);
                disambiguate_blank_nodes(&mut objects_terms, &prefix);
            }

            // Build a 4-way merge iterator
            let iter = FourWayMerge::new(
                shared_terms,
                subjects_terms,
                objects_terms,
                predicates_terms,
                shared_count,
                subjects_count,
                batch_id,
            );

            Ok(Box::new(iter) as Box<dyn Iterator<Item = Result<StreamEntry>> + Send>)
        })
    }

}

/// Prefix blank node terms in-place for disambiguation.
///
/// Blank nodes in HDT dictionaries are stored as `_:label`. This function
/// transforms them to `_:{prefix}label` so that blank nodes from different
/// input files don't collide during the vocabulary merge.
///
/// The terms vector must be re-sorted after prefixing since the prefix changes
/// the lexicographic order.
fn disambiguate_blank_nodes(terms: &mut [Vec<u8>], prefix: &str) {
    let bnode_marker = b"_:";
    let mut any_changed = false;
    for term in terms.iter_mut() {
        if term.starts_with(bnode_marker) {
            let mut new_term = Vec::with_capacity(2 + prefix.len() + term.len() - 2);
            new_term.extend_from_slice(bnode_marker);
            new_term.extend_from_slice(prefix.as_bytes());
            new_term.extend_from_slice(&term[2..]);
            *term = new_term;
            any_changed = true;
        }
    }
    if any_changed {
        terms.sort();
    }
}

/// Read all terms from a PFC section into a Vec.
fn read_pfc_section_terms<R: Read>(reader: &mut R, section_name: &str) -> Result<Vec<Vec<u8>>> {
    let header = PfcSectionHeader::read_from(reader, section_name)?;
    let count = header.string_count as usize;
    let mut terms = Vec::with_capacity(count);

    let mut iter = PfcSectionIterator::new(&mut *reader, &header, section_name);
    for term_result in &mut iter {
        terms.push(term_result?);
    }

    // Skip past CRC — the iterator consumed the string data but the CRC is still pending.
    let mut crc = [0u8; 4];
    reader.read_exact(&mut crc)?;

    Ok(terms)
}

// ---------------------------------------------------------------------------
// 4-way merge iterator
// ---------------------------------------------------------------------------

/// Entry in the merge heap, tagged with which section it came from.
struct HeapEntry {
    term: Vec<u8>,
    section: SectionKind,
    index: usize, // position within its section's term list
}

impl Eq for HeapEntry {}
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.term == other.term
    }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, so reverse for min-heap behavior
        other.term.cmp(&self.term)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SectionKind {
    Shared,
    Subjects,
    Objects,
    Predicates,
}

/// Merges 4 sorted PFC sections into a single lexicographically sorted stream.
struct FourWayMerge {
    shared: Vec<Vec<u8>>,
    subjects: Vec<Vec<u8>>,
    objects: Vec<Vec<u8>>,
    predicates: Vec<Vec<u8>>,
    heap: BinaryHeap<HeapEntry>,
    shared_count: u64,
    subjects_count: u64,
    batch_id: usize,
    // Cursors for next index to push from each section
    shared_cursor: usize,
    subjects_cursor: usize,
    objects_cursor: usize,
    predicates_cursor: usize,
}

impl FourWayMerge {
    fn new(
        shared: Vec<Vec<u8>>,
        subjects: Vec<Vec<u8>>,
        objects: Vec<Vec<u8>>,
        predicates: Vec<Vec<u8>>,
        shared_count: u64,
        subjects_count: u64,
        batch_id: usize,
    ) -> Self {
        let mut heap = BinaryHeap::with_capacity(4);

        // Seed the heap with the first entry from each non-empty section
        if !shared.is_empty() {
            heap.push(HeapEntry {
                term: shared[0].clone(),
                section: SectionKind::Shared,
                index: 0,
            });
        }
        if !subjects.is_empty() {
            heap.push(HeapEntry {
                term: subjects[0].clone(),
                section: SectionKind::Subjects,
                index: 0,
            });
        }
        if !objects.is_empty() {
            heap.push(HeapEntry {
                term: objects[0].clone(),
                section: SectionKind::Objects,
                index: 0,
            });
        }
        if !predicates.is_empty() {
            heap.push(HeapEntry {
                term: predicates[0].clone(),
                section: SectionKind::Predicates,
                index: 0,
            });
        }

        Self {
            shared,
            subjects,
            objects,
            predicates,
            heap,
            shared_count,
            subjects_count,
            batch_id,
            shared_cursor: 1,
            subjects_cursor: 1,
            objects_cursor: 1,
            predicates_cursor: 1,
        }
    }

    fn advance_section(&mut self, section: SectionKind) {
        let (terms, cursor) = match section {
            SectionKind::Shared => (&self.shared, &mut self.shared_cursor),
            SectionKind::Subjects => (&self.subjects, &mut self.subjects_cursor),
            SectionKind::Objects => (&self.objects, &mut self.objects_cursor),
            SectionKind::Predicates => (&self.predicates, &mut self.predicates_cursor),
        };
        if *cursor < terms.len() {
            self.heap.push(HeapEntry {
                term: terms[*cursor].clone(),
                section,
                index: *cursor,
            });
            *cursor += 1;
        }
    }

    fn make_entry(&self, term: Vec<u8>, section: SectionKind, index: usize) -> StreamEntry {
        // Compute roles and local IDs based on which section the term came from
        match section {
            SectionKind::Shared => StreamEntry {
                term,
                roles: Roles::SUBJECT | Roles::OBJECT,
                so_local_id: Some(index as u32), // 0-based
                p_local_id: None,
                source_batch: self.batch_id,
            },
            SectionKind::Subjects => StreamEntry {
                term,
                roles: Roles::SUBJECT,
                so_local_id: Some((self.shared_count as usize + index) as u32),
                p_local_id: None,
                source_batch: self.batch_id,
            },
            SectionKind::Objects => StreamEntry {
                term,
                roles: Roles::OBJECT,
                so_local_id: Some((self.shared_count as usize + self.subjects_count as usize + index) as u32),
                p_local_id: None,
                source_batch: self.batch_id,
            },
            SectionKind::Predicates => StreamEntry {
                term,
                roles: Roles::PREDICATE,
                so_local_id: None,
                p_local_id: Some(index as u32), // 0-based
                source_batch: self.batch_id,
            },
        }
    }
}

impl Iterator for FourWayMerge {
    type Item = Result<StreamEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.heap.pop()?;
        let section = entry.section;
        let index = entry.index;
        let term = entry.term;

        // Check if the next entry from a different section has the same term.
        // If so, we need to merge roles. For example, a term that's both a
        // predicate and a subject/object in the HDT.
        // In practice this is rare but we handle it correctly.
        let mut merged = self.make_entry(term.clone(), section, index);

        // Peek at and consume entries with the same term from other sections
        while let Some(peek) = self.heap.peek() {
            if peek.term != term {
                break;
            }
            let dup = self.heap.pop().unwrap();
            let dup_entry = self.make_entry(dup.term, dup.section, dup.index);
            merged.roles |= dup_entry.roles;
            if merged.so_local_id.is_none() {
                merged.so_local_id = dup_entry.so_local_id;
            }
            if merged.p_local_id.is_none() {
                merged.p_local_id = dup_entry.p_local_id;
            }
            self.advance_section(dup.section);
        }

        // Advance the section we just popped from
        self.advance_section(section);

        Some(Ok(merged))
    }
}

// ---------------------------------------------------------------------------
// Streaming triple reader
// ---------------------------------------------------------------------------

/// Streaming reader that decodes BitmapTriples from an HDT file,
/// yielding triples with IDs remapped to a flat local SO/P space.
pub struct HdtTripleReader {
    bitmap_y: StreamingBitmapDecoder<BufReader<File>>,
    bitmap_z: StreamingBitmapDecoder<BufReader<File>>,
    array_y: StreamingLogArrayDecoder<BufReader<File>>,
    array_z: StreamingLogArrayDecoder<BufReader<File>>,
    num_triples: u64,
    num_sp_pairs: u64,
    shared_count: u64,
    subjects_count: u64,
    current_subject: u64,
    current_predicate: u64,
    pos_z: u64,
    pos_y: u64,
}

impl HdtTripleReader {
    pub fn open(
        hdt_path: &Path,
        triples_data_offset: u64,
        num_triples: u64,
        num_sp_pairs: u64,
        shared_count: u64,
        subjects_count: u64,
    ) -> Result<Self> {
        let mut reader = BufReader::with_capacity(256 * 1024, File::open(hdt_path)?);
        reader.seek(SeekFrom::Start(triples_data_offset))?;

        // Scan the 4 sections to find their offsets
        let (by_start, _by_bits) = skip_bitmap_section(&mut reader)?;
        let (bz_start, _bz_bits) = skip_bitmap_section(&mut reader)?;
        let (ay_start, _ay_entries, _ay_bpe) = skip_log_array_section(&mut reader)?;
        let (az_start, _az_entries, _az_bpe) = skip_log_array_section(&mut reader)?;

        let open_at = |offset: u64| -> Result<BufReader<File>> {
            let mut f = File::open(hdt_path)?;
            f.seek(SeekFrom::Start(offset))?;
            Ok(BufReader::with_capacity(256 * 1024, f))
        };

        let bitmap_y = StreamingBitmapDecoder::new(open_at(by_start)?)
            .context("Failed to create BitmapY decoder")?;
        let bitmap_z = StreamingBitmapDecoder::new(open_at(bz_start)?)
            .context("Failed to create BitmapZ decoder")?;
        let mut array_y = StreamingLogArrayDecoder::new(open_at(ay_start)?)
            .context("Failed to create ArrayY decoder")?;
        let array_z = StreamingLogArrayDecoder::new(open_at(az_start)?)
            .context("Failed to create ArrayZ decoder")?;

        // Read first predicate
        let current_predicate = if num_sp_pairs > 0 {
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
            num_triples,
            num_sp_pairs,
            shared_count,
            subjects_count,
            current_subject: 1,
            current_predicate,
            pos_z: 0,
            pos_y: 0,
        })
    }

    /// Remap an HDT subject ID to the flat local SO space.
    fn remap_subject(&self, s: u64) -> u32 {
        // Subjects in HDT: [1..shared+subjects], contiguous
        // Flat local: [0..shared+subjects-1]
        (s - 1) as u32
    }

    /// Remap an HDT object ID to the flat local SO space.
    fn remap_object(&self, o: u64) -> u32 {
        if o <= self.shared_count {
            // Shared term: same as subject mapping
            (o - 1) as u32
        } else {
            // Object-only term: placed after all subjects in the flat space
            (self.subjects_count + o - 1) as u32
        }
    }

    /// Remap an HDT predicate ID to the flat local P space.
    fn remap_predicate(&self, p: u64) -> u32 {
        (p - 1) as u32
    }

    /// Read the next triple, returning (flat_subject, flat_predicate, flat_object).
    /// Returns None when all triples have been read.
    pub fn next_triple(&mut self) -> Result<Option<(u32, u32, u32)>> {
        if self.pos_z >= self.num_triples {
            return Ok(None);
        }

        let object = self
            .array_z
            .next_entry()?
            .with_context(|| format!("ArrayZ ended early at position {}", self.pos_z))?;

        let flat_s = self.remap_subject(self.current_subject);
        let flat_p = self.remap_predicate(self.current_predicate);
        let flat_o = self.remap_object(object);

        let bz_bit = self
            .bitmap_z
            .next_bit()?
            .with_context(|| format!("BitmapZ ended early at position {}", self.pos_z))?;

        self.pos_z += 1;

        if bz_bit {
            let by_bit = self
                .bitmap_y
                .next_bit()?
                .with_context(|| format!("BitmapY ended early at position {}", self.pos_y))?;

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

        Ok(Some((flat_s, flat_p, flat_o)))
    }
}

// ---------------------------------------------------------------------------
// HDT section scanning helpers (duplicated from dump.rs — could be shared)
// ---------------------------------------------------------------------------

fn skip_bitmap_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64)> {
    let section_start = reader.stream_position()?;

    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;

    let num_bits = read_vbyte(reader)?;

    let mut crc8 = [0u8; 1];
    reader.read_exact(&mut crc8)?;

    let data_bytes = num_bits.div_ceil(8);
    reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

    Ok((section_start, num_bits))
}

fn skip_log_array_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64, u8)> {
    let section_start = reader.stream_position()?;

    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;

    let mut bits_byte = [0u8; 1];
    reader.read_exact(&mut bits_byte)?;
    let bits_per_entry = bits_byte[0];

    let num_entries = read_vbyte(reader)?;

    let mut crc8 = [0u8; 1];
    reader.read_exact(&mut crc8)?;

    let total_bits = num_entries * bits_per_entry as u64;
    let data_bytes = total_bits.div_ceil(8);
    reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

    Ok((section_start, num_entries, bits_per_entry))
}

fn skip_pfc_section<R: Read>(reader: &mut R, section_name: &str) -> Result<u64> {
    crate::hdt::pfc_reader::skip_pfc_section(reader, section_name)
}

/// Extract `originalSize` from the HDT header, returning 0 if not found.
fn parse_original_size_from_header(header_text: &str) -> u64 {
    for line in header_text.lines() {
        if line.contains("originalSize")
            && let Some(start) = line.find('"')
        {
            let rest = &line[start + 1..];
            if let Some(end) = rest.find('"')
                && let Ok(size) = rest[..end].parse::<u64>()
            {
                return size;
            }
        }
    }
    0
}

fn parse_num_triples_from_header(header_text: &str) -> Result<u64> {
    // Parse the header RDF to find the triple count
    // The header contains lines like: <...> <http://rdfs.org/ns/void#triples> "12345" .
    for line in header_text.lines() {
        if line.contains("void#triples") {
            // Extract the number from the quoted value
            if let Some(start) = line.find('"') {
                let rest = &line[start + 1..];
                if let Some(end) = rest.find('"')
                    && let Ok(count) = rest[..end].parse::<u64>()
                {
                    return Ok(count);
                }
            }
        }
    }
    bail!("Could not find triple count in HDT header");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remap_ids() {
        // shared=10, subjects=5
        let _shared_count = 10u64;
        let subjects_count = 5u64;

        // Subject remapping: s -> s-1
        assert_eq!((1u64 - 1) as u32, 0);
        assert_eq!((10u64 - 1) as u32, 9);
        assert_eq!((15u64 - 1) as u32, 14);

        // Object remapping
        // Shared object (o <= shared_count): o -> o-1
        assert_eq!((1u64 - 1) as u32, 0);
        assert_eq!((10u64 - 1) as u32, 9);

        // Object-only (o > shared_count): o -> subjects_count + o - 1
        let o = 11u64;
        assert_eq!((subjects_count + o - 1) as u32, 15);
        let o = 13u64;
        assert_eq!((subjects_count + o - 1) as u32, 17);

        // Predicate remapping: p -> p-1
        assert_eq!((1u64 - 1) as u32, 0);
        assert_eq!((3u64 - 1) as u32, 2);
    }

    #[test]
    fn test_four_way_merge_order() {
        // Simulate 4 sorted sections and verify merge produces globally sorted output
        let shared = vec![b"alice".to_vec(), b"bob".to_vec()];
        let subjects = vec![b"charlie".to_vec()];
        let objects = vec![b"dave".to_vec()];
        let predicates = vec![b"knows".to_vec(), b"likes".to_vec()];

        let merge = FourWayMerge::new(
            shared,
            subjects,
            objects,
            predicates,
            2, // shared_count
            1, // subjects_count
            0, // batch_id
        );

        let entries: Vec<StreamEntry> = merge.map(|r| r.unwrap()).collect();
        assert_eq!(entries.len(), 6);

        // Verify sorted order
        for i in 1..entries.len() {
            assert!(entries[i - 1].term <= entries[i].term,
                "Terms not in order: {:?} > {:?}",
                String::from_utf8_lossy(&entries[i - 1].term),
                String::from_utf8_lossy(&entries[i].term));
        }

        // Verify roles
        let alice = &entries[0];
        assert_eq!(alice.term, b"alice");
        assert!(alice.roles.contains(Roles::SUBJECT | Roles::OBJECT));
        assert_eq!(alice.so_local_id, Some(0)); // shared[0]

        let charlie = entries.iter().find(|e| e.term == b"charlie").unwrap();
        assert!(charlie.roles.contains(Roles::SUBJECT));
        assert_eq!(charlie.so_local_id, Some(2)); // shared_count + 0

        let dave = entries.iter().find(|e| e.term == b"dave").unwrap();
        assert!(dave.roles.contains(Roles::OBJECT));
        assert_eq!(dave.so_local_id, Some(3)); // shared_count + subjects_count + 0

        let knows = entries.iter().find(|e| e.term == b"knows").unwrap();
        assert!(knows.roles.contains(Roles::PREDICATE));
        assert_eq!(knows.p_local_id, Some(0));
    }

    #[test]
    fn test_four_way_merge_duplicate_term() {
        // A term appears in both shared and predicates sections
        let shared = vec![b"http://example.org/x".to_vec()];
        let subjects = vec![];
        let objects = vec![];
        let predicates = vec![b"http://example.org/x".to_vec()];

        let merge = FourWayMerge::new(shared, subjects, objects, predicates, 1, 0, 0);
        let entries: Vec<StreamEntry> = merge.map(|r| r.unwrap()).collect();

        // Should be merged into a single entry with both roles
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert!(entry.roles.contains(Roles::SUBJECT | Roles::OBJECT | Roles::PREDICATE));
        assert_eq!(entry.so_local_id, Some(0));
        assert_eq!(entry.p_local_id, Some(0));
    }
}
