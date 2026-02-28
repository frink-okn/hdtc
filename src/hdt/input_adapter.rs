//! HDT input adapter — makes an existing HDT file look like a "batch" to the pipeline.
//!
//! Provides two capabilities:
//! 1. A sorted vocabulary stream (for the k-way merge in Stage 4)
//! 2. A streaming triple reader (for the ID remapper in Stage 5)

use crate::hdt::pfc_reader::{PfcSectionHeader, PfcSectionIterator};
use crate::io::{ControlInfo, ControlType, StreamingBitmapDecoder, StreamingLogArrayDecoder, skip_bitmap_section, skip_log_array_section};
use crate::pipeline::batch_vocab::Roles;
use crate::pipeline::vocab_merger::StreamEntry;
use anyhow::{Context, Result, bail};
use oxrdfio::{RdfFormat, RdfParser};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
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
        let (num_triples, original_size) = parse_header_metadata(&header_text)?;

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
            let _dict_ci = ControlInfo::read_from(&mut reader)?;

            // Read each PFC section sequentially into memory for the 4-way merge
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

/// Remap an HDT subject ID (1-based) to the flat local SO space (0-based).
fn remap_subject(s: u64) -> u32 {
    (s - 1) as u32
}

/// Remap an HDT object ID to the flat local SO space.
///
/// Shared objects (ID <= shared_count) map to [0..shared_count-1].
/// Object-only terms (ID > shared_count) are placed after all subjects.
fn remap_object(o: u64, shared_count: u64, subjects_count: u64) -> u32 {
    if o <= shared_count {
        (o - 1) as u32
    } else {
        (subjects_count + o - 1) as u32
    }
}

/// Remap an HDT predicate ID (1-based) to the flat local P space (0-based).
fn remap_predicate(p: u64) -> u32 {
    (p - 1) as u32
}

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

    fn remap_subject(&self, s: u64) -> u32 {
        remap_subject(s)
    }

    fn remap_object(&self, o: u64) -> u32 {
        remap_object(o, self.shared_count, self.subjects_count)
    }

    fn remap_predicate(&self, p: u64) -> u32 {
        remap_predicate(p)
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

fn skip_pfc_section<R: Read + Seek>(reader: &mut R, section_name: &str) -> Result<u64> {
    crate::hdt::pfc_reader::skip_pfc_section(reader, section_name)
}

/// Parse header metadata from the HDT header RDF.
///
/// Returns `(num_triples, original_size)`. The triple count is required;
/// original size defaults to 0 if not present.
fn parse_header_metadata(header_text: &str) -> Result<(u64, u64)> {
    const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";
    const HDT_TRIPLES_NUM: &str = "http://purl.org/HDT/hdt#triplesnumTriples";
    const ORIGINAL_SIZE: &str = "http://purl.org/HDT/hdt#originalSize";

    let mut triples_from_void: Option<u64> = None;
    let mut triples_from_hdt: Option<u64> = None;
    let mut original_size: u64 = 0;

    let parser =
        RdfParser::from_format(RdfFormat::NTriples).for_reader(Cursor::new(header_text.as_bytes()));

    for quad_result in parser {
        let quad = quad_result.context("Invalid N-Triples in HDT header metadata")?;
        let predicate = quad.predicate.as_str();

        let oxrdf::Term::Literal(literal) = quad.object else {
            continue;
        };

        if predicate == VOID_TRIPLES {
            triples_from_void = Some(literal.value().parse::<u64>().with_context(|| {
                format!("Invalid numeric triple-count literal: {}", literal.value())
            })?);
        } else if predicate == HDT_TRIPLES_NUM {
            triples_from_hdt = Some(literal.value().parse::<u64>().with_context(|| {
                format!("Invalid numeric triple-count literal: {}", literal.value())
            })?);
        } else if predicate == ORIGINAL_SIZE
            && let Ok(size) = literal.value().parse::<u64>()
        {
            original_size = size;
        }
    }

    let num_triples = match (triples_from_void, triples_from_hdt) {
        (Some(v), Some(h)) if v != h => {
            bail!(
                "Header triple-count mismatch between void:triples ({v}) and hdt:triplesnumTriples ({h})"
            )
        }
        (Some(v), Some(_)) => v,
        (Some(v), None) => v,
        (None, Some(h)) => h,
        (None, None) => bail!("Header metadata missing triple-count predicate"),
    };

    Ok((num_triples, original_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remap_ids() {
        let shared_count = 10u64;
        let subjects_count = 5u64;

        // Subject remapping: 1-based HDT ID → 0-based flat ID
        assert_eq!(remap_subject(1), 0);
        assert_eq!(remap_subject(10), 9);
        assert_eq!(remap_subject(15), 14);

        // Object remapping: shared objects (o <= shared_count) → o-1
        assert_eq!(remap_object(1, shared_count, subjects_count), 0);
        assert_eq!(remap_object(10, shared_count, subjects_count), 9);

        // Object-only (o > shared_count) → subjects_count + o - 1
        assert_eq!(remap_object(11, shared_count, subjects_count), 15);
        assert_eq!(remap_object(13, shared_count, subjects_count), 17);

        // Predicate remapping: 1-based → 0-based
        assert_eq!(remap_predicate(1), 0);
        assert_eq!(remap_predicate(3), 2);
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
