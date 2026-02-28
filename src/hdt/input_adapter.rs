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
    /// File offsets where each PFC dictionary section starts.
    shared_section_offset: u64,
    subjects_section_offset: u64,
    predicates_section_offset: u64,
    objects_section_offset: u64,
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

        // Record each PFC section's file offset, then skip past it
        let shared_section_offset = reader.stream_position()?;
        let shared_count = skip_pfc_section(&mut reader, "shared")?;
        let subjects_section_offset = reader.stream_position()?;
        let subjects_count = skip_pfc_section(&mut reader, "subjects")?;
        let predicates_section_offset = reader.stream_position()?;
        let predicates_count = skip_pfc_section(&mut reader, "predicates")?;
        let objects_section_offset = reader.stream_position()?;
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
            shared_section_offset,
            subjects_section_offset,
            predicates_section_offset,
            objects_section_offset,
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
    /// The stream is a streaming 4-way merge of PFC sections (shared, subjects,
    /// objects, predicates) yielding `StreamEntry` items in lexicographic order.
    /// Only O(block_size) memory per section — no full materialization.
    ///
    /// When `file_index` is `Some(i)`, blank node terms are disambiguated by
    /// prefixing them with `f{i}_` (e.g. `_:b1` → `_:f2_b1`), matching the
    /// parser's per-file blank node disambiguation. This prefix is applied
    /// inline and preserves sort order because it inserts a common string
    /// after the `_:` marker shared by all blank nodes.
    pub fn vocab_factory(
        &self,
        batch_id: usize,
        file_index: Option<usize>,
    ) -> crate::pipeline::vocab_merger::VocabFactory
    {
        let path = self.path.clone();
        let shared_section_offset = self.shared_section_offset;
        let subjects_section_offset = self.subjects_section_offset;
        let predicates_section_offset = self.predicates_section_offset;
        let objects_section_offset = self.objects_section_offset;
        let shared_count = self.shared_count;
        let subjects_count = self.subjects_count;

        Box::new(move || {
            let bnode_prefix = file_index.map(|idx| format!("f{idx}_"));

            let iter = StreamingFourWayMerge::open(
                &path,
                shared_section_offset,
                subjects_section_offset,
                predicates_section_offset,
                objects_section_offset,
                shared_count,
                subjects_count,
                batch_id,
                bnode_prefix,
            )?;

            Ok(Box::new(iter) as Box<dyn Iterator<Item = Result<StreamEntry>> + Send>)
        })
    }

}

// ---------------------------------------------------------------------------
// Streaming 4-way merge iterator
// ---------------------------------------------------------------------------

/// Entry in the merge heap, tagged with which section it came from.
struct HeapEntry {
    term: Vec<u8>,
    section: SectionKind,
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

/// Streaming 4-way merge of PFC dictionary sections.
///
/// Holds 4 `PfcSectionIterator`s (each with its own file handle) and a
/// 4-entry BinaryHeap. Memory is O(block_size) per section — no full
/// materialization of dictionary terms.
struct StreamingFourWayMerge {
    shared_iter: PfcSectionIterator<BufReader<File>>,
    subjects_iter: PfcSectionIterator<BufReader<File>>,
    objects_iter: PfcSectionIterator<BufReader<File>>,
    predicates_iter: PfcSectionIterator<BufReader<File>>,
    heap: BinaryHeap<HeapEntry>,
    shared_count: u64,
    subjects_count: u64,
    batch_id: usize,
    bnode_prefix: Option<String>,
    // Per-section counters (0-based index of next term to yield)
    shared_index: u64,
    subjects_index: u64,
    objects_index: u64,
    predicates_index: u64,
}

impl StreamingFourWayMerge {
    #[allow(clippy::too_many_arguments)]
    fn open(
        path: &Path,
        shared_offset: u64,
        subjects_offset: u64,
        predicates_offset: u64,
        objects_offset: u64,
        shared_count: u64,
        subjects_count: u64,
        batch_id: usize,
        bnode_prefix: Option<String>,
    ) -> Result<Self> {
        let open_section = |offset: u64, name: &str| -> Result<PfcSectionIterator<BufReader<File>>> {
            let mut f = File::open(path)?;
            f.seek(SeekFrom::Start(offset))?;
            let mut reader = BufReader::with_capacity(256 * 1024, f);
            let header = PfcSectionHeader::read_from(&mut reader, name)?;
            Ok(PfcSectionIterator::new(reader, &header, name))
        };

        let mut shared_iter = open_section(shared_offset, "shared")?;
        let mut subjects_iter = open_section(subjects_offset, "subjects")?;
        let mut predicates_iter = open_section(predicates_offset, "predicates")?;
        let mut objects_iter = open_section(objects_offset, "objects")?;

        let mut heap = BinaryHeap::with_capacity(4);

        // Seed the heap with the first term from each non-empty section
        if let Some(term) = Self::next_from(&mut shared_iter)? {
            heap.push(HeapEntry { term, section: SectionKind::Shared });
        }
        if let Some(term) = Self::next_from(&mut subjects_iter)? {
            heap.push(HeapEntry { term, section: SectionKind::Subjects });
        }
        if let Some(term) = Self::next_from(&mut predicates_iter)? {
            heap.push(HeapEntry { term, section: SectionKind::Predicates });
        }
        if let Some(term) = Self::next_from(&mut objects_iter)? {
            heap.push(HeapEntry { term, section: SectionKind::Objects });
        }

        Ok(Self {
            shared_iter,
            subjects_iter,
            objects_iter,
            predicates_iter,
            heap,
            shared_count,
            subjects_count,
            batch_id,
            bnode_prefix,
            shared_index: 0,
            subjects_index: 0,
            objects_index: 0,
            predicates_index: 0,
        })
    }

    /// Pull the next term from a PFC iterator, transposing the Result.
    fn next_from(iter: &mut PfcSectionIterator<BufReader<File>>) -> Result<Option<Vec<u8>>> {
        match iter.next() {
            Some(Ok(term)) => Ok(Some(term)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Get the iterator for a section and advance it, pushing the next term onto the heap.
    fn advance_section(&mut self, section: SectionKind) -> Result<()> {
        let iter = match section {
            SectionKind::Shared => &mut self.shared_iter,
            SectionKind::Subjects => &mut self.subjects_iter,
            SectionKind::Objects => &mut self.objects_iter,
            SectionKind::Predicates => &mut self.predicates_iter,
        };
        if let Some(term) = Self::next_from(iter)? {
            self.heap.push(HeapEntry { term, section });
        }
        Ok(())
    }

    /// Consume the current index for a section and return it, then increment.
    fn take_index(&mut self, section: SectionKind) -> u64 {
        let counter = match section {
            SectionKind::Shared => &mut self.shared_index,
            SectionKind::Subjects => &mut self.subjects_index,
            SectionKind::Objects => &mut self.objects_index,
            SectionKind::Predicates => &mut self.predicates_index,
        };
        let idx = *counter;
        *counter += 1;
        idx
    }

    /// Apply blank node prefix if needed. Preserves sort order because the
    /// prefix is inserted after the common `_:` marker.
    fn disambiguate(&self, mut term: Vec<u8>) -> Vec<u8> {
        if let Some(ref prefix) = self.bnode_prefix
            && term.starts_with(b"_:")
        {
            let mut new_term = Vec::with_capacity(2 + prefix.len() + term.len() - 2);
            new_term.extend_from_slice(b"_:");
            new_term.extend_from_slice(prefix.as_bytes());
            new_term.extend_from_slice(&term[2..]);
            term = new_term;
        }
        term
    }

    fn make_entry(&self, term: Vec<u8>, section: SectionKind, index: u64) -> StreamEntry {
        match section {
            SectionKind::Shared => StreamEntry {
                term,
                roles: Roles::SUBJECT | Roles::OBJECT,
                so_local_id: Some(index as u32),
                p_local_id: None,
                source_batch: self.batch_id,
            },
            SectionKind::Subjects => StreamEntry {
                term,
                roles: Roles::SUBJECT,
                so_local_id: Some((self.shared_count + index) as u32),
                p_local_id: None,
                source_batch: self.batch_id,
            },
            SectionKind::Objects => StreamEntry {
                term,
                roles: Roles::OBJECT,
                so_local_id: Some((self.shared_count + self.subjects_count + index) as u32),
                p_local_id: None,
                source_batch: self.batch_id,
            },
            SectionKind::Predicates => StreamEntry {
                term,
                roles: Roles::PREDICATE,
                so_local_id: None,
                p_local_id: Some(index as u32),
                source_batch: self.batch_id,
            },
        }
    }
}

impl Iterator for StreamingFourWayMerge {
    type Item = Result<StreamEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.heap.pop()?;
        let section = entry.section;
        let term = entry.term;

        let index = self.take_index(section);
        let disambiguated = self.disambiguate(term.clone());
        let mut merged = self.make_entry(disambiguated, section, index);

        // Consume entries with the same term from other sections (role merging)
        while let Some(peek) = self.heap.peek() {
            if peek.term != term {
                break;
            }
            let dup = self.heap.pop().unwrap();
            let dup_index = self.take_index(dup.section);
            let dup_term = self.disambiguate(dup.term);
            let dup_entry = self.make_entry(dup_term, dup.section, dup_index);
            merged.roles |= dup_entry.roles;
            if merged.so_local_id.is_none() {
                merged.so_local_id = dup_entry.so_local_id;
            }
            if merged.p_local_id.is_none() {
                merged.p_local_id = dup_entry.p_local_id;
            }
            if let Err(e) = self.advance_section(dup.section) {
                return Some(Err(e));
            }
        }

        // Advance the section we just popped from
        if let Err(e) = self.advance_section(section) {
            return Some(Err(e));
        }

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

    /// Verify CRC32C checksums on all four BitmapTriples sections.
    /// Must be called after all triples have been consumed.
    pub fn finalize(self) -> Result<()> {
        self.bitmap_y
            .finish()
            .context("BitmapY CRC32C verification failed")?;
        self.bitmap_z
            .finish()
            .context("BitmapZ CRC32C verification failed")?;
        self.array_y
            .finish()
            .context("ArrayY CRC32C verification failed")?;
        self.array_z
            .finish()
            .context("ArrayZ CRC32C verification failed")?;
        Ok(())
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

    // Streaming 4-way merge is tested end-to-end via the integration test
    // `test_hdt_input_merged_with_ntriples` which exercises vocab_factory on
    // real HDT files, verifying sorted order, role assignment, and blank node
    // disambiguation through the full pipeline.
}
