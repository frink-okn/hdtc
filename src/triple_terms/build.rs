//! Finishing a build that met triple terms.
//!
//! The parser records every triple term it sees ([`super::spool`]) but cannot
//! tell which are asserted, so the work happens here, once `data.hdt` and its
//! permutation index exist:
//!
//! 1. **Deduplicate.** The records are sorted by IRI. Equal IRIs are equal
//!    terms by construction, so a sorted, deduplicated stream is the set of
//!    distinct triple terms, and a record's position in it — its *rank* — is a
//!    stable key for everything that follows.
//! 2. **Classify.** Each proposition's terms are located in `data.hdt`'s
//!    dictionary and the located triples are merge-joined with `data.hdt`'s SPO
//!    stream. A triple found there is *asserted*, and is known from then on only
//!    by its identifiers and position.
//! 3. **Store the rest.** The unasserted propositions become the triples of a
//!    second, standard HDT, built by the ordinary pipeline, with its own
//!    permutation index.
//! 4. **Index.** The deduplicated stream is walked once more beside the runs of
//!    triple-term IRIs in both dictionaries, writing each IRI's proposition and
//!    the layer entries that mark reified triples in SPO, POS and OPS order;
//!    the POS and OPS positions come from merge-joins with the permutation
//!    indexes' own streams.
//!
//! Every pass is a sequential read or an external sort, so memory is bounded by
//! the budget and not by the number of triple terms.

use super::PropositionRecord;
use super::format::{self, DraftSection, SourceFacts, component, kind, section_type};
use super::iri::TT_NAMESPACE;
use super::spool::{PropositionSpool, SpoolWriter};
use crate::cli::InputSidecarPolicy;
use crate::hdt::artifacts::SourceIdentity;
use crate::hdt::reader::{
    BitmapTriplesScanner, DictionaryResolver, open_hdt, write_nt_object, write_nt_subject,
};
use crate::io::{BitPacker, StreamingLogArrayEncoder};
use crate::permutation::format::{
    SUBBLOCK_BITS, SUPERBLOCK_BITS, id_width, sub_count, super_count,
};
use crate::permutation::{
    PermutationComponent, PermutationIndex, PositionMaps, RawBitmap, build_rank_directories,
    scan_hdt,
};
use crate::quads::GraphAssignments;
use crate::rdf::RdfInput;
use crate::rdf::input::{Compression, RdfFormat};
use crate::sort::{ExternalSorter, Sortable};
use anyhow::{Context, Result, bail, ensure};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tempfile::{NamedTempFile, TempPath};

// ---------------------------------------------------------------------------
// Sortable rows
// ---------------------------------------------------------------------------

/// A fixed-width tuple of identifiers, ordered lexicographically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Row<const N: usize>(pub [u64; N]);

/// Fill `buf` from `reader`, or report a clean end of input before any byte.
fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        let read = reader.read(&mut buf[filled..])?;
        if read == 0 {
            ensure!(filled == 0, "truncated sort record");
            return Ok(false);
        }
        filled += read;
    }
    Ok(true)
}

impl<const N: usize> Sortable for Row<N> {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        for value in self.0 {
            writer.write_all(&value.to_le_bytes())?;
        }
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut out = [0u64; N];
        let mut buf = [0u8; 8];
        for (i, slot) in out.iter_mut().enumerate() {
            if i == 0 {
                if !read_exact_or_eof(reader, &mut buf)? {
                    return Ok(None);
                }
            } else {
                reader.read_exact(&mut buf)?;
            }
            *slot = u64::from_le_bytes(buf);
        }
        Ok(Some(Self(out)))
    }

    fn mem_size(&self) -> usize {
        N * 8
    }
}

fn write_field<W: Write>(writer: &mut W, field: &str) -> Result<()> {
    writer.write_all(&(field.len() as u32).to_le_bytes())?;
    writer.write_all(field.as_bytes())?;
    Ok(())
}

fn read_field<R: Read>(reader: &mut R, first: bool) -> Result<Option<String>> {
    let mut len = [0u8; 4];
    if first {
        if !read_exact_or_eof(reader, &mut len)? {
            return Ok(None);
        }
    } else {
        reader.read_exact(&mut len)?;
    }
    let mut bytes = vec![0u8; u32::from_le_bytes(len) as usize];
    reader.read_exact(&mut bytes)?;
    Ok(Some(String::from_utf8(bytes)?))
}

impl Sortable for PropositionRecord {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_field(writer, &self.iri)?;
        write_field(writer, &self.subject)?;
        write_field(writer, &self.predicate)?;
        write_field(writer, &self.object)
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let Some(iri) = read_field(reader, true)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            iri,
            subject: read_field(reader, false)?.context("truncated record")?,
            predicate: read_field(reader, false)?.context("truncated record")?,
            object: read_field(reader, false)?.context("truncated record")?,
        }))
    }

    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.iri.len()
            + self.subject.len()
            + self.predicate.len()
            + self.object.len()
    }
}

/// An external sort that owns its buffer, so a caller pushes and finishes.
struct Sorted<T: Sortable> {
    sorter: ExternalSorter,
    buffer: Vec<T>,
    used: usize,
}

impl<T: Sortable + PartialEq + 'static> Sorted<T> {
    fn new(temp_dir: &Path, budget: usize) -> Self {
        Self {
            sorter: ExternalSorter::new(temp_dir, budget.max(1 << 20)),
            buffer: Vec::new(),
            used: 0,
        }
    }

    fn push(&mut self, item: T) -> Result<()> {
        self.sorter.push(item, &mut self.buffer, &mut self.used)
    }

    /// The sorted, deduplicated items. The sorter is returned with them because
    /// it owns the chunk files the iterator reads, and removes them on drop.
    fn finish(mut self) -> Result<(ExternalSorter, crate::sort::external::MergeIterator<T>)> {
        let iter = self.sorter.finish(&mut self.buffer)?;
        Ok((self.sorter, iter))
    }
}

// ---------------------------------------------------------------------------
// Rank sets: sorted u64 streams on disk
// ---------------------------------------------------------------------------

struct RankFile {
    path: PathBuf,
}

impl RankFile {
    fn write(path: PathBuf, ranks: impl Iterator<Item = Result<u64>>) -> Result<(Self, u64)> {
        let mut encoder = zstd::Encoder::new(BufWriter::new(File::create(&path)?), 1)?;
        let mut count = 0u64;
        for rank in ranks {
            encoder.write_all(&rank?.to_le_bytes())?;
            count += 1;
        }
        encoder.finish()?.flush()?;
        Ok((Self { path }, count))
    }

    fn reader(&self) -> Result<impl Iterator<Item = Result<u64>> + use<>> {
        let mut decoder = zstd::Decoder::new(File::open(&self.path)?)?;
        Ok(std::iter::from_fn(move || {
            let mut buf = [0u8; 8];
            match read_exact_or_eof(&mut decoder, &mut buf) {
                Ok(true) => Some(Ok(u64::from_le_bytes(buf))),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            }
        }))
    }
}

impl Drop for RankFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

// ---------------------------------------------------------------------------
// Payload writers
// ---------------------------------------------------------------------------

fn scratch(temp_dir: &Path, prefix: &str) -> Result<NamedTempFile> {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempfile_in(temp_dir)
        .with_context(|| format!("Failed to create scratch file in {}", temp_dir.display()))
}

/// A bare packed array written in order.
struct PackedWriter {
    file: NamedTempFile,
    encoder: StreamingLogArrayEncoder<BufWriter<File>>,
}

impl PackedWriter {
    fn new(temp_dir: &Path, width: u8) -> Result<Self> {
        let file = scratch(temp_dir, ".hdtc-tt-array-")?;
        let encoder = StreamingLogArrayEncoder::new(
            width,
            BufWriter::with_capacity(256 * 1024, file.reopen()?),
        );
        Ok(Self { file, encoder })
    }

    fn push(&mut self, value: u64) -> Result<()> {
        Ok(self.encoder.push(value)?)
    }

    fn finish(self, section_type: u32) -> Result<DraftSection> {
        let (entries, width, mut writer) = self.encoder.finish()?;
        writer.flush()?;
        drop(writer);
        Ok(DraftSection {
            section_type,
            payload: self.file.into_temp_path(),
            entry_count: entries,
            bits_per_entry: width,
            parameter: 0,
            indexed_bits: 0,
        })
    }
}

/// A layer: a bitmap over one HDT's triples in one order, the rank directory
/// over it, and the reverse array giving each set bit's triple-term IRI.
struct LayerWriter {
    component: u32,
    bitmap_file: NamedTempFile,
    bitmap: BitPacker<BufWriter<File>>,
    bits_written: u64,
    total_bits: u64,
    reverse: PackedWriter,
}

impl LayerWriter {
    fn new(temp_dir: &Path, component: u32, total_bits: u64, reverse_width: u8) -> Result<Self> {
        let bitmap_file = scratch(temp_dir, ".hdtc-tt-layer-")?;
        let bitmap = BitPacker::new(BufWriter::with_capacity(256 * 1024, bitmap_file.reopen()?));
        Ok(Self {
            component,
            bitmap_file,
            bitmap,
            bits_written: 0,
            total_bits,
            reverse: PackedWriter::new(temp_dir, reverse_width)?,
        })
    }

    /// Mark position `at` (strictly increasing across calls) and record the
    /// `data.hdt` object identifier of its triple-term IRI.
    fn set(&mut self, at: u64, data_term: u64) -> Result<()> {
        ensure!(
            at >= self.bits_written && at < self.total_bits,
            "layer position {at} out of order or range"
        );
        self.bitmap.push_zeros(at - self.bits_written)?;
        self.bitmap.push_bits(1, 1)?;
        self.bits_written = at + 1;
        self.reverse.push(data_term)
    }

    fn finish(mut self, temp_dir: &Path) -> Result<Vec<DraftSection>> {
        self.bitmap
            .push_zeros(self.total_bits - self.bits_written)?;
        self.bitmap.finish()?;
        // The packer writes whole words; the format stores exactly ceil(L/8).
        let file = self.bitmap_file.reopen()?;
        file.set_len(self.total_bits.div_ceil(8))?;
        file.sync_data()?;
        let bitmap_path = self.bitmap_file.into_temp_path();
        let (superrank, subrank) = build_rank_directories(
            &RawBitmap {
                path: bitmap_path.to_path_buf(),
                data_offset: 0,
                bits: self.total_bits,
            },
            temp_dir,
        )?;
        let n = self.total_bits;
        Ok(vec![
            DraftSection {
                section_type: section_type(self.component, kind::LAYER),
                payload: bitmap_path,
                entry_count: n,
                bits_per_entry: 1,
                parameter: 0,
                indexed_bits: 0,
            },
            DraftSection {
                section_type: section_type(self.component, kind::LAYER_SUPERRANK),
                payload: superrank,
                entry_count: super_count(n),
                bits_per_entry: 64,
                parameter: u64::from(SUPERBLOCK_BITS),
                indexed_bits: n,
            },
            DraftSection {
                section_type: section_type(self.component, kind::LAYER_SUBRANK),
                payload: subrank,
                entry_count: sub_count(n),
                bits_per_entry: 16,
                parameter: u64::from(SUBBLOCK_BITS),
                indexed_bits: n,
            },
            self.reverse
                .finish(section_type(self.component, kind::REVERSE))?,
        ])
    }
}

// ---------------------------------------------------------------------------
// The finishing pass
// ---------------------------------------------------------------------------

/// Where the finishing pass reads and writes.
pub(crate) struct FinishContext<'a> {
    /// The staged `data.hdt`.
    pub hdt: &'a Path,
    /// Its staged permutation index.
    pub perm: &'a Path,
    /// Directory for the staged outputs; the same file system as their final
    /// names, so publishing them is a rename.
    pub output_dir: &'a Path,
    pub temp_dir: &'a Path,
    pub memory_budget: usize,
    pub dataset_uri: &'a str,
}

/// The staged triple-term artifacts.
pub(crate) struct BuiltTripleTerms {
    pub index: TempPath,
    /// The propositions HDT and its permutation index, when any triple term
    /// is unasserted.
    pub propositions: Option<(TempPath, TempPath)>,
    pub distinct: u64,
    pub asserted: u64,
    pub unasserted: u64,
}

/// One HDT's identifier-space sizes and its run of triple-term IRIs.
#[derive(Debug, Clone, Copy, Default)]
struct HdtShape {
    triples: u64,
    subjects: u64,
    predicates: u64,
    objects: u64,
    source_length: u64,
    /// Global object identifiers `[run_start, run_end)`; empty when equal.
    run_start: u64,
    run_end: u64,
}

fn shape_of(hdt: &Path, dictionary: &mut DictionaryResolver) -> Result<HdtShape> {
    let metadata = scan_hdt(hdt)?;
    let shared = dictionary.shared.prefix_range(TT_NAMESPACE.as_bytes())?;
    ensure!(
        shared.is_empty(),
        "{} has a triple-term IRI in subject position",
        hdt.display()
    );
    let objects = dictionary.objects.prefix_range(TT_NAMESPACE.as_bytes())?;
    let offset = dictionary.shared.string_count;
    Ok(HdtShape {
        triples: metadata.triples,
        subjects: metadata.subjects,
        predicates: metadata.predicates,
        objects: metadata.objects,
        source_length: metadata.file_length - metadata.data_offset,
        run_start: offset + objects.start,
        run_end: offset + objects.end,
    })
}

/// A dictionary term with the rank of the record that needs its identifier,
/// sorted by term so a whole dictionary role can be joined in one pass.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct KeyedRank {
    key: Vec<u8>,
    rank: u64,
}

impl Sortable for KeyedRank {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&(self.key.len() as u32).to_le_bytes())?;
        writer.write_all(&self.key)?;
        writer.write_all(&self.rank.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut len = [0u8; 4];
        if !read_exact_or_eof(reader, &mut len)? {
            return Ok(None);
        }
        let mut key = vec![0u8; u32::from_le_bytes(len) as usize];
        reader.read_exact(&mut key)?;
        let mut rank = [0u8; 8];
        reader.read_exact(&mut rank)?;
        Ok(Some(Self {
            key,
            rank: u64::from_le_bytes(rank),
        }))
    }

    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.key.len()
    }
}

type PfcTerms = crate::hdt::pfc_reader::PfcSectionIterator<std::io::BufReader<File>>;

fn section_terms(
    hdt: &Path,
    section: &crate::hdt::pfc_reader::PfcSection,
    name: &str,
) -> Result<std::iter::Peekable<PfcTerms>> {
    use std::io::{Seek, SeekFrom};
    let mut reader = std::io::BufReader::with_capacity(256 * 1024, File::open(hdt)?);
    reader.seek(SeekFrom::Start(section.section_start))?;
    let header = crate::hdt::pfc_reader::PfcSectionHeader::read_from(&mut reader, name)?;
    Ok(crate::hdt::pfc_reader::PfcSectionIterator::new(reader, header, name).peekable())
}

/// One role's terms in byte order with their global identifiers: the shared
/// section merged with the role's own section.
struct RoleTerms {
    shared: std::iter::Peekable<PfcTerms>,
    own: std::iter::Peekable<PfcTerms>,
    next_shared: u64,
    next_own: u64,
}

impl RoleTerms {
    fn open(hdt: &Path, own_section: Role) -> Result<Self> {
        let file = File::open(hdt)?;
        let mut reader = std::io::BufReader::new(file);
        let sections = crate::hdt::sections::scan_hdt_sections(&mut reader)?;
        let own = match own_section {
            Role::Subject => &sections.subjects,
            Role::Object => &sections.objects,
        };
        Ok(Self {
            shared: section_terms(hdt, &sections.shared, "shared")?,
            own: section_terms(hdt, own, "own")?,
            next_shared: 1,
            next_own: sections.shared.string_count + 1,
        })
    }
}

impl Iterator for RoleTerms {
    type Item = Result<(Vec<u8>, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let take_shared = match (self.shared.peek(), self.own.peek()) {
            (Some(Err(_)), _) => true,
            (_, Some(Err(_))) => false,
            (Some(Ok(a)), Some(Ok(b))) => a <= b,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return None,
        };
        if take_shared {
            let id = self.next_shared;
            self.next_shared += 1;
            Some(self.shared.next()?.map(|term| (term, id)))
        } else {
            let id = self.next_own;
            self.next_own += 1;
            Some(self.own.next()?.map(|term| (term, id)))
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Role {
    Subject,
    Object,
}

/// Join `keys` (sorted by term) with a role's dictionary, yielding the
/// identifier found for each rank, sorted by rank.
fn join_role(
    hdt: &Path,
    role: Role,
    keys: Sorted<KeyedRank>,
    temp_dir: &Path,
    budget: usize,
) -> Result<Sorted<Row<2>>> {
    let mut found = Sorted::<Row<2>>::new(temp_dir, budget);
    let (_sorter, keys) = keys.finish()?;
    let mut terms = RoleTerms::open(hdt, role)?.peekable();
    for key in keys {
        let key = key?;
        loop {
            match terms.peek() {
                Some(Ok((term, _))) if term.as_slice() < key.key.as_slice() => {
                    terms.next();
                }
                Some(Err(_)) => return Err(terms.next().unwrap().unwrap_err()),
                _ => break,
            }
        }
        if let Some(Ok((term, id))) = terms.peek()
            && *term == key.key
        {
            found.push(Row([key.rank, *id]))?;
        }
    }
    Ok(found)
}

/// Find every proposition in `ranks` (all of `distinct` when `None`) among an
/// HDT's triples, by joining its terms with the HDT's dictionary and the
/// located triples with its SPO stream. Every step is a sequential pass or an
/// external sort; nothing is looked up at random.
///
/// Returns the found rows `[rank, position, s, p, o]` and the ranks not found.
/// With `require_all`, a miss is an error.
fn place(
    hdt: &Path,
    distinct: &PropositionSpool,
    ranks: Option<&RankFile>,
    temp_dir: &Path,
    budget: usize,
    require_all: bool,
) -> Result<(Sorted<Row<5>>, Sorted<Row<1>>)> {
    let started = std::time::Instant::now();
    // Predicates are few: a map.
    let mut predicates = std::collections::HashMap::new();
    {
        let file = File::open(hdt)?;
        let sections = crate::hdt::sections::scan_hdt_sections(&mut std::io::BufReader::new(file))?;
        for (index, term) in section_terms(hdt, &sections.predicates, "predicates")?.enumerate() {
            predicates.insert(term?, index as u64 + 1);
        }
    }

    let mut subjects = Sorted::<KeyedRank>::new(temp_dir, budget / 3);
    let mut objects = Sorted::<KeyedRank>::new(temp_dir, budget / 3);
    let mut predicate_ids = Sorted::<Row<2>>::new(temp_dir, budget / 6);
    let mut wanted = match ranks {
        Some(file) => Some(file.reader()?.peekable()),
        None => None,
    };
    let mut considered = Sorted::<Row<1>>::new(temp_dir, budget / 6);
    for (rank, record) in distinct.reader()?.enumerate() {
        let record = record?;
        let rank = rank as u64;
        if let Some(wanted) = wanted.as_mut() {
            match wanted.peek() {
                Some(Ok(next)) if *next == rank => {
                    wanted.next();
                }
                Some(Err(_)) => return Err(wanted.next().unwrap().unwrap_err()),
                _ => continue,
            }
        }
        considered.push(Row([rank]))?;
        let Some(&predicate) = predicates.get(record.predicate.as_bytes()) else {
            continue;
        };
        predicate_ids.push(Row([rank, predicate]))?;
        subjects.push(KeyedRank {
            key: record.subject.into_bytes(),
            rank,
        })?;
        objects.push(KeyedRank {
            key: record.object.into_bytes(),
            rank,
        })?;
    }
    drop(predicates);
    let subject_ids = join_role(hdt, Role::Subject, subjects, temp_dir, budget / 6)?;
    let object_ids = join_role(hdt, Role::Object, objects, temp_dir, budget / 6)?;

    // Zip the three identifier streams by rank.
    let mut candidates = Sorted::<Row<4>>::new(temp_dir, budget / 3);
    let mut missing = Sorted::<Row<1>>::new(temp_dir, budget / 6);
    {
        let (_c, considered) = considered.finish()?;
        let (_p, predicate_ids) = predicate_ids.finish()?;
        let (_s, subject_ids) = subject_ids.finish()?;
        let (_o, object_ids) = object_ids.finish()?;
        let mut predicate_ids = predicate_ids.peekable();
        let mut subject_ids = subject_ids.peekable();
        let mut object_ids = object_ids.peekable();
        fn take(
            stream: &mut std::iter::Peekable<impl Iterator<Item = Result<Row<2>>>>,
            rank: u64,
        ) -> Result<Option<u64>> {
            match stream.peek() {
                Some(Ok(row)) if row.0[0] == rank => Ok(Some(stream.next().unwrap()?.0[1])),
                Some(Err(_)) => Err(stream.next().unwrap().unwrap_err()),
                _ => Ok(None),
            }
        }
        for rank in considered {
            let rank = rank?.0[0];
            let p = take(&mut predicate_ids, rank)?;
            let s = take(&mut subject_ids, rank)?;
            let o = take(&mut object_ids, rank)?;
            match (s, p, o) {
                (Some(s), Some(p), Some(o)) => candidates.push(Row([s, p, o, rank]))?,
                _ if require_all => {
                    bail!("a proposition is missing from {}", hdt.display())
                }
                _ => missing.push(Row([rank]))?,
            }
        }
    }

    let metadata = scan_hdt(hdt)?;
    let mut scanner = BitmapTriplesScanner::new(&metadata.offsets, hdt)?;
    let mut position = 0u64;
    let mut current = scanner.next_triple()?;
    let mut found = Sorted::<Row<5>>::new(temp_dir, budget / 3);
    let (_sorter, sorted) = candidates.finish()?;
    for row in sorted {
        let [s, p, o, rank] = row?.0;
        let key = (s, p, o);
        while current.is_some_and(|triple| triple < key) {
            current = scanner.next_triple()?;
            position += 1;
        }
        if current == Some(key) {
            found.push(Row([rank, position, s, p, o]))?;
        } else if require_all {
            bail!("a proposition is missing from {}", hdt.display());
        } else {
            missing.push(Row([rank]))?;
        }
    }
    tracing::debug!(
        "Placed propositions in {} ({:.1}s)",
        hdt.display(),
        started.elapsed().as_secs_f64()
    );
    Ok((found, missing))
}

/// Write the given propositions as N-Triples for the propositions HDT build.
fn write_propositions_ntriples(
    distinct: &PropositionSpool,
    ranks: &RankFile,
    path: &Path,
) -> Result<()> {
    let mut encoder =
        zstd::Encoder::new(BufWriter::with_capacity(1 << 20, File::create(path)?), 1)?;
    let mut wanted = ranks.reader()?.peekable();
    for (rank, record) in distinct.reader()?.enumerate() {
        let record = record?;
        match wanted.peek() {
            Some(Ok(next)) if *next == rank as u64 => {
                wanted.next();
            }
            Some(Err(_)) => return Err(wanted.next().unwrap().unwrap_err()),
            None => break,
            _ => continue,
        }
        write_nt_subject(&mut encoder, record.subject.as_bytes())?;
        encoder.write_all(b" <")?;
        encoder.write_all(record.predicate.as_bytes())?;
        encoder.write_all(b"> ")?;
        write_nt_object(&mut encoder, record.object.as_bytes())?;
        encoder.write_all(b" .\n")?;
    }
    encoder.finish()?.flush()?;
    Ok(())
}

/// Build the propositions HDT and its permutation index with the ordinary
/// pipeline, staging both in `output_dir`.
fn build_propositions_hdt(
    ctx: &FinishContext<'_>,
    ntriples: &Path,
) -> Result<(TempPath, TempPath)> {
    use crate::pipeline::{ParserParallelismConfig, run_pipeline};
    let input = RdfInput {
        path: ntriples.to_path_buf(),
        format: RdfFormat::NTriples,
        compression: Compression::Zstd,
    };
    let parallelism = ParserParallelismConfig {
        trust_triple_term_iris: true,
        ..ParserParallelismConfig::default()
    };
    // The pipeline names its scratch files by role, not uniquely, so a second
    // run beside the first must not share its directory.
    let scratch_dir = tempfile::Builder::new()
        .prefix("tt-propositions-")
        .tempdir_in(ctx.temp_dir)?;
    let mut result = run_pipeline(
        &[input],
        &[],
        scratch_dir.path(),
        ctx.memory_budget,
        false,
        InputSidecarPolicy::Drop,
        &GraphAssignments::parse(&[], None)?,
        ctx.dataset_uri,
        &parallelism,
        Some(PositionMaps::default()),
        false,
        false,
    )?;
    ensure!(
        result.propositions.is_none(),
        "the propositions of triple terms produced triple terms of their own"
    );
    let hdt = tempfile::Builder::new()
        .prefix(".hdtc-props-")
        .tempfile_in(ctx.output_dir)?
        .into_temp_path();
    crate::hdt::write_hdt_streaming(
        &hdt,
        ctx.dataset_uri,
        &result.counts,
        &result.dict_section_paths,
        &result.dict_section_sizes,
        &result.bitmap_triples,
        result.ntriples_size,
    )?;
    let perm = tempfile::Builder::new()
        .prefix(".hdtc-props-perm-")
        .tempfile_in(ctx.output_dir)?
        .into_temp_path();
    crate::permutation::finish_prepared_index(
        result
            .permutation
            .take()
            .context("propositions permutation collection was not prepared")?,
        &perm,
        &hdt,
        &result.counts,
        &result.bitmap_triples,
    )?;
    for path in &result.dict_section_paths {
        let _ = std::fs::remove_file(path);
    }
    let _ = std::fs::remove_file(&result.graph_section_path);
    result.bitmap_triples.cleanup();
    Ok((hdt, perm))
}

/// Mark the reified triples of one HDT in POS or OPS order by merge-joining
/// `entries` — `[key…, data_term]`, sorted by the order's key — with the
/// permutation index's own stream of that order.
fn layer_from_permutation(
    perm: &Path,
    hdt: &Path,
    order: PermutationComponent,
    layer: &mut LayerWriter,
    entries: Sorted<Row<4>>,
) -> Result<()> {
    let index = PermutationIndex::open(perm, hdt)
        .with_context(|| format!("Failed to open permutation index {}", perm.display()))?;
    let (_sorter, sorted) = entries.finish()?;
    let mut sorted = sorted.peekable();
    if sorted.peek().is_none() {
        return Ok(());
    }
    for (position, triple) in index.all_triples(order)?.enumerate() {
        let (s, p, o) = triple?;
        let key = match order {
            PermutationComponent::Pos => [p, o, s],
            _ => [o, p, s],
        };
        while let Some(Ok(next)) = sorted.peek() {
            let next_key = [next.0[0], next.0[1], next.0[2]];
            if next_key < key {
                bail!("a reified triple is missing from the permutation index");
            }
            if next_key > key {
                break;
            }
            let data_term = next.0[3];
            sorted.next();
            layer.set(position as u64, data_term)?;
        }
        if let Some(Err(_)) = sorted.peek() {
            return Err(sorted.next().unwrap().unwrap_err());
        }
        if sorted.peek().is_none() {
            break;
        }
    }
    ensure!(
        sorted.next().is_none(),
        "a reified triple is missing from the permutation index"
    );
    Ok(())
}

fn layer_from_spo(layer: &mut LayerWriter, entries: Sorted<Row<2>>) -> Result<()> {
    let (_sorter, sorted) = entries.finish()?;
    for row in sorted {
        let [position, data_term] = row?.0;
        layer.set(position, data_term)?;
    }
    Ok(())
}

/// The three layer sorts of one HDT.
struct LayerEntries {
    spo: Sorted<Row<2>>,
    pos: Sorted<Row<4>>,
    ops: Sorted<Row<4>>,
}

impl LayerEntries {
    fn new(temp_dir: &Path, budget: usize) -> Self {
        Self {
            spo: Sorted::new(temp_dir, budget / 3),
            pos: Sorted::new(temp_dir, budget / 3),
            ops: Sorted::new(temp_dir, budget / 3),
        }
    }

    fn push(&mut self, position: u64, s: u64, p: u64, o: u64, data_term: u64) -> Result<()> {
        self.spo.push(Row([position, data_term]))?;
        self.pos.push(Row([p, o, s, data_term]))?;
        self.ops.push(Row([o, p, s, data_term]))
    }

    #[allow(clippy::too_many_arguments)]
    fn write(
        self,
        hdt: &Path,
        perm: &Path,
        orders: [u32; 3],
        triples: u64,
        reverse_width: u8,
        temp_dir: &Path,
        drafts: &mut Vec<DraftSection>,
    ) -> Result<()> {
        let mut spo = LayerWriter::new(temp_dir, orders[0], triples, reverse_width)?;
        layer_from_spo(&mut spo, self.spo)?;
        drafts.extend(spo.finish(temp_dir)?);
        let mut pos = LayerWriter::new(temp_dir, orders[1], triples, reverse_width)?;
        layer_from_permutation(perm, hdt, PermutationComponent::Pos, &mut pos, self.pos)?;
        drafts.extend(pos.finish(temp_dir)?);
        let mut ops = LayerWriter::new(temp_dir, orders[2], triples, reverse_width)?;
        layer_from_permutation(perm, hdt, PermutationComponent::Ops, &mut ops, self.ops)?;
        drafts.extend(ops.finish(temp_dir)?);
        Ok(())
    }
}

/// Walks one dictionary's run of triple-term IRIs in order.
struct RunCursor<'a> {
    dictionary: Option<&'a mut DictionaryResolver>,
    next: u64,
    end: u64,
    current: Vec<u8>,
}

impl<'a> RunCursor<'a> {
    fn new(dictionary: Option<&'a mut DictionaryResolver>, shape: &HdtShape) -> Result<Self> {
        let mut cursor = Self {
            dictionary,
            next: shape.run_start,
            end: shape.run_end,
            current: Vec::new(),
        };
        cursor.load()?;
        Ok(cursor)
    }

    fn load(&mut self) -> Result<()> {
        if self.next < self.end
            && let Some(dictionary) = self.dictionary.as_mut()
        {
            dictionary.object_term(self.next, &mut self.current)?;
        }
        Ok(())
    }

    fn exhausted(&self) -> bool {
        self.dictionary.is_none() || self.next >= self.end
    }

    /// If the run's next IRI is `iri`, consume it and return its identifier.
    fn take(&mut self, iri: &str) -> Result<Option<u64>> {
        if self.exhausted() {
            return Ok(None);
        }
        match self.current.as_slice().cmp(iri.as_bytes()) {
            std::cmp::Ordering::Less => bail!(
                "triple-term IRI {} has no proposition",
                String::from_utf8_lossy(&self.current)
            ),
            std::cmp::Ordering::Greater => Ok(None),
            std::cmp::Ordering::Equal => {
                let id = self.next;
                self.next += 1;
                self.load()?;
                Ok(Some(id))
            }
        }
    }
}

/// Three packed arrays holding the proposition of each IRI in a run.
struct RunWriter {
    component: u32,
    subject: PackedWriter,
    predicate: PackedWriter,
    object: PackedWriter,
}

impl RunWriter {
    fn new(temp_dir: &Path, component: u32, widths: (u8, u8, u8)) -> Result<Self> {
        Ok(Self {
            component,
            subject: PackedWriter::new(temp_dir, widths.0)?,
            predicate: PackedWriter::new(temp_dir, widths.1)?,
            object: PackedWriter::new(temp_dir, widths.2)?,
        })
    }

    fn push(&mut self, target: (u64, u64, u64, bool)) -> Result<()> {
        let (s, p, o, unasserted) = target;
        self.subject.push((s << 1) | u64::from(unasserted))?;
        self.predicate.push(p)?;
        self.object.push(o)
    }

    fn finish(self, drafts: &mut Vec<DraftSection>) -> Result<()> {
        drafts.push(
            self.subject
                .finish(section_type(self.component, kind::TARGET_SUBJECT))?,
        );
        drafts.push(
            self.predicate
                .finish(section_type(self.component, kind::TARGET_PREDICATE))?,
        );
        drafts.push(
            self.object
                .finish(section_type(self.component, kind::TARGET_OBJECT))?,
        );
        Ok(())
    }
}

/// Classify, store, and index the triple terms recorded in `spools`.
pub(crate) fn finish(
    ctx: &FinishContext<'_>,
    spools: Vec<PropositionSpool>,
) -> Result<BuiltTripleTerms> {
    let budget = ctx.memory_budget;
    let temp_dir = ctx.temp_dir;

    // 1. Deduplicate.
    let started = std::time::Instant::now();
    let mut records = Sorted::<PropositionRecord>::new(temp_dir, budget / 2);
    for spool in &spools {
        for record in spool.reader()? {
            records.push(record?)?;
        }
    }
    let (_records_sorter, merged) = records.finish()?;
    let mut writer = SpoolWriter::create(temp_dir.join("tt-distinct.zst"))?;
    let mut previous: Option<PropositionRecord> = None;
    for record in merged {
        let record = record?;
        if let Some(previous) = &previous {
            ensure!(
                previous.iri != record.iri,
                "two different triple terms share the IRI {}",
                record.iri
            );
        }
        writer.push(&record)?;
        previous = Some(record);
    }
    drop(_records_sorter);
    for spool in &spools {
        spool.remove();
    }
    let distinct = writer.finish()?;
    tracing::info!(
        "Triple terms: {} distinct ({:.1}s)",
        distinct.records,
        started.elapsed().as_secs_f64()
    );

    // 2. Classify against data.hdt.
    let started = std::time::Instant::now();
    let (_, mut data_dictionary) = open_hdt(ctx.hdt, budget / 4)?;
    let data_shape = shape_of(ctx.hdt, &mut data_dictionary)?;
    let (asserted, missing) = place(ctx.hdt, &distinct, None, temp_dir, budget / 2, false)?;
    let (_missing_sorter, missing) = missing.finish()?;
    let (unasserted_ranks, unasserted_count) = RankFile::write(
        temp_dir.join("tt-unasserted.ranks"),
        missing.map(|row| row.map(|row| row.0[0])),
    )?;
    let asserted_count = distinct.records - unasserted_count;
    tracing::info!(
        "Triple terms: {asserted_count} asserted, {unasserted_count} unasserted ({:.1}s)",
        started.elapsed().as_secs_f64()
    );

    // 3. Store the unasserted propositions.
    let mut propositions = None;
    let mut props_dictionary = None;
    let mut props_shape = HdtShape::default();
    let mut placed_unasserted = None;
    if unasserted_count > 0 {
        let started = std::time::Instant::now();
        let ntriples = temp_dir.join("tt-propositions.nt.zst");
        write_propositions_ntriples(&distinct, &unasserted_ranks, &ntriples)?;
        let (hdt, perm) = build_propositions_hdt(ctx, &ntriples)?;
        let _ = std::fs::remove_file(&ntriples);
        let (_, mut dictionary) = open_hdt(&hdt, budget / 4)?;
        props_shape = shape_of(&hdt, &mut dictionary)?;
        let (found, _) = place(
            &hdt,
            &distinct,
            Some(&unasserted_ranks),
            temp_dir,
            budget / 2,
            true,
        )?;
        placed_unasserted = Some(found);
        props_dictionary = Some(dictionary);
        propositions = Some((hdt, perm));
        tracing::info!(
            "Triple terms: propositions HDT built ({:.1}s)",
            started.elapsed().as_secs_f64()
        );
    }
    drop(unasserted_ranks);

    // 4. Runs and layer entries.
    let started = std::time::Instant::now();
    let widths = (
        id_width(data_shape.subjects.max(props_shape.subjects)) + 1,
        id_width(data_shape.predicates.max(props_shape.predicates)),
        id_width(data_shape.objects.max(props_shape.objects)),
    );
    let reverse_width = id_width(data_shape.objects);
    let mut drafts = Vec::new();
    let mut data_run = RunWriter::new(temp_dir, component::DATA_RUN, widths)?;
    let mut props_run = RunWriter::new(temp_dir, component::PROPS_RUN, widths)?;
    let mut data_layers = LayerEntries::new(temp_dir, budget / 4);
    let mut props_layers = LayerEntries::new(temp_dir, budget / 4);
    {
        let (_a, asserted) = asserted.finish()?;
        let mut asserted = asserted.peekable();
        let mut placed = match placed_unasserted {
            Some(found) => {
                let (sorter, iter) = found.finish()?;
                Some((sorter, iter.peekable()))
            }
            None => None,
        };
        let mut data_cursor = RunCursor::new(Some(&mut data_dictionary), &data_shape)?;
        let mut props_cursor = RunCursor::new(props_dictionary.as_mut(), &props_shape)?;
        for (rank, record) in distinct.reader()?.enumerate() {
            let record = record?;
            let rank = rank as u64;
            let (target, layer) = match asserted.peek() {
                Some(Ok(row)) if row.0[0] == rank => {
                    let [_, position, s, p, o] = asserted.next().unwrap()?.0;
                    ((s, p, o, false), (true, position, s, p, o))
                }
                Some(Err(_)) => return Err(asserted.next().unwrap().unwrap_err()),
                _ => {
                    let (_, placed) = placed
                        .as_mut()
                        .context("a triple term is neither asserted nor stored")?;
                    let row = placed
                        .next()
                        .context("a triple term is neither asserted nor stored")??;
                    let [placed_rank, position, s, p, o] = row.0;
                    ensure!(
                        placed_rank == rank,
                        "a triple term is neither asserted nor stored"
                    );
                    ((s, p, o, true), (false, position, s, p, o))
                }
            };
            let data_term = data_cursor.take(&record.iri)?;
            let props_term = props_cursor.take(&record.iri)?;
            ensure!(
                data_term.is_some() || props_term.is_some(),
                "triple term {} occurs in neither dictionary",
                record.iri
            );
            if data_term.is_some() {
                data_run.push(target)?;
            }
            if props_term.is_some() {
                props_run.push(target)?;
            }
            if let Some(data_term) = data_term {
                let (in_data, position, s, p, o) = layer;
                let entries = if in_data {
                    &mut data_layers
                } else {
                    &mut props_layers
                };
                entries.push(position, s, p, o, data_term)?;
            }
        }
        ensure!(
            data_cursor.exhausted() && props_cursor.exhausted(),
            "a triple-term IRI has no proposition"
        );
    }
    data_run.finish(&mut drafts)?;
    props_run.finish(&mut drafts)?;
    distinct.remove();

    // 5. Layers.
    data_layers.write(
        ctx.hdt,
        ctx.perm,
        format::DATA_ORDERS,
        data_shape.triples,
        reverse_width,
        temp_dir,
        &mut drafts,
    )?;
    if let Some((hdt, perm)) = &propositions {
        props_layers.write(
            hdt,
            perm,
            format::PROPS_ORDERS,
            props_shape.triples,
            reverse_width,
            temp_dir,
            &mut drafts,
        )?;
    }
    tracing::info!(
        "Triple terms: index layers built ({:.1}s)",
        started.elapsed().as_secs_f64()
    );

    // 6. Assemble.
    let facts = |hdt: &Path, shape: &HdtShape| -> Result<SourceFacts> {
        Ok(SourceFacts {
            triples: shape.triples,
            objects: shape.objects,
            source_length: shape.source_length,
            run_start: if shape.run_end > shape.run_start {
                shape.run_start
            } else {
                0
            },
            run_length: shape.run_end - shape.run_start,
            digest: *SourceIdentity::capture(hdt)?.digest(),
        })
    };
    let data_facts = facts(ctx.hdt, &data_shape)?;
    let props_facts = match &propositions {
        Some((hdt, _)) => facts(hdt, &props_shape)?,
        None => SourceFacts::default(),
    };
    let flags = if propositions.is_some() {
        format::HAS_PROPOSITIONS
    } else {
        0
    };
    let index = tempfile::Builder::new()
        .prefix(".hdtc-tt-")
        .tempfile_in(ctx.output_dir)?
        .into_temp_path();
    format::assemble(&index, flags, &data_facts, &props_facts, drafts)?;
    Ok(BuiltTripleTerms {
        index,
        propositions,
        distinct: asserted_count + unasserted_count,
        asserted: asserted_count,
        unasserted: unasserted_count,
    })
}
