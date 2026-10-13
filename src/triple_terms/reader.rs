//! Reading the triple-term index, and rendering triple terms as N-Triples 1.2.
//!
//! This is the seek-based reader for this crate's CLI, in the style of the
//! permutation index's: every access is a positioned read, nothing is loaded
//! whole, and memory does not grow with the index. Mapped readers (KGF's
//! `kgf-store`) use [`TripleTermIndex::sections`] and the format instead.

use super::format::{self, Header, Section, component, kind, section_type};
use crate::hdt::reader::{DictionaryResolver, open_hdt, write_nt_object, write_nt_subject};
use crate::permutation::format::{SUBBLOCK_BITS, SUPERBLOCK_BITS, id_width};
use crate::permutation::scan_hdt;
use anyhow::{Context, Result, ensure};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

/// Which HDT a run, layer, or target belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// `data.hdt`.
    Data,
    /// The propositions HDT, `data.hdt.props.hdt`.
    Propositions,
}

/// The three triple orders a layer is kept in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Spo,
    Pos,
    Ops,
}

/// A triple term's proposition: its three identifiers in the HDT that holds
/// it — `data.hdt` when asserted, the propositions HDT when not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Target {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64,
    pub asserted: bool,
}

impl Target {
    pub fn source(&self) -> Source {
        if self.asserted {
            Source::Data
        } else {
            Source::Propositions
        }
    }
}

/// An open `data.hdt.tt`.
#[derive(Debug)]
pub struct TripleTermIndex {
    path: PathBuf,
    file: File,
    header: Header,
    sections: Vec<Section>,
    /// Each run's subject, predicate and object arrays, data run first: found
    /// once at open rather than on every decode.
    runs: [[Section; 3]; 2],
}

fn read_packed(file: &File, section: &Section, index: u64) -> Result<u64> {
    read_packed_with(section, index, |offset, bytes| {
        Ok(file.read_exact_at(bytes, offset)?)
    })
}

/// Entry `index` of a packed array, its bytes fetched by `read(offset, buf)`.
fn read_packed_with(
    section: &Section,
    index: u64,
    read: impl FnOnce(u64, &mut [u8]) -> Result<()>,
) -> Result<u64> {
    ensure!(
        index < section.entry_count,
        "packed index {index} out of range in section {:#06x}",
        section.section_type
    );
    let width = u64::from(section.bits_per_entry);
    if width == 0 {
        return Ok(0);
    }
    let bit = index * width;
    let shift = (bit % 8) as u32;
    let needed = (u64::from(shift) + width).div_ceil(8) as usize;
    let mut bytes = [0u8; 9];
    read(section.offset + bit / 8, &mut bytes[..needed])?;
    let mut value = 0u128;
    for (i, byte) in bytes[..needed].iter().enumerate() {
        value |= u128::from(*byte) << (i * 8);
    }
    let mask = if width == 64 {
        u128::from(u64::MAX)
    } else {
        (1u128 << width) - 1
    };
    Ok(((value >> shift) & mask) as u64)
}

/// Bytes per page of a [`RunCache`].
const RUN_PAGE_BYTES: u64 = 4096;

/// The run arrays' pages, held for decoding many triple terms: a decode whose
/// pages are held costs no system call, where an uncached one costs three
/// positioned reads and more for each nested term.
///
/// Direct-mapped over the stretch of the file the runs occupy, so a budget
/// covering that stretch reads each page once, and a smaller one still serves
/// a sequential walk of a run from a page per few hundred entries. Pages are
/// read as they are first needed; memory grows to the budget, never past it.
pub(crate) struct RunCache {
    first_page: u64,
    file_length: u64,
    slots: Vec<Option<(u64, Box<[u8]>)>>,
}

impl RunCache {
    /// Copy `out.len()` bytes of the index file from `offset` into `out`.
    fn read(&mut self, file: &File, offset: u64, out: &mut [u8]) -> Result<()> {
        let mut done = 0;
        while done < out.len() {
            let at = offset + done as u64;
            let page = at / RUN_PAGE_BYTES;
            let slot = (page.saturating_sub(self.first_page) % self.slots.len() as u64) as usize;
            if !matches!(&self.slots[slot], Some((held, _)) if *held == page) {
                let start = page * RUN_PAGE_BYTES;
                ensure!(
                    start < self.file_length,
                    "run entry past the end of the index"
                );
                let length = RUN_PAGE_BYTES.min(self.file_length - start) as usize;
                let mut bytes = vec![0u8; length].into_boxed_slice();
                file.read_exact_at(&mut bytes, start)?;
                self.slots[slot] = Some((page, bytes));
            }
            let Some((_, bytes)) = &self.slots[slot] else {
                unreachable!("filled above")
            };
            let within = (at - page * RUN_PAGE_BYTES) as usize;
            let take = (out.len() - done).min(bytes.len().saturating_sub(within));
            ensure!(take > 0, "run entry past the end of the index");
            out[done..done + take].copy_from_slice(&bytes[within..within + take]);
            done += take;
        }
        Ok(())
    }
}

impl std::fmt::Debug for RunCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunCache")
            .field("slots", &self.slots.len())
            .finish()
    }
}

/// A layer: which positions of one HDT's triples in one order are marked,
/// with the rank directory and the reverse array.
///
/// Stored dense or sparse, as its counts choose (§8.1); the queries are the
/// same either way. A sparse layer holds its high bits' rank directory in
/// memory — about 5% of the high bits' size, 1.8 MB for ROBOKOP's — because
/// each of its ranks is two selects there.
#[derive(Debug, Clone)]
pub struct Layer {
    /// Positions covered: the HDT's triples.
    positions: u64,
    /// The bitmap the rank directory indexes: the layer itself, or, sparse,
    /// its Elias–Fano high bits.
    pub bitmap: Section,
    pub superrank: Section,
    pub subrank: Section,
    pub reverse: Section,
    sparse: Option<SparseLayer>,
}

#[derive(Debug, Clone)]
struct SparseLayer {
    low: Section,
    low_bits: u8,
    high: std::sync::Arc<crate::permutation::CachedBitmap>,
}

impl Layer {
    /// The layer whose sections `section` finds by kind, over `positions`
    /// positions. The directory check (§6) has fixed which encoding is
    /// present.
    pub(crate) fn from_sections(
        file: &File,
        positions: u64,
        section: impl Fn(u32) -> Option<Section>,
    ) -> Result<Self> {
        let get = |k: u32| section(k).with_context(|| format!("missing layer section {k:#04x}"));
        let (superrank, subrank, reverse) = (
            get(kind::LAYER_SUPERRANK)?,
            get(kind::LAYER_SUBRANK)?,
            get(kind::REVERSE)?,
        );
        let (bitmap, sparse) = match section(kind::LAYER) {
            Some(bitmap) => (bitmap, None),
            None => {
                let (high, low) = (get(kind::LAYER_HIGH)?, get(kind::LAYER_LOW)?);
                let cached = crate::permutation::CachedBitmap::from_samples(
                    high.offset,
                    high.entry_count,
                    read_samples(file, &superrank, 8)?,
                    read_subrank(file, &subrank)?,
                );
                let sparse = SparseLayer {
                    low_bits: low.bits_per_entry,
                    low,
                    high: std::sync::Arc::new(cached),
                };
                (high, Some(sparse))
            }
        };
        Ok(Self {
            positions,
            bitmap,
            superrank,
            subrank,
            reverse,
            sparse,
        })
    }

    /// Positions covered.
    pub fn len(&self) -> u64 {
        self.positions
    }

    /// Marked positions.
    pub fn marked(&self) -> u64 {
        self.reverse.entry_count
    }

    /// Marked positions in `[0, position)`.
    pub fn rank1(&self, file: &File, position: u64) -> Result<u64> {
        ensure!(position <= self.positions, "rank position out of range");
        let Some(sparse) = &self.sparse else {
            return self.dense_rank1(file, position);
        };
        if position == self.positions {
            return Ok(self.marked());
        }
        Ok(Self::sparse_locate(file, sparse, position)?.0)
    }

    /// In a sparse layer, the rank of the first mark at or after `position`
    /// within its bucket, the rank one past the bucket's last mark, and
    /// `position`'s low bits: `position` is marked exactly when the first is
    /// below the second and its low bits are the third.
    fn sparse_locate(file: &File, sparse: &SparseLayer, position: u64) -> Result<(u64, u64, u64)> {
        // The marks in `position`'s bucket are those between the clear bits
        // closing the bucket before it and closing it; their low bits ascend.
        let bucket = position >> sparse.low_bits;
        let start = match bucket {
            0 => 0,
            _ => sparse.high.select0(file, bucket - 1)? - (bucket - 1),
        };
        let end = sparse.high.select0(file, bucket)? - bucket;
        let target = position & ((1u64 << sparse.low_bits) - 1);
        let (mut low, mut high) = (start, end);
        while low < high {
            let mid = low + (high - low) / 2;
            if read_packed(file, &sparse.low, mid)? < target {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        Ok((low, end, target))
    }

    /// The rank of `position` among the marks if it is marked, `None` if not:
    /// a membership test and [`Self::rank1`] for the price of one rank.
    pub fn rank_if_marked(&self, file: &File, position: u64) -> Result<Option<u64>> {
        ensure!(position < self.positions, "bit position out of range");
        let Some(sparse) = &self.sparse else {
            let mut byte = [0u8; 1];
            file.read_exact_at(&mut byte, self.bitmap.offset + position / 8)?;
            if byte[0] >> (position % 8) & 1 == 0 {
                return Ok(None);
            }
            return self.dense_rank1(file, position).map(Some);
        };
        let (rank, end, target) = Self::sparse_locate(file, sparse, position)?;
        Ok((rank < end && read_packed(file, &sparse.low, rank)? == target).then_some(rank))
    }

    /// The position of the `rank`-th mark. Sparse, through the directory
    /// held in memory; dense, by loading the layer's directory for this one
    /// call — fine for a test, not for a loop, where a [`LayerCursor`] in
    /// select mode is the tool.
    #[cfg(test)]
    pub(crate) fn select1(&self, file: &File, rank: u64) -> Result<u64> {
        ensure!(rank < self.marked(), "select rank out of range");
        match &self.sparse {
            Some(sparse) => {
                let high = sparse.high.select1(file, rank)? - rank;
                Ok(high << sparse.low_bits | read_packed(file, &sparse.low, rank)?)
            }
            None => crate::permutation::CachedBitmap::from_samples(
                self.bitmap.offset,
                self.bitmap.entry_count,
                read_samples(file, &self.superrank, 8)?,
                read_subrank(file, &self.subrank)?,
            )
            .select1(file, rank),
        }
    }

    fn dense_rank1(&self, file: &File, position: u64) -> Result<u64> {
        let length = self.bitmap.entry_count;
        if length == 0 {
            return Ok(0);
        }
        if position == length {
            return read_packed(file, &self.superrank, self.superrank.entry_count - 1);
        }
        let sub_index = position / u64::from(SUBBLOCK_BITS);
        let base = read_packed(file, &self.superrank, position / u64::from(SUPERBLOCK_BITS))?
            + read_packed(file, &self.subrank, sub_index)?;
        let start = sub_index * u64::from(SUBBLOCK_BITS);
        let bits = position - start;
        let byte_count = bits.div_ceil(8) as usize;
        if byte_count == 0 {
            return Ok(base);
        }
        let mut bytes = [0u8; 64];
        file.read_exact_at(&mut bytes[..byte_count], self.bitmap.offset + start / 8)?;
        if !bits.is_multiple_of(8) {
            bytes[byte_count - 1] &= (1u8 << (bits % 8)) - 1;
        }
        Ok(base
            + bytes[..byte_count]
                .iter()
                .map(|b| u64::from(b.count_ones()))
                .sum::<u64>())
    }

    /// Marks counted by the rank directory: its sentinel, the bitmap's
    /// population, recorded independently of the reverse array.
    pub fn population(&self, file: &File) -> Result<u64> {
        if self.superrank.entry_count == 0 {
            return Ok(0);
        }
        read_packed(file, &self.superrank, self.superrank.entry_count - 1)
    }

    /// The `data.hdt` object identifier of the triple-term IRI at `rank`.
    pub fn reverse(&self, file: &File, rank: u64) -> Result<u64> {
        read_packed(file, &self.reverse, rank)
    }
}

impl TripleTermIndex {
    /// Open the triple-term index beside `hdt_path`, checking its binding to
    /// `data.hdt` and to the propositions HDT the way the permutation index
    /// checks its own: counts and lengths, not a full digest.
    pub fn open(hdt_path: &Path) -> Result<Self> {
        let path = format::canonical_path(hdt_path);
        let (header, sections) = format::read_header_and_sections(&path)?;
        let file = File::open(&path)?;
        let data = scan_hdt(hdt_path)?;
        ensure!(
            header.data.triples == data.triples
                && header.data.objects == data.objects
                && header.data.source_length == data.file_length - data.data_offset,
            "{} does not belong to {}",
            path.display(),
            hdt_path.display()
        );
        let (mut subjects, mut predicates) = (data.subjects, data.predicates);
        if header.has_propositions() {
            let props_path = format::propositions_path(hdt_path);
            let props = scan_hdt(&props_path)
                .with_context(|| format!("Failed to open {}", props_path.display()))?;
            ensure!(
                header.props.triples == props.triples
                    && header.props.objects == props.objects
                    && header.props.source_length == props.file_length - props.data_offset,
                "{} does not belong to {}",
                path.display(),
                props_path.display()
            );
            subjects = subjects.max(props.subjects);
            predicates = predicates.max(props.predicates);
        }
        // §7's run widths, which follow from the bound HDTs' identifier spaces.
        let width_of = |k: u32| {
            sections
                .iter()
                .find(|s| s.section_type == section_type(component::DATA_RUN, k))
                .map(|s| s.bits_per_entry)
        };
        ensure!(
            width_of(kind::TARGET_SUBJECT) == Some(id_width(subjects) + 1)
                && width_of(kind::TARGET_PREDICATE) == Some(id_width(predicates)),
            "{} run arrays have the wrong widths for {}",
            path.display(),
            hdt_path.display()
        );
        let run_sections = |run: u32, expected: u64| -> Result<[Section; 3]> {
            let mut found = Vec::with_capacity(3);
            for k in [
                kind::TARGET_SUBJECT,
                kind::TARGET_PREDICATE,
                kind::TARGET_OBJECT,
            ] {
                let section = sections
                    .binary_search_by_key(&section_type(run, k), |s| s.section_type)
                    .map(|i| sections[i].clone())
                    .map_err(|_| {
                        anyhow::anyhow!(
                            "missing triple-term index section {:#06x}",
                            section_type(run, k)
                        )
                    })?;
                ensure!(
                    section.entry_count == expected,
                    "triple-term run arrays do not match the run length"
                );
                found.push(section);
            }
            Ok(found.try_into().expect("three run arrays"))
        };
        let runs = [
            run_sections(component::DATA_RUN, header.data.run_length)?,
            run_sections(component::PROPS_RUN, header.props.run_length)?,
        ];
        Ok(Self {
            path,
            file,
            header,
            sections,
            runs,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn sections(&self) -> &[Section] {
        &self.sections
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn has_propositions(&self) -> bool {
        self.header.has_propositions()
    }

    fn section(&self, section_type: u32) -> Result<&Section> {
        self.sections
            .binary_search_by_key(&section_type, |s| s.section_type)
            .map(|i| &self.sections[i])
            .map_err(|_| anyhow::anyhow!("missing triple-term index section {section_type:#06x}"))
    }

    /// The object identifiers of the triple-term IRIs in one HDT.
    pub fn run(&self, source: Source) -> Range<u64> {
        let facts = match source {
            Source::Data => &self.header.data,
            Source::Propositions => &self.header.props,
        };
        facts.run_start..facts.run_start + facts.run_length
    }

    /// The proposition of the triple-term IRI with object identifier `object`
    /// in `source`, or `None` if that identifier is not a triple-term IRI.
    /// Three positioned reads: for decoding many terms, [`Self::cached_target`].
    pub fn target(&self, source: Source, object: u64) -> Result<Option<Target>> {
        self.target_with(source, object, |section, at| {
            read_packed(&self.file, section, at)
        })
    }

    /// A cache for [`Self::cached_target`] holding at most `budget` bytes of
    /// the run arrays' pages, and never more than they span.
    pub(crate) fn run_cache(&self, budget: usize) -> RunCache {
        let spans: Vec<(u64, u64)> = self
            .runs
            .iter()
            .flatten()
            .filter(|section| section.length > 0)
            .map(|section| (section.offset, section.offset + section.length))
            .collect();
        let first = spans.iter().map(|span| span.0).min().unwrap_or(0) / RUN_PAGE_BYTES;
        let last = spans
            .iter()
            .map(|span| span.1)
            .max()
            .unwrap_or(0)
            .div_ceil(RUN_PAGE_BYTES);
        let slots = (budget as u64 / RUN_PAGE_BYTES).clamp(1, last.saturating_sub(first).max(1));
        RunCache {
            first_page: first,
            file_length: self.header.sidecar_size,
            slots: std::iter::repeat_with(|| None)
                .take(slots as usize)
                .collect(),
        }
    }

    /// As [`Self::target`], reading the run arrays through `cache`.
    pub(crate) fn cached_target(
        &self,
        cache: &mut RunCache,
        source: Source,
        object: u64,
    ) -> Result<Option<Target>> {
        self.target_with(source, object, |section, at| {
            read_packed_with(section, at, |offset, bytes| {
                cache.read(&self.file, offset, bytes)
            })
        })
    }

    fn target_with(
        &self,
        source: Source,
        object: u64,
        mut read: impl FnMut(&Section, u64) -> Result<u64>,
    ) -> Result<Option<Target>> {
        let run = self.run(source);
        if !run.contains(&object) {
            return Ok(None);
        }
        let [subject, predicate, object_array] = match source {
            Source::Data => &self.runs[0],
            Source::Propositions => &self.runs[1],
        };
        let at = object - run.start;
        let subject = read(subject, at)?;
        Ok(Some(Target {
            subject: subject >> 1,
            predicate: read(predicate, at)?,
            object: read(object_array, at)?,
            asserted: subject & 1 == 0,
        }))
    }

    /// One layer of one source.
    pub fn layer(&self, source: Source, order: Order) -> Result<Layer> {
        let orders = match source {
            Source::Data => format::DATA_ORDERS,
            Source::Propositions => {
                ensure!(self.has_propositions(), "index has no propositions HDT");
                format::PROPS_ORDERS
            }
        };
        let component = orders[match order {
            Order::Spo => 0,
            Order::Pos => 1,
            Order::Ops => 2,
        }];
        let positions = match source {
            Source::Data => self.header.data.triples,
            Source::Propositions => self.header.props.triples,
        };
        Layer::from_sections(&self.file, positions, |k| {
            self.section(section_type(component, k)).ok().cloned()
        })
    }
}

/// Renders triple-term IRIs as N-Triples 1.2 triple terms.
///
/// Holds the index and the propositions HDT's dictionary; `data.hdt`'s
/// dictionary is passed in, because this decoder is carried inside it
/// (`DictionaryResolver::enable_triple_terms`).
pub struct TripleTermDecoder {
    index: TripleTermIndex,
    propositions: Option<DictionaryResolver>,
    /// Output decodes every triple-term object it writes, in no useful order.
    runs: RunCache,
    data_run: Range<u64>,
    scratch: Vec<u8>,
}

/// Nesting deeper than the parser accepts is refused rather than followed: a
/// well-formed index has no cycles, and this bounds the damage of one that does.
const MAX_DEPTH: usize = super::iri::MAX_NESTING;

impl std::fmt::Debug for TripleTermDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TripleTermDecoder")
            .field("index", &self.index.path)
            .finish()
    }
}

impl TripleTermDecoder {
    /// The share of a command's `memory_limit` a decoder for `hdt_path`
    /// takes: an eighth when the HDT has a triple-term index, nothing when
    /// not. A caller takes it out of what it gives the HDT's own dictionary,
    /// so the two together stay within the limit.
    pub fn budget(hdt_path: &Path, memory_limit: usize) -> usize {
        if format::canonical_path(hdt_path).exists() {
            memory_limit / 8
        } else {
            0
        }
    }

    /// The decoder for `hdt_path`, or `None` when the HDT has no triple
    /// terms. An HDT whose dictionary holds triple-term IRIs but has no index
    /// beside it is an error: its triple terms could not be rendered.
    ///
    /// `memory_limit` bounds the propositions HDT's dictionary and the run
    /// cache together.
    pub fn open(
        hdt_path: &Path,
        data: &mut DictionaryResolver,
        memory_limit: usize,
    ) -> Result<Option<Self>> {
        let index_path = format::canonical_path(hdt_path);
        if !index_path.exists() {
            ensure!(
                !super::merge::has_triple_term_iris(data)?,
                "{} contains RDF 1.2 triple terms but its triple-term index {} is missing",
                hdt_path.display(),
                index_path.display()
            );
            return Ok(None);
        }
        let index = TripleTermIndex::open(hdt_path)?;
        let (propositions, runs) = if index.has_propositions() {
            let dictionary = open_hdt(&format::propositions_path(hdt_path), memory_limit / 2)?.1;
            (Some(dictionary), index.run_cache(memory_limit / 2))
        } else {
            (None, index.run_cache(memory_limit))
        };
        let data_run = index.run(Source::Data);
        Ok(Some(Self {
            index,
            propositions,
            runs,
            data_run,
            scratch: Vec::new(),
        }))
    }

    /// The propositions HDT's dictionary, when there is one.
    pub fn propositions(&mut self) -> Option<&mut DictionaryResolver> {
        self.propositions.as_mut()
    }

    /// Whether `data.hdt` object identifier `object` names a triple term.
    #[inline]
    pub fn is_data_term(&self, object: u64) -> bool {
        self.data_run.contains(&object)
    }

    /// Write the triple term named by `data.hdt` object `object` to `out`.
    pub fn render_data(
        &mut self,
        data: &mut DictionaryResolver,
        object: u64,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let target = self
            .index
            .cached_target(&mut self.runs, Source::Data, object)?
            .context("not a triple-term IRI")?;
        self.render_target(data, target, out, 0)
    }

    /// Write the triple term whose proposition is `target`.
    pub fn render_target(
        &mut self,
        data: &mut DictionaryResolver,
        target: Target,
        out: &mut Vec<u8>,
        depth: usize,
    ) -> Result<()> {
        ensure!(depth < MAX_DEPTH, "triple terms nested too deeply");
        out.extend_from_slice(b"<<( ");
        let source = target.source();
        {
            let dictionary = match source {
                Source::Data => &mut *data,
                Source::Propositions => self
                    .propositions
                    .as_mut()
                    .context("unasserted triple term without a propositions HDT")?,
            };
            dictionary.subject_term_raw(target.subject, &mut self.scratch)?;
            write_nt_subject(out, &self.scratch)?;
            out.extend_from_slice(b" <");
            dictionary.predicate_term(target.predicate, &mut self.scratch)?;
            out.extend_from_slice(&self.scratch);
            out.extend_from_slice(b"> ");
        }
        match self
            .index
            .cached_target(&mut self.runs, source, target.object)?
        {
            Some(nested) => self.render_target(data, nested, out, depth + 1)?,
            None => {
                let dictionary = match source {
                    Source::Data => &mut *data,
                    Source::Propositions => self.propositions.as_mut().unwrap(),
                };
                dictionary.object_term_raw(target.object, &mut self.scratch)?;
                write_nt_object(out, &self.scratch)?;
            }
        }
        out.extend_from_slice(b" )>>");
        Ok(())
    }
}

/// Streams the set bits of a layer within a position range, with the
/// `data.hdt` object identifier each one's reverse entry gives.
///
/// The work is proportional to the matches, not the range: the cursor knows
/// from two ranks how many set bits the range holds and stops after the last;
/// where the bits are sparse it jumps to each through the layer's rank
/// directory instead of reading the bitmap between them. A sparse layer is
/// read in its high bits, where marks are never far apart, and each one's
/// position is decoded from its place there and its low bits.
pub(crate) struct LayerCursor {
    file: File,
    bitmap_offset: u64,
    position: u64,
    end: u64,
    /// Set bits not yet returned.
    remaining: u64,
    /// The next rank, for select mode and sparse layers.
    rank: u64,
    /// Present in select mode: the rank directory of the bitmap read — the
    /// layer, or a sparse layer's high bits — in memory.
    directory: Option<std::sync::Arc<crate::permutation::CachedBitmap>>,
    word: u64,
    reverse: PackedStream,
    /// A sparse layer's low bits, from the first rank, and their width.
    low: Option<(PackedStream, u8)>,
    /// Bitmap bytes read ahead, and the byte offset within the bitmap of the
    /// first: a long scan is a few large reads, not one per word.
    chunk: Vec<u8>,
    chunk_start: u64,
}

/// Bitmap bytes a scanning cursor reads at once.
const CHUNK_BYTES: u64 = 4096;

/// Positions per match above which a range is read by select rather than by
/// scanning: one 64-byte read per match against `range / 8` bytes in total.
const SELECT_DENSITY: u64 = 4096;

impl LayerCursor {
    pub(crate) fn new(
        index: &TripleTermIndex,
        source: Source,
        order: Order,
        range: std::ops::Range<u64>,
    ) -> Result<Self> {
        Self::from_layer(index.path(), &index.layer(source, order)?, range)
    }

    /// The marks of `layer`, stored in the file at `path`, within `range`.
    pub(crate) fn from_layer(
        path: &Path,
        layer: &Layer,
        range: std::ops::Range<u64>,
    ) -> Result<Self> {
        let file = File::open(path)?;
        let start_rank = layer.rank1(&file, range.start)?;
        let end_rank = layer.rank1(&file, range.end)?;
        let matches = end_rank - start_rank;
        if let Some(sparse) = &layer.sparse {
            // The first mark's place in the high bits; the rest follow in
            // order, separated by the clear bits closing the buckets between.
            // Where those far outnumber the marks, each is selected instead.
            let buckets = (range.end - range.start) >> sparse.low_bits;
            let select = matches > 0 && buckets / matches > SELECT_DENSITY;
            let position = match matches {
                0 => 0,
                _ => sparse.high.select1(&file, start_rank)?,
            };
            return Ok(Self {
                bitmap_offset: layer.bitmap.offset,
                position,
                end: layer.bitmap.entry_count,
                remaining: matches,
                rank: start_rank,
                directory: select.then(|| std::sync::Arc::clone(&sparse.high)),
                word: 0,
                reverse: PackedStream::new(path, &layer.reverse, start_rank)?,
                low: Some((
                    PackedStream::new(path, &sparse.low, start_rank)?,
                    sparse.low_bits,
                )),
                chunk: Vec::new(),
                chunk_start: 0,
                file,
            });
        }
        let directory = if matches > 0 && (range.end - range.start) / matches > SELECT_DENSITY {
            Some(std::sync::Arc::new(
                crate::permutation::CachedBitmap::from_samples(
                    layer.bitmap.offset,
                    layer.bitmap.entry_count,
                    read_samples(&file, &layer.superrank, 8)?,
                    read_subrank(&file, &layer.subrank)?,
                ),
            ))
        } else {
            None
        };
        Ok(Self {
            bitmap_offset: layer.bitmap.offset,
            position: range.start,
            end: range.end,
            remaining: matches,
            rank: start_rank,
            directory,
            word: 0,
            reverse: PackedStream::new(path, &layer.reverse, start_rank)?,
            low: None,
            chunk: Vec::new(),
            chunk_start: 0,
            file,
        })
    }

    /// The bitmap's 64 bits from `word_start`, zero past its end.
    fn load_word(&mut self, word_start: u64) -> Result<u64> {
        let byte = word_start / 8;
        let bytes_total = self.end.div_ceil(8);
        // The word's bytes that exist must all be in the chunk.
        let needed_end = (byte + 8).min(bytes_total);
        let chunk_end = self.chunk_start + self.chunk.len() as u64;
        if byte < self.chunk_start || needed_end > chunk_end {
            let length = CHUNK_BYTES.min(bytes_total - byte) as usize;
            self.chunk.resize(length, 0);
            self.file
                .read_exact_at(&mut self.chunk, self.bitmap_offset + byte)?;
            self.chunk_start = byte;
        }
        let at = (byte - self.chunk_start) as usize;
        let mut bytes = [0u8; 8];
        let available = (self.chunk.len() - at).min(8);
        bytes[..available].copy_from_slice(&self.chunk[at..at + available]);
        Ok(u64::from_le_bytes(bytes))
    }

    pub(crate) fn next(&mut self) -> Result<Option<(u64, u64)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        if let Some(directory) = &self.directory {
            let place = directory.select1(&self.file, self.rank)?;
            let position = match &mut self.low {
                Some((low, low_bits)) => (place - self.rank) << *low_bits | low.next()?,
                None => place,
            };
            self.rank += 1;
            return Ok(Some((position, self.reverse.next()?)));
        }
        loop {
            ensure!(
                self.position < self.end,
                "layer range holds fewer bits than ranked"
            );
            let offset_in_word = self.position % 64;
            if offset_in_word == 0 || self.word == 0 {
                // Load the word holding `position`, masked below it.
                let word_start = self.position - offset_in_word;
                self.word = self.load_word(word_start)? & (u64::MAX << offset_in_word);
                if self.word == 0 {
                    self.position = word_start + 64;
                    continue;
                }
            }
            let bit = u64::from(self.word.trailing_zeros());
            let position = self.position - offset_in_word + bit;
            self.word &= self.word - 1;
            self.position = position + 1;
            if self.word == 0 {
                self.position = self.position.div_ceil(64) * 64;
            }
            // Sparse: the set bit is a mark's place in the high bits, its
            // bucket plus the marks before it.
            let position = match &mut self.low {
                Some((low, low_bits)) => {
                    let decoded = (position - self.rank) << *low_bits | low.next()?;
                    self.rank += 1;
                    decoded
                }
                None => position,
            };
            return Ok(Some((position, self.reverse.next()?)));
        }
    }
}

/// A rank directory's 16-bit subblock samples, as they are stored.
fn read_subrank(file: &File, section: &Section) -> Result<Vec<u16>> {
    let mut bytes = vec![0u8; section.entry_count as usize * 2];
    file.read_exact_at(&mut bytes, section.offset)?;
    Ok(bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect())
}

fn read_samples(file: &File, section: &Section, width: usize) -> Result<Vec<u64>> {
    let mut bytes = vec![0u8; section.entry_count as usize * width];
    file.read_exact_at(&mut bytes, section.offset)?;
    Ok(bytes
        .chunks_exact(width)
        .map(|chunk| {
            let mut value = [0u8; 8];
            value[..width].copy_from_slice(chunk);
            u64::from_le_bytes(value)
        })
        .collect())
}

/// Sequential reads of a packed array from a starting index.
struct PackedStream {
    reader: BufReader<File>,
    width: u32,
    buffer: u128,
    buffered: u32,
}

impl PackedStream {
    fn new(path: &Path, section: &Section, start: u64) -> Result<Self> {
        let width = u32::from(section.bits_per_entry);
        let bit = start * u64::from(width);
        let mut reader = BufReader::with_capacity(64 * 1024, File::open(path)?);
        reader.seek(SeekFrom::Start(section.offset + bit / 8))?;
        let mut stream = Self {
            reader,
            width,
            buffer: 0,
            buffered: 0,
        };
        let skip = (bit % 8) as u32;
        if skip > 0 {
            let mut byte = [0u8; 1];
            stream.reader.read_exact(&mut byte)?;
            stream.buffer = u128::from(byte[0] >> skip);
            stream.buffered = 8 - skip;
        }
        Ok(stream)
    }

    fn next(&mut self) -> Result<u64> {
        if self.width == 0 {
            return Ok(0);
        }
        while self.buffered < self.width {
            let mut byte = [0u8; 1];
            self.reader.read_exact(&mut byte)?;
            self.buffer |= u128::from(byte[0]) << self.buffered;
            self.buffered += 8;
        }
        let mask = if self.width == 64 {
            u128::from(u64::MAX)
        } else {
            (1u128 << self.width) - 1
        };
        let value = (self.buffer & mask) as u64;
        self.buffer >>= self.width;
        self.buffered -= self.width;
        Ok(value)
    }
}
