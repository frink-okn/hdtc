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
}

fn read_packed(file: &File, section: &Section, index: u64) -> Result<u64> {
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
    file.read_exact_at(&mut bytes[..needed], section.offset + bit / 8)?;
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

/// A layer: a bitmap with its rank directory and reverse array.
#[derive(Debug, Clone)]
pub struct Layer {
    pub bitmap: Section,
    pub superrank: Section,
    pub subrank: Section,
    pub reverse: Section,
}

impl Layer {
    /// Positions covered.
    pub fn len(&self) -> u64 {
        self.bitmap.entry_count
    }

    /// Set bits in `[0, position)`.
    pub fn rank1(&self, file: &File, position: u64) -> Result<u64> {
        let length = self.bitmap.entry_count;
        ensure!(position <= length, "rank position out of range");
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

    pub fn bit(&self, file: &File, position: u64) -> Result<bool> {
        ensure!(
            position < self.bitmap.entry_count,
            "bit position out of range"
        );
        let mut byte = [0u8; 1];
        file.read_exact_at(&mut byte, self.bitmap.offset + position / 8)?;
        Ok(byte[0] >> (position % 8) & 1 == 1)
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
        let index = Self {
            path,
            file,
            header,
            sections,
        };
        for run in [component::DATA_RUN, component::PROPS_RUN] {
            for k in [
                kind::TARGET_SUBJECT,
                kind::TARGET_PREDICATE,
                kind::TARGET_OBJECT,
            ] {
                let section = index.section(section_type(run, k))?;
                let expected = if run == component::DATA_RUN {
                    index.header.data.run_length
                } else {
                    index.header.props.run_length
                };
                ensure!(
                    section.entry_count == expected,
                    "triple-term run arrays do not match the run length"
                );
            }
        }
        Ok(index)
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
    pub fn target(&self, source: Source, object: u64) -> Result<Option<Target>> {
        let run = self.run(source);
        if !run.contains(&object) {
            return Ok(None);
        }
        let component = match source {
            Source::Data => component::DATA_RUN,
            Source::Propositions => component::PROPS_RUN,
        };
        let at = object - run.start;
        let subject = read_packed(
            &self.file,
            self.section(section_type(component, kind::TARGET_SUBJECT))?,
            at,
        )?;
        Ok(Some(Target {
            subject: subject >> 1,
            predicate: read_packed(
                &self.file,
                self.section(section_type(component, kind::TARGET_PREDICATE))?,
                at,
            )?,
            object: read_packed(
                &self.file,
                self.section(section_type(component, kind::TARGET_OBJECT))?,
                at,
            )?,
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
        Ok(Layer {
            bitmap: self.section(section_type(component, kind::LAYER))?.clone(),
            superrank: self
                .section(section_type(component, kind::LAYER_SUPERRANK))?
                .clone(),
            subrank: self
                .section(section_type(component, kind::LAYER_SUBRANK))?
                .clone(),
            reverse: self
                .section(section_type(component, kind::REVERSE))?
                .clone(),
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
    /// The decoder for `hdt_path`, or `None` when the HDT has no triple
    /// terms. An HDT whose dictionary holds triple-term IRIs but has no index
    /// beside it is an error: its triple terms could not be rendered.
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
        let propositions = if index.has_propositions() {
            Some(open_hdt(&format::propositions_path(hdt_path), memory_limit)?.1)
        } else {
            None
        };
        let data_run = index.run(Source::Data);
        Ok(Some(Self {
            index,
            propositions,
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
            .target(Source::Data, object)?
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
        match self.index.target(source, target.object)? {
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
/// directory instead of reading the bitmap between them.
pub(crate) struct LayerCursor {
    file: File,
    bitmap_offset: u64,
    position: u64,
    end: u64,
    /// Set bits not yet returned.
    remaining: u64,
    /// The next rank, for select mode.
    rank: u64,
    /// Present in select mode: the layer's rank directory, in memory.
    directory: Option<crate::permutation::CachedBitmap>,
    word: u64,
    reverse: PackedStream,
}

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
        let layer = index.layer(source, order)?;
        let file = File::open(index.path())?;
        let start_rank = layer.rank1(&file, range.start)?;
        let end_rank = layer.rank1(&file, range.end)?;
        let matches = end_rank - start_rank;
        let directory = if matches > 0 && (range.end - range.start) / matches > SELECT_DENSITY {
            Some(crate::permutation::CachedBitmap::from_samples(
                layer.bitmap.offset,
                layer.bitmap.entry_count,
                read_samples(&file, &layer.superrank, 8)?,
                read_samples(&file, &layer.subrank, 2)?
                    .into_iter()
                    .map(|v| v as u16)
                    .collect(),
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
            reverse: PackedStream::new(index.path(), &layer.reverse, start_rank)?,
            file,
        })
    }

    pub(crate) fn next(&mut self) -> Result<Option<(u64, u64)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        if let Some(directory) = &self.directory {
            let position = directory.select1(&self.file, self.rank)?;
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
                let mut bytes = [0u8; 8];
                let available = ((self.end - word_start).div_ceil(8)).min(8) as usize;
                self.file
                    .read_exact_at(&mut bytes[..available], self.bitmap_offset + word_start / 8)?;
                self.word = u64::from_le_bytes(bytes) & (u64::MAX << offset_in_word);
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
            return Ok(Some((position, self.reverse.next()?)));
        }
    }
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
