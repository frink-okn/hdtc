//! Reading the triple-term index, and rendering triple terms as N-Triples 1.2.
//!
//! This is the seek-based reader for this crate's CLI, in the style of the
//! permutation index's: every access is a positioned read, nothing is loaded
//! whole, and memory does not grow with the index. Mapped readers (KGF's
//! `kgf-store`) use [`TripleTermIndex::sections`] and the format instead.

use super::format::{self, Header, Section, component, kind, section_type};
use super::iri::TT_NAMESPACE;
use crate::hdt::reader::{DictionaryResolver, open_hdt, write_nt_object, write_nt_subject};
use crate::permutation::format::{SUBBLOCK_BITS, SUPERBLOCK_BITS};
use crate::permutation::scan_hdt;
use anyhow::{Context, Result, ensure};
use std::fs::File;
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
        }
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

/// Nesting deeper than this is refused rather than followed: a well-formed
/// index has no cycles, and this bounds the damage of one that does.
const MAX_DEPTH: usize = 64;

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
            let shared = data.shared.prefix_range(TT_NAMESPACE.as_bytes())?;
            let objects = data.objects.prefix_range(TT_NAMESPACE.as_bytes())?;
            ensure!(
                shared.is_empty() && objects.is_empty(),
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
