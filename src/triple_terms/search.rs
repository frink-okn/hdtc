//! Patterns with variables inside a triple term: `S P <<( s p o )>>`.
//!
//! A ground triple term needs nothing here — it is its IRI, and the ordinary
//! search finds it. This module answers the other case, where some of the
//! inner positions are wildcards, with one of two plans:
//!
//! - **Inner first** (the outer subject is unbound). The inner pattern is a
//!   range in SPO, POS or OPS of each HDT that holds propositions, and the
//!   triple-term index's layer marks which positions in it are reified; the
//!   reverse array names each one's IRI. Each IRI is then a `? ? O` (or
//!   `? P O`) lookup in `data.hdt`'s permutation index. Work is proportional to
//!   the matching triple terms, never to the range.
//! - **Outer first** (the outer subject is bound). The subject's triples are
//!   few; each triple-term object among them is decoded and tested.
//!
//! Both stream: rows are written as they are found, and memory does not grow
//! with the result.

use super::format;
use super::reader::{LayerCursor, Order, Source, Target, TripleTermIndex};
use crate::hdt::reader::{DictionaryResolver, OutputWriter, open_hdt, write_triple_tab};
use crate::permutation::{ObjectGroups, PermutationIndex, canonical_path};
use anyhow::{Context, Result};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

/// The inner positions of a triple-term pattern, as dictionary strings; `None`
/// is a wildcard. A nested triple term in object position is given by its IRI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InnerPattern {
    pub subject: Option<Vec<u8>>,
    pub predicate: Option<Vec<u8>>,
    pub object: Option<Vec<u8>>,
}

/// The inner pattern resolved to one HDT's identifiers.
#[derive(Debug, Clone, Copy)]
struct Resolved {
    subject: Option<u64>,
    predicate: Option<u64>,
    object: Option<u64>,
}

impl Resolved {
    fn matches(&self, target: &Target) -> bool {
        self.subject.is_none_or(|s| s == target.subject)
            && self.predicate.is_none_or(|p| p == target.predicate)
            && self.object.is_none_or(|o| o == target.object)
    }
}

/// Resolve the bound inner positions in one dictionary; `None` when one of
/// them does not occur there, so nothing in that HDT can match.
fn resolve(dictionary: &mut DictionaryResolver, inner: &InnerPattern) -> Result<Option<Resolved>> {
    let subject = match &inner.subject {
        Some(term) => match dictionary.locate_subject(term)? {
            Some(id) => Some(id),
            None => return Ok(None),
        },
        None => None,
    };
    let predicate = match &inner.predicate {
        Some(term) => match dictionary.locate_predicate(term)? {
            Some(id) => Some(id),
            None => return Ok(None),
        },
        None => None,
    };
    let object = match &inner.object {
        Some(term) => match dictionary.locate_object(term)? {
            Some(id) => Some(id),
            None => return Ok(None),
        },
        None => None,
    };
    Ok(Some(Resolved {
        subject,
        predicate,
        object,
    }))
}

/// Writes matching rows, applying offset and limit.
struct Emitter<'a> {
    writer: &'a mut OutputWriter,
    count_only: bool,
    offset: u64,
    limit: Option<u64>,
    count: u64,
    subject: Vec<u8>,
    predicate: Vec<u8>,
    object: Vec<u8>,
}

impl Emitter<'_> {
    fn full(&self) -> bool {
        self.limit.is_some_and(|limit| self.count >= limit)
    }

    /// Count `n` rows without writing them (`--count`).
    fn add(&mut self, n: u64) {
        let skipped = n.min(self.offset);
        self.offset -= skipped;
        self.count += n - skipped;
    }

    /// Emit one row of `data.hdt`; returns false once the limit is reached.
    fn emit(
        &mut self,
        dictionary: &mut DictionaryResolver,
        triple: (u64, u64, u64),
    ) -> Result<bool> {
        if self.offset > 0 {
            self.offset -= 1;
            return Ok(true);
        }
        self.count += 1;
        if !self.count_only {
            dictionary.subject_term(triple.0, &mut self.subject)?;
            dictionary.predicate_term(triple.1, &mut self.predicate)?;
            dictionary.object_term(triple.2, &mut self.object)?;
            write_triple_tab(self.writer, &self.subject, &self.predicate, &self.object)?;
        }
        Ok(!self.full())
    }
}

/// One HDT that holds propositions: `data.hdt` or the propositions HDT.
struct PropositionSource {
    source: Source,
    hdt: PathBuf,
    perm: PermutationIndex,
    dictionary: Option<DictionaryResolver>,
}

/// Answer `S P <<( inner )>>`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn search_inner_pattern(
    hdt_path: &Path,
    dictionary: &mut DictionaryResolver,
    outer_subject: Option<&[u8]>,
    outer_predicate: Option<&[u8]>,
    inner: &InnerPattern,
    writer: &mut OutputWriter,
    count_only: bool,
    offset: Option<u64>,
    limit: Option<u64>,
    memory_limit: usize,
) -> Result<u64> {
    let mut emitter = Emitter {
        writer,
        count_only,
        offset: offset.unwrap_or(0),
        limit,
        count: 0,
        subject: Vec::new(),
        predicate: Vec::new(),
        object: Vec::new(),
    };
    if !format::canonical_path(hdt_path).exists() {
        // No triple terms at all, so nothing can match.
        return Ok(0);
    }
    let index = TripleTermIndex::open(hdt_path)?;
    let outer_subject = match outer_subject {
        Some(term) => match dictionary.locate_subject(term)? {
            Some(id) => Some(id),
            None => return Ok(0),
        },
        None => None,
    };
    let outer_predicate = match outer_predicate {
        Some(term) => match dictionary.locate_predicate(term)? {
            Some(id) => Some(id),
            None => return Ok(0),
        },
        None => None,
    };
    let data_perm = PermutationIndex::open(&canonical_path(hdt_path), hdt_path)
        .context("triple-term patterns need the permutation index")?;
    let props_path = format::propositions_path(hdt_path);
    let mut props_dictionary = if index.has_propositions() {
        Some(open_hdt(&props_path, memory_limit / 4)?.1)
    } else {
        None
    };

    // Outer first: the subject's triples, each triple-term object tested.
    if let Some(subject) = outer_subject {
        let data_resolved = resolve(dictionary, inner)?;
        let props_resolved = match props_dictionary.as_mut() {
            Some(props) => resolve(props, inner)?,
            None => None,
        };
        for triple in data_perm.spo_triples(hdt_path, subject, outer_predicate)? {
            let Some(target) = index.target(Source::Data, triple.2)? else {
                continue;
            };
            let resolved = if target.asserted {
                data_resolved
            } else {
                props_resolved
            };
            if resolved.is_some_and(|r| r.matches(&target)) && !emitter.emit(dictionary, triple)? {
                break;
            }
        }
        return Ok(emitter.count);
    }

    let mut groups = ObjectGroups::open(&data_perm)?;

    // Every inner position a wildcard: the triples whose object is any triple
    // term, one contiguous range of OPS, since triple-term IRIs are one run of
    // object identifiers.
    if inner.subject.is_none() && inner.predicate.is_none() && inner.object.is_none() {
        let range = groups.objects_range(index.run(Source::Data))?;
        if count_only && outer_predicate.is_none() {
            emitter.add(range.end - range.start);
            return Ok(emitter.count);
        }
        for triple in groups.range_triples(&canonical_path(hdt_path), range)? {
            let triple = triple?;
            if outer_predicate.is_some_and(|p| p != triple.1) {
                continue;
            }
            if count_only {
                emitter.add(1);
            } else if !emitter.emit(dictionary, triple)? {
                break;
            }
        }
        return Ok(emitter.count);
    }

    // Inner first, over each HDT that holds propositions.
    let mut sources = vec![PropositionSource {
        source: Source::Data,
        hdt: hdt_path.to_path_buf(),
        perm: data_perm.clone(),
        dictionary: None,
    }];
    if let Some(props) = props_dictionary.take() {
        sources.push(PropositionSource {
            source: Source::Propositions,
            perm: PermutationIndex::open(&canonical_path(&props_path), &props_path)?,
            hdt: props_path,
            dictionary: Some(props),
        });
    }
    'sources: for mut source in sources {
        let resolved = match source.dictionary.as_mut() {
            Some(props) => resolve(props, inner)?,
            None => resolve(dictionary, inner)?,
        };
        let Some(resolved) = resolved else {
            continue;
        };
        let triples = match source.source {
            Source::Data => index.header().data.triples,
            Source::Propositions => index.header().props.triples,
        };
        // The inner pattern's range, and whether S?O needs an object probe.
        let (order, range, probe_object) =
            match (resolved.subject, resolved.predicate, resolved.object) {
                (Some(s), p, o) => {
                    let narrowed = p.is_some();
                    (
                        Order::Spo,
                        source.perm.spo_range(
                            &source.hdt,
                            s,
                            p,
                            if narrowed { o } else { None },
                        )?,
                        if narrowed { None } else { o },
                    )
                }
                (None, Some(p), o) => (Order::Pos, source.perm.index_range(Some(p), o)?.1, None),
                (None, None, Some(o)) => {
                    (Order::Ops, source.perm.index_range(None, Some(o))?.1, None)
                }
                (None, None, None) => (Order::Spo, 0..triples, None),
            };
        let objects = match probe_object {
            Some(_) => Some(ArrayZ::open(&source.hdt)?),
            None => None,
        };
        let mut cursor = LayerCursor::new(&index, source.source, order, range)?;
        while let Some((position, term)) = cursor.next()? {
            if let (Some(object), Some(objects)) = (probe_object, objects.as_ref())
                && objects.get(position)? != object
            {
                continue;
            }
            if count_only {
                emitter.add(groups.for_object(term, outer_predicate, true, |_, _| Ok(true))?);
                continue;
            }
            let mut more = true;
            groups.for_object(term, outer_predicate, false, |subject, predicate| {
                more = emitter.emit(dictionary, (subject, predicate, term))?;
                Ok(more)
            })?;
            if !more {
                break 'sources;
            }
        }
    }
    Ok(emitter.count)
}

/// Random reads of an HDT's `ArrayZ`: the object at an SPO position.
struct ArrayZ {
    file: File,
    offset: u64,
    width: u8,
}

impl ArrayZ {
    fn open(hdt: &Path) -> Result<Self> {
        let mut reader = BufReader::new(File::open(hdt)?);
        let sections = crate::hdt::sections::scan_hdt_sections(&mut reader)?;
        Ok(Self {
            file: reader.into_inner(),
            offset: sections.array_z.data_start,
            width: sections.array_z.bits_per_entry,
        })
    }

    fn get(&self, index: u64) -> Result<u64> {
        use std::os::unix::fs::FileExt;
        let width = u64::from(self.width);
        if width == 0 {
            return Ok(0);
        }
        let bit = index * width;
        let needed = ((bit % 8) + width).div_ceil(8) as usize;
        let mut bytes = [0u8; 9];
        self.file
            .read_exact_at(&mut bytes[..needed], self.offset + bit / 8)?;
        let mut value = 0u128;
        for (i, byte) in bytes[..needed].iter().enumerate() {
            value |= u128::from(*byte) << (i * 8);
        }
        Ok(((value >> (bit % 8)) & ((1u128 << width) - 1)) as u64)
    }
}
