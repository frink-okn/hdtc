//! Patterns with variables inside a triple term: `S P <<( s p o )>>`.
//!
//! A ground triple term needs nothing here — it is its IRI, and the ordinary
//! search finds it. This module answers the other case, where some of the
//! inner positions are wildcards, with one of two plans:
//!
//! - **Inner first.** The inner pattern is a range in SPO, POS or OPS of each
//!   HDT that holds propositions, and the triple-term index's layer marks which
//!   positions in it are reified; the reverse array names each one's IRI. Each
//!   IRI is then one OPS group of `data.hdt`. Work is proportional to the
//!   matching triple terms, never to the range.
//! - **Outer first**, when the outer subject is bound and its group is smaller
//!   than the inner matches: the subject's triples are streamed, and each
//!   triple-term object among them is decoded and tested.
//!
//! Both sizes are known before either plan runs — a group's size and a layer
//! range's set bits are each two directory lookups — so the cheaper is chosen.
//! When every inner position is a wildcard no layer is needed: the triples
//! whose object is a triple term are one contiguous range of OPS, or of POS
//! within a bound outer predicate.
//!
//! Every plan streams: rows are written as they are found, and memory does not
//! grow with the result.

use super::format;
use super::reader::{LayerCursor, Order, Source, Target, TripleTermIndex};
use crate::hdt::reader::{DictionaryResolver, OutputWriter, write_triple_tab};
use crate::permutation::{ObjectGroups, PermutationIndex, SpoAccess, canonical_path};
use anyhow::{Context, Result};
use std::path::Path;

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

/// Writes matching rows, applying offset and limit. With `count_only` the
/// limit does not apply, as for every other counted pattern.
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
        !self.count_only && self.limit.is_some_and(|limit| self.count >= limit)
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

/// One HDT that holds propositions, with the inner pattern resolved there.
struct PropositionSource {
    source: Source,
    triples: u64,
    spo: SpoAccess,
    perm: PermutationIndex,
    resolved: Resolved,
}

impl PropositionSource {
    /// The inner pattern's range in this HDT, and the object to probe for when
    /// the range is a subject's group but the predicate is unbound (`s ? o`).
    fn range(&self) -> Result<(Order, std::ops::Range<u64>, Option<u64>)> {
        let r = self.resolved;
        Ok(match (r.subject, r.predicate, r.object) {
            (Some(s), Some(p), o) => (Order::Spo, self.spo.range(s, Some(p), o)?, None),
            (Some(s), None, o) => (Order::Spo, self.spo.range(s, None, None)?, o),
            (None, Some(p), o) => (Order::Pos, self.perm.index_range(Some(p), o)?.1, None),
            (None, None, Some(o)) => (Order::Ops, self.perm.index_range(None, Some(o))?.1, None),
            (None, None, None) => (Order::Spo, 0..self.triples, None),
        })
    }
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
    let data_spo = SpoAccess::open(&data_perm, hdt_path)?;

    // Every inner position a wildcard: one contiguous range, no layer.
    if inner.subject.is_none() && inner.predicate.is_none() && inner.object.is_none() {
        let run = index.run(Source::Data);
        if let Some(subject) = outer_subject {
            for triple in data_spo.subject_triples(subject, outer_predicate)? {
                let triple = triple?;
                if run.contains(&triple.2) && !emitter.emit(dictionary, triple)? {
                    break;
                }
            }
            return Ok(emitter.count);
        }
        let (groups, range) = match outer_predicate {
            Some(p) => {
                let groups = ObjectGroups::open_pos(&data_perm)?;
                let range = groups.second_range(p, run)?;
                (groups, range)
            }
            None => {
                let groups = ObjectGroups::open(&data_perm)?;
                let range = groups.objects_range(run)?;
                (groups, range)
            }
        };
        if count_only {
            emitter.add(range.end - range.start);
            return Ok(emitter.count);
        }
        for triple in groups.range_triples(range)? {
            if !emitter.emit(dictionary, triple?)? {
                break;
            }
        }
        return Ok(emitter.count);
    }

    // Each HDT that holds propositions, with the inner pattern resolved in it.
    let mut sources = Vec::with_capacity(2);
    if let Some(resolved) = resolve(dictionary, inner)? {
        sources.push(PropositionSource {
            source: Source::Data,
            triples: index.header().data.triples,
            spo: SpoAccess::open(&data_perm, hdt_path)?,
            perm: data_perm.clone(),
            resolved,
        });
    }
    if index.has_propositions() {
        let resolved = match dictionary.propositions_mut() {
            Some(props) => resolve(props, inner)?,
            None => None,
        };
        if let Some(resolved) = resolved {
            let props_path = format::propositions_path(hdt_path);
            let perm = PermutationIndex::open(&canonical_path(&props_path), &props_path)?;
            sources.push(PropositionSource {
                source: Source::Propositions,
                triples: index.header().props.triples,
                spo: SpoAccess::open(&perm, &props_path)?,
                perm,
                resolved,
            });
        }
    }

    // How many triple terms each side would visit.
    let mut planned = Vec::with_capacity(sources.len());
    let mut inner_matches = 0u64;
    for source in sources {
        let (order, range, probe) = source.range()?;
        let layer = index.layer(source.source, order)?;
        inner_matches +=
            layer.rank1(index.file(), range.end)? - layer.rank1(index.file(), range.start)?;
        planned.push((source, order, range, probe));
    }

    // Outer first, when the subject's group is the smaller side.
    if let Some(subject) = outer_subject {
        let group = data_spo.range(subject, outer_predicate, None)?;
        if group.end - group.start <= inner_matches {
            let resolved_for = |source: Source| {
                planned
                    .iter()
                    .find(|(p, ..)| p.source == source)
                    .map(|(p, ..)| p.resolved)
            };
            let (data_resolved, props_resolved) = (
                resolved_for(Source::Data),
                resolved_for(Source::Propositions),
            );
            for triple in data_spo.subject_triples(subject, outer_predicate)? {
                let triple = triple?;
                let Some(target) = index.target(Source::Data, triple.2)? else {
                    continue;
                };
                let resolved = if target.asserted {
                    data_resolved
                } else {
                    props_resolved
                };
                if resolved.is_some_and(|r| r.matches(&target))
                    && !emitter.emit(dictionary, triple)?
                {
                    break;
                }
            }
            return Ok(emitter.count);
        }
    }

    // Inner first.
    let mut groups = ObjectGroups::open(&data_perm)?;
    'sources: for (source, order, range, probe) in planned {
        let mut cursor = LayerCursor::new(&index, source.source, order, range)?;
        while let Some((position, term)) = cursor.next()? {
            if let Some(object) = probe
                && source.spo.object_at(position)? != object
            {
                continue;
            }
            if count_only && outer_subject.is_none() {
                emitter.add(groups.for_object(term, outer_predicate, true, |_, _| Ok(true))?);
                continue;
            }
            let mut more = true;
            groups.for_object(term, outer_predicate, false, |subject, predicate| {
                if outer_subject.is_some_and(|s| s != subject) {
                    return Ok(true);
                }
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
