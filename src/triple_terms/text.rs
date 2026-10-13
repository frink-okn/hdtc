//! Text-search hits on literals of unasserted triple terms.
//!
//! A text index over an HDT with a propositions HDT also covers that HDT's
//! literals (`docs/text-index-format.md` §2.4). A hit there is a literal that
//! occurs only inside triple terms, so the rows it resolves to are not its own
//! occurrences — those are propositions, never asserted — but the triples of
//! `data.hdt` whose object is a triple term containing it:
//!
//! ```text
//! literal  →  (s, p) in the propositions HDT's OPS group
//!          →  its OPS position → layer bit → reverse entry: the IRI's id
//!          →  (r, q) in data.hdt's OPS group of that id
//! row:     r q <<( s p "literal" )>>
//! ```
//!
//! A proposition may also be named only from inside another unasserted one,
//! and so be no object of `data.hdt`; the term naming the outer proposition
//! contains the literal too. Each proposition's name is therefore minted and
//! looked up among the propositions HDT's objects, and every proposition with
//! it as object is followed in turn, so that no literal is out of reach.

use super::format;
use super::iri::{Component, MAX_NESTING, Proposition, iri_for};
use super::reader::{Layer, Order, Source, TripleTermIndex};
use crate::hdt::reader::{DictionaryResolver, open_hdt};
use crate::permutation::{ObjectGroups, PermutationIndex, canonical_path};
use anyhow::{Context, Result};
use std::path::Path;

/// Resolves hits on propositions literals to `data.hdt` rows.
pub(crate) struct PropositionHits {
    /// Document IDs above this address the propositions HDT.
    pub object_offset: u64,
    index: TripleTermIndex,
    /// The propositions HDT's OPS layer, read at the positions its OPS groups
    /// visit.
    layer: Layer,
    props_groups: ObjectGroups,
    data_groups: ObjectGroups,
    /// The propositions HDT's dictionary, when some proposition is named only
    /// inside another: the names to follow are minted from its strings.
    nesting: Option<DictionaryResolver>,
    /// The inner predicate to keep, as a propositions-HDT identifier.
    /// `Some(None)`: a predicate was asked for and the propositions HDT does
    /// not use it, so nothing matches.
    predicate: Option<Option<u64>>,
    scratch: Vec<u8>,
}

impl PropositionHits {
    /// `memory_limit` bounds the propositions HDT's dictionary.
    pub(crate) fn open(
        hdt_path: &Path,
        object_offset: u64,
        predicate: Option<&str>,
        memory_limit: usize,
    ) -> Result<Self> {
        let index = TripleTermIndex::open(hdt_path)?;
        let props_path = format::propositions_path(hdt_path);
        let props_perm = PermutationIndex::open(&canonical_path(&props_path), &props_path)
            .context("propositions HDT permutation index")?;
        let data_perm = PermutationIndex::open(&canonical_path(hdt_path), hdt_path)
            .context("permutation index")?;
        // The propositions HDT's own run holds exactly the names of
        // propositions nested in others.
        let nested = !index.run(Source::Propositions).is_empty();
        let mut dictionary = match nested || predicate.is_some() {
            true => Some(open_hdt(&props_path, memory_limit)?.1),
            false => None,
        };
        let predicate = match (predicate, dictionary.as_mut()) {
            (Some(iri), Some(dictionary)) => Some(dictionary.locate_predicate(iri.as_bytes())?),
            _ => None,
        };
        Ok(Self {
            object_offset,
            layer: index.layer(Source::Propositions, Order::Ops)?,
            props_groups: ObjectGroups::open(&props_perm)?,
            data_groups: ObjectGroups::open(&data_perm)?,
            nesting: dictionary.filter(|_| nested),
            predicate,
            index,
            scratch: Vec::new(),
        })
    }

    /// Whether a document ID addresses the propositions HDT.
    pub(crate) fn covers(&self, document: u64) -> bool {
        document > self.object_offset
    }

    /// Pass the `data.hdt` rows `(r, q, triple term)` of one hit to `visit`,
    /// in order, until it returns false. Returns how many proposition
    /// occurrences of the literal there are, and whether every row was
    /// visited. Nothing is collected: a literal in millions of claims costs
    /// the rows visited, not memory in proportion to them.
    pub(crate) fn visit_rows(
        &mut self,
        document: u64,
        mut visit: impl FnMut(u64, u64, u64) -> Result<bool>,
    ) -> Result<(u64, bool)> {
        self.resolve(document, &mut Sink::Visit(&mut visit))
    }

    /// The number of `data.hdt` rows one hit resolves to, and how many
    /// proposition occurrences of the literal there are. Each triple term's
    /// rows are counted from its group's size, never read.
    pub(crate) fn count_rows(&mut self, document: u64) -> Result<(u64, u64)> {
        let mut rows = 0;
        let (examined, _) = self.resolve(document, &mut Sink::Count(&mut rows))?;
        Ok((examined, rows))
    }

    fn resolve(&mut self, document: u64, sink: &mut Sink<'_>) -> Result<(u64, bool)> {
        let literal = document - self.object_offset;
        let predicate = match self.predicate {
            Some(None) => return Ok((0, true)),
            Some(Some(p)) => Some(p),
            None => None,
        };
        let groups = self.props_groups.predicate_ranges(literal, predicate)?;
        let examined = groups
            .iter()
            .map(|(_, positions)| positions.end - positions.start)
            .sum();
        let object = match self.nesting.is_some() {
            true => Some(self.component(literal, false)?),
            false => None,
        };
        for (p, positions) in groups {
            for position in positions {
                let proposition = match &object {
                    Some(object) => {
                        let s = self.props_groups.subject_at(position)?;
                        Some(self.proposition(s, p, object.clone())?)
                    }
                    None => None,
                };
                if !self.climb(position, proposition, 1, sink)? {
                    return Ok((examined, false));
                }
            }
        }
        Ok((examined, true))
    }

    /// Pass on the rows naming the proposition at OPS `position`, then follow
    /// every unasserted proposition that names it in turn; false once the sink
    /// wants no more. `proposition` is its content, present when there is
    /// nesting to follow; `depth` counts the triple terms from the literal out
    /// to this one.
    fn climb(
        &mut self,
        position: u64,
        proposition: Option<Proposition>,
        depth: usize,
        sink: &mut Sink<'_>,
    ) -> Result<bool> {
        if let Some(rank) = self.layer.rank_if_marked(self.index.file(), position)? {
            let term = self.layer.reverse(self.index.file(), rank)?;
            match sink {
                Sink::Count(rows) => {
                    **rows += self
                        .data_groups
                        .for_object(term, None, true, |_, _, _| Ok(true))?;
                }
                Sink::Visit(visit) => {
                    let mut more = true;
                    self.data_groups.for_object(term, None, false, |r, q, _| {
                        more = visit(r, q, term)?;
                        Ok(more)
                    })?;
                    if !more {
                        return Ok(false);
                    }
                }
            }
        }
        let (Some(proposition), Some(dictionary)) = (proposition, self.nesting.as_mut()) else {
            return Ok(true);
        };
        if depth >= MAX_NESTING {
            return Ok(true);
        }
        let Some(name) = dictionary.locate_object(iri_for(&proposition).as_bytes())? else {
            return Ok(true);
        };
        // A term's containing propositions are few by predicate, though not by
        // subject: only the per-predicate ranges are held.
        let inner = Component::Triple(Box::new(proposition));
        for (p, positions) in self.props_groups.predicate_ranges(name, None)? {
            for position in positions {
                let s = self.props_groups.subject_at(position)?;
                let outer = self.proposition(s, p, inner.clone())?;
                if !self.climb(position, Some(outer), depth + 1, sink)? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// A proposition of the propositions HDT, from its subject and predicate
    /// identifiers and its object.
    fn proposition(
        &mut self,
        subject: u64,
        predicate: u64,
        object: Component,
    ) -> Result<Proposition> {
        let subject = self.component(subject, true)?;
        let dictionary = self
            .nesting
            .as_mut()
            .context("no propositions dictionary")?;
        dictionary.predicate_term(predicate, &mut self.scratch)?;
        Ok(Proposition {
            subject,
            predicate: String::from_utf8(self.scratch.clone())?,
            object,
        })
    }

    /// A subject or object of the propositions HDT as a triple-term component,
    /// read from its dictionary string as every name is.
    fn component(&mut self, id: u64, subject: bool) -> Result<Component> {
        let dictionary = self
            .nesting
            .as_mut()
            .context("no propositions dictionary")?;
        if subject {
            dictionary.subject_term_raw(id, &mut self.scratch)?;
        } else {
            dictionary.object_term_raw(id, &mut self.scratch)?;
        }
        Ok(Component::from_dictionary(String::from_utf8(
            self.scratch.clone(),
        )?))
    }
}

/// Where resolved rows go: counted, or each handed to a visitor that says
/// whether to go on.
enum Sink<'a> {
    Count(&'a mut u64),
    Visit(&'a mut dyn FnMut(u64, u64, u64) -> Result<bool>),
}
