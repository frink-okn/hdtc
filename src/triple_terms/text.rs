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
//!          →  its SPO position → layer bit → reverse entry: the IRI's id
//!          →  (r, q) in data.hdt's OPS group of that id
//! row:     r q <<( s p "literal" )>>
//! ```

use super::format;
use super::reader::{Layer, Order, Source, TripleTermIndex};
use crate::hdt::reader::open_hdt;
use crate::permutation::{ObjectGroups, PermutationIndex, SpoAccess, canonical_path};
use anyhow::{Context, Result};
use std::path::Path;

/// Resolves hits on propositions literals to `data.hdt` rows.
pub(crate) struct PropositionHits {
    /// Document IDs above this address the propositions HDT.
    pub object_offset: u64,
    index: TripleTermIndex,
    layer: Layer,
    props_groups: ObjectGroups,
    props_spo: SpoAccess,
    data_groups: ObjectGroups,
    /// The inner predicate to keep, as a propositions-HDT identifier.
    /// `Some(None)`: a predicate was asked for and the propositions HDT does
    /// not use it, so nothing matches.
    predicate: Option<Option<u64>>,
}

impl PropositionHits {
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
        let predicate = match predicate {
            None => None,
            Some(iri) => Some(
                open_hdt(&props_path, memory_limit / 16)?
                    .1
                    .locate_predicate(iri.as_bytes())?,
            ),
        };
        Ok(Self {
            object_offset,
            layer: index.layer(Source::Propositions, Order::Spo)?,
            props_groups: ObjectGroups::open(&props_perm)?,
            props_spo: SpoAccess::open(&props_perm, &props_path)?,
            data_groups: ObjectGroups::open(&data_perm)?,
            predicate,
            index,
        })
    }

    /// Whether a document ID addresses the propositions HDT.
    pub(crate) fn covers(&self, document: u64) -> bool {
        document > self.object_offset
    }

    /// The `data.hdt` rows `(r, q, triple term)` for one hit, and how many
    /// proposition occurrences were examined to find them.
    pub(crate) fn rows(&mut self, document: u64, out: &mut Vec<(u64, u64, u64)>) -> Result<u64> {
        let literal = document - self.object_offset;
        let predicate = match self.predicate {
            Some(None) => return Ok(0),
            Some(Some(p)) => Some(p),
            None => None,
        };
        let mut pairs = Vec::new();
        let examined = self
            .props_groups
            .for_object(literal, predicate, false, |s, p| {
                pairs.push((s, p));
                Ok(true)
            })?;
        for (s, p) in pairs {
            let position = self.props_spo.range(s, Some(p), Some(literal))?;
            if position.is_empty() || !self.layer.bit(self.index.file(), position.start)? {
                // Named only from inside another proposition: no data.hdt
                // triple has it as object.
                continue;
            }
            let rank = self.layer.rank1(self.index.file(), position.start)?;
            let term = self.layer.reverse(self.index.file(), rank)?;
            self.data_groups.for_object(term, None, false, |r, q| {
                out.push((r, q, term));
                Ok(true)
            })?;
        }
        Ok(examined)
    }
}
