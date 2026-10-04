//! RDF 1.2 triple terms in a standard HDT.
//!
//! A triple term is stored in `data.hdt` as an IRI in a reserved namespace
//! ([`iri`]), so the HDT stays readable by every HDT tool. What each IRI
//! denotes is kept beside it, split by whether the triple is asserted:
//!
//! - an **asserted** triple term's proposition is already a triple of
//!   `data.hdt`, so it is recorded only as a position there;
//! - an **unasserted** one's proposition is stored as a triple of a second,
//!   standard HDT, `data.hdt.props.hdt`, which holds nothing else.
//!
//! The triple-term index `data.hdt.tt` ties the two together: a map from each
//! triple-term IRI to its proposition, a layer marking the reified triples of
//! `data.hdt` in each of SPO, POS and OPS order, and the reverse maps that take
//! a matched proposition back to its IRI. `docs/triple-terms-format.md` is the
//! normative specification.

pub mod build;
pub mod format;
pub mod iri;
pub mod reader;
pub mod spool;
pub mod validate;

pub use iri::{Component, Proposition, PropositionRecord, TT_NAMESPACE};
