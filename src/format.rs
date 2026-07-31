//! The on-disk format layer, re-exported for downstream crates.
//!
//! # What this module is for
//!
//! hdtc writes bundles; other programs read them. The read side that matters
//! most — KGF's `kgf-store` (KGF doc 20) — memory-maps the same files hdtc
//! streams, so it shares hdtc's *format knowledge* while its *access code* is
//! necessarily different: hdtc's readers seek a `File` with bounded memory,
//! while a server maps files read-only and addresses fixed-width structures in
//! place. Same bytes, opposite memory model.
//!
//! This module draws the line between the two. Everything here is knowledge a
//! second implementation would otherwise have to re-derive from the format docs
//! and would eventually get subtly wrong:
//!
//! - **Section location and framing** — control info, VByte, the LogArray and
//!   Bitmap section preambles, the PFC preamble and its block-offset array.
//!   A mapped reader needs these to find regions before it maps anything.
//! - **Identity** — [`sha256_to_end`], so a sidecar's binding to its HDT is
//!   verified by one implementation rather than two.
//! - **Sidecar directories** — [`PermutationHeader`] and [`PermutationSection`]
//!   describe `.hdt.perm`'s regions precisely enough to map them directly.
//! - **Read-side runtime logic that must not be duplicated**, of which the
//!   load-bearing case is the text analyzer. `/search` is correct only if query
//!   analysis is the same code that built the index; a divergence here does not
//!   corrupt anything a checksum would catch, it silently returns the wrong
//!   answers. [`TextSearcher`] exists so that no one is tempted.
//!
//! # Stability
//!
//! The crate's module tree is private and free to change. **This module is the
//! published surface**: additions are routine, removals and signature changes
//! are breaking, and both are semver events for downstream crates. If something
//! you need is missing, add it here rather than widening a module's visibility
//! — the point of the façade is that the list of shared knowledge stays
//! reviewable in one place.
//!
//! # What is deliberately absent
//!
//! Builder entry points, the pipeline, the sorter, and the RDF parsers. Those
//! are the CLI's business. Also absent are readers whose logic a mapped
//! implementation replaces outright rather than reuses; `PermutationIndex`
//! appears here for its directory accessors, not for `triples()`.

// ---------------------------------------------------------------------------
// io primitives — section framing
// ---------------------------------------------------------------------------

pub use crate::io::{
    BitmapReader, ControlInfo, ControlType, LogArrayReader, decode_vbyte, encode_vbyte, read_vbyte,
    skip_bitmap_section, skip_log_array_section,
};

// ---------------------------------------------------------------------------
// HDT dictionary
// ---------------------------------------------------------------------------

pub use crate::dictionary::DictCounts;
pub use crate::hdt::pfc_reader::{PfcSectionHeader, PfcSectionIterator, skip_pfc_section};

/// SHA-256 over the remainder of a reader, the identity digest every sidecar
/// binds to its HDT with (`docs/permutation-index-format.md` §9,
/// `docs/graphs-sidecar-format.md` §10).
pub use crate::hdt::reader::sha256_to_end;

// ---------------------------------------------------------------------------
// Permutation sidecar (.hdt.perm)
// ---------------------------------------------------------------------------

pub use crate::permutation::{
    Header as PermutationHeader, PermutationIndex, Section as PermutationSection,
    canonical_path as permutation_index_path, validate_permutation_index,
};

// ---------------------------------------------------------------------------
// Graphs sidecar index (.hdt.graphs.idx)
// ---------------------------------------------------------------------------

pub use crate::graph_index::{
    GraphIndex, GraphIndexSpace, canonical_path as graph_index_path, validate_graph_index,
};

// ---------------------------------------------------------------------------
// Text index (.hdt.text)
// ---------------------------------------------------------------------------

pub use crate::text::{
    DEFAULT_MAX_LITERAL_BYTES, DEFAULT_UNTAGGED_LANGUAGE, DatatypeExclusions, MatchKind, MatchMode,
    TEXT_INDEX_SUFFIX, TextHit, TextQuery, TextSearcher, default_text_index_path,
    normalize_language,
};
