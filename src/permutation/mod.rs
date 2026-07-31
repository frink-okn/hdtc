//! Memory-mapped permutation index (`.hdt.perm`).
//!
//! The normative wire format is `docs/permutation-index-format.md`.  This
//! module owns construction, opening, query access, and strict validation so
//! those four paths cannot drift on section identifiers or sizing rules.

mod builder;
mod format;
mod reader;

pub(crate) use builder::{PermEntry, PreparedPermutationAssembler, scan_hdt};
pub use builder::{
    PermutationCollector, PositionMaps, create_permutation_index, finish_prepared_index,
};
pub use format::{Header, Section, canonical_path};
pub use reader::{PermutationIndex, validate_permutation_index};
