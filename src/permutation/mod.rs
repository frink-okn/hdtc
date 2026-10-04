//! Memory-mapped permutation index (`.hdt.perm`).
//!
//! The normative wire format is `docs/permutation-index-format.md`.  This
//! module owns construction, opening, query access, and strict validation so
//! those four paths cannot drift on section identifiers or sizing rules.

mod builder;
pub(crate) mod format;
mod reader;

pub(crate) use builder::{
    PermEntry, PreparedPermutationAssembler, RawBitmap, build_rank_directories, scan_hdt,
};
pub use builder::{
    PermutationCollector, PositionMaps, create_permutation_index, finish_prepared_index,
};
pub use format::{Header, PermutationComponent, PermutationSectionKind, Section, canonical_path};
pub(crate) use reader::{CachedBitmap, ObjectGroups, SpoAccess};
pub use reader::{PermutationIndex, PermutationIndexOpenError, validate_permutation_index};
