//! External merge sort and parallel merge tree for large datasets.
//!
//! Provides:
//! 1. `ExternalSorter` — accumulates items, sorts chunks to disk, k-way merges
//! 2. `parallel_merge` — reusable parallel binary merge tree with bounded fan-in

pub(crate) mod external;
pub(crate) mod parallel_merge;

pub use external::{ExternalSorter, Sortable};
