//! External merge sort and parallel merge tree for large datasets.
//!
//! Provides:
//! 1. `ExternalSorter` — accumulates items, sorts chunks to disk, k-way merges
//! 2. `parallel_merge` — reusable parallel binary merge tree with bounded fan-in
//! 3. `parallel_sort` — the in-memory chunk sort, with data-independent stack depth

pub(crate) mod external;
pub(crate) mod parallel_merge;
pub(crate) mod parallel_sort;

pub(crate) use external::MergeIterator;
pub use external::{ExternalSorter, Sortable};
