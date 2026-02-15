//! External merge sort for large datasets that don't fit in memory.
//!
//! Provides a generic external sort that:
//! 1. Accumulates items in memory up to a configurable limit
//! 2. Sorts and flushes chunks to temporary files
//! 3. K-way merges the sorted chunks into a single sorted output

pub(crate) mod external;

pub use external::{ExternalSorter, Sortable};
