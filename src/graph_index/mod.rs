//! Derived index over an HDT graphs sidecar (`.hdt.graphs.idx`).
//!
//! The normative wire format is `docs/graphs-index-format.md`.

mod builder;
mod reader;

pub use builder::{
    GraphIndexOptions, PreparedGraphIndexCollector, canonical_path, create_graph_index,
    finish_prepared_graph_index,
};
#[allow(unused_imports)]
pub use reader::{GraphIndex, GraphIndexOpenError, GraphIndexSpace, validate_graph_index};
