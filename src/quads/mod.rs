//! Named-graph memberships and the packed `.hdt.graphs` sidecar.

mod assignment;
mod export;
mod id_quad;
mod reader;
mod search;
mod writer;

pub use assignment::{GraphAssignments, SourceGraphAssignment};
pub use export::export_dataset_nquads;
pub use id_quad::{GraphMembership, IdQuad, PositionGraphMembership, QuadUnionIterator};
pub use reader::{GraphSidecarReader, GraphTerm};
pub use search::search_dataset_streaming;
pub use writer::{canonical_sidecar_path, write_graph_sidecar};
