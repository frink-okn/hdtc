//! Named-graph memberships and the packed `.hdt.graphs` sidecar.

mod assignment;
mod export;
mod id_quad;
mod reader;
mod search;
pub(crate) mod transpose;
pub(crate) mod writer;

pub use assignment::{GraphAssignments, SourceGraphAssignment};
pub use export::export_dataset_nquads;
pub use id_quad::{GraphMembership, IdQuad, PositionGraphMembership, QuadUnionIterator};
pub use reader::{
    ELIAS_FANO_HEADER_SIZE, ELIAS_FANO_SUBBLOCK_BITS, ELIAS_FANO_SUPERBLOCK_BITS, EliasFanoHeader,
    GRAPH_ARRAY_CONTAINER_MAX, GRAPH_BITMAP_CONTAINER_BYTES, GRAPH_BITMAP_CONTAINER_SUBBLOCK_BITS,
    GRAPH_BITMAP_CONTAINER_SUBRANK_BYTES, GRAPH_CHUNK_ENTRY_SIZE, GRAPH_LAYER_ENTRY_SIZE,
    GRAPH_POSITION_CHUNK_SHIFT, GraphChunkContainer, GraphChunkEntry, GraphLayerEncoding,
    GraphLayerEntry, GraphSidecarDirectory, GraphSidecarHeader, GraphSidecarReader, GraphTerm,
};
pub(crate) use reader::{EmbeddedLayerSetReader, LayerMemberIter};
pub use search::search_dataset_streaming;
pub use writer::{canonical_sidecar_path, write_graph_sidecar};
