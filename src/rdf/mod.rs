pub(crate) mod input;
pub(crate) mod parser;
pub(crate) use parser::{HeaderCounts, header_counts, header_triples};
pub(crate) mod serializer;

pub use input::{RdfInput, discover_inputs};
pub use parser::{DEFAULT_MAX_TERM_BYTES, ExtractedQuad, ParseOptions, stream_quads_with_options};
pub(crate) use serializer::serialize_triples;
