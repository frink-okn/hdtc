mod input;
pub(crate) mod parser;
pub(crate) mod serializer;

pub use input::{discover_inputs, RdfInput};
pub use parser::{stream_quads_with_options, ExtractedQuad, ParseOptions};
pub(crate) use serializer::serialize_triples;
