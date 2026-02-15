mod input;
pub(crate) mod parser;

pub use input::{discover_inputs, RdfInput};
pub use parser::stream_quads;
