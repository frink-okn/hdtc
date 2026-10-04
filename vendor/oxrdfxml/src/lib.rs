#![doc = include_str!("../README.md")]
#![doc(test(attr(deny(warnings))))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/oxigraph/oxigraph/main/logo.svg")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/oxigraph/oxigraph/main/logo.svg")]

mod error;
mod parser;
mod serializer;
mod utils;

pub use error::{RdfXmlParseError, RdfXmlSyntaxError};

/// Deepest RDF 1.2 triple-term nesting the parser accepts (hdtc change 1).
/// Nested terms are boxed, and their `Clone` and `Drop` recurse, so unbounded
/// nesting overflows the stack long before any input-size bound applies.
pub const MAX_TRIPLE_TERM_NESTING: usize = 64;

/// How many triple terms `term` nests: 0 unless it is one. Triple terms nest
/// only in object position, so this follows a single chain.
#[cfg(feature = "rdf-12")]
fn triple_term_depth(mut term: &oxrdf::Term) -> usize {
    let mut depth = 0;
    while let oxrdf::Term::Triple(triple) = term {
        depth += 1;
        term = &triple.object;
    }
    depth
}

#[cfg(feature = "rdf-12")]
fn triple_term_nesting_error() -> RdfXmlSyntaxError {
    RdfXmlSyntaxError::msg(format!(
        "triple term nested more than {MAX_TRIPLE_TERM_NESTING} levels deep"
    ))
}
#[cfg(feature = "async-tokio")]
pub use parser::TokioAsyncReaderRdfXmlParser;
pub use parser::{RdfXmlParser, RdfXmlPrefixesIter, ReaderRdfXmlParser, SliceRdfXmlParser};
#[cfg(feature = "async-tokio")]
pub use serializer::TokioAsyncWriterRdfXmlSerializer;
pub use serializer::{RdfXmlSerializer, WriterRdfXmlSerializer};
