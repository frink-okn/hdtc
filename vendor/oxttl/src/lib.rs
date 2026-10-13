#![doc = include_str!("../README.md")]
#![doc(test(attr(deny(warnings))))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/oxigraph/oxigraph/main/logo.svg")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/oxigraph/oxigraph/main/logo.svg")]

mod chunker;
mod lexer;
mod line_formats;
pub mod n3;
pub mod nquads;
pub mod ntriples;
mod terse;
mod toolkit;
pub mod trig;
pub mod turtle;

pub use crate::n3::N3Parser;
pub use crate::nquads::{NQuadsParser, NQuadsSerializer};
pub use crate::ntriples::{NTriplesParser, NTriplesSerializer};
pub use crate::toolkit::{BufferLimitExceeded, TextPosition, TurtleParseError, TurtleSyntaxError};
pub use crate::trig::{TriGParser, TriGSerializer};
pub use crate::turtle::{TurtleParser, TurtleSerializer};

pub(crate) const MIN_BUFFER_SIZE: usize = 4096;
/// Default upper bound on the lexer buffer in bytes, and so on the largest
/// single IRI or literal a parser accepts. Each parser's
/// `with_max_buffer_size` overrides it (hdtc backport of oxigraph de4dc5f04).
pub(crate) const DEFAULT_MAX_BUFFER_SIZE: usize = 4096 * 4096;
#[expect(clippy::decimal_literal_representation)]
pub(crate) const MIN_PARALLEL_CHUNK_SIZE: usize = 16384;

/// Deepest RDF 1.2 triple-term nesting the parsers accept (hdtc change 7).
/// Nested terms are boxed, and their `Clone` and `Drop` recurse, so unbounded
/// nesting overflows the stack long before any term-size bound applies. The
/// parsers count open triple terms on their own state stacks, which error
/// recovery clears, so a malformed statement cannot leak depth into the next.
pub const MAX_TRIPLE_TERM_NESTING: usize = 64;

/// The error a statement nesting deeper than [`MAX_TRIPLE_TERM_NESTING`] gets.
pub(crate) fn triple_term_nesting_error() -> String {
    format!("triple term nested more than {MAX_TRIPLE_TERM_NESTING} levels deep")
}

/// How many triple terms `term` nests: 0 unless it is one. Triple terms nest
/// only in object position, so this follows a single chain.
///
/// Reifying a triple makes it a triple term, one level deeper than its
/// object, so a statement reifying a triple whose object is already at the
/// limit must be refused where the reifier is written (hdtc change 7).
#[cfg(feature = "rdf-12")]
pub(crate) fn triple_term_depth(mut term: &oxrdf::Term) -> usize {
    let mut depth = 0;
    while let oxrdf::Term::Triple(triple) = term {
        depth += 1;
        term = &triple.object;
    }
    depth
}
