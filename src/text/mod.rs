//! Full-text search over an HDT's literals.
//!
//! `hdtc text` builds an index whose documents are the **distinct literals** of
//! an HDT's object dictionary, identified by their object dictionary ID, and
//! `hdtc search --text` queries it. `docs/text-index-format.md` is the normative
//! description of what gets published; doc 19 of the KGF design docs is where
//! the design comes from.
//!
//! Two properties shape everything here:
//!
//! - **Every literal is indexed**, subject only to the mechanical exclusions in
//!   [`analyzer`]. There is no configured predicate list, because across
//!   heterogeneous datasets a per-publisher index configuration is a guess that
//!   is wrong somewhere and undiagnosable everywhere. Field filtering is a
//!   query-time concern instead.
//! - **The unit is the distinct literal**, so the index scales with distinct
//!   strings rather than with triples, and a hit is an HDT ID rather than a
//!   materialized string.
//!
//! Unlike the other dictionary-derived artifacts, the published bytes are
//! Tantivy's, not hdtc's; what hdtc specifies is the convention around them —
//! the schema, the analyzer, the exclusion rules, and the manifest — under an
//! exact version pin. `docs/text-index-format.md` §1.1 states the tradeoff.

pub(crate) mod analyzer;
pub(crate) mod builder;
pub(crate) mod manifest;
pub(crate) mod schema;
pub(crate) mod searcher;

pub use analyzer::{
    DEFAULT_MAX_LITERAL_BYTES, DEFAULT_UNTAGGED_LANGUAGE, DatatypeExclusions, normalize_language,
};
pub use builder::{TextConfig, create_text_index};
pub use searcher::{MatchMode, TextHit, TextQuery, TextSearcher};

use std::path::{Path, PathBuf};

/// Directory suffix a text index is published under beside its HDT.
pub const TEXT_INDEX_SUFFIX: &str = ".text";

/// Where `hdtc text` writes, and where `hdtc search --text` looks, when neither
/// is given an explicit path: `<hdt-file>.text` beside the HDT.
pub fn default_text_index_path(hdt_path: &Path) -> PathBuf {
    let mut name = hdt_path.as_os_str().to_os_string();
    name.push(TEXT_INDEX_SUFFIX);
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_index_sits_beside_its_hdt() {
        assert_eq!(
            default_text_index_path(Path::new("/data/ubergraph.hdt")),
            PathBuf::from("/data/ubergraph.hdt.text")
        );
        assert_eq!(
            default_text_index_path(Path::new("data.hdt")),
            PathBuf::from("data.hdt.text")
        );
    }
}
