//! Full-text search over an HDT's literals.
//!
//! `hdtc text` builds an index whose documents are the **distinct literals** of
//! an HDT's object dictionary, identified by their object dictionary ID, and
//! `hdtc search --text` queries it. `docs/text-index-format.md` is the
//! self-contained, normative description of what gets published and how hdtc
//! queries it. Downstream applications may compose those results into their own
//! entity-oriented APIs without changing this artifact.
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
//! the schema, the analyzer, the exclusion rules, and the manifest. Tantivy
//! decides byte-level compatibility; `docs/text-index-format.md` §1.1 states
//! the tradeoff.

pub(crate) mod analyzer;
pub(crate) mod builder;
pub(crate) mod manifest;
pub(crate) mod schema;
pub(crate) mod searcher;

pub use analyzer::{
    DEFAULT_MAX_LITERAL_BYTES, DEFAULT_UNTAGGED_LANGUAGE, DatatypeExclusions, normalize_language,
};
pub use builder::{TextConfig, create_text_index};
pub use manifest::{LanguageCount, TextManifest};
pub use searcher::{
    MatchKind, MatchMode, TextHit, TextMatchPage, TextQuery, TextScanPosition, TextSearch,
    TextSearcher,
};

use std::path::{Path, PathBuf};

/// Fail unless the index at `dir` was built from the HDT at `hdt_path`.
///
/// **Costs a pass over the HDT's payload**, because that is what the binding
/// is: the manifest records a SHA-256 over the dictionary-and-triples suffix,
/// and the index holds no cheaper witness to compare instead. So this belongs
/// where the other whole-file checks go — publication, ingest, `hdtc verify` —
/// and not on a query path.
///
/// Worth stating that this is a weaker position than the other
/// dictionary-derived sidecars are in. `.hdt.perm` records dictionary counts, a
/// triple count and a suffix length, so
/// [`PermutationIndex::open`](crate::permutation::PermutationIndex::open)
/// rejects a foreign sidecar for the price of a header read, before it answers
/// anything. A text index has no equivalent, so a consumer that must not answer
/// from a foreign index has to be *told* its bundle was verified rather than
/// establish it at open.
pub fn verify_text_index_binding(dir: &Path, hdt_path: &Path) -> anyhow::Result<()> {
    use anyhow::{Context, ensure};

    let manifest = manifest::TextManifest::read(dir).with_context(|| {
        format!(
            "Failed to read the text index manifest in {}",
            dir.display()
        )
    })?;
    let digest = crate::hdt::reader::hdt_data_digest(hdt_path)?;
    ensure!(
        digest == manifest.source_digest,
        "Text index {} was not built from {}: source SHA-256 binding mismatch",
        dir.display(),
        hdt_path.display()
    );
    Ok(())
}

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
