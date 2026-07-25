//! RDF vocabulary for the HDT header metadata block — the single source of
//! truth shared by the writer (which *emits* these triples on `create`) and the
//! `header` command (which must recognize them as data-derived / "managed").
//!
//! Keeping these in one place prevents the two sides from drifting: if a new
//! data-derived statistic is ever emitted, adding it to [`VOID_STAT_LOCALS`]
//! teaches `header --replace` to keep it (and to reject user attempts to set
//! it) automatically.

/// The void namespace.
pub(crate) const VOID_NS: &str = "http://rdfs.org/ns/void#";
/// The HDT namespace (every predicate/class under it is hdtc-managed).
pub(crate) const HDT_NS: &str = "http://purl.org/HDT/hdt#";
/// `rdf:type`.
pub(crate) const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
/// `hdt:Dataset` — the class the dataset subject is typed with.
pub(crate) const HDT_DATASET: &str = "http://purl.org/HDT/hdt#Dataset";
/// `void:Dataset` — the dataset subject is typed with this too (Java style).
pub(crate) const VOID_DATASET: &str = "http://rdfs.org/ns/void#Dataset";

/// Local names (within [`VOID_NS`]) of the data-derived void statistics the
/// writer emits on the dataset subject. The `header` command treats these as
/// reserved: they are preserved on `--replace` and may not be set from user
/// input. The writer builds its statistic triples by iterating this list, so it
/// cannot emit a void statistic the `header` command doesn't know about.
pub(crate) const VOID_STAT_LOCALS: &[&str] = &[
    "triples",
    "properties",
    "distinctSubjects",
    "distinctObjects",
];
