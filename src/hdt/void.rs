//! VoID (Vocabulary of Interlinked Datasets) statistics computation.
//!
//! Implements a two-pass, ID-based algorithm:
//! - Pass 1: Scan all triples to find `rdf:type` triples; build a `ClassComboIndex` that
//!   maps each subject to its deduplicated class combination (4 bytes per subject).
//! - Pass 2: Full sequential scan to accumulate dataset-level and class-level statistics.
//! - Optional OPS pass: Read the permutation sidecar to count distinct objects per partition.
//! - Serialize results as N-Triples.
//!
//! With the `dataset` graph view, the same passes also describe every graph of the
//! sidecar-backed RDF dataset as a `void:subset`. Each pass joins its scan against the
//! graph memberships transposed into its own position order — the `.graphs` sidecar
//! for SPO, the OPS layer set of `.graphs.idx` for OPS — and feeds each membership to
//! that graph's accumulators. A subset is exactly what a standalone run over the
//! graph's triples would produce; `docs/void-format.md` specifies the output.
//!
//! The algorithm is equivalent to the Python `void-hdt` tool but uses Rust's u64 integer
//! arithmetic throughout, avoiding the integer overflow that affected hdt-cpp on large inputs
//! like Wikidata.

use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result, ensure};
// Partition maps are keyed by dictionary IDs and probed several times per
// triple, so they use hashbrown's small inlinable hasher rather than SipHash.
use hashbrown::HashMap;

use super::reader::{
    BitmapTriplesScanner, DictionaryResolver, HdtSectionOffsets, find_literal_boundary,
    make_writer, open_hdt,
};
use crate::graph_index::{GraphIndex, GraphIndexSpace};
use crate::permutation::{self, PermutationComponent, PermutationIndex};
use crate::quads::transpose::{LayerSource, PositionMajorMemberships, layer_merge_reserve};
use crate::quads::{GraphSidecarReader, GraphTerm, PositionGraphMembership};

// ---------------------------------------------------------------------------
// ClassComboIndex: compact subject→classes mapping via combo deduplication
// ---------------------------------------------------------------------------

/// Compact index mapping each subject ID to its class combination.
///
/// Instead of `HashMap<u64, Vec<u64>>` (~96 bytes per typed subject), this uses
/// 4 bytes per subject (typed or not) via class-combination deduplication.
/// Subjects sharing the same set of `rdf:type` classes (e.g., all `wikibase:Statement`
/// nodes) map to the same combo ID.
///
/// Memory: `4 × nb_subjects` bytes + `O(distinct_combos × avg_classes)` for the lookup table.
///
/// When graph subsets are described, typing is graph-local — a subject typed `Person` in
/// one graph is untyped in another — so a combination is a set of `(graph, class)` pairs
/// rather than of classes. The per-subject cost is unchanged; only the table grows.
struct ClassComboIndex {
    /// Combo ID for each subject, indexed by subject_id (1-based; index 0 unused).
    /// 0 = untyped.
    subject_combos: Vec<u32>,
    /// Sorted class IDs for each combo, across all graphs. `combo_to_classes[combo_id - 1]`
    /// gives the class IDs for `combo_id > 0`.
    combo_to_classes: Vec<Vec<u64>>,
    /// The same combos split by graph. Empty unless graph subsets are described.
    combo_to_graph_classes: Vec<GraphClasses>,
}

/// One combo's `(graph, class)` pairs in sorted order, stored as parallel arrays so that
/// one graph's classes are a contiguous slice.
struct GraphClasses {
    graphs: Vec<u64>,
    classes: Vec<u64>,
}

impl GraphClasses {
    fn from_pairs(pairs: &[(u64, u64)]) -> Self {
        Self {
            graphs: pairs.iter().map(|&(graph, _)| graph).collect(),
            classes: pairs.iter().map(|&(_, class)| class).collect(),
        }
    }

    #[inline]
    fn classes_in(&self, graph: u64) -> &[u64] {
        let start = self.graphs.partition_point(|&g| g < graph);
        let end = self.graphs.partition_point(|&g| g <= graph);
        &self.classes[start..end]
    }
}

/// The class combinations collected by Pass 1, before non-IRI classes are removed.
///
/// Pairs are `(graph, class)`. A union-only run records every class under graph 0.
struct RawClassCombos {
    subject_combos: Vec<u32>,
    combo_pairs: Vec<Vec<(u64, u64)>>,
}

impl RawClassCombos {
    /// Distinct class IDs across all combos.
    fn distinct_class_ids(&self) -> std::collections::HashSet<u64> {
        let mut set = std::collections::HashSet::new();
        for pairs in &self.combo_pairs {
            set.extend(pairs.iter().map(|&(_, class)| class));
        }
        set
    }
}

/// Which dataset a class lookup answers for.
#[derive(Clone, Copy)]
enum ClassScope {
    /// The triples union: a subject's classes from every graph.
    Union,
    /// One graph of the RDF dataset: only that graph's `rdf:type` triples count.
    Graph(u64),
}

impl ClassComboIndex {
    fn new(raw: RawClassCombos, by_graph: bool) -> Self {
        let combo_to_classes = raw
            .combo_pairs
            .iter()
            .map(|pairs| {
                let mut classes: Vec<u64> = pairs.iter().map(|&(_, class)| class).collect();
                classes.sort_unstable();
                classes.dedup();
                classes
            })
            .collect();
        let combo_to_graph_classes = if by_graph {
            raw.combo_pairs
                .iter()
                .map(|pairs| GraphClasses::from_pairs(pairs))
                .collect()
        } else {
            Vec::new()
        };
        Self {
            subject_combos: raw.subject_combos,
            combo_to_classes,
            combo_to_graph_classes,
        }
    }

    /// Look up the classes for a subject ID.
    #[inline]
    fn classes(&self, subject_id: u64, scope: ClassScope) -> &[u64] {
        let combo_id = self
            .subject_combos
            .get(subject_id as usize)
            .copied()
            .unwrap_or(0);
        if combo_id == 0 {
            return &[];
        }
        let combo = combo_id as usize - 1;
        match scope {
            ClassScope::Union => &self.combo_to_classes[combo],
            ClassScope::Graph(graph) => self.combo_to_graph_classes[combo].classes_in(graph),
        }
    }
}

/// Class lookups for one described dataset.
#[derive(Clone, Copy)]
struct ClassLookup<'a> {
    index: &'a ClassComboIndex,
    nb_shared: u64,
    scope: ClassScope,
}

impl<'a> ClassLookup<'a> {
    #[inline]
    fn subject(&self, subject_id: u64) -> &'a [u64] {
        self.index.classes(subject_id, self.scope)
    }

    /// Objects with ID > nb_shared are in the object-only section (literals or
    /// object-only URIs) and can never appear as subjects of rdf:type triples.
    /// Objects in the shared section (ID <= nb_shared) may be typed: look them up
    /// in the class combo index (shared IDs appear as both subjects and objects).
    #[inline]
    fn object(&self, object_id: u64) -> &'a [u64] {
        if object_id <= self.nb_shared {
            self.index.classes(object_id, self.scope)
        } else {
            &[]
        }
    }
}

// ---------------------------------------------------------------------------
// DatatypeIndex: compact object→datatype+language mapping
// ---------------------------------------------------------------------------

/// Compact index mapping each object-only ID to its datatype (and language tag).
///
/// Uses a unified `u16` ID space:
/// - `0` = not a literal (URI / blank node)
/// - `1..D` = non-langString datatypes (xsd:string, xsd:integer, etc.)
/// - `D+1..D+L` = language tags (implicitly `rdf:langString`)
///
/// Only covers object-only IDs (shared-section terms are never literals since
/// literals cannot appear as subjects).
///
/// Memory: 2 bytes per object-only term + small string tables.
struct DatatypeIndex {
    /// Entry ID for each object-only term.
    /// Indexed by `(global_object_id - nb_shared - 1)`.
    object_only_entries: Vec<u16>,
    /// Datatype IRIs for IDs `1..lang_boundary` (index = id - 1).
    datatype_iris: Vec<String>,
    /// Language tags for IDs `lang_boundary..` (index = id - lang_boundary).
    language_tags: Vec<String>,
    /// First language-tag ID = `datatype_iris.len() as u16 + 1`.
    lang_boundary: u16,
    /// Number of shared-section terms (for computing array index).
    nb_shared: u64,
}

impl DatatypeIndex {
    /// Look up the entry ID for a global object ID.
    /// Returns `0` for shared-section IDs, out-of-range IDs, or non-literal objects.
    #[inline]
    fn get(&self, object_id: u64) -> u16 {
        if object_id <= self.nb_shared {
            return 0;
        }
        let idx = (object_id - self.nb_shared - 1) as usize;
        if idx < self.object_only_entries.len() {
            self.object_only_entries[idx]
        } else {
            0
        }
    }

    /// True if the entry ID represents a language tag (not a pure datatype).
    #[inline]
    fn is_language(&self, entry_id: u16) -> bool {
        entry_id >= self.lang_boundary
    }

    /// Get the datatype IRI for a non-language entry ID (1..lang_boundary-1).
    fn datatype_iri(&self, entry_id: u16) -> &str {
        debug_assert!(entry_id > 0 && entry_id < self.lang_boundary);
        &self.datatype_iris[entry_id as usize - 1]
    }

    /// Get the language tag for a language entry ID (lang_boundary..).
    fn language_tag(&self, entry_id: u16) -> &str {
        debug_assert!(entry_id >= self.lang_boundary);
        &self.language_tags[(entry_id - self.lang_boundary) as usize]
    }
}

// ---------------------------------------------------------------------------
// VoID vocabulary constants (raw IRIs, without angle-bracket delimiters)
// ---------------------------------------------------------------------------

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const VOID_DATASET: &str = "http://rdfs.org/ns/void#Dataset";
const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";
const VOID_DISTINCT_SUBJECTS: &str = "http://rdfs.org/ns/void#distinctSubjects";
const VOID_DISTINCT_OBJECTS: &str = "http://rdfs.org/ns/void#distinctObjects";
const VOID_PROPERTIES: &str = "http://rdfs.org/ns/void#properties";
const VOID_PROPERTY_PARTITION: &str = "http://rdfs.org/ns/void#propertyPartition";
const VOID_CLASS_PARTITION: &str = "http://rdfs.org/ns/void#classPartition";
const VOID_PROPERTY: &str = "http://rdfs.org/ns/void#property";
const VOID_CLASS: &str = "http://rdfs.org/ns/void#class";
const VOID_ENTITIES: &str = "http://rdfs.org/ns/void#entities";
const VOID_SUBSET: &str = "http://rdfs.org/ns/void#subset";
const SD_DATASET: &str = "http://www.w3.org/ns/sparql-service-description#Dataset";
const SD_GRAPH_CLASS: &str = "http://www.w3.org/ns/sparql-service-description#Graph";
const SD_NAMED_GRAPH_CLASS: &str = "http://www.w3.org/ns/sparql-service-description#NamedGraph";
const SD_NAMED_GRAPH: &str = "http://www.w3.org/ns/sparql-service-description#namedGraph";
const SD_NAME: &str = "http://www.w3.org/ns/sparql-service-description#name";
const SD_GRAPH: &str = "http://www.w3.org/ns/sparql-service-description#graph";
const VOIDEXT_OBJECT_CLASS_PARTITION: &str = "http://ldf.fi/void-ext#objectClassPartition";
const VOIDEXT_DATATYPE_PARTITION: &str = "http://ldf.fi/void-ext#datatypePartition";
const VOIDEXT_DATATYPE: &str = "http://ldf.fi/void-ext#datatype";
const VOIDEXT_LANGUAGE_PARTITION: &str = "http://ldf.fi/void-ext#languagePartition";
const VOIDEXT_LANGUAGE: &str = "http://ldf.fi/void-ext#language";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

/// The string hashed for untyped (literal or untyped URI) target partitions,
/// matching the Python tool's `"__untyped__"` sentinel.
const UNTYPED_HASH_INPUT: &str = "__untyped__";

/// Partition levels that receive exact distinct subject and object counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionDistinctScope {
    /// Only property partitions directly below the dataset.
    DatasetProperties,
    /// Every emitted class, property, target-class, datatype, and language partition.
    All,
}

/// Which view of the HDT the VoID description covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoidGraphView {
    /// The HDT's triples union only.
    Union,
    /// The union plus one `void:subset` per graph of the sidecar-backed RDF dataset.
    Dataset,
}

/// Options for [`compute_void`].
pub struct VoidOptions<'a> {
    /// IRI of the described dataset; every minted node IRI extends it.
    pub dataset_uri: &'a str,
    /// Output file, or stdout when `None`.
    pub output_path: Option<&'a Path>,
    /// Emit blank nodes instead of minted IRIs for partitions and subsets.
    pub use_blank_nodes: bool,
    /// Soft memory limit for dictionary caches and graph-membership transposes.
    pub memory_limit: usize,
    /// Partition levels that receive exact distinct subject and object counts.
    pub distinct_scope: Option<PartitionDistinctScope>,
    /// Whether graph subsets are described.
    pub graph_view: VoidGraphView,
    /// Directory for the external sort a many-graph transpose falls back to.
    /// A self-cleaning directory under the system temp dir is used when `None`.
    pub temp_dir: Option<&'a Path>,
}

// ---------------------------------------------------------------------------
// Internal data structures
// ---------------------------------------------------------------------------

#[derive(Default)]
struct DistinctStats {
    distinct_subjects: u64,
    distinct_objects: u64,
    last_subject: u64,
    last_object: u64,
}

impl DistinctStats {
    /// Record a triple from the SPO scan. IDs are monotonic in subject order, so
    /// one last-seen scalar is enough to count distinct subjects exactly. The
    /// count, rather than an ID sentinel, records whether a first ID was seen.
    #[inline]
    fn add_subject(&mut self, subject_id: u64) {
        if self.distinct_subjects == 0 || self.last_subject != subject_id {
            self.last_subject = subject_id;
            self.distinct_subjects += 1;
        }
    }

    /// Record membership from the OPS scan. IDs are monotonic in object order,
    /// so one last-seen scalar is enough to count distinct objects exactly. The
    /// count, rather than an ID sentinel, records whether a first ID was seen.
    #[inline]
    fn add_object(&mut self, object_id: u64) {
        if self.distinct_objects == 0 || self.last_object != object_id {
            self.last_object = object_id;
            self.distinct_objects += 1;
        }
    }
}

#[derive(Default)]
struct DistinctObjectStats {
    distinct_objects: u64,
    last_object: u64,
}

impl DistinctObjectStats {
    #[inline]
    fn add_object(&mut self, object_id: u64) {
        if self.distinct_objects == 0 || self.last_object != object_id {
            self.last_object = object_id;
            self.distinct_objects += 1;
        }
    }
}

#[derive(Default)]
struct PropPartitionData {
    triple_count: u64,
    /// Target class breakdown: `None` = untyped objects (literals or object-only URIs).
    target_classes: HashMap<Option<u64>, u64>,
    /// Datatype/language breakdown for literal objects.
    /// Key is a DatatypeIndex entry ID (>0); non-literals are excluded.
    target_datatypes: HashMap<u16, u64>,
}

#[derive(Default)]
struct DatasetPropData {
    triple_count: u64,
}

#[derive(Default)]
struct ClassPartitionData {
    entity_count: u64,
    /// Property partitions within this class: predicate_id → data.
    prop_partitions: HashMap<u64, PropPartitionData>,
}

#[derive(Default)]
struct PropPartitionDistinctData {
    stats: DistinctStats,
    target_classes: HashMap<Option<u64>, DistinctStats>,
    target_datatypes: HashMap<u16, DistinctStats>,
    lang_string: DistinctStats,
}

#[derive(Default)]
struct ClassPartitionDistinctData {
    objects: DistinctObjectStats,
    prop_partitions: HashMap<u64, PropPartitionDistinctData>,
}

/// Optional distinct-count state, kept outside the core count maps so an
/// index-free run retains the original compact value layouts. The nested map
/// is absent for the `dataset-properties` scope.
struct PartitionDistinctData {
    dataset_prop_data: HashMap<u64, DistinctStats>,
    class_partitions: Option<HashMap<u64, ClassPartitionDistinctData>>,
}

impl PartitionDistinctData {
    fn new(scope: PartitionDistinctScope) -> Self {
        Self {
            dataset_prop_data: HashMap::new(),
            class_partitions: (scope == PartitionDistinctScope::All).then(HashMap::new),
        }
    }
}

/// The four dataset-level counts heading every dataset description.
struct DatasetCounts {
    triples: u64,
    distinct_subjects: u64,
    properties: u64,
    distinct_objects: u64,
}

/// Partition statistics for one described dataset: the union, or one graph subset.
struct PartitionStats {
    dataset_prop_data: HashMap<u64, DatasetPropData>,
    class_partitions: HashMap<u64, ClassPartitionData>,
    distinct_data: Option<PartitionDistinctData>,
}

impl ClassPartitionData {
    fn total_triples(&self) -> u64 {
        self.prop_partitions.values().map(|p| p.triple_count).sum()
    }
}

impl PartitionStats {
    fn new(distinct_scope: Option<PartitionDistinctScope>) -> Self {
        Self {
            dataset_prop_data: HashMap::new(),
            class_partitions: HashMap::new(),
            distinct_data: distinct_scope.map(PartitionDistinctData::new),
        }
    }

    /// Record one triple from the SPO scan.
    fn record_triple(
        &mut self,
        (s_id, p_id, o_id): (u64, u64, u64),
        dt_id: u16,
        datatype_index: &DatatypeIndex,
        classes: ClassLookup<'_>,
    ) {
        // Dataset-level property count and datatype accumulation.
        self.dataset_prop_data.entry(p_id).or_default().triple_count += 1;
        if let Some(distinct) = self.distinct_data.as_mut() {
            distinct
                .dataset_prop_data
                .entry(p_id)
                .or_default()
                .add_subject(s_id);
        }

        let subject_classes = classes.subject(s_id);
        if subject_classes.is_empty() {
            return;
        }
        let obj_classes = classes.object(o_id);

        // Record this triple in every class partition the subject belongs to.
        let mut distinct_classes = self
            .distinct_data
            .as_mut()
            .and_then(|distinct| distinct.class_partitions.as_mut());
        for &class_id in subject_classes {
            let cp = self.class_partitions.entry(class_id).or_default();
            let pp = cp.prop_partitions.entry(p_id).or_default();
            pp.triple_count += 1;
            if dt_id > 0 {
                *pp.target_datatypes.entry(dt_id).or_insert(0) += 1;
            }
            if obj_classes.is_empty() {
                *pp.target_classes.entry(None).or_insert(0) += 1;
            } else {
                for &obj_class_id in obj_classes {
                    *pp.target_classes.entry(Some(obj_class_id)).or_insert(0) += 1;
                }
            }

            if let Some(distinct_classes) = distinct_classes.as_mut() {
                let distinct_cp = distinct_classes.entry(class_id).or_default();
                let distinct_pp = distinct_cp.prop_partitions.entry(p_id).or_default();
                distinct_pp.stats.add_subject(s_id);
                if dt_id > 0 {
                    distinct_pp
                        .target_datatypes
                        .entry(dt_id)
                        .or_default()
                        .add_subject(s_id);
                    if datatype_index.is_language(dt_id) {
                        distinct_pp.lang_string.add_subject(s_id);
                    }
                }
                if obj_classes.is_empty() {
                    distinct_pp
                        .target_classes
                        .entry(None)
                        .or_default()
                        .add_subject(s_id);
                } else {
                    for &obj_class_id in obj_classes {
                        distinct_pp
                            .target_classes
                            .entry(Some(obj_class_id))
                            .or_default()
                            .add_subject(s_id);
                    }
                }
            }
        }
    }

    /// Record one triple from the OPS scan into the distinct-object trackers.
    ///
    /// `classes` is required exactly when the nested class hierarchy is tracked.
    fn record_object(
        &mut self,
        (s_id, p_id, o_id): (u64, u64, u64),
        dt_id: u16,
        datatype_index: &DatatypeIndex,
        classes: Option<ClassLookup<'_>>,
    ) -> Result<()> {
        let Some(distinct_data) = self.distinct_data.as_mut() else {
            return Ok(());
        };
        distinct_data
            .dataset_prop_data
            .get_mut(&p_id)
            .with_context(|| format!("OPS permutation contains unknown predicate ID {p_id}"))?
            .add_object(o_id);

        let (Some(distinct_classes), Some(classes)) =
            (distinct_data.class_partitions.as_mut(), classes)
        else {
            return Ok(());
        };
        let subject_classes = classes.subject(s_id);
        if subject_classes.is_empty() {
            return Ok(());
        }
        let object_classes = classes.object(o_id);

        for &class_id in subject_classes {
            let distinct_cp = distinct_classes.get_mut(&class_id).with_context(|| {
                format!("OPS permutation references unknown class ID {class_id}")
            })?;
            distinct_cp.objects.add_object(o_id);
            let distinct_pp = distinct_cp
                .prop_partitions
                .get_mut(&p_id)
                .with_context(|| {
                    format!(
                        "OPS permutation references unknown property ID {p_id} in class {class_id}"
                    )
                })?;
            distinct_pp.stats.add_object(o_id);

            if object_classes.is_empty() {
                distinct_pp
                    .target_classes
                    .get_mut(&None)
                    .context("missing untyped target-class partition")?
                    .add_object(o_id);
            } else {
                for &object_class_id in object_classes {
                    distinct_pp
                        .target_classes
                        .get_mut(&Some(object_class_id))
                        .with_context(|| {
                            format!("missing target-class partition for class ID {object_class_id}")
                        })?
                        .add_object(o_id);
                }
            }

            if dt_id > 0 {
                distinct_pp
                    .target_datatypes
                    .get_mut(&dt_id)
                    .with_context(|| format!("missing datatype partition for entry ID {dt_id}"))?
                    .add_object(o_id);
                if datatype_index.is_language(dt_id) {
                    distinct_pp.lang_string.add_object(o_id);
                }
            }
        }
        Ok(())
    }

    /// Set `void:entities` on every class partition from the Pass 1 subject counts.
    fn apply_entity_counts(&mut self, entity_counts: &HashMap<u64, u64>) {
        for (class_id, cp) in self.class_partitions.iter_mut() {
            cp.entity_count = *entity_counts.get(class_id).unwrap_or(&0);
        }
    }
}

/// Statistics for one graph of the RDF dataset.
struct GraphSubset {
    /// Memberships seen in the SPO scan.
    triples: u64,
    /// Dataset-level distinct subjects (SPO scan) and objects (OPS scan). A graph's
    /// memberships are a subsequence of each scan, so IDs stay monotonic.
    distinct: DistinctStats,
    partitions: PartitionStats,
    /// Subjects per class, counting only this graph's `rdf:type` triples.
    entity_counts: HashMap<u64, u64>,
}

impl GraphSubset {
    fn counts(&self) -> DatasetCounts {
        DatasetCounts {
            triples: self.triples,
            distinct_subjects: self.distinct.distinct_subjects,
            properties: self.partitions.dataset_prop_data.len() as u64,
            distinct_objects: self.distinct.distinct_objects,
        }
    }
}

/// Joins a position-major membership stream to a scan of the same position space.
struct MembershipJoin {
    memberships: PositionMajorMemberships,
    pending: Option<PositionGraphMembership>,
    graphs: Vec<u64>,
}

impl MembershipJoin {
    fn new(mut memberships: PositionMajorMemberships) -> Result<Self> {
        let pending = memberships.next().transpose()?;
        Ok(Self {
            memberships,
            pending,
            graphs: Vec::new(),
        })
    }

    /// The increasing graph IDs containing `position`. Every position of the scan
    /// must be requested, in increasing order.
    fn graphs_at(&mut self, position: u64) -> Result<&[u64]> {
        self.graphs.clear();
        while let Some(membership) = self.pending {
            ensure!(
                membership.position >= position,
                "graph membership at position {} was skipped",
                membership.position
            );
            if membership.position != position {
                break;
            }
            self.graphs.push(membership.graph);
            self.pending = self.memberships.next().transpose()?;
        }
        ensure!(
            !self.graphs.is_empty(),
            "position {position} has no graph membership"
        );
        Ok(&self.graphs)
    }

    fn finish(self) -> Result<()> {
        ensure!(
            self.pending.is_none(),
            "graph membership position exceeds the triple count"
        );
        Ok(())
    }
}

/// The per-graph half of a dataset-view pass.
struct SubsetPass<'a> {
    join: MembershipJoin,
    subsets: &'a mut [GraphSubset],
}

// ---------------------------------------------------------------------------
// N-Triples output helpers
// ---------------------------------------------------------------------------

/// Compute MD5 hex digest of a UTF-8 string, matching Python's
/// `hashlib.md5(s.encode("utf-8")).hexdigest()`.
fn md5_hex(s: &str) -> String {
    let digest = md5::compute(s.as_bytes());
    format!("{digest:x}")
}

/// Format an integer as an xsd:integer literal node.
fn int_node(n: u64) -> String {
    format!("\"{n}\"^^<{XSD_INTEGER}>")
}

/// Return true if the raw HDT term bytes represent an IRI (not a blank node or literal).
fn is_iri(term: &[u8]) -> bool {
    !term.starts_with(b"_:") && !term.starts_with(b"\"")
}

/// Generate a partition node string: `<URI>` for URI mode, `_:bN` for blank-node mode.
fn make_partition_node(use_blank_nodes: bool, uri_inner: &str, counter: &mut u64) -> String {
    if use_blank_nodes {
        *counter += 1;
        format!("_:b{}", *counter)
    } else {
        format!("<{uri_inner}>")
    }
}

/// Write one N-Triple to `w`.
///
/// `s` and `o` are already formatted N-Triples nodes (e.g. `<IRI>`, `_:bN`,
/// `"123"^^<xsd:integer>`). `p` is a raw IRI (without angle brackets).
#[inline]
fn nt(w: &mut impl Write, s: &str, p: &str, o: &str) -> Result<()> {
    writeln!(w, "{s} <{p}> {o} .").context("write N-Triple")
}

// ---------------------------------------------------------------------------
// Pass 1: Build ClassComboIndex from rdf:type triples
// ---------------------------------------------------------------------------

/// A union-only scan attributes every triple to one implicit graph.
const UNION_GRAPHS: &[u64] = &[0];

/// Scan all triples; collect `rdf:type` triples into raw class combinations.
///
/// Exploits SPO ordering: all triples for a subject are contiguous, so we buffer
/// each subject's class IDs with O(1) memory per subject, then deduplicate via
/// a combo map. With `memberships`, each class is recorded under every graph
/// holding its `rdf:type` triple.
fn build_class_combo_index(
    hdt_path: &Path,
    offsets: &HdtSectionOffsets,
    rdf_type_pred_id: u64,
    nb_subjects: u64,
    mut memberships: Option<MembershipJoin>,
) -> Result<RawClassCombos> {
    let alloc_bytes = (nb_subjects as usize + 1) * std::mem::size_of::<u32>();
    tracing::info!(
        "  Allocating class combo index: {:.1} GB for {} subjects",
        alloc_bytes as f64 / 1_073_741_824.0,
        nb_subjects
    );

    let mut subject_combos = vec![0u32; nb_subjects as usize + 1];
    let mut combo_map: HashMap<Vec<(u64, u64)>, u32> = HashMap::new();
    let mut combo_pairs: Vec<Vec<(u64, u64)>> = Vec::new();

    let mut scanner =
        BitmapTriplesScanner::new(offsets, hdt_path).context("open scanner for Pass 1")?;

    let mut current_subject: u64 = 0;
    let mut current_pairs: Vec<(u64, u64)> = Vec::new();
    let mut scanned: u64 = 0;
    let mut typed_subjects: u64 = 0;

    // Closure-like helper: finalize a subject's collected class IDs into the combo index.
    // Defined inline because closures can't borrow multiple fields mutably.
    macro_rules! finalize_subject {
        () => {
            if !current_pairs.is_empty() {
                current_pairs.sort_unstable();
                current_pairs.dedup();
                let combo_id = if let Some(&id) = combo_map.get(&current_pairs) {
                    id
                } else {
                    anyhow::ensure!(
                        combo_pairs.len() < u32::MAX as usize,
                        "More than {} unique class combinations; dataset too complex for VoID analysis",
                        u32::MAX
                    );
                    let id = combo_pairs.len() as u32 + 1;
                    let pairs = current_pairs.clone();
                    combo_map.insert(pairs.clone(), id);
                    combo_pairs.push(pairs);
                    id
                };
                subject_combos[current_subject as usize] = combo_id;
                typed_subjects += 1;
                current_pairs.clear();
            }
        };
    }

    while let Some((s_id, p_id, o_id)) = scanner.next_triple()? {
        let graphs = match memberships.as_mut() {
            Some(join) => join.graphs_at(scanned)?,
            None => UNION_GRAPHS,
        };
        if s_id != current_subject {
            finalize_subject!();
            current_subject = s_id;
        }
        if p_id == rdf_type_pred_id {
            current_pairs.extend(graphs.iter().map(|&graph| (graph, o_id)));
        }
        scanned += 1;
        if scanned.is_multiple_of(10_000_000) {
            tracing::info!("  Pass 1: {scanned} triples scanned...");
        }
    }
    // Finalize last subject.
    finalize_subject!();
    if let Some(join) = memberships {
        join.finish()?;
    }

    let raw = RawClassCombos {
        subject_combos,
        combo_pairs,
    };
    tracing::info!(
        "  Pass 1 complete: {scanned} triples scanned, {typed_subjects} typed subjects, \
         {} distinct classes, {} unique class combinations",
        raw.distinct_class_ids().len(),
        raw.combo_pairs.len()
    );

    Ok(raw)
}

// ---------------------------------------------------------------------------
// Post–Pass 1: filter non-IRI classes
// ---------------------------------------------------------------------------

/// Remove non-IRI class IDs (blank nodes, literals) from the raw class combinations.
///
/// The Python `void-hdt` tool only treats `URIRef` objects of `rdf:type` triples as
/// valid classes.  Blank nodes used as `rdf:type` objects are common in OWL ontologies
/// (anonymous class expressions) and should not produce class partitions or affect
/// type-based counting.
///
/// After filtering, combos that become empty are mapped to 0 (untyped), and
/// duplicate filtered combos are merged.
fn filter_non_iri_classes(
    raw: &mut RawClassCombos,
    resolver: &mut DictionaryResolver,
) -> Result<()> {
    // Collect all unique class IDs across all combos.
    let all_class_ids = raw.distinct_class_ids();

    // Resolve each class ID and build set of non-IRI ones.
    let mut non_iri_class_ids = std::collections::HashSet::new();
    let mut term_buf = Vec::new();
    for &class_id in &all_class_ids {
        term_buf.clear();
        resolver.object_term(class_id, &mut term_buf)?;
        if !is_iri(&term_buf) {
            non_iri_class_ids.insert(class_id);
        }
    }

    if non_iri_class_ids.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "  Filtering {} non-IRI class ID(s) (blank nodes / literals)",
        non_iri_class_ids.len()
    );

    // Build a remapping: old combo_id → new combo_id.
    let mut new_combo_map: HashMap<Vec<(u64, u64)>, u32> = HashMap::new();
    let mut new_combo_pairs: Vec<Vec<(u64, u64)>> = Vec::new();
    // combo_remap[i] = new combo_id for old combo_id (i+1). 0 = became untyped.
    let mut combo_remap: Vec<u32> = Vec::with_capacity(raw.combo_pairs.len());

    for pairs in &raw.combo_pairs {
        let filtered: Vec<(u64, u64)> = pairs
            .iter()
            .copied()
            .filter(|(_, class)| !non_iri_class_ids.contains(class))
            .collect();

        if filtered.is_empty() {
            combo_remap.push(0);
        } else if let Some(&id) = new_combo_map.get(&filtered) {
            combo_remap.push(id);
        } else {
            let id = new_combo_pairs.len() as u32 + 1;
            new_combo_pairs.push(filtered.clone());
            new_combo_map.insert(filtered, id);
            combo_remap.push(id);
        }
    }

    // Remap all subject entries.
    for combo_id in raw.subject_combos.iter_mut() {
        if *combo_id > 0 {
            *combo_id = combo_remap[*combo_id as usize - 1];
        }
    }

    raw.combo_pairs = new_combo_pairs;

    Ok(())
}

/// Subjects per class: across the union, and per graph when `graph_count` is given.
///
/// Every subject sharing a combo has the same classes, so counting subjects per combo
/// first makes this one pass over the index plus one over the combo table.
fn class_entity_counts(
    index: &ClassComboIndex,
    graph_count: Option<usize>,
) -> (HashMap<u64, u64>, Vec<HashMap<u64, u64>>) {
    let mut subjects_per_combo = vec![0u64; index.combo_to_classes.len()];
    for &combo_id in &index.subject_combos {
        if combo_id > 0 {
            subjects_per_combo[combo_id as usize - 1] += 1;
        }
    }

    let mut union = HashMap::new();
    let mut graphs: Vec<HashMap<u64, u64>> = std::iter::repeat_with(HashMap::new)
        .take(graph_count.unwrap_or(0))
        .collect();
    for (combo, &subjects) in subjects_per_combo.iter().enumerate() {
        for &class_id in &index.combo_to_classes[combo] {
            *union.entry(class_id).or_insert(0) += subjects;
        }
        if let Some(graph_classes) = index.combo_to_graph_classes.get(combo) {
            for (&graph, &class_id) in graph_classes.graphs.iter().zip(&graph_classes.classes) {
                *graphs[graph as usize].entry(class_id).or_insert(0) += subjects;
            }
        }
    }
    (union, graphs)
}

// ---------------------------------------------------------------------------
// Build DatatypeIndex from object-only dictionary entries
// ---------------------------------------------------------------------------

/// Scan the object-only section of the dictionary to build a [`DatatypeIndex`].
///
/// Each object-only term is classified:
/// - Non-literal (URI/blank node) → entry 0
/// - Typed literal `"..."^^<IRI>` → entry for that datatype IRI
/// - Language-tagged literal `"..."@tag` → entry for that language tag (implicitly rdf:langString)
/// - Plain literal `"..."` → entry for xsd:string (RDF 1.1)
///
/// Sequential access through `PfcSectionIndex::get_bytes()` achieves near-optimal
/// block-cache hit rates since IDs are accessed in order.
fn build_datatype_index(
    resolver: &mut DictionaryResolver,
    nb_shared: u64,
    nb_objects: u64,
) -> Result<DatatypeIndex> {
    let nb_object_only = nb_objects - nb_shared;

    let alloc_bytes = nb_object_only as usize * std::mem::size_of::<u16>();
    tracing::info!(
        "  Allocating datatype index: {:.1} GB for {} object-only terms",
        alloc_bytes as f64 / 1_073_741_824.0,
        nb_object_only
    );

    let mut object_only_entries = vec![0u16; nb_object_only as usize];
    let mut datatype_map: HashMap<Vec<u8>, u16> = HashMap::new();
    let mut datatype_iris: Vec<String> = Vec::new();
    let mut language_map: HashMap<Vec<u8>, u16> = HashMap::new();
    let mut language_tags: Vec<String> = Vec::new();

    // Pre-register xsd:string as the first datatype (for plain literals).
    let xsd_string_id: u16 = 1;
    datatype_map.insert(XSD_STRING.as_bytes().to_vec(), xsd_string_id);
    datatype_iris.push(XSD_STRING.to_string());

    // Language-tagged entries are stored with bit 15 set during the scan, then
    // remapped to final IDs (lang_boundary + idx) after all datatypes are known.
    const LANG_FLAG: u16 = 0x8000;

    let mut term_buf = Vec::new();
    let mut literals_found: u64 = 0;

    for local_id in 1..=nb_object_only {
        term_buf.clear();
        resolver
            .objects
            .get_bytes(local_id, &mut term_buf)
            .with_context(|| format!("Failed to read object-only ID {local_id}"))?;

        if !term_buf.starts_with(b"\"") {
            // Not a literal — leave as 0.
            continue;
        }

        literals_found += 1;

        let (_, suffix_start) = find_literal_boundary(&term_buf);
        let suffix = &term_buf[suffix_start..];

        let entry = if suffix.starts_with(b"^^<") && suffix.ends_with(b">") {
            // Typed literal: extract datatype IRI.
            let dt_bytes = &suffix[3..suffix.len() - 1];
            if let Some(&id) = datatype_map.get(dt_bytes) {
                id
            } else {
                let id = datatype_iris.len() as u16 + 1;
                anyhow::ensure!(
                    id < LANG_FLAG,
                    "More than {} distinct datatypes; dataset too complex",
                    LANG_FLAG - 1
                );
                let iri = String::from_utf8_lossy(dt_bytes).into_owned();
                datatype_map.insert(dt_bytes.to_vec(), id);
                datatype_iris.push(iri);
                id
            }
        } else if suffix.starts_with(b"@") {
            // Language-tagged literal: store 0-based index with LANG_FLAG.
            let tag_bytes = &suffix[1..];
            let idx = if let Some(&idx) = language_map.get(tag_bytes) {
                idx
            } else {
                let idx = language_tags.len() as u16;
                anyhow::ensure!(
                    idx < LANG_FLAG,
                    "More than {} distinct language tags; dataset too complex",
                    LANG_FLAG - 1
                );
                let tag = String::from_utf8_lossy(tag_bytes).into_owned();
                language_map.insert(tag_bytes.to_vec(), idx);
                language_tags.push(tag);
                idx
            };
            LANG_FLAG | idx
        } else {
            // Plain literal → xsd:string.
            xsd_string_id
        };

        object_only_entries[(local_id - 1) as usize] = entry;

        if literals_found.is_multiple_of(10_000_000) {
            tracing::info!("  Datatype index: {literals_found} literals classified...");
        }
    }

    // Remap language-tag entries from (LANG_FLAG | idx) to (lang_boundary + idx).
    let lang_boundary = datatype_iris.len() as u16 + 1;
    anyhow::ensure!(
        (lang_boundary as usize) + language_tags.len() <= u16::MAX as usize,
        "More than {} distinct datatypes + language tags combined; dataset too complex",
        u16::MAX
    );

    for entry in object_only_entries.iter_mut() {
        if *entry & LANG_FLAG != 0 {
            let idx = *entry & !LANG_FLAG;
            *entry = lang_boundary + idx;
        }
    }

    tracing::info!(
        "  Datatype index complete: {} literals, {} distinct datatypes, {} distinct languages",
        literals_found,
        datatype_iris.len(),
        language_tags.len()
    );

    Ok(DatatypeIndex {
        object_only_entries,
        datatype_iris,
        language_tags,
        lang_boundary,
        nb_shared,
    })
}

// ---------------------------------------------------------------------------
// Pass 2: Accumulate statistics
// ---------------------------------------------------------------------------

/// Scan all triples to accumulate the union's partition statistics, and with
/// `subsets`, every graph's:
/// - `dataset_prop_data`: triple and optional distinct-subject counts per predicate ID.
/// - `class_partitions`: per-class property, target-class, and datatype breakdowns,
///   including optional distinct-subject counts.
fn run_stats_pass(
    hdt_path: &Path,
    offsets: &HdtSectionOffsets,
    nb_shared: u64,
    class_combo_index: &ClassComboIndex,
    datatype_index: &DatatypeIndex,
    distinct_scope: Option<PartitionDistinctScope>,
    mut subsets: Option<SubsetPass<'_>>,
) -> Result<PartitionStats> {
    let mut union = PartitionStats::new(distinct_scope);
    let union_classes = ClassLookup {
        index: class_combo_index,
        nb_shared,
        scope: ClassScope::Union,
    };

    let mut scanner =
        BitmapTriplesScanner::new(offsets, hdt_path).context("open scanner for Pass 2")?;

    let mut position = 0u64;
    while let Some(triple) = scanner.next_triple()? {
        // Datatype lookup for this object (0 = not a literal).
        let dt_id = datatype_index.get(triple.2);
        union.record_triple(triple, dt_id, datatype_index, union_classes);

        if let Some(pass) = subsets.as_mut() {
            for &graph in pass.join.graphs_at(position)? {
                let subset = &mut pass.subsets[graph as usize];
                subset.triples += 1;
                subset.distinct.add_subject(triple.0);
                subset.partitions.record_triple(
                    triple,
                    dt_id,
                    datatype_index,
                    ClassLookup {
                        scope: ClassScope::Graph(graph),
                        ..union_classes
                    },
                );
            }
        }

        position += 1;
        if position.is_multiple_of(10_000_000) {
            tracing::info!(
                "  Pass 2: {position}/{} triples processed...",
                offsets.num_triples
            );
        }
    }
    if let Some(pass) = subsets {
        pass.join.finish()?;
    }

    Ok(union)
}

/// Scan the permutation sidecar in OPS order and add exact distinct-object
/// counts. Because every partition sees object IDs monotonically, each one only
/// needs a last-seen object scalar rather than a set of all its objects.
///
/// `class_combo_index` is required only when the nested class hierarchy is
/// tracked (the `all` scope). A `dataset-properties` run attributes objects by
/// predicate alone, so it passes `None` and the caller releases the index — 4
/// bytes per subject — before this pass rather than after it.
///
/// With `subsets`, each graph additionally counts its dataset-level distinct
/// objects, which unlike the union's are not a dictionary section size.
fn run_distinct_object_pass(
    index: &PermutationIndex,
    nb_shared: u64,
    class_combo_index: Option<&ClassComboIndex>,
    datatype_index: &DatatypeIndex,
    distinct_scope: Option<PartitionDistinctScope>,
    union: &mut PartitionStats,
    mut subsets: Option<SubsetPass<'_>>,
) -> Result<()> {
    // Hoisted out of the scan: the nested class maps and the combo index are
    // present together or absent together.
    if distinct_scope == Some(PartitionDistinctScope::All) {
        ensure!(
            class_combo_index.is_some(),
            "class partition distinct counts require the subject-to-class index"
        );
    }
    let union_classes = class_combo_index.map(|index| ClassLookup {
        index,
        nb_shared,
        scope: ClassScope::Union,
    });

    let mut scanner = index
        .all_triples(PermutationComponent::Ops)
        .context("Failed to open OPS permutation scan")?;
    let num_triples = index.header().triples;
    let mut position = 0u64;
    for triple in &mut scanner {
        let triple = triple.context("Failed to scan OPS permutation")?;
        let dt_id = datatype_index.get(triple.2);
        union.record_object(triple, dt_id, datatype_index, union_classes)?;

        if let Some(pass) = subsets.as_mut() {
            for &graph in pass.join.graphs_at(position)? {
                let subset = &mut pass.subsets[graph as usize];
                subset.distinct.add_object(triple.2);
                subset.partitions.record_object(
                    triple,
                    dt_id,
                    datatype_index,
                    union_classes.map(|classes| ClassLookup {
                        scope: ClassScope::Graph(graph),
                        ..classes
                    }),
                )?;
            }
        }

        position += 1;
        if position.is_multiple_of(10_000_000) {
            tracing::info!("  OPS pass: {position}/{num_triples} triples processed...");
        }
    }
    if let Some(pass) = subsets {
        pass.join.finish()?;
    }

    tracing::info!("  OPS pass complete: {position} triples processed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

fn write_distinct_values(
    w: &mut impl Write,
    node: &str,
    distinct_subjects: u64,
    distinct_objects: u64,
) -> Result<u64> {
    nt(
        w,
        node,
        VOID_DISTINCT_SUBJECTS,
        &int_node(distinct_subjects),
    )?;
    nt(w, node, VOID_DISTINCT_OBJECTS, &int_node(distinct_objects))?;
    Ok(2)
}

fn write_distinct_counts(w: &mut impl Write, node: &str, stats: &DistinctStats) -> Result<u64> {
    write_distinct_values(w, node, stats.distinct_subjects, stats.distinct_objects)
}

#[derive(Clone, Copy)]
struct PartitionWriteOptions<'a> {
    use_blank_nodes: bool,
    distinct: Option<&'a PropPartitionDistinctData>,
}

/// Emit datatype and language partitions for a property partition's `target_datatypes` map.
///
/// Groups entries into non-langString datatypes and language tags (which are aggregated
/// under a single `rdf:langString` datatype partition with nested language partitions).
///
/// Returns the number of N-Triples written.
fn write_datatype_partitions(
    w: &mut impl Write,
    prop_part_uri: &str,
    prop_part_node: &str,
    target_datatypes: &HashMap<u16, u64>,
    datatype_index: &DatatypeIndex,
    options: PartitionWriteOptions<'_>,
    bnode_counter: &mut u64,
) -> Result<u64> {
    if target_datatypes.is_empty() {
        return Ok(0);
    }

    let mut written: u64 = 0;

    // Separate entries into non-langString datatypes and language tags.
    let mut datatype_entries: Vec<(u16, u64)> = Vec::new();
    let mut lang_entries: Vec<(u16, u64)> = Vec::new();
    let mut lang_total = 0u64;

    let mut sorted_ids: Vec<u16> = target_datatypes.keys().copied().collect();
    sorted_ids.sort_unstable();

    for &entry_id in &sorted_ids {
        let count = target_datatypes[&entry_id];
        if datatype_index.is_language(entry_id) {
            lang_entries.push((entry_id, count));
            lang_total += count;
        } else {
            datatype_entries.push((entry_id, count));
        }
    }

    // Emit non-langString datatype partitions.
    for (entry_id, count) in &datatype_entries {
        let dt_iri = datatype_index.datatype_iri(*entry_id);
        let dt_part_uri = format!("{prop_part_uri}/datatype/{}", md5_hex(dt_iri));
        let dt_part_node =
            make_partition_node(options.use_blank_nodes, &dt_part_uri, bnode_counter);

        nt(w, prop_part_node, VOIDEXT_DATATYPE_PARTITION, &dt_part_node)?;
        written += 1;
        nt(w, &dt_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
        written += 1;
        nt(w, &dt_part_node, VOIDEXT_DATATYPE, &format!("<{dt_iri}>"))?;
        written += 1;
        nt(w, &dt_part_node, VOID_TRIPLES, &int_node(*count))?;
        written += 1;
        if let Some(distinct) = options.distinct {
            let distinct_stats = distinct
                .target_datatypes
                .get(entry_id)
                .context("missing distinct datatype partition")?;
            written += write_distinct_counts(w, &dt_part_node, distinct_stats)?;
        }
    }

    // Emit rdf:langString datatype partition with nested language partitions.
    if !lang_entries.is_empty() {
        let dt_part_uri = format!("{prop_part_uri}/datatype/{}", md5_hex(RDF_LANG_STRING));
        let dt_part_node =
            make_partition_node(options.use_blank_nodes, &dt_part_uri, bnode_counter);

        nt(w, prop_part_node, VOIDEXT_DATATYPE_PARTITION, &dt_part_node)?;
        written += 1;
        nt(w, &dt_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
        written += 1;
        nt(
            w,
            &dt_part_node,
            VOIDEXT_DATATYPE,
            &format!("<{RDF_LANG_STRING}>"),
        )?;
        written += 1;
        nt(w, &dt_part_node, VOID_TRIPLES, &int_node(lang_total))?;
        written += 1;
        if let Some(distinct) = options.distinct {
            written += write_distinct_counts(w, &dt_part_node, &distinct.lang_string)?;
        }

        // Nested language partitions.
        for (entry_id, count) in &lang_entries {
            let lang_tag = datatype_index.language_tag(*entry_id);
            let lang_part_uri = format!("{dt_part_uri}/language/{}", md5_hex(lang_tag));
            let lang_part_node =
                make_partition_node(options.use_blank_nodes, &lang_part_uri, bnode_counter);

            nt(
                w,
                &dt_part_node,
                VOIDEXT_LANGUAGE_PARTITION,
                &lang_part_node,
            )?;
            written += 1;
            nt(w, &lang_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
            written += 1;
            nt(
                w,
                &lang_part_node,
                VOIDEXT_LANGUAGE,
                &format!("\"{lang_tag}\""),
            )?;
            written += 1;
            nt(w, &lang_part_node, VOID_TRIPLES, &int_node(*count))?;
            written += 1;
            if let Some(distinct) = options.distinct {
                let distinct_stats = distinct
                    .target_datatypes
                    .get(entry_id)
                    .context("missing distinct language partition")?;
                written += write_distinct_counts(w, &lang_part_node, distinct_stats)?;
            }
        }
    }

    Ok(written)
}

/// Where one dataset description is rooted, and the statements that head it.
struct DatasetHead<'a> {
    /// IRI every partition node of this description extends.
    uri: &'a str,
    /// The formatted subject node: the IRI, or a blank node in blank-node mode.
    node: &'a str,
    types: &'a [&'a str],
    counts: DatasetCounts,
}

/// Serialize one dataset description — the union, or one graph subset — as
/// N-Triples written to `w`.
///
/// Returns the number of N-Triples written.
fn write_dataset_description(
    w: &mut impl Write,
    head: &DatasetHead<'_>,
    stats: &PartitionStats,
    datatype_index: &DatatypeIndex,
    resolver: &mut DictionaryResolver,
    use_blank_nodes: bool,
    bnode_counter: &mut u64,
) -> Result<u64> {
    let mut written: u64 = 0;
    let dataset_uri = head.uri;
    let dataset_node = head.node;
    let distinct_data = stats.distinct_data.as_ref();

    // Reusable term buffer for dictionary lookups.
    let mut term_buf = Vec::<u8>::new();

    // -----------------------------------------------------------------------
    // 1. Dataset-level statistics
    // -----------------------------------------------------------------------
    for class in head.types {
        nt(w, dataset_node, RDF_TYPE, &format!("<{class}>"))?;
        written += 1;
    }
    let counts = &head.counts;
    nt(w, dataset_node, VOID_TRIPLES, &int_node(counts.triples))?;
    written += 1;
    nt(
        w,
        dataset_node,
        VOID_DISTINCT_SUBJECTS,
        &int_node(counts.distinct_subjects),
    )?;
    written += 1;
    nt(
        w,
        dataset_node,
        VOID_PROPERTIES,
        &int_node(counts.properties),
    )?;
    written += 1;
    nt(
        w,
        dataset_node,
        VOID_DISTINCT_OBJECTS,
        &int_node(counts.distinct_objects),
    )?;
    written += 1;

    // -----------------------------------------------------------------------
    // 2. Dataset-level property partitions (one per predicate)
    // -----------------------------------------------------------------------
    let mut pred_ids: Vec<u64> = stats.dataset_prop_data.keys().copied().collect();
    pred_ids.sort_unstable();

    for pred_id in &pred_ids {
        let dpd = &stats.dataset_prop_data[pred_id];
        term_buf.clear();
        resolver.predicate_term(*pred_id, &mut term_buf)?;
        if !is_iri(&term_buf) {
            continue;
        }
        let pred_iri = String::from_utf8_lossy(&term_buf).into_owned();
        let part_uri = format!("{dataset_uri}/property/{}", md5_hex(&pred_iri));
        let part_node = make_partition_node(use_blank_nodes, &part_uri, bnode_counter);

        nt(w, dataset_node, VOID_PROPERTY_PARTITION, &part_node)?;
        written += 1;
        nt(w, &part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
        written += 1;
        nt(w, &part_node, VOID_PROPERTY, &format!("<{pred_iri}>"))?;
        written += 1;
        nt(w, &part_node, VOID_TRIPLES, &int_node(dpd.triple_count))?;
        written += 1;
        if let Some(distinct) = distinct_data {
            let distinct_stats = distinct
                .dataset_prop_data
                .get(pred_id)
                .context("missing dataset property distinct statistics")?;
            written += write_distinct_counts(w, &part_node, distinct_stats)?;
        }
    }

    // -----------------------------------------------------------------------
    // 3. Class partitions (with nested property and target-class partitions)
    // -----------------------------------------------------------------------
    let mut class_ids: Vec<u64> = stats.class_partitions.keys().copied().collect();
    class_ids.sort_unstable();

    let mut class_buf = Vec::<u8>::new();
    let mut target_buf = Vec::<u8>::new();

    for class_id in &class_ids {
        let cp = &stats.class_partitions[class_id];
        let distinct_cp = distinct_data
            .and_then(|distinct| distinct.class_partitions.as_ref())
            .map(|classes| {
                classes
                    .get(class_id)
                    .context("missing class partition distinct statistics")
            })
            .transpose()?;

        // Resolve the class IRI (class_id is an object ID in the HDT dictionary).
        class_buf.clear();
        resolver.object_term(*class_id, &mut class_buf)?;
        if !is_iri(&class_buf) {
            continue; // Skip blank-node or literal "classes".
        }
        let class_iri = String::from_utf8_lossy(&class_buf).into_owned();
        let class_part_uri = format!("{dataset_uri}/class/{}", md5_hex(&class_iri));
        let class_part_node = make_partition_node(use_blank_nodes, &class_part_uri, bnode_counter);

        nt(w, dataset_node, VOID_CLASS_PARTITION, &class_part_node)?;
        written += 1;
        nt(w, &class_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
        written += 1;
        nt(w, &class_part_node, VOID_CLASS, &format!("<{class_iri}>"))?;
        written += 1;
        nt(
            w,
            &class_part_node,
            VOID_ENTITIES,
            &int_node(cp.entity_count),
        )?;
        written += 1;
        nt(
            w,
            &class_part_node,
            VOID_TRIPLES,
            &int_node(cp.total_triples()),
        )?;
        written += 1;
        if let Some(distinct_cp) = distinct_cp {
            written += write_distinct_values(
                w,
                &class_part_node,
                cp.entity_count,
                distinct_cp.objects.distinct_objects,
            )?;
        }

        // Nested property partitions within this class.
        let mut prop_ids: Vec<u64> = cp.prop_partitions.keys().copied().collect();
        prop_ids.sort_unstable();

        for prop_id in &prop_ids {
            let pp = &cp.prop_partitions[prop_id];
            let distinct_pp = distinct_cp
                .map(|distinct_cp| {
                    distinct_cp
                        .prop_partitions
                        .get(prop_id)
                        .context("missing class property distinct statistics")
                })
                .transpose()?;

            term_buf.clear();
            resolver.predicate_term(*prop_id, &mut term_buf)?;
            if !is_iri(&term_buf) {
                continue;
            }
            let pred_iri = String::from_utf8_lossy(&term_buf).into_owned();
            let prop_part_uri = format!("{class_part_uri}/property/{}", md5_hex(&pred_iri));
            let prop_part_node =
                make_partition_node(use_blank_nodes, &prop_part_uri, bnode_counter);

            nt(
                w,
                &class_part_node,
                VOID_PROPERTY_PARTITION,
                &prop_part_node,
            )?;
            written += 1;
            nt(w, &prop_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
            written += 1;
            nt(w, &prop_part_node, VOID_PROPERTY, &format!("<{pred_iri}>"))?;
            written += 1;
            nt(w, &prop_part_node, VOID_TRIPLES, &int_node(pp.triple_count))?;
            written += 1;
            if let Some(distinct_pp) = distinct_pp {
                written += write_distinct_counts(w, &prop_part_node, &distinct_pp.stats)?;
            }

            // Target class partitions (objectClassPartition).
            // Sort: untyped (None) last, typed by class_id for determinism.
            let mut target_keys: Vec<Option<u64>> = pp.target_classes.keys().copied().collect();
            target_keys.sort_unstable_by_key(|k| k.unwrap_or(u64::MAX));

            for target_class_opt in &target_keys {
                let target_count = pp.target_classes[target_class_opt];

                // Resolve target class IRI (if typed).
                let target_iri_opt: Option<String> = if let Some(tc_id) = target_class_opt {
                    target_buf.clear();
                    resolver.object_term(*tc_id, &mut target_buf)?;
                    if is_iri(&target_buf) {
                        Some(String::from_utf8_lossy(&target_buf).into_owned())
                    } else {
                        // Treat non-IRI target class as untyped.
                        None
                    }
                } else {
                    None
                };

                let hash_input = target_iri_opt.as_deref().unwrap_or(UNTYPED_HASH_INPUT);
                let target_part_uri = format!("{prop_part_uri}/target/{}", md5_hex(hash_input));
                let target_part_node =
                    make_partition_node(use_blank_nodes, &target_part_uri, bnode_counter);

                nt(
                    w,
                    &prop_part_node,
                    VOIDEXT_OBJECT_CLASS_PARTITION,
                    &target_part_node,
                )?;
                written += 1;
                nt(w, &target_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
                written += 1;
                if let Some(ref tc_iri) = target_iri_opt {
                    nt(w, &target_part_node, VOID_CLASS, &format!("<{tc_iri}>"))?;
                    written += 1;
                }
                nt(w, &target_part_node, VOID_TRIPLES, &int_node(target_count))?;
                written += 1;
                if let Some(distinct_pp) = distinct_pp {
                    let distinct_stats = distinct_pp
                        .target_classes
                        .get(target_class_opt)
                        .context("missing target-class distinct statistics")?;
                    written += write_distinct_counts(w, &target_part_node, distinct_stats)?;
                }
            }

            // Datatype/language partitions for this class-level property partition.
            written += write_datatype_partitions(
                w,
                &prop_part_uri,
                &prop_part_node,
                &pp.target_datatypes,
                datatype_index,
                PartitionWriteOptions {
                    use_blank_nodes,
                    distinct: distinct_pp,
                },
                bnode_counter,
            )?;
        }
    }

    Ok(written)
}

// ---------------------------------------------------------------------------
// Graph subsets
// ---------------------------------------------------------------------------

/// The artifacts a dataset-view run reads besides the HDT.
struct DatasetViewInputs {
    sidecar: GraphSidecarReader,
    graph_index: GraphIndex,
    temp_dir: std::path::PathBuf,
    /// Keeps a self-cleaning temp dir alive for the whole run.
    _owned_temp_dir: Option<tempfile::TempDir>,
    /// Memory for one membership transpose; the dictionary cache takes the rest.
    stream_budget: usize,
}

impl DatasetViewInputs {
    fn open(hdt_path: &Path, temp_dir: Option<&Path>, stream_budget: usize) -> Result<Self> {
        let sidecar_path = crate::quads::canonical_sidecar_path(hdt_path);
        ensure!(
            sidecar_path.is_file(),
            "the dataset graph view requires graph sidecar {}; create the HDT with `--mode quads`",
            sidecar_path.display()
        );
        let sidecar = GraphSidecarReader::open(&sidecar_path, hdt_path)
            .with_context(|| format!("Failed to open graph sidecar {}", sidecar_path.display()))?;

        let index_path = crate::graph_index::canonical_path(hdt_path);
        ensure!(
            index_path.is_file(),
            "the dataset graph view requires graph index {} for per-graph distinct objects; \
             create it with `hdtc graphs-index {}`",
            index_path.display(),
            hdt_path.display()
        );
        let graph_index = GraphIndex::open(&index_path, hdt_path)
            .with_context(|| format!("Failed to open graph index {}", index_path.display()))?;
        ensure!(
            graph_index.has_ops_layers(),
            "graph index {} has no OPS layer set; rebuild it with `hdtc graphs-index {} --positions pos,ops`",
            index_path.display(),
            hdt_path.display()
        );

        let (temp_dir, owned_temp_dir) = match temp_dir {
            Some(dir) => {
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("Failed to create temp dir {}", dir.display()))?;
                (dir.to_path_buf(), None)
            }
            None => {
                let owned = tempfile::Builder::new()
                    .prefix("hdtc-void-")
                    .tempdir()
                    .context("Failed to create temp dir")?;
                (owned.path().to_path_buf(), Some(owned))
            }
        };

        Ok(Self {
            sidecar,
            graph_index,
            temp_dir,
            _owned_temp_dir: owned_temp_dir,
            stream_budget,
        })
    }

    fn named_graphs(&self) -> u64 {
        self.sidecar.named_graph_count()
    }

    /// The sidecar's memberships in SPO position order.
    fn spo_join(&mut self) -> Result<MembershipJoin> {
        let named_graphs = self.named_graphs();
        open_join(
            &mut self.sidecar,
            named_graphs,
            &self.temp_dir,
            self.stream_budget,
        )
    }

    /// The graph index's memberships in OPS position order.
    fn ops_join(&mut self) -> Result<MembershipJoin> {
        let named_graphs = self.named_graphs();
        open_join(
            self.graph_index.layers_mut(GraphIndexSpace::Ops)?,
            named_graphs,
            &self.temp_dir,
            self.stream_budget,
        )
    }
}

/// Transpose a layer set into position order, by k-way merge when its layers fit
/// the budget as concurrent iterators and by external sort otherwise.
fn open_join(
    layers: &mut impl LayerSource,
    named_graphs: u64,
    temp_dir: &Path,
    stream_budget: usize,
) -> Result<MembershipJoin> {
    let merge = layer_merge_reserve(named_graphs + 1, stream_budget).is_some();
    MembershipJoin::new(PositionMajorMemberships::open(
        layers,
        named_graphs,
        temp_dir,
        stream_budget,
        merge,
    )?)
}

/// Link every graph subset from the dataset and serialize its description.
///
/// Subsets are keyed by the MD5 of the graph term: `{dataset}/graph/{md5}` for the
/// subset and `{dataset}/named-graph/{md5}` for its `sd:NamedGraph`. The default graph
/// has no term, so it is `{dataset}/default-graph`, and is described only when it
/// holds triples. A blank-node graph name cannot be an `sd:name`, so such a graph
/// gets its subset but no `sd:NamedGraph`.
#[allow(clippy::too_many_arguments)]
fn write_graph_subsets(
    w: &mut impl Write,
    dataset_uri: &str,
    subsets: &[GraphSubset],
    sidecar: &mut GraphSidecarReader,
    datatype_index: &DatatypeIndex,
    resolver: &mut DictionaryResolver,
    use_blank_nodes: bool,
    bnode_counter: &mut u64,
) -> Result<u64> {
    let dataset_node = format!("<{dataset_uri}>");
    let mut written = 0u64;
    for (graph, subset) in subsets.iter().enumerate() {
        let name = match sidecar.graph(graph as u64)? {
            GraphTerm::DefaultGraph if subset.triples == 0 => continue,
            GraphTerm::DefaultGraph => None,
            GraphTerm::Named(term) => Some(term),
        };
        let subset_uri = match &name {
            None => format!("{dataset_uri}/default-graph"),
            Some(term) => format!("{dataset_uri}/graph/{}", md5_hex(term)),
        };
        let subset_node = make_partition_node(use_blank_nodes, &subset_uri, bnode_counter);

        nt(w, &dataset_node, VOID_SUBSET, &subset_node)?;
        written += 1;
        if let Some(term) = name.as_deref().filter(|term| is_iri(term.as_bytes())) {
            let named_uri = format!("{dataset_uri}/named-graph/{}", md5_hex(term));
            let named_node = make_partition_node(use_blank_nodes, &named_uri, bnode_counter);
            nt(w, &dataset_node, SD_NAMED_GRAPH, &named_node)?;
            nt(
                w,
                &named_node,
                RDF_TYPE,
                &format!("<{SD_NAMED_GRAPH_CLASS}>"),
            )?;
            nt(w, &named_node, SD_NAME, &format!("<{term}>"))?;
            nt(w, &named_node, SD_GRAPH, &subset_node)?;
            written += 4;
        }

        let head = DatasetHead {
            uri: &subset_uri,
            node: &subset_node,
            types: &[VOID_DATASET, SD_GRAPH_CLASS],
            counts: subset.counts(),
        };
        written += write_dataset_description(
            w,
            &head,
            &subset.partitions,
            datatype_index,
            resolver,
            use_blank_nodes,
            bnode_counter,
        )?;
    }
    Ok(written)
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Compute VoID statistics for the given HDT file and write N-Triples to
/// `options.output_path` (or stdout if `None`).
///
/// Returns the number of VoID N-Triples written.
///
/// # Memory
///
/// `memory_limit` controls the PFC block cache used for term resolution during
/// serialization. The analysis data structures (`subject→class` index and partition
/// statistics) use additional memory proportional to the number of typed subjects and
/// class/property combinations in the dataset. `distinct_scope` requires the canonical
/// permutation sidecar and adds one sequential OPS pass. The `All` scope also allocates
/// exact scalar tracking state proportional to the emitted partition combinations; that
/// analysis state is not bounded by `memory_limit`.
///
/// The `Dataset` graph view requires the `.graphs` sidecar, the permutation sidecar, and
/// a graph index with OPS layers. It splits `memory_limit` between the dictionary cache
/// and the membership transposes, and repeats the partition statistics once per graph,
/// so analysis memory grows with graphs × partitions.
pub fn compute_void(hdt_path: &Path, options: &VoidOptions<'_>) -> Result<u64> {
    let by_graph = options.graph_view == VoidGraphView::Dataset;
    let distinct_scope = options.distinct_scope;
    let dictionary_budget = if by_graph {
        options.memory_limit / 2
    } else {
        options.memory_limit
    };

    // Open the HDT file and build the dictionary resolver.
    let (offsets, mut resolver) =
        open_hdt(hdt_path, dictionary_budget).context("Failed to open HDT file")?;
    // The sidecar is the dataset view's defining input, so it is checked first.
    let mut dataset_view = by_graph
        .then(|| {
            DatasetViewInputs::open(
                hdt_path,
                options.temp_dir,
                options.memory_limit - dictionary_budget,
            )
        })
        .transpose()?;
    let needs_ops_pass = distinct_scope.is_some() || by_graph;
    let permutation_index = needs_ops_pass
        .then(|| {
            let perm_path = permutation::canonical_path(hdt_path);
            anyhow::ensure!(
                perm_path.is_file(),
                "{} require permutation index {}; create it with `hdtc perm {}`",
                if distinct_scope.is_some() {
                    "partition distinct counts"
                } else {
                    "per-graph distinct objects"
                },
                perm_path.display(),
                hdt_path.display()
            );
            PermutationIndex::open(&perm_path, hdt_path).with_context(|| {
                format!("Failed to open permutation index {}", perm_path.display())
            })
        })
        .transpose()?;
    let graph_count = dataset_view
        .as_ref()
        .map(|inputs| usize::try_from(inputs.named_graphs() + 1))
        .transpose()
        .context("graph count overflow")?;

    let nb_shared = resolver.shared.string_count;
    let nb_subjects = nb_shared + resolver.subjects.string_count;
    let nb_predicates = resolver.predicates.string_count;
    let nb_objects = nb_shared + resolver.objects.string_count;
    let num_triples = offsets.num_triples;

    tracing::info!(
        "HDT stats: {num_triples} triples, {nb_subjects} subjects, \
         {nb_predicates} predicates, {nb_objects} objects"
    );
    if let Some(inputs) = dataset_view.as_ref() {
        tracing::info!(
            "Graph sidecar: {} named graphs, {} memberships",
            inputs.named_graphs(),
            inputs.sidecar.membership_count()
        );
    }

    // Locate the rdf:type predicate ID in the dictionary.
    let rdf_type_pred_id = resolver
        .locate_predicate(RDF_TYPE.as_bytes())
        .context("Failed to locate rdf:type predicate")?;

    // Pass 1: build class combo index from rdf:type triples.
    let mut raw_class_combos = if let Some(type_pred_id) = rdf_type_pred_id {
        tracing::info!("Pass 1: scanning rdf:type triples (pred_id={type_pred_id})...");
        let memberships = dataset_view
            .as_mut()
            .map(DatasetViewInputs::spo_join)
            .transpose()?;
        build_class_combo_index(hdt_path, &offsets, type_pred_id, nb_subjects, memberships)?
    } else {
        tracing::info!("rdf:type predicate not found; skipping class partition analysis");
        RawClassCombos {
            subject_combos: Vec::new(),
            combo_pairs: Vec::new(),
        }
    };

    // Filter out non-IRI class IDs (blank nodes, literals).
    // The Python tool only considers URIRef classes; blank nodes used as rdf:type
    // objects should not create class partitions or affect type-based counting.
    filter_non_iri_classes(&mut raw_class_combos, &mut resolver)?;
    let class_combo_index = ClassComboIndex::new(raw_class_combos, by_graph);

    // Compute entity counts per class from the class combo index.
    let (class_entity_counts, graph_entity_counts) =
        class_entity_counts(&class_combo_index, graph_count);
    let mut subsets: Vec<GraphSubset> = graph_entity_counts
        .into_iter()
        .map(|entity_counts| GraphSubset {
            triples: 0,
            distinct: DistinctStats::default(),
            partitions: PartitionStats::new(distinct_scope),
            entity_counts,
        })
        .collect();

    // Build datatype index from object-only dictionary entries.
    tracing::info!("Building datatype index from object-only dictionary...");
    let datatype_index = build_datatype_index(&mut resolver, nb_shared, nb_objects)?;

    // Pass 2: full triple scan — dataset-level property counts and class partitions.
    tracing::info!("Pass 2: scanning all triples for statistics...");
    let subset_pass = dataset_view
        .as_mut()
        .map(|inputs| -> Result<_> {
            Ok(SubsetPass {
                join: inputs.spo_join()?,
                subsets: &mut subsets,
            })
        })
        .transpose()?;
    let mut union = run_stats_pass(
        hdt_path,
        &offsets,
        nb_shared,
        &class_combo_index,
        &datatype_index,
        distinct_scope,
        subset_pass,
    )?;

    // Only the `all` scope attributes OPS-ordered triples to class partitions.
    // Every other run is done with the index here, so release it now: at 4 bytes
    // per subject it is the largest allocation this command makes, and holding
    // it across a full extra pass costs gigabytes on large datasets for nothing.
    let class_combo_index = if distinct_scope == Some(PartitionDistinctScope::All) {
        Some(class_combo_index)
    } else {
        drop(class_combo_index);
        None
    };

    if let Some(permutation_index) = permutation_index.as_ref() {
        tracing::info!("Scanning OPS permutation for exact distinct-object counts...");
        let subset_pass = dataset_view
            .as_mut()
            .map(|inputs| -> Result<_> {
                Ok(SubsetPass {
                    join: inputs.ops_join()?,
                    subsets: &mut subsets,
                })
            })
            .transpose()?;
        run_distinct_object_pass(
            permutation_index,
            nb_shared,
            class_combo_index.as_ref(),
            &datatype_index,
            distinct_scope,
            &mut union,
            subset_pass,
        )?;
    }

    // Release the class combo index (unused after the optional OPS pass).
    drop(class_combo_index);

    // Merge entity counts into class partitions.
    union.apply_entity_counts(&class_entity_counts);
    for subset in &mut subsets {
        subset.partitions.apply_entity_counts(&subset.entity_counts);
    }

    tracing::info!(
        "Analysis complete: {} predicates, {} class partitions, {} datatypes, {} languages",
        union.dataset_prop_data.len(),
        union.class_partitions.len(),
        datatype_index.datatype_iris.len(),
        datatype_index.language_tags.len(),
    );

    // Serialize as N-Triples.
    tracing::info!("Serializing VoID statistics as N-Triples...");
    let mut writer = make_writer(options.output_path)?;
    let mut bnode_counter = 0u64;
    let dataset_node = format!("<{}>", options.dataset_uri);
    let union_types: &[&str] = if by_graph {
        &[VOID_DATASET, SD_DATASET]
    } else {
        &[VOID_DATASET]
    };
    let head = DatasetHead {
        uri: options.dataset_uri,
        node: &dataset_node,
        types: union_types,
        counts: DatasetCounts {
            triples: num_triples,
            distinct_subjects: nb_subjects,
            properties: nb_predicates,
            distinct_objects: nb_objects,
        },
    };
    let mut written = write_dataset_description(
        &mut writer,
        &head,
        &union,
        &datatype_index,
        &mut resolver,
        options.use_blank_nodes,
        &mut bnode_counter,
    )?;
    if let Some(inputs) = dataset_view.as_mut() {
        written += write_graph_subsets(
            &mut writer,
            options.dataset_uri,
            &subsets,
            &mut inputs.sidecar,
            &datatype_index,
            &mut resolver,
            options.use_blank_nodes,
            &mut bnode_counter,
        )?;
    }
    writer.flush().context("flush VoID output")?;

    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn optional_distinct_state_leaves_core_dataset_counts_compact() {
        assert_eq!(size_of::<DatasetPropData>(), size_of::<u64>());

        let dataset_only = PartitionDistinctData::new(PartitionDistinctScope::DatasetProperties);
        assert!(dataset_only.class_partitions.is_none());
    }

    /// `compute_void` releases the subject-to-class index before the OPS pass for
    /// every scope but `All`, while `run_distinct_object_pass` demands the index
    /// whenever the nested class hierarchy is present. Both key off the scope, so
    /// the hierarchy must exist for `All` and only for `All`.
    #[test]
    fn nested_class_hierarchy_is_allocated_exactly_for_the_all_scope() {
        assert!(
            PartitionDistinctData::new(PartitionDistinctScope::All)
                .class_partitions
                .is_some()
        );
        assert!(
            PartitionDistinctData::new(PartitionDistinctScope::DatasetProperties)
                .class_partitions
                .is_none()
        );
    }

    #[test]
    fn distinct_tracking_does_not_reserve_zero_as_a_sentinel() {
        let mut stats = DistinctStats::default();
        stats.add_subject(0);
        stats.add_subject(0);
        stats.add_subject(1);
        stats.add_object(0);
        stats.add_object(0);
        stats.add_object(1);

        assert_eq!(stats.distinct_subjects, 2);
        assert_eq!(stats.distinct_objects, 2);
    }
}
