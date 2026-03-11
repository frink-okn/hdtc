//! VoID (Vocabulary of Interlinked Datasets) statistics computation.
//!
//! Implements a two-pass, ID-based algorithm:
//! - Pass 1: Scan all triples to find `rdf:type` triples; build a `ClassComboIndex` that
//!   maps each subject to its deduplicated class combination (4 bytes per subject).
//! - Pass 2: Full sequential scan to accumulate dataset-level and class-level statistics.
//! - Serialize results as N-Triples.
//!
//! The algorithm is equivalent to the Python `void-hdt` tool but uses Rust's u64 integer
//! arithmetic throughout, avoiding the integer overflow that affected hdt-cpp on large inputs
//! like Wikidata.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};

use super::reader::{
    BitmapTriplesScanner, DictionaryResolver, HdtSectionOffsets, find_literal_boundary, make_writer,
    open_hdt,
};

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
struct ClassComboIndex {
    /// Combo ID for each subject, indexed by subject_id (1-based; index 0 unused).
    /// 0 = untyped.
    subject_combos: Vec<u32>,
    /// Sorted class IDs for each combo. `combo_to_classes[combo_id - 1]` gives the
    /// class IDs for `combo_id > 0`.
    combo_to_classes: Vec<Vec<u64>>,
}

impl ClassComboIndex {
    /// Look up the classes for a subject ID.
    #[inline]
    fn classes(&self, subject_id: u64) -> &[u64] {
        let idx = subject_id as usize;
        if idx < self.subject_combos.len() {
            let combo_id = self.subject_combos[idx];
            if combo_id > 0 {
                return &self.combo_to_classes[combo_id as usize - 1];
            }
        }
        &[]
    }

    /// Check if a subject is typed (has any `rdf:type`).
    #[inline]
    fn is_typed(&self, subject_id: u64) -> bool {
        let idx = subject_id as usize;
        idx < self.subject_combos.len() && self.subject_combos[idx] > 0
    }

    /// Distinct class IDs across all combos.
    fn distinct_class_ids(&self) -> std::collections::HashSet<u64> {
        let mut set = std::collections::HashSet::new();
        for classes in &self.combo_to_classes {
            set.extend(classes.iter().copied());
        }
        set
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

// ---------------------------------------------------------------------------
// Internal data structures
// ---------------------------------------------------------------------------

struct PropPartitionData {
    triple_count: u64,
    /// Target class breakdown: `None` = untyped objects (literals or object-only URIs).
    target_classes: HashMap<Option<u64>, u64>,
    /// Datatype/language breakdown for literal objects.
    /// Key is a DatatypeIndex entry ID (>0); non-literals are excluded.
    target_datatypes: HashMap<u16, u64>,
}

/// Dataset-level property partition data (triple count + datatype breakdown).
struct DatasetPropData {
    triple_count: u64,
    /// Datatype/language breakdown for literal objects at dataset level.
    target_datatypes: HashMap<u16, u64>,
}

struct ClassPartitionData {
    entity_count: u64,
    /// Property partitions within this class: predicate_id → data.
    prop_partitions: HashMap<u64, PropPartitionData>,
}

/// Collected statistics from both analysis passes, ready for serialization.
struct VoidStats {
    num_triples: u64,
    nb_subjects: u64,
    nb_predicates: u64,
    nb_objects: u64,
    dataset_prop_data: HashMap<u64, DatasetPropData>,
    class_partitions: HashMap<u64, ClassPartitionData>,
}

impl ClassPartitionData {
    fn total_triples(&self) -> u64 {
        self.prop_partitions.values().map(|p| p.triple_count).sum()
    }
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

/// Scan all triples; collect `rdf:type` triples to build a [`ClassComboIndex`].
///
/// Exploits SPO ordering: all triples for a subject are contiguous, so we buffer
/// each subject's class IDs with O(1) memory per subject, then deduplicate via
/// a combo map.
fn build_class_combo_index(
    hdt_path: &Path,
    offsets: &HdtSectionOffsets,
    rdf_type_pred_id: u64,
    nb_subjects: u64,
) -> Result<ClassComboIndex> {
    let alloc_bytes = (nb_subjects as usize + 1) * std::mem::size_of::<u32>();
    tracing::info!(
        "  Allocating class combo index: {:.1} GB for {} subjects",
        alloc_bytes as f64 / 1_073_741_824.0,
        nb_subjects
    );

    let mut subject_combos = vec![0u32; nb_subjects as usize + 1];
    let mut combo_map: HashMap<Vec<u64>, u32> = HashMap::new();
    let mut combo_to_classes: Vec<Vec<u64>> = Vec::new();

    let mut scanner =
        BitmapTriplesScanner::new(offsets, hdt_path).context("open scanner for Pass 1")?;

    let mut current_subject: u64 = 0;
    let mut current_classes: Vec<u64> = Vec::new();
    let mut scanned: u64 = 0;
    let mut typed_subjects: u64 = 0;

    // Closure-like helper: finalize a subject's collected class IDs into the combo index.
    // Defined inline because closures can't borrow multiple fields mutably.
    macro_rules! finalize_subject {
        () => {
            if !current_classes.is_empty() {
                current_classes.sort_unstable();
                current_classes.dedup();
                let combo_id = if let Some(&id) = combo_map.get(&current_classes) {
                    id
                } else {
                    anyhow::ensure!(
                        combo_to_classes.len() < u32::MAX as usize,
                        "More than {} unique class combinations; dataset too complex for VoID analysis",
                        u32::MAX
                    );
                    let id = combo_to_classes.len() as u32 + 1;
                    let classes = current_classes.clone();
                    combo_map.insert(classes.clone(), id);
                    combo_to_classes.push(classes);
                    id
                };
                subject_combos[current_subject as usize] = combo_id;
                typed_subjects += 1;
                current_classes.clear();
            }
        };
    }

    while let Some((s_id, p_id, o_id)) = scanner.next_triple()? {
        if s_id != current_subject {
            finalize_subject!();
            current_subject = s_id;
        }
        if p_id == rdf_type_pred_id {
            current_classes.push(o_id);
        }
        scanned += 1;
        if scanned.is_multiple_of(10_000_000) {
            tracing::info!("  Pass 1: {scanned} triples scanned...");
        }
    }
    // Finalize last subject.
    finalize_subject!();

    let class_count = {
        let mut seen = std::collections::HashSet::new();
        for classes in &combo_to_classes {
            seen.extend(classes.iter().copied());
        }
        seen.len()
    };
    tracing::info!(
        "  Pass 1 complete: {scanned} triples scanned, {typed_subjects} typed subjects, \
         {class_count} distinct classes, {} unique class combinations",
        combo_to_classes.len()
    );

    Ok(ClassComboIndex {
        subject_combos,
        combo_to_classes,
    })
}

// ---------------------------------------------------------------------------
// Post–Pass 1: filter non-IRI classes
// ---------------------------------------------------------------------------

/// Remove non-IRI class IDs (blank nodes, literals) from the class combo index.
///
/// The Python `void-hdt` tool only treats `URIRef` objects of `rdf:type` triples as
/// valid classes.  Blank nodes used as `rdf:type` objects are common in OWL ontologies
/// (anonymous class expressions) and should not produce class partitions or affect
/// type-based counting.
///
/// After filtering, combos that become empty are mapped to 0 (untyped), and
/// duplicate filtered combos are merged.
fn filter_non_iri_classes(
    class_combo_index: &mut ClassComboIndex,
    resolver: &mut DictionaryResolver,
) -> Result<()> {
    // Collect all unique class IDs across all combos.
    let all_class_ids = class_combo_index.distinct_class_ids();

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
    let mut new_combo_map: HashMap<Vec<u64>, u32> = HashMap::new();
    let mut new_combo_to_classes: Vec<Vec<u64>> = Vec::new();
    // combo_remap[i] = new combo_id for old combo_id (i+1). 0 = became untyped.
    let mut combo_remap: Vec<u32> =
        Vec::with_capacity(class_combo_index.combo_to_classes.len());

    for classes in &class_combo_index.combo_to_classes {
        let filtered: Vec<u64> = classes
            .iter()
            .copied()
            .filter(|c| !non_iri_class_ids.contains(c))
            .collect();

        if filtered.is_empty() {
            combo_remap.push(0);
        } else if let Some(&id) = new_combo_map.get(&filtered) {
            combo_remap.push(id);
        } else {
            let id = new_combo_to_classes.len() as u32 + 1;
            new_combo_to_classes.push(filtered.clone());
            new_combo_map.insert(filtered, id);
            combo_remap.push(id);
        }
    }

    // Remap all subject entries.
    for combo_id in class_combo_index.subject_combos.iter_mut() {
        if *combo_id > 0 {
            *combo_id = combo_remap[*combo_id as usize - 1];
        }
    }

    class_combo_index.combo_to_classes = new_combo_to_classes;

    Ok(())
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

/// Scan all triples to accumulate:
/// - `dataset_prop_data`: total triple count and datatype counts per predicate ID.
/// - `class_partitions`: per-class property, target-class, and datatype breakdowns.
fn run_stats_pass(
    hdt_path: &Path,
    offsets: &HdtSectionOffsets,
    nb_shared: u64,
    class_combo_index: &ClassComboIndex,
    datatype_index: &DatatypeIndex,
) -> Result<(HashMap<u64, DatasetPropData>, HashMap<u64, ClassPartitionData>)> {
    let mut dataset_prop_data: HashMap<u64, DatasetPropData> = HashMap::new();
    let mut class_partitions: HashMap<u64, ClassPartitionData> = HashMap::new();

    let mut scanner =
        BitmapTriplesScanner::new(offsets, hdt_path).context("open scanner for Pass 2")?;

    // Single-entry subject-type cache exploiting HDT's SPO ordering: consecutive triples
    // almost always share the same subject, so caching just the last lookup gives ~100%
    // hit rate with O(1) memory.
    let mut prev_subject_id = u64::MAX;
    let mut current_subject_classes: &[u64] = &[];

    let mut processed = 0u64;
    while let Some((s_id, p_id, o_id)) = scanner.next_triple()? {
        // Datatype lookup for this object (0 = not a literal).
        let dt_id = datatype_index.get(o_id);

        // Dataset-level property count and datatype accumulation.
        let dpd = dataset_prop_data
            .entry(p_id)
            .or_insert_with(|| DatasetPropData {
                triple_count: 0,
                target_datatypes: HashMap::new(),
            });
        dpd.triple_count += 1;
        if dt_id > 0 {
            *dpd.target_datatypes.entry(dt_id).or_insert(0) += 1;
        }

        // Subject type lookup (update cache on subject change).
        if s_id != prev_subject_id {
            prev_subject_id = s_id;
            current_subject_classes = class_combo_index.classes(s_id);
        }

        if current_subject_classes.is_empty() {
            processed += 1;
            if processed.is_multiple_of(10_000_000) {
                tracing::info!(
                    "  Pass 2: {processed}/{} triples processed...",
                    offsets.num_triples
                );
            }
            continue;
        }

        // Object type lookup.
        // Objects with ID > nb_shared are in the object-only section (literals or
        // object-only URIs) and can never appear as subjects of rdf:type triples.
        // Objects in the shared section (ID <= nb_shared) may be typed: look them up
        // in the class combo index (shared IDs appear as both subjects and objects).
        let obj_classes: &[u64] = if o_id <= nb_shared && class_combo_index.is_typed(o_id) {
            class_combo_index.classes(o_id)
        } else {
            &[]
        };

        // Record this triple in every class partition the subject belongs to.
        for &class_id in current_subject_classes {
            let cp = class_partitions
                .entry(class_id)
                .or_insert_with(|| ClassPartitionData {
                    entity_count: 0,
                    prop_partitions: HashMap::new(),
                });
            let pp = cp
                .prop_partitions
                .entry(p_id)
                .or_insert_with(|| PropPartitionData {
                    triple_count: 0,
                    target_classes: HashMap::new(),
                    target_datatypes: HashMap::new(),
                });
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
        }

        processed += 1;
        if processed.is_multiple_of(10_000_000) {
            tracing::info!(
                "  Pass 2: {processed}/{} triples processed...",
                offsets.num_triples
            );
        }
    }

    Ok((dataset_prop_data, class_partitions))
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

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
    use_blank_nodes: bool,
    target_datatypes: &HashMap<u16, u64>,
    datatype_index: &DatatypeIndex,
    bnode_counter: &mut u64,
) -> Result<u64> {
    if target_datatypes.is_empty() {
        return Ok(0);
    }

    let mut written: u64 = 0;

    // Separate entries into non-langString datatypes and language tags.
    let mut datatype_entries: Vec<(u16, u64)> = Vec::new();
    let mut lang_entries: Vec<(u16, u64)> = Vec::new();
    let mut lang_total: u64 = 0;

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
        let dt_part_node = make_partition_node(use_blank_nodes, &dt_part_uri, bnode_counter);

        nt(w, prop_part_node, VOIDEXT_DATATYPE_PARTITION, &dt_part_node)?;
        written += 1;
        nt(w, &dt_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
        written += 1;
        nt(w, &dt_part_node, VOIDEXT_DATATYPE, &format!("<{dt_iri}>"))?;
        written += 1;
        nt(w, &dt_part_node, VOID_TRIPLES, &int_node(*count))?;
        written += 1;
    }

    // Emit rdf:langString datatype partition with nested language partitions.
    if !lang_entries.is_empty() {
        let dt_part_uri = format!("{prop_part_uri}/datatype/{}", md5_hex(RDF_LANG_STRING));
        let dt_part_node = make_partition_node(use_blank_nodes, &dt_part_uri, bnode_counter);

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

        // Nested language partitions.
        for (entry_id, count) in &lang_entries {
            let lang_tag = datatype_index.language_tag(*entry_id);
            let lang_part_uri = format!("{dt_part_uri}/language/{}", md5_hex(lang_tag));
            let lang_part_node =
                make_partition_node(use_blank_nodes, &lang_part_uri, bnode_counter);

            nt(w, &dt_part_node, VOIDEXT_LANGUAGE_PARTITION, &lang_part_node)?;
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
        }
    }

    Ok(written)
}

/// Serialize all VoID statistics as N-Triples, written to `w`.
///
/// Returns the number of N-Triples written.
fn write_void_triples(
    w: &mut impl Write,
    dataset_uri: &str,
    use_blank_nodes: bool,
    stats: &VoidStats,
    datatype_index: &DatatypeIndex,
    resolver: &mut DictionaryResolver,
) -> Result<u64> {
    let mut written: u64 = 0;
    let mut bnode_counter: u64 = 0;
    let dataset_node = format!("<{dataset_uri}>");

    // Reusable term buffer for dictionary lookups.
    let mut term_buf = Vec::<u8>::new();

    // -----------------------------------------------------------------------
    // 1. Dataset-level statistics
    // -----------------------------------------------------------------------
    nt(w, &dataset_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
    written += 1;
    nt(w, &dataset_node, VOID_TRIPLES, &int_node(stats.num_triples))?;
    written += 1;
    nt(w, &dataset_node, VOID_DISTINCT_SUBJECTS, &int_node(stats.nb_subjects))?;
    written += 1;
    nt(w, &dataset_node, VOID_PROPERTIES, &int_node(stats.nb_predicates))?;
    written += 1;
    nt(w, &dataset_node, VOID_DISTINCT_OBJECTS, &int_node(stats.nb_objects))?;
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
        let part_node = make_partition_node(use_blank_nodes, &part_uri, &mut bnode_counter);

        nt(w, &dataset_node, VOID_PROPERTY_PARTITION, &part_node)?;
        written += 1;
        nt(w, &part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
        written += 1;
        nt(w, &part_node, VOID_PROPERTY, &format!("<{pred_iri}>"))?;
        written += 1;
        nt(w, &part_node, VOID_TRIPLES, &int_node(dpd.triple_count))?;
        written += 1;

        // Datatype/language partitions for this dataset-level property partition.
        written += write_datatype_partitions(
            w,
            &part_uri,
            &part_node,
            use_blank_nodes,
            &dpd.target_datatypes,
            datatype_index,
            &mut bnode_counter,
        )?;
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

        // Resolve the class IRI (class_id is an object ID in the HDT dictionary).
        class_buf.clear();
        resolver.object_term(*class_id, &mut class_buf)?;
        if !is_iri(&class_buf) {
            continue; // Skip blank-node or literal "classes".
        }
        let class_iri = String::from_utf8_lossy(&class_buf).into_owned();
        let class_part_uri = format!("{dataset_uri}/class/{}", md5_hex(&class_iri));
        let class_part_node =
            make_partition_node(use_blank_nodes, &class_part_uri, &mut bnode_counter);

        nt(w, &dataset_node, VOID_CLASS_PARTITION, &class_part_node)?;
        written += 1;
        nt(w, &class_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
        written += 1;
        nt(w, &class_part_node, VOID_CLASS, &format!("<{class_iri}>"))?;
        written += 1;
        nt(w, &class_part_node, VOID_ENTITIES, &int_node(cp.entity_count))?;
        written += 1;
        nt(w, &class_part_node, VOID_TRIPLES, &int_node(cp.total_triples()))?;
        written += 1;

        // Nested property partitions within this class.
        let mut prop_ids: Vec<u64> = cp.prop_partitions.keys().copied().collect();
        prop_ids.sort_unstable();

        for prop_id in &prop_ids {
            let pp = &cp.prop_partitions[prop_id];

            term_buf.clear();
            resolver.predicate_term(*prop_id, &mut term_buf)?;
            if !is_iri(&term_buf) {
                continue;
            }
            let pred_iri = String::from_utf8_lossy(&term_buf).into_owned();
            let prop_part_uri = format!("{class_part_uri}/property/{}", md5_hex(&pred_iri));
            let prop_part_node =
                make_partition_node(use_blank_nodes, &prop_part_uri, &mut bnode_counter);

            nt(w, &class_part_node, VOID_PROPERTY_PARTITION, &prop_part_node)?;
            written += 1;
            nt(w, &prop_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
            written += 1;
            nt(w, &prop_part_node, VOID_PROPERTY, &format!("<{pred_iri}>"))?;
            written += 1;
            nt(w, &prop_part_node, VOID_TRIPLES, &int_node(pp.triple_count))?;
            written += 1;

            // Target class partitions (objectClassPartition).
            // Sort: untyped (None) last, typed by class_id for determinism.
            let mut target_keys: Vec<Option<u64>> = pp.target_classes.keys().copied().collect();
            target_keys.sort_unstable_by_key(|k| k.unwrap_or(u64::MAX));

            for target_class_opt in &target_keys {
                let tc_count = pp.target_classes[target_class_opt];

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
                let target_part_uri =
                    format!("{prop_part_uri}/target/{}", md5_hex(hash_input));
                let target_part_node =
                    make_partition_node(use_blank_nodes, &target_part_uri, &mut bnode_counter);

                nt(w, &prop_part_node, VOIDEXT_OBJECT_CLASS_PARTITION, &target_part_node)?;
                written += 1;
                nt(w, &target_part_node, RDF_TYPE, &format!("<{VOID_DATASET}>"))?;
                written += 1;
                if let Some(ref tc_iri) = target_iri_opt {
                    nt(w, &target_part_node, VOID_CLASS, &format!("<{tc_iri}>"))?;
                    written += 1;
                }
                nt(w, &target_part_node, VOID_TRIPLES, &int_node(tc_count))?;
                written += 1;
            }

            // Datatype/language partitions for this class-level property partition.
            written += write_datatype_partitions(
                w,
                &prop_part_uri,
                &prop_part_node,
                use_blank_nodes,
                &pp.target_datatypes,
                datatype_index,
                &mut bnode_counter,
            )?;
        }
    }

    w.flush().context("flush VoID output")?;
    Ok(written)
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Compute VoID statistics for the given HDT file and write N-Triples to
/// `output_path` (or stdout if `None`).
///
/// Returns the number of VoID N-Triples written.
///
/// # Memory
///
/// `memory_limit` controls the PFC block cache used for term resolution during
/// serialization. The analysis data structures (`subject→class` index and partition
/// statistics) use additional memory proportional to the number of typed subjects and
/// class/property combinations in the dataset.
pub fn compute_void(
    hdt_path: &Path,
    dataset_uri: &str,
    output_path: Option<&Path>,
    use_blank_nodes: bool,
    memory_limit: usize,
) -> Result<u64> {
    // Open the HDT file and build the dictionary resolver.
    let (offsets, mut resolver) =
        open_hdt(hdt_path, memory_limit).context("Failed to open HDT file")?;

    let nb_shared = resolver.shared.string_count;
    let nb_subjects = nb_shared + resolver.subjects.string_count;
    let nb_predicates = resolver.predicates.string_count;
    let nb_objects = nb_shared + resolver.objects.string_count;
    let num_triples = offsets.num_triples;

    tracing::info!(
        "HDT stats: {num_triples} triples, {nb_subjects} subjects, \
         {nb_predicates} predicates, {nb_objects} objects"
    );

    // Locate the rdf:type predicate ID in the dictionary.
    let rdf_type_pred_id = resolver
        .locate_predicate(RDF_TYPE.as_bytes())
        .context("Failed to locate rdf:type predicate")?;

    // Pass 1: build class combo index from rdf:type triples.
    let mut class_combo_index = if let Some(type_pred_id) = rdf_type_pred_id {
        tracing::info!("Pass 1: scanning rdf:type triples (pred_id={type_pred_id})...");
        build_class_combo_index(hdt_path, &offsets, type_pred_id, nb_subjects)?
    } else {
        tracing::info!("rdf:type predicate not found; skipping class partition analysis");
        ClassComboIndex {
            subject_combos: Vec::new(),
            combo_to_classes: Vec::new(),
        }
    };

    // Filter out non-IRI class IDs (blank nodes, literals).
    // The Python tool only considers URIRef classes; blank nodes used as rdf:type
    // objects should not create class partitions or affect type-based counting.
    filter_non_iri_classes(&mut class_combo_index, &mut resolver)?;

    // Compute entity counts per class from the class combo index.
    let mut class_entity_counts: HashMap<u64, u64> = HashMap::new();
    for &combo_id in &class_combo_index.subject_combos {
        if combo_id > 0 {
            for &class_id in &class_combo_index.combo_to_classes[combo_id as usize - 1] {
                *class_entity_counts.entry(class_id).or_insert(0) += 1;
            }
        }
    }

    // Build datatype index from object-only dictionary entries.
    tracing::info!("Building datatype index from object-only dictionary...");
    let datatype_index = build_datatype_index(&mut resolver, nb_shared, nb_objects)?;

    // Pass 2: full triple scan — dataset-level property counts and class partitions.
    tracing::info!("Pass 2: scanning all triples for statistics...");
    let (dataset_prop_data, mut class_partitions) =
        run_stats_pass(hdt_path, &offsets, nb_shared, &class_combo_index, &datatype_index)?;

    // Release the class combo index (no longer needed).
    drop(class_combo_index);

    // Merge entity counts into class_partitions.
    for (class_id, cp) in class_partitions.iter_mut() {
        cp.entity_count = *class_entity_counts.get(class_id).unwrap_or(&0);
    }

    let stats = VoidStats {
        num_triples,
        nb_subjects,
        nb_predicates,
        nb_objects,
        dataset_prop_data,
        class_partitions,
    };

    tracing::info!(
        "Analysis complete: {} predicates, {} class partitions, {} datatypes, {} languages",
        stats.dataset_prop_data.len(),
        stats.class_partitions.len(),
        datatype_index.datatype_iris.len(),
        datatype_index.language_tags.len(),
    );

    // Serialize as N-Triples.
    tracing::info!("Serializing VoID statistics as N-Triples...");
    let mut writer = make_writer(output_path)?;
    let written = write_void_triples(
        &mut writer,
        dataset_uri,
        use_blank_nodes,
        &stats,
        &datatype_index,
        &mut resolver,
    )?;

    Ok(written)
}
