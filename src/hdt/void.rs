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
    BitmapTriplesScanner, DictionaryResolver, HdtSectionOffsets, make_writer, open_hdt,
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
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

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
    dataset_prop_counts: HashMap<u64, u64>,
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
// Pass 2: Accumulate statistics
// ---------------------------------------------------------------------------

/// Scan all triples to accumulate:
/// - `dataset_prop_counts`: total triple count per predicate ID.
/// - `class_partitions`: per-class property and target-class breakdowns.
fn run_stats_pass(
    hdt_path: &Path,
    offsets: &HdtSectionOffsets,
    nb_shared: u64,
    class_combo_index: &ClassComboIndex,
) -> Result<(HashMap<u64, u64>, HashMap<u64, ClassPartitionData>)> {
    let mut dataset_prop_counts: HashMap<u64, u64> = HashMap::new();
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
        // Dataset-level property count (all triples, including untyped subjects).
        *dataset_prop_counts.entry(p_id).or_insert(0) += 1;

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
                });
            pp.triple_count += 1;
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

    Ok((dataset_prop_counts, class_partitions))
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

/// Serialize all VoID statistics as N-Triples, written to `w`.
///
/// Returns the number of N-Triples written.
fn write_void_triples(
    w: &mut impl Write,
    dataset_uri: &str,
    use_blank_nodes: bool,
    stats: &VoidStats,
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
    let mut pred_ids: Vec<u64> = stats.dataset_prop_counts.keys().copied().collect();
    pred_ids.sort_unstable();

    for pred_id in &pred_ids {
        let count = stats.dataset_prop_counts[pred_id];
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
        nt(w, &part_node, VOID_TRIPLES, &int_node(count))?;
        written += 1;
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

    // Pass 2: full triple scan — dataset-level property counts and class partitions.
    tracing::info!("Pass 2: scanning all triples for statistics...");
    let (dataset_prop_counts, mut class_partitions) =
        run_stats_pass(hdt_path, &offsets, nb_shared, &class_combo_index)?;

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
        dataset_prop_counts,
        class_partitions,
    };

    tracing::info!(
        "Analysis complete: {} predicates, {} class partitions",
        stats.dataset_prop_counts.len(),
        stats.class_partitions.len()
    );

    // Serialize as N-Triples.
    tracing::info!("Serializing VoID statistics as N-Triples...");
    let mut writer = make_writer(output_path)?;
    let written = write_void_triples(&mut writer, dataset_uri, use_blank_nodes, &stats, &mut resolver)?;

    Ok(written)
}
