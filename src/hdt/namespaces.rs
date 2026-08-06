//! Namespace inventory construction from sorted HDT dictionary sections.
//!
//! Subject, predicate, and object counts are prefix brackets over PFC sections.
//! The graph-wide count also removes IRIs duplicated between the predicate ID
//! space and the subject/object ID spaces.

use super::reader::{DictionaryResolver, PfcSectionIndex, make_writer, open_hdt};
use anyhow::{Context, Result, bail, ensure};
use oxrdf::NamedNode;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};

const SHARED: usize = 0;
const SUBJECTS: usize = 1;
const PREDICATES: usize = 2;
const OBJECTS: usize = 3;

/// Serialization selected for a namespace inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceOutputFormat {
    Json,
    Yaml,
}

/// Parameters for one namespace inventory.
pub struct NamespaceConfig<'a> {
    pub hdt_path: &'a Path,
    pub prefix_paths: &'a [PathBuf],
    pub output_path: Option<&'a Path>,
    pub format: NamespaceOutputFormat,
    pub include_examples: bool,
    pub memory_limit: usize,
}

/// Counts reported after a namespace inventory is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceSummary {
    pub prefixes_loaded: usize,
    pub namespaces_emitted: usize,
}

#[derive(Serialize)]
struct NamespaceDocument {
    prefix_table: PrefixTableIdentity,
    roles: RolesDocument,
    namespaces: Vec<NamespaceRow>,
}

#[derive(Serialize)]
struct PrefixTableIdentity {
    source: String,
    version: String,
}

#[derive(Serialize)]
struct RolesDocument {
    subject: RoleSummary,
    predicate: RoleSummary,
    object: RoleSummary,
}

#[derive(Serialize)]
struct RoleSummary {
    distinct_iris: u64,
    matched: u64,
    residual: u64,
}

#[derive(Serialize)]
struct NamespaceRow {
    prefix: String,
    namespace: String,
    distinct_iris: u64,
    subject: u64,
    predicate: u64,
    object: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    example: Option<String>,
}

#[derive(Clone)]
struct NamespaceMetrics {
    section_ranges: [Vec<Range<u64>>; 4],
    subject: u64,
    predicate: u64,
    object: u64,
    distinct_iris: u64,
    example: Option<String>,
}

struct SectionMatch {
    ranges: Vec<Range<u64>>,
    count: u64,
    example: Option<Vec<u8>>,
}

/// Build and serialize a namespace inventory.
pub fn write_namespace_inventory(config: NamespaceConfig<'_>) -> Result<NamespaceSummary> {
    ensure!(
        !config.prefix_paths.is_empty(),
        "At least one --prefixes table is required"
    );
    let prefixes = load_prefix_tables(config.prefix_paths)?;
    let prefix_count = prefixes.len();

    let (_, mut dictionary) = open_hdt(config.hdt_path, config.memory_limit)
        .context("Failed to open HDT file for namespace counting")?;
    let iri_ranges = dictionary_iri_ranges(&mut dictionary)?;
    let iri_counts = iri_ranges
        .each_ref()
        .map(|ranges| ranges_len(ranges))
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let iri_counts: [u64; 4] = iri_counts
        .try_into()
        .map_err(|_| anyhow::anyhow!("Internal dictionary section count mismatch"))?;

    // Multiple prefix names may intentionally identify the same namespace. Compute
    // its dictionary ranges once, then emit one row for every name.
    let unique_namespaces: BTreeSet<&str> = prefixes.values().map(String::as_str).collect();
    let mut metrics_by_namespace = BTreeMap::new();
    for namespace in unique_namespaces {
        let metrics = namespace_metrics(
            &mut dictionary,
            &iri_ranges,
            namespace,
            config.include_examples,
        )?;
        metrics_by_namespace.insert(namespace.to_owned(), metrics);
    }

    let mut all_matched: [Vec<Range<u64>>; 4] = std::array::from_fn(|_| Vec::new());
    for metrics in metrics_by_namespace.values() {
        for (all, ranges) in all_matched.iter_mut().zip(&metrics.section_ranges) {
            all.extend(ranges.iter().cloned());
        }
    }
    let all_matched = all_matched.map(merge_ranges);
    let matched_counts = all_matched
        .each_ref()
        .map(|ranges| ranges_len(ranges))
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let matched_counts: [u64; 4] = matched_counts
        .try_into()
        .map_err(|_| anyhow::anyhow!("Internal matched section count mismatch"))?;

    let role_summary = |distinct_iris: u64, matched: u64| -> Result<RoleSummary> {
        Ok(RoleSummary {
            distinct_iris,
            matched,
            residual: distinct_iris
                .checked_sub(matched)
                .context("Matched namespace count exceeds role IRI count")?,
        })
    };
    let roles = RolesDocument {
        subject: role_summary(
            checked_sum(iri_counts[SHARED], iri_counts[SUBJECTS])?,
            checked_sum(matched_counts[SHARED], matched_counts[SUBJECTS])?,
        )?,
        predicate: role_summary(iri_counts[PREDICATES], matched_counts[PREDICATES])?,
        object: role_summary(
            checked_sum(iri_counts[SHARED], iri_counts[OBJECTS])?,
            checked_sum(matched_counts[SHARED], matched_counts[OBJECTS])?,
        )?,
    };

    let mut rows = Vec::new();
    for (prefix, namespace) in &prefixes {
        let metrics = metrics_by_namespace
            .get(namespace)
            .context("Missing computed namespace metrics")?;
        if metrics.subject == 0 && metrics.predicate == 0 && metrics.object == 0 {
            continue;
        }
        rows.push(NamespaceRow {
            prefix: prefix.clone(),
            namespace: namespace.clone(),
            distinct_iris: metrics.distinct_iris,
            subject: metrics.subject,
            predicate: metrics.predicate,
            object: metrics.object,
            example: metrics.example.clone(),
        });
    }

    let document = NamespaceDocument {
        prefix_table: PrefixTableIdentity {
            source: prefix_table_source(config.prefix_paths),
            version: prefix_table_version(&prefixes),
        },
        roles,
        namespaces: rows,
    };

    let mut writer = make_writer(config.output_path)?;
    match config.format {
        NamespaceOutputFormat::Json => {
            serde_json::to_writer_pretty(&mut writer, &document)
                .context("Failed to serialize namespace inventory as JSON")?;
            writer.write_all(b"\n")?;
        }
        NamespaceOutputFormat::Yaml => {
            serde_norway::to_writer(&mut writer, &document)
                .context("Failed to serialize namespace inventory as YAML")?;
        }
    }
    writer.flush()?;

    Ok(NamespaceSummary {
        prefixes_loaded: prefix_count,
        namespaces_emitted: document.namespaces.len(),
    })
}

fn namespace_metrics(
    dictionary: &mut DictionaryResolver,
    iri_ranges: &[Vec<Range<u64>>; 4],
    namespace: &str,
    include_example: bool,
) -> Result<NamespaceMetrics> {
    let namespace_bytes = namespace.as_bytes();
    let shared = section_match(
        &mut dictionary.shared,
        &iri_ranges[SHARED],
        namespace_bytes,
        include_example,
    )?;
    let subjects = section_match(
        &mut dictionary.subjects,
        &iri_ranges[SUBJECTS],
        namespace_bytes,
        include_example,
    )?;
    let predicates = section_match(
        &mut dictionary.predicates,
        &iri_ranges[PREDICATES],
        namespace_bytes,
        include_example,
    )?;
    let objects = section_match(
        &mut dictionary.objects,
        &iri_ranges[OBJECTS],
        namespace_bytes,
        include_example,
    )?;

    let section_ranges = [
        shared.ranges,
        subjects.ranges,
        predicates.ranges,
        objects.ranges,
    ];
    let section_counts = [
        shared.count,
        subjects.count,
        predicates.count,
        objects.count,
    ];
    let subject = checked_sum(shared.count, subjects.count)?;
    let object = checked_sum(shared.count, objects.count)?;
    let distinct_iris = graph_distinct_count(dictionary, &section_ranges, section_counts)?;

    let example = [
        shared.example,
        subjects.example,
        predicates.example,
        objects.example,
    ]
    .into_iter()
    .flatten()
    .min()
    .map(String::from_utf8)
    .transpose()
    .context("HDT dictionary contains a non-UTF-8 IRI")?;

    Ok(NamespaceMetrics {
        section_ranges,
        subject,
        predicate: predicates.count,
        object,
        distinct_iris,
        example,
    })
}

/// Count the union of all four dictionary sections for a namespace.
///
/// Shared, subject-only, and object-only are pairwise disjoint by dictionaryFour
/// construction. Predicates have a separate ID space, so matching predicates are
/// probed against those three sections and duplicate copies are subtracted.
fn graph_distinct_count(
    dictionary: &mut DictionaryResolver,
    section_ranges: &[Vec<Range<u64>>; 4],
    section_counts: [u64; 4],
) -> Result<u64> {
    let mut total = 0u64;
    for count in section_counts {
        total = checked_sum(total, count)?;
    }

    let mut overlap = 0u64;
    let mut term = Vec::new();
    for range in &section_ranges[PREDICATES] {
        for id in range.clone() {
            dictionary
                .predicates
                .get_bytes(id, &mut term)
                .with_context(|| format!("Failed to read predicate dictionary ID {id}"))?;
            for duplicate in [
                dictionary.shared.locate(&term)?,
                dictionary.subjects.locate(&term)?,
                dictionary.objects.locate(&term)?,
            ] {
                if duplicate.is_some() {
                    overlap = overlap
                        .checked_add(1)
                        .context("Namespace overlap count overflow")?;
                }
            }
        }
    }
    total
        .checked_sub(overlap)
        .context("Cross-role overlap exceeds namespace IRI count")
}

fn section_match(
    section: &mut PfcSectionIndex,
    iri_ranges: &[Range<u64>],
    namespace: &[u8],
    include_example: bool,
) -> Result<SectionMatch> {
    let prefix_range = section.prefix_range(namespace)?;
    let ranges = intersect_range(prefix_range, iri_ranges);
    let count = ranges_len(&ranges)?;
    let example = if include_example && let Some(first) = ranges.first() {
        let mut value = Vec::new();
        section.get_bytes(first.start, &mut value)?;
        Some(value)
    } else {
        None
    };
    Ok(SectionMatch {
        ranges,
        count,
        example,
    })
}

fn dictionary_iri_ranges(dictionary: &mut DictionaryResolver) -> Result<[Vec<Range<u64>>; 4]> {
    Ok([
        section_iri_ranges(&mut dictionary.shared)?,
        section_iri_ranges(&mut dictionary.subjects)?,
        section_iri_ranges(&mut dictionary.predicates)?,
        section_iri_ranges(&mut dictionary.objects)?,
    ])
}

/// Return the ranges that are canonical RDF IRIs, excluding literals and bnodes.
fn section_iri_ranges(section: &mut PfcSectionIndex) -> Result<Vec<Range<u64>>> {
    let end = section
        .string_count
        .checked_add(1)
        .context("PFC string count has no past-the-end position")?;
    let excluded = merge_ranges(vec![
        section.prefix_range(b"\"")?,
        section.prefix_range(b"_:")?,
    ]);

    let mut ranges = Vec::new();
    let mut cursor = 1u64;
    for range in excluded {
        if cursor < range.start {
            ranges.push(cursor..range.start);
        }
        cursor = cursor.max(range.end);
    }
    if cursor < end {
        ranges.push(cursor..end);
    }
    Ok(ranges)
}

fn intersect_range(range: Range<u64>, allowed: &[Range<u64>]) -> Vec<Range<u64>> {
    allowed
        .iter()
        .filter_map(|candidate| {
            let start = range.start.max(candidate.start);
            let end = range.end.min(candidate.end);
            (start < end).then_some(start..end)
        })
        .collect()
}

fn merge_ranges(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.retain(|range| range.start < range.end);
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    merged
}

fn ranges_len(ranges: &[Range<u64>]) -> Result<u64> {
    ranges.iter().try_fold(0u64, |total, range| {
        let len = range
            .end
            .checked_sub(range.start)
            .context("Invalid dictionary range")?;
        checked_sum(total, len)
    })
}

fn checked_sum(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).context("Namespace count overflow")
}

fn load_prefix_tables(paths: &[PathBuf]) -> Result<BTreeMap<String, String>> {
    let mut merged = BTreeMap::new();
    for path in paths {
        let bytes = std::fs::read(path)
            .with_context(|| format!("Failed to read prefix table {}", path.display()))?;
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .map(str::to_ascii_lowercase);
        let table: BTreeMap<String, String> = match extension.as_deref() {
            Some("json") => serde_json::from_slice(&bytes)
                .with_context(|| format!("Invalid JSON prefix table {}", path.display()))?,
            Some("yaml" | "yml") => serde_norway::from_slice(&bytes)
                .with_context(|| format!("Invalid YAML prefix table {}", path.display()))?,
            _ => bail!(
                "Prefix table {} must have a .json, .yaml, or .yml extension",
                path.display()
            ),
        };

        for (prefix, namespace) in table {
            ensure!(
                !prefix.is_empty(),
                "Prefix table {} contains an empty prefix name",
                path.display()
            );
            NamedNode::new(&namespace).with_context(|| {
                format!(
                    "Prefix {prefix:?} in {} has an invalid namespace IRI {namespace:?}",
                    path.display()
                )
            })?;
            merged.insert(prefix, namespace);
        }
    }
    Ok(merged)
}

fn prefix_table_source(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(" + ")
}

fn prefix_table_version(prefixes: &BTreeMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    for (prefix, namespace) in prefixes {
        hasher.update((prefix.len() as u64).to_le_bytes());
        hasher.update(prefix.as_bytes());
        hasher.update((namespace.len() as u64).to_le_bytes());
        hasher.update(namespace.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_touching_and_overlapping_ranges() {
        assert_eq!(merge_ranges(vec![8..10, 2..5, 4..8, 12..12]), vec![2..10]);
    }

    #[test]
    fn canonical_prefix_table_version_is_ordered() {
        let first = BTreeMap::from([
            ("b".to_owned(), "http://b.example/".to_owned()),
            ("a".to_owned(), "http://a.example/".to_owned()),
        ]);
        let second = BTreeMap::from([
            ("a".to_owned(), "http://a.example/".to_owned()),
            ("b".to_owned(), "http://b.example/".to_owned()),
        ]);
        assert_eq!(prefix_table_version(&first), prefix_table_version(&second));
    }
}
