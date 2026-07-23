//! Bounded-memory quad-pattern search over an HDT/graphs-sidecar pair.

use crate::hdt::reader::{
    BitmapTriplesScanner, DictionaryResolver, HdtSectionOffsets, OutputWriter, make_writer,
    open_hdt, write_nt_object, write_nt_subject, write_triple_tab,
};
use crate::hdt::search::{GraphQueryTerm, QuadPattern, QueryTerm};
use crate::quads::{GraphSidecarReader, GraphTerm, PositionGraphMembership};
use crate::sort::ExternalSorter;
use anyhow::{Context, Result, bail, ensure};
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, Copy)]
struct ResolvedTriplePattern {
    subject: Option<u64>,
    predicate: Option<u64>,
    object: Option<u64>,
}

const MAX_DIRECT_GRAPH_LAYER_PROBES: u64 = 1_000_000;

fn should_probe_wildcard_graph_direct(
    pattern: ResolvedTriplePattern,
    named_graph_count: u64,
    limit: Option<u64>,
    offset: Option<u64>,
) -> bool {
    if pattern.subject.is_none() {
        return false;
    }

    // An exact SPO lookup can match at most one triple, so a single
    // `graphs_of` call is preferable to transposing every membership.
    if pattern.predicate.is_some() && pattern.object.is_some() {
        return true;
    }

    // Each matching triple has at least one membership. With a finite result
    // window, offset + limit therefore bounds the number of `graphs_of` calls.
    // Unbounded scans use the graph-major transpose to avoid O(T * G) reads.
    let Some(limit) = limit else {
        return false;
    };
    offset
        .unwrap_or(0)
        .checked_add(limit)
        .and_then(|needed| {
            named_graph_count
                .checked_add(1)
                .and_then(|layers| needed.checked_mul(layers))
        })
        .is_some_and(|probes| probes <= MAX_DIRECT_GRAPH_LAYER_PROBES)
}

impl ResolvedTriplePattern {
    fn matches(self, subject: u64, predicate: u64, object: u64) -> bool {
        self.subject.is_none_or(|expected| expected == subject)
            && self.predicate.is_none_or(|expected| expected == predicate)
            && self.object.is_none_or(|expected| expected == object)
    }
}

/// Search dataset graph memberships. The query has already been classified as
/// a four-position pattern by the shared CLI parser.
#[allow(clippy::too_many_arguments)]
pub fn search_dataset_streaming(
    hdt_path: &Path,
    pattern: &QuadPattern,
    output: Option<&Path>,
    count_only: bool,
    limit: Option<u64>,
    offset: Option<u64>,
    memory_limit: usize,
    temp_dir: Option<&Path>,
) -> Result<u64> {
    let sidecar_path = crate::quads::canonical_sidecar_path(hdt_path);
    if !sidecar_path.exists() {
        bail!(
            "Four-position query requires graph sidecar {}",
            sidecar_path.display()
        );
    }
    let mut sidecar = GraphSidecarReader::open(&sidecar_path, hdt_path)?;

    let graph_filter = match &pattern.graph {
        GraphQueryTerm::Wildcard => None,
        GraphQueryTerm::DefaultGraph => Some(0),
        GraphQueryTerm::Named(term) => {
            let term = std::str::from_utf8(term).context("Graph query term is not UTF-8")?;
            match sidecar.graph_id_str(term)? {
                Some(graph_id) => Some(graph_id),
                None => return write_empty_result(output, count_only),
            }
        }
    };

    let unconstrained_triple = matches!(pattern.subject, QueryTerm::Wildcard)
        && matches!(pattern.predicate, QueryTerm::Wildcard)
        && matches!(pattern.object, QueryTerm::Wildcard);
    if count_only && unconstrained_triple {
        let count = match graph_filter {
            Some(graph_id) => sidecar.count(graph_id)?,
            None => sidecar.membership_count(),
        };
        return write_count_result(output, count);
    }
    if limit == Some(0) {
        return write_empty_result(output, count_only);
    }

    // Wildcard graph queries transpose the graph-major sidecar into SPO-position
    // order. Split the budget between that external sort and dictionary caches.
    let dictionary_budget = if graph_filter.is_none() {
        memory_limit / 2
    } else {
        memory_limit
    };
    let (offsets, mut dictionary) = open_hdt(hdt_path, dictionary_budget)
        .with_context(|| format!("Failed to open HDT file {}", hdt_path.display()))?;
    let Some(resolved) = resolve_triple_pattern(pattern, &mut dictionary)? else {
        return write_empty_result(output, count_only);
    };

    let mut writer = make_writer(output)?;
    let count = if let Some(graph_id) = graph_filter {
        let fixed_graph = if count_only {
            None
        } else {
            Some(sidecar.graph(graph_id)?)
        };
        let memberships = sidecar.layer_iter(graph_id)?.map(move |position| {
            position.map(|position| PositionGraphMembership {
                position,
                graph: graph_id,
            })
        });
        scan_memberships(
            memberships,
            Some(graph_id),
            fixed_graph.as_ref(),
            &mut sidecar,
            &offsets,
            hdt_path,
            &mut dictionary,
            resolved,
            &mut writer,
            count_only,
            limit,
            offset,
        )?
    } else if should_probe_wildcard_graph_direct(
        resolved,
        sidecar.named_graph_count(),
        limit,
        offset,
    ) {
        // Direct probing is bounded either by an exact SPO lookup or a finite
        // result window. Larger scans use the graph-major transpose below.
        scan_wildcard_graph_direct(
            &mut sidecar,
            &offsets,
            hdt_path,
            &mut dictionary,
            resolved,
            &mut writer,
            count_only,
            limit,
            offset,
        )?
    } else {
        let owned_temp = if temp_dir.is_none() {
            Some(
                tempfile::Builder::new()
                    .prefix("hdtc-search-")
                    .tempdir()
                    .context("Failed to create search temp directory")?,
            )
        } else {
            None
        };
        let sort_dir = match temp_dir {
            Some(path) => {
                std::fs::create_dir_all(path).with_context(|| {
                    format!("Failed to create search temp dir {}", path.display())
                })?;
                path
            }
            None => owned_temp.as_ref().unwrap().path(),
        };
        let sort_budget = memory_limit
            .saturating_sub(dictionary_budget)
            .max(std::mem::size_of::<PositionGraphMembership>());
        let mut sorter = ExternalSorter::new(sort_dir, sort_budget);
        let mut buffer = Vec::<PositionGraphMembership>::new();
        let mut memory_used = 0usize;
        for graph in 0..=sidecar.named_graph_count() {
            for position in sidecar.layer_iter(graph)? {
                sorter.push(
                    PositionGraphMembership {
                        position: position?,
                        graph,
                    },
                    &mut buffer,
                    &mut memory_used,
                )?;
            }
        }
        let memberships = sorter.finish(&mut buffer)?;
        scan_memberships(
            memberships,
            None,
            None,
            &mut sidecar,
            &offsets,
            hdt_path,
            &mut dictionary,
            resolved,
            &mut writer,
            count_only,
            limit,
            offset,
        )?
    };

    if count_only {
        writeln!(writer, "{count}")?;
    }
    writer.flush()?;
    Ok(count)
}

fn resolve_triple_pattern(
    pattern: &QuadPattern,
    dictionary: &mut DictionaryResolver,
) -> Result<Option<ResolvedTriplePattern>> {
    let mut missing = false;
    let subject = match &pattern.subject {
        QueryTerm::Wildcard => None,
        QueryTerm::Bound(term) => match dictionary.locate_subject(term)? {
            Some(id) => Some(id),
            None => {
                missing = true;
                None
            }
        },
    };
    let predicate = match &pattern.predicate {
        QueryTerm::Wildcard => None,
        QueryTerm::Bound(term) => match dictionary.locate_predicate(term)? {
            Some(id) => Some(id),
            None => {
                missing = true;
                None
            }
        },
    };
    let object = match &pattern.object {
        QueryTerm::Wildcard => None,
        QueryTerm::Bound(term) => match dictionary.locate_object(term)? {
            Some(id) => Some(id),
            None => {
                missing = true;
                None
            }
        },
    };

    Ok((!missing).then_some(ResolvedTriplePattern {
        subject,
        predicate,
        object,
    }))
}

#[allow(clippy::too_many_arguments)]
fn scan_memberships<I>(
    memberships: I,
    fixed_graph_id: Option<u64>,
    fixed_graph: Option<&GraphTerm>,
    sidecar: &mut GraphSidecarReader,
    offsets: &HdtSectionOffsets,
    hdt_path: &Path,
    dictionary: &mut DictionaryResolver,
    pattern: ResolvedTriplePattern,
    writer: &mut OutputWriter,
    count_only: bool,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<u64>
where
    I: Iterator<Item = Result<PositionGraphMembership>>,
{
    let mut memberships = memberships.peekable();
    let mut scanner = BitmapTriplesScanner::new(offsets, hdt_path)
        .context("Failed to create BitmapTriples scanner")?;
    let mut position = 0u64;
    let mut remaining_offset = offset.unwrap_or(0);
    let mut count = 0u64;
    let mut subject_buf = Vec::new();
    let mut predicate_buf = Vec::new();
    let mut object_buf = Vec::new();
    let mut cached_graph: Option<(u64, GraphTerm)> = None;

    while let Some((subject, predicate, object)) = scanner.next_triple()? {
        if fixed_graph_id.is_some() && memberships.peek().is_none() {
            return Ok(count);
        }
        if pattern.subject.is_some_and(|expected| subject > expected) {
            return Ok(count);
        }
        let triple_matches = pattern.matches(subject, predicate, object);
        let mut triple_resolved = false;
        let mut memberships_at_position = 0u64;

        loop {
            let at_current_position = match memberships.peek() {
                Some(Ok(membership)) if membership.position < position => {
                    bail!("Graph sidecar membership positions are not monotone")
                }
                Some(Ok(membership)) => membership.position == position,
                Some(Err(_)) => true,
                None => false,
            };
            if !at_current_position {
                break;
            }

            let membership = memberships.next().unwrap()?;
            memberships_at_position += 1;
            if !triple_matches {
                continue;
            }
            if remaining_offset > 0 {
                remaining_offset -= 1;
                continue;
            }

            count = count
                .checked_add(1)
                .context("Search result count overflow")?;
            if !count_only {
                if !triple_resolved {
                    dictionary.subject_term(subject, &mut subject_buf)?;
                    dictionary.predicate_term(predicate, &mut predicate_buf)?;
                    dictionary.object_term(object, &mut object_buf)?;
                    triple_resolved = true;
                }
                let graph = if let Some(graph) = fixed_graph {
                    debug_assert_eq!(fixed_graph_id, Some(membership.graph));
                    graph
                } else {
                    if cached_graph.as_ref().map(|(id, _)| *id) != Some(membership.graph) {
                        cached_graph = Some((membership.graph, sidecar.graph(membership.graph)?));
                    }
                    &cached_graph.as_ref().unwrap().1
                };
                write_statement(writer, &subject_buf, &predicate_buf, &object_buf, graph)?;
            }

            if limit.is_some_and(|limit| count >= limit) {
                return Ok(count);
            }
        }

        if fixed_graph_id.is_none() {
            ensure!(
                memberships_at_position > 0,
                "graph sidecar is not exhaustive at position {position}"
            );
        }
        position += 1;
    }

    if let Some(membership) = memberships.next() {
        let membership = membership?;
        bail!(
            "Graph sidecar contains membership at out-of-range position {}",
            membership.position
        );
    }
    scanner.finish()?;
    Ok(count)
}

/// Wildcard-graph scan for a pattern with a bound subject.
///
/// The SPO scan terminates at the first subject past the bound one, so the
/// membership set of each matching triple can be read directly from the
/// sidecar instead of externally sorting every membership in the file.
#[allow(clippy::too_many_arguments)]
fn scan_wildcard_graph_direct(
    sidecar: &mut GraphSidecarReader,
    offsets: &HdtSectionOffsets,
    hdt_path: &Path,
    dictionary: &mut DictionaryResolver,
    pattern: ResolvedTriplePattern,
    writer: &mut OutputWriter,
    count_only: bool,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<u64> {
    let mut scanner = BitmapTriplesScanner::new(offsets, hdt_path)
        .context("Failed to create BitmapTriples scanner")?;
    let mut position = 0u64;
    let mut remaining_offset = offset.unwrap_or(0);
    let mut count = 0u64;
    let mut subject_buf = Vec::new();
    let mut predicate_buf = Vec::new();
    let mut object_buf = Vec::new();
    let mut cached_graph: Option<(u64, GraphTerm)> = None;

    while let Some((subject, predicate, object)) = scanner.next_triple()? {
        if pattern.subject.is_some_and(|expected| subject > expected) {
            return Ok(count);
        }
        if pattern.matches(subject, predicate, object) {
            let graphs = sidecar.graphs_of(position)?;
            ensure!(
                !graphs.is_empty(),
                "graph sidecar is not exhaustive at position {position}"
            );
            let mut triple_resolved = false;
            for graph_id in graphs {
                if remaining_offset > 0 {
                    remaining_offset -= 1;
                    continue;
                }
                count = count
                    .checked_add(1)
                    .context("Search result count overflow")?;
                if !count_only {
                    if !triple_resolved {
                        dictionary.subject_term(subject, &mut subject_buf)?;
                        dictionary.predicate_term(predicate, &mut predicate_buf)?;
                        dictionary.object_term(object, &mut object_buf)?;
                        triple_resolved = true;
                    }
                    if cached_graph.as_ref().map(|(id, _)| *id) != Some(graph_id) {
                        cached_graph = Some((graph_id, sidecar.graph(graph_id)?));
                    }
                    write_statement(
                        writer,
                        &subject_buf,
                        &predicate_buf,
                        &object_buf,
                        &cached_graph.as_ref().unwrap().1,
                    )?;
                }
                if limit.is_some_and(|limit| count >= limit) {
                    return Ok(count);
                }
            }
        }
        position += 1;
    }
    scanner.finish()?;
    Ok(count)
}

fn write_statement(
    writer: &mut impl Write,
    subject: &[u8],
    predicate: &[u8],
    object: &[u8],
    graph: &GraphTerm,
) -> Result<()> {
    match graph {
        GraphTerm::DefaultGraph => write_triple_tab(writer, subject, predicate, object)?,
        GraphTerm::Named(graph) => {
            write_nt_subject(writer, subject)?;
            writer.write_all(b"\t<")?;
            writer.write_all(predicate)?;
            writer.write_all(b">\t")?;
            write_nt_object(writer, object)?;
            writer.write_all(b"\t")?;
            write_nt_subject(writer, graph.as_bytes())?;
            writer.write_all(b"\t.\n")?;
        }
    }
    Ok(())
}

fn write_empty_result(output: Option<&Path>, count_only: bool) -> Result<u64> {
    let mut writer = make_writer(output)?;
    if count_only {
        writeln!(writer, "0")?;
    }
    writer.flush()?;
    Ok(0)
}

fn write_count_result(output: Option<&Path>, count: u64) -> Result<u64> {
    let mut writer = make_writer(output)?;
    writeln!(writer, "{count}")?;
    writer.flush()?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(
        subject: Option<u64>,
        predicate: Option<u64>,
        object: Option<u64>,
    ) -> ResolvedTriplePattern {
        ResolvedTriplePattern {
            subject,
            predicate,
            object,
        }
    }

    #[test]
    fn direct_graph_probing_requires_a_bound_subject() {
        assert!(!should_probe_wildcard_graph_direct(
            pattern(None, Some(1), Some(1)),
            1,
            Some(1),
            None,
        ));
    }

    #[test]
    fn exact_spo_lookup_always_uses_direct_graph_probing() {
        assert!(should_probe_wildcard_graph_direct(
            pattern(Some(1), Some(1), Some(1)),
            u64::MAX,
            None,
            None,
        ));
    }

    #[test]
    fn unbounded_subject_scan_uses_membership_transpose() {
        assert!(!should_probe_wildcard_graph_direct(
            pattern(Some(1), None, None),
            1,
            None,
            None,
        ));
    }

    #[test]
    fn finite_result_window_bounds_direct_graph_probing() {
        assert!(should_probe_wildcard_graph_direct(
            pattern(Some(1), None, None),
            99,
            Some(10),
            Some(5),
        ));
        assert!(!should_probe_wildcard_graph_direct(
            pattern(Some(1), None, None),
            100_000,
            Some(10),
            Some(5),
        ));
    }

    #[test]
    fn direct_graph_probe_estimate_rejects_overflow() {
        assert!(!should_probe_wildcard_graph_direct(
            pattern(Some(1), None, None),
            u64::MAX,
            Some(1),
            Some(u64::MAX),
        ));
    }
}
