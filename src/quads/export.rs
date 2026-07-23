//! Streaming lossless N-Quads export from an HDT/graphs-sidecar pair.

use crate::hdt::reader::{
    BitmapTriplesScanner, make_writer, open_hdt, write_nt_object, write_nt_subject,
};
use crate::quads::{GraphSidecarReader, GraphTerm, PositionGraphMembership};
use crate::sort::ExternalSorter;
use anyhow::{Context, Result, ensure};
use std::io::Write;
use std::path::Path;

/// Export one N-Quads statement per distinct graph membership. Memberships are
/// transposed with the external sorter, so memory remains bounded independently
/// of the number of triples and graphs.
pub fn export_dataset_nquads(
    hdt_path: &Path,
    output: Option<&Path>,
    temp_dir: &Path,
    memory_limit: usize,
) -> Result<u64> {
    let dictionary_budget = memory_limit / 2;
    let sort_budget = memory_limit
        .saturating_sub(dictionary_budget)
        .max(std::mem::size_of::<PositionGraphMembership>());
    let (offsets, mut dictionary) = open_hdt(hdt_path, dictionary_budget)?;
    let mut scanner = BitmapTriplesScanner::new(&offsets, hdt_path)?;
    let mut sidecar = GraphSidecarReader::open_for_hdt(hdt_path)?;
    ensure!(
        sidecar.triple_count() == offsets.num_triples,
        "graph sidecar triple count differs from HDT"
    );

    let mut sorter = ExternalSorter::new(temp_dir, sort_budget);
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
    let mut memberships = sorter.finish(&mut buffer)?.peekable();
    let mut writer = make_writer(output)?;
    let mut subject = Vec::new();
    let mut predicate = Vec::new();
    let mut object = Vec::new();
    let mut position = 0u64;
    let mut written = 0u64;
    // Memberships arrive in position order, so consecutive rows repeat graph
    // IDs. Caching the last term avoids a PFC block decode per statement.
    let mut cached_graph: Option<(u64, GraphTerm)> = None;

    while let Some((subject_id, predicate_id, object_id)) = scanner.next_triple()? {
        dictionary.subject_term(subject_id, &mut subject)?;
        dictionary.predicate_term(predicate_id, &mut predicate)?;
        dictionary.object_term(object_id, &mut object)?;
        let mut memberships_at_position = 0u64;
        while memberships
            .peek()
            .is_some_and(|item| item.as_ref().is_ok_and(|item| item.position == position))
        {
            let membership = memberships.next().unwrap()?;
            write_nt_subject(&mut writer, &subject)?;
            writer.write_all(b"\t<")?;
            writer.write_all(&predicate)?;
            writer.write_all(b">\t")?;
            write_nt_object(&mut writer, &object)?;
            if cached_graph.as_ref().map(|(id, _)| *id) != Some(membership.graph) {
                cached_graph = Some((membership.graph, sidecar.graph(membership.graph)?));
            }
            match &cached_graph.as_ref().unwrap().1 {
                GraphTerm::DefaultGraph => writer.write_all(b"\t.\n")?,
                GraphTerm::Named(graph) => {
                    writer.write_all(b"\t")?;
                    write_nt_subject(&mut writer, graph.as_bytes())?;
                    writer.write_all(b"\t.\n")?;
                }
            }
            memberships_at_position += 1;
            written = written.checked_add(1).context("N-Quads count overflow")?;
        }
        ensure!(
            memberships_at_position > 0,
            "graph sidecar is not exhaustive at position {position}"
        );
        position += 1;
    }
    ensure!(
        memberships.next().is_none(),
        "graph sidecar contains positions beyond the HDT"
    );
    ensure!(
        written == sidecar.membership_count(),
        "graph sidecar membership count mismatch"
    );
    scanner.finish()?;
    writer.flush()?;
    Ok(written)
}
