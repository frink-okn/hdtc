//! Pipelined HDT construction with backpressure.
//!
//! This module implements a 6-stage pipeline that transforms RDF input into HDT format:
//! 1. Parser - batches RDF quads
//! 2. Batch Vocab Builder - assigns local IDs via hash map
//! 3. Vocab Writer - writes partial vocabularies and local-ID triples to disk
//! 4. Vocab Merger - k-way merges partial vocabs into global dictionary
//! 5. ID Remapper - converts local IDs to global IDs (parallel)
//! 6. BitmapTriples Builder - builds final HDT triples structure
//!
//! Stages are connected by bounded crossbeam channels for automatic backpressure.

mod batch_vocab;
mod partial_vocab;
mod vocab_merger;
mod id_remapper;

pub use batch_vocab::BatchVocabBuilder;
pub use partial_vocab::{PartialVocabEntry, PartialVocabReader, PartialVocabWriter};

use crate::dictionary::DictCounts;
use crate::rdf::{stream_quads, ExtractedQuad, RdfInput};
use crate::sort::ExternalSorter;
use crate::triples::builder::BitmapTriplesData;
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use batch_vocab::{LocalIdTriple, Roles, VocabEntry};

/// Result of pipeline execution.
pub struct PipelineResult {
    pub counts: DictCounts,
    pub dict_sections: Vec<Vec<u8>>, // PFC-encoded sections
    pub bitmap_triples: BitmapTriplesData,
    pub ntriples_size: u64, // N-Triples serialization size of parsed data
}

/// Batch of parsed quads (Stage 1 → Stage 2).
type BatchedQuads = Vec<ExtractedQuad>;

/// Processed batch with sorted vocabulary and local-ID triples (Stage 2 → Stage 3).
struct ProcessedBatch {
    batch_id: usize,
    vocab: Vec<VocabEntry>,
    triples: Vec<LocalIdTriple>,
}

/// Notification that a batch has been written (Stage 3 → Stage 4).
struct BatchComplete {
    batch_id: usize,
    vocab_path: PathBuf,
    triples_path: PathBuf,
    #[allow(dead_code)]
    term_count: usize,
    #[allow(dead_code)]
    triple_count: usize,
}

/// Helper to generate file paths.
fn partial_vocab_path(temp_dir: &Path, batch_id: usize) -> PathBuf {
    temp_dir.join(format!("partial_vocab_{:06}.pvoc.zst", batch_id))
}

fn local_triples_path(temp_dir: &Path, batch_id: usize) -> PathBuf {
    temp_dir.join(format!("local_triples_{:06}.ltr.zst", batch_id))
}

/// Calculate adaptive batch size based on memory budget.
fn calculate_batch_size(memory_budget: usize) -> usize {
    // Reserve 1GB for external sorter and misc overhead
    let available = memory_budget.saturating_sub(1024 * 1024 * 1024);

    // Each triple needs ~150 bytes when buffered across all pipeline stages
    // With 5 batches in flight: ~750 bytes per triple total
    let bytes_per_triple = 150;
    let num_batches_buffered = 5;

    let batch_size = available / (bytes_per_triple * num_batches_buffered);

    // Clamp to reasonable range: 1M - 20M triples per batch
    batch_size.clamp(1_000_000, 20_000_000)
}

/// Stage 1: Parse RDF and batch quads.
fn parser_stage(
    inputs: Vec<RdfInput>,
    batch_size: usize,
    _include_graphs: bool,
    base_uri: String,
    batch_tx: Sender<BatchedQuads>,
) -> Result<u64> {
    let mut current_batch = Vec::with_capacity(batch_size);
    let mut total_quads = 0u64;
    let mut ntriples_size = 0u64;

    for (file_index, input) in inputs.iter().enumerate() {
        tracing::info!("Parsing: {}", input.path.display());

        let parse_stats = stream_quads(input, file_index, Some(&base_uri), |quad| {
            // In triples mode, we include all quads but ignore the graph component
            // (it will be None when building triples)
            current_batch.push(quad);
            total_quads += 1;

            if current_batch.len() >= batch_size {
                // Send batch (blocks if channel is full - backpressure!)
                let batch = std::mem::replace(&mut current_batch, Vec::with_capacity(batch_size));
                if batch_tx.send(batch).is_err() {
                    anyhow::bail!("Batch receiver disconnected");
                }
            }

            Ok(())
        })?;

        // Accumulate original N-Triples size from parser stats
        ntriples_size += parse_stats.original_ntriples_size;
    }

    // Send final partial batch
    if !current_batch.is_empty() {
        batch_tx.send(current_batch).ok();
    }

    tracing::info!("Parsed {} quads total, N-Triples size: {} bytes", total_quads, ntriples_size);
    Ok(ntriples_size)
}

/// Stage 2: Build vocabulary with hash map + arena for each batch.
fn vocab_builder_stage(
    batch_rx: Receiver<BatchedQuads>,
    processed_tx: Sender<ProcessedBatch>,
    include_graphs: bool,
) -> Result<()> {
    let mut batch_id = 0;

    for quads_batch in batch_rx {
        // ~100 bytes per triple for unique term storage after dedup, floor 10MB
        let arena_cap = (quads_batch.len() * 100).max(10_000_000);
        let arena = bumpalo::Bump::with_capacity(arena_cap);
        let expected_terms = (quads_batch.len() * 2).min(5_000_000); // Estimate unique terms
        let mut builder = BatchVocabBuilder::new(&arena, expected_terms);

        // Process quads in this batch
        for quad in &quads_batch {
            let s_id = builder.get_or_assign_id(quad.subject.as_bytes(), Roles::SUBJECT);
            let p_id = builder.get_or_assign_id(quad.predicate.as_bytes(), Roles::PREDICATE);
            let o_id = builder.get_or_assign_id(quad.object.as_bytes(), Roles::OBJECT);

            if include_graphs
                && let Some(ref graph) = quad.graph
            {
                builder.get_or_assign_id(graph.as_bytes(), Roles::GRAPH);
            }

            builder.id_triples.push(LocalIdTriple {
                subject: s_id,
                predicate: p_id,
                object: o_id,
            });
        }

        let stats = builder.stats();
        tracing::info!(
            "Batch {}: {} quads, {} unique terms",
            batch_id,
            stats.num_triples,
            stats.num_terms
        );

        // Finish and get sorted vocab + triples
        let (vocab, triples) = builder.finish();

        // Send to writer stage
        if processed_tx
            .send(ProcessedBatch {
                batch_id,
                vocab,
                triples,
            })
            .is_err()
        {
            anyhow::bail!("Processed batch receiver disconnected");
        }

        batch_id += 1;
    }

    tracing::info!("Processed {} batches total", batch_id);
    Ok(())
}

/// Stage 3: Write partial vocabularies and local-ID triples to disk.
fn vocab_writer_stage(
    processed_rx: Receiver<ProcessedBatch>,
    complete_tx: Sender<BatchComplete>,
    temp_dir: PathBuf,
) -> Result<()> {
    for batch in processed_rx {
        let vocab_path = partial_vocab_path(&temp_dir, batch.batch_id);
        let triples_path = local_triples_path(&temp_dir, batch.batch_id);

        // Write partial vocabulary
        // First, compute max local IDs and entry count
        let entry_count = batch.vocab.len() as u32;
        let mut max_so_id = 0u32;
        let mut max_p_id = 0u32;
        for entry in &batch.vocab {
            if let Some(so_id) = entry.so_local_id {
                max_so_id = max_so_id.max(so_id);
            }
            if let Some(p_id) = entry.p_local_id {
                max_p_id = max_p_id.max(p_id);
            }
        }

        let mut vocab_writer = PartialVocabWriter::create(&vocab_path)
            .with_context(|| format!("Failed to create vocab writer for batch {}", batch.batch_id))?;
        vocab_writer.write_header(entry_count, max_so_id, max_p_id)?;
        for entry in &batch.vocab {
            vocab_writer.write_entry(&PartialVocabEntry::from_vocab_entry(entry))?;
        }
        let term_count = vocab_writer.count() as usize;
        vocab_writer.finish()?;

        // Write local-ID triples
        let triples_file = File::create(&triples_path)
            .with_context(|| format!("Failed to create triples file for batch {}", batch.batch_id))?;
        let buf_writer = BufWriter::new(triples_file);
        let mut encoder = zstd::Encoder::new(buf_writer, 3)?;

        for triple in &batch.triples {
            triple.write_to(&mut encoder)?;
        }

        encoder.finish()?;
        let triple_count = batch.triples.len();

        tracing::info!(
            "Wrote batch {}: {} terms, {} triples",
            batch.batch_id,
            term_count,
            triple_count
        );

        // Notify completion
        if complete_tx
            .send(BatchComplete {
                batch_id: batch.batch_id,
                vocab_path,
                triples_path,
                term_count,
                triple_count,
            })
            .is_err()
        {
            anyhow::bail!("Complete receiver disconnected");
        }
    }

    Ok(())
}

/// Clean up temporary files created during pipeline execution.
fn cleanup_temp_files(batches: &[BatchComplete], mapping_paths: &[PathBuf]) {
    // Delete partial vocabulary files
    for batch in batches {
        if let Err(e) = std::fs::remove_file(&batch.vocab_path) {
            tracing::warn!(
                "Failed to delete partial vocab file {}: {}",
                batch.vocab_path.display(),
                e
            );
        }
    }

    // Delete local triples files
    for batch in batches {
        if let Err(e) = std::fs::remove_file(&batch.triples_path) {
            tracing::warn!(
                "Failed to delete local triples file {}: {}",
                batch.triples_path.display(),
                e
            );
        }
    }

    // Delete ID mapping files
    for path in mapping_paths {
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!(
                "Failed to delete ID mapping file {}: {}",
                path.display(),
                e
            );
        }
    }

    tracing::debug!("Temporary files cleaned up");
}

/// Run the complete pipeline: RDF → HDT.
pub fn run_pipeline(
    inputs: &[RdfInput],
    temp_dir: &Path,
    memory_budget: usize,
    include_graphs: bool,
    base_uri: &str,
) -> Result<PipelineResult> {
    tracing::info!("Starting pipelined HDT construction");

    let batch_size = calculate_batch_size(memory_budget);
    tracing::info!(
        "Batch size: {} triples (memory budget: {} MB)",
        batch_size,
        memory_budget / 1024 / 1024
    );

    // Set up channels with bounded capacities for backpressure
    let (batch_tx, batch_rx) = bounded::<BatchedQuads>(3); // 3 batches buffered
    let (processed_tx, processed_rx) = bounded::<ProcessedBatch>(2); // 2 processed batches buffered
    let (complete_tx, complete_rx) = bounded::<BatchComplete>(10); // 10 completion notifications

    // Spawn stages in separate threads
    let inputs_owned = inputs.to_vec();
    let base_uri_owned = base_uri.to_string();
    let parser_handle = std::thread::spawn(move || {
        parser_stage(
            inputs_owned,
            batch_size,
            include_graphs,
            base_uri_owned,
            batch_tx,
        )
    });

    let builder_handle = std::thread::spawn(move || {
        if let Err(e) = vocab_builder_stage(batch_rx, processed_tx, include_graphs) {
            tracing::error!("Vocab builder stage failed: {}", e);
            return Err(e);
        }
        Ok(())
    });

    let temp_dir_owned = temp_dir.to_path_buf();
    let writer_handle = std::thread::spawn(move || {
        if let Err(e) = vocab_writer_stage(processed_rx, complete_tx, temp_dir_owned) {
            tracing::error!("Vocab writer stage failed: {}", e);
            return Err(e);
        }
        Ok(())
    });

    // Collect batch completions
    let mut batches: Vec<BatchComplete> = Vec::new();
    for batch in complete_rx {
        batches.push(batch);
    }

    tracing::info!("All batches written, {} batches total", batches.len());

    // Wait for all stages to complete
    let ntriples_size = match parser_handle.join() {
        Ok(Ok(size)) => size,
        Ok(Err(e)) => {
            cleanup_temp_files(&batches, &[]);
            return Err(e);
        }
        Err(_) => {
            cleanup_temp_files(&batches, &[]);
            return Err(anyhow::anyhow!("Parser thread panicked"));
        }
    };

    match builder_handle.join() {
        Ok(Err(e)) => {
            cleanup_temp_files(&batches, &[]);
            return Err(e);
        }
        Err(_) => {
            cleanup_temp_files(&batches, &[]);
            return Err(anyhow::anyhow!("Builder thread panicked"));
        }
        Ok(Ok(())) => {}
    }

    match writer_handle.join() {
        Ok(Err(e)) => {
            cleanup_temp_files(&batches, &[]);
            return Err(e);
        }
        Err(_) => {
            cleanup_temp_files(&batches, &[]);
            return Err(anyhow::anyhow!("Writer thread panicked"));
        }
        Ok(Ok(())) => {}
    }

    // Stage 4: Merge vocabularies and build global dictionary
    tracing::info!("Stage 4: Merging vocabularies");
    let batch_infos: Vec<(usize, PathBuf)> = batches
        .iter()
        .map(|b| (b.batch_id, b.vocab_path.clone()))
        .collect();

    let merge_result = vocab_merger::merge_vocabularies(batch_infos, temp_dir)?;

    tracing::info!(
        "Dictionary built: {} shared, {} subjects, {} predicates, {} objects",
        merge_result.counts.shared,
        merge_result.counts.subjects,
        merge_result.counts.predicates,
        merge_result.counts.objects
    );

    // Stage 5: ID remapping (parallel)
    tracing::info!("Stage 5: Remapping local IDs to global IDs");

    // Set up channel for global-ID triples
    let (global_triple_tx, global_triple_rx) = bounded::<IdTriple>(100_000);

    // Prepare batch remap info and track mapping paths for cleanup
    let (remap_tx, remap_rx) = bounded(batches.len());
    let mut mapping_paths: Vec<PathBuf> = Vec::new();
    for batch in &batches {
        let mapping_path = temp_dir.join(format!("id_mapping_{:06}.map.zst", batch.batch_id));
        mapping_paths.push(mapping_path.clone());
        remap_tx
            .send(id_remapper::BatchRemapInfo {
                batch_id: batch.batch_id,
                triples_path: batch.triples_path.clone(),
                mapping_path,
            })
            .ok();
    }
    drop(remap_tx); // Signal completion

    // Spawn remapper in separate thread
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let remapper_handle = std::thread::spawn(move || {
        id_remapper::id_remapper_stage(remap_rx, global_triple_tx, num_cpus)
    });

    // Stage 6: External sort + BitmapTriples construction
    tracing::info!("Stage 6: Sorting global-ID triples in SPO order");

    // Collect triples into external sorter, tracking max IDs for BitmapTriples bit widths
    let mut sorter = ExternalSorter::new(temp_dir, memory_budget);
    let mut buffer: Vec<IdTriple> = Vec::new();
    let mut mem_used: usize = 0;
    let mut triple_count = 0u64;
    let mut max_subject: u64 = 0;
    let mut max_predicate: u64 = 0;
    let mut max_object: u64 = 0;

    for triple in global_triple_rx {
        max_subject = max_subject.max(triple.subject);
        max_predicate = max_predicate.max(triple.predicate);
        max_object = max_object.max(triple.object);
        sorter.push(triple, &mut buffer, &mut mem_used)?;
        triple_count += 1;

        if triple_count.is_multiple_of(10_000_000) {
            tracing::info!("Collected {} triples for sorting", triple_count);
        }
    }

    // Wait for remapper to complete
    let remapped_count = remapper_handle
        .join()
        .map_err(|_| anyhow::anyhow!("Remapper thread panicked"))??;

    tracing::info!("Remapped {} triples, sorting...", remapped_count);

    // Finish sorting
    let sorted_triples = sorter.finish(&mut buffer)?;

    // Build BitmapTriples (max IDs enable single-pass streaming construction)
    tracing::info!("Building BitmapTriples");
    let bitmap_triples = crate::triples::build_bitmap_triples(
        sorted_triples,
        max_subject,
        max_predicate,
        max_object,
    )?;

    tracing::info!(
        "Pipeline complete: {} triples encoded",
        bitmap_triples.num_triples
    );

    // Clean up temporary files
    cleanup_temp_files(&batches, &mapping_paths);

    Ok(PipelineResult {
        counts: merge_result.counts,
        dict_sections: merge_result.dict_sections,
        bitmap_triples,
        ntriples_size,
    })
}
