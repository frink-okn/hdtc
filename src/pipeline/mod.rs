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
use crate::rdf::{stream_quads_with_options, ExtractedQuad, ParseOptions, RdfInput};
use crate::sort::ExternalSorter;
use crate::triples::builder::BitmapTriplesData;
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use batch_vocab::{LocalIdTriple, Roles, VocabEntry};

const STAGE5_TO_STAGE6_CHUNK_SIZE: usize = 16_384;

#[derive(Debug, Clone, Default)]
pub struct ParserParallelismConfig {
    pub file_workers: Option<usize>,
    pub chunk_workers: Option<usize>,
    pub chunk_size_bytes: Option<usize>,
    pub max_inflight_bytes: Option<usize>,
}

/// Result of pipeline execution.
pub struct PipelineResult {
    pub counts: DictCounts,
    pub dict_sections: Vec<Vec<u8>>, // PFC-encoded sections
    pub bitmap_triples: BitmapTriplesData,
    pub ntriples_size: u64, // N-Triples serialization size of parsed data
}

/// Batch of parsed quads (Stage 1 → Stage 2).
type BatchedQuads = Vec<ExtractedQuad>;

#[derive(Default)]
struct BatchAssemblerState {
    current_batch: BatchedQuads,
    total_quads: u64,
}

struct SharedBatchAssembler {
    batch_size: usize,
    batch_tx: Sender<BatchedQuads>,
    state: Mutex<BatchAssemblerState>,
}

impl SharedBatchAssembler {
    fn new(batch_size: usize, batch_tx: Sender<BatchedQuads>) -> Self {
        Self {
            batch_size,
            batch_tx,
            state: Mutex::new(BatchAssemblerState {
                current_batch: Vec::with_capacity(batch_size),
                total_quads: 0,
            }),
        }
    }

    fn push_many(&self, quads: Vec<ExtractedQuad>) -> Result<()> {
        let mut full_batches: Vec<BatchedQuads> = Vec::new();

        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("Batch assembler mutex poisoned"))?;

            for quad in quads {
                state.current_batch.push(quad);
                state.total_quads += 1;

                if state.current_batch.len() >= self.batch_size {
                    let batch = std::mem::replace(
                        &mut state.current_batch,
                        Vec::with_capacity(self.batch_size),
                    );
                    full_batches.push(batch);
                }
            }
        }

        for batch in full_batches {
            self.batch_tx
                .send(batch)
                .map_err(|_| anyhow::anyhow!("Batch receiver disconnected"))?;
        }

        Ok(())
    }

    fn flush_final(&self) -> Result<()> {
        let final_batch = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("Batch assembler mutex poisoned"))?;
            if state.current_batch.is_empty() {
                None
            } else {
                Some(std::mem::replace(
                    &mut state.current_batch,
                    Vec::with_capacity(self.batch_size),
                ))
            }
        };

        if let Some(batch) = final_batch {
            self.batch_tx
                .send(batch)
                .map_err(|_| anyhow::anyhow!("Batch receiver disconnected"))?;
        }
        Ok(())
    }

    fn total_quads(&self) -> Result<u64> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Batch assembler mutex poisoned"))?;
        Ok(state.total_quads)
    }
}

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
}

#[derive(Debug)]
struct StageMetric {
    name: &'static str,
    duration: Duration,
    peak_rss_bytes: u64,
    peak_rss_delta_bytes: u64,
}

#[cfg(target_os = "linux")]
fn process_peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    if usage.ru_maxrss < 0 {
        return None;
    }
    Some((usage.ru_maxrss as u64) * 1024)
}

#[cfg(not(target_os = "linux"))]
fn process_peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    if usage.ru_maxrss < 0 {
        return None;
    }
    Some(usage.ru_maxrss as u64)
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GiB", b / GB)
    } else if b >= MB {
        format!("{:.2} MiB", b / MB)
    } else if b >= KB {
        format!("{:.2} KiB", b / KB)
    } else {
        format!("{} B", bytes)
    }
}

fn push_stage_metric(
    metrics: &mut Vec<StageMetric>,
    name: &'static str,
    start: Instant,
    rss_before: Option<u64>,
    benchmark: bool,
) {
    if !benchmark {
        return;
    }

    let peak_rss_bytes = process_peak_rss_bytes().unwrap_or(0);
    let peak_rss_delta_bytes = match rss_before {
        Some(before) => peak_rss_bytes.saturating_sub(before),
        None => 0,
    };
    metrics.push(StageMetric {
        name,
        duration: start.elapsed(),
        peak_rss_bytes,
        peak_rss_delta_bytes,
    });
}

fn log_benchmark_summary(metrics: &[StageMetric], benchmark: bool) {
    if !benchmark || metrics.is_empty() {
        return;
    }

    tracing::info!("Benchmark summary (pipeline):");
    for metric in metrics {
        tracing::info!(
            "  {:<28} time {:>8.3}s | peak RSS {:>10} | +{}",
            metric.name,
            metric.duration.as_secs_f64(),
            format_bytes(metric.peak_rss_bytes),
            format_bytes(metric.peak_rss_delta_bytes)
        );
    }
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
    memory_budget: usize,
    _include_graphs: bool,
    base_uri: String,
    parser_parallelism: ParserParallelismConfig,
    batch_tx: Sender<BatchedQuads>,
) -> Result<u64> {
    let expected_files = inputs.len();
    let disambiguate_blank_nodes = inputs.len() > 1;
    let available_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1);
    let default_file_workers = inputs.len().min(available_cpus).max(1);
    let file_workers = parser_parallelism
        .file_workers
        .unwrap_or(default_file_workers)
        .max(1)
        .min(inputs.len().max(1));
    let default_chunk_workers = (available_cpus / file_workers).max(1);
    let capped_default_chunk_workers = default_chunk_workers.min(4);
    let chunk_workers = parser_parallelism
        .chunk_workers
        .unwrap_or(capped_default_chunk_workers)
        .max(1);

    // Keep parser memory bounded relative to global memory budget.
    // Reserve most memory for downstream stages and sorting.
    let parser_budget_total = (memory_budget / 8).clamp(64 * 1024 * 1024, 2 * 1024 * 1024 * 1024);
    let parser_budget_per_file = (parser_budget_total / file_workers.max(1)).max(16 * 1024 * 1024);

    let chunk_size_bytes = parser_parallelism
        .chunk_size_bytes
        .unwrap_or((parser_budget_per_file / 8).clamp(1 * 1024 * 1024, 8 * 1024 * 1024))
        .max(1);
    let max_inflight_bytes = parser_parallelism
        .max_inflight_bytes
        .unwrap_or((parser_budget_per_file / 2).max(chunk_size_bytes))
        .max(chunk_size_bytes);

    tracing::info!(
        "Parser parallelism: {} file worker(s), {} chunk worker(s)/file, {} chunk bytes, {} in-flight bytes/file (parser budget total: {})",
        file_workers,
        chunk_workers,
        chunk_size_bytes,
        max_inflight_bytes,
        parser_budget_total
    );

    let parse_options = ParseOptions {
        enable_ntnq_parallel: true,
        chunk_size_bytes,
        chunk_workers,
        max_inflight_bytes,
    };

    let assembler = Arc::new(SharedBatchAssembler::new(batch_size, batch_tx));

    let (file_tx, file_rx) = bounded::<(usize, RdfInput)>(inputs.len().max(1));
    for (file_index, input) in inputs.into_iter().enumerate() {
        file_tx
            .send((file_index, input))
            .map_err(|_| anyhow::anyhow!("File parser queue disconnected"))?;
    }
    drop(file_tx);

    let (stats_tx, stats_rx) = crossbeam_channel::unbounded::<(usize, Result<u64>)>();
    let mut worker_handles = Vec::with_capacity(file_workers);

    for _ in 0..file_workers {
        let file_rx = file_rx.clone();
        let stats_tx = stats_tx.clone();
        let assembler = Arc::clone(&assembler);
        let base_uri = base_uri.clone();
        let parse_options = parse_options.clone();

        worker_handles.push(std::thread::spawn(move || {
            for (file_index, input) in file_rx {
                tracing::info!("Parsing: {}", input.path.display());

                let mut staged_quads = Vec::with_capacity(4096);
                let parse_result = stream_quads_with_options(
                    &input,
                    file_index,
                    disambiguate_blank_nodes,
                    Some(&base_uri),
                    &parse_options,
                    |quad| {
                        staged_quads.push(quad);
                        if staged_quads.len() >= 4096 {
                            let chunk = std::mem::take(&mut staged_quads);
                            assembler.push_many(chunk)?;
                            staged_quads = Vec::with_capacity(4096);
                        }
                        Ok(())
                    },
                )
                .and_then(|stats| {
                    if !staged_quads.is_empty() {
                        let chunk = std::mem::take(&mut staged_quads);
                        assembler.push_many(chunk)?;
                    }
                    Ok(stats.original_ntriples_size)
                });

                if stats_tx.send((file_index, parse_result)).is_err() {
                    return;
                }
            }
        }));
    }
    drop(stats_tx);

    for handle in worker_handles {
        if handle.join().is_err() {
            return Err(anyhow::anyhow!("Parser worker thread panicked"));
        }
    }

    let mut ntriples_size = 0u64;
    let mut outcomes = 0usize;
    let mut first_error = None;
    for (file_index, parse_result) in stats_rx {
        outcomes += 1;
        match parse_result {
            Ok(size) => ntriples_size += size,
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!(
                        "Parser failed for file index {}: {}",
                        file_index,
                        e
                    ));
                }
            }
        }
    }

    if outcomes != expected_files {
        return Err(anyhow::anyhow!(
            "Parser outcomes mismatch: expected {}, got {}",
            expected_files,
            outcomes
        ));
    }

    if let Some(e) = first_error {
        return Err(e);
    }

    assembler.flush_final()?;
    let total_quads = assembler.total_quads()?;

    tracing::info!(
        "Parsed {} quads total, N-Triples size: {} bytes",
        total_quads,
        ntriples_size
    );
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
    parser_parallelism: &ParserParallelismConfig,
    benchmark: bool,
) -> Result<PipelineResult> {
    tracing::info!("Starting pipelined HDT construction");

    let mut stage_metrics: Vec<StageMetric> = Vec::new();
    let rss_start = if benchmark { process_peak_rss_bytes() } else { None };
    let stages123_start = Instant::now();

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
    let parser_parallelism_owned = parser_parallelism.clone();
    let parser_handle = std::thread::spawn(move || {
        parser_stage(
            inputs_owned,
            batch_size,
            memory_budget,
            include_graphs,
            base_uri_owned,
            parser_parallelism_owned,
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

    push_stage_metric(
        &mut stage_metrics,
        "Stages 1-3 parse/build/write",
        stages123_start,
        rss_start,
        benchmark,
    );

    // Stage 4: Merge vocabularies and build global dictionary
    tracing::info!("Stage 4: Merging vocabularies");
    let stage4_start = Instant::now();
    let stage4_rss_before = if benchmark { process_peak_rss_bytes() } else { None };
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

    push_stage_metric(
        &mut stage_metrics,
        "Stage 4 vocab merge",
        stage4_start,
        stage4_rss_before,
        benchmark,
    );

    // Stage 5: ID remapping (parallel)
    tracing::info!("Stage 5: Remapping local IDs to global IDs");
    let stage5_start = Instant::now();
    let stage5_rss_before = if benchmark { process_peak_rss_bytes() } else { None };

    // Set up channel for global-ID triples (chunked to reduce per-message overhead)
    let (global_triple_tx, global_triple_rx) = bounded::<Vec<IdTriple>>(256);

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
        id_remapper::id_remapper_stage(
            remap_rx,
            global_triple_tx,
            num_cpus,
            STAGE5_TO_STAGE6_CHUNK_SIZE,
        )
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

    for triple_chunk in global_triple_rx {
        for triple in triple_chunk {
            max_subject = max_subject.max(triple.subject);
            max_predicate = max_predicate.max(triple.predicate);
            max_object = max_object.max(triple.object);
            sorter.push(triple, &mut buffer, &mut mem_used)?;
            triple_count += 1;

            if triple_count.is_multiple_of(10_000_000) {
                tracing::info!("Collected {} triples for sorting", triple_count);
            }
        }
    }

    // Wait for remapper to complete
    let remapped_count = remapper_handle
        .join()
        .map_err(|_| anyhow::anyhow!("Remapper thread panicked"))??;

    tracing::info!("Remapped {} triples, sorting...", remapped_count);

    push_stage_metric(
        &mut stage_metrics,
        "Stage 5 remap/collect",
        stage5_start,
        stage5_rss_before,
        benchmark,
    );

    // Finish sorting
    let sort_start = Instant::now();
    let sort_rss_before = if benchmark { process_peak_rss_bytes() } else { None };
    let sorted_triples = sorter.finish(&mut buffer)?;

    push_stage_metric(
        &mut stage_metrics,
        "Stage 6 external sort",
        sort_start,
        sort_rss_before,
        benchmark,
    );

    // Build BitmapTriples (max IDs enable single-pass streaming construction)
    tracing::info!("Building BitmapTriples");
    let bitmap_start = Instant::now();
    let bitmap_rss_before = if benchmark { process_peak_rss_bytes() } else { None };
    let bitmap_triples = crate::triples::build_bitmap_triples(
        sorted_triples,
        max_subject,
        max_predicate,
        max_object,
    )?;

    push_stage_metric(
        &mut stage_metrics,
        "Stage 6 bitmap build",
        bitmap_start,
        bitmap_rss_before,
        benchmark,
    );

    tracing::info!(
        "Pipeline complete: {} triples encoded",
        bitmap_triples.num_triples
    );

    // Clean up temporary files
    cleanup_temp_files(&batches, &mapping_paths);

    log_benchmark_summary(&stage_metrics, benchmark);

    Ok(PipelineResult {
        counts: merge_result.counts,
        dict_sections: merge_result.dict_sections,
        bitmap_triples,
        ntriples_size,
    })
}
