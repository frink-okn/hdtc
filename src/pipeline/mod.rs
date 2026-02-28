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

pub(crate) mod batch_vocab;
mod partial_vocab;
pub(crate) mod vocab_merger;
pub(crate) mod id_remapper;

pub use batch_vocab::BatchVocabBuilder;
pub use partial_vocab::{PartialVocabEntry, PartialVocabReader, PartialVocabWriter};

use crate::dictionary::DictCounts;
use crate::hdt::input_adapter::HdtInputAdapter;
use crate::rdf::{stream_quads_with_options, ExtractedQuad, ParseOptions, RdfInput};
use crate::sort::ExternalSorter;
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
const RECOMMENDED_MIN_MEMORY_BUDGET: usize = 2 * 1024 * 1024 * 1024;
const MIB: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// Memory planning
// ---------------------------------------------------------------------------
//
// The pipeline has two temporally disjoint groups:
//
//   Group A (concurrent): Stages 1-3 — parser, batch vocab builder, vocab writer
//   Group B (sequential after A):
//     B1: Stage 4 — vocab merger (runs alone)
//     B2: Stages 5+6 — ID remapper + external sort (concurrent with each other)
//
// Because Group A finishes before Group B starts, and B1 finishes before B2,
// each group can independently use the *full* memory budget.  Within a group
// we split the budget among the concurrent consumers.

#[derive(Debug, Clone, Copy)]
struct Stage56Budget {
    sort_budget_bytes: usize,
    remap_threads: usize,
    remap_to_sort_channel_capacity: usize,
}

/// Complete memory plan for all pipeline stages.
#[derive(Debug, Clone, Copy)]
struct PipelineMemoryPlan {
    // Group A
    parser_budget_bytes: usize,
    batch_size: usize,
    batch_channel_cap: usize,      // Stage 1→2 channel capacity (batches of quads)
    processed_channel_cap: usize,   // Stage 2→3 channel capacity (processed batches)

    // Group B1
    stage4_budget_bytes: usize,

    // Group B2
    stage56_budget: Stage56Budget,
}

pub(super) fn tune_f64(name: &str, default: f64) -> f64 {
    match std::env::var(name) {
        Ok(raw) => match raw.parse::<f64>() {
            Ok(v) if v.is_finite() && v > 0.0 => v,
            _ => {
                tracing::warn!("Ignoring invalid {}='{}', using default {}", name, raw, default);
                default
            }
        },
        Err(_) => default,
    }
}

pub(super) fn tune_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(raw) => match raw.parse::<usize>() {
            Ok(v) if v > 0 => v,
            _ => {
                tracing::warn!("Ignoring invalid {}='{}', using default {}", name, raw, default);
                default
            }
        },
        Err(_) => default,
    }
}

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
    pub dict_section_paths: Vec<PathBuf>, // PFC section temp files
    pub dict_section_sizes: Vec<u64>,     // Corresponding file sizes
    pub bitmap_triples: crate::triples::BitmapTriplesFiles,
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

/// Get current (not peak) resident set size.
#[cfg(target_os = "macos")]
#[allow(dead_code, deprecated)] // diagnostic utility; mach_task_self deprecated in libc
fn current_rss_bytes() -> Option<u64> {
    use std::mem::MaybeUninit;
    // mach_task_basic_info gives current resident_size
    const MACH_TASK_BASIC_INFO: u32 = 20;
    #[repr(C)]
    struct MachTaskBasicInfo {
        virtual_size: u64,
        resident_size: u64,
        resident_size_max: u64,
        user_time: [u32; 2],   // time_value_t
        system_time: [u32; 2], // time_value_t
        policy: i32,
        suspend_count: i32,
    }
    unsafe {
        let mut info = MaybeUninit::<MachTaskBasicInfo>::uninit();
        let mut count = (std::mem::size_of::<MachTaskBasicInfo>() / std::mem::size_of::<u32>()) as u32;
        let kr = libc::task_info(
            libc::mach_task_self(),
            MACH_TASK_BASIC_INFO,
            info.as_mut_ptr() as *mut i32,
            &mut count,
        );
        if kr != 0 {
            return None;
        }
        Some(info.assume_init().resident_size)
    }
}

/// Get current (not peak) resident set size.
#[cfg(target_os = "linux")]
#[allow(dead_code)]
fn current_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb_str = rest.trim().strip_suffix("kB")?.trim();
            let kb: u64 = kb_str.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Get current (not peak) resident set size — fallback for other platforms.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
#[allow(dead_code)]
fn current_rss_bytes() -> Option<u64> {
    None
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

// ---------------------------------------------------------------------------
// Group A: Stages 1-3 (parser + batch vocab builder + vocab writer)
// ---------------------------------------------------------------------------
// These run concurrently.  Memory consumers differ by stage:
//
//   Batch channel (Stage 1→2):  `batch_channel_cap` batches of Vec<ExtractedQuad>.
//     Each ExtractedQuad has 3 heap-allocated Strings (subject, predicate, object)
//     + Option<String> graph.  Stack: ~96 bytes, heap: ~240 bytes → ~350 bytes/quad.
//
//   Stage 2 processing (1 batch): arena (~100 bytes/triple for unique terms) +
//     hashmap (~80 bytes/unique term) + LocalIdTriple vec (12 bytes/triple).
//     Effective: ~200 bytes/triple.
//
//   Processed channel (Stage 2→3): `processed_channel_cap` ProcessedBatch items.
//     Each holds Vec<VocabEntry> (amortized ~20 bytes/triple for unique terms) +
//     Vec<LocalIdTriple> (12 bytes/triple).  Effective: ~50 bytes/triple.
//
//   Parser I/O: chunk buffers + in-flight bytes, allocated separately.
//
// We compute batch_size by solving:
//   batch_size × (batch_cap × raw_cost + 1 × processing_cost +
//                 processed_cap × processed_cost) ≤ batch_budget
//
// Group A throughput is I/O-bound (parsing), not batch-size-bound.  Empirically,
// larger batches past ~3-5M triples don't improve throughput and just waste memory.
// We therefore cap the total Group A memory footprint so that extra --memory-limit
// budget flows to later stages (sort, merge) where more memory genuinely helps.

fn calculate_group_a(memory_budget: usize) -> (usize, usize, usize, usize) {
    // Parser gets a moderate share; it's I/O-bound so diminishing returns past ~15%.
    let parser_share = tune_f64("HDTC_TUNE_PARSER_SHARE", 0.15).clamp(0.05, 0.40);
    let parser_min = tune_usize("HDTC_TUNE_PARSER_MIN_MIB", 64) * MIB;
    let parser_max = tune_usize("HDTC_TUNE_PARSER_MAX_MIB", 2048) * MIB;
    let parser_budget = (((memory_budget as f64) * parser_share) as usize)
        .clamp(parser_min, parser_max.max(parser_min));

    // Channel depths
    let batch_channel_cap = tune_usize("HDTC_TUNE_BATCH_CHANNEL_CAP", 3).clamp(1, 16);
    let processed_channel_cap = tune_usize("HDTC_TUNE_PROCESSED_CHANNEL_CAP", 2).clamp(1, 8);

    // Per-triple memory costs at each pipeline stage.
    // ExtractedQuad: 3 Strings (24 bytes stack + ~80 bytes heap each) + Option<String>
    // = ~96 bytes stack + ~240 bytes heap ≈ 350 bytes per quad.
    let raw_bytes_per_quad = tune_usize("HDTC_TUNE_RAW_BYTES_PER_QUAD", 350).max(100);
    // Stage 2: arena + hashmap + LocalIdTriple vec
    let processing_bytes_per_triple = tune_usize("HDTC_TUNE_PROCESSING_BYTES_PER_TRIPLE", 200).max(50);
    // ProcessedBatch: Vec<LocalIdTriple> (12b) + amortized Vec<VocabEntry>
    let processed_bytes_per_triple = tune_usize("HDTC_TUNE_PROCESSED_BYTES_PER_TRIPLE", 50).max(16);

    let min_batch = tune_usize("HDTC_TUNE_MIN_BATCH", 50_000);
    let max_batch = tune_usize("HDTC_TUNE_MAX_BATCH", 20_000_000).max(min_batch);

    // Cap total Group A memory: past ~8 GiB for batches there's no throughput gain,
    // just wasted memory that could help the sorter.  At default costs (1350 bytes/
    // triple), 8 GiB ≈ 6.2M triples/batch which is well past the throughput plateau.
    let group_a_max_mib = tune_usize("HDTC_TUNE_GROUP_A_MAX_MIB", 8192);
    let batch_budget = memory_budget
        .saturating_sub(parser_budget)
        .min(group_a_max_mib * MIB);

    // Weighted cost per triple across all in-flight positions
    let weighted_cost_per_triple =
        batch_channel_cap * raw_bytes_per_quad
        + processing_bytes_per_triple
        + processed_channel_cap * processed_bytes_per_triple;

    let batch_size = (batch_budget / weighted_cost_per_triple)
        .clamp(min_batch, max_batch);

    (parser_budget, batch_size, batch_channel_cap, processed_channel_cap)
}

// ---------------------------------------------------------------------------
// Group B1: Stage 4 (vocab merger — runs alone after Group A)
// ---------------------------------------------------------------------------
// Gets the full memory budget.  The main consumers are:
//   - id_mappings: Vec<IdMapping> — per-batch SO + P mapping arrays accumulated
//     in RAM during Stage 4 before being flushed to temporary files on disk
//   - PFC encoders: accumulate all dictionary strings (currently unbounded)
//   - Merge stream channel: bounded buffer of StreamEntry items
//   - Per-batch partial vocab reader threads
//
// We reserve a fraction for the stream channel and I/O; the rest is available
// for PFC accumulators and the transient in-RAM id_mappings before they are
// written to disk.

fn calculate_stage4_budget(memory_budget: usize) -> usize {
    // Stage 4 runs alone — it can use nearly all available memory.
    // We reserve a small margin for OS/allocator overhead.
    let stage4_share = tune_f64("HDTC_TUNE_STAGE4_SHARE", 0.85).clamp(0.30, 0.95);
    let stage4_min = tune_usize("HDTC_TUNE_STAGE4_MIN_MIB", 256) * MIB;
    (((memory_budget as f64) * stage4_share) as usize).max(stage4_min)
}

// ---------------------------------------------------------------------------
// Group B2: Stages 5+6 (remapper + external sort — concurrent)
// ---------------------------------------------------------------------------
// The external sort is the primary throughput bottleneck: more sort memory =
// fewer disk spills = dramatically faster.  The remapper's overhead is modest
// and predictable (threads × per-worker mapping size), so we derive it
// directly and give the rest to the sorter.

fn calculate_stage56_budget(memory_budget: usize) -> Stage56Budget {
    let available_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1);

    // Remapper: each worker holds one decompressed IdMapping + decoder + chunk buffer.
    let per_worker_mib = tune_usize("HDTC_TUNE_REMAP_WORKER_MIB", 128);
    let per_worker_bytes = per_worker_mib * MIB;

    // Number of remapper threads: limited by CPUs and memory.
    let max_remap_threads = (memory_budget / 4 / per_worker_bytes).max(1); // don't let remap exceed 25%
    let remap_threads = max_remap_threads.min(available_cpus);
    let remap_budget_bytes = remap_threads * per_worker_bytes;

    // Remap→sort channel capacity: small bounded queue.
    let chunk_bytes = STAGE5_TO_STAGE6_CHUNK_SIZE * std::mem::size_of::<IdTriple>();
    let queue_budget = (memory_budget / 32).clamp(16 * MIB, 256 * MIB);
    let remap_to_sort_channel_capacity = (queue_budget / chunk_bytes).clamp(16, 256);

    // Sorter gets the full budget minus remapper overhead and channel.
    let sort_min = tune_usize("HDTC_TUNE_SORT_MIN_MIB", 256) * MIB;
    let sort_budget_bytes = memory_budget
        .saturating_sub(remap_budget_bytes)
        .saturating_sub(queue_budget)
        .max(sort_min);

    Stage56Budget {
        sort_budget_bytes,
        remap_threads,
        remap_to_sort_channel_capacity,
    }
}

fn build_memory_plan(memory_budget: usize) -> PipelineMemoryPlan {
    let (parser_budget_bytes, batch_size, batch_channel_cap, processed_channel_cap) =
        calculate_group_a(memory_budget);
    let stage4_budget_bytes = calculate_stage4_budget(memory_budget);
    let stage56_budget = calculate_stage56_budget(memory_budget);

    PipelineMemoryPlan {
        parser_budget_bytes,
        batch_size,
        batch_channel_cap,
        processed_channel_cap,
        stage4_budget_bytes,
        stage56_budget,
    }
}

/// Stage 1: Parse RDF and batch quads.
#[allow(clippy::too_many_arguments)]
fn parser_stage(
    inputs: Vec<RdfInput>,
    batch_size: usize,
    parser_budget_total: usize,
    include_graphs: bool,
    base_uri: String,
    parser_parallelism: ParserParallelismConfig,
    batch_tx: Sender<BatchedQuads>,
    total_input_count: usize,
) -> Result<u64> {
    let expected_files = inputs.len();
    let disambiguate_blank_nodes = total_input_count > 1;
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

    let parser_budget_per_file = (parser_budget_total / file_workers.max(1)).max(16 * MIB);

    let chunk_size_bytes = parser_parallelism
        .chunk_size_bytes
        .unwrap_or((parser_budget_per_file / 8).clamp(MIB, 8 * MIB))
        .max(1);
    let max_inflight_bytes = parser_parallelism
        .max_inflight_bytes
        .unwrap_or((parser_budget_per_file / 2).max(chunk_size_bytes))
        .max(chunk_size_bytes);

    tracing::debug!(
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
        "Parsed {} {} total",
        total_quads,
        if include_graphs { "quads" } else { "triples" }
    );
    tracing::debug!("N-Triples size: {} bytes", ntriples_size);
    Ok(ntriples_size)
}

/// Stage 2: Build vocabulary with hash map + arena for each batch.
fn vocab_builder_stage(
    batch_rx: Receiver<BatchedQuads>,
    processed_tx: Sender<ProcessedBatch>,
    include_graphs: bool,
) -> Result<()> {
    let mut batch_id = 0;
    let mut cumulative_triples: u64 = 0;

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
        cumulative_triples += stats.num_triples as u64;
        tracing::info!(
            "Batch {}: {} {}, {} unique terms (cumulative: {})",
            batch_id,
            stats.num_triples,
            if include_graphs { "quads" } else { "triples" },
            stats.num_terms,
            cumulative_triples,
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

    tracing::debug!("Processed {} batches total", batch_id);
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

/// Clean up batch temp files on error (vocab + triples only).
///
/// Used for early-abort error paths in Stages 1-3 before ID mappings exist.
/// During normal execution, files are cleaned up eagerly:
/// - `.pvoc.zst` — deleted per-file during Stage 4 merge (DeleteOnDrop)
/// - `.ltr.zst` — deleted per-batch during Stage 5 (id_remapper)
/// - `.map.zst` — deleted per-batch during Stage 5 (id_remapper)
fn cleanup_batch_files(batches: &[BatchComplete]) {
    for batch in batches {
        let _ = std::fs::remove_file(&batch.vocab_path);
        let _ = std::fs::remove_file(&batch.triples_path);
    }
    tracing::debug!("Cleaned up {} batch temp files", batches.len());
}

/// Run the complete pipeline: RDF → HDT.
///
/// Accepts both RDF text inputs (parsed via stages 1-3) and HDT inputs
/// (injected directly into the vocabulary merge at stage 4).
#[allow(clippy::too_many_arguments)]
pub fn run_pipeline(
    inputs: &[RdfInput],
    hdt_inputs: &[PathBuf],
    temp_dir: &Path,
    memory_budget: usize,
    include_graphs: bool,
    base_uri: &str,
    parser_parallelism: &ParserParallelismConfig,
    benchmark: bool,
) -> Result<PipelineResult> {
    tracing::info!("Stage 1-3: Parsing, building vocabulary, writing batches");

    if memory_budget < RECOMMENDED_MIN_MEMORY_BUDGET {
        tracing::warn!(
            "Configured memory limit {} MiB is below the recommended floor {} MiB; enabling low-memory tuning (smaller batches, tighter stage budgets) may reduce throughput.",
            memory_budget / 1024 / 1024,
            RECOMMENDED_MIN_MEMORY_BUDGET / 1024 / 1024
        );
    }

    let memory_plan = build_memory_plan(memory_budget);

    let mut stage_metrics: Vec<StageMetric> = Vec::new();
    let rss_start = if benchmark { process_peak_rss_bytes() } else { None };
    let stages123_start = Instant::now();

    let batch_size = memory_plan.batch_size;
    tracing::debug!(
        "Batch size: {} triples (memory budget: {} MiB)",
        batch_size,
        memory_budget / MIB
    );

    if benchmark {
        tracing::info!(
            "Memory plan — Group A: parser {} MiB, batch_size {}, channels {}/{} | \
             Group B1 (stage4): {} MiB | \
             Group B2: sort {} MiB, remap {} threads, queue {} chunks",
            memory_plan.parser_budget_bytes / MIB,
            memory_plan.batch_size,
            memory_plan.batch_channel_cap,
            memory_plan.processed_channel_cap,
            memory_plan.stage4_budget_bytes / MIB,
            memory_plan.stage56_budget.sort_budget_bytes / MIB,
            memory_plan.stage56_budget.remap_threads,
            memory_plan.stage56_budget.remap_to_sort_channel_capacity,
        );
    }

    // Set up channels with bounded capacities for backpressure
    let (batch_tx, batch_rx) = bounded::<BatchedQuads>(memory_plan.batch_channel_cap);
    let (processed_tx, processed_rx) = bounded::<ProcessedBatch>(memory_plan.processed_channel_cap);
    let (complete_tx, complete_rx) = bounded::<BatchComplete>(10);

    // Spawn stages in separate threads
    let inputs_owned = inputs.to_vec();
    let base_uri_owned = base_uri.to_string();
    let parser_parallelism_owned = parser_parallelism.clone();
    let parser_budget = memory_plan.parser_budget_bytes;
    let total_input_count = inputs.len() + hdt_inputs.len();
    let parser_handle = std::thread::spawn(move || {
        parser_stage(
            inputs_owned,
            batch_size,
            parser_budget,
            include_graphs,
            base_uri_owned,
            parser_parallelism_owned,
            batch_tx,
            total_input_count,
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

    tracing::info!(
        "Stage 1-3 complete: {} batches written (elapsed: {:.1}s)",
        batches.len(),
        stages123_start.elapsed().as_secs_f64()
    );

    // Wait for all stages to complete
    let mut ntriples_size = match parser_handle.join() {
        Ok(Ok(size)) => size,
        Ok(Err(e)) => {
            cleanup_batch_files(&batches);
            return Err(e);
        }
        Err(_) => {
            cleanup_batch_files(&batches);
            return Err(anyhow::anyhow!("Parser thread panicked"));
        }
    };

    match builder_handle.join() {
        Ok(Err(e)) => {
            cleanup_batch_files(&batches);
            return Err(e);
        }
        Err(_) => {
            cleanup_batch_files(&batches);
            return Err(anyhow::anyhow!("Builder thread panicked"));
        }
        Ok(Ok(())) => {}
    }

    match writer_handle.join() {
        Ok(Err(e)) => {
            cleanup_batch_files(&batches);
            return Err(e);
        }
        Err(_) => {
            cleanup_batch_files(&batches);
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

    // Pre-process HDT inputs: scan headers, create adapters
    let mut hdt_adapters: Vec<(usize, HdtInputAdapter)> = Vec::new();
    if !hdt_inputs.is_empty() {
        tracing::info!("Scanning {} HDT input file(s)", hdt_inputs.len());
        let rdf_batch_count = batches.len();
        for (i, hdt_path) in hdt_inputs.iter().enumerate() {
            let batch_id = rdf_batch_count + i;
            let adapter = HdtInputAdapter::scan(hdt_path)
                .with_context(|| format!("Failed to scan HDT input {}", hdt_path.display()))?;
            ntriples_size += adapter.original_size;
            hdt_adapters.push((batch_id, adapter));
        }
    }

    // Stage 4: Merge vocabularies and build global dictionary
    let total_sources = batches.len() + hdt_adapters.len();
    tracing::info!("Stage 4: Merging {} vocabulary sources ({} RDF batches + {} HDT inputs)",
        total_sources, batches.len(), hdt_adapters.len());
    let stage4_start = Instant::now();
    let stage4_rss_before = if benchmark { process_peak_rss_bytes() } else { None };
    let mut vocab_sources: Vec<vocab_merger::VocabSource> = Vec::with_capacity(total_sources);
    for batch in &batches {
        let source =
            vocab_merger::vocab_source_from_pvoc(batch.batch_id, batch.vocab_path.clone())?;
        vocab_sources.push(source);
    }
    // Disambiguate blank nodes when there are multiple total input files
    let total_input_files = inputs.len() + hdt_inputs.len();
    let disambiguate = total_input_files > 1;
    for (i, (batch_id, adapter)) in hdt_adapters.iter().enumerate() {
        let file_index = if disambiguate { Some(inputs.len() + i) } else { None };
        vocab_sources.push(vocab_merger::VocabSource {
            batch_id: *batch_id,
            max_so_id: adapter.max_so_id(),
            max_p_id: adapter.max_p_id(),
            factory: adapter.vocab_factory(*batch_id, file_index),
        });
    }

    let stage4_budget = memory_plan.stage4_budget_bytes;
    tracing::debug!("Stage 4 merge budget: {} MiB", stage4_budget / MIB);

    let merge_result =
        vocab_merger::merge_vocabularies(vocab_sources, temp_dir, stage4_budget)?;

    tracing::info!(
        "Stage 4 complete: {} shared, {} subjects, {} predicates, {} objects (elapsed: {:.1}s)",
        merge_result.counts.shared,
        merge_result.counts.subjects,
        merge_result.counts.predicates,
        merge_result.counts.objects,
        stage4_start.elapsed().as_secs_f64()
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

    let stage56_budget = memory_plan.stage56_budget;

    // Set up channel for global-ID triples (chunked to reduce per-message overhead)
    let (global_triple_tx, global_triple_rx) =
        bounded::<Vec<IdTriple>>(stage56_budget.remap_to_sort_channel_capacity);

    // Prepare batch remap info (files are cleaned up per-batch by the remapper)
    let total_remap_batches = batches.len() + hdt_adapters.len();
    let (remap_tx, remap_rx) = bounded(total_remap_batches);
    for batch in &batches {
        let mapping_path = temp_dir.join(format!("id_mapping_{:06}.map.zst", batch.batch_id));
        remap_tx
            .send(id_remapper::BatchRemapInfo {
                batch_id: batch.batch_id,
                triple_source: id_remapper::TripleSource::LtrFile(batch.triples_path.clone()),
                mapping_path,
            })
            .ok();
    }
    for (batch_id, adapter) in &hdt_adapters {
        let mapping_path = temp_dir.join(format!("id_mapping_{:06}.map.zst", batch_id));
        remap_tx
            .send(id_remapper::BatchRemapInfo {
                batch_id: *batch_id,
                triple_source: id_remapper::TripleSource::HdtFile {
                    path: adapter.path.clone(),
                    triples_data_offset: adapter.triples_data_offset(),
                    num_triples: adapter.num_triples,
                    num_sp_pairs: adapter.num_sp_pairs(),
                    shared_count: adapter.shared_count,
                    subjects_count: adapter.subjects_count,
                },
                mapping_path,
            })
            .ok();
    }
    drop(remap_tx); // Signal completion

    // Spawn remapper in separate thread
    let remap_threads = stage56_budget.remap_threads;
    tracing::debug!("Stage 5 remapper threads: {}", remap_threads);
    let remapper_handle = std::thread::spawn(move || {
        id_remapper::id_remapper_stage(
            remap_rx,
            global_triple_tx,
            remap_threads,
            STAGE5_TO_STAGE6_CHUNK_SIZE,
        )
    });

    // Stage 6: External sort + BitmapTriples construction
    tracing::info!("Stage 6: Sorting global-ID triples in SPO order");

    // Collect triples into external sorter, tracking max IDs for BitmapTriples bit widths
    let mut sorter = ExternalSorter::new(temp_dir, stage56_budget.sort_budget_bytes);
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

    tracing::info!(
        "Stage 5 complete: {} triples remapped ({:.1}s), sorting...",
        remapped_count,
        stage5_start.elapsed().as_secs_f64()
    );

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

    // Build BitmapTriples — stream each component to temp files (O(1) memory)
    let bitmap_start = Instant::now();
    let bitmap_rss_before = if benchmark { process_peak_rss_bytes() } else { None };
    let bitmap_triples = crate::triples::build_bitmap_triples_to_files(
        sorted_triples,
        max_subject,
        max_predicate,
        max_object,
        temp_dir,
    )?;

    push_stage_metric(
        &mut stage_metrics,
        "Stage 6 bitmap build",
        bitmap_start,
        bitmap_rss_before,
        benchmark,
    );

    tracing::info!(
        "Stage 6 complete: {} triples encoded (elapsed: {:.1}s)",
        bitmap_triples.num_triples,
        sort_start.elapsed().as_secs_f64()
    );

    log_benchmark_summary(&stage_metrics, benchmark);

    Ok(PipelineResult {
        counts: merge_result.counts,
        dict_section_paths: merge_result.dict_section_paths,
        dict_section_sizes: merge_result.dict_section_sizes,
        bitmap_triples,
        ntriples_size,
    })
}
