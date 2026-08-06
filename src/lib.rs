//! hdtc — HDT creation, sidecar construction, and the on-disk format layer.
//!
//! The module tree below is **private**: it is the implementation of the CLI and
//! is free to change shape. Downstream crates depend on [`format`], a curated
//! re-export of the format knowledge and read-side runtime logic that must not be
//! reimplemented elsewhere. See that module for the stability contract.

mod cli;
mod dictionary;
mod graph_index;
mod hdt;
mod index;
mod io;
mod permutation;
mod pipeline;
mod quads;
mod rdf;
mod sort;
mod text;
mod triples;

pub mod format;

use anyhow::{Context, Result};
use clap::Parser;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing_subscriber::EnvFilter;

/// Raise the soft file descriptor limit toward the hard limit.
///
/// This is a best-effort safety net for stages that open many files
/// simultaneously (vocab merger k-way merge, external sort merge).
/// The parallel merge tree also bounds fan-in, but raising the limit
/// provides additional headroom.
fn raise_fd_limit() -> Option<(u64, u64)> {
    #[cfg(unix)]
    unsafe {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            let target = rlim.rlim_max.min(65536);
            if rlim.rlim_cur < target {
                let old = rlim.rlim_cur;
                rlim.rlim_cur = target;
                if libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) == 0 {
                    return Some((old, target));
                }
            }
        }
    }

    None
}

fn make_default_temp_dir() -> Result<std::path::PathBuf> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("hdtc_work_{}_{}", std::process::id(), now));
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create temp dir {}", dir.display()))?;
    Ok(dir)
}

/// An existing graph sidecar moved out of its canonical name while a new HDT
/// is published. Keeping the backup in a sibling directory makes both moves
/// atomic on supported filesystems.
struct RetiredArtifact {
    original_path: PathBuf,
    backup_path: PathBuf,
    _quarantine: tempfile::TempDir,
}

impl RetiredArtifact {
    fn restore(&self) -> Result<()> {
        match std::fs::symlink_metadata(&self.original_path) {
            Ok(_) => anyhow::bail!(
                "Cannot restore artifact {} because another entry now exists there",
                self.original_path.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "Failed to inspect artifact path {} before rollback",
                        self.original_path.display()
                    )
                });
            }
        }
        std::fs::rename(&self.backup_path, &self.original_path).with_context(|| {
            format!(
                "Failed to restore artifact {}",
                self.original_path.display()
            )
        })
    }
}

fn retire_existing_artifact(
    artifact_path: &Path,
    output_parent: &Path,
    label: &str,
) -> Result<Option<RetiredArtifact>> {
    let metadata = match std::fs::symlink_metadata(artifact_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "Failed to inspect existing {label} {}",
                    artifact_path.display()
                )
            });
        }
    };
    let file_type = metadata.file_type();
    anyhow::ensure!(
        file_type.is_file() || file_type.is_symlink(),
        "Refusing to replace {label} path {} because it is not a file",
        artifact_path.display()
    );

    let quarantine = tempfile::Builder::new()
        .prefix(".hdtc-retired-artifact-")
        .tempdir_in(output_parent)
        .with_context(|| {
            format!(
                "Failed to create artifact quarantine in {}",
                output_parent.display()
            )
        })?;
    let backup_path = quarantine.path().join("previous-artifact");
    std::fs::rename(artifact_path, &backup_path).with_context(|| {
        format!(
            "Failed to retire existing {label} {}",
            artifact_path.display()
        )
    })?;

    Ok(Some(RetiredArtifact {
        original_path: artifact_path.to_path_buf(),
        backup_path,
        _quarantine: quarantine,
    }))
}

/// Run the `hdtc` command-line interface.
///
/// The binary target is a thin wrapper around this; everything the CLI does is
/// reachable from the library so tests and downstream crates can drive it
/// without spawning a process.
pub fn run() -> Result<()> {
    // Restore SIGPIPE to its default disposition so that piping to tools like
    // `head` or `grep` terminates the process silently (exit 141) rather than
    // propagating EPIPE as an error.  Rust sets SIGPIPE to SIG_IGN at startup,
    // which causes broken-pipe writes to return an error instead of killing
    // the process, resulting in a spurious "Broken pipe" message on stderr.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }

    let raised_fd_limit = raise_fd_limit();

    let cli = cli::Cli::parse();
    let benchmark = cli.benchmark;

    // Set up logging
    let filter = match (cli.quiet, cli.verbose) {
        (true, _) => EnvFilter::new("error"),
        (_, 0) => EnvFilter::new("info"),
        (_, 1) => EnvFilter::new("debug"),
        (_, _) => EnvFilter::new("trace"),
    };
    if let Err(error) = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init()
    {
        // Embedders commonly install their own process-global subscriber before
        // invoking a library entry point. Keep their subscriber and preserve
        // `run`'s Result-based failure contract instead of panicking.
        tracing::debug!(%error, "Using existing tracing subscriber");
    }

    if let Some((old, target)) = raised_fd_limit {
        tracing::debug!(old, target, "Raised file descriptor limit");
    }

    tracing::info!("hdtc - HDT Creator");

    // Route to appropriate subcommand
    match cli.command {
        cli::Commands::Create(args) => create_hdt(args, benchmark),
        cli::Commands::Index(args) => create_index_from_hdt(args, benchmark),
        cli::Commands::Perm(args) => create_permutation_from_hdt(args, benchmark),
        cli::Commands::GraphIndex(args) => create_graph_index_from_hdt(args, benchmark),
        cli::Commands::Dump(args) => dump_hdt_to_ntriples(args, benchmark),
        cli::Commands::Search(args) => search_hdt(args, benchmark),
        cli::Commands::Validate(args) => validate_hdt_file(args, benchmark),
        cli::Commands::Void(args) => compute_void(args, benchmark),
        cli::Commands::Namespaces(args) => compute_namespaces(args, benchmark),
        cli::Commands::Sketch(args) => create_sketches(args, benchmark),
        cli::Commands::Keyset(args) => create_keysets(args, benchmark),
        cli::Commands::Text(args) => create_text_index(args, benchmark),
        cli::Commands::Header(args) => run_header(args, benchmark),
    }
}

/// Create HDT file from RDF input(s)
fn create_hdt(args: cli::CreateArgs, benchmark: bool) -> Result<()> {
    // Discover input files
    let discovered = rdf::discover_inputs(&args.inputs)?;
    let inputs = discovered.rdf_inputs;
    let hdt_inputs = discovered.hdt_inputs;
    for input in &inputs {
        tracing::debug!(
            "  {} ({:?}, {:?})",
            input.path.display(),
            input.format,
            input.compression
        );
    }
    for hdt_path in &hdt_inputs {
        tracing::debug!("  {} (HDT)", hdt_path.display());
    }

    tracing::info!("Output: {}", args.output.display());
    let include_graphs = args.mode == cli::OutputMode::Quads;
    let graph_options_require_quads = !args.graph_map.is_empty()
        || args.default_graph.is_some()
        || args.graphs_index
        || matches!(
            args.input_sidecars,
            Some(cli::InputSidecarPolicy::Preserve | cli::InputSidecarPolicy::Require)
        );
    if !include_graphs && graph_options_require_quads {
        anyhow::bail!(
            "--graphs-index, --graph-map, --default-graph, and preserve/require \
             --input-sidecars require --mode quads"
        );
    }
    let input_sidecar_policy = if include_graphs {
        args.input_sidecars
            .unwrap_or(cli::InputSidecarPolicy::Preserve)
    } else {
        cli::InputSidecarPolicy::Drop
    };
    tracing::info!("Mode: {:?}", args.mode);
    tracing::debug!("Input sidecars: {:?}", input_sidecar_policy);

    // Set up temp directory
    let temp_dir = match &args.temp_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create temp dir {}", dir.display()))?;
            dir.clone()
        }
        None => make_default_temp_dir()?,
    };
    tracing::info!("Temp directory: {}", temp_dir.display());

    let memory_budget = args.memory_limit.as_bytes();
    tracing::info!("Memory limit: {}", args.memory_limit);

    let graph_assignments =
        quads::GraphAssignments::parse(&args.graph_map, args.default_graph.as_deref())?;
    let parser_parallelism = pipeline::ParserParallelismConfig {
        file_workers: args.parse_file_workers,
        chunk_workers: args.parse_chunk_workers,
        chunk_size_bytes: args.parse_chunk_bytes,
        max_inflight_bytes: args.parse_max_inflight_bytes,
    };

    // Compute base URI: use provided value, or derive from first input file
    let base_uri = match &args.base_uri {
        Some(uri) => uri.clone(),
        None => {
            // Use file:// URI of first input file (must be absolute path)
            let first_path = inputs
                .first()
                .map(|i| &i.path)
                .or(hdt_inputs.first())
                .expect("at least one input file");
            let abs_path = std::fs::canonicalize(first_path).unwrap_or_else(|_| first_path.clone());
            format!("file://{}", abs_path.display())
        }
    };

    // Dataset IRI for the header subject; defaults to the parse base URI.
    let dataset_uri = args.dataset_uri.clone().unwrap_or_else(|| base_uri.clone());

    // Run the pipelined HDT construction
    let permutation_maps = args.perm.then(|| permutation::PositionMaps {
        pos: args
            .perm_position_maps
            .contains(&cli::PermutationPositionMap::Pos),
        ops: args
            .perm_position_maps
            .contains(&cli::PermutationPositionMap::Ops),
    });
    let mut pipeline_result = pipeline::run_pipeline(
        &inputs,
        &hdt_inputs,
        &temp_dir,
        memory_budget,
        include_graphs,
        input_sidecar_policy,
        &graph_assignments,
        &base_uri,
        &parser_parallelism,
        permutation_maps,
        args.graphs_index,
        benchmark,
    )?;

    // Assemble the HDT and sidecar under sibling temporary names. The HDT is
    // published only after sidecar construction succeeds, then the sidecar is
    // renamed second as required by the detectable two-file publication model.
    let output_parent = args
        .output
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let hdt_temp = tempfile::Builder::new()
        .prefix(".hdtc-hdt-")
        .tempfile_in(output_parent)
        .with_context(|| {
            format!(
                "Failed to create HDT temp file in {}",
                output_parent.display()
            )
        })?
        .into_temp_path();
    hdt::write_hdt_streaming(
        &hdt_temp,
        &dataset_uri,
        &pipeline_result.counts,
        &pipeline_result.dict_section_paths,
        &pipeline_result.dict_section_sizes,
        &pipeline_result.bitmap_triples,
        pipeline_result.ntriples_size,
    )?;

    let num_triples = pipeline_result.bitmap_triples.num_triples;

    let canonical_graphs_path = quads::canonical_sidecar_path(&args.output);
    let canonical_graph_index_path = graph_index::canonical_path(&args.output);
    let sidecar_temp = if include_graphs {
        let membership_path = pipeline_result
            .membership_path
            .as_deref()
            .context("Graph sidecar requested but the membership stream is missing")?;
        let sidecar_temp = tempfile::Builder::new()
            .prefix(".hdtc-graphs-")
            .tempfile_in(output_parent)
            .with_context(|| {
                format!(
                    "Failed to create sidecar temp file in {}",
                    output_parent.display()
                )
            })?
            .into_temp_path();
        quads::write_graph_sidecar(
            &sidecar_temp,
            &hdt_temp,
            &pipeline_result.graph_section_path,
            pipeline_result.graph_section_size,
            pipeline_result.counts.graphs,
            num_triples,
            pipeline_result.membership_count,
            membership_path,
            pipeline_result.has_blank_graph_names,
            &temp_dir,
        )?;
        Some(sidecar_temp)
    } else {
        None
    };

    let canonical_perm_path = permutation::canonical_path(&args.output);
    let mut perm_temp = None;
    let graph_index_temp = if args.graphs_index {
        let prepared = pipeline_result
            .graph_permutation
            .take()
            .context("Graph-index collection was not prepared")?;
        let graph_temp = tempfile::Builder::new()
            .prefix(".hdtc-graph-index-")
            .tempfile_in(output_parent)
            .with_context(|| {
                format!(
                    "Failed to create graph-index temp file in {}",
                    output_parent.display()
                )
            })?
            .into_temp_path();
        let prepared_perm = args
            .perm
            .then(|| {
                tempfile::Builder::new()
                    .prefix(".hdtc-perm-")
                    .tempfile_in(output_parent)
                    .map(|file| file.into_temp_path())
            })
            .transpose()
            .with_context(|| {
                format!(
                    "Failed to create permutation-index temp file in {}",
                    output_parent.display()
                )
            })?;
        let sidecar_path = sidecar_temp
            .as_deref()
            .context("Graph-index creation requires a staged graph sidecar")?;
        graph_index::finish_prepared_graph_index(
            prepared,
            &graph_temp,
            prepared_perm.as_deref(),
            &hdt_temp,
            sidecar_path,
            &pipeline_result.counts,
            &pipeline_result.bitmap_triples,
            pipeline_result.membership_count,
            &temp_dir,
            memory_budget,
            permutation_maps.unwrap_or_default(),
        )?;
        perm_temp = prepared_perm;
        Some(graph_temp)
    } else {
        if args.perm {
            let prepared = pipeline_result
                .permutation
                .take()
                .context("Permutation collection was not prepared")?;
            let temp = tempfile::Builder::new()
                .prefix(".hdtc-perm-")
                .tempfile_in(output_parent)
                .with_context(|| {
                    format!(
                        "Failed to create permutation-index temp file in {}",
                        output_parent.display()
                    )
                })?
                .into_temp_path();
            permutation::finish_prepared_index(
                prepared,
                &temp,
                &hdt_temp,
                &pipeline_result.counts,
                &pipeline_result.bitmap_triples,
            )?;
            perm_temp = Some(temp);
        }
        None
    };

    // Move derived graph data out first: a graph index binds to both the HDT
    // and sidecar, so it must never survive replacement of either parent.
    let retired_graph_index =
        retire_existing_artifact(&canonical_graph_index_path, output_parent, "graph index")?;
    // Move the old sidecar out of its canonical name before replacing the HDT.
    // A crash can therefore leave detectably missing artifacts, but never a
    // new HDT next to stale graph data.
    let retired_sidecar =
        match retire_existing_artifact(&canonical_graphs_path, output_parent, "graph sidecar") {
            Ok(retired) => retired,
            Err(error) => {
                if let Some(retired) = retired_graph_index.as_ref()
                    && let Err(restore_error) = retired.restore()
                {
                    return Err(anyhow::anyhow!(
                        "{error:#}; additionally failed to roll back the graph index: \
                     {restore_error:#}"
                    ));
                }
                return Err(error);
            }
        };
    let retired_perm =
        match retire_existing_artifact(&canonical_perm_path, output_parent, "permutation index") {
            Ok(retired) => retired,
            Err(error) => {
                let graph_restore = (|| -> Result<()> {
                    if let Some(retired) = retired_sidecar.as_ref() {
                        retired.restore()?;
                    }
                    if let Some(retired) = retired_graph_index.as_ref() {
                        retired.restore()?;
                    }
                    Ok(())
                })();
                if let Err(restore_error) = graph_restore {
                    return Err(anyhow::anyhow!(
                        "{error:#}; additionally failed to roll back a graph artifact: \
                         {restore_error:#}"
                    ));
                }
                return Err(error);
            }
        };
    if let Err(error) = hdt_temp.persist(&args.output) {
        let publish_error = anyhow::Error::new(error.error).context(format!(
            "Failed to publish HDT file {}",
            args.output.display()
        ));
        let perm_restore = retired_perm
            .as_ref()
            .map(RetiredArtifact::restore)
            .transpose();
        let graph_restore = (|| -> Result<()> {
            if let Some(retired) = retired_sidecar.as_ref() {
                retired.restore()?;
            }
            if let Some(retired) = retired_graph_index.as_ref() {
                retired.restore()?;
            }
            Ok(())
        })();
        if let Err(restore_error) = perm_restore.map(|_| ()).and(graph_restore) {
            return Err(anyhow::anyhow!(
                "{publish_error:#}; additionally failed to roll back an artifact: {restore_error:#}"
            ));
        }
        return Err(publish_error);
    }

    if !include_graphs && retired_sidecar.is_some() {
        tracing::info!(
            "Removed stale graph sidecar: {}",
            canonical_graphs_path.display()
        );
    }
    if !args.graphs_index && retired_graph_index.is_some() {
        tracing::info!(
            "Removed stale graph index: {}",
            canonical_graph_index_path.display()
        );
    }

    if let Some(sidecar_temp) = sidecar_temp {
        sidecar_temp
            .persist(&canonical_graphs_path)
            .with_context(|| {
                format!(
                    "Failed to publish graph sidecar {}",
                    canonical_graphs_path.display()
                )
            })?;
        tracing::info!("Graph sidecar written: {}", canonical_graphs_path.display());
    }

    if !args.perm && retired_perm.is_some() {
        tracing::info!(
            "Removed stale permutation index: {}",
            canonical_perm_path.display()
        );
    }
    if let Some(perm_temp) = perm_temp {
        perm_temp.persist(&canonical_perm_path).with_context(|| {
            format!(
                "Failed to publish permutation index {}",
                canonical_perm_path.display()
            )
        })?;
        tracing::info!(
            "Permutation index written: {}",
            canonical_perm_path.display()
        );
    }
    if let Some(graph_index_temp) = graph_index_temp {
        graph_index_temp
            .persist(&canonical_graph_index_path)
            .with_context(|| {
                format!(
                    "Failed to publish graph index {}",
                    canonical_graph_index_path.display()
                )
            })?;
        tracing::info!(
            "Graph index written: {}",
            canonical_graph_index_path.display()
        );
    }

    // Clean up dict section and triples temp files
    for path in &pipeline_result.dict_section_paths {
        if let Err(e) = std::fs::remove_file(path) {
            tracing::debug!("Failed to remove dict section temp file: {e}");
        }
    }
    if let Err(e) = std::fs::remove_file(&pipeline_result.graph_section_path) {
        tracing::debug!("Failed to remove graph dictionary temp file: {e}");
    }
    if let Some(path) = &pipeline_result.membership_path
        && let Err(e) = std::fs::remove_file(path)
    {
        tracing::debug!("Failed to remove membership temp file: {e}");
    }
    pipeline_result.bitmap_triples.cleanup();

    // Optionally create index file
    if args.index {
        let expected_index_path = args.output.with_extension("hdt.index.v1-1");
        tracing::info!("Creating index: {}", expected_index_path.display());
        match index::create_index(&args.output, memory_budget, &temp_dir) {
            Ok(index_path) => {
                tracing::info!("Index written: {}", index_path.display());
            }
            Err(e) => {
                tracing::error!("Failed to create index: {}", e);
                return Err(e);
            }
        }
    }

    tracing::info!(
        "Done! {} triples written to {}",
        num_triples,
        args.output.display()
    );

    Ok(())
}

/// Create index file for an existing HDT file
fn create_index_from_hdt(args: cli::IndexArgs, benchmark: bool) -> Result<()> {
    // Verify the HDT file exists
    if !args.hdt_file.exists() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    // Set up temp directory
    let temp_dir = match &args.temp_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create temp dir {}", dir.display()))?;
            dir.clone()
        }
        None => make_default_temp_dir()?,
    };

    let memory_budget = args.memory_limit.as_bytes();

    let expected_index_path = args.hdt_file.with_extension("hdt.index.v1-1");
    tracing::info!("Creating index: {}", expected_index_path.display());
    tracing::info!("Temp directory: {}", temp_dir.display());
    tracing::info!("Memory limit: {}", args.memory_limit);

    // Create the index
    let index_start = std::time::Instant::now();
    match index::create_index(&args.hdt_file, memory_budget, &temp_dir) {
        Ok(index_path) => {
            tracing::info!("Index written: {}", index_path.display());
            if benchmark {
                tracing::info!(
                    "Benchmark summary (index): total {:.3}s",
                    index_start.elapsed().as_secs_f64()
                );
            }
            tracing::info!("Done!");
            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to create index: {}", e);
            Err(e)
        }
    }
}

/// Create a POS/OPS permutation index for an existing HDT file.
fn create_permutation_from_hdt(args: cli::PermArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.is_file() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }
    let temp_dir = match &args.temp_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create temp dir {}", dir.display()))?;
            dir.clone()
        }
        None => make_default_temp_dir()?,
    };
    let maps = permutation::PositionMaps {
        pos: args
            .position_maps
            .contains(&cli::PermutationPositionMap::Pos),
        ops: args
            .position_maps
            .contains(&cli::PermutationPositionMap::Ops),
    };
    tracing::info!(
        "Creating permutation index for: {}",
        args.hdt_file.display()
    );
    tracing::info!(
        "Output: {}",
        permutation::canonical_path(&args.hdt_file).display()
    );
    tracing::info!("Temp directory: {}", temp_dir.display());
    tracing::info!("Memory limit: {}", args.memory_limit);
    let start = std::time::Instant::now();
    let output = permutation::create_permutation_index(
        &args.hdt_file,
        args.memory_limit.as_bytes(),
        &temp_dir,
        maps,
    )?;
    if benchmark {
        tracing::info!(
            "Benchmark summary (perm): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }
    tracing::info!("Permutation index written: {}", output.display());
    tracing::info!("Done!");
    Ok(())
}

/// Create the derived graph index for an existing HDT/graphs pair.
fn create_graph_index_from_hdt(args: cli::GraphIndexArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.is_file() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }
    let temp_dir = match &args.temp_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create temp dir {}", dir.display()))?;
            dir.clone()
        }
        None => make_default_temp_dir()?,
    };
    let default_positions = args.positions.is_empty() && !args.no_layers;
    let options = graph_index::GraphIndexOptions {
        pos_layers: default_positions
            || args.positions.contains(&cli::GraphIndexPositionSpace::Pos),
        ops_layers: default_positions
            || args.positions.contains(&cli::GraphIndexPositionSpace::Ops),
        membership_ranks: args.transpose_ranks || args.transpose_ids,
        membership_ids: args.transpose_ids,
    };
    tracing::info!("Creating graph index for: {}", args.hdt_file.display());
    tracing::info!(
        "Output: {}",
        graph_index::canonical_path(&args.hdt_file).display()
    );
    tracing::info!("Temp directory: {}", temp_dir.display());
    tracing::info!("Memory limit: {}", args.memory_limit);
    let start = std::time::Instant::now();
    let output = graph_index::create_graph_index(
        &args.hdt_file,
        args.memory_limit.as_bytes(),
        &temp_dir,
        options,
    )?;
    if benchmark {
        tracing::info!(
            "Benchmark summary (graphs-index): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }
    tracing::info!("Graph index written: {}", output.display());
    tracing::info!("Done!");
    Ok(())
}

/// Dump an existing HDT file as the triples union or lossless RDF dataset.
fn dump_hdt_to_ntriples(args: cli::DumpArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.exists() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    tracing::info!(
        "Dumping HDT {} view: {}",
        match args.graph_view {
            cli::DumpGraphView::Union => "union",
            cli::DumpGraphView::Dataset => "dataset",
        },
        args.hdt_file.display()
    );
    match &args.output {
        Some(p) => tracing::info!("Output: {}", p.display()),
        None => tracing::info!("Output: stdout"),
    }

    let start = std::time::Instant::now();
    let memory_limit = args.memory_limit.as_bytes();
    tracing::info!("Memory limit: {} bytes", memory_limit);
    let count = match args.graph_view {
        cli::DumpGraphView::Union => hdt::search_hdt_streaming(
            &args.hdt_file,
            "? ? ?",
            args.output.as_deref(),
            false,
            None,
            None,
            memory_limit,
            None,
            false,
        )?,
        cli::DumpGraphView::Dataset => {
            let temp_dir = match &args.temp_dir {
                Some(path) => {
                    std::fs::create_dir_all(path)
                        .with_context(|| format!("Failed to create temp dir {}", path.display()))?;
                    path.clone()
                }
                None => make_default_temp_dir()?,
            };
            quads::export_dataset_nquads(
                &args.hdt_file,
                args.output.as_deref(),
                &temp_dir,
                memory_limit,
            )?
        }
    };

    if benchmark {
        tracing::info!(
            "Benchmark summary (dump): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }

    match &args.output {
        Some(p) => tracing::info!("Done! {count} statements written to {}", p.display()),
        None => tracing::info!("Done! {count} statements written"),
    }
    Ok(())
}

/// Search an HDT file with a triple pattern.
fn search_hdt(args: cli::SearchArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.exists() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    if args.count && args.limit.is_some() {
        tracing::warn!("--limit is ignored when combined with --count; counting all matches");
    }
    if args.count && args.offset.is_some() {
        tracing::warn!("--offset is ignored when combined with --count; counting all matches");
    }

    tracing::info!("Searching HDT: {}", args.hdt_file.display());
    let start = std::time::Instant::now();
    let memory_limit = args.memory_limit.as_bytes();
    let limit = if args.count { None } else { args.limit };
    let offset = if args.count { None } else { args.offset };

    // `--text` and `--query` are one required, mutually exclusive group, so a
    // text query means there is no pattern to parse.
    if let Some(text) = &args.text {
        return search_text(&args, text, limit, offset, memory_limit, start, benchmark);
    }
    let pattern = args
        .query
        .as_deref()
        .expect("clap requires --query when --text is absent");
    tracing::info!("Query: {pattern}");

    let query =
        hdt::parse_search_query(pattern).with_context(|| format!("Invalid query: {pattern:?}"))?;
    let is_quad_query = matches!(query, hdt::SearchQuery::Quad(_));

    let count = match query {
        hdt::SearchQuery::Triple(_) => hdt::search_hdt_streaming(
            &args.hdt_file,
            pattern,
            args.output.as_deref(),
            args.count,
            limit,
            offset,
            memory_limit,
            args.index.as_deref(),
            args.no_index,
        )?,
        hdt::SearchQuery::Quad(pattern) => quads::search_dataset_streaming(
            &args.hdt_file,
            &pattern,
            args.output.as_deref(),
            args.count,
            limit,
            offset,
            memory_limit,
            args.temp_dir.as_deref(),
        )?,
    };

    if benchmark {
        tracing::info!(
            "Benchmark summary (search): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }

    tracing::info!(
        "Done! {count} matching {}",
        if is_quad_query {
            "quad(s)"
        } else {
            "triple(s)"
        }
    );
    Ok(())
}

/// Ranked full-text search over the index built by `hdtc text`.
#[allow(clippy::too_many_arguments)]
fn search_text(
    args: &cli::SearchArgs,
    text: &str,
    limit: Option<u64>,
    offset: Option<u64>,
    memory_limit: usize,
    start: std::time::Instant,
    benchmark: bool,
) -> Result<()> {
    tracing::info!("Text query: {text:?}");
    let mode = match args.text_match.unwrap_or(cli::TextMatchMode::All) {
        cli::TextMatchMode::All => text::MatchMode::All,
        cli::TextMatchMode::Any => text::MatchMode::Any,
        cli::TextMatchMode::Phrase => text::MatchMode::Phrase,
    };
    let languages: Vec<String> = args
        .lang
        .iter()
        .map(|range| text::normalize_language(range.as_bytes()))
        .collect();

    let count = hdt::search_text_streaming(&hdt::TextSearchOptions {
        hdt_path: &args.hdt_file,
        text_index: args.text_index.as_deref(),
        query: text,
        mode,
        fuzzy: args.fuzzy.unwrap_or(0),
        prefix: args.prefix,
        languages: &languages,
        predicate: args.predicate.as_deref(),
        scores: args.scores,
        output: args.output.as_deref(),
        count_only: args.count,
        limit,
        offset,
        memory_limit,
        temp_dir: args.temp_dir.as_deref(),
        index_path: args.index.as_deref(),
        no_index: args.no_index,
    })?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (search --text): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }
    tracing::info!("Done! {count} matching triple(s)");
    Ok(())
}

/// Build a full-text index over an HDT's literals.
fn create_text_index(args: cli::TextArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.is_file() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    let output_dir = args
        .output
        .unwrap_or_else(|| text::default_text_index_path(&args.hdt_file));
    let exclusions = if args.index_all_datatypes {
        text::DatatypeExclusions::default()
    } else {
        text::DatatypeExclusions::with_defaults(&args.exclude_datatype)
    };

    tracing::info!("Building text index: {}", args.hdt_file.display());
    tracing::info!("Output directory: {}", output_dir.display());
    tracing::info!("Maximum literal size: {} bytes", args.max_literal_bytes);
    tracing::info!("Excluded datatypes: {}", exclusions.iris().len());
    tracing::info!("Untagged literals stemmed as: {}", args.untagged_language);

    let start = std::time::Instant::now();
    let summary = text::create_text_index(&text::TextConfig {
        hdt_path: args.hdt_file.clone(),
        output_dir,
        max_literal_bytes: args.max_literal_bytes,
        exclusions,
        untagged_language: (args.untagged_language != "none").then_some(args.untagged_language),
        memory_limit: args.memory_limit.as_bytes(),
        threads: args.threads,
    })?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (text): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }
    let manifest = &summary.manifest;
    tracing::info!(
        "Done! {} literal(s) indexed of {} scanned ({} bytes); excluded {} \
         (oversize {}, datatype {}, no tokens {}); {} language(s)",
        manifest.indexed_docs,
        manifest.literals_scanned,
        summary.index_bytes,
        manifest.excluded_total(),
        manifest.excluded_oversize,
        manifest.excluded_datatype,
        manifest.excluded_no_tokens,
        manifest.languages.len()
    );
    Ok(())
}

/// Compute VoID statistics for an HDT file.
fn compute_void(args: cli::VoidArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.exists() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    tracing::info!("Computing VoID statistics: {}", args.hdt_file.display());
    tracing::info!("Dataset URI: {}", args.dataset_uri);
    match &args.output {
        Some(p) => tracing::info!("Output: {}", p.display()),
        None => tracing::info!("Output: stdout"),
    }

    let start = std::time::Instant::now();
    let memory_limit = args.memory_limit.as_bytes();

    let count = hdt::compute_void(
        &args.hdt_file,
        &args.dataset_uri,
        args.output.as_deref(),
        args.use_blank_nodes,
        memory_limit,
    )?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (void): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }

    match &args.output {
        Some(p) => tracing::info!("Done! {count} VoID triples written to {}", p.display()),
        None => tracing::info!("Done! {count} VoID triples written"),
    }
    Ok(())
}

/// Count namespace usage in an HDT dictionary.
fn compute_namespaces(args: cli::NamespacesArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.is_file() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    tracing::info!("Counting HDT namespaces: {}", args.hdt_file.display());
    match &args.output {
        Some(path) => tracing::info!("Output: {}", path.display()),
        None => tracing::info!("Output: stdout"),
    }

    let format = match args.format {
        cli::NamespaceOutputFormat::Json => hdt::NamespaceOutputFormat::Json,
        cli::NamespaceOutputFormat::Yaml => hdt::NamespaceOutputFormat::Yaml,
    };
    let start = std::time::Instant::now();
    let summary = hdt::write_namespace_inventory(hdt::NamespaceConfig {
        hdt_path: &args.hdt_file,
        prefix_paths: &args.prefixes,
        output_path: args.output.as_deref(),
        format,
        include_examples: args.example || !args.no_example,
        memory_limit: args.memory_limit.as_bytes(),
    })?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (namespaces): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }
    tracing::info!(
        "Done! {} namespace(s) emitted from {} prefix entr{}",
        summary.namespaces_emitted,
        summary.prefixes_loaded,
        if summary.prefixes_loaded == 1 {
            "y"
        } else {
            "ies"
        }
    );
    Ok(())
}

/// Build role-specific membership filters and MinHash sketches.
fn create_sketches(args: cli::SketchArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.is_file() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    let output_dir = args.output_dir.unwrap_or_else(|| {
        args.hdt_file
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("filters")
    });
    let mut roles: Vec<hdt::SketchRole> = Vec::with_capacity(args.roles.len());
    for role in &args.roles {
        let role = match role {
            cli::SketchRole::Subjects => hdt::SketchRole::Subjects,
            cli::SketchRole::Objects => hdt::SketchRole::Objects,
        };
        if !roles.contains(&role) {
            roles.push(role);
        }
    }

    let temp_dir = match &args.temp_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create temp dir {}", dir.display()))?;
            dir.clone()
        }
        None => make_default_temp_dir()?,
    };

    tracing::info!("Building sketches: {}", args.hdt_file.display());
    tracing::info!("Output directory: {}", output_dir.display());
    tracing::info!("Temp directory: {}", temp_dir.display());
    tracing::info!(
        "Roles: {}",
        roles
            .iter()
            .map(|role| role.file_stem())
            .collect::<Vec<_>>()
            .join(",")
    );
    tracing::info!("MinHash k: {}", args.k);
    tracing::info!("Filter bits: {}", args.filter_bits.as_u8());

    let start = std::time::Instant::now();
    let summary = hdt::create_sketches(hdt::SketchConfig {
        hdt_path: &args.hdt_file,
        output_dir: &output_dir,
        temp_dir: &temp_dir,
        roles: &roles,
        k: args.k,
        filter_bits: args.filter_bits.as_u8(),
        memory_limit: args.memory_limit.as_bytes(),
    })?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (sketch): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }
    tracing::info!(
        "Done! {} file(s) written ({})",
        summary.files_written,
        summary
            .role_counts
            .iter()
            .map(|(role, count)| format!("{}: {count} IRIs", role.file_stem()))
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(())
}

/// Build exact role-specific key sets.
fn create_keysets(args: cli::KeysetArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.is_file() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    let output_dir = args.output_dir.unwrap_or_else(|| {
        args.hdt_file
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("keysets")
    });
    let mut roles: Vec<hdt::KeyRole> = Vec::with_capacity(args.roles.len());
    for role in &args.roles {
        let role = match role {
            cli::KeysetRole::Subjects => hdt::KeyRole::Subjects,
            cli::KeysetRole::Objects => hdt::KeyRole::Objects,
            cli::KeysetRole::Predicates => hdt::KeyRole::Predicates,
            cli::KeysetRole::Shared => hdt::KeyRole::Shared,
            cli::KeysetRole::SubjectsOnly => hdt::KeyRole::SubjectsOnly,
            cli::KeysetRole::ObjectsOnly => hdt::KeyRole::ObjectsOnly,
        };
        if !roles.contains(&role) {
            roles.push(role);
        }
    }
    let encoding = match args.encoding {
        cli::KeysetEncoding::EliasFano => hdt::KeysetEncoding::EliasFano,
        cli::KeysetEncoding::Raw => hdt::KeysetEncoding::Raw,
    };

    let temp_dir = match &args.temp_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create temp dir {}", dir.display()))?;
            dir.clone()
        }
        None => make_default_temp_dir()?,
    };

    tracing::info!("Building key sets: {}", args.hdt_file.display());
    tracing::info!("Output directory: {}", output_dir.display());
    tracing::info!("Temp directory: {}", temp_dir.display());
    tracing::info!(
        "Roles: {}",
        roles
            .iter()
            .map(|role| role.file_stem())
            .collect::<Vec<_>>()
            .join(",")
    );
    tracing::info!("Encoding: {}", encoding.label());

    let start = std::time::Instant::now();
    let summary = hdt::create_keysets(hdt::KeysetConfig {
        hdt_path: &args.hdt_file,
        output_dir: &output_dir,
        temp_dir: &temp_dir,
        roles: &roles,
        encoding,
        memory_limit: args.memory_limit.as_bytes(),
    })?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (keyset): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }
    tracing::info!(
        "Done! {} file(s) written ({})",
        summary.files_written,
        summary
            .roles
            .iter()
            .map(|role| format!(
                "{}: {} keys, {} bytes, {:.2} B/key",
                role.role.file_stem(),
                role.key_count,
                role.file_bytes,
                role.bytes_per_key()
            ))
            .collect::<Vec<_>>()
            .join("; ")
    );
    Ok(())
}

/// Dump or modify the RDF triples embedded in an HDT file's header.
fn run_header(args: cli::HeaderArgs, benchmark: bool) -> Result<()> {
    let start = std::time::Instant::now();

    hdt::run_header_command(
        &args.hdt_file,
        args.replace.as_deref(),
        args.add.as_deref(),
        args.dataset_uri.as_deref(),
        args.output.as_deref(),
    )?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (header): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

/// Validate an HDT and any discovered graph/permutation sidecars.
fn validate_hdt_file(args: cli::ValidateArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.exists() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    tracing::info!(
        "Validating HDT triples structures: {}",
        args.hdt_file.display()
    );

    let start = std::time::Instant::now();
    match index::validate_hdt_triples(&args.hdt_file) {
        Ok(()) => {
            let sidecar_path = quads::canonical_sidecar_path(&args.hdt_file);
            let graph_index_path = graph_index::canonical_path(&args.hdt_file);
            // Strict graph-index validation includes strict validation of both
            // parents and reuses the sidecar's position-major membership stream.
            // Running the standalone sidecar validator first would externally
            // sort every membership twice.
            if sidecar_path.exists() && !graph_index_path.exists() {
                tracing::info!("Validating graph sidecar: {}", sidecar_path.display());
                let temp_dir = match &args.temp_dir {
                    Some(path) => {
                        std::fs::create_dir_all(path)?;
                        path.clone()
                    }
                    None => make_default_temp_dir()?,
                };
                let mut sidecar = quads::GraphSidecarReader::open(&sidecar_path, &args.hdt_file)?;
                sidecar.validate_strict(&temp_dir, args.memory_limit.as_bytes(), None)?;
            }
            let perm_path = permutation::canonical_path(&args.hdt_file);
            if perm_path.exists() {
                tracing::info!("Validating permutation index: {}", perm_path.display());
                let temp_dir = match &args.temp_dir {
                    Some(path) => {
                        std::fs::create_dir_all(path)?;
                        path.clone()
                    }
                    None => make_default_temp_dir()?,
                };
                permutation::validate_permutation_index(
                    &perm_path,
                    &args.hdt_file,
                    &temp_dir,
                    args.memory_limit.as_bytes(),
                )?;
            }
            if graph_index_path.exists() {
                tracing::info!(
                    "Validating graph sidecar and graph index: {}",
                    graph_index_path.display()
                );
                let temp_dir = match &args.temp_dir {
                    Some(path) => {
                        std::fs::create_dir_all(path)?;
                        path.clone()
                    }
                    None => make_default_temp_dir()?,
                };
                graph_index::validate_graph_index(
                    &graph_index_path,
                    &args.hdt_file,
                    &temp_dir,
                    args.memory_limit.as_bytes(),
                )?;
            }
            if benchmark {
                tracing::info!(
                    "Benchmark summary (validate): total {:.3}s",
                    start.elapsed().as_secs_f64()
                );
            }
            tracing::info!("Validation passed");
            Ok(())
        }
        Err(e) => {
            tracing::error!("Validation failed: {}", e);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_sidecar_can_be_restored() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar = dir.path().join("dataset.hdt.graphs");
        std::fs::write(&sidecar, b"old sidecar").unwrap();

        let retired = retire_existing_artifact(&sidecar, dir.path(), "graph sidecar")
            .unwrap()
            .unwrap();
        assert!(!sidecar.exists());
        assert_eq!(std::fs::read(&retired.backup_path).unwrap(), b"old sidecar");

        retired.restore().unwrap();
        drop(retired);
        assert_eq!(std::fs::read(sidecar).unwrap(), b"old sidecar");
    }

    #[test]
    fn sidecar_directory_is_not_retired() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar = dir.path().join("dataset.hdt.graphs");
        std::fs::create_dir(&sidecar).unwrap();

        let result = retire_existing_artifact(&sidecar, dir.path(), "graph sidecar");
        assert!(result.is_err());
        assert!(sidecar.is_dir());
    }
}
