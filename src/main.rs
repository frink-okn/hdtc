mod cli;
mod dictionary;
mod hdt;
mod index;
mod io;
mod pipeline;
mod quads;
mod rdf;
mod sort;
mod triples;

use anyhow::{Context, Result};
use clap::Parser;
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

fn main() -> Result<()> {
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
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();

    if let Some((old, target)) = raised_fd_limit {
        tracing::debug!(old, target, "Raised file descriptor limit");
    }

    tracing::info!("hdtc - HDT Creator");

    // Route to appropriate subcommand
    match cli.command {
        cli::Commands::Create(args) => create_hdt(args, benchmark),
        cli::Commands::Index(args) => create_index_from_hdt(args, benchmark),
        cli::Commands::Dump(args) => dump_hdt_to_ntriples(args, benchmark),
        cli::Commands::Validate(args) => validate_hdt_file(args, benchmark),
    }
}

/// Create HDT file from RDF input(s)
fn create_hdt(args: cli::CreateArgs, benchmark: bool) -> Result<()> {
    // Discover input files
    let inputs = rdf::discover_inputs(&args.inputs)?;
    for input in &inputs {
        tracing::debug!(
            "  {} ({:?}, {:?})",
            input.path.display(),
            input.format,
            input.compression
        );
    }

    tracing::info!("Output: {}", args.output.display());
    tracing::info!("Mode: {:?}", args.mode);

    // Set up temp directory
    let temp_dir = match &args.temp_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create temp dir {}", dir.display()))?;
            dir.clone()
        }
        None => {
            let dir = std::env::temp_dir().join("hdtc_work");
            std::fs::create_dir_all(&dir)?;
            dir
        }
    };
    tracing::info!("Temp directory: {}", temp_dir.display());

    let memory_budget = args.memory_limit.as_bytes();
    tracing::info!("Memory limit: {}", args.memory_limit);

    let include_graphs = matches!(args.mode, cli::OutputMode::Quads);
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
            let first_input = &inputs[0];
            let abs_path = std::fs::canonicalize(&first_input.path)
                .unwrap_or_else(|_| first_input.path.clone());
            format!("file://{}", abs_path.display())
        }
    };

    // Run the pipelined HDT construction
    let pipeline_result = pipeline::run_pipeline(
        &inputs,
        &temp_dir,
        memory_budget,
        include_graphs,
        &base_uri,
        &parser_parallelism,
        benchmark,
    )?;

    // Write HDT file (streaming: reads dict sections and triples from temp files)
    hdt::write_hdt_streaming(
        &args.output,
        &base_uri,
        &pipeline_result.counts,
        &pipeline_result.dict_section_paths,
        &pipeline_result.dict_section_sizes,
        &pipeline_result.bitmap_triples,
        pipeline_result.ntriples_size,
    )?;

    let num_triples = pipeline_result.bitmap_triples.num_triples;

    // Clean up dict section and triples temp files
    for path in &pipeline_result.dict_section_paths {
        if let Err(e) = std::fs::remove_file(path) {
            tracing::debug!("Failed to remove dict section temp file: {e}");
        }
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
        None => {
            let dir = std::env::temp_dir().join("hdtc_work");
            std::fs::create_dir_all(&dir)?;
            dir
        }
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

/// Dump an existing HDT file to N-Triples.
fn dump_hdt_to_ntriples(args: cli::DumpArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.exists() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    tracing::info!("Dumping HDT to N-Triples: {}", args.hdt_file.display());
    tracing::info!("Output: {}", args.output.display());

    let start = std::time::Instant::now();
    let count = hdt::dump_hdt_to_ntriples_streaming(&args.hdt_file, &args.output)?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (dump): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }

    tracing::info!(
        "Done! {} triples written to {}",
        count,
        args.output.display()
    );
    Ok(())
}

/// Validate an existing HDT file's triples structures for indexing.
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
