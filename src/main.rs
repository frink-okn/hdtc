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

fn main() -> Result<()> {
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
        cli::Commands::Search(args) => search_hdt(args, benchmark),
        cli::Commands::Validate(args) => validate_hdt_file(args, benchmark),
        cli::Commands::Void(args) => compute_void(args, benchmark),
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
    tracing::info!("Mode: {:?}", args.mode);

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
            let first_path = inputs
                .first()
                .map(|i| &i.path)
                .or(hdt_inputs.first())
                .expect("at least one input file");
            let abs_path =
                std::fs::canonicalize(first_path).unwrap_or_else(|_| first_path.clone());
            format!("file://{}", abs_path.display())
        }
    };

    // Run the pipelined HDT construction
    let pipeline_result = pipeline::run_pipeline(
        &inputs,
        &hdt_inputs,
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

/// Dump an existing HDT file to N-Triples.
fn dump_hdt_to_ntriples(args: cli::DumpArgs, benchmark: bool) -> Result<()> {
    if !args.hdt_file.exists() {
        anyhow::bail!("HDT file not found: {}", args.hdt_file.display());
    }

    tracing::info!("Dumping HDT to N-Triples: {}", args.hdt_file.display());
    match &args.output {
        Some(p) => tracing::info!("Output: {}", p.display()),
        None => tracing::info!("Output: stdout"),
    }

    let start = std::time::Instant::now();
    let memory_limit = args.memory_limit.as_bytes();
    tracing::info!("Memory limit: {} bytes", memory_limit);
    let count =
        hdt::search_hdt_streaming(
            &args.hdt_file,
            "? ? ?",
            args.output.as_deref(),
            false,
            None,
            None,
            memory_limit,
            None,
            false,
        )?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (dump): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }

    match &args.output {
        Some(p) => tracing::info!("Done! {count} triples written to {}", p.display()),
        None => tracing::info!("Done! {count} triples written"),
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
    tracing::info!("Query: {}", args.query);

    let start = std::time::Instant::now();
    let memory_limit = args.memory_limit.as_bytes();

    let count = hdt::search_hdt_streaming(
        &args.hdt_file,
        &args.query,
        args.output.as_deref(),
        args.count,
        if args.count { None } else { args.limit },
        if args.count { None } else { args.offset },
        memory_limit,
        args.index.as_deref(),
        args.no_index,
    )?;

    if benchmark {
        tracing::info!(
            "Benchmark summary (search): total {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }

    tracing::info!("Done! {count} matching triple(s)");
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
