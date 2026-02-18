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

fn main() -> Result<()> {
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

    tracing::info!("hdtc - HDT Creator");

    // Route to appropriate subcommand
    match cli.command {
        cli::Commands::Create(args) => create_hdt(args, benchmark),
        cli::Commands::Index(args) => create_index_from_hdt(args, benchmark),
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

    // Memory budget: default ~4GB or user-specified
    let memory_budget = args.memory_limit.unwrap_or(4096) * 1024 * 1024;

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

    // Run the new pipelined HDT construction
    tracing::info!("Using new pipelined architecture");
    let pipeline_result = pipeline::run_pipeline(
        &inputs,
        &temp_dir,
        memory_budget,
        include_graphs,
        &base_uri,
        &parser_parallelism,
        benchmark,
    )?;

    // Write HDT file
    hdt::write_hdt(
        &args.output,
        &base_uri,
        &pipeline_result.counts,
        &pipeline_result.dict_sections,
        &pipeline_result.bitmap_triples,
        pipeline_result.ntriples_size,
    )?;

    // Optionally create index file
    if args.index {
        tracing::info!("Creating index file...");
        match index::create_index(&args.output, memory_budget, &temp_dir) {
            Ok(index_path) => {
                tracing::info!("Index created: {}", index_path.display());
            }
            Err(e) => {
                tracing::error!("Failed to create index: {}", e);
                return Err(e);
            }
        }
    }

    tracing::info!(
        "Done! {} triples written to {}",
        pipeline_result.bitmap_triples.num_triples,
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

    tracing::info!("Creating index for: {}", args.hdt_file.display());

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

    // Memory budget: default ~4GB or user-specified
    let memory_budget = args.memory_limit.unwrap_or(4096) * 1024 * 1024;

    // Create the index
    let index_start = std::time::Instant::now();
    match index::create_index(&args.hdt_file, memory_budget, &temp_dir) {
        Ok(index_path) => {
            tracing::info!("Index created: {}", index_path.display());
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
