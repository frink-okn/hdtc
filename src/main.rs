mod cli;
mod dictionary;
mod hdt;
mod index;
mod io;
mod quads;
mod rdf;
mod sort;
mod triples;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let cli = cli::Cli::parse();

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

    // Discover input files
    let inputs = rdf::discover_inputs(&cli.inputs)?;
    for input in &inputs {
        tracing::debug!(
            "  {} ({:?}, {:?})",
            input.path.display(),
            input.format,
            input.compression
        );
    }

    tracing::info!("Output: {}", cli.output.display());
    tracing::info!("Mode: {:?}", cli.mode);

    // Set up temp directory
    let temp_dir = match &cli.temp_dir {
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
    let memory_budget = cli.memory_limit.unwrap_or(4096) * 1024 * 1024;

    let include_graphs = matches!(cli.mode, cli::OutputMode::Quads);
    let base_uri = &cli.base_uri;

    // Phase 3: Build dictionary
    let dict_result = dictionary::build_dictionary(
        &inputs,
        &temp_dir,
        memory_budget,
        include_graphs,
        Some(base_uri),
    )?;

    // Phase 4: Generate and sort ID triples
    let sorted_triples = triples::generate_id_triples(
        &inputs,
        &dict_result.sst,
        &dict_result.predicate_ids,
        &dict_result.counts,
        &temp_dir,
        memory_budget,
        Some(base_uri),
    )?;

    // Phase 4b: Build BitmapTriples
    let bitmap_triples = triples::build_bitmap_triples(sorted_triples)?;

    // Phase 5: Assemble HDT file
    hdt::write_hdt(
        &cli.output,
        base_uri,
        &dict_result.counts,
        &dict_result.sections,
        &bitmap_triples,
    )?;

    // Clean up SST
    dict_result.sst.cleanup();

    tracing::info!(
        "Done! {} triples written to {}",
        bitmap_triples.num_triples,
        cli.output.display()
    );

    Ok(())
}
