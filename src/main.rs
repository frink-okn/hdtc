mod cli;
mod dictionary;
mod hdt;
mod index;
mod io;
mod quads;
mod rdf;
mod triples;

use anyhow::Result;
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

    // TODO: Phase 3-5 pipeline
    tracing::warn!("HDT generation pipeline not yet implemented");

    Ok(())
}
