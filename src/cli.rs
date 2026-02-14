use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputMode {
    Triples,
    Quads,
}

#[derive(Debug, Parser)]
#[command(
    name = "hdtc",
    about = "HDT Creator - converts RDF files to HDT format",
    long_about = "Converts RDF files in any standard format to HDT (Header, Dictionary, Triples) \
                  binary format. Optimized for very large inputs with bounded memory usage."
)]
pub struct Cli {
    /// Input RDF files or directories containing RDF files
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,

    /// Output HDT file path
    #[arg(short, long)]
    pub output: PathBuf,

    /// Output mode: triples or quads
    #[arg(short, long, value_enum, default_value_t = OutputMode::Triples)]
    pub mode: OutputMode,

    /// Directory for temporary working files
    #[arg(long)]
    pub temp_dir: Option<PathBuf>,

    /// Generate HDT index file (.hdt.index.v1-1)
    #[arg(long)]
    pub index: bool,

    /// Base URI for the dataset
    #[arg(long, default_value = "http://example.org/dataset")]
    pub base_uri: String,

    /// Map input files/directories to named graphs (format: path=uri)
    #[arg(long = "graph-map", value_name = "PATH=URI")]
    pub graph_map: Vec<String>,

    /// Default graph URI for triples without an explicit graph (quads mode)
    #[arg(long)]
    pub default_graph: Option<String>,

    /// Soft memory limit in megabytes for internal buffers
    #[arg(long, value_name = "MB")]
    pub memory_limit: Option<usize>,

    /// Increase logging verbosity (-v for debug, -vv for trace)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Suppress all output except errors
    #[arg(short, long)]
    pub quiet: bool,
}
