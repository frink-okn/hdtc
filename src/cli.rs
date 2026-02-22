use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// A memory size parsed from a human-readable string like "4G" or "2000M".
/// Stores the value in bytes internally.
#[derive(Debug, Clone, Copy)]
pub struct MemorySize(usize);

impl MemorySize {
    pub fn as_bytes(self) -> usize {
        self.0
    }
}

impl std::str::FromStr for MemorySize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let (num_str, multiplier) = if let Some(n) = s.strip_suffix(['G', 'g']) {
            (n, 1024 * 1024 * 1024usize)
        } else if let Some(n) = s.strip_suffix(['M', 'm']) {
            (n, 1024 * 1024usize)
        } else {
            return Err(format!(
                "expected a size with suffix G or M (e.g. 4G, 2000M), got '{}'",
                s
            ));
        };
        let n: usize = num_str.trim().parse().map_err(|_| {
            format!(
                "expected a size with suffix G or M (e.g. 4G, 2000M), got '{}'",
                s
            )
        })?;
        Ok(MemorySize(n * multiplier))
    }
}

impl std::fmt::Display for MemorySize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.0;
        if bytes % (1024 * 1024 * 1024) == 0 {
            write!(f, "{}G", bytes / (1024 * 1024 * 1024))
        } else {
            write!(f, "{}M", bytes / (1024 * 1024))
        }
    }
}

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
                  binary format. Optimized for very large inputs with bounded memory usage. \
                  Can also create index files (.hdt.index.v1-1) for existing HDT files."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Increase logging verbosity (-v for debug, -vv for trace)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,

    /// Suppress all output except errors
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Emit stage-by-stage timing and RSS high-water summaries
    #[arg(long, global = true)]
    pub benchmark: bool,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Create HDT file from RDF input(s)
    Create(CreateArgs),

    /// Create index file for an existing HDT file
    Index(IndexArgs),
}

#[derive(Debug, Parser)]
pub struct CreateArgs {
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

    /// Base URI for the dataset (defaults to first input file's file:// URI if not specified)
    #[arg(long)]
    pub base_uri: Option<String>,

    /// Map input files/directories to named graphs (format: path=uri)
    #[arg(long = "graph-map", value_name = "PATH=URI")]
    pub graph_map: Vec<String>,

    /// Default graph URI for triples without an explicit graph (quads mode)
    #[arg(long)]
    pub default_graph: Option<String>,

    /// Soft memory limit for internal buffers (e.g. 4G, 2000M)
    #[arg(long, value_name = "SIZE", default_value = "4G")]
    pub memory_limit: MemorySize,

    /// Number of files to parse concurrently (default: auto)
    #[arg(long, value_name = "N")]
    pub parse_file_workers: Option<usize>,

    /// Number of parser workers per active NT/NQ file (default: auto)
    #[arg(long, value_name = "N")]
    pub parse_chunk_workers: Option<usize>,

    /// Target parser chunk size in bytes for NT/NQ parallel parsing (default: 8388608)
    #[arg(long, value_name = "BYTES")]
    pub parse_chunk_bytes: Option<usize>,

    /// Maximum in-flight parser chunk bytes per file (default: 268435456)
    #[arg(long, value_name = "BYTES")]
    pub parse_max_inflight_bytes: Option<usize>,
}

#[derive(Debug, Parser)]
pub struct IndexArgs {
    /// Path to existing HDT file
    pub hdt_file: PathBuf,

    /// Directory for temporary working files
    #[arg(long)]
    pub temp_dir: Option<PathBuf>,

    /// Soft memory limit for sorting operations (e.g. 4G, 2000M)
    #[arg(long, value_name = "SIZE", default_value = "4G")]
    pub memory_limit: MemorySize,
}
