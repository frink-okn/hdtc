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
        if n == 0 {
            return Err("memory size must be greater than zero".to_string());
        }
        n.checked_mul(multiplier)
            .map(MemorySize)
            .ok_or_else(|| "memory size is too large".to_string())
    }
}

impl std::fmt::Display for MemorySize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.0;
        if bytes.is_multiple_of(1024 * 1024 * 1024) {
            write!(f, "{}G", bytes / (1024 * 1024 * 1024))
        } else {
            write!(f, "{}M", bytes / (1024 * 1024))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputMode {
    Triples,
    Quads,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InputSidecarPolicy {
    Preserve,
    Require,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DumpGraphView {
    /// The N unique triples in the HDT, ignoring dataset graph layers.
    Union,
    /// The M distinct dataset memberships from the graph sidecar as N-Quads.
    Dataset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum SketchRole {
    Subjects,
    Objects,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SketchFilterBits {
    #[value(name = "8")]
    Eight,
    #[value(name = "16")]
    Sixteen,
}

impl SketchFilterBits {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Eight => 8,
            Self::Sixteen => 16,
        }
    }
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

    /// Export an HDT triples union or sidecar-backed RDF dataset
    Dump(DumpArgs),

    /// Search an HDT/sidecar with a triple or quad pattern
    Search(SearchArgs),

    /// Validate HDT structures and any discovered graph sidecar
    Validate(ValidateArgs),

    /// Compute VoID statistics for an HDT file and output as N-Triples
    Void(VoidArgs),

    /// Build role-specific membership filters and overlap sketches
    Sketch(SketchArgs),

    /// Dump or modify the RDF triples embedded in an HDT file's header
    Header(HeaderArgs),
}

#[derive(Debug, Parser)]
pub struct CreateArgs {
    /// Input RDF files or directories containing RDF files
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,

    /// Output HDT file path
    #[arg(short, long)]
    pub output: PathBuf,

    /// Output mode: triples drops graph information; quads writes a packed sidecar
    #[arg(short, long, value_enum, default_value = "triples")]
    pub mode: OutputMode,

    /// Handle .graphs files beside HDT inputs: preserve if present, require, or ignore
    #[arg(long = "input-sidecars", value_enum)]
    pub input_sidecars: Option<InputSidecarPolicy>,

    /// Directory for temporary working files
    #[arg(long)]
    pub temp_dir: Option<PathBuf>,

    /// Generate HDT index file (.hdt.index.v1-1)
    #[arg(long)]
    pub index: bool,

    /// Base URI used to resolve relative IRIs while parsing the input RDF
    /// (defaults to the first input file's file:// URI if not specified)
    #[arg(long)]
    pub base_uri: Option<String>,

    /// Dataset IRI recorded as the subject of the header metadata
    /// (defaults to the --base-uri value if not specified)
    #[arg(long)]
    pub dataset_uri: Option<String>,

    /// Map input files/directories to named graphs (format: path=uri)
    #[arg(long = "graph-map", value_name = "PATH=URI")]
    pub graph_map: Vec<String>,

    /// Fallback named graph URI for otherwise-unassigned statements
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

#[derive(Debug, Parser)]
pub struct ValidateArgs {
    /// Path to existing HDT file
    pub hdt_file: PathBuf,

    /// Directory for graph-sidecar validation sort files
    #[arg(long)]
    pub temp_dir: Option<PathBuf>,

    /// Soft memory limit for graph-sidecar validation (e.g. 4G, 2000M)
    #[arg(long, value_name = "SIZE", default_value = "4G")]
    pub memory_limit: MemorySize,
}

#[derive(Debug, Parser)]
pub struct SearchArgs {
    /// Path to existing HDT file
    pub hdt_file: PathBuf,

    /// Triple/quad pattern: 3 positions search the union, 4 search graph memberships
    ///
    /// Examples:
    ///   "<http://example.org/alice> ? ?"
    ///   "? <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?"
    ///   "? ? \"Alice\"@en"
    ///   "? ? ? <http://example.org/graph>"
    ///   "? ? ? default"
    #[arg(long, value_name = "PATTERN")]
    pub query: String,

    /// Write results to file instead of stdout
    #[arg(short, long, value_name = "PATH")]
    pub output: Option<PathBuf>,

    /// Print only the count of matching triples or graph memberships
    #[arg(long)]
    pub count: bool,

    /// Stop after N results (warning: ignored when combined with --count)
    #[arg(long, value_name = "N")]
    pub limit: Option<u64>,

    /// Skip the first N matching results
    #[arg(long, value_name = "N")]
    pub offset: Option<u64>,

    /// Index file path (default: <HDT_FILE>.hdt.index.v1-1)
    #[arg(long, value_name = "PATH")]
    pub index: Option<PathBuf>,

    /// Disable index use; fall back to sequential scan for all patterns
    #[arg(long)]
    pub no_index: bool,

    /// Directory for bounded external sorting used by wildcard-graph queries
    #[arg(long, value_name = "DIR")]
    pub temp_dir: Option<PathBuf>,

    /// Soft memory limit for dictionary caches and graph-membership sorting
    #[arg(short = 'm', long, value_name = "SIZE", default_value = "4G")]
    pub memory_limit: MemorySize,
}

#[derive(Debug, Parser)]
pub struct VoidArgs {
    /// Path to existing HDT file
    pub hdt_file: PathBuf,

    /// URI identifying the dataset being described
    #[arg(long, default_value = "http://example.org/dataset")]
    pub dataset_uri: String,

    /// Write results to file instead of stdout
    #[arg(short, long, value_name = "PATH")]
    pub output: Option<PathBuf>,

    /// Use blank nodes for partition identifiers instead of URI references
    #[arg(long)]
    pub use_blank_nodes: bool,

    /// Soft memory limit for dictionary caches (e.g. 4G, 2000M)
    ///
    /// Controls the PFC block cache used for term resolution during serialization.
    /// The analysis data structures (subject→class index, partition statistics) use
    /// additional memory proportional to the number of typed subjects and class/property
    /// combinations in the dataset.
    #[arg(short = 'm', long, value_name = "SIZE", default_value = "4G")]
    pub memory_limit: MemorySize,
}

#[derive(Debug, Parser)]
pub struct SketchArgs {
    /// Path to the existing HDT file
    pub hdt_file: PathBuf,

    /// Output directory (defaults to a filters/ directory beside the HDT)
    #[arg(short, long, value_name = "DIR")]
    pub output_dir: Option<PathBuf>,

    /// Bottom-k MinHash capacity
    #[arg(long, value_name = "N", default_value_t = 65_536, value_parser = clap::value_parser!(u32).range(2..))]
    pub k: u32,

    /// Binary fuse fingerprint width
    #[arg(long, value_enum, default_value = "8", value_name = "BITS")]
    pub filter_bits: SketchFilterBits,

    /// Dictionary roles to emit, as a comma-separated list
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "subjects,objects",
        value_name = "ROLE,..."
    )]
    pub roles: Vec<SketchRole>,

    /// Directory for temporary hashed-key files
    #[arg(long, value_name = "DIR")]
    pub temp_dir: Option<PathBuf>,

    /// Soft memory limit for binary fuse and MinHash construction (e.g. 4G, 2000M)
    #[arg(short = 'm', long, value_name = "SIZE", default_value = "4G")]
    pub memory_limit: MemorySize,
}

#[derive(Debug, Parser)]
pub struct DumpArgs {
    /// Path to existing HDT file
    pub hdt_file: PathBuf,

    /// Write results to file instead of stdout
    #[arg(short, long, value_name = "PATH")]
    pub output: Option<PathBuf>,

    /// Export the triples union or the lossless RDF dataset graph memberships
    #[arg(long, value_enum, default_value = "union")]
    pub graph_view: DumpGraphView,

    /// Directory for bounded external sorting used by dataset export
    #[arg(long, value_name = "DIR")]
    pub temp_dir: Option<PathBuf>,

    /// Soft memory limit for dictionary cache (e.g. 4G, 2000M)
    #[arg(long, value_name = "SIZE", default_value = "4G")]
    pub memory_limit: MemorySize,
}

#[derive(Debug, Parser)]
#[command(group(
    clap::ArgGroup::new("source").args(["replace", "add"])
))]
pub struct HeaderArgs {
    /// Path to existing HDT file
    pub hdt_file: PathBuf,

    /// Replace the descriptive header metadata with the triples from this RDF file
    /// (the data-derived statistics generated by hdtc are preserved)
    #[arg(long, value_name = "FILE")]
    pub replace: Option<PathBuf>,

    /// Augment the header with the triples from this RDF file
    #[arg(long, value_name = "FILE")]
    pub add: Option<PathBuf>,

    /// Rename the dataset IRI: rewrite every occurrence of the current dataset
    /// IRI in the header (as subject or object) to use this IRI instead
    #[arg(long, value_name = "IRI")]
    pub dataset_uri: Option<String>,

    /// Output path for the modified HDT file (required for any modification;
    /// the original file is never changed). With no modification flags, the
    /// header is dumped to stdout and this flag is not accepted.
    #[arg(short, long, value_name = "PATH")]
    pub output: Option<PathBuf>,
}
