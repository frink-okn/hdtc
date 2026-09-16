//! Streaming RDF parser wrapping oxrdfio with compression and blank node disambiguation.

use crate::dictionary::term::encode_literal;
use crate::rdf::input::{Compression, RdfFormat, RdfInput};
use anyhow::{Context, Result};
use crossbeam_channel::TrySendError;
use oxrdf::{BlankNode, GraphName, Literal, NamedOrBlankNode, Quad, Term, Triple};
use oxttl::n3::{N3Quad, N3Term};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

/// Default upper bound, in bytes, on one parsed term (an IRI or a literal).
///
/// The Turtle-family lexer buffers a single token at a time and refuses one
/// larger than its buffer; the released oxttl hard-codes that bound at 16 MiB.
/// Real data exceeds it — GADM ships country boundaries as WKT literals up to
/// 86 MB — and the released parser does not skip such a term: its reader loop
/// reports the same error again without advancing, forever. The vendored oxttl
/// under `vendor/oxttl` exposes the bound, hdtc passes this value unless
/// `--max-term-bytes` says otherwise, and an input holding a term past the
/// bound fails at once, naming the flag. The buffer grows on demand and
/// compacts at the same point as before, so raising the bound costs only what
/// the largest term actually needs.
pub const DEFAULT_MAX_TERM_BYTES: usize = 256 * 1024 * 1024;

/// What the lexer buffer holds beyond the term itself: the term's delimiters
/// and the few bytes of lookahead that end it. Added to the bound when a
/// parser is built, so a term of exactly `--max-term-bytes` is accepted.
const TERM_SLACK: usize = 4096;

/// How many consecutive interrupted reads are retried before the input is
/// failed. A signal mid-`read(2)` clears on retry; a reader that reports
/// `Interrupted` forever would otherwise spin with no diagnostic.
const MAX_INTERRUPTED_RETRIES: u32 = 1000;

/// Parser parallelism controls.
#[derive(Debug, Clone)]
pub struct ParseOptions {
    pub enable_ntnq_parallel: bool,
    pub chunk_size_bytes: usize,
    pub chunk_workers: usize,
    pub max_inflight_bytes: usize,
    /// Largest single IRI or literal accepted; an input holding a larger one
    /// fails, since the lexer cannot skip what it cannot buffer. See
    /// [`DEFAULT_MAX_TERM_BYTES`].
    pub max_term_bytes: usize,
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self {
            enable_ntnq_parallel: true,
            chunk_size_bytes: 8 * 1024 * 1024,
            chunk_workers: std::thread::available_parallelism()
                .map(|n| n.get().max(1))
                .unwrap_or(4),
            max_inflight_bytes: 256 * 1024 * 1024,
            max_term_bytes: DEFAULT_MAX_TERM_BYTES,
        }
    }
}

#[derive(Debug)]
struct ChunkTask {
    sequence: u64,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct ChunkParsed {
    sequence: u64,
    quads: Vec<ExtractedQuad>,
    stats: ParseStats,
    error_samples: Vec<String>,
    /// Set when the chunk hit an error the parser cannot get past
    /// ([`ParseError::is_fatal`]); the message ends the whole input.
    fatal: Option<String>,
}

#[derive(Debug)]
struct InflightBudget {
    state: Mutex<usize>,
    condvar: Condvar,
    limit: usize,
}

impl InflightBudget {
    fn new(limit: usize) -> Self {
        Self {
            state: Mutex::new(0),
            condvar: Condvar::new(),
            limit: limit.max(1),
        }
    }

    fn acquire(&self, bytes: usize) {
        let bytes = bytes.max(1).min(self.limit);
        let mut used = self.state.lock().expect("inflight budget mutex poisoned");
        while *used + bytes > self.limit {
            used = self
                .condvar
                .wait(used)
                .expect("inflight budget condvar wait failed");
        }
        *used += bytes;
    }

    fn release(&self, bytes: usize) {
        let bytes = bytes.max(1).min(self.limit);
        let mut used = self.state.lock().expect("inflight budget mutex poisoned");
        *used = used.saturating_sub(bytes);
        self.condvar.notify_all();
    }
}

/// A canonical RDF term string and its role in a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedQuad {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub graph: Option<Arc<str>>,
}

#[derive(Default)]
struct GraphInterner {
    terms: HashMap<Arc<str>, ()>,
    last: Option<Arc<str>>,
}

impl GraphInterner {
    fn intern(&mut self, term: &str) -> Arc<str> {
        if let Some(last) = &self.last
            && last.as_ref() == term
        {
            return Arc::clone(last);
        }
        if let Some((existing, ())) = self.terms.get_key_value(term) {
            self.last = Some(Arc::clone(existing));
            return Arc::clone(existing);
        }

        // Sequential parsers do not have natural chunk lifetimes, so cap the
        // cache as well as using per-chunk instances in the parallel parser.
        if self.terms.len() >= 4_096 {
            self.terms.clear();
        }
        let term: Arc<str> = Arc::from(term);
        self.terms.insert(Arc::clone(&term), ());
        self.last = Some(Arc::clone(&term));
        term
    }
}

fn extract_graph_name<'a>(graph_name: &'a GraphName, blank_prefix: &str) -> Option<Cow<'a, str>> {
    match graph_name {
        GraphName::DefaultGraph => None,
        GraphName::NamedNode(node) => Some(Cow::Borrowed(node.as_str())),
        GraphName::BlankNode(node) => {
            Some(Cow::Owned(format!("_:{}{}", blank_prefix, node.as_str())))
        }
    }
}

/// Convert our RdfFormat enum to oxrdfio's RdfFormat.
fn to_oxrdf_format(format: RdfFormat) -> oxrdfio::RdfFormat {
    match format {
        RdfFormat::NTriples => oxrdfio::RdfFormat::NTriples,
        RdfFormat::NQuads => oxrdfio::RdfFormat::NQuads,
        RdfFormat::Turtle => oxrdfio::RdfFormat::Turtle,
        RdfFormat::TriG => oxrdfio::RdfFormat::TriG,
        RdfFormat::RdfXml => oxrdfio::RdfFormat::RdfXml,
        RdfFormat::N3 => oxrdfio::RdfFormat::N3,
        RdfFormat::JsonLd => oxrdfio::RdfFormat::JsonLd {
            profile: oxrdfio::JsonLdProfileSet::empty(),
        },
    }
}

/// Open a file with optional decompression, returning a boxed reader.
fn open_input(input: &RdfInput) -> Result<Box<dyn Read>> {
    let file = File::open(&input.path)
        .with_context(|| format!("Failed to open {}", input.path.display()))?;
    let buf_reader = BufReader::with_capacity(256 * 1024, file);

    let reader: Box<dyn Read> = match input.compression {
        Compression::None => Box::new(buf_reader),
        Compression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(buf_reader)),
        Compression::Bzip2 => Box::new(bzip2::read::MultiBzDecoder::new(buf_reader)),
        Compression::Xz => Box::new(xz2::read::XzDecoder::new_multi_decoder(buf_reader)),
        Compression::Zstd => {
            Box::new(zstd::Decoder::with_buffer(buf_reader).with_context(|| {
                format!(
                    "Failed to initialize Zstandard decoder for {}",
                    input.path.display()
                )
            })?)
        }
    };

    Ok(reader)
}

/// Stream quads from an RDF input file, calling the callback for each quad.
///
/// When `disambiguate_blank_nodes` is true, blank nodes are disambiguated by
/// prefixing with `f{file_index}_`.
/// Malformed input is skipped with a warning; the total skip count is returned.
#[cfg_attr(not(test), allow(dead_code))]
pub fn stream_quads<F>(
    input: &RdfInput,
    file_index: usize,
    disambiguate_blank_nodes: bool,
    base_uri: Option<&str>,
    callback: F,
) -> Result<ParseStats>
where
    F: FnMut(ExtractedQuad) -> Result<()>,
{
    stream_quads_with_options(
        input,
        file_index,
        disambiguate_blank_nodes,
        base_uri,
        &ParseOptions::default(),
        callback,
    )
}

/// Stream quads with explicit parser options.
pub fn stream_quads_with_options<F>(
    input: &RdfInput,
    file_index: usize,
    disambiguate_blank_nodes: bool,
    base_uri: Option<&str>,
    options: &ParseOptions,
    mut callback: F,
) -> Result<ParseStats>
where
    F: FnMut(ExtractedQuad) -> Result<()>,
{
    if options.enable_ntnq_parallel
        && options.chunk_workers > 1
        && matches!(input.format, RdfFormat::NTriples | RdfFormat::NQuads)
    {
        return stream_quads_parallel_ntnq(
            input,
            file_index,
            disambiguate_blank_nodes,
            base_uri,
            options,
            callback,
        );
    }

    stream_quads_sequential(
        input,
        file_index,
        disambiguate_blank_nodes,
        base_uri,
        options.max_term_bytes,
        &mut callback,
    )
}

/// The triples of an HDT header, which is N-Triples text held whole in memory.
///
/// No term bound applies: `hdtc header` accepts terms up to its own
/// `--max-term-bytes`, and every reader of a header must accept whatever the
/// writer wrote. What makes that so is `for_slice`: a lexer over a slice has
/// no buffer to bound, so `with_max_buffer_size` would be inert here, whereas
/// `for_reader` would reinstate the fixed 16 MiB buffer and break those
/// headers again. Strict, not lenient: a malformed line is an error, never
/// skipped.
pub(crate) fn header_triples(
    text: &[u8],
) -> impl Iterator<Item = std::result::Result<Triple, oxttl::TurtleSyntaxError>> + '_ {
    oxttl::NTriplesParser::new().for_slice(text)
}

/// The counts an HDT header records about its data.
#[derive(Debug)]
pub(crate) struct HeaderCounts {
    /// `void:triples` and `hdt:triplesnumTriples`, which must agree when both
    /// are present.
    pub triples: u64,
    /// `hdt:originalSize`, or zero when absent.
    pub original_size: u64,
}

/// Read the counts out of an HDT header. One scan for every reader of the
/// header, which used to carry four copies of it. A count stated twice with
/// different values is refused, as the sidecar reader's copy always did: the
/// count is load-bearing, and nothing hdtc writes repeats it.
pub(crate) fn header_counts(text: &[u8]) -> Result<HeaderCounts> {
    const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";
    const HDT_TRIPLES_NUM: &str = "http://purl.org/HDT/hdt#triplesnumTriples";
    const ORIGINAL_SIZE: &str = "http://purl.org/HDT/hdt#originalSize";

    let mut from_void: Option<u64> = None;
    let mut from_hdt: Option<u64> = None;
    let mut original_size = 0u64;

    for result in header_triples(text) {
        let triple = result.context("Invalid N-Triples in HDT header metadata")?;
        let predicate = triple.predicate.as_str();
        if predicate != VOID_TRIPLES && predicate != HDT_TRIPLES_NUM && predicate != ORIGINAL_SIZE {
            continue;
        }
        let Term::Literal(literal) = triple.object else {
            continue;
        };
        if predicate == ORIGINAL_SIZE {
            if let Ok(size) = literal.value().parse::<u64>() {
                original_size = size;
            }
            continue;
        }
        let parsed = literal.value().parse::<u64>().with_context(|| {
            format!("Invalid numeric triple-count literal: {}", literal.value())
        })?;
        let slot = if predicate == VOID_TRIPLES {
            &mut from_void
        } else {
            &mut from_hdt
        };
        if let Some(previous) = *slot
            && previous != parsed
        {
            anyhow::bail!(
                "Conflicting HDT header triple counts: <{predicate}> is stated as {previous} and as {parsed}"
            );
        }
        *slot = Some(parsed);
    }

    let triples = match (from_void, from_hdt) {
        (Some(v), Some(h)) if v != h => anyhow::bail!(
            "Header triple-count mismatch between void:triples ({v}) and hdt:triplesnumTriples ({h})"
        ),
        (Some(v), _) => v,
        (None, Some(h)) => h,
        (None, None) => anyhow::bail!("Header metadata missing triple-count predicate"),
    };
    Ok(HeaderCounts {
        triples,
        original_size,
    })
}

/// Triples parsed from an RDF input, in oxrdf form (for header serialization).
#[derive(Debug)]
pub(crate) struct ParsedTriples {
    pub triples: Vec<Triple>,
    /// True if any quad carried a non-default graph (dropped — headers are triples-only).
    pub named_graph_seen: bool,
}

/// Parse an RDF input file into oxrdf `Triple`s, dropping graph names.
///
/// Unlike [`stream_quads`], this preserves native oxrdf terms (rather than the
/// dictionary string form) so the triples can be re-serialized as N-Triples for
/// an HDT header. When `blank_prefix` is non-empty, blank-node labels are
/// prefixed with it, which keeps blank nodes from different sources disjoint
/// (used when merging input triples into an existing header).
///
/// Strict, and any error ends the parse: what is written into a header is read
/// back strictly by everything that opens the HDT, so a leniently accepted term
/// (an IRI with a space) or a skipped line would make the file unreadable or
/// silently incomplete.
pub(crate) fn parse_rdf_to_triples(
    input: &RdfInput,
    base_uri: Option<&str>,
    blank_prefix: &str,
    max_term_bytes: usize,
) -> Result<ParsedTriples> {
    let reader = open_input(input)?;
    let parser = FormatParser::new(input.format, base_uri, max_term_bytes, Strictness::Strict);

    let mut out = ParsedTriples {
        triples: Vec::new(),
        named_graph_seen: false,
    };
    let mut interrupted = 0u32;

    for result in parser.for_reader(reader) {
        match result {
            Ok(quad) => {
                interrupted = 0;
                if !matches!(quad.graph_name, GraphName::DefaultGraph) {
                    out.named_graph_seen = true;
                }
                let subject = prefix_blank_subject(quad.subject, blank_prefix);
                let object = prefix_blank_object(quad.object, blank_prefix);
                out.triples
                    .push(Triple::new(subject, quad.predicate, object));
            }
            Err(e) if e.is_interrupted() && interrupted < MAX_INTERRUPTED_RETRIES => {
                interrupted += 1;
            }
            Err(e) => return Err(e.into_fatal(&input.path, max_term_bytes)),
        }
    }

    Ok(out)
}

/// Prefix a subject's blank-node label, leaving named nodes untouched.
fn prefix_blank_subject(subject: NamedOrBlankNode, prefix: &str) -> NamedOrBlankNode {
    if prefix.is_empty() {
        return subject;
    }
    match subject {
        NamedOrBlankNode::BlankNode(b) => {
            NamedOrBlankNode::BlankNode(BlankNode::new_unchecked(format!("{prefix}{}", b.as_str())))
        }
        named => named,
    }
}

/// Prefix an object's blank-node label, leaving named nodes and literals untouched.
fn prefix_blank_object(object: Term, prefix: &str) -> Term {
    if prefix.is_empty() {
        return object;
    }
    match object {
        Term::BlankNode(b) => {
            Term::BlankNode(BlankNode::new_unchecked(format!("{prefix}{}", b.as_str())))
        }
        other => other,
    }
}

fn stream_quads_sequential<F>(
    input: &RdfInput,
    file_index: usize,
    disambiguate_blank_nodes: bool,
    base_uri: Option<&str>,
    max_term_bytes: usize,
    callback: &mut F,
) -> Result<ParseStats>
where
    F: FnMut(ExtractedQuad) -> Result<()>,
{
    let reader = open_input(input)?;
    stream_quads_from_reader(
        reader,
        input,
        file_index,
        disambiguate_blank_nodes,
        base_uri,
        max_term_bytes,
        callback,
    )
}

/// The sequential parse over an already-open reader, split from
/// [`stream_quads_sequential`] so a test can supply a reader that misbehaves.
fn stream_quads_from_reader<F>(
    reader: impl Read,
    input: &RdfInput,
    file_index: usize,
    disambiguate_blank_nodes: bool,
    base_uri: Option<&str>,
    max_term_bytes: usize,
    callback: &mut F,
) -> Result<ParseStats>
where
    F: FnMut(ExtractedQuad) -> Result<()>,
{
    let parser = FormatParser::new(input.format, base_uri, max_term_bytes, Strictness::Lenient);

    let blank_prefix = if disambiguate_blank_nodes {
        format!("f{file_index}_")
    } else {
        String::new()
    };
    let mut stats = ParseStats::default();
    let mut graph_interner = GraphInterner::default();
    let mut interrupted = 0u32;

    for result in parser.for_reader(reader) {
        match result {
            Ok(quad) => {
                interrupted = 0;
                // Calculate original N-Triples size BEFORE adding blank node prefix
                let original_size = calculate_original_ntriples_size(
                    &quad.subject,
                    &quad.predicate,
                    &quad.object,
                    &quad.graph_name,
                );
                stats.original_ntriples_size += original_size;

                let subject = term_to_hdt_string(&Term::from(quad.subject), &blank_prefix);
                let predicate = quad.predicate.as_str().to_string();
                let object = term_to_hdt_string(&quad.object, &blank_prefix);
                let graph = extract_graph_name(&quad.graph_name, &blank_prefix)
                    .map(|graph| graph_interner.intern(graph.as_ref()));

                stats.quads += 1;
                callback(ExtractedQuad {
                    subject,
                    predicate,
                    object,
                    graph,
                })?;
            }
            Err(e) if e.is_interrupted() && interrupted < MAX_INTERRUPTED_RETRIES => {
                interrupted += 1;
            }
            Err(e) if e.is_fatal() => return Err(e.into_fatal(&input.path, max_term_bytes)),
            Err(e) => {
                interrupted = 0;
                stats.errors += 1;
                if stats.errors <= 10 {
                    tracing::warn!(
                        "Skipping malformed input in {}: {}",
                        input.path.display(),
                        e
                    );
                } else if stats.errors == 11 {
                    tracing::warn!(
                        "Further parse errors in {} will be suppressed",
                        input.path.display()
                    );
                }
            }
        }
    }

    if stats.errors > 0 {
        tracing::warn!(
            "{}: parsed {} quads, skipped {} errors",
            input.path.display(),
            stats.quads,
            stats.errors
        );
    }

    Ok(stats)
}

/// A parse failure from whichever parser produced it.
///
/// Inspected for what kind of failure it is — skippable, retryable or fatal —
/// then logged for the first few per input and counted for the rest. Wrapping
/// the original error keeps its source chain for the fatal case.
#[derive(Debug, thiserror::Error)]
enum ParseError {
    #[error(transparent)]
    Turtle(#[from] oxttl::TurtleParseError),
    #[error(transparent)]
    Rdf(#[from] oxrdfio::RdfParseError),
    /// A well-formed N3 statement that is not an RDF quad (a variable, or a
    /// literal in subject position). Skippable, like a syntax error.
    #[error("{0}")]
    N3(&'static str),
}

impl ParseError {
    /// The I/O error underneath, if this is one.
    fn io_error(&self) -> Option<&io::Error> {
        match self {
            Self::Turtle(oxttl::TurtleParseError::Io(e))
            | Self::Rdf(oxrdfio::RdfParseError::Io(e)) => Some(e),
            _ => None,
        }
    }

    /// A read interrupted by a signal. Nothing was consumed, so the same read
    /// is tried again, as every `Read` loop in std does — up to
    /// [`MAX_INTERRUPTED_RETRIES`] times in a row, after which it is fatal.
    fn is_interrupted(&self) -> bool {
        self.io_error()
            .is_some_and(|e| e.kind() == io::ErrorKind::Interrupted)
    }

    /// Whether this error ends the input rather than being skipped.
    ///
    /// A syntax error is skippable: the parser has consumed the bad token and
    /// resynchronises after it. An I/O error is not: the reader loop reports
    /// it without moving the lexer, so asking again yields the same error
    /// forever. That is also how a term past the buffer bound surfaces, and
    /// the released oxttl spins on exactly that case, rescanning its full
    /// buffer per iteration. An interrupted read is the one I/O error retried
    /// first; the callers give up on it after [`MAX_INTERRUPTED_RETRIES`].
    fn is_fatal(&self) -> bool {
        self.io_error().is_some()
    }

    /// The lexer refused a single term larger than its buffer — recognised by
    /// the cause the vendored lexer attaches, not by `ErrorKind::OutOfMemory`,
    /// which a decompressor failing to allocate can produce too.
    fn exceeds_term_bound(&self) -> bool {
        matches!(
            self,
            Self::Turtle(oxttl::TurtleParseError::Io(e))
                if e.get_ref().is_some_and(|cause| cause.is::<oxttl::BufferLimitExceeded>())
        )
    }

    /// The error to fail the input with: the path and the remedy, when there
    /// is one, with the original error as its cause.
    fn into_fatal(self, path: &Path, max_term_bytes: usize) -> anyhow::Error {
        let what = if self.exceeds_term_bound() {
            format!(
                "a single IRI or literal is larger than --max-term-bytes ({max_term_bytes} bytes); raise it"
            )
        } else if let Some(e) = self.io_error() {
            read_failure(e)
        } else {
            // Only the strict path sends a syntax error here; the lenient ones
            // skip it. The cause says what was wrong, and where.
            "invalid RDF".to_owned()
        };
        anyhow::Error::new(self).context(format!("{}: {what}", path.display()))
    }
}

type ParsedQuad = std::result::Result<Quad, ParseError>;

/// What to say about an I/O error that ends an input: a read the callers gave
/// up retrying, or one that failed outright.
fn read_failure(e: &io::Error) -> String {
    if e.kind() == io::ErrorKind::Interrupted {
        format!("read interrupted {MAX_INTERRUPTED_RETRIES} times in a row")
    } else {
        "unrecoverable read error".to_owned()
    }
}

/// One read, retried on `Interrupted` as every `Read` loop in std does, up to
/// [`MAX_INTERRUPTED_RETRIES`] times in a row; after that the error stands.
fn read_retrying_interrupted(reader: &mut dyn Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut interrupted = 0u32;
    loop {
        match reader.read(buf) {
            Err(e)
                if e.kind() == io::ErrorKind::Interrupted
                    && interrupted < MAX_INTERRUPTED_RETRIES =>
            {
                interrupted += 1;
            }
            result => return result,
        }
    }
}

/// Apply the base IRI to a parser builder, keeping the builder unchanged when
/// the base does not parse as an IRI. The default base is the input's own
/// `file://` path, which always does.
fn with_base<P: Clone, E>(
    parser: P,
    base_uri: Option<&str>,
    apply: impl FnOnce(P, &str) -> std::result::Result<P, E>,
) -> P {
    match base_uri {
        Some(base) => apply(parser.clone(), base).unwrap_or(parser),
        None => parser,
    }
}

/// Whether a parser skips what it cannot validate or refuses it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Strictness {
    /// Data loads: malformed statements are skipped and counted.
    Lenient,
    /// Header edits: what is written is read back strictly, so nothing that
    /// would fail that read may be accepted.
    Strict,
}

/// A parser for one input format, honouring the term-size bound.
///
/// The Turtle family — Turtle, TriG, N-Triples, N-Quads and N3 — is built from
/// the vendored oxttl directly, because oxrdfio's generic `RdfParser` offers no
/// way to set the bound ([`DEFAULT_MAX_TERM_BYTES`]); RDF/XML and JSON-LD go
/// through oxrdfio as before.
enum FormatParser {
    Turtle(oxttl::TurtleParser),
    TriG(oxttl::TriGParser),
    NTriples(oxttl::NTriplesParser),
    NQuads(oxttl::NQuadsParser),
    N3(oxttl::N3Parser),
    Generic(oxrdfio::RdfParser),
}

/// The RDF quad an N3 statement denotes, with oxrdfio's rules: variables are
/// never RDF terms, and a literal may not be a subject or a predicate.
fn n3_to_quad(quad: N3Quad) -> std::result::Result<Quad, ParseError> {
    let subject = match quad.subject {
        N3Term::NamedNode(node) => NamedOrBlankNode::from(node),
        N3Term::BlankNode(node) => NamedOrBlankNode::from(node),
        N3Term::Literal(_) => {
            return Err(ParseError::N3(
                "literals are not allowed in regular RDF subjects",
            ));
        }
        N3Term::Variable(_) => {
            return Err(ParseError::N3(
                "variables are not allowed in regular RDF subjects",
            ));
        }
    };
    let predicate = match quad.predicate {
        N3Term::NamedNode(node) => node,
        N3Term::BlankNode(_) => {
            return Err(ParseError::N3(
                "blank nodes are not allowed in regular RDF predicates",
            ));
        }
        N3Term::Literal(_) => {
            return Err(ParseError::N3(
                "literals are not allowed in regular RDF predicates",
            ));
        }
        N3Term::Variable(_) => {
            return Err(ParseError::N3(
                "variables are not allowed in regular RDF predicates",
            ));
        }
    };
    let object = match quad.object {
        N3Term::NamedNode(node) => Term::from(node),
        N3Term::BlankNode(node) => Term::from(node),
        N3Term::Literal(literal) => Term::from(literal),
        N3Term::Variable(_) => {
            return Err(ParseError::N3(
                "variables are not allowed in regular RDF objects",
            ));
        }
    };
    Ok(Quad::new(subject, predicate, object, quad.graph_name))
}

impl FormatParser {
    fn new(
        format: RdfFormat,
        base_uri: Option<&str>,
        max_term_bytes: usize,
        strictness: Strictness,
    ) -> Self {
        let lenient = strictness == Strictness::Lenient;
        // The lexer holds the term plus its delimiters and lookahead, so the
        // buffer is the bound plus slack: a term of exactly the bound fits.
        let buffer = max_term_bytes.saturating_add(TERM_SLACK);
        // One definition of the Turtle-family builder, so no format can miss
        // an option the others get: bound the buffer, relax if lenient, and
        // apply the base for the syntaxes that resolve relative IRIs.
        macro_rules! bounded {
            ($parser:expr) => {{
                let parser = $parser.with_max_buffer_size(buffer);
                if lenient { parser.lenient() } else { parser }
            }};
            ($parser:expr, base) => {
                with_base(bounded!($parser), base_uri, |parser, base| {
                    parser.with_base_iri(base)
                })
            };
        }
        match format {
            RdfFormat::Turtle => Self::Turtle(bounded!(oxttl::TurtleParser::new(), base)),
            RdfFormat::TriG => Self::TriG(bounded!(oxttl::TriGParser::new(), base)),
            RdfFormat::NTriples => Self::NTriples(bounded!(oxttl::NTriplesParser::new())),
            RdfFormat::NQuads => Self::NQuads(bounded!(oxttl::NQuadsParser::new())),
            RdfFormat::N3 => Self::N3(bounded!(oxttl::N3Parser::new(), base)),
            other => {
                let mut parser = oxrdfio::RdfParser::from_format(to_oxrdf_format(other));
                if lenient {
                    parser = parser.lenient();
                }
                Self::Generic(with_base(parser, base_uri, |parser, base| {
                    parser.with_base_iri(base)
                }))
            }
        }
    }

    fn for_reader<R: Read>(self, reader: R) -> LenientQuads<R> {
        match self {
            Self::Turtle(parser) => LenientQuads::Turtle(parser.for_reader(reader)),
            Self::TriG(parser) => LenientQuads::TriG(parser.for_reader(reader)),
            Self::NTriples(parser) => LenientQuads::NTriples(parser.for_reader(reader)),
            Self::NQuads(parser) => LenientQuads::NQuads(parser.for_reader(reader)),
            Self::N3(parser) => LenientQuads::N3(parser.for_reader(reader)),
            Self::Generic(parser) => LenientQuads::Generic(parser.for_reader(reader)),
        }
    }
}

/// The quads of one input, dispatched statically per format: a `match` per
/// quad rather than a virtual call, on the hottest loop in the program.
enum LenientQuads<R: Read> {
    Turtle(oxttl::turtle::ReaderTurtleParser<R>),
    TriG(oxttl::trig::ReaderTriGParser<R>),
    NTriples(oxttl::ntriples::ReaderNTriplesParser<R>),
    NQuads(oxttl::nquads::ReaderNQuadsParser<R>),
    N3(oxttl::n3::ReaderN3Parser<R>),
    Generic(oxrdfio::ReaderQuadParser<R>),
}

impl<R: Read> Iterator for LenientQuads<R> {
    type Item = ParsedQuad;

    fn next(&mut self) -> Option<ParsedQuad> {
        Some(match self {
            Self::Turtle(parser) => parser
                .next()?
                .map(|triple| triple.in_graph(GraphName::DefaultGraph))
                .map_err(ParseError::Turtle),
            Self::TriG(parser) => parser.next()?.map_err(ParseError::Turtle),
            Self::NTriples(parser) => parser
                .next()?
                .map(|triple| triple.in_graph(GraphName::DefaultGraph))
                .map_err(ParseError::Turtle),
            Self::NQuads(parser) => parser.next()?.map_err(ParseError::Turtle),
            Self::N3(parser) => parser
                .next()?
                .map_err(ParseError::Turtle)
                .and_then(n3_to_quad),
            Self::Generic(parser) => parser.next()?.map_err(ParseError::Rdf),
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn consume_parsed_chunk<F>(
    input: &RdfInput,
    parsed: ChunkParsed,
    pending: &mut BTreeMap<u64, ChunkParsed>,
    next_sequence: &mut u64,
    stats: &mut ParseStats,
    logged_errors: &mut u64,
    suppression_logged: &mut bool,
    callback: &mut F,
) -> Result<()>
where
    F: FnMut(ExtractedQuad) -> Result<()>,
{
    pending.insert(parsed.sequence, parsed);

    while let Some(chunk) = pending.remove(next_sequence) {
        if let Some(message) = chunk.fatal {
            return Err(anyhow::anyhow!(message));
        }
        for msg in &chunk.error_samples {
            if *logged_errors < 10 {
                tracing::warn!(
                    "Skipping malformed input in {}: {}",
                    input.path.display(),
                    msg
                );
                *logged_errors += 1;
            } else if !*suppression_logged {
                tracing::warn!(
                    "Further parse errors in {} will be suppressed",
                    input.path.display()
                );
                *suppression_logged = true;
                break;
            }
        }

        for quad in chunk.quads {
            callback(quad)?;
        }
        stats.quads += chunk.stats.quads;
        stats.errors += chunk.stats.errors;
        stats.original_ntriples_size += chunk.stats.original_ntriples_size;
        *next_sequence += 1;
    }

    Ok(())
}

fn stream_quads_parallel_ntnq<F>(
    input: &RdfInput,
    file_index: usize,
    disambiguate_blank_nodes: bool,
    base_uri: Option<&str>,
    options: &ParseOptions,
    mut callback: F,
) -> Result<ParseStats>
where
    F: FnMut(ExtractedQuad) -> Result<()>,
{
    let blank_prefix = if disambiguate_blank_nodes {
        format!("f{file_index}_")
    } else {
        String::new()
    };

    let chunk_workers = options.chunk_workers.max(1);
    let task_capacity = (options.max_inflight_bytes / options.chunk_size_bytes.max(1))
        .max(chunk_workers)
        .max(1);
    let result_capacity = (chunk_workers * 2).max(2);
    let (task_tx, task_rx) = crossbeam_channel::bounded::<ChunkTask>(task_capacity);
    let (result_tx, result_rx) = crossbeam_channel::bounded::<ChunkParsed>(result_capacity);
    let budget = Arc::new(InflightBudget::new(options.max_inflight_bytes));
    // Raised by the worker that hits a fatal chunk. The others still answer
    // every queued task, so the producer's sequence numbering stays complete,
    // but answer it empty rather than parse a build that is already lost.
    let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut worker_handles = Vec::with_capacity(chunk_workers);
    for _ in 0..chunk_workers {
        let task_rx = task_rx.clone();
        let result_tx = result_tx.clone();
        let budget = Arc::clone(&budget);
        let aborted = Arc::clone(&aborted);
        let base_uri = base_uri.map(ToOwned::to_owned);
        let blank_prefix = blank_prefix.clone();
        let format = input.format;
        let max_term_bytes = options.max_term_bytes;
        let path = input.path.clone();

        worker_handles.push(std::thread::spawn(move || -> Result<()> {
            for task in task_rx {
                let chunk_len = task.bytes.len();
                // One interner per bounded parser chunk shares common graph names
                // without retaining an unbounded file-wide vocabulary.
                let mut graph_interner = GraphInterner::default();
                let mut parsed = ChunkParsed {
                    sequence: task.sequence,
                    quads: Vec::new(),
                    stats: ParseStats::default(),
                    error_samples: Vec::new(),
                    fatal: None,
                };
                if aborted.load(std::sync::atomic::Ordering::Relaxed) {
                    budget.release(chunk_len);
                    if result_tx.send(parsed).is_err() {
                        return Ok(());
                    }
                    continue;
                }
                // The chunk is a slice in memory, and reading a slice cannot
                // fail, so the one I/O error this lexer can raise is its own
                // term bound: there is no interrupted read to retry here.
                let parser = FormatParser::new(
                    format,
                    base_uri.as_deref(),
                    max_term_bytes,
                    Strictness::Lenient,
                );
                for result in parser.for_reader(task.bytes.as_slice()) {
                    match result {
                        Ok(quad) => {
                            let original_size = calculate_original_ntriples_size(
                                &quad.subject,
                                &quad.predicate,
                                &quad.object,
                                &quad.graph_name,
                            );
                            parsed.stats.original_ntriples_size += original_size;

                            let subject =
                                term_to_hdt_string(&Term::from(quad.subject), &blank_prefix);
                            let predicate = quad.predicate.as_str().to_string();
                            let object = term_to_hdt_string(&quad.object, &blank_prefix);
                            let graph = extract_graph_name(&quad.graph_name, &blank_prefix)
                                .map(|graph| graph_interner.intern(graph.as_ref()));

                            parsed.stats.quads += 1;
                            parsed.quads.push(ExtractedQuad {
                                subject,
                                predicate,
                                object,
                                graph,
                            });
                        }
                        Err(e) => {
                            if e.is_fatal() {
                                aborted.store(true, std::sync::atomic::Ordering::Relaxed);
                                parsed.fatal =
                                    Some(format!("{:#}", e.into_fatal(&path, max_term_bytes)));
                                break;
                            }
                            parsed.stats.errors += 1;
                            if parsed.error_samples.len() < 12 {
                                parsed.error_samples.push(e.to_string());
                            }
                        }
                    }
                }

                budget.release(chunk_len);
                if result_tx.send(parsed).is_err() {
                    return Ok(());
                }
            }
            Ok(())
        }));
    }
    drop(result_tx);

    let mut pending = BTreeMap::<u64, ChunkParsed>::new();
    let mut next_sequence: u64 = 0;
    let mut stats = ParseStats::default();
    let mut logged_errors = 0u64;
    let mut suppression_logged = false;

    let mut reader = open_input(input)?;
    let mut task_count: u64 = 0;
    let produce_result = read_newline_chunks(
        reader.as_mut(),
        &input.path,
        options.chunk_size_bytes.max(1),
        options.max_term_bytes,
        |chunk_bytes| {
            budget.acquire(chunk_bytes.len());
            let task = ChunkTask {
                sequence: task_count,
                bytes: chunk_bytes,
            };
            task_count += 1;

            let mut pending_task = Some(task);
            while let Some(task) = pending_task {
                match task_tx.try_send(task) {
                    Ok(()) => pending_task = None,
                    Err(TrySendError::Full(task)) => {
                        pending_task = Some(task);
                        let parsed = result_rx.recv().map_err(|_| {
                            anyhow::anyhow!("Chunk parser result channel disconnected")
                        })?;
                        consume_parsed_chunk(
                            input,
                            parsed,
                            &mut pending,
                            &mut next_sequence,
                            &mut stats,
                            &mut logged_errors,
                            &mut suppression_logged,
                            &mut callback,
                        )?;
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        return Err(anyhow::anyhow!("Chunk parser workers disconnected"));
                    }
                }
            }

            while let Ok(parsed) = result_rx.try_recv() {
                consume_parsed_chunk(
                    input,
                    parsed,
                    &mut pending,
                    &mut next_sequence,
                    &mut stats,
                    &mut logged_errors,
                    &mut suppression_logged,
                    &mut callback,
                )?;
            }

            Ok(())
        },
    );
    drop(task_tx);

    if let Err(e) = produce_result {
        // Workers still hold queued tasks and send each result into a bounded
        // channel nobody reads any more. Dropping the receiver makes those
        // sends fail, and a failed send is how a worker learns to stop; joining
        // before that would wait forever on the first worker to fill the
        // channel.
        drop(result_rx);
        for handle in worker_handles {
            let _ = handle.join();
        }
        return Err(e);
    }

    let drained = (|| -> Result<()> {
        while next_sequence < task_count {
            let parsed = result_rx
                .recv()
                .map_err(|_| anyhow::anyhow!("Chunk parser result channel disconnected"))?;
            consume_parsed_chunk(
                input,
                parsed,
                &mut pending,
                &mut next_sequence,
                &mut stats,
                &mut logged_errors,
                &mut suppression_logged,
                &mut callback,
            )?;
        }
        Ok(())
    })();

    // Same discipline as the producer's error path, whichever way the drain
    // ended: the receiver goes first, so a worker still holding a task cannot
    // block on a send nobody reads, and every worker is joined so a panic or
    // an error in one is reported rather than detached.
    drop(result_rx);
    let mut worker_failure = None;
    for handle in worker_handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                worker_failure.get_or_insert(e);
            }
            Err(_) => {
                worker_failure
                    .get_or_insert(anyhow::anyhow!("Chunk parser worker thread panicked"));
            }
        }
    }
    drained?;
    if let Some(e) = worker_failure {
        return Err(e);
    }

    if stats.errors > 0 {
        tracing::warn!(
            "{}: parsed {} quads, skipped {} errors",
            input.path.display(),
            stats.quads,
            stats.errors
        );
    }

    Ok(stats)
}

/// The longest line the chunker will buffer for a given term bound: an
/// N-Triples or N-Quads line holds at most four terms, so a longer line
/// certainly contains a term past the bound and is refused before it is
/// buffered whole.
fn max_line_bytes(max_term_bytes: usize) -> usize {
    max_term_bytes.saturating_mul(4).saturating_add(TERM_SLACK)
}

fn read_newline_chunks<F>(
    reader: &mut dyn Read,
    path: &Path,
    target_chunk_bytes: usize,
    max_term_bytes: usize,
    mut emit_chunk: F,
) -> Result<()>
where
    F: FnMut(Vec<u8>) -> Result<()>,
{
    let mut read_buffer = vec![0u8; 1024 * 1024];
    let mut pending = Vec::<u8>::with_capacity(target_chunk_bytes.max(1024 * 1024));
    let line_cap = max_line_bytes(max_term_bytes);
    // Where the line under way starts in `pending`, once it has been looked
    // for. It is found by scanning back from the target, and it holds until
    // the next split: a later read appends, and changes nothing before the
    // target.
    let mut line_start: Option<usize> = None;
    let refuse_line = || {
        anyhow::anyhow!(
            "{}: a line is longer than {line_cap} bytes, so one of its terms is larger than --max-term-bytes ({max_term_bytes} bytes); raise it",
            path.display()
        )
    };

    loop {
        let bytes_read = read_retrying_interrupted(reader, &mut read_buffer).map_err(|e| {
            let what = read_failure(&e);
            anyhow::Error::new(e).context(format!("{}: {what}", path.display()))
        })?;
        if bytes_read == 0 {
            break;
        }
        pending.extend_from_slice(&read_buffer[..bytes_read]);

        while pending.len() >= target_chunk_bytes {
            match pending[target_chunk_bytes..]
                .iter()
                .position(|&b| b == b'\n')
            {
                Some(offset) => {
                    let split_point = target_chunk_bytes + offset + 1;
                    let tail = pending.split_off(split_point);
                    let chunk = std::mem::replace(&mut pending, tail);
                    line_start = None;
                    emit_chunk(chunk)?;
                }
                None => {
                    // The line under way is what has arrived since the last
                    // newline: `pending` also holds the complete lines before
                    // it, up to a chunk's worth, which must not count against
                    // the line.
                    let start = *line_start
                        .get_or_insert_with(|| line_start_in(&pending[..target_chunk_bytes]));
                    if pending.len() - start > line_cap {
                        return Err(refuse_line());
                    }
                    break;
                }
            }
        }
    }

    if !pending.is_empty() {
        // The last line may end without a newline; it gets the same check
        // before it is handed on whole.
        let start = line_start.unwrap_or_else(|| line_start_in(&pending));
        if pending.len() - start > line_cap {
            return Err(refuse_line());
        }
        emit_chunk(pending)?;
    }

    Ok(())
}

/// Where the last line in `bytes` starts: just past its last newline, or at
/// the beginning when there is none.
fn line_start_in(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |newline| newline + 1)
}

/// Convert an oxrdf Term to its HDT dictionary string form.
///
/// IRIs are stored without angle brackets; blank nodes keep their `_:` prefix
/// after input scoping; literals follow [`encode_literal`].
fn term_to_hdt_string(term: &Term, blank_prefix: &str) -> String {
    match term {
        Term::BlankNode(b) => format!("_:{}{}", blank_prefix, b.as_str()),
        Term::NamedNode(n) => n.as_str().to_string(),
        Term::Literal(l) => literal_to_hdt_string(l),
    }
}

/// Convert a literal to its HDT dictionary string form.
///
/// The convention itself — angle brackets around a datatype IRI, `xsd:string`
/// dropped — belongs to [`encode_literal`], which downstream readers share
/// through [`crate::format`]; this function only adapts oxrdf's literal to it.
fn literal_to_hdt_string(l: &Literal) -> String {
    encode_literal(l.value(), l.language(), Some(l.datatype().as_str()))
}

/// Calculate the N-Triples serialization size of a literal without allocating.
fn literal_ntriples_size(l: &Literal) -> u64 {
    let value_len = l.value().len() as u64;
    if let Some(lang) = l.language() {
        // "value"@lang
        2 + value_len + 1 + lang.len() as u64 // 2 quotes + value + @ + language
    } else {
        let dt = l.datatype().as_str();
        if dt == "http://www.w3.org/2001/XMLSchema#string" {
            // "value"
            2 + value_len // 2 quotes
        } else {
            // "value"^^<type>
            2 + value_len + 4 + dt.len() as u64 // 2 quotes + ^^ + < + type + >
        }
    }
}

/// Calculate N-Triples serialization size for a quad's terms (before blank node prefixing).
///
/// This calculates the size as if the original RDF file were serialized to N-Triples/N-Quads,
/// WITHOUT the internal blank node disambiguation prefix that we add during parsing.
fn calculate_original_ntriples_size(
    subject: &oxrdf::NamedOrBlankNode,
    predicate: &oxrdf::NamedNode,
    object: &Term,
    graph: &GraphName,
) -> u64 {
    let mut size = 0u64;

    // Subject: URI or blank node (from oxrdf::NamedOrBlankNode)
    match subject {
        oxrdf::NamedOrBlankNode::BlankNode(b) => {
            // Blank nodes are serialized as "_:name" with no extra brackets
            size += 2 + b.as_str().len() as u64; // _: + name
        }
        oxrdf::NamedOrBlankNode::NamedNode(n) => {
            // URIs are serialized as <uri>
            size += 2 + n.as_str().len() as u64; // < + uri + >
        }
    }

    // Space
    size += 1;

    // Predicate: always a URI
    size += 2 + predicate.as_str().len() as u64; // < + uri + >

    // Space
    size += 1;

    // Object: URI, blank node, or literal
    match object {
        Term::BlankNode(b) => {
            // Blank nodes: _:name
            size += 2 + b.as_str().len() as u64;
        }
        Term::NamedNode(n) => {
            // URIs: <uri>
            size += 2 + n.as_str().len() as u64;
        }
        Term::Literal(l) => {
            // Literals: "value" or "value"@lang or "value"^^<type>
            size += literal_ntriples_size(l);
        }
    }

    // Graph (if present, not default)
    match graph {
        GraphName::DefaultGraph => {
            // Default graph: no graph suffix in serialization
        }
        GraphName::NamedNode(n) => {
            // Named graph: space + <uri>
            size += 1 + 2 + n.as_str().len() as u64;
        }
        GraphName::BlankNode(b) => {
            // Graph blank node: space + _:name
            size += 1 + 2 + b.as_str().len() as u64;
        }
    }

    // Closing: space + dot + newline = 3 bytes
    size += 3;

    size
}

/// Statistics from parsing a single input file.
#[derive(Debug, Default)]
pub struct ParseStats {
    pub quads: u64,
    pub errors: u64,
    pub original_ntriples_size: u64, // N-Triples serialization size (before blank node prefixing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression as GzipLevel, write::GzEncoder};
    use std::io::Write;

    fn make_temp_nt(content: &str) -> (tempfile::NamedTempFile, RdfInput) {
        make_temp_with(content.as_bytes(), ".nt", RdfFormat::NTriples)
    }

    /// Delivers its bytes in short reads, with one `Interrupted` error before
    /// the second read — a signal arriving mid-`read(2)`.
    struct InterruptingReader<'a> {
        data: &'a [u8],
        reads: usize,
    }

    impl Read for InterruptingReader<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.reads += 1;
            if self.reads == 2 {
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            let n = self.data.len().min(buf.len()).min(40);
            buf[..n].copy_from_slice(&self.data[..n]);
            self.data = &self.data[n..];
            Ok(n)
        }
    }

    #[test]
    fn test_bound_applies_per_term_not_per_statement() {
        // A 480 KiB IRI already consumed, then a 600 KiB literal: each is
        // under a 1 MiB bound, their sum is not. The lexer must compact the
        // consumed prefix before judging the literal too large.
        let iri = format!("http://example.org/{}", "a".repeat(480 * 1024));
        let literal = "x".repeat(600 * 1024);
        let content = format!(
            "<{iri}> <http://example.org/big> \"{literal}\" .\n\
             <http://example.org/s> <http://example.org/p> <http://example.org/o> .\n"
        );
        let (_f, input) = make_temp_with(content.as_bytes(), ".ttl", RdfFormat::Turtle);
        let options = ParseOptions {
            max_term_bytes: 1024 * 1024,
            ..ParseOptions::default()
        };
        let mut quads = Vec::new();
        let stats = stream_quads_with_options(&input, 0, false, None, &options, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();
        assert_eq!(stats.errors, 0);
        assert_eq!(quads.len(), 2);
        assert_eq!(quads[0].subject, iri);
    }

    /// Fails every read the same way, as a truncated gzip member or a bad zstd
    /// frame does.
    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::InvalidData, "bad frame"))
        }
    }

    #[test]
    fn test_persistent_read_error_ends_the_input() {
        let (_f, input) = make_temp_with(b"", ".nt", RdfFormat::NTriples);
        let error = stream_quads_from_reader(
            FailingReader,
            &input,
            0,
            false,
            None,
            DEFAULT_MAX_TERM_BYTES,
            &mut |_| Ok(()),
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("unrecoverable read error"), "{message}");
        assert!(message.contains("bad frame"), "{message}");
        assert!(
            message.contains(&input.path.display().to_string()),
            "{message}"
        );
    }

    /// Reports `Interrupted` on every read, forever.
    struct AlwaysInterruptedReader;

    impl Read for AlwaysInterruptedReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::Interrupted))
        }
    }

    #[test]
    fn test_persistent_interruption_is_capped() {
        let (_f, input) = make_temp_with(b"", ".nt", RdfFormat::NTriples);
        let error = stream_quads_from_reader(
            AlwaysInterruptedReader,
            &input,
            0,
            false,
            None,
            DEFAULT_MAX_TERM_BYTES,
            &mut |_| Ok(()),
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("interrupted"), "{message}");
        assert!(message.contains("in a row"), "{message}");
    }

    #[test]
    fn test_term_of_exactly_the_bound_is_accepted() {
        let literal = "x".repeat(1024 * 1024);
        let content = format!(
            "<http://example.org/s> <http://example.org/big> \"{literal}\" .\n\
             <http://example.org/s> <http://example.org/p> <http://example.org/o> .\n"
        );
        let (_f, input) = make_temp_with(content.as_bytes(), ".ttl", RdfFormat::Turtle);
        let options = ParseOptions {
            max_term_bytes: 1024 * 1024,
            ..ParseOptions::default()
        };
        let mut quads = Vec::new();
        let stats = stream_quads_with_options(&input, 0, false, None, &options, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();
        assert_eq!(stats.errors, 0);
        assert_eq!(quads.len(), 2);
    }

    #[test]
    fn test_over_long_line_is_refused_by_the_chunker() {
        // An N-Triples line holds at most four terms, so a line past four times
        // the bound must contain an over-bound term; the chunker refuses it
        // before buffering it whole, ahead of any parser.
        let literal = "x".repeat(5 * 1024 * 1024);
        let content = format!("<http://example.org/s> <http://example.org/big> \"{literal}\" .\n");
        let (_f, input) = make_temp_with(content.as_bytes(), ".nt", RdfFormat::NTriples);
        let options = ParseOptions {
            enable_ntnq_parallel: true,
            chunk_size_bytes: 4096,
            chunk_workers: 2,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_term_bytes: 1024 * 1024,
        };
        let error = stream_quads_with_options(&input, 0, false, None, &options, |_| Ok(()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("a line is longer than"), "{error}");
        assert!(
            error.contains("--max-term-bytes (1048576 bytes)"),
            "{error}"
        );
    }

    #[test]
    fn test_chunker_measures_the_line_not_the_buffer() {
        // The chunker keeps up to a chunk's worth of complete lines ahead of
        // the line under way. A legal line of four terms just under the bound
        // must not be refused for the company it keeps.
        let bound = 1024 * 1024;
        let term = |label: &str| {
            format!(
                "<http://example.org/{label}{}>",
                "a".repeat(bound - 64 - label.len())
            )
        };
        let long = format!(
            "{} {} \"{}\" {} .\n",
            term("s"),
            term("p"),
            "o".repeat(bound - 64),
            term("g")
        );
        let short = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n";
        let short_lines = 50 * 1024 / short.len();
        let content = format!("{}{long}", short.repeat(short_lines));
        assert!(
            content.len() > max_line_bytes(bound),
            "fixture must exceed the cap as a whole"
        );
        assert!(
            long.len() <= max_line_bytes(bound),
            "fixture line must be legal"
        );

        let (_f, input) = make_temp_with(content.as_bytes(), ".nq", RdfFormat::NQuads);
        let options = ParseOptions {
            enable_ntnq_parallel: true,
            chunk_size_bytes: 64 * 1024,
            chunk_workers: 2,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_term_bytes: bound,
        };
        let mut quads = 0usize;
        let stats = stream_quads_with_options(&input, 0, false, None, &options, |_| {
            quads += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(stats.errors, 0);
        assert_eq!(quads, short_lines + 1);
    }

    /// Collect what the chunker emits from `reader`, joined back together.
    fn chunk_through(reader: &mut dyn Read, target_chunk_bytes: usize) -> Result<Vec<u8>> {
        let mut joined = Vec::new();
        read_newline_chunks(
            reader,
            Path::new("in.nt"),
            target_chunk_bytes,
            DEFAULT_MAX_TERM_BYTES,
            |chunk| {
                joined.extend_from_slice(&chunk);
                Ok(())
            },
        )?;
        Ok(joined)
    }

    #[test]
    fn test_chunker_retries_an_interrupted_read() {
        let content = b"<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n\
                        <http://example.org/s2> <http://example.org/p> \"v\" .\n";
        let mut reader = InterruptingReader {
            data: content,
            reads: 0,
        };
        let joined = chunk_through(&mut reader, 4096).unwrap();
        assert_eq!(joined, content);
    }

    #[test]
    fn test_chunker_gives_up_on_persistent_interruption() {
        let error = format!(
            "{:#}",
            chunk_through(&mut AlwaysInterruptedReader, 4096).unwrap_err()
        );
        assert!(error.starts_with("in.nt: read interrupted"), "{error}");
    }

    #[test]
    fn test_chunker_reports_a_failed_read() {
        let error = format!("{:#}", chunk_through(&mut FailingReader, 4096).unwrap_err());
        assert!(
            error.starts_with("in.nt: unrecoverable read error"),
            "{error}"
        );
        assert!(error.contains("bad frame"), "{error}");
    }

    #[test]
    fn test_strict_parse_names_a_syntax_error_as_invalid_rdf() {
        // A header input is parsed strictly, so a syntax error ends it; it is
        // not a read error and must not be reported as one.
        let content = b"<http://example.org/s> <http://example.org/p> <http://example.org/a b> .\n";
        let (_f, input) = make_temp_with(content, ".nt", RdfFormat::NTriples);
        let error = format!(
            "{:#}",
            parse_rdf_to_triples(&input, None, "", DEFAULT_MAX_TERM_BYTES).unwrap_err()
        );
        assert!(error.contains(": invalid RDF"), "{error}");
        assert!(!error.contains("read error"), "{error}");
    }

    #[test]
    fn test_header_counts_refuse_a_count_stated_two_ways() {
        let stated = |a: u64, b: u64| {
            format!(
                "<urn:d> <http://rdfs.org/ns/void#triples> \"{a}\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n\
                 <urn:d> <http://rdfs.org/ns/void#triples> \"{b}\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n"
            )
        };
        assert_eq!(header_counts(stated(7, 7).as_bytes()).unwrap().triples, 7);
        let error = header_counts(stated(7, 8).as_bytes())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Conflicting HDT header triple counts"),
            "{error}"
        );
    }

    #[test]
    fn test_interrupted_read_is_retried_not_fatal() {
        // The retry only works because the vendored lexer discards the zero
        // padding it reserved for the failed read; the released one left it in
        // the buffer, where it would be parsed as data.
        let content = b"<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n\
                        <http://example.org/s2> <http://example.org/p> \"v\" .\n";
        let (_f, input) = make_temp_with(content, ".nt", RdfFormat::NTriples);
        let reader = InterruptingReader {
            data: content,
            reads: 0,
        };
        let mut quads = Vec::new();
        let stats = stream_quads_from_reader(
            reader,
            &input,
            0,
            false,
            None,
            DEFAULT_MAX_TERM_BYTES,
            &mut |q| {
                quads.push(q);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(stats.errors, 0);
        assert_eq!(quads.len(), 2);
        assert_eq!(quads[1].object, "\"v\"");
    }

    #[test]
    fn graph_interner_reuses_repeated_graph_storage() {
        let mut interner = GraphInterner::default();
        let first = interner.intern("http://example.org/graph");
        let second = interner.intern("http://example.org/graph");
        assert!(Arc::ptr_eq(&first, &second));
    }

    fn make_temp_nt_gz(content: &str) -> (tempfile::NamedTempFile, RdfInput) {
        let mut f = tempfile::Builder::new()
            .suffix(".nt.gz")
            .tempfile()
            .unwrap();
        {
            let mut encoder = GzEncoder::new(&mut f, GzipLevel::default());
            encoder.write_all(content.as_bytes()).unwrap();
            encoder.finish().unwrap();
        }
        f.flush().unwrap();

        let input = RdfInput {
            path: f.path().to_path_buf(),
            format: RdfFormat::NTriples,
            compression: Compression::Gzip,
        };
        (f, input)
    }

    fn make_temp_with(
        content: &[u8],
        suffix: &str,
        format: RdfFormat,
    ) -> (tempfile::NamedTempFile, RdfInput) {
        let mut f = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
        f.write_all(content).unwrap();
        f.flush().unwrap();
        let input = RdfInput {
            path: f.path().to_path_buf(),
            format,
            compression: Compression::None,
        };
        (f, input)
    }

    /// Two statements, the first carrying a literal past the 16 MiB bound the
    /// released oxttl hard-codes — the GADM boundary case in miniature.
    fn oversized_literal_document(graph_wrapped: bool) -> String {
        let big = "x".repeat(17 * 1024 * 1024);
        let body = format!(
            "<http://example.org/s> <http://example.org/big> \"{big}\" .\n\
             <http://example.org/s> <http://example.org/p> <http://example.org/o> .\n"
        );
        if graph_wrapped {
            format!("<http://example.org/g> {{\n{body}}}\n")
        } else {
            body
        }
    }

    #[test]
    fn test_oversized_literal_parses_under_default_bound() {
        let (_f, input) = make_temp_with(
            oversized_literal_document(true).as_bytes(),
            ".trig",
            RdfFormat::TriG,
        );
        let mut quads = Vec::new();
        let stats = stream_quads(&input, 0, false, None, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();

        assert_eq!(stats.errors, 0);
        assert_eq!(stats.quads, 2);
        assert!(quads[0].object.len() > 17 * 1024 * 1024);
        assert_eq!(quads[0].graph.as_deref(), Some("http://example.org/g"));
    }

    #[test]
    fn test_oversized_literal_fails_under_small_bound() {
        // The lexer cannot skip a term it cannot buffer, and the released
        // oxttl loops forever on one; hdtc must end the input with the remedy.
        let (_f, input) = make_temp_with(
            oversized_literal_document(true).as_bytes(),
            ".trig",
            RdfFormat::TriG,
        );
        let options = ParseOptions {
            max_term_bytes: 1024 * 1024,
            ..ParseOptions::default()
        };
        let error = stream_quads_with_options(&input, 0, false, None, &options, |_| Ok(()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("--max-term-bytes"), "{error}");
        assert!(error.contains("1048576"), "{error}");
    }

    #[test]
    fn test_oversized_literal_fails_under_small_bound_parallel() {
        let (_f, input) = make_temp_with(
            oversized_literal_document(false).as_bytes(),
            ".nt",
            RdfFormat::NTriples,
        );
        let options = ParseOptions {
            enable_ntnq_parallel: true,
            chunk_size_bytes: 4096,
            chunk_workers: 2,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_term_bytes: 1024 * 1024,
        };
        let error = stream_quads_with_options(&input, 0, false, None, &options, |_| Ok(()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("--max-term-bytes"), "{error}");
    }

    #[test]
    fn test_oversized_literal_parallel_ntriples() {
        let (_f, input) = make_temp_with(
            oversized_literal_document(false).as_bytes(),
            ".nt",
            RdfFormat::NTriples,
        );
        let options = ParseOptions {
            enable_ntnq_parallel: true,
            chunk_size_bytes: 4096,
            chunk_workers: 2,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_term_bytes: DEFAULT_MAX_TERM_BYTES,
        };
        let mut quads = Vec::new();
        let stats = stream_quads_with_options(&input, 0, false, None, &options, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();

        assert_eq!(stats.errors, 0);
        assert_eq!(stats.quads, 2);
        assert!(quads[0].object.len() > 17 * 1024 * 1024);
    }

    #[test]
    fn test_non_power_of_two_bound_rejects_and_accepts_correctly() {
        // Vec growth can carry the lexer buffer past a bound that is not a
        // power of two; unclamped, the overshoot is shrunk away on the next
        // read, which discards bytes and reads an empty slice as end of file.
        // The symptom was a 4.5 MiB literal under a 3M bound producing a
        // successful build that had silently dropped the following triple.
        let big = "x".repeat(9 * 512 * 1024); // 4.5 MiB
        let content = format!(
            "<http://example.org/s> <http://example.org/big> \"{big}\" .\n\
             <http://example.org/s> <http://example.org/p> <http://example.org/o> .\n"
        );
        let (_f, input) = make_temp_with(content.as_bytes(), ".ttl", RdfFormat::Turtle);

        let too_small = ParseOptions {
            max_term_bytes: 3 * 1024 * 1024,
            ..ParseOptions::default()
        };
        let error = stream_quads_with_options(&input, 0, false, None, &too_small, |_| Ok(()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("--max-term-bytes"), "{error}");

        let big_enough = ParseOptions {
            max_term_bytes: 5 * 1024 * 1024,
            ..ParseOptions::default()
        };
        let mut quads = Vec::new();
        let stats = stream_quads_with_options(&input, 0, false, None, &big_enough, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();
        assert_eq!(stats.errors, 0);
        assert_eq!(quads.len(), 2);
        assert_eq!(quads[1].predicate, "http://example.org/p");
    }

    #[test]
    fn test_fatal_chunk_error_does_not_deadlock_parallel_parser() {
        // An oversized first literal followed by enough ordinary triples to
        // fill the bounded result channel. The fatal chunk must end the parse
        // promptly; before the receiver was dropped ahead of the join, the
        // workers blocked forever on their sends.
        let mut content = oversized_literal_document(false);
        for i in 0..20_000 {
            content.push_str(&format!(
                "<http://example.org/s{i}> <http://example.org/p> <http://example.org/o> .\n"
            ));
        }
        let (_f, input) = make_temp_with(content.as_bytes(), ".nt", RdfFormat::NTriples);
        let options = ParseOptions {
            enable_ntnq_parallel: true,
            chunk_size_bytes: 4096,
            chunk_workers: 2,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_term_bytes: 1024 * 1024,
        };

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = stream_quads_with_options(&input, 0, false, None, &options, |_| Ok(()));
            let _ = done_tx.send(result.map(|_| ()).map_err(|e| e.to_string()));
        });
        let outcome = done_rx
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("parallel parse did not finish: workers deadlocked");
        let error = outcome.unwrap_err();
        assert!(error.contains("--max-term-bytes"), "{error}");
    }

    #[test]
    fn test_n3_honours_term_bound() {
        let (_f, input) = make_temp_with(
            oversized_literal_document(false).as_bytes(),
            ".n3",
            RdfFormat::N3,
        );

        let mut quads = Vec::new();
        let stats = stream_quads(&input, 0, false, None, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.quads, 2);
        assert!(quads[0].object.len() > 17 * 1024 * 1024);

        let options = ParseOptions {
            max_term_bytes: 1024 * 1024,
            ..ParseOptions::default()
        };
        let error = stream_quads_with_options(&input, 0, false, None, &options, |_| Ok(()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("--max-term-bytes"), "{error}");
    }

    #[test]
    fn test_n3_statement_that_is_not_rdf_is_skipped() {
        let content = "@prefix ex: <http://example.org/> .\n?x ex:p ex:o .\nex:s ex:p ex:o .\n";
        let (_f, input) = make_temp_with(content.as_bytes(), ".n3", RdfFormat::N3);
        let mut quads = Vec::new();
        let stats = stream_quads(&input, 0, false, None, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();
        assert_eq!(stats.errors, 1);
        assert_eq!(quads.len(), 1);
        assert_eq!(quads[0].subject, "http://example.org/s");
    }

    #[test]
    fn test_parse_ntriples() {
        let content = r#"<http://example.org/s> <http://example.org/p> <http://example.org/o> .
<http://example.org/s> <http://example.org/p> "hello" .
"#;
        let (_f, input) = make_temp_nt(content);
        let mut quads = Vec::new();
        let stats = stream_quads(&input, 0, true, None, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();

        assert_eq!(stats.quads, 2);
        assert_eq!(stats.errors, 0);
        assert_eq!(quads[0].subject, "http://example.org/s");
        assert_eq!(quads[0].predicate, "http://example.org/p");
        assert_eq!(quads[0].object, "http://example.org/o");
        assert!(quads[0].graph.is_none());
        assert_eq!(quads[1].object, "\"hello\"");
    }

    #[test]
    fn test_blank_node_disambiguation() {
        let content = "_:b1 <http://example.org/p> _:b2 .\n";
        let (_f, input) = make_temp_nt(content);
        let mut quads = Vec::new();
        stream_quads(&input, 5, true, None, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();

        assert_eq!(quads.len(), 1);
        assert!(quads[0].subject.starts_with("_:f5_"));
        assert!(quads[0].object.starts_with("_:f5_"));
    }

    #[test]
    fn test_malformed_input_skipped() {
        let content = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\nthis is not valid RDF\n<http://example.org/s2> <http://example.org/p2> <http://example.org/o2> .\n";
        let (_f, input) = make_temp_nt(content);
        let mut quads = Vec::new();
        let stats = stream_quads(&input, 0, true, None, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();

        assert_eq!(stats.quads, 2);
        assert!(stats.errors > 0);
    }

    #[test]
    fn test_original_ntriples_size() {
        // Test data matching representative.nt structure
        let content = r#"<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/name> "Bob" .
<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> .
<http://example.org/alice> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/alice> <http://example.org/label> "Alice"@en .
<http://example.org/alice> <http://example.org/label> "Alicia"@es .
_:b1 <http://example.org/type> <http://example.org/Thing> .
"#;
        let (_f, input) = make_temp_nt(content);
        let stats = stream_quads(&input, 0, true, None, |_q| Ok(())).unwrap();

        assert_eq!(stats.quads, 8);
        assert_eq!(stats.errors, 0);
        // Verify the original N-Triples size matches the content
        assert_eq!(stats.original_ntriples_size, content.len() as u64);
    }

    #[test]
    fn test_original_ntriples_size_simple() {
        // Test with simple.nt structure (URIs only, no literals with decorators)
        let content = r#"<http://example.org/subject1> <http://example.org/predicate1> <http://example.org/object1> .
<http://example.org/subject1> <http://example.org/predicate1> <http://example.org/object2> .
<http://example.org/subject2> <http://example.org/predicate2> <http://example.org/object1> .
<http://example.org/subject2> <http://example.org/predicate2> <http://example.org/object3> .
<http://example.org/subject3> <http://example.org/predicate1> <http://example.org/object3> .
"#;
        let (_f, input) = make_temp_nt(content);
        let stats = stream_quads(&input, 0, true, None, |_q| Ok(())).unwrap();

        assert_eq!(stats.quads, 5);
        assert_eq!(stats.errors, 0);
        // Verify the original N-Triples size matches the content
        assert_eq!(stats.original_ntriples_size, content.len() as u64);
    }

    #[test]
    fn test_original_ntriples_size_with_blank_nodes() {
        // Test that blank node size is calculated WITHOUT the file prefix
        let content = "_:b1 <http://example.org/p> <http://example.org/o> .\n";
        let (_f, input) = make_temp_nt(content);
        let stats = stream_quads(&input, 0, true, None, |_q| Ok(())).unwrap();

        assert_eq!(stats.quads, 1);
        assert_eq!(stats.errors, 0);
        // Size should be: _:b1 (4) + space (1) + <http://example.org/p> (22) + space (1) +
        //                <http://example.org/o> (22) + space (1) + . (1) + \n (1) = 53 bytes
        assert_eq!(stats.original_ntriples_size, 53);
        // The content length should match since it's a single newline-terminated line
        assert_eq!(stats.original_ntriples_size, content.len() as u64);
    }

    #[test]
    fn test_parallel_chunk_parsing_plain_nt() {
        let mut content = String::new();
        for i in 0..200 {
            content.push_str(&format!(
                "<http://example.org/s{0}> <http://example.org/p> <http://example.org/o{0}> .\n",
                i
            ));
        }

        let (_f, input) = make_temp_nt(&content);

        let mut seq_quads = Vec::new();
        let seq_stats = stream_quads_with_options(
            &input,
            0,
            true,
            None,
            &ParseOptions {
                enable_ntnq_parallel: false,
                chunk_size_bytes: 128,
                chunk_workers: 1,
                max_inflight_bytes: 1024,
                max_term_bytes: DEFAULT_MAX_TERM_BYTES,
            },
            |q| {
                seq_quads.push(q);
                Ok(())
            },
        )
        .unwrap();

        let mut par_quads = Vec::new();
        let par_stats = stream_quads_with_options(
            &input,
            0,
            true,
            None,
            &ParseOptions {
                enable_ntnq_parallel: true,
                chunk_size_bytes: 128,
                chunk_workers: 4,
                max_inflight_bytes: 8 * 1024,
                max_term_bytes: DEFAULT_MAX_TERM_BYTES,
            },
            |q| {
                par_quads.push(q);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(seq_stats.quads, par_stats.quads);
        assert_eq!(seq_stats.errors, par_stats.errors);
        assert_eq!(
            seq_stats.original_ntriples_size,
            par_stats.original_ntriples_size
        );
        assert_eq!(seq_quads, par_quads);
    }

    #[test]
    fn test_parallel_chunk_parsing_gzip_nt() {
        let mut content = String::new();
        for i in 0..120 {
            content.push_str(&format!(
                "<http://example.org/sg{0}> <http://example.org/p> <http://example.org/og{0}> .\n",
                i
            ));
        }

        let (_f, input) = make_temp_nt_gz(&content);

        let mut quads = Vec::new();
        let stats = stream_quads_with_options(
            &input,
            3,
            true,
            None,
            &ParseOptions {
                enable_ntnq_parallel: true,
                chunk_size_bytes: 96,
                chunk_workers: 3,
                max_inflight_bytes: 4 * 1024,
                max_term_bytes: DEFAULT_MAX_TERM_BYTES,
            },
            |q| {
                quads.push(q);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(stats.quads, 120);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.original_ntriples_size, content.len() as u64);
        assert_eq!(quads.len(), 120);
    }
}
