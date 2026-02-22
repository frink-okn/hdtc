//! Streaming RDF parser wrapping oxrdfio with compression and blank node disambiguation.

use crate::rdf::input::{Compression, RdfFormat, RdfInput};
use anyhow::{Context, Result};
use crossbeam_channel::TrySendError;
use oxrdf::{GraphName, Literal, Term};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::{Arc, Condvar, Mutex};

/// Parser parallelism controls.
#[derive(Debug, Clone)]
pub struct ParseOptions {
    pub enable_ntnq_parallel: bool,
    pub chunk_size_bytes: usize,
    pub chunk_workers: usize,
    pub max_inflight_bytes: usize,
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
    pub graph: Option<String>,
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
        Compression::Gzip => Box::new(flate2::read::GzDecoder::new(buf_reader)),
        Compression::Bzip2 => Box::new(bzip2::read::BzDecoder::new(buf_reader)),
        Compression::Xz => Box::new(xz2::read::XzDecoder::new(buf_reader)),
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
        &mut callback,
    )
}

fn stream_quads_sequential<F>(
    input: &RdfInput,
    file_index: usize,
    disambiguate_blank_nodes: bool,
    base_uri: Option<&str>,
    callback: &mut F,
) -> Result<ParseStats>
where
    F: FnMut(ExtractedQuad) -> Result<()>,
{
    let reader = open_input(input)?;
    let format = to_oxrdf_format(input.format);
    let parser = make_lenient_parser(format, base_uri);

    let blank_prefix = if disambiguate_blank_nodes {
        format!("f{file_index}_")
    } else {
        String::new()
    };
    let mut stats = ParseStats::default();

    for result in parser.for_reader(reader) {
        match result {
            Ok(quad) => {
                // Calculate original N-Triples size BEFORE adding blank node prefix
                let original_size = calculate_original_ntriples_size(
                    &quad.subject,
                    &quad.predicate,
                    &quad.object,
                    &quad.graph_name,
                );
                stats.original_ntriples_size += original_size;

                let subject = term_to_hdt_string(
                    &Term::from(quad.subject),
                    &blank_prefix,
                );
                let predicate = quad.predicate.as_str().to_string();
                let object = term_to_hdt_string(&quad.object, &blank_prefix);
                let graph = match &quad.graph_name {
                    GraphName::DefaultGraph => None,
                    GraphName::NamedNode(n) => Some(n.as_str().to_string()),
                    GraphName::BlankNode(b) => {
                        Some(format!("_:{}{}", blank_prefix, b.as_str()))
                    }
                };

                stats.quads += 1;
                callback(ExtractedQuad {
                    subject,
                    predicate,
                    object,
                    graph,
                })?;
            }
            Err(e) => {
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

fn make_lenient_parser(format: oxrdfio::RdfFormat, base_uri: Option<&str>) -> oxrdfio::RdfParser {
    let mut parser = oxrdfio::RdfParser::from_format(format).lenient();
    if let Some(base) = base_uri
        && let Ok(p) = parser.clone().with_base_iri(base)
    {
        parser = p;
    }
    parser
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

    let mut worker_handles = Vec::with_capacity(chunk_workers);
    for _ in 0..chunk_workers {
        let task_rx = task_rx.clone();
        let result_tx = result_tx.clone();
        let budget = Arc::clone(&budget);
        let base_uri = base_uri.map(ToOwned::to_owned);
        let blank_prefix = blank_prefix.clone();
        let format = to_oxrdf_format(input.format);

        worker_handles.push(std::thread::spawn(move || -> Result<()> {
            for task in task_rx {
                let chunk_len = task.bytes.len();
                let mut parsed = ChunkParsed {
                    sequence: task.sequence,
                    quads: Vec::new(),
                    stats: ParseStats::default(),
                    error_samples: Vec::new(),
                };

                let parser = make_lenient_parser(format, base_uri.as_deref());
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
                            let graph = match &quad.graph_name {
                                GraphName::DefaultGraph => None,
                                GraphName::NamedNode(n) => Some(n.as_str().to_string()),
                                GraphName::BlankNode(b) => {
                                    Some(format!("_:{}{}", blank_prefix, b.as_str()))
                                }
                            };

                            parsed.stats.quads += 1;
                            parsed.quads.push(ExtractedQuad {
                                subject,
                                predicate,
                                object,
                                graph,
                            });
                        }
                        Err(e) => {
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
        options.chunk_size_bytes.max(1),
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
        for handle in worker_handles {
            let _ = handle.join();
        }
        return Err(e);
    }

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

    for handle in worker_handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(anyhow::anyhow!("Chunk parser worker thread panicked")),
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

fn read_newline_chunks<F>(
    reader: &mut dyn Read,
    target_chunk_bytes: usize,
    mut emit_chunk: F,
) -> Result<()>
where
    F: FnMut(Vec<u8>) -> Result<()>,
{
    let mut read_buffer = vec![0u8; 1024 * 1024];
    let mut pending = Vec::<u8>::with_capacity(target_chunk_bytes.max(1024 * 1024));

    loop {
        let bytes_read = reader.read(&mut read_buffer)?;
        if bytes_read == 0 {
            break;
        }
        pending.extend_from_slice(&read_buffer[..bytes_read]);

        while pending.len() >= target_chunk_bytes {
            let split_point = match pending[target_chunk_bytes..]
                .iter()
                .position(|&b| b == b'\n')
            {
                Some(offset) => target_chunk_bytes + offset + 1,
                None => break,
            };

            let tail = pending.split_off(split_point);
            let chunk = std::mem::replace(&mut pending, tail);
            emit_chunk(chunk)?;
        }
    }

    if !pending.is_empty() {
        emit_chunk(pending)?;
    }

    Ok(())
}

/// Convert an oxrdf Term to its HDT dictionary string form.
///
/// URIs are stored without angle brackets. Literals use the HDT convention:
/// bare datatype URIs without angle brackets in `^^type` annotations.
fn term_to_hdt_string(term: &Term, blank_prefix: &str) -> String {
    match term {
        Term::BlankNode(b) => format!("_:{}{}", blank_prefix, b.as_str()),
        Term::NamedNode(n) => n.as_str().to_string(),
        Term::Literal(l) => literal_to_hdt_string(l),
    }
}

/// Convert a literal to its HDT dictionary string form.
///
/// HDT stores typed literals with angle brackets around the datatype URI:
/// `"value"^^<http://www.w3.org/2001/XMLSchema#integer>`
fn literal_to_hdt_string(l: &Literal) -> String {
    if let Some(lang) = l.language() {
        format!("\"{}\"@{}", l.value(), lang)
    } else {
        let dt = l.datatype().as_str();
        if dt == "http://www.w3.org/2001/XMLSchema#string" {
            format!("\"{}\"", l.value())
        } else {
            format!("\"{}\"^^<{}>", l.value(), dt)
        }
    }
}

/// Calculate the N-Triples serialization size of a literal without allocating.
fn literal_ntriples_size(l: &Literal) -> u64 {
    let value_len = l.value().len() as u64;
    if let Some(lang) = l.language() {
        // "value"@lang
        2 + value_len + 1 + lang.len() as u64  // 2 quotes + value + @ + language
    } else {
        let dt = l.datatype().as_str();
        if dt == "http://www.w3.org/2001/XMLSchema#string" {
            // "value"
            2 + value_len  // 2 quotes
        } else {
            // "value"^^<type>
            2 + value_len + 4 + dt.len() as u64  // 2 quotes + ^^ + < + type + >
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
        let mut f = tempfile::Builder::new().suffix(".nt").tempfile().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        let input = RdfInput {
            path: f.path().to_path_buf(),
            format: RdfFormat::NTriples,
            compression: Compression::None,
        };
        (f, input)
    }

    fn make_temp_nt_gz(content: &str) -> (tempfile::NamedTempFile, RdfInput) {
        let mut f = tempfile::Builder::new().suffix(".nt.gz").tempfile().unwrap();
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
        let stats = stream_quads(&input, 0, true, None, |_q| Ok(()))
            .unwrap();

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
        let stats = stream_quads(&input, 0, true, None, |_q| Ok(()))
            .unwrap();

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
        let stats = stream_quads(&input, 0, true, None, |_q| Ok(()))
            .unwrap();

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
            },
            |q| {
                par_quads.push(q);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(seq_stats.quads, par_stats.quads);
        assert_eq!(seq_stats.errors, par_stats.errors);
        assert_eq!(seq_stats.original_ntriples_size, par_stats.original_ntriples_size);
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
