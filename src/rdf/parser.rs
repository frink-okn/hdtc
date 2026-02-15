//! Streaming RDF parser wrapping oxrdfio with compression and blank node disambiguation.

use crate::rdf::input::{Compression, RdfFormat, RdfInput};
use anyhow::{Context, Result};
use oxrdf::{GraphName, Literal, Term};
use std::fs::File;
use std::io::{BufReader, Read};

/// A canonical RDF term string and its role in a statement.
#[derive(Debug, Clone)]
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
/// Blank nodes are disambiguated by prefixing with `f{file_index}_`.
/// Malformed input is skipped with a warning; the total skip count is returned.
pub fn stream_quads<F>(
    input: &RdfInput,
    file_index: usize,
    base_uri: Option<&str>,
    mut callback: F,
) -> Result<ParseStats>
where
    F: FnMut(ExtractedQuad) -> Result<()>,
{
    let reader = open_input(input)?;
    let format = to_oxrdf_format(input.format);

    let mut parser = oxrdfio::RdfParser::from_format(format).lenient();
    if let Some(base) = base_uri {
        if let Ok(p) = parser.clone().with_base_iri(base) {
            parser = p;
        }
    }

    let blank_prefix = format!("f{file_index}_");
    let mut stats = ParseStats::default();

    for result in parser.for_reader(reader) {
        match result {
            Ok(quad) => {
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

/// Statistics from parsing a single input file.
#[derive(Debug, Default)]
pub struct ParseStats {
    pub quads: u64,
    pub errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn test_parse_ntriples() {
        let content = r#"<http://example.org/s> <http://example.org/p> <http://example.org/o> .
<http://example.org/s> <http://example.org/p> "hello" .
"#;
        let (_f, input) = make_temp_nt(content);
        let mut quads = Vec::new();
        let stats = stream_quads(&input, 0, None, |q| {
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
        stream_quads(&input, 5, None, |q| {
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
        let stats = stream_quads(&input, 0, None, |q| {
            quads.push(q);
            Ok(())
        })
        .unwrap();

        assert_eq!(stats.quads, 2);
        assert!(stats.errors > 0);
    }
}
