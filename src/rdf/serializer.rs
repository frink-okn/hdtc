//! N-Triples serialization of oxrdf triples (wraps oxrdfio).
//!
//! The mirror of [`crate::rdf::parser`]: where the parser turns RDF input into
//! triples, this turns triples back into N-Triples text. Used to build the
//! N-Triples block embedded in an HDT header (both when writing a fresh HDT and
//! when rewriting the header via the `header` command).

use anyhow::{Context, Result};
use oxrdf::Triple;

/// Serialize triples to an N-Triples string using oxrdfio's serializer.
pub(crate) fn serialize_triples(triples: &[Triple]) -> Result<String> {
    let mut buf = Vec::new();
    let mut serializer =
        oxrdfio::RdfSerializer::from_format(oxrdfio::RdfFormat::NTriples).for_writer(&mut buf);
    for triple in triples {
        serializer
            .serialize_triple(triple)
            .context("Failed to serialize triple")?;
    }
    serializer
        .finish()
        .context("Failed to finish N-Triples serialization")?;
    String::from_utf8(buf).context("Serialized N-Triples is not valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{Literal, NamedNode, Term};

    fn iri(s: &str) -> Term {
        Term::NamedNode(NamedNode::new_unchecked(s))
    }

    fn nt(s: &str, p: &str, o: Term) -> Triple {
        Triple::new(NamedNode::new_unchecked(s), NamedNode::new_unchecked(p), o)
    }

    /// Re-parse N-Triples text back into triples (test helper).
    fn parse(text: &str) -> Vec<Triple> {
        oxrdfio::RdfParser::from_format(oxrdfio::RdfFormat::NTriples)
            .for_reader(text.as_bytes())
            .map(|q| {
                let q = q.unwrap();
                Triple::new(q.subject, q.predicate, q.object)
            })
            .collect()
    }

    #[test]
    fn test_serialize_roundtrip() {
        let triples = vec![
            nt("http://example.org/s", "http://example.org/p", iri("http://example.org/o")),
            nt(
                "http://example.org/s",
                "http://example.org/label",
                Term::Literal(Literal::new_simple_literal("hi")),
            ),
        ];
        let text = serialize_triples(&triples).unwrap();
        assert!(text.contains("<http://example.org/s> <http://example.org/p> <http://example.org/o> ."));
        assert!(text.contains("\"hi\""));
        assert_eq!(parse(&text), triples);
    }

    #[test]
    fn test_serialize_escapes_literals() {
        // A raw quote/newline in the value must be escaped to stay valid N-Triples.
        let triples = vec![nt(
            "http://example.org/s",
            "http://example.org/p",
            Term::Literal(Literal::new_simple_literal("a \"quote\"\nand newline")),
        )];
        let text = serialize_triples(&triples).unwrap();
        assert!(text.contains("\\\""));
        assert!(text.contains("\\n"));
        // Round-trips back to the same triple.
        assert_eq!(parse(&text), triples);
    }
}
