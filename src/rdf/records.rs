//! hdtc's own record of dictionary terms: one triple per line, each term as
//! the dictionary spells it.
//!
//! The propositions HDT of RDF 1.2 triple terms is built by the ordinary
//! pipeline from terms the build already holds — read from the parsed input,
//! or from HDT inputs. Lenient ingest stores strings no RDF syntax can spell
//! in every case: a blank-node label such as `x.`, a language tag such as
//! `en_US`. Writing those as N-Triples and parsing them back would refuse or
//! change them, and the propositions HDT must hold exactly the strings
//! `data.hdt` does, since a blank node there is the data's node with that
//! label. These records carry the strings themselves, so nothing is spelled
//! and nothing can be refused.
//!
//! A line is `subject TAB predicate TAB object LF`, each field a dictionary
//! string with `\`, TAB, LF and CR escaped as `\\`, `\t`, `\n` and `\r`. A
//! field is read back with oxrdf's unchecked constructors, which validate
//! nothing; a field that opens with a quote becomes a literal only when the
//! parser writes that literal back as the same string, and is otherwise an
//! IRI, which it stores as given. Either way the parser's own conversion to
//! dictionary strings reproduces the field byte for byte.

use anyhow::{Context, Result, bail};
use oxrdf::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, Triple};
use std::io::{BufRead, BufReader, Read, Write};

/// Write one triple of dictionary strings as a record.
pub(crate) fn write_record(
    out: &mut impl Write,
    subject: &[u8],
    predicate: &[u8],
    object: &[u8],
) -> std::io::Result<()> {
    write_field(out, subject)?;
    out.write_all(b"\t")?;
    write_field(out, predicate)?;
    out.write_all(b"\t")?;
    write_field(out, object)?;
    out.write_all(b"\n")
}

fn write_field(out: &mut impl Write, field: &[u8]) -> std::io::Result<()> {
    let mut start = 0;
    for (i, byte) in field.iter().enumerate() {
        let escape: &[u8] = match byte {
            b'\\' => b"\\\\",
            b'\t' => b"\\t",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            _ => continue,
        };
        out.write_all(&field[start..i])?;
        out.write_all(escape)?;
        start = i + 1;
    }
    out.write_all(&field[start..])
}

fn read_field(field: &str) -> Result<String> {
    let mut out = String::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        out.push(match chars.next() {
            Some('\\') => '\\',
            Some('t') => '\t',
            Some('n') => '\n',
            Some('r') => '\r',
            other => bail!(
                "a term record holds the escape `\\{}`",
                other.unwrap_or(' ')
            ),
        });
    }
    Ok(out)
}

/// The term a dictionary string spells, built without validation.
fn subject(text: String) -> NamedOrBlankNode {
    match text.strip_prefix("_:") {
        Some(label) => BlankNode::new_unchecked(label).into(),
        None => NamedNode::new_unchecked(text).into(),
    }
}

fn object(text: String) -> Term {
    if !text.starts_with('"') {
        return subject(text).into();
    }
    match literal(&text) {
        // Only a literal the parser writes back as this very string: lenient
        // ingest also stores strings that open with a quote but are not in
        // that form (a tag in capitals, an explicit `xsd:string`, an IRI
        // spelled `<"x>`), and those pass through whole as IRIs, which the
        // parser stores as given.
        Some(literal)
            if crate::rdf::parser::literal_to_hdt_string(&literal, true)
                .is_ok_and(|written| written == text) =>
        {
            literal.into()
        }
        _ => NamedNode::new_unchecked(text).into(),
    }
}

/// The literal a dictionary string spells, if it has a literal's shape:
/// `"value"`, `"value"@tag` or `"value"^^<iri>`, the value unescaped. The
/// suffix is found from the end, where the value cannot reach: a tag holds no
/// quote, and a datatype closes the string.
fn literal(text: &str) -> Option<Literal> {
    if text.len() >= 2 && text.ends_with('"') {
        return Some(Literal::new_simple_literal(&text[1..text.len() - 1]));
    }
    if text.ends_with('>')
        && let Some(cut) = text.rfind("\"^^<")
    {
        let datatype = &text[cut + 4..text.len() - 1];
        return Some(Literal::new_typed_literal(
            &text[1..cut],
            NamedNode::new_unchecked(datatype),
        ));
    }
    let cut = text.rfind("\"@")?;
    Some(Literal::new_language_tagged_literal_unchecked(
        &text[1..cut],
        &text[cut + 2..],
    ))
}

/// Reads records back as triples.
pub(crate) struct RecordReader<R: Read> {
    lines: std::io::Lines<BufReader<R>>,
    line: u64,
}

impl<R: Read> RecordReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            lines: BufReader::with_capacity(1 << 20, reader).lines(),
            line: 0,
        }
    }

    fn triple(line: &str) -> Result<Triple> {
        let mut fields = line.split('\t');
        let (Some(s), Some(p), Some(o), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            bail!("a term record does not have three fields");
        };
        Ok(Triple::new(
            subject(read_field(s)?),
            NamedNode::new_unchecked(read_field(p)?),
            object(read_field(o)?),
        ))
    }
}

impl<R: Read> Iterator for RecordReader<R> {
    type Item = Result<Triple>;

    fn next(&mut self) -> Option<Self::Item> {
        let line = self.lines.next()?;
        self.line += 1;
        let line_number = self.line;
        Some(
            line.context("reading term records")
                .and_then(|line| Self::triple(&line))
                .with_context(|| format!("term record {line_number}")),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The dictionary spelling of a subject, as the parser writes it.
    fn named_or_blank_to_string(term: &NamedOrBlankNode) -> String {
        match term {
            NamedOrBlankNode::NamedNode(node) => node.as_str().to_owned(),
            NamedOrBlankNode::BlankNode(node) => format!("_:{}", node.as_str()),
        }
    }

    /// The dictionary spelling of a literal, as the parser writes it.
    fn literal_to_hdt_string(literal: &Literal) -> String {
        crate::rdf::parser::literal_to_hdt_string(literal, true).unwrap()
    }

    /// Every string the dictionary can hold reads back as itself, including
    /// those no RDF syntax spells.
    #[test]
    fn a_record_reads_back_the_strings_it_was_written_from() {
        let rows: [[&str; 3]; 10] = [
            ["http://ex/s", "http://ex/p", "http://ex/o"],
            ["_:x.", "http://ex/p a", "\"v\"@en_us"],
            [
                "_:b",
                "http://ex/p",
                "\"tab\there\\n \"quoted\" \"^^<x>\"^^<http://ex/d\"t>",
            ],
            ["http://ex/a>b", "http://ex/p", "\"x\"@ar--rtl"],
            ["http://ex/s", "http://ex/p", "\"\""],
            ["http://ex/s", "http://ex/p", "\"line\nbreak\\\"@x\""],
            // Strings that open with a quote but are no literal the parser
            // writes: they come back as IRIs, spelled the same.
            ["http://ex/s", "http://ex/p", "\"x"],
            ["http://ex/s", "http://ex/p", "\"a\"@EN"],
            [
                "http://ex/s",
                "http://ex/p",
                "\"a\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ],
            ["\"q\"", "http://ex/p", "_:x"],
        ];
        let mut buffer = Vec::new();
        for [s, p, o] in rows {
            write_record(&mut buffer, s.as_bytes(), p.as_bytes(), o.as_bytes()).unwrap();
        }
        let read: Vec<Triple> = RecordReader::new(buffer.as_slice())
            .collect::<Result<_>>()
            .unwrap();
        for ([s, p, o], triple) in rows.iter().zip(read) {
            assert_eq!(named_or_blank_to_string(&triple.subject), *s);
            assert_eq!(triple.predicate.as_str(), *p);
            let object = match &triple.object {
                Term::Literal(literal) => literal_to_hdt_string(literal),
                Term::NamedNode(node) => node.as_str().to_owned(),
                Term::BlankNode(node) => format!("_:{}", node.as_str()),
                Term::Triple(_) => unreachable!(),
            };
            assert_eq!(object, *o);
        }
    }
}
