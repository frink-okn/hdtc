//! Naming a triple term: the IRI that stands for it in the dictionary.
//!
//! An HDT dictionary holds IRIs, blank nodes and literals, and nothing else a
//! standard reader would accept. RDF 1.2 adds a fourth kind of term, the triple
//! term `<<( s p o )>>`, which may appear as an object. hdtc keeps `data.hdt`
//! standard by giving every distinct triple term an IRI in a reserved namespace
//! and storing that IRI where the term occurs; what the IRI denotes is recorded
//! beside the HDT (`docs/triple-terms-format.md`).
//!
//! Two forms, both under [`TT_NAMESPACE`]:
//!
//! - **Ground** terms, with no blank node at any depth, are named by content:
//!   `urn:x-kgf:tt:g:{h}`, where `h` is the first 16 bytes of the SHA-256 of
//!   the term's canonical N-Triples 1.2 form, in lowercase hex. Equal terms get
//!   equal IRIs in every build, which is what lets separately built HDTs merge
//!   their triple terms by string equality.
//! - Terms with a blank node are named by **skeleton and labels**:
//!   `urn:x-kgf:tt:b:{k}:{label}…`, where `k` hashes the canonical form with each
//!   blank node replaced by `_:b1`, `_:b2`, … in order of first appearance, and
//!   the labels follow in that order, percent-encoded. A content hash cannot
//!   name such a term across a merge — blank nodes are renamed apart there, and
//!   a hash cannot be renamed — so the labels are kept where the merge's rename
//!   can reach them ([`prefix_labels`]): renaming a term's blank nodes and then
//!   minting gives the same string as minting and then renaming the IRI's labels.

use sha2::{Digest, Sha256};

/// The namespace every triple-term IRI lives under. Reserved whole: source RDF
/// that uses it anywhere is refused (`docs/triple-terms-format.md` §2.3).
pub const TT_NAMESPACE: &str = "urn:x-kgf:tt:";
/// Prefix of a ground triple term's IRI.
pub const GROUND_PREFIX: &str = "urn:x-kgf:tt:g:";
/// Prefix of a blank-node-bearing triple term's IRI.
pub const BLANK_PREFIX: &str = "urn:x-kgf:tt:b:";
/// Hex digits of the hash in either form.
pub const HASH_HEX_LEN: usize = 32;
/// The deepest nesting accepted, counting the outermost triple term as the
/// first level. Deeper input is refused when parsed, so every reader can
/// follow any term it meets (`docs/triple-terms-format.md` §2.4).
pub const MAX_NESTING: usize = 64;

/// One position of a proposition, in dictionary form.
///
/// `Blank` holds the label without its `_:`; `Literal` holds the dictionary
/// string (`"raw"`, `"raw"@lang`, `"raw"^^<dt>`); a nested triple term is held
/// structurally, because its canonical form is written inline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Component {
    Iri(String),
    Blank(String),
    Literal(String),
    Triple(Box<Proposition>),
}

/// The content of a triple term: subject, predicate, object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposition {
    pub subject: Component,
    pub predicate: String,
    pub object: Component,
}

/// A distinct triple term as the build records it: its IRI and its three
/// positions in dictionary form, a nested triple term given by its IRI.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PropositionRecord {
    pub iri: String,
    pub subject: String,
    pub predicate: String,
    pub object: String,
}

impl Component {
    /// The dictionary string, minting nested triple terms as needed and
    /// recording each one's proposition in `records`.
    fn dictionary_string(&self, records: &mut Vec<PropositionRecord>) -> String {
        match self {
            Self::Iri(iri) => iri.clone(),
            Self::Blank(label) => format!("_:{label}"),
            Self::Literal(literal) => literal.clone(),
            Self::Triple(inner) => mint_recording(inner, records),
        }
    }
}

/// Mint the IRI for a proposition, appending a record for it and for every
/// triple term nested inside it (innermost first) to `records`.
pub fn mint(proposition: &Proposition, records: &mut Vec<PropositionRecord>) -> String {
    mint_recording(proposition, records)
}

fn mint_recording(proposition: &Proposition, records: &mut Vec<PropositionRecord>) -> String {
    let subject = proposition.subject.dictionary_string(records);
    let object = proposition.object.dictionary_string(records);
    let iri = iri_for(proposition);
    records.push(PropositionRecord {
        iri: iri.clone(),
        subject,
        predicate: proposition.predicate.clone(),
        object,
    });
    iri
}

/// The IRI of a proposition, without recording anything.
pub fn iri_for(proposition: &Proposition) -> String {
    let mut labels: Vec<&str> = Vec::new();
    let mut text = Vec::with_capacity(256);
    write_canonical_triple(&mut text, proposition, &mut labels);
    let digest = Sha256::digest(&text);
    let mut iri = String::with_capacity(BLANK_PREFIX.len() + HASH_HEX_LEN + labels.len() * 16);
    iri.push_str(if labels.is_empty() {
        GROUND_PREFIX
    } else {
        BLANK_PREFIX
    });
    push_hex(&mut iri, &digest[..HASH_HEX_LEN / 2]);
    for label in labels {
        iri.push(':');
        push_percent_encoded(&mut iri, label.as_bytes());
    }
    iri
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0F)] as char);
    }
}

/// Percent-encode everything outside RFC 3986 `unreserved`. The separator `:`
/// is therefore never part of an encoded label, which is what keeps
/// [`prefix_labels`] order-preserving.
fn push_percent_encoded(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(HEX[usize::from(byte >> 4)] as char);
            out.push(HEX[usize::from(byte & 0x0F)] as char);
        }
    }
}

/// Write a triple term's canonical N-Triples 1.2 form, `<<( S P O )>>`, with
/// blank nodes replaced by positional placeholders in `labels` order.
fn write_canonical_triple<'a>(
    out: &mut Vec<u8>,
    proposition: &'a Proposition,
    labels: &mut Vec<&'a str>,
) {
    out.extend_from_slice(b"<<( ");
    write_canonical_component(out, &proposition.subject, labels);
    out.push(b' ');
    write_canonical_iri(out, &proposition.predicate);
    out.push(b' ');
    write_canonical_component(out, &proposition.object, labels);
    out.extend_from_slice(b" )>>");
}

fn write_canonical_component<'a>(
    out: &mut Vec<u8>,
    component: &'a Component,
    labels: &mut Vec<&'a str>,
) {
    match component {
        Component::Iri(iri) => write_canonical_iri(out, iri),
        Component::Blank(label) => {
            let ordinal = match labels.iter().position(|seen| seen == label) {
                Some(index) => index + 1,
                None => {
                    labels.push(label);
                    labels.len()
                }
            };
            out.extend_from_slice(b"_:b");
            out.extend_from_slice(ordinal.to_string().as_bytes());
        }
        Component::Literal(literal) => write_canonical_literal(out, literal),
        Component::Triple(inner) => write_canonical_triple(out, inner, labels),
    }
}

fn write_canonical_iri(out: &mut Vec<u8>, iri: &str) {
    out.push(b'<');
    out.extend_from_slice(iri.as_bytes());
    out.push(b'>');
}

/// A dictionary literal in canonical N-Triples 1.2 form. The dictionary already
/// lowercases language tags and drops `xsd:string`; this escapes the value as
/// the canonical form requires.
fn write_canonical_literal(out: &mut Vec<u8>, literal: &str) {
    let parsed = crate::text::analyzer::parse_literal(literal.as_bytes());
    let Some(parsed) = parsed else {
        // Not a dictionary literal; cannot happen for terms the parser made.
        out.extend_from_slice(literal.as_bytes());
        return;
    };
    out.push(b'"');
    let value = String::from_utf8_lossy(parsed.value);
    for c in value.chars() {
        match c {
            '\u{08}' => out.extend_from_slice(b"\\b"),
            '\t' => out.extend_from_slice(b"\\t"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\u{0C}' => out.extend_from_slice(b"\\f"),
            '\r' => out.extend_from_slice(b"\\r"),
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\u{00}'..='\u{07}'
            | '\u{0B}'
            | '\u{0E}'..='\u{1F}'
            | '\u{7F}'
            | '\u{FFFE}'
            | '\u{FFFF}' => {
                out.extend_from_slice(format!("\\u{:04X}", c as u32).as_bytes());
            }
            c => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    out.push(b'"');
    if let Some(language) = parsed.language {
        out.push(b'@');
        out.extend_from_slice(&language.to_ascii_lowercase());
    } else if let Some(datatype) = parsed.datatype {
        out.extend_from_slice(b"^^<");
        out.extend_from_slice(datatype);
        out.push(b'>');
    }
}

/// Rename the blank nodes inside a triple-term IRI the way hdtc renames blank
/// nodes apart when it merges separately built inputs: `_:x` becomes
/// `_:{prefix}x`. Ground IRIs are returned unchanged.
///
/// Inserting one fixed prefix before every label preserves byte order among
/// IRIs of the same input, which is what the merge's k-way vocabulary merge
/// requires of each input stream.
pub fn prefix_labels(iri: &[u8], prefix: &str) -> Option<Vec<u8>> {
    let rest = iri.strip_prefix(BLANK_PREFIX.as_bytes())?;
    let mut out = Vec::with_capacity(iri.len() + prefix.len() * 4);
    out.extend_from_slice(BLANK_PREFIX.as_bytes());
    let mut segments = rest.split(|&b| b == b':');
    out.extend_from_slice(segments.next().unwrap_or_default());
    let mut encoded_prefix = String::new();
    push_percent_encoded(&mut encoded_prefix, prefix.as_bytes());
    for segment in segments {
        out.push(b':');
        out.extend_from_slice(encoded_prefix.as_bytes());
        out.extend_from_slice(segment);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iri(s: &str) -> Component {
        Component::Iri(s.to_owned())
    }
    fn blank(s: &str) -> Component {
        Component::Blank(s.to_owned())
    }
    fn prop(s: Component, p: &str, o: Component) -> Proposition {
        Proposition {
            subject: s,
            predicate: p.to_owned(),
            object: o,
        }
    }

    fn canonical(proposition: &Proposition) -> String {
        let mut text = Vec::new();
        write_canonical_triple(&mut text, proposition, &mut Vec::new());
        String::from_utf8(text).unwrap()
    }

    #[test]
    fn the_canonical_form_is_n_triples_1_2() {
        let p = prop(
            iri("http://ex/s"),
            "http://ex/p",
            Component::Literal("\"a \"quoted\"\tvalue\u{1}\"@en".to_owned()),
        );
        assert_eq!(
            canonical(&p),
            "<<( <http://ex/s> <http://ex/p> \"a \\\"quoted\\\"\\tvalue\\u0001\"@en )>>"
        );
        let typed = prop(
            iri("http://ex/s"),
            "http://ex/p",
            Component::Literal("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned()),
        );
        assert_eq!(
            canonical(&typed),
            "<<( <http://ex/s> <http://ex/p> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> )>>"
        );
        let nested = prop(
            iri("http://ex/a"),
            "http://ex/says",
            Component::Triple(Box::new(prop(
                iri("http://ex/s"),
                "http://ex/p",
                iri("http://ex/o"),
            ))),
        );
        assert_eq!(
            canonical(&nested),
            "<<( <http://ex/a> <http://ex/says> <<( <http://ex/s> <http://ex/p> <http://ex/o> )>> )>>"
        );
    }

    #[test]
    fn a_ground_iri_is_a_content_hash() {
        let p = prop(iri("http://ex/s"), "http://ex/p", iri("http://ex/o"));
        let name = iri_for(&p);
        let expected = Sha256::digest(b"<<( <http://ex/s> <http://ex/p> <http://ex/o> )>>");
        let mut hex = String::new();
        push_hex(&mut hex, &expected[..16]);
        assert_eq!(name, format!("{GROUND_PREFIX}{hex}"));
        assert_eq!(name, iri_for(&p.clone()));
    }

    #[test]
    fn blank_nodes_are_hashed_by_position_and_kept_as_labels() {
        let a = prop(blank("x"), "http://ex/p", blank("y"));
        let b = prop(blank("u"), "http://ex/p", blank("v"));
        let name_a = iri_for(&a);
        let name_b = iri_for(&b);
        // Same skeleton, so the same hash; different labels, so different IRIs.
        assert_eq!(
            name_a[..BLANK_PREFIX.len() + HASH_HEX_LEN],
            name_b[..BLANK_PREFIX.len() + HASH_HEX_LEN]
        );
        assert_ne!(name_a, name_b);
        assert!(name_a.ends_with(":x:y"));
        // Co-reference is part of the skeleton.
        let same = prop(blank("x"), "http://ex/p", blank("x"));
        assert!(iri_for(&same).ends_with(":x"));
        assert_ne!(
            iri_for(&same)[..BLANK_PREFIX.len() + HASH_HEX_LEN],
            name_a[..BLANK_PREFIX.len() + HASH_HEX_LEN]
        );
    }

    #[test]
    fn renaming_blank_nodes_commutes_with_minting() {
        let original = prop(
            blank("b1"),
            "http://ex/p",
            Component::Triple(Box::new(prop(
                blank("odd:label%"),
                "http://ex/q",
                blank("b1"),
            ))),
        );
        let renamed = prop(
            blank("f3_b1"),
            "http://ex/p",
            Component::Triple(Box::new(prop(
                blank("f3_odd:label%"),
                "http://ex/q",
                blank("f3_b1"),
            ))),
        );
        let minted_then_renamed = prefix_labels(iri_for(&original).as_bytes(), "f3_").unwrap();
        assert_eq!(
            String::from_utf8(minted_then_renamed).unwrap(),
            iri_for(&renamed)
        );
    }

    #[test]
    fn renaming_preserves_order_among_iris() {
        let mut names: Vec<String> = ["a", "a2", "a.b", "b", "zz", "a-", "A"]
            .iter()
            .flat_map(|l| {
                [
                    iri_for(&prop(blank(l), "http://ex/p", iri("http://ex/o"))),
                    iri_for(&prop(blank(l), "http://ex/p", blank("m"))),
                ]
            })
            .collect();
        names.sort();
        let renamed: Vec<Vec<u8>> = names
            .iter()
            .map(|n| prefix_labels(n.as_bytes(), "f12_").unwrap())
            .collect();
        let mut sorted = renamed.clone();
        sorted.sort();
        assert_eq!(renamed, sorted);
    }

    #[test]
    fn minting_records_nested_terms_innermost_first() {
        let inner = prop(iri("http://ex/s"), "http://ex/p", iri("http://ex/o"));
        let outer = prop(
            iri("http://ex/a"),
            "http://ex/says",
            Component::Triple(Box::new(inner.clone())),
        );
        let mut records = Vec::new();
        let name = mint(&outer, &mut records);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].iri, iri_for(&inner));
        assert_eq!(records[1].iri, name);
        assert_eq!(records[1].object, records[0].iri);
        assert_eq!(records[1].subject, "http://ex/a");
    }
}
