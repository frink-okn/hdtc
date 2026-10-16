//! U+0000 in parsed terms, removed before a term reaches the dictionary.
//!
//! An HDT dictionary ends each string at a `0x00` byte (Plain Front Coding),
//! so it cannot store a term holding U+0000: a reader would end the term at
//! the character and decode the rest of it as the next entry. RDF has no use
//! for the character either. The lexical space of `xsd:string`, and so of
//! `rdf:langString`, is sequences of XML `Char`, which excludes it; an IRI
//! cannot contain it; RDF/XML cannot express it. N-Triples, Turtle and JSON-LD
//! can still write it, as `\u0000` or as a raw byte, and Wikidata's dumps do.
//!
//! So it is removed, always: `"a\u0000b"` is stored as `"ab"`. Removing it,
//! rather than putting a stand-in character in its place, leaves every term
//! exactly as valid as it was and never longer. The obvious stand-in, U+FFFD,
//! is not a legal IRI character, so a graph name holding it fails hdtc's own
//! sidecar validation; and at three bytes for one it can grow a term past the
//! bound the lexer enforced. A term that becomes equal to another is that
//! term, as any duplicate is. The statement is kept, and counted; the first
//! few per input are logged, naming the statement so that it can be found in
//! the source.

use crate::triple_terms;
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term, Triple};

/// How many removals per input are logged one by one. The rest are only
/// counted, and the count is logged when the input ends.
pub(crate) const LOGGED_PER_INPUT: u64 = 10;

/// How much of a term a message shows, in characters.
const EXCERPT_CHARS: usize = 200;

/// What [`remove_from_quad`] did to a quad.
pub(crate) struct Removal {
    /// Whether the quad held U+0000.
    pub removed: bool,
    /// When it did and a note was asked for: which statement, and where in it,
    /// as the source wrote it.
    pub note: Option<String>,
}

/// The quad with every U+0000 in its terms removed.
///
/// A quad without one, which is every quad but a handful, comes back as it
/// went in, without allocating; one with it is cleaned in place. The note is
/// built only when `want_note` asks for it, so statements past the logging
/// cap cost no more than the scan.
pub(crate) fn remove_from_quad(quad: Quad, want_note: bool) -> (Quad, Removal) {
    let in_subject = subject_has_nul(&quad.subject);
    let in_predicate = has_nul(quad.predicate.as_str());
    let in_object = term_has_nul(&quad.object, 0);
    let in_graph = graph_has_nul(&quad.graph_name);
    if !(in_subject || in_predicate || in_object || in_graph) {
        let removal = Removal {
            removed: false,
            note: None,
        };
        return (quad, removal);
    }

    let note = want_note.then(|| {
        let positions: Vec<&str> = [
            (in_subject, "subject"),
            (in_predicate, "predicate"),
            (in_object, "object"),
            (in_graph, "graph name"),
        ]
        .into_iter()
        .filter_map(|(found, position)| found.then_some(position))
        .collect();
        let subject = match &quad.subject {
            NamedOrBlankNode::NamedNode(n) => format!("<{}>", excerpt(n.as_str())),
            NamedOrBlankNode::BlankNode(b) => format!("_:{}", excerpt(b.as_str())),
        };
        format!(
            "U+0000 removed from the {} of the statement {subject} <{}>",
            positions.join(" and "),
            excerpt(quad.predicate.as_str()),
        )
    });

    let Quad {
        subject,
        predicate,
        object,
        graph_name,
    } = quad;
    let quad = Quad::new(
        remove_from_subject(subject),
        remove_from_named(predicate),
        remove_from_term(object, 0),
        remove_from_graph(graph_name),
    );
    let removal = Removal {
        removed: true,
        note,
    };
    (quad, removal)
}

/// At most [`EXCERPT_CHARS`] of `text`, with U+0000 written as `\u0000` so a
/// message never carries the raw byte.
pub(crate) fn excerpt(text: &str) -> String {
    let mut shown = String::with_capacity(text.len().min(EXCERPT_CHARS + 8));
    for (count, c) in text.chars().enumerate() {
        if count == EXCERPT_CHARS {
            shown.push('…');
            break;
        }
        if c == '\0' {
            shown.push_str("\\u0000");
        } else {
            shown.push(c);
        }
    }
    shown
}

fn has_nul(text: &str) -> bool {
    text.as_bytes().contains(&0)
}

/// `text` without U+0000, cleaned in place.
fn removed(mut text: String) -> String {
    if has_nul(&text) {
        text.retain(|c| c != '\0');
    }
    text
}

fn subject_has_nul(subject: &NamedOrBlankNode) -> bool {
    match subject {
        NamedOrBlankNode::NamedNode(n) => has_nul(n.as_str()),
        NamedOrBlankNode::BlankNode(b) => has_nul(b.as_str()),
    }
}

fn literal_has_nul(literal: &Literal) -> bool {
    has_nul(literal.value())
        || literal.language().is_some_and(has_nul)
        || has_nul(literal.datatype().as_str())
}

/// Whether `term` holds U+0000. A triple term is searched only to the nesting
/// limit: a deeper one is refused when the quad is converted, so nothing in it
/// reaches the dictionary.
fn term_has_nul(term: &Term, depth: usize) -> bool {
    match term {
        Term::NamedNode(n) => has_nul(n.as_str()),
        Term::BlankNode(b) => has_nul(b.as_str()),
        Term::Literal(l) => literal_has_nul(l),
        Term::Triple(t) => {
            depth < triple_terms::iri::MAX_NESTING
                && (subject_has_nul(&t.subject)
                    || has_nul(t.predicate.as_str())
                    || term_has_nul(&t.object, depth + 1))
        }
    }
}

fn graph_has_nul(graph: &GraphName) -> bool {
    match graph {
        GraphName::NamedNode(n) => has_nul(n.as_str()),
        GraphName::BlankNode(b) => has_nul(b.as_str()),
        GraphName::DefaultGraph => false,
    }
}

// Each `remove_from_*` checks only its own strings, so a term is scanned once
// however deeply it nests.

fn remove_from_named(node: NamedNode) -> NamedNode {
    if has_nul(node.as_str()) {
        NamedNode::new_unchecked(removed(node.into_string()))
    } else {
        node
    }
}

fn remove_from_blank(node: BlankNode) -> BlankNode {
    if has_nul(node.as_str()) {
        BlankNode::new_unchecked(removed(node.into_string()))
    } else {
        node
    }
}

fn remove_from_subject(subject: NamedOrBlankNode) -> NamedOrBlankNode {
    match subject {
        NamedOrBlankNode::NamedNode(n) => remove_from_named(n).into(),
        NamedOrBlankNode::BlankNode(b) => remove_from_blank(b).into(),
    }
}

fn remove_from_literal(literal: Literal) -> Literal {
    if !literal_has_nul(&literal) {
        return literal;
    }
    let (value, datatype, language, direction) = literal.destruct();
    let value = removed(value);
    match (language, direction, datatype) {
        (Some(language), Some(direction), _) => {
            Literal::new_directional_language_tagged_literal_unchecked(
                value,
                removed(language),
                direction,
            )
        }
        (Some(language), None, _) => {
            Literal::new_language_tagged_literal_unchecked(value, removed(language))
        }
        (None, _, Some(datatype)) => Literal::new_typed_literal(value, remove_from_named(datatype)),
        (None, _, None) => Literal::new_simple_literal(value),
    }
}

/// `term` without U+0000, to the nesting limit [`term_has_nul`] searches.
fn remove_from_term(term: Term, depth: usize) -> Term {
    match term {
        Term::NamedNode(n) => remove_from_named(n).into(),
        Term::BlankNode(b) => remove_from_blank(b).into(),
        Term::Literal(l) => remove_from_literal(l).into(),
        Term::Triple(t) if depth < triple_terms::iri::MAX_NESTING => {
            let Triple {
                subject,
                predicate,
                object,
            } = *t;
            Triple::new(
                remove_from_subject(subject),
                remove_from_named(predicate),
                remove_from_term(object, depth + 1),
            )
            .into()
        }
        deeper => deeper,
    }
}

fn remove_from_graph(graph: GraphName) -> GraphName {
    match graph {
        GraphName::NamedNode(n) => remove_from_named(n).into(),
        GraphName::BlankNode(b) => remove_from_blank(b).into(),
        GraphName::DefaultGraph => GraphName::DefaultGraph,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::BaseDirection;

    fn named(iri: &str) -> NamedNode {
        NamedNode::new_unchecked(iri)
    }

    fn quad_with_object(object: impl Into<Term>) -> Quad {
        Quad::new(
            named("http://example.org/s"),
            named("http://example.org/p"),
            object,
            GraphName::DefaultGraph,
        )
    }

    #[test]
    fn a_quad_without_nul_is_returned_unchanged() {
        let quad = quad_with_object(Literal::new_simple_literal("alpha"));
        let (out, removal) = remove_from_quad(quad.clone(), true);
        assert_eq!(out, quad);
        assert!(!removal.removed);
        assert!(removal.note.is_none());
    }

    #[test]
    fn nul_in_a_literal_value_is_removed_and_the_statement_named() {
        let quad = quad_with_object(Literal::new_simple_literal("alpha\0beta"));
        let (out, removal) = remove_from_quad(quad, true);
        assert_eq!(
            out.object,
            Term::from(Literal::new_simple_literal("alphabeta"))
        );
        assert!(removal.removed);
        assert_eq!(
            removal.note.unwrap(),
            "U+0000 removed from the object of the statement \
             <http://example.org/s> <http://example.org/p>"
        );
    }

    #[test]
    fn the_note_is_built_only_when_asked_for() {
        let quad = quad_with_object(Literal::new_simple_literal("a\0"));
        let (out, removal) = remove_from_quad(quad, false);
        assert_eq!(out.object, Term::from(Literal::new_simple_literal("a")));
        assert!(removal.removed);
        assert!(removal.note.is_none());
    }

    #[test]
    fn every_nul_in_a_term_is_removed() {
        let quad = quad_with_object(Literal::new_simple_literal("\0a\0\0b\0"));
        let (out, _) = remove_from_quad(quad, false);
        assert_eq!(out.object, Term::from(Literal::new_simple_literal("ab")));
    }

    #[test]
    fn literal_kind_is_kept() {
        let tagged = Literal::new_language_tagged_literal_unchecked("a\0", "en");
        let (out, _) = remove_from_quad(quad_with_object(tagged), false);
        assert_eq!(
            out.object,
            Term::from(Literal::new_language_tagged_literal_unchecked("a", "en"))
        );

        let directional = Literal::new_directional_language_tagged_literal_unchecked(
            "a\0",
            "ar",
            BaseDirection::Rtl,
        );
        let (out, _) = remove_from_quad(quad_with_object(directional), false);
        assert_eq!(
            out.object,
            Term::from(Literal::new_directional_language_tagged_literal_unchecked(
                "a",
                "ar",
                BaseDirection::Rtl
            ))
        );

        let typed = Literal::new_typed_literal("1\0", named("http://example.org/dt\0"));
        let (out, _) = remove_from_quad(quad_with_object(typed), false);
        assert_eq!(
            out.object,
            Term::from(Literal::new_typed_literal(
                "1",
                named("http://example.org/dt")
            ))
        );
    }

    #[test]
    fn every_position_is_covered_and_named() {
        let quad = Quad::new(
            named("http://example.org/s\0"),
            named("http://example.org/p\0"),
            BlankNode::new_unchecked("b\0"),
            named("http://example.org/g\0"),
        );
        let (out, removal) = remove_from_quad(quad, true);
        assert_eq!(
            out,
            Quad::new(
                named("http://example.org/s"),
                named("http://example.org/p"),
                BlankNode::new_unchecked("b"),
                named("http://example.org/g"),
            )
        );
        // The note names the source's terms, with the character escaped.
        assert_eq!(
            removal.note.unwrap(),
            "U+0000 removed from the subject and predicate and object and graph name \
             of the statement <http://example.org/s\\u0000> <http://example.org/p\\u0000>"
        );
    }

    /// An IRI loses only the character, so it stays as valid as it was.
    #[test]
    fn an_iri_stays_valid() {
        let quad = quad_with_object(named("http://example.org/o\0"));
        let (out, _) = remove_from_quad(quad, false);
        let Term::NamedNode(object) = out.object else {
            panic!("object is still an IRI");
        };
        assert!(NamedNode::new(object.as_str()).is_ok());
    }

    #[test]
    fn nul_inside_a_triple_term_is_removed() {
        let inner = Triple::new(
            named("http://example.org/a"),
            named("http://example.org/b"),
            Literal::new_simple_literal("c\0"),
        );
        let outer = Triple::new(
            named("http://example.org/x"),
            named("http://example.org/y"),
            inner,
        );
        let (out, removal) = remove_from_quad(quad_with_object(outer), true);
        let Term::Triple(outer) = out.object else {
            panic!("object is still a triple term");
        };
        let Term::Triple(inner) = outer.object else {
            panic!("nested object is still a triple term");
        };
        assert_eq!(inner.object, Term::from(Literal::new_simple_literal("c")));
        assert!(removal.note.unwrap().contains("from the object of"));
    }

    #[test]
    fn excerpt_is_bounded_and_escapes_nul() {
        assert_eq!(excerpt("a\0b"), "a\\u0000b");
        let long = "x".repeat(EXCERPT_CHARS + 50);
        let shown = excerpt(&long);
        assert_eq!(shown.chars().count(), EXCERPT_CHARS + 1);
        assert!(shown.ends_with('…'));
    }
}
