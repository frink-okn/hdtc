//! U+0000 in parsed terms, replaced with U+FFFD before a term reaches the
//! dictionary.
//!
//! An HDT dictionary ends each string at a `0x00` byte (Plain Front Coding),
//! so it cannot store a term holding U+0000: a reader would end the term at
//! the character and decode the rest of it as the next entry. RDF has no use
//! for the character either. The lexical space of `xsd:string`, and so of
//! `rdf:langString`, is sequences of XML `Char`, which excludes it; an IRI
//! cannot contain it; RDF/XML cannot express it. N-Triples, Turtle and JSON-LD
//! can still write it, as `\u0000` or as a raw byte, and Wikidata's dumps do.
//!
//! So it is replaced, always, with U+FFFD REPLACEMENT CHARACTER. The statement
//! is kept, and counted; the first few per input are logged, naming the
//! statement so that it can be found in the source.

use crate::triple_terms;
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term, Triple};

/// How many replacements per input are logged one by one. The rest are only
/// counted, and the count is logged when the input ends.
pub(crate) const LOGGED_PER_INPUT: u64 = 10;

/// How much of a term a message shows, in characters.
const EXCERPT_CHARS: usize = 200;

/// The quad with every U+0000 in its terms replaced by U+FFFD, and a note
/// naming the statement when there was one to replace.
///
/// A quad without one, which is every quad but a handful, comes back as it
/// went in, without allocating.
pub(crate) fn replace_in_quad(quad: Quad) -> (Quad, Option<String>) {
    let in_subject = subject_has_nul(&quad.subject);
    let in_predicate = has_nul(quad.predicate.as_str());
    let in_object = term_has_nul(&quad.object, 0);
    let in_graph = graph_has_nul(&quad.graph_name);
    if !(in_subject || in_predicate || in_object || in_graph) {
        return (quad, None);
    }

    let positions: Vec<&str> = [
        (in_subject, "subject"),
        (in_predicate, "predicate"),
        (in_object, "object"),
        (in_graph, "graph name"),
    ]
    .into_iter()
    .filter_map(|(found, position)| found.then_some(position))
    .collect();
    let note = format!(
        "U+0000 replaced with U+FFFD in the {} of the statement {} {}",
        positions.join(" and "),
        excerpt(&quad.subject.to_string()),
        excerpt(&quad.predicate.to_string()),
    );

    let Quad {
        subject,
        predicate,
        object,
        graph_name,
    } = quad;
    let quad = Quad::new(
        replace_in_subject(subject),
        replace_in_named(predicate),
        replace_in_term(object, 0),
        replace_in_graph(graph_name),
    );
    (quad, Some(note))
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

fn replaced(text: &str) -> String {
    text.replace('\0', "\u{FFFD}")
}

fn replace_in_string(text: String) -> String {
    if has_nul(&text) {
        replaced(&text)
    } else {
        text
    }
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

fn replace_in_named(node: NamedNode) -> NamedNode {
    if has_nul(node.as_str()) {
        NamedNode::new_unchecked(replaced(node.as_str()))
    } else {
        node
    }
}

fn replace_in_blank(node: BlankNode) -> BlankNode {
    if has_nul(node.as_str()) {
        BlankNode::new_unchecked(replaced(node.as_str()))
    } else {
        node
    }
}

fn replace_in_subject(subject: NamedOrBlankNode) -> NamedOrBlankNode {
    match subject {
        NamedOrBlankNode::NamedNode(n) => replace_in_named(n).into(),
        NamedOrBlankNode::BlankNode(b) => replace_in_blank(b).into(),
    }
}

fn replace_in_literal(literal: Literal) -> Literal {
    if !literal_has_nul(&literal) {
        return literal;
    }
    let (value, datatype, language, direction) = literal.destruct();
    let value = replace_in_string(value);
    match (language, direction, datatype) {
        (Some(language), Some(direction), _) => {
            Literal::new_directional_language_tagged_literal_unchecked(
                value,
                replace_in_string(language),
                direction,
            )
        }
        (Some(language), None, _) => {
            Literal::new_language_tagged_literal_unchecked(value, replace_in_string(language))
        }
        (None, _, Some(datatype)) => Literal::new_typed_literal(value, replace_in_named(datatype)),
        (None, _, None) => Literal::new_simple_literal(value),
    }
}

fn replace_in_term(term: Term, depth: usize) -> Term {
    if !term_has_nul(&term, depth) {
        return term;
    }
    match term {
        Term::NamedNode(n) => replace_in_named(n).into(),
        Term::BlankNode(b) => replace_in_blank(b).into(),
        Term::Literal(l) => replace_in_literal(l).into(),
        Term::Triple(t) => {
            let Triple {
                subject,
                predicate,
                object,
            } = *t;
            Triple::new(
                replace_in_subject(subject),
                replace_in_named(predicate),
                replace_in_term(object, depth + 1),
            )
            .into()
        }
    }
}

fn replace_in_graph(graph: GraphName) -> GraphName {
    match graph {
        GraphName::NamedNode(n) => replace_in_named(n).into(),
        GraphName::BlankNode(b) => replace_in_blank(b).into(),
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
        let (out, note) = replace_in_quad(quad.clone());
        assert_eq!(out, quad);
        assert!(note.is_none());
    }

    #[test]
    fn nul_in_a_literal_value_is_replaced_and_the_statement_named() {
        let quad = quad_with_object(Literal::new_simple_literal("alpha\0beta"));
        let (out, note) = replace_in_quad(quad);
        assert_eq!(
            out.object,
            Term::from(Literal::new_simple_literal("alpha\u{FFFD}beta"))
        );
        assert_eq!(
            note.unwrap(),
            "U+0000 replaced with U+FFFD in the object of the statement \
             <http://example.org/s> <http://example.org/p>"
        );
    }

    #[test]
    fn every_nul_in_a_term_is_replaced() {
        let quad = quad_with_object(Literal::new_simple_literal("\0a\0\0b\0"));
        let (out, _) = replace_in_quad(quad);
        assert_eq!(
            out.object,
            Term::from(Literal::new_simple_literal(
                "\u{FFFD}a\u{FFFD}\u{FFFD}b\u{FFFD}"
            ))
        );
    }

    #[test]
    fn literal_kind_is_kept() {
        let tagged = Literal::new_language_tagged_literal_unchecked("a\0", "en");
        let (out, _) = replace_in_quad(quad_with_object(tagged));
        assert_eq!(
            out.object,
            Term::from(Literal::new_language_tagged_literal_unchecked(
                "a\u{FFFD}",
                "en"
            ))
        );

        let directional = Literal::new_directional_language_tagged_literal_unchecked(
            "a\0",
            "ar",
            BaseDirection::Rtl,
        );
        let (out, _) = replace_in_quad(quad_with_object(directional));
        assert_eq!(
            out.object,
            Term::from(Literal::new_directional_language_tagged_literal_unchecked(
                "a\u{FFFD}",
                "ar",
                BaseDirection::Rtl
            ))
        );

        let typed = Literal::new_typed_literal("1\0", named("http://example.org/dt\0"));
        let (out, _) = replace_in_quad(quad_with_object(typed));
        assert_eq!(
            out.object,
            Term::from(Literal::new_typed_literal(
                "1\u{FFFD}",
                named("http://example.org/dt\u{FFFD}")
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
        let (out, note) = replace_in_quad(quad);
        assert_eq!(
            out,
            Quad::new(
                named("http://example.org/s\u{FFFD}"),
                named("http://example.org/p\u{FFFD}"),
                BlankNode::new_unchecked("b\u{FFFD}"),
                named("http://example.org/g\u{FFFD}"),
            )
        );
        // The note names the source's terms, with the character escaped.
        assert_eq!(
            note.unwrap(),
            "U+0000 replaced with U+FFFD in the subject and predicate and object and graph name \
             of the statement <http://example.org/s\\u0000> <http://example.org/p\\u0000>"
        );
    }

    #[test]
    fn nul_inside_a_triple_term_is_replaced() {
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
        let (out, note) = replace_in_quad(quad_with_object(outer));
        let Term::Triple(outer) = out.object else {
            panic!("object is still a triple term");
        };
        let Term::Triple(inner) = outer.object else {
            panic!("nested object is still a triple term");
        };
        assert_eq!(
            inner.object,
            Term::from(Literal::new_simple_literal("c\u{FFFD}"))
        );
        assert!(note.unwrap().contains("in the object of"));
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
