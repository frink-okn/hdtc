//! Writing a term in the form the dictionary stores it.
//!
//! The reading direction lives in [`crate::text::analyzer::parse_literal`],
//! which splits a stored term back into its RDF parts; the two are published
//! together from [`crate::format`] because a reader that disagrees with this
//! writer looks up a term that is present and is told it is absent. Nothing
//! detects that — it is a wrong answer, not a corrupt file, so no checksum
//! catches it and the query simply returns fewer rows than the data holds.
//!
//! `docs/text-index-format.md` §3.1 is normative for the split, and this module
//! is its inverse.

/// The datatype RDF 1.1 makes implicit, and which HDT therefore omits.
pub const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

/// A literal in the form the dictionary stores it.
///
/// `value` is the *raw* lexical form: HDT stores literals unescaped, so a value
/// containing a quote is written as-is and the reader finds the closing quote
/// from the end of the term.
///
/// A literal carries at most one of a language tag and a datatype. When both
/// are supplied the language wins, because a language-tagged literal's datatype
/// is `rdf:langString` by definition and storing it would be redundant.
/// `xsd:string` is dropped for the same reason: RDF 1.1 makes `"a"` and
/// `"a"^^xsd:string` the same term, so writing the long form would put a second
/// entry in the dictionary for a term that is already there.
pub fn encode_literal(value: &str, language: Option<&str>, datatype: Option<&str>) -> String {
    match (language, datatype) {
        (Some(language), _) => format!("\"{value}\"@{language}"),
        (None, Some(datatype)) if datatype != XSD_STRING => {
            format!("\"{value}\"^^<{datatype}>")
        }
        (None, _) => format!("\"{value}\""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::analyzer::parse_literal;

    #[test]
    fn every_shape_survives_a_round_trip_through_the_reader() {
        // The pairing that matters: whatever this module writes, the split in
        // `docs/text-index-format.md` §3.1 must recover.
        let cases: [(&str, Option<&str>, Option<&str>); 6] = [
            ("plain", None, None),
            ("tagged", Some("en"), None),
            ("tagged", Some("pt-BR"), None),
            ("42", None, Some("http://www.w3.org/2001/XMLSchema#integer")),
            ("implicit", None, Some(XSD_STRING)),
            // Unescaped, so the closing quote is only findable from the end.
            ("a \" inside", None, None),
        ];

        for (value, language, datatype) in cases {
            let term = encode_literal(value, language, datatype);
            let parsed = parse_literal(term.as_bytes())
                .unwrap_or_else(|| panic!("{term} is not recognised as a literal"));
            assert_eq!(parsed.value, value.as_bytes(), "value of {term}");
            assert_eq!(
                parsed.language,
                language.map(str::as_bytes),
                "language of {term}"
            );
            let expected_datatype = datatype.filter(|d| *d != XSD_STRING);
            assert_eq!(
                parsed.datatype,
                expected_datatype.map(str::as_bytes),
                "datatype of {term}"
            );
        }
    }

    #[test]
    fn the_two_spellings_of_a_plain_literal_are_one_term() {
        assert_eq!(
            encode_literal("a", None, None),
            encode_literal("a", None, Some(XSD_STRING)),
        );
    }

    #[test]
    fn a_language_tag_displaces_a_datatype() {
        assert_eq!(
            encode_literal("a", Some("en"), Some("http://example.org/dt")),
            "\"a\"@en",
        );
    }
}
