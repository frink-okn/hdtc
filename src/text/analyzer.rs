//! What counts as an indexable literal, and how its text is tokenized.
//!
//! `docs/text-index-format.md` §3 is the normative statement of everything in
//! this file, and every index records `analyzer_id` to assert it. Two separable
//! concerns live here:
//!
//! - **RDF-side rules** — splitting a dictionary term into value, language tag
//!   and datatype, and deciding whether that literal is indexed at all. Tantivy
//!   knows nothing about RDF, so this is ours to define.
//! - **The tokenizer chains** — delegated to Tantivy and registered so that
//!   indexing and querying cannot drift apart.
//!
//! There are two chains, not one. The plain chain splits, caps and lowercases,
//! and nothing else; the stemmed chain adds the Snowball stemmer for the
//! literal's own language. They feed two separate fields, which is what lets an
//! exact match be told apart from — and ranked above — a match found only by
//! stemming. The separation makes language-aware expansion explicit: a stemmed
//! hit is never mistaken for an exact one.
//!
//! Still deliberately absent: stopwords and ASCII folding, both of which
//! discard information a client cannot recover.

use crate::hdt::reader::find_literal_boundary;
use tantivy::tokenizer::{
    Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, TextAnalyzer, Token,
    TokenStream,
};

/// The analyzer convention this module implements, recorded in every index
/// manifest. Changing any rule below is a new `analyzer_id`, not a patch.
pub const ANALYZER_ID: u32 = 1;

/// Name the tokenizer chain is registered under, and the name the schema stores
/// against the text field.
pub const TOKENIZER_NAME: &str = "hdtc";

/// Default byte cap on a literal's lexical form (§3.4 of the format document).
pub const DEFAULT_MAX_LITERAL_BYTES: usize = 4096;

/// Tokens longer than this are dropped rather than truncated.
///
/// Tantivy's own `default` chain caps at 40 bytes, which would silently discard
/// systematic chemical names and long identifiers — exactly the strings this
/// index exists to find. Dropping rather than truncating keeps a 200-character
/// sequence from matching the first 128 characters of an unrelated one.
pub const MAX_TOKEN_BYTES: usize = 128;

/// Language value stored for literals carrying no language tag.
///
/// BCP 47's registered tag for "undetermined". Every document carries a
/// language value so that an untagged literal can be asked for positively and
/// kept eligible under the language-filtering rules in §7.2 of the format
/// document.
pub const UNDETERMINED_LANGUAGE: &str = "und";

/// The tokenizer chain, built fresh for each index instance.
///
/// Tantivy stores the tokenizer *name* in the schema, not the chain, so both
/// the builder and the reader must register this under [`TOKENIZER_NAME`]
/// before use. A reader that forgot would tokenize queries with the built-in
/// `default` chain and silently fail to match long tokens.
pub fn tokenizer() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES + 1))
        .filter(LowerCaser)
        .build()
}

/// Byte cap on a whole-literal key (§3.7).
///
/// Whole-literal matching answers "which resource is *named* this", so it only
/// has to cover the strings that name things — labels, synonyms, identifiers.
/// A definition is never typed out in full as a query, and indexing every one
/// of them as a single term would grow the term dictionary by the size of the
/// corpus text for no reachable benefit.
///
/// 256 rather than something tighter because the strings people *do* paste
/// whole are sometimes long: systematic chemical names, full taxonomic labels.
/// On Ubergraph, dropping to 96 bytes saves 60 MiB of a 677 MiB index and
/// gives up exactly those. A literal over the cap is still findable — it just
/// cannot be matched as a whole.
pub const WHOLE_LITERAL_MAX_BYTES: usize = 256;

/// The whole-literal key for a token sequence, or `None` when there is none.
///
/// The key is the plain tokens joined by single spaces, so it matches on the
/// same terms the index is built from: `"BODY"`, `"body"` and `"Body."` share
/// one key, and a query for `body` finds all three. Comparing raw lexical
/// forms instead would make punctuation and case decide identity, which is
/// exactly what tokenization exists to normalize away.
///
/// `None` for an empty token sequence, or one whose key exceeds
/// [`WHOLE_LITERAL_MAX_BYTES`].
pub fn whole_literal_key<'a>(tokens: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let mut key = String::new();
    for token in tokens {
        if !key.is_empty() {
            key.push(' ');
        }
        key.push_str(token);
        if key.len() > WHOLE_LITERAL_MAX_BYTES {
            return None;
        }
    }
    (!key.is_empty()).then_some(key)
}

/// The default language assumed for literals carrying no tag.
///
/// Untagged literals are stemmed as if they were English unless a build says
/// otherwise. The alternative — leaving untagged text unstemmed because its
/// language is formally unknown — sounds cautious but produces incoherent
/// results on real data: in Ubergraph, `rdfs:label` is untagged on UBERON terms
/// and `@en` on GO terms *in the same merged graph*, so "which ontology did this
/// term come from" would decide whether a search stems. The assumption is
/// recorded in every manifest, so a consumer reads it rather than guesses, and
/// `--untagged-language` overrides it for a corpus where it is wrong.
pub const DEFAULT_UNTAGGED_LANGUAGE: &str = "en";

/// [`tokenizer`] followed by the Snowball stemmer for `language`.
///
/// Stemming folds `running` and `runs` onto `run`, so a query for one finds the
/// others. It is applied to a *separate* field rather than replacing the plain
/// tokens (`docs/text-index-format.md` §5.1): the unstemmed field is what keeps
/// exact matching possible and lets an exact hit outrank a stemmed one.
pub fn stemming_tokenizer(language: Language) -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES + 1))
        .filter(LowerCaser)
        .filter(Stemmer::new(language))
        .build()
}

/// The Snowball stemmer for a normalized BCP 47 tag, or `None` when there is
/// none for that language.
///
/// Matched on the primary subtag, so `en-gb` and `en-US` both stem as English.
/// The set is what Snowball publishes algorithms for — not a European list; it
/// includes Arabic, Russian, Greek, Turkish and Tamil. Languages with no
/// algorithm (Polish, Czech, Hindi, Hebrew) and the ones where stemming is the
/// wrong tool entirely (Chinese, Japanese, Korean, Thai, which need word
/// segmentation instead) simply get no stemmed field, and remain exactly
/// searchable.
pub fn stemmer_language(tag: &str) -> Option<Language> {
    let primary = tag.split('-').next().unwrap_or(tag);
    Some(match primary {
        "ar" => Language::Arabic,
        "da" => Language::Danish,
        "nl" => Language::Dutch,
        "en" => Language::English,
        "fi" => Language::Finnish,
        "fr" => Language::French,
        "de" => Language::German,
        "el" => Language::Greek,
        "hu" => Language::Hungarian,
        "it" => Language::Italian,
        "no" | "nb" | "nn" => Language::Norwegian,
        "pt" => Language::Portuguese,
        "ro" => Language::Romanian,
        "ru" => Language::Russian,
        "es" => Language::Spanish,
        "sv" => Language::Swedish,
        "ta" => Language::Tamil,
        "tr" => Language::Turkish,
        _ => return None,
    })
}

/// Run `analyzer` over `text` and collect the tokens it produces.
///
/// Positions and byte offsets come through unchanged, which is what lets a
/// phrase query work against the stemmed field as well as the plain one.
pub fn collect_tokens(analyzer: &mut TextAnalyzer, text: &str) -> Vec<Token> {
    let mut stream = analyzer.token_stream(text);
    let mut tokens = Vec::new();
    while stream.advance() {
        tokens.push(stream.token().clone());
    }
    tokens
}

/// An HDT literal split into its three RDF parts.
///
/// Byte slices point into the dictionary term, and `value` is the *raw* lexical
/// form as HDT stores it: unescaped, not the N-Triples escaping seen on output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedLiteral<'a> {
    pub value: &'a [u8],
    /// The language tag without its `@`, as written in the dictionary.
    pub language: Option<&'a [u8]>,
    /// The datatype IRI without its `^^<` and `>`.
    pub datatype: Option<&'a [u8]>,
}

/// Split a dictionary term into a literal's parts, or `None` when the term is
/// not a literal.
///
/// HDT writes IRIs as their raw bytes and blank nodes as `_:…`, so a leading
/// quote is what identifies a literal.
pub fn parse_literal(term: &[u8]) -> Option<ParsedLiteral<'_>> {
    if !term.starts_with(b"\"") {
        return None;
    }
    let (value_end, suffix_start) = find_literal_boundary(term);
    if value_end < 1 {
        return None;
    }
    let value = &term[1..value_end];
    let suffix = &term[suffix_start.min(term.len())..];

    let (language, datatype) = if let Some(tag) = suffix.strip_prefix(b"@") {
        (Some(tag), None)
    } else if let Some(rest) = suffix.strip_prefix(b"^^<") {
        (None, rest.strip_suffix(b">"))
    } else {
        (None, None)
    };

    Some(ParsedLiteral {
        value,
        language,
        datatype,
    })
}

/// Normalize a language tag for the index's language field.
///
/// BCP 47 tags are case-insensitive, so they are lowercased; comparison is then
/// plain byte equality both here and in [`language_matches`].
pub fn normalize_language(tag: &[u8]) -> String {
    String::from_utf8_lossy(tag).to_lowercase()
}

/// BCP 47 basic filtering (RFC 4647 §3.3.1): does `tag` fall under `range`?
///
/// `en` matches `en` and `en-gb` but not `english`. Both arguments must already
/// be normalized. Callers can preserve request order by testing ranges in that
/// order.
pub fn language_matches(range: &str, tag: &str) -> bool {
    if range == "*" {
        return true;
    }
    tag == range
        || (tag.len() > range.len()
            && tag.starts_with(range)
            && tag.as_bytes()[range.len()] == b'-')
}

/// Datatypes excluded from the text index by default (§3.5 of the format
/// document).
///
/// These are the XSD datatypes with an ordered value space — numbers, dates,
/// durations, binary blobs — plus GeoSPARQL WKT geometry. Indexing their lexical
/// forms as text produces tokens nobody searches for and duplicates what range
/// and spatial indexes do properly. Everything else is indexed, including
/// `xsd:string`, `rdf:langString`, `xsd:anyURI` and the string-derived types,
/// which preserves the format's exhaustive-by-default stance.
pub const DEFAULT_EXCLUDED_DATATYPES: &[&str] = &[
    "http://www.opengis.net/ont/geosparql#wktLiteral",
    "http://www.w3.org/2001/XMLSchema#base64Binary",
    "http://www.w3.org/2001/XMLSchema#boolean",
    "http://www.w3.org/2001/XMLSchema#byte",
    "http://www.w3.org/2001/XMLSchema#date",
    "http://www.w3.org/2001/XMLSchema#dateTime",
    "http://www.w3.org/2001/XMLSchema#dateTimeStamp",
    "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
    "http://www.w3.org/2001/XMLSchema#decimal",
    "http://www.w3.org/2001/XMLSchema#double",
    "http://www.w3.org/2001/XMLSchema#duration",
    "http://www.w3.org/2001/XMLSchema#float",
    "http://www.w3.org/2001/XMLSchema#gDay",
    "http://www.w3.org/2001/XMLSchema#gMonth",
    "http://www.w3.org/2001/XMLSchema#gMonthDay",
    "http://www.w3.org/2001/XMLSchema#gYear",
    "http://www.w3.org/2001/XMLSchema#gYearMonth",
    "http://www.w3.org/2001/XMLSchema#hexBinary",
    "http://www.w3.org/2001/XMLSchema#int",
    "http://www.w3.org/2001/XMLSchema#integer",
    "http://www.w3.org/2001/XMLSchema#long",
    "http://www.w3.org/2001/XMLSchema#negativeInteger",
    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
    "http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
    "http://www.w3.org/2001/XMLSchema#positiveInteger",
    "http://www.w3.org/2001/XMLSchema#short",
    "http://www.w3.org/2001/XMLSchema#time",
    "http://www.w3.org/2001/XMLSchema#unsignedByte",
    "http://www.w3.org/2001/XMLSchema#unsignedInt",
    "http://www.w3.org/2001/XMLSchema#unsignedLong",
    "http://www.w3.org/2001/XMLSchema#unsignedShort",
    "http://www.w3.org/2001/XMLSchema#yearMonthDuration",
];

/// The datatype exclusion set of one build, sorted for binary search and for
/// the deterministic order it is written to the manifest in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DatatypeExclusions {
    iris: Vec<String>,
}

impl DatatypeExclusions {
    /// The default set of [`DEFAULT_EXCLUDED_DATATYPES`] plus `extra`.
    pub fn with_defaults(extra: &[String]) -> Self {
        let mut iris: Vec<String> = DEFAULT_EXCLUDED_DATATYPES
            .iter()
            .map(|iri| (*iri).to_string())
            .collect();
        iris.extend(extra.iter().cloned());
        Self::from_iris(iris)
    }

    /// Exactly `iris`, with no defaults — how `--index-all-datatypes` (an empty
    /// list) and a manifest read back from disk are built.
    pub fn from_iris(mut iris: Vec<String>) -> Self {
        iris.sort_unstable();
        iris.dedup();
        Self { iris }
    }

    pub fn iris(&self) -> &[String] {
        &self.iris
    }

    pub fn contains(&self, datatype: &[u8]) -> bool {
        let Ok(datatype) = std::str::from_utf8(datatype) else {
            return false;
        };
        self.iris
            .binary_search_by(|iri| iri.as_str().cmp(datatype))
            .is_ok()
    }
}

/// Why a literal was not indexed, or `None` when it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exclusion {
    /// The lexical form is longer than the build's byte cap.
    Oversize,
    /// The datatype is in the build's exclusion set.
    Datatype,
    /// The lexical form holds no alphanumeric character, so the tokenizer would
    /// produce no tokens and the document could never be retrieved.
    NoTokens,
}

/// Decide whether a parsed literal is indexed, and why not when it is not.
///
/// The `NoTokens` test is the cheap characterization of "[`SimpleTokenizer`]
/// emits at least one token", not a second tokenization pass: a literal whose
/// every token exceeds [`MAX_TOKEN_BYTES`] still gets indexed, as an empty
/// document that matches nothing. `docs/text-index-format.md` §3.4 states this
/// so a manifest's counts are read for what they are.
pub fn classify(
    literal: &ParsedLiteral<'_>,
    max_literal_bytes: usize,
    exclusions: &DatatypeExclusions,
) -> Option<Exclusion> {
    if let Some(datatype) = literal.datatype
        && exclusions.contains(datatype)
    {
        return Some(Exclusion::Datatype);
    }
    if literal.value.len() > max_literal_bytes {
        return Some(Exclusion::Oversize);
    }
    if !has_alphanumeric(literal.value) {
        return Some(Exclusion::NoTokens);
    }
    None
}

/// Whether any character of a possibly-malformed UTF-8 byte string is
/// alphanumeric.
fn has_alphanumeric(value: &[u8]) -> bool {
    // ASCII is the overwhelmingly common case and needs no decoding at all.
    if value.is_ascii() {
        return value.iter().any(u8::is_ascii_alphanumeric);
    }
    String::from_utf8_lossy(value)
        .chars()
        .any(char::is_alphanumeric)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::tokenizer::TokenStream;

    fn tokens(value: &str) -> Vec<String> {
        let mut analyzer = tokenizer();
        let mut stream = analyzer.token_stream(value);
        let mut out = Vec::new();
        while stream.advance() {
            out.push(stream.token().text.clone());
        }
        out
    }

    #[test]
    fn literals_split_into_value_language_and_datatype() {
        assert_eq!(
            parse_literal(b"\"Atrazine\""),
            Some(ParsedLiteral {
                value: b"Atrazine",
                language: None,
                datatype: None
            })
        );
        assert_eq!(
            parse_literal(b"\"Atrazine\"@en-GB"),
            Some(ParsedLiteral {
                value: b"Atrazine",
                language: Some(b"en-GB"),
                datatype: None
            })
        );
        assert_eq!(
            parse_literal(b"\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            Some(ParsedLiteral {
                value: b"42",
                language: None,
                datatype: Some(b"http://www.w3.org/2001/XMLSchema#integer".as_slice())
            })
        );
        // A quote inside the value does not end it.
        assert_eq!(
            parse_literal(b"\"say \"hi\"\"@en").map(|literal| literal.value),
            Some(b"say \"hi\"".as_slice())
        );
        assert_eq!(parse_literal(b"http://example.org/a"), None);
        assert_eq!(parse_literal(b"_:b1"), None);
    }

    /// The chain is three links, and each one is observable: split on
    /// non-alphanumerics, drop over-long tokens, lowercase.
    #[test]
    fn the_tokenizer_chain_splits_caps_and_lowercases() {
        assert_eq!(tokens("Atrazine degradation"), ["atrazine", "degradation"]);
        assert_eq!(
            tokens("1,3-dichlorobenzene (CAS 541-73-1)"),
            ["1", "3", "dichlorobenzene", "cas", "541", "73", "1"]
        );
        assert_eq!(tokens("   "), Vec::<String>::new());
        assert_eq!(tokens("ÉTÉ Straße"), ["été", "straße"]);
        // Repetition is what term frequency is made of, so it is preserved.
        assert_eq!(tokens("cat cat dog"), ["cat", "cat", "dog"]);
    }

    /// Tantivy's built-in `default` chain would cut this at 40 bytes; ours
    /// keeps it, which is the reason for a custom chain at all.
    #[test]
    fn long_chemical_names_survive_and_only_the_extreme_is_dropped() {
        let name = "dimethylaminopropylaminopropylaminopropylamine";
        assert!(name.len() > 40);
        assert_eq!(tokens(name), [name]);

        let at_limit = "a".repeat(MAX_TOKEN_BYTES);
        assert_eq!(tokens(&at_limit), [at_limit]);
        let over_limit = "a".repeat(MAX_TOKEN_BYTES + 1);
        assert_eq!(tokens(&format!("gene {over_limit} end")), ["gene", "end"]);
    }

    /// The stemmed chain is the plain chain plus a Snowball stemmer, and it is
    /// chosen per language — English must not stem German, or the recall it
    /// buys comes with matches nobody asked for.
    #[test]
    fn stemming_folds_word_forms_in_the_right_language() {
        let stem = |language, text: &str| {
            let mut analyzer = stemming_tokenizer(language);
            collect_tokens(&mut analyzer, text)
                .into_iter()
                .map(|token| token.text)
                .collect::<Vec<_>>()
        };

        assert_eq!(
            stem(Language::English, "running runs runner"),
            ["run", "run", "runner"]
        );
        assert_eq!(stem(Language::English, "processes"), ["process"]);
        assert_eq!(stem(Language::German, "laufen Läufer"), ["lauf", "lauf"]);
        // Suffix stripping, not lemmatization: an irregular form stays put, and
        // the specification says so rather than implying otherwise.
        assert_eq!(stem(Language::English, "ran mice"), ["ran", "mice"]);
        // The German stemmer must not be the English one.
        assert_ne!(
            stem(Language::English, "laufen"),
            stem(Language::German, "laufen")
        );
    }

    /// Positions and offsets survive stemming, which is what lets a phrase
    /// query work against the stemmed field as well as the plain one.
    #[test]
    fn stemmed_tokens_keep_their_positions() {
        let mut analyzer = stemming_tokenizer(Language::English);
        let tokens = collect_tokens(&mut analyzer, "atrazine degradation pathways");
        let positions: Vec<usize> = tokens.iter().map(|token| token.position).collect();
        assert_eq!(positions, [0, 1, 2]);
        assert_eq!(tokens[2].text, "pathway");
        assert_eq!(
            &"atrazine degradation pathways"[tokens[2].offset_from..tokens[2].offset_to],
            "pathways"
        );
    }

    /// Stemmers are selected by primary subtag, and a language Snowball has no
    /// algorithm for gets none rather than a wrong one.
    #[test]
    fn stemmer_selection_falls_back_to_none_rather_than_to_english() {
        assert_eq!(stemmer_language("en"), Some(Language::English));
        assert_eq!(stemmer_language("en-gb"), Some(Language::English));
        assert_eq!(stemmer_language("de-AT"), Some(Language::German));
        assert_eq!(stemmer_language("nb"), Some(Language::Norwegian));
        // Not a European list: Snowball covers these too.
        assert_eq!(stemmer_language("ar"), Some(Language::Arabic));
        assert_eq!(stemmer_language("ta"), Some(Language::Tamil));
        // No algorithm — and, for CJK, stemming would be the wrong tool anyway.
        assert_eq!(stemmer_language("ja"), None);
        assert_eq!(stemmer_language("zh"), None);
        assert_eq!(stemmer_language("pl"), None);
        assert_eq!(stemmer_language(UNDETERMINED_LANGUAGE), None);
        assert_eq!(stemmer_language(""), None);
    }

    #[test]
    fn language_ranges_match_by_subtag_boundary() {
        assert!(language_matches("en", "en"));
        assert!(language_matches("en", "en-gb"));
        assert!(!language_matches("en", "english"));
        assert!(!language_matches("en-gb", "en"));
        assert!(language_matches("*", "de"));
        assert_eq!(normalize_language(b"en-GB"), "en-gb");
    }

    #[test]
    fn value_space_datatypes_are_excluded_and_string_ones_are_not() {
        let exclusions = DatatypeExclusions::with_defaults(&[]);
        assert!(exclusions.contains(b"http://www.opengis.net/ont/geosparql#wktLiteral"));
        assert!(exclusions.contains(b"http://www.w3.org/2001/XMLSchema#integer"));
        assert!(exclusions.contains(b"http://www.w3.org/2001/XMLSchema#dateTime"));
        assert!(!exclusions.contains(b"http://www.w3.org/2001/XMLSchema#string"));
        assert!(!exclusions.contains(b"http://www.w3.org/2001/XMLSchema#anyURI"));
        assert!(!exclusions.contains(b"http://example.org/custom"));

        let extended =
            DatatypeExclusions::with_defaults(&["http://example.org/custom".to_string()]);
        assert!(extended.contains(b"http://example.org/custom"));
        assert!(DatatypeExclusions::from_iris(Vec::new()).iris().is_empty());
    }

    #[test]
    fn classification_reports_the_first_reason_a_literal_is_skipped() {
        let exclusions = DatatypeExclusions::with_defaults(&[]);
        let classify_term =
            |term: &[u8], cap: usize| classify(&parse_literal(term).unwrap(), cap, &exclusions);

        assert_eq!(classify_term(b"\"atrazine\"@en", 4096), None);
        assert_eq!(
            classify_term(b"\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>", 4096),
            Some(Exclusion::Datatype)
        );
        assert_eq!(classify_term(b"\"atrazine\"", 4), Some(Exclusion::Oversize));
        assert_eq!(
            classify_term(b"\"--- ---\"", 4096),
            Some(Exclusion::NoTokens)
        );
        assert_eq!(classify_term(b"\"\"", 4096), Some(Exclusion::NoTokens));
        // Non-ASCII letters are alphanumeric and take the decoding path.
        assert_eq!(classify_term("\"日本語\"".as_bytes(), 4096), None);
        // An excluded datatype outranks the byte cap: the count a manifest
        // reports should say the datatype was the reason.
        assert_eq!(
            classify_term(b"\"12345\"^^<http://www.w3.org/2001/XMLSchema#integer>", 2),
            Some(Exclusion::Datatype)
        );
        assert_eq!(
            classify_term(
                b"\"Point(78.0419 27.175)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                4096
            ),
            Some(Exclusion::Datatype)
        );
    }
}
