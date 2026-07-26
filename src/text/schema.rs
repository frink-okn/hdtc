//! The Tantivy schema, and the tokenizer registration that must accompany it.
//!
//! `docs/text-index-format.md` §5 is normative for the field names, types and
//! options here. They are part of the published contract rather than an
//! implementation detail: a consumer opening the index directory with its own
//! Tantivy build reads documents through exactly these three fields.

use super::analyzer::{TOKENIZER_NAME, tokenizer};
use tantivy::Index;
use tantivy::schema::{FAST, IndexRecordOption, STRING, Schema, TextFieldIndexing, TextOptions};

/// The literal's lexical form, tokenized but not stemmed.
pub const FIELD_TEXT: &str = "text";
/// The same lexical form, stemmed for the literal's own language.
pub const FIELD_TEXT_STEMMED: &str = "text_stemmed";
/// The whole normalized literal as a single term, for short literals.
pub const FIELD_TEXT_EXACT: &str = "text_exact";
/// The HDT object dictionary ID this document stands for.
pub const FIELD_OBJECT: &str = "object";
/// The literal's normalized language tag, or `und` when it carries none.
pub const FIELD_LANG: &str = "lang";

/// The schema every hdtc text index is built with.
///
/// Five fields, and deliberately no stored text: the literal is already in the
/// HDT dictionary, addressed by the `object` ID, so storing it again would
/// duplicate the dataset's largest component to save one dictionary read.
///
/// The text is indexed twice, once plain and once stemmed, because the two
/// answer different questions and neither subsumes the other. Stemming alone
/// would lose exact matching — `running` and `runs` both become `run`, and
/// nothing can tell them apart afterwards. Plain alone cannot find `running`
/// from `run`. Keeping both means an exact hit always exists to be ranked
/// first, and stemming only ever adds results below it.
pub fn text_schema() -> Schema {
    let mut builder = Schema::builder();
    // Positions cost space but are what phrase queries need, and a phrase is
    // how a client asks for the `exact` match kind of doc 03 §3.4.5.
    let text_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(TOKENIZER_NAME)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    builder.add_text_field(FIELD_TEXT, text_options);
    // Written as already-tokenized values, because the stemmer depends on the
    // *document's* language and Tantivy binds one tokenizer per field. The
    // declared tokenizer is therefore `raw`: it never runs, and naming a real
    // chain here would misdescribe what is stored. Query terms are built
    // directly, so nothing analyzes this field on the read side either.
    let stemmed_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    builder.add_text_field(FIELD_TEXT_STEMMED, stemmed_options);
    // One term per document and nothing else: a query either is the whole
    // literal or is not, so there is no frequency to weigh and no position to
    // record. `STRING` is raw-tokenized and index-option Basic, which is
    // exactly that. Absent for literals over the length cap (§3.7).
    builder.add_text_field(FIELD_TEXT_EXACT, STRING);
    // FAST, not STORED: retrieving a hit's object ID is a columnar read rather
    // than a document-store fetch.
    builder.add_u64_field(FIELD_OBJECT, FAST);
    // Indexed as one raw token so a language filter is a term query. Not fast
    // and not stored, because the tag is already part of the literal's
    // dictionary bytes and comes back with it.
    builder.add_text_field(FIELD_LANG, STRING);
    builder.build()
}

/// Register the hdtc tokenizer chain on an index instance.
///
/// Tantivy stores the tokenizer *name* in the schema, not the chain itself, so
/// every `Index` — freshly created or opened from disk — needs this before it
/// indexes or parses a query. A reader that skipped it would fall back to the
/// built-in `default` chain and silently fail to match any token over 40 bytes.
pub fn register_tokenizer(index: &Index) {
    index.tokenizers().register(TOKENIZER_NAME, tokenizer());
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::schema::Type;

    #[test]
    fn the_schema_has_exactly_the_five_published_fields() {
        let schema = text_schema();
        let fields: Vec<&str> = schema.fields().map(|(_, entry)| entry.name()).collect();
        assert_eq!(
            fields,
            [
                FIELD_TEXT,
                FIELD_TEXT_STEMMED,
                FIELD_TEXT_EXACT,
                FIELD_OBJECT,
                FIELD_LANG
            ]
        );

        // One term, no frequencies, no positions: a query either is the whole
        // literal or is not.
        let exact = schema.get_field_entry(schema.get_field(FIELD_TEXT_EXACT).unwrap());
        let tantivy::schema::FieldType::Str(options) = exact.field_type() else {
            panic!("whole-literal field is not a string field");
        };
        let indexing = options.get_indexing_options().unwrap();
        assert_eq!(indexing.tokenizer(), "raw");
        assert_eq!(indexing.index_option(), IndexRecordOption::Basic);
        assert!(!exact.is_stored());

        let object = schema.get_field(FIELD_OBJECT).unwrap();
        let entry = schema.get_field_entry(object);
        assert_eq!(entry.field_type().value_type(), Type::U64);
        assert!(entry.is_fast(), "object IDs are read columnar");
        assert!(
            !entry.is_stored(),
            "nothing is stored; the HDT holds the text"
        );

        let text = schema.get_field(FIELD_TEXT).unwrap();
        assert!(!schema.get_field_entry(text).is_stored());

        // The stemmed field carries values already tokenized by hdtc, so it
        // declares `raw`: no chain must run over it on either side.
        let stemmed = schema.get_field_entry(schema.get_field(FIELD_TEXT_STEMMED).unwrap());
        let tantivy::schema::FieldType::Str(options) = stemmed.field_type() else {
            panic!("stemmed field is not a string field");
        };
        let indexing = options.get_indexing_options().unwrap();
        assert_eq!(indexing.tokenizer(), "raw");
        assert_eq!(
            indexing.index_option(),
            IndexRecordOption::WithFreqsAndPositions,
            "positions let a phrase query reach the stemmed field too"
        );
        assert!(!stemmed.is_stored());
    }

    /// The text field must name our chain, not Tantivy's `default` — the whole
    /// point of registering one.
    #[test]
    fn the_text_field_names_the_hdtc_tokenizer_with_positions() {
        let schema = text_schema();
        let entry = schema.get_field_entry(schema.get_field(FIELD_TEXT).unwrap());
        let tantivy::schema::FieldType::Str(options) = entry.field_type() else {
            panic!("text field is not a string field");
        };
        let indexing = options.get_indexing_options().unwrap();
        assert_eq!(indexing.tokenizer(), TOKENIZER_NAME);
        assert_eq!(
            indexing.index_option(),
            IndexRecordOption::WithFreqsAndPositions
        );
    }

    /// Registration is what makes a long token findable; without it the index
    /// would silently use `default`, which caps tokens at 40 bytes.
    #[test]
    fn a_registered_index_tokenizes_long_terms() {
        let index = Index::create_in_ram(text_schema());
        register_tokenizer(&index);
        let mut analyzer = index.tokenizers().get(TOKENIZER_NAME).unwrap();
        let long = "dimethylaminopropylaminopropylaminopropylamine";
        assert!(long.len() > 40);
        let mut stream = analyzer.token_stream(long);
        assert!(tantivy::tokenizer::TokenStream::advance(&mut stream));
        assert_eq!(stream.token().text, long);
    }
}
