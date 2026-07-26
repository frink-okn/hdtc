//! End-to-end tests for `hdtc text` and `hdtc search --text`.
//!
//! The claims under test are the ones `docs/text-index-format.md` makes to a
//! consumer: what the manifest says was indexed and left out (§3.4, §4), how
//! results rank and deduplicate (§6, §7.4), and that the filters mean what §7
//! says they mean.

mod common;

use common::{run_hdtc_to_path, run_hdtc_to_path_with_args, write_file};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// A small corpus with one of everything the exclusion and language rules care
/// about: long and short literals on the same subject, a language pair, two
/// untagged literals, an excluded datatype, an oversize value, and a literal
/// with no alphanumeric character at all.
const TEXT_NT: &str = r#"<http://example.org/chebi/38769> <http://www.w3.org/2000/01/rdf-schema#label> "atrazine"@en .
<http://example.org/chebi/38769> <http://www.w3.org/2004/02/skos/core#altLabel> "Atrazin"@de .
<http://example.org/chebi/38769> <http://www.w3.org/2000/01/rdf-schema#comment> "Atrazine is a widely used herbicide whose degradation in soil has been studied across many field trials and laboratory experiments over several decades of work."@en .
<http://example.org/chebi/38769> <http://example.org/formula> "C8H14ClN5" .
<http://example.org/chebi/38769> <http://example.org/mass> "215.68"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.org/gene/1> <http://www.w3.org/2000/01/rdf-schema#label> "atrazine degradation pathway"@en .
<http://example.org/gene/2> <http://www.w3.org/2000/01/rdf-schema#label> "Atrazine chlorohydrolase"@en .
<http://example.org/gene/2> <http://example.org/synonym> "atzA" .
<http://example.org/other/1> <http://www.w3.org/2000/01/rdf-schema#label> "degradation of pesticides"@en .
<http://example.org/other/2> <http://www.w3.org/2000/01/rdf-schema#label> "Pflanzenschutzmittel"@de .
<http://example.org/other/3> <http://www.w3.org/2000/01/rdf-schema#label> "- - -" .
<http://example.org/other/4> <http://example.org/created> "2024-01-01"^^<http://www.w3.org/2001/XMLSchema#date> .
"#;

/// Word forms that only match once stemming is applied, in three languages —
/// one of them untagged, which is how OBO ontologies write labels.
const STEMMING_NT: &str = r#"<http://example.org/a> <http://example.org/label> "running quickly" .
<http://example.org/b> <http://example.org/label> "biological processes"@en .
<http://example.org/d> <http://example.org/label> "Läufer laufen"@de .
<http://example.org/f> <http://example.org/label> "run" .
"#;

const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
const RDFS_COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn hdtc(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(args)
        .output()
        .expect("run hdtc")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// Build the fixture HDT, its HDT-FoQ index, and its text index.
fn fixture(temp: &Path) -> PathBuf {
    let input = temp.join("input.nt");
    write_file(&input, TEXT_NT.as_bytes());
    let hdt = run_hdtc_to_path_with_args(temp, &[&input], "data.hdt", &["--index"]);
    build_text(&hdt, &[]);
    hdt
}

fn build_text(hdt: &Path, args: &[&str]) -> Output {
    let output = hdtc(&[&["text", hdt.to_str().unwrap()], args].concat());
    assert!(
        output.status.success(),
        "hdtc text failed: {}",
        stderr(&output)
    );
    output
}

fn text_index_dir(hdt: &Path) -> PathBuf {
    PathBuf::from(format!("{}.text", hdt.display()))
}

/// The manifest as key → list of records, since several keys repeat.
fn manifest(hdt: &Path) -> HashMap<String, Vec<Vec<String>>> {
    let path = text_index_dir(hdt).join("hdtc-text.meta");
    let text = std::fs::read_to_string(&path).expect("read manifest");
    let mut records: HashMap<String, Vec<Vec<String>>> = HashMap::new();
    for line in text.lines().filter(|line| !line.is_empty()) {
        let mut fields = line.split('\t');
        let key = fields.next().unwrap().to_string();
        records
            .entry(key)
            .or_default()
            .push(fields.map(str::to_string).collect());
    }
    records
}

fn manifest_value(hdt: &Path, key: &str) -> String {
    manifest(hdt)
        .get(key)
        .unwrap_or_else(|| panic!("manifest has no {key} line"))[0][0]
        .clone()
}

fn search(hdt: &Path, args: &[&str]) -> Output {
    hdtc(&[&["search", hdt.to_str().unwrap()], args].concat())
}

/// Result rows as `(subject, predicate, object)`, in the order emitted.
fn rows(hdt: &Path, args: &[&str]) -> Vec<(String, String, String)> {
    let output = search(hdt, args);
    assert!(
        output.status.success(),
        "search failed: {}",
        stderr(&output)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let fields: Vec<&str> = line.split('\t').collect();
            assert_eq!(fields.len(), 4, "expected S\\tP\\tO\\t. in {line:?}");
            assert_eq!(fields[3], ".");
            (
                fields[0].to_string(),
                fields[1].to_string(),
                fields[2].to_string(),
            )
        })
        .collect()
}

fn subjects(hdt: &Path, args: &[&str]) -> Vec<String> {
    rows(hdt, args)
        .into_iter()
        .map(|(subject, _, _)| subject)
        .collect()
}

fn objects(hdt: &Path, args: &[&str]) -> Vec<String> {
    rows(hdt, args)
        .into_iter()
        .map(|(_, _, object)| object)
        .collect()
}

fn iri(suffix: &str) -> String {
    format!("<http://example.org/{suffix}>")
}

// ---------------------------------------------------------------------------
// Building
// ---------------------------------------------------------------------------

/// §4: every literal is accounted for, under exactly one reason.
#[test]
fn the_manifest_accounts_for_every_literal() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    assert_eq!(manifest_value(&hdt, "hdtc-text"), "1");
    assert_eq!(manifest_value(&hdt, "analyzer"), "3");
    assert_eq!(
        manifest_value(&hdt, "untagged_language"),
        "en",
        "§3.6: the assumption made about untagged literals is recorded"
    );
    assert_eq!(manifest_value(&hdt, "tantivy"), "0.26.1");
    assert_eq!(manifest_value(&hdt, "max_literal_bytes"), "4096");

    let scanned: u64 = manifest_value(&hdt, "literals_scanned").parse().unwrap();
    let indexed: u64 = manifest_value(&hdt, "indexed_docs").parse().unwrap();
    let oversize: u64 = manifest_value(&hdt, "excluded_oversize").parse().unwrap();
    let datatype: u64 = manifest_value(&hdt, "excluded_datatype").parse().unwrap();
    let no_tokens: u64 = manifest_value(&hdt, "excluded_no_tokens").parse().unwrap();

    assert_eq!(scanned, 12, "distinct literals in the fixture");
    assert_eq!(datatype, 2, "the xsd:double and the xsd:date");
    assert_eq!(no_tokens, 1, "\"- - -\" holds no alphanumeric character");
    assert_eq!(oversize, 0);
    assert_eq!(
        scanned,
        indexed + oversize + datatype + no_tokens,
        "§4: the counts partition the scanned literals"
    );

    // §3.7: whole-literal coverage is published, since a literal over the cap
    // is findable but cannot be matched as a whole.
    let whole: u64 = manifest_value(&hdt, "whole_literal_keys").parse().unwrap();
    assert!(whole <= indexed);
    assert_eq!(whole, indexed, "every fixture literal is short enough");

    // §4: the language counts sum to the document count, and untagged literals
    // are counted under `und`.
    let languages: HashMap<String, u64> = manifest(&hdt)["language"]
        .iter()
        .map(|fields| (fields[0].clone(), fields[1].parse().unwrap()))
        .collect();
    assert_eq!(languages["en"], 5);
    assert_eq!(languages["de"], 2);
    assert_eq!(languages["und"], 2, "\"C8H14ClN5\" and \"atzA\"");
    assert_eq!(languages.values().sum::<u64>(), indexed);

    // §3.5: the datatype set is published in full, not assumed.
    let excluded: Vec<String> = manifest(&hdt)["excluded_datatype_iri"]
        .iter()
        .map(|fields| fields[0].clone())
        .collect();
    assert!(excluded.contains(&XSD_DOUBLE.to_string()));
    assert!(!excluded.iter().any(|iri| iri.ends_with("#string")));
    let mut sorted = excluded.clone();
    sorted.sort();
    assert_eq!(excluded, sorted, "§4: repeated keys are written ascending");
}

/// §3.4: the byte cap and the datatype set are per-build, and the manifest says
/// which ones a given index used.
#[test]
fn exclusions_are_configurable_and_recorded() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nt");
    write_file(&input, TEXT_NT.as_bytes());

    let capped = run_hdtc_to_path(temp.path(), &[&input], "capped.hdt");
    build_text(&capped, &["--max-literal-bytes", "20"]);
    assert_eq!(manifest_value(&capped, "max_literal_bytes"), "20");
    assert_eq!(
        manifest_value(&capped, "excluded_oversize"),
        "4",
        "the comment and the three labels over 20 bytes"
    );
    // The long comment is gone; the short label is still there.
    assert!(objects(&capped, &["--text", "herbicide", "--no-index"]).is_empty());
    assert!(
        objects(&capped, &["--text", "atrazine", "--no-index"])
            .contains(&r#""atrazine"@en"#.to_string()),
        "the short exact label remains indexed"
    );

    let everything = run_hdtc_to_path(temp.path(), &[&input], "everything.hdt");
    build_text(&everything, &["--index-all-datatypes"]);
    assert_eq!(manifest_value(&everything, "excluded_datatype"), "0");
    assert!(!manifest(&everything).contains_key("excluded_datatype_iri"));
    assert_eq!(
        objects(&everything, &["--text", "2024", "--no-index"]),
        [format!(
            r#""2024-01-01"^^<{}>"#,
            "http://www.w3.org/2001/XMLSchema#date"
        )],
        "an indexed date is findable by its digits"
    );

    let extra = run_hdtc_to_path(temp.path(), &[&input], "extra.hdt");
    build_text(&extra, &["--exclude-datatype", "http://example.org/custom"]);
    let excluded: Vec<String> = manifest(&extra)["excluded_datatype_iri"]
        .iter()
        .map(|fields| fields[0].clone())
        .collect();
    assert!(excluded.contains(&"http://example.org/custom".to_string()));
    assert!(
        excluded.contains(&XSD_DOUBLE.to_string()),
        "--exclude-datatype adds to the defaults rather than replacing them"
    );
}

/// A dataset with no literals is a legitimate input, and the artifact it
/// produces has to be openable rather than a special case at query time.
#[test]
fn an_hdt_with_no_literals_builds_an_empty_index() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nt");
    write_file(
        &input,
        b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .\n",
    );
    let hdt = run_hdtc_to_path(temp.path(), &[&input], "data.hdt");
    build_text(&hdt, &[]);

    assert_eq!(manifest_value(&hdt, "literals_scanned"), "0");
    assert_eq!(manifest_value(&hdt, "indexed_docs"), "0");
    assert!(!manifest(&hdt).contains_key("language"));

    let output = search(&hdt, &["--text", "anything", "--count", "--no-index"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "0");
}

/// Publication is all-or-nothing: an existing index is never replaced, and a
/// failed build leaves no directory behind under the published name.
#[test]
fn an_existing_index_is_never_replaced() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    let output = hdtc(&["text", hdt.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("Refusing to replace"),
        "{}",
        stderr(&output)
    );

    // The first index is still intact and queryable.
    assert_eq!(manifest_value(&hdt, "indexed_docs"), "9");
}

#[test]
fn a_missing_hdt_is_reported_before_anything_is_written() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("nope.hdt");
    let output = hdtc(&["text", missing.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(stderr(&output).contains("not found"), "{}", stderr(&output));
    assert!(!text_index_dir(&missing).exists());
}

// ---------------------------------------------------------------------------
// Ranking and result shape
// ---------------------------------------------------------------------------

/// §3.3 and §6: BM25's length normalization is what makes a short label outrank
/// a long comment, with no predicate configured anywhere. This is the claim the
/// whole exhaustive-indexing design rests on.
#[test]
fn a_short_label_outranks_a_long_comment_without_any_field_configuration() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    let found = objects(&hdt, &["--text", "atrazine"]);
    assert_eq!(found[0], r#""atrazine"@en"#);
    let comment_rank = found
        .iter()
        .position(|object| object.contains("herbicide"))
        .expect("the comment matches too");
    assert!(
        comment_rank > 0,
        "the comment should rank below the labels: {found:?}"
    );
}

/// §7.4: every matching RDF statement is emitted, even when several have the
/// same subject.
#[test]
fn every_matching_triple_is_emitted() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    let found = rows(&hdt, &["--text", "atrazine"]);
    let chebi_predicates: Vec<&str> = found
        .iter()
        .filter(|(subject, _, _)| *subject == iri("chebi/38769"))
        .map(|(_, predicate, _)| predicate.as_str())
        .collect();
    assert!(chebi_predicates.contains(&format!("<{RDFS_LABEL}>").as_str()));
    assert!(chebi_predicates.contains(&format!("<{RDFS_COMMENT}>").as_str()));
}

/// §7.4: identical calls return identical pages. A ranking that varies between
/// runs is unusable for caching or for diffing responses.
#[test]
fn repeated_queries_return_the_same_page() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());
    let first = rows(
        &hdt,
        &["--text", "atrazine degradation", "--text-match", "any"],
    );
    for _ in 0..3 {
        assert_eq!(
            rows(
                &hdt,
                &["--text", "atrazine degradation", "--text-match", "any"]
            ),
            first
        );
    }
}

/// The page controls compose with ranking: `--limit` takes a prefix of the
/// ranking and `--offset` skips within it.
#[test]
fn limit_and_offset_page_through_the_ranking() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    let all = subjects(&hdt, &["--text", "atrazine"]);
    assert!(all.len() >= 3, "{all:?}");
    assert_eq!(
        subjects(&hdt, &["--text", "atrazine", "--limit", "1"]),
        all[..1]
    );
    assert_eq!(
        subjects(&hdt, &["--text", "atrazine", "--limit", "2"]),
        all[..2]
    );
    assert_eq!(
        subjects(
            &hdt,
            &["--text", "atrazine", "--limit", "1", "--offset", "1"]
        ),
        all[1..2]
    );
    // Past the end is empty, not an error.
    assert!(
        subjects(
            &hdt,
            &["--text", "atrazine", "--limit", "5", "--offset", "99"]
        )
        .is_empty()
    );

    let output = search(&hdt, &["--text", "atrazine", "--count"]);
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        all.len().to_string(),
        "--count counts the same rows the unlimited query emits"
    );
}

#[test]
fn scores_are_emitted_as_trailing_comments_and_remain_valid_ntriples() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    let output = search(&hdt, &["--text", "atrazine", "--scores"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines().filter(|line| !line.is_empty()) {
        let (triple, score) = line
            .rsplit_once(" # score=")
            .expect("trailing score comment");
        let fields: Vec<&str> = triple.split('\t').collect();
        assert_eq!(fields.len(), 4, "S\\tP\\tO\\t. before the comment");
        assert_eq!(fields[3], ".");
        let score: f64 = score.parse().expect("numeric score in comment");
        assert!(score.is_finite() && score > 0.0);
    }

    // An RDF parser must ignore the comments and accept the output directly.
    let scored_nt = temp.path().join("scored.nt");
    write_file(&scored_nt, &output.stdout);
    run_hdtc_to_path(temp.path(), &[&scored_nt], "scored.hdt");
}

// ---------------------------------------------------------------------------
// Query semantics
// ---------------------------------------------------------------------------

/// §7.1: the three ways query tokens combine.
#[test]
fn match_modes_mean_what_the_specification_says() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    // all: both tokens must be present.
    let all = subjects(&hdt, &["--text", "atrazine degradation"]);
    assert!(all.contains(&iri("gene/1")), "{all:?}");
    assert!(
        !all.contains(&iri("other/1")),
        "\"degradation of pesticides\" lacks \"atrazine\": {all:?}"
    );

    // any: either token is enough.
    let any = subjects(
        &hdt,
        &["--text", "atrazine degradation", "--text-match", "any"],
    );
    assert!(any.contains(&iri("other/1")), "{any:?}");
    assert!(any.len() > all.len());

    // phrase: adjacent and in order.
    let phrase = subjects(
        &hdt,
        &["--text", "atrazine degradation", "--text-match", "phrase"],
    );
    assert_eq!(phrase, [iri("gene/1")]);
    assert!(
        subjects(
            &hdt,
            &["--text", "degradation atrazine", "--text-match", "phrase"]
        )
        .is_empty(),
        "order matters in a phrase"
    );
}

/// §7.1 and §6: approximate matching finds what exact matching cannot, and an
/// exact match still outranks an approximate one.
#[test]
fn fuzzy_and_prefix_widen_the_query() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    assert!(
        subjects(&hdt, &["--text", "atrasine"]).is_empty(),
        "a typo matches nothing exactly"
    );
    assert!(!subjects(&hdt, &["--text", "atrasine", "--fuzzy", "1"]).is_empty());

    assert!(
        subjects(&hdt, &["--text", "atraz"]).is_empty(),
        "a prefix is not a token"
    );
    let prefixed = objects(&hdt, &["--text", "atraz", "--prefix"]);
    assert!(
        prefixed
            .iter()
            .any(|object| object.contains("Atrazin\"@de")),
        "{prefixed:?}"
    );

    // "atrazin" is an exact token of the German label and a prefix of the
    // English ones; §6 requires the exact match to lead.
    let mixed = objects(&hdt, &["--text", "atrazin", "--prefix"]);
    assert!(mixed[0].contains("Atrazin\"@de"), "{mixed:?}");
    assert!(mixed.len() > 1, "prefix matches follow: {mixed:?}");
}

/// §3.6: stemming finds the other forms of a word, in the language the literal
/// declares — and in the assumed language when it declares none.
#[test]
fn stemming_finds_other_word_forms_including_in_untagged_literals() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nt");
    write_file(&input, STEMMING_NT.as_bytes());
    let hdt = run_hdtc_to_path_with_args(temp.path(), &[&input], "data.hdt", &["--index"]);
    build_text(&hdt, &[]);

    // The case that motivated the whole feature: an *untagged* literal, which
    // is how OBO ontologies write labels, still stems.
    let found = objects(&hdt, &["--text", "run"]);
    assert!(
        found.contains(&r#""running quickly""#.to_string()),
        "untagged text must stem: {found:?}"
    );

    // Tagged literals stem in their own language, not English.
    assert_eq!(
        objects(&hdt, &["--text", "laufen"]),
        [r#""Läufer laufen"@de"#]
    );
    assert_eq!(
        objects(&hdt, &["--text", "process"]),
        [r#""biological processes"@en"#]
    );

    // §6: an exact hit outranks a stemmed one, as a class.
    let ranked = objects(&hdt, &["--text", "run"]);
    assert_eq!(ranked[0], r#""run""#, "exact first: {ranked:?}");
    assert!(ranked.len() > 1, "the stemmed hit follows: {ranked:?}");

    // Stemming is suffix stripping, not lemmatization: irregular forms do not
    // fold together, and the spec says so rather than implying otherwise.
    assert!(objects(&hdt, &["--text", "ran"]).is_empty());
}

/// §3.6: the assumption about untagged literals is a build choice, and turning
/// it off leaves them exactly searchable rather than unsearchable.
#[test]
fn the_untagged_language_is_configurable_and_recorded() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nt");
    write_file(&input, STEMMING_NT.as_bytes());

    let none = run_hdtc_to_path(temp.path(), &[&input], "none.hdt");
    build_text(&none, &["--untagged-language", "none"]);
    assert_eq!(manifest_value(&none, "untagged_language"), "none");
    assert!(
        objects(&none, &["--text", "run", "--no-index"])
            .iter()
            .all(|object| object == r#""run""#),
        "untagged literals are no longer stemmed"
    );
    assert_eq!(
        objects(&none, &["--text", "running", "--no-index"]),
        [r#""running quickly""#],
        "but they remain exactly searchable"
    );
    // A tagged literal still stems: the flag governs only the untagged ones.
    assert_eq!(
        objects(&none, &["--text", "process", "--no-index"]),
        [r#""biological processes"@en"#]
    );

    let german = run_hdtc_to_path(temp.path(), &[&input], "de.hdt");
    build_text(&german, &["--untagged-language", "de"]);
    assert_eq!(manifest_value(&german, "untagged_language"), "de");

    // A language Snowball has no algorithm for is refused at build time, with
    // the supported set named, rather than silently ignored.
    let bad = run_hdtc_to_path(temp.path(), &[&input], "bad.hdt");
    let output = hdtc(&["text", bad.to_str().unwrap(), "--untagged-language", "zz"]);
    assert!(!output.status.success());
    let message = stderr(&output);
    assert!(
        message.contains("No stemmer") && message.contains("none"),
        "{message}"
    );
    assert!(!text_index_dir(&bad).exists(), "nothing is published");
}

/// §7.2: language ranges filter by basic filtering, and untagged literals stay
/// eligible throughout.
#[test]
fn language_filtering_keeps_untagged_literals_eligible() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    assert_eq!(
        objects(&hdt, &["--text", "atrazin", "--lang", "de"]),
        [r#""Atrazin"@de"#]
    );
    // "atrazin" is what English stemming makes of "atrazine", so under an
    // English filter it now reaches the English labels through the stemmed
    // field — the recall stemming exists to buy. A token no English literal
    // holds in any form still returns nothing.
    assert!(!objects(&hdt, &["--text", "atrazin", "--lang", "en"]).is_empty());
    assert!(
        objects(&hdt, &["--text", "pflanzenschutzmittel", "--lang", "en"]).is_empty(),
        "no English literal holds this token, stemmed or not"
    );

    // An untagged literal is returned under any language filter (§2.3).
    assert_eq!(
        objects(&hdt, &["--text", "atzA", "--lang", "de"]),
        [r#""atzA""#]
    );
    assert_eq!(
        objects(&hdt, &["--text", "atzA", "--lang", "fr"]),
        [r#""atzA""#]
    );

    // Whether an untagged literal is returned must not depend on which other
    // languages happen to be in the index: `fr` above and `de` before it answer
    // the same way about an untagged formula. A range matching no *tagged*
    // document simply leaves the tagged ones out.
    assert!(
        objects(&hdt, &["--text", "atrazine", "--lang", "ja"]).is_empty(),
        "every literal holding \"atrazine\" is tagged @en"
    );

    // Cross-language convergence is a real consequence of one stem space:
    // German "Atrazin" and English "atrazine" now share a stem. It is contained
    // rather than eliminated — the German literal is reachable from the English
    // query, but only in the stemmed class, below every exact hit.
    let converged = objects(&hdt, &["--text", "atrazine"]);
    assert_eq!(
        converged[0], r#""atrazine"@en"#,
        "the exact hit still leads: {converged:?}"
    );

    // Several ranges are a union.
    let both = objects(&hdt, &["--text", "pflanzenschutzmittel", "--lang", "en,de"]);
    assert_eq!(both, [r#""Pflanzenschutzmittel"@de"#]);
}

/// §7.3: the predicate restriction is applied when a hit is resolved, and it
/// selects among a subject's occurrences rather than among its literals.
#[test]
fn a_predicate_filter_selects_among_occurrences() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    let labels = rows(&hdt, &["--text", "atrazine", "--predicate", RDFS_LABEL]);
    assert!(!labels.is_empty());
    assert!(
        labels
            .iter()
            .all(|(_, predicate, _)| *predicate == format!("<{RDFS_LABEL}>"))
    );

    let comments = rows(&hdt, &["--text", "atrazine", "--predicate", RDFS_COMMENT]);
    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0].0, iri("chebi/38769"));

    // A predicate the dataset does not use is an empty result, not an error.
    let output = search(
        &hdt,
        &[
            "--text",
            "atrazine",
            "--predicate",
            "http://example.org/nope",
        ],
    );
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(String::from_utf8_lossy(&output.stdout).trim().is_empty());
}

/// A literal whose occurrence group is too large to batch falls back to
/// per-literal resolution. The two paths must agree, and the page must still
/// come out in the same order.
///
/// The batch threshold has a floor of 4096 entries, so this needs a literal
/// with more occurrences than that — which is also the shape that makes the
/// fallback worth having.
#[test]
fn a_literal_with_a_very_large_occurrence_group_still_resolves() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nt");
    let mut nt = String::new();
    for subject in 0..4200 {
        nt.push_str(&format!(
            "<http://example.org/s{subject}> <http://example.org/label> \"shared label\" .\n"
        ));
    }
    // A second, ordinary literal so the page mixes batched and fallback hits.
    nt.push_str("<http://example.org/rare> <http://example.org/label> \"shared label detail\" .\n");
    write_file(&input, nt.as_bytes());
    let hdt = run_hdtc_to_path_with_args(temp.path(), &[&input], "data.hdt", &["--index"]);
    build_text(&hdt, &[]);

    // Each of the first ten triples has a distinct subject in this fixture.
    let page = subjects(&hdt, &["--text", "shared label", "--limit", "10"]);
    assert_eq!(page.len(), 10);
    let mut distinct = page.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), 10, "no subject repeats: {page:?}");

    // The indexed fallback and the sequential scan must agree, exactly as they
    // do for batched literals.
    assert_eq!(
        rows(&hdt, &["--text", "shared label", "--limit", "25"]),
        rows(
            &hdt,
            &["--text", "shared label", "--limit", "25", "--no-index"]
        )
    );

    // And the whole occurrence set is reachable, not truncated at the batch
    // threshold.
    let output = search(&hdt, &["--text", "shared label", "--count"]);
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "4201",
        "every occurrence of both literals"
    );
}

/// §7.3: the two resolution paths must agree. `--no-index` trades one OPS
/// descent per literal for a single sequential pass, and that is the only
/// difference a caller may observe.
#[test]
fn indexed_and_sequential_resolution_agree() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    for query in [
        vec!["--text", "atrazine"],
        vec!["--text", "degradation", "--predicate", RDFS_LABEL],
        vec!["--text", "atrazine degradation", "--text-match", "any"],
    ] {
        let indexed = rows(&hdt, &query);
        let scanned = rows(&hdt, &[query.clone(), vec!["--no-index"]].concat());
        assert_eq!(indexed, scanned, "resolution paths disagree for {query:?}");
    }
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

#[test]
fn text_search_without_an_index_says_how_to_build_one() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nt");
    write_file(&input, TEXT_NT.as_bytes());
    let hdt = run_hdtc_to_path(temp.path(), &[&input], "data.hdt");

    let output = search(&hdt, &["--text", "atrazine"]);
    assert!(!output.status.success());
    assert!(stderr(&output).contains("hdtc text"), "{}", stderr(&output));
}

/// Resolving hits needs the HDT-FoQ index; the error has to name the way out.
#[test]
fn text_search_without_an_hdt_index_offers_the_sequential_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nt");
    write_file(&input, TEXT_NT.as_bytes());
    let hdt = run_hdtc_to_path(temp.path(), &[&input], "data.hdt");
    build_text(&hdt, &[]);

    let output = search(&hdt, &["--text", "atrazine"]);
    assert!(!output.status.success());
    let message = stderr(&output);
    assert!(message.contains("hdtc index"), "{message}");
    assert!(message.contains("--no-index"), "{message}");

    // And the fallback it names does work.
    assert!(!subjects(&hdt, &["--text", "atrazine", "--no-index"]).is_empty());
}

/// §3.2: a query of only separators has no tokens, and saying so beats
/// returning nothing.
#[test]
fn a_query_with_no_tokens_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    let output = search(&hdt, &["--text", "!!! ---"]);
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("no indexable tokens"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn pattern_and_text_search_are_mutually_exclusive() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());

    let both = search(&hdt, &["--query", "? ? ?", "--text", "atrazine"]);
    assert!(!both.status.success());
    assert!(
        stderr(&both).contains("cannot be used with"),
        "{}",
        stderr(&both)
    );

    let neither = search(&hdt, &[]);
    assert!(!neither.status.success());
    assert!(
        stderr(&neither).contains("required"),
        "{}",
        stderr(&neither)
    );

    // Text-only flags are rejected against a pattern query rather than ignored.
    for flag in [
        vec!["--fuzzy", "1"],
        vec!["--prefix"],
        vec!["--scores"],
        vec!["--lang", "en"],
        vec!["--text-match", "phrase"],
        vec!["--predicate", RDFS_LABEL],
    ] {
        let misapplied = search(&hdt, &[vec!["--query", "? ? ?"], flag.clone()].concat());
        assert!(
            !misapplied.status.success(),
            "{flag:?} should require --text"
        );
    }
}

/// §4: a reader must refuse an index whose convention it does not implement,
/// rather than query it and return silently wrong results.
#[test]
fn an_index_from_another_convention_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());
    let manifest_path = text_index_dir(&hdt).join("hdtc-text.meta");
    let original = std::fs::read_to_string(&manifest_path).unwrap();

    for (broken, expected) in [
        (original.replace("analyzer\t3", "analyzer\t9"), "analyzer 9"),
        (
            original.replace("tantivy\t0.26.1", "tantivy\t0.21.0"),
            "0.21.0",
        ),
        (
            original.replace("hdtc-text\t1", "hdtc-text\t4"),
            "version 4",
        ),
    ] {
        write_file(&manifest_path, broken.as_bytes());
        let output = search(&hdt, &["--text", "atrazine"]);
        assert!(!output.status.success(), "expected refusal for {expected}");
        assert!(stderr(&output).contains(expected), "{}", stderr(&output));
    }

    // A later version adding a line it does not know must still be readable.
    write_file(
        &manifest_path,
        format!("{original}some_future_key\tvalue\n").as_bytes(),
    );
    assert!(!subjects(&hdt, &["--text", "atrazine"]).is_empty());
}

/// A directory that is not an hdtc text index is named as such, rather than
/// failing somewhere inside Tantivy.
#[test]
fn a_directory_that_is_not_a_text_index_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path());
    let empty = temp.path().join("not-an-index");
    std::fs::create_dir(&empty).unwrap();

    let output = search(
        &hdt,
        &[
            "--text",
            "atrazine",
            "--text-index",
            empty.to_str().unwrap(),
        ],
    );
    assert!(!output.status.success());
    assert!(stderr(&output).contains("manifest"), "{}", stderr(&output));
}
