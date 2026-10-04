//! RDF 1.2 triple terms: build, render, validate (docs/triple-terms-format.md).

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

const RDF_REIFIES: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies";

fn hdtc(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(args)
        .output()
        .expect("failed to run hdtc")
}

fn create(dir: &Path, inputs: &[&Path], name: &str) -> PathBuf {
    common::run_hdtc_to_path_with_args(dir, inputs, name, &["-q"])
}

fn dump(hdt: &Path) -> Vec<String> {
    let output = hdtc(&["dump", "-q", hdt.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "dump failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut lines: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| line.replace('\t', " "))
        .collect();
    lines.sort();
    lines
}

fn validate(hdt: &Path, dir: &Path) {
    let output = hdtc(&[
        "validate",
        "-q",
        hdt.to_str().unwrap(),
        "--temp-dir",
        dir.join("validate").to_str().unwrap(),
    ]);
    assert!(
        output.status.success(),
        "validate failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn tt_path(hdt: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", hdt.display()))
}

const TURTLE: &str = r#"@prefix : <http://ex/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
:alice :knows :bob {| :source :survey1 |} .
:e2 rdf:reifies <<( :bob :knows :carol )>> .
:e2 :source :rumor .
:e3 :claims <<( :x :says <<( :bob :knows :carol )>> )>> .
:e4 rdf:reifies <<( :s :p "a \"quoted\"\nline"@EN )>> .
:dave :name "Dave"@en--ltr .
"#;

#[test]
fn triple_terms_round_trip_through_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.ttl");
    common::write_file(&input, TURTLE.as_bytes());
    let hdt = create(dir.path(), &[&input], "out.hdt");

    for suffix in [".perm", ".tt", ".props.hdt", ".props.hdt.perm"] {
        assert!(tt_path(&hdt, suffix).exists(), "{suffix} missing");
    }
    validate(&hdt, dir.path());

    let lines = dump(&hdt);
    let reifier_line = lines
        .iter()
        .find(|line| line.contains("<http://ex/alice> <http://ex/knows> <http://ex/bob> )>>"))
        .expect("annotation reifier rendered");
    assert!(
        reifier_line.starts_with("_:"),
        "annotation mints a blank reifier"
    );
    for expected in [
        "<http://ex/alice> <http://ex/knows> <http://ex/bob> .".to_owned(),
        format!("<http://ex/e2> <{RDF_REIFIES}> <<( <http://ex/bob> <http://ex/knows> <http://ex/carol> )>> ."),
        "<http://ex/e3> <http://ex/claims> <<( <http://ex/x> <http://ex/says> <<( <http://ex/bob> <http://ex/knows> <http://ex/carol> )>> )>> .".to_owned(),
        format!("<http://ex/e4> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> \"a \\\"quoted\\\"\\nline\"@en )>> ."),
        "<http://ex/dave> <http://ex/name> \"Dave\"@en--ltr .".to_owned(),
    ] {
        assert!(lines.contains(&expected), "missing {expected}\nin {lines:#?}");
    }
    // The unasserted claim is not a triple of the data.
    assert!(
        !lines
            .iter()
            .any(|l| l.starts_with("<http://ex/bob> <http://ex/knows>"))
    );

    // The dump is RDF 1.2 that rebuilds to the same graph.
    let again = dir.path().join("again.nt");
    common::write_file(&again, (lines.join("\n") + "\n").as_bytes());
    let rebuilt = create(dir.path(), &[&again], "rebuilt.hdt");
    assert_eq!(dump(&rebuilt), lines);
}

#[test]
fn an_input_without_triple_terms_leaves_no_artifacts_behind() {
    let dir = tempfile::tempdir().unwrap();
    let with = dir.path().join("with.ttl");
    common::write_file(&with, TURTLE.as_bytes());
    let hdt = create(dir.path(), &[&with], "out.hdt");
    assert!(tt_path(&hdt, ".tt").exists());

    let without = dir.path().join("without.nt");
    common::write_file(&without, common::REPRESENTATIVE_NT.as_bytes());
    let hdt = create(dir.path(), &[&without], "out.hdt");
    for suffix in [".perm", ".tt", ".props.hdt", ".props.hdt.perm"] {
        assert!(!tt_path(&hdt, suffix).exists(), "stale {suffix} survived");
    }
    assert_eq!(dump(&hdt).len(), common::REPRESENTATIVE_TRIPLE_COUNT);
}

#[test]
fn the_reserved_namespace_is_refused_in_source_rdf() {
    let dir = tempfile::tempdir().unwrap();
    for (name, content) in [
        (
            "object.nt",
            "<http://ex/s> <http://ex/p> <urn:x-kgf:tt:g:00000000000000000000000000000000> .\n",
        ),
        (
            "nested.ttl",
            "<http://ex/s> <http://ex/p> <<( <urn:x-kgf:tt:anything> <http://ex/q> <http://ex/o> )>> .\n",
        ),
        (
            "datatype.nt",
            "<http://ex/s> <http://ex/p> \"1\"^^<urn:x-kgf:tt:dt> .\n",
        ),
    ] {
        let input = dir.path().join(name);
        common::write_file(&input, content.as_bytes());
        let output = hdtc(&[
            "create",
            "-q",
            input.to_str().unwrap(),
            "-o",
            dir.path().join("out.hdt").to_str().unwrap(),
            "--temp-dir",
            dir.path().join("work").to_str().unwrap(),
        ]);
        assert!(!output.status.success(), "{name} was accepted");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("urn:x-kgf:tt:"), "{name}: {stderr}");
    }
}

#[test]
fn triple_term_iris_without_their_index_are_not_rendered() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.ttl");
    common::write_file(&input, TURTLE.as_bytes());
    let hdt = create(dir.path(), &[&input], "out.hdt");
    std::fs::remove_file(tt_path(&hdt, ".tt")).unwrap();
    let output = hdtc(&["dump", "-q", hdt.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("triple-term index"));
}

#[test]
fn blank_nodes_in_triple_terms_stay_apart_across_files() {
    let dir = tempfile::tempdir().unwrap();
    // The same label in two files is two nodes; only the first file asserts
    // its triple.
    let a = dir.path().join("a.nt");
    let b = dir.path().join("b.nt");
    common::write_file(
        &a,
        format!(
            "_:b1 <http://ex/p> <http://ex/o> .\n<http://ex/r1> <{RDF_REIFIES}> <<( _:b1 <http://ex/p> <http://ex/o> )>> .\n"
        )
        .as_bytes(),
    );
    common::write_file(
        &b,
        format!("<http://ex/r2> <{RDF_REIFIES}> <<( _:b1 <http://ex/p> <http://ex/o> )>> .\n")
            .as_bytes(),
    );
    let hdt = create(dir.path(), &[&a, &b], "out.hdt");
    validate(&hdt, dir.path());
    let lines = dump(&hdt);
    let term_of = |reifier: &str| -> String {
        lines
            .iter()
            .find(|l| l.starts_with(reifier))
            .unwrap()
            .split_once(" <<( ")
            .unwrap()
            .1
            .to_owned()
    };
    let (r1, r2) = (term_of("<http://ex/r1>"), term_of("<http://ex/r2>"));
    assert_ne!(r1, r2, "two blank nodes collapsed into one triple term");
    // r1's term is the asserted triple; its blank node is the asserted one's.
    let asserted_subject = lines
        .iter()
        .find(|l| l.ends_with("<http://ex/p> <http://ex/o> ."))
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .to_owned();
    assert!(
        r1.starts_with(&asserted_subject),
        "{r1} vs {asserted_subject}"
    );
    assert!(
        tt_path(&hdt, ".props.hdt").exists(),
        "r2's term is unasserted"
    );
}

fn search(hdt: &Path, query: &str, extra: &[&str]) -> Vec<String> {
    let mut args = vec!["search", "-q", hdt.to_str().unwrap(), "--query", query];
    args.extend_from_slice(extra);
    let output = hdtc(&args);
    assert!(
        output.status.success(),
        "search {query} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut lines: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| line.replace('\t', " "))
        .collect();
    lines.sort();
    lines
}

#[test]
fn search_matches_patterns_inside_triple_terms() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.ttl");
    common::write_file(&input, TURTLE.as_bytes());
    let hdt = create(dir.path(), &[&input], "out.hdt");
    let all = dump(&hdt);
    let with_term: Vec<String> = all.iter().filter(|l| l.contains("<<(")).cloned().collect();
    let reifies = format!("<{RDF_REIFIES}>");
    let filter = |needle: &str| -> Vec<String> {
        with_term
            .iter()
            .filter(|l| l.contains(needle))
            .cloned()
            .collect()
    };

    // Every inner position a wildcard.
    assert_eq!(search(&hdt, "? ? <<( ? ? ? )>>", &[]), with_term);
    assert_eq!(
        search(&hdt, &format!("? {reifies} <<( ? ? ? )>>"), &[]),
        filter(&reifies)
    );
    // Subject-, predicate-, and object-rooted inner patterns, asserted and not.
    // Only the outer triple term's subject: e3's bob is one level down.
    assert_eq!(
        search(&hdt, "? ? <<( <http://ex/bob> ? ? )>>", &[]),
        filter(&format!("{reifies} <<( <http://ex/bob>"))
    );
    assert_eq!(
        search(&hdt, "? ? <<( ? <http://ex/knows> ? )>>", &[]),
        filter(&format!("{reifies} <<( "))
            .into_iter()
            .filter(|l| l.contains("<http://ex/knows>"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        search(&hdt, "? ? <<( ? ? <http://ex/bob> )>>", &[]),
        filter("<http://ex/bob> )>>")
    );
    // S ? O inside, a literal inside, and a ground nested term inside.
    assert_eq!(
        search(&hdt, "? ? <<( <http://ex/alice> ? <http://ex/bob> )>>", &[]).len(),
        1
    );
    assert_eq!(
        search(&hdt, "? ? <<( ? ? \"a \\\"quoted\\\"\\nline\"@en )>>", &[]),
        filter("<http://ex/e4>")
    );
    assert_eq!(
        search(
            &hdt,
            "? ? <<( ? ? <<( <http://ex/bob> <http://ex/knows> <http://ex/carol> )>> )>>",
            &[]
        ),
        filter("<http://ex/e3>")
    );
    // A ground triple term is just its IRI.
    assert_eq!(
        search(
            &hdt,
            "? ? <<( <http://ex/bob> <http://ex/knows> <http://ex/carol> )>>",
            &[]
        ),
        filter("<http://ex/e2>")
    );
    // Outer subject bound.
    assert_eq!(
        search(&hdt, "<http://ex/e3> ? <<( ? ? ? )>>", &[]),
        filter("<http://ex/e3>")
    );
    // Counting, offset, and limit.
    assert_eq!(
        search(&hdt, "? ? <<( ? ? ? )>>", &["--count"]),
        vec![with_term.len().to_string()]
    );
    assert_eq!(
        search(&hdt, "? ? <<( ? <http://ex/knows> ? )>>", &["--count"]),
        vec!["2".to_owned()]
    );
    assert_eq!(
        search(&hdt, "? ? <<( ? ? ? )>>", &["--limit", "2"]).len(),
        2
    );
    assert_eq!(
        search(&hdt, "? ? <<( ? ? ? )>>", &["--offset", "1"]).len(),
        with_term.len() - 1
    );
    // Nothing matches a term the data does not have.
    assert!(search(&hdt, "? ? <<( <http://ex/nobody> ? ? )>>", &[]).is_empty());
}

#[test]
fn search_refuses_misplaced_triple_terms() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.ttl");
    common::write_file(&input, TURTLE.as_bytes());
    let hdt = create(dir.path(), &[&input], "out.hdt");
    for query in [
        "<<( <http://ex/a> <http://ex/b> <http://ex/c> )>> ? ?",
        "? ? <<( ? ? <<( ? <http://ex/b> <http://ex/c> )>> )>>",
        "? ? <<( ? \"lit\" ? )>>",
    ] {
        let output = hdtc(&["search", "-q", hdt.to_str().unwrap(), "--query", query]);
        assert!(!output.status.success(), "{query} was accepted");
    }
}
