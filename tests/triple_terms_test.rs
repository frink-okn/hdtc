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

const SECOND: &str = r#"@prefix : <http://ex/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
:bob :knows :carol ~ :r2 {| :source :census |} .
:e9 rdf:reifies <<( _:b1 :p <<( _:b1 :q "x" )>> )>> .
_:b1 :name "anon" .
:e10 rdf:reifies <<( :zed :p :o )>> .
"#;

#[test]
fn merging_hdts_matches_building_from_their_sources() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.ttl");
    let second = dir.path().join("second.ttl");
    // Named reifiers: an annotation's own blank node is labelled afresh by
    // every parse, which would make the two builds differ in name only.
    common::write_file(
        &first,
        TURTLE
            .replace("{| :source :survey1 |}", "~ :r1 {| :source :survey1 |}")
            .as_bytes(),
    );
    common::write_file(&second, SECOND.as_bytes());
    let direct = create(dir.path(), &[&first, &second], "direct.hdt");
    validate(&direct, dir.path());

    let a = create(dir.path(), &[&first], "a.hdt");
    let b = create(dir.path(), &[&second], "b.hdt");
    let merged = create(dir.path(), &[&a, &b], "merged.hdt");
    validate(&merged, dir.path());
    // `:bob :knows :carol` is quoted in the first input and asserted in the
    // second, so the merge asserts it; the dumps agree exactly.
    assert_eq!(dump(&merged), dump(&direct));

    // An RDF input beside an HDT input. Inputs are numbered differently there
    // (RDF files first, in name order), so compare with the prefixes removed.
    let mixed = create(dir.path(), &[&a, &second], "mixed.hdt");
    validate(&mixed, dir.path());
    let unprefixed = |lines: Vec<String>| -> Vec<String> {
        let mut lines: Vec<String> = lines
            .into_iter()
            .map(|line| line.replace("_:f0_", "_:").replace("_:f1_", "_:"))
            .collect();
        lines.sort();
        lines
    };
    assert_eq!(unprefixed(dump(&mixed)), unprefixed(dump(&direct)));

    // A merge of a merge.
    let again = create(dir.path(), &[&merged, &b], "again.hdt");
    validate(&again, dir.path());
    let lines = dump(&again);
    // Inputs are numbered in name order: b.hdt is f0, merged.hdt f1. The node
    // renamed twice is renamed inside its triple terms too, and stays distinct
    // from b's own `_:b1`.
    assert!(
        lines
            .iter()
            .any(|l| l.contains("<<( _:f1_f1_b1 <http://ex/p> <<( _:f1_f1_b1")),
        "{lines:#?}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("<<( _:f0_b1 <http://ex/p> <<( _:f0_b1"))
    );
}

#[test]
fn an_hdt_input_with_unindexed_triple_terms_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.ttl");
    common::write_file(&input, TURTLE.as_bytes());
    let a = create(dir.path(), &[&input], "a.hdt");
    std::fs::remove_file(tt_path(&a, ".tt")).unwrap();
    let output = hdtc(&[
        "create",
        "-q",
        a.to_str().unwrap(),
        "-o",
        dir.path().join("m.hdt").to_str().unwrap(),
        "--temp-dir",
        dir.path().join("work").to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("triple-term index"));
}

#[test]
fn a_propositions_hdt_is_never_an_input_of_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let input = dir.path().join("in.ttl");
    common::write_file(&input, TURTLE.as_bytes());
    let built = common::run_hdtc_to_path_with_args(dir.path(), &[&input], "data/a.hdt", &["-q"]);
    assert!(tt_path(&built, ".props.hdt").exists());

    // Discovered in a directory: skipped, so unasserted claims stay unasserted.
    let merged = create(dir.path(), &[&data], "merged.hdt");
    validate(&merged, dir.path());
    assert!(
        !dump(&merged)
            .iter()
            .any(|l| l == "<http://ex/bob> <http://ex/knows> <http://ex/carol> ."),
        "an unasserted claim became a triple"
    );
    // Named explicitly: refused.
    let output = hdtc(&[
        "create",
        "-q",
        tt_path(&built, ".props.hdt").to_str().unwrap(),
        "-o",
        dir.path().join("x.hdt").to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("propositions HDT"));
    // Found in a directory without its owner: refused, not merged as data.
    let orphans = dir.path().join("orphans");
    std::fs::create_dir(&orphans).unwrap();
    std::fs::copy(
        tt_path(&built, ".props.hdt"),
        orphans.join("a.hdt.props.hdt"),
    )
    .unwrap();
    let output = hdtc(&[
        "create",
        "-q",
        orphans.to_str().unwrap(),
        "-o",
        dir.path().join("y.hdt").to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("without the HDT it belongs to"));
}

#[test]
fn a_failed_rebuild_keeps_the_old_triple_term_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.ttl");
    common::write_file(&input, TURTLE.as_bytes());
    let hdt = create(dir.path(), &[&input], "o.hdt");
    let before = dump(&hdt);
    // A directory where the graph sidecar would go makes retiring it fail,
    // after the triple-term artifacts have already been retired.
    std::fs::create_dir(tt_path(&hdt, ".graphs")).unwrap();
    let plain = dir.path().join("plain.nt");
    common::write_file(&plain, common::REPRESENTATIVE_NT.as_bytes());
    let output = hdtc(&[
        "create",
        "-q",
        plain.to_str().unwrap(),
        "-o",
        hdt.to_str().unwrap(),
        "--temp-dir",
        dir.path().join("work").to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    for suffix in [".tt", ".props.hdt", ".props.hdt.perm", ".perm"] {
        assert!(tt_path(&hdt, suffix).exists(), "{suffix} was not restored");
    }
    assert_eq!(dump(&hdt), before);
    std::fs::remove_dir(tt_path(&hdt, ".graphs")).unwrap();
    validate(&hdt, dir.path());
}

#[test]
fn nesting_beyond_the_limit_is_skipped_not_crashed_on() {
    let dir = tempfile::tempdir().unwrap();
    for depth in [65usize, 20_000] {
        let mut term = "<http://ex/o>".to_owned();
        for _ in 0..depth {
            term = format!("<<( <http://ex/s> <http://ex/p> {term} )>>");
        }
        let input = dir.path().join(format!("deep{depth}.nt"));
        common::write_file(
            &input,
            format!("<http://ex/a> <http://ex/b> {term} .\n").as_bytes(),
        );
        let output = hdtc(&[
            "create",
            "-q",
            input.to_str().unwrap(),
            "-o",
            dir.path().join("deep.hdt").to_str().unwrap(),
            "--temp-dir",
            dir.path().join("work").to_str().unwrap(),
        ]);
        // Skipped with a warning, like any statement the parser rejects —
        // and in particular not a crash: 20,000 levels used to overflow the
        // stack.
        assert!(
            output.status.success(),
            "depth {depth}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let hdt = dir.path().join("deep.hdt");
        assert!(dump(&hdt).is_empty(), "depth {depth} was kept");
    }
    // The limit itself is accepted and readable.
    let mut term = "<http://ex/o>".to_owned();
    for _ in 0..64 {
        term = format!("<<( <http://ex/s> <http://ex/p> {term} )>>");
    }
    let input = dir.path().join("deep64.nt");
    common::write_file(
        &input,
        format!("<http://ex/a> <http://ex/b> {term} .\n").as_bytes(),
    );
    let hdt = create(dir.path(), &[&input], "deep64.hdt");
    validate(&hdt, dir.path());
    assert_eq!(
        dump(&hdt),
        vec![format!("<http://ex/a> <http://ex/b> {term} .")]
    );

    // The statement after a rejected one is kept: the depth count does not
    // leak across the parser's recovery, in either syntax.
    let mut deep = "<http://ex/o>".to_owned();
    for _ in 0..65 {
        deep = format!("<<( <http://ex/s> <http://ex/p> {deep} )>>");
    }
    for (name, prefix) in [
        ("leak.nt", ""),
        ("leak.ttl", "@prefix ex: <http://ex/> .\n"),
    ] {
        let input = dir.path().join(name);
        common::write_file(
            &input,
            format!(
                "{prefix}<http://ex/a> <http://ex/b> {deep} .\n\
                 <http://ex/c> <http://ex/d> <<( <http://ex/s> <http://ex/p> <http://ex/o> )>> .\n"
            )
            .as_bytes(),
        );
        let hdt = create(dir.path(), &[&input], &format!("{name}.hdt"));
        assert_eq!(
            dump(&hdt),
            vec![
                "<http://ex/c> <http://ex/d> <<( <http://ex/s> <http://ex/p> <http://ex/o> )>> ."
                    .to_owned()
            ],
            "{name}"
        );
    }

    // Reified triples nest through their reifiers, not through triple terms,
    // so they are not bounded by the limit: 65 levels are kept whole.
    let mut reified = "<http://ex/o>".to_owned();
    for _ in 0..65 {
        reified = format!("<< <http://ex/s> <http://ex/p> {reified} >>");
    }
    let input = dir.path().join("reified.ttl");
    common::write_file(
        &input,
        format!("{reified} <http://ex/q> <http://ex/z> .\n").as_bytes(),
    );
    let hdt = create(dir.path(), &[&input], "reified.hdt");
    validate(&hdt, dir.path());
    // One rdf:reifies triple per level, and the asserted one.
    assert_eq!(dump(&hdt).len(), 66);
}

#[test]
fn the_reserved_namespace_is_refused_as_predicate_and_graph_name() {
    let dir = tempfile::tempdir().unwrap();
    for (name, content, mode) in [
        (
            "predicate.nt",
            "<http://ex/s> <urn:x-kgf:tt:p> <http://ex/o> .\n",
            "triples",
        ),
        (
            "graph.nq",
            "<http://ex/s> <http://ex/p> <http://ex/o> <urn:x-kgf:tt:g> .\n",
            "quads",
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
            "-m",
            mode,
            "--temp-dir",
            dir.path().join("work").to_str().unwrap(),
        ]);
        assert!(!output.status.success(), "{name} was accepted");
    }
}

#[test]
fn triple_terms_render_through_every_route() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.trig");
    common::write_file(
        &input,
        format!(
            "<http://ex/g> {{ <http://ex/e1> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> <http://ex/o> )>> . }}\n\
             <http://ex/e2> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> \"v\" )>> .\n"
        )
        .as_bytes(),
    );
    let hdt = common::run_hdtc_to_path_with_args(
        dir.path(),
        &[&input],
        "q.hdt",
        &["-q", "-m", "quads", "--graphs-index", "--perm"],
    );
    validate(&hdt, dir.path());
    let rendered = "<<( <http://ex/s> <http://ex/p> <http://ex/o> )>>".to_owned();
    // ? P ? (permutation index), S ? ?, and ? ? O by its IRI rendered back.
    for query in [
        format!("? <{RDF_REIFIES}> ?"),
        "<http://ex/e1> ? ?".to_owned(),
        format!("? ? {rendered}"),
    ] {
        let lines = search(&hdt, &query, &[]);
        assert!(
            lines.iter().any(|l| l.contains(&rendered)),
            "{query}: {lines:#?}"
        );
    }
    // The dataset view renders them too.
    let output = hdtc(&[
        "dump",
        "-q",
        hdt.to_str().unwrap(),
        "--graph-view",
        "dataset",
    ]);
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains(&format!("{rendered}\t<http://ex/g>")),
        "{text}"
    );
    // So does a quad pattern.
    let lines = search(&hdt, "? ? ? <http://ex/g>", &[]);
    assert!(lines.iter().any(|l| l.contains(&rendered)), "{lines:#?}");
}

#[test]
fn text_search_reaches_literals_of_unasserted_triple_terms() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.ttl");
    common::write_file(
        &input,
        format!(
            "{TURTLE}:alice :label \"Alice the asserted\"@en .\n\
             :e5 rdf:reifies <<( :bob :label \"Bob the rumoured\"@en )>> .\n\
             :e6 rdf:reifies <<( :alice :label \"Alice the asserted\"@en )>> .\n"
        )
        .as_bytes(),
    );
    let hdt = create(dir.path(), &[&input], "x.hdt");
    let output = hdtc(&["text", "-q", hdt.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest = std::fs::read_to_string(PathBuf::from(format!(
        "{}.text/hdtc-text.meta",
        hdt.display()
    )))
    .unwrap();
    assert!(
        manifest.contains("propositions_object_offset\t"),
        "{manifest}"
    );

    let text = |query: &str, extra: &[&str]| -> Vec<String> {
        let mut args = vec!["search", "-q", hdt.to_str().unwrap(), "--text", query];
        args.extend_from_slice(extra);
        let output = hdtc(&args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut lines: Vec<String> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|l| l.replace('\t', " "))
            .collect();
        lines.sort();
        lines
    };
    // Only inside an unasserted claim: found, as the claim's reifying triple.
    assert_eq!(
        text("rumoured", &[]),
        vec![format!(
            "<http://ex/e5> <{RDF_REIFIES}> <<( <http://ex/bob> <http://ex/label> \"Bob the rumoured\"@en )>> ."
        )]
    );
    assert_eq!(text("rumoured", &["--count"]), vec!["1".to_owned()]);
    assert_eq!(
        text("rumoured", &["--predicate", "http://ex/label"]).len(),
        1
    );
    assert!(text("rumoured", &["--predicate", "http://ex/name"]).is_empty());
    // Asserted (and also reified): found as the asserted triple.
    assert_eq!(
        text("asserted", &[]),
        vec!["<http://ex/alice> <http://ex/label> \"Alice the asserted\"@en .".to_owned()]
    );
    // Nested two levels down inside an unasserted claim is still reachable.
    assert_eq!(text("quoted", &[]).len(), 1);
}

#[test]
fn namespace_inventories_do_not_count_triple_terms_as_iris() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.ttl");
    common::write_file(&input, TURTLE.as_bytes());
    let hdt = create(dir.path(), &[&input], "x.hdt");
    let table = dir.path().join("prefixes.yaml");
    common::write_file(&table, b"kgf: \"urn:x-kgf:\"\nex: \"http://ex/\"\n");
    let output = hdtc(&[
        "namespaces",
        "-q",
        hdt.to_str().unwrap(),
        "--prefixes",
        table.to_str().unwrap(),
    ]);
    assert!(output.status.success());
    let json = String::from_utf8_lossy(&output.stdout);
    assert!(!json.contains("\"kgf\""), "{json}");
    assert!(!json.contains("urn:x-kgf:tt:"), "{json}");
}
