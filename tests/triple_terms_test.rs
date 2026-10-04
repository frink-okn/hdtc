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
    ] {
        let output = hdtc(&["search", "-q", hdt.to_str().unwrap(), "--query", query]);
        assert!(!output.status.success(), "{query} was accepted");
    }
    // A predicate is read as stored, so one spelled like a literal is no
    // error: like an ordinary pattern's, it simply matches nothing here.
    assert!(search(&hdt, "? ? <<( ? \"lit\" ? )>>", &[]).is_empty());
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
    // An explicit orphan is told that, not to give an owner that is missing.
    let output = hdtc(&[
        "create",
        "-q",
        orphans.join("a.hdt.props.hdt").to_str().unwrap(),
        "-o",
        dir.path().join("z.hdt").to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("without the HDT it belongs to"));

    // `-o` takes any name, so an owner need not be called `*.hdt`.
    let bare = dir.path().join("bare");
    std::fs::create_dir(&bare).unwrap();
    let unasserted = dir.path().join("unasserted.nt");
    common::write_file(
        &unasserted,
        format!(
            "<http://ex/e> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> <http://ex/o> )>> .\n"
        )
        .as_bytes(),
    );
    let output = hdtc(&[
        "create",
        "-q",
        unasserted.to_str().unwrap(),
        "-o",
        bare.join("data").to_str().unwrap(),
        "--temp-dir",
        dir.path().join("work").to_str().unwrap(),
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(bare.join("data.props.hdt").exists());
    let output = hdtc(&[
        "create",
        "-q",
        bare.to_str().unwrap(),
        "-o",
        dir.path().join("w.hdt").to_str().unwrap(),
    ]);
    // `data` is not an HDT by name, so nothing is left to merge — and above
    // all its propositions are not merged as asserted triples.
    let merged = dir.path().join("w.hdt");
    assert!(
        !output.status.success()
            || !dump(&merged)
                .iter()
                .any(|l| l == "<http://ex/s> <http://ex/p> <http://ex/o> ."),
        "an unasserted claim became a triple"
    );
    let output = hdtc(&[
        "create",
        "-q",
        bare.join("data.props.hdt").to_str().unwrap(),
        "-o",
        dir.path().join("v.hdt").to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("propositions HDT of"));
}

/// Rewrite bytes of a triple-term index's header, keeping its checksums
/// right, so that only the field under test is wrong.
fn corrupt_header(index: &Path, edit: impl FnOnce(&mut [u8])) {
    let crc = |bytes: &[u8]| crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(bytes);
    let mut bytes = std::fs::read(index).unwrap();
    edit(&mut bytes[..256]);
    let header_crc = crc(&bytes[..252]);
    bytes[252..256].copy_from_slice(&header_crc.to_le_bytes());
    let footer = bytes.len() - 64;
    bytes[footer + 56..footer + 60].copy_from_slice(&header_crc.to_le_bytes());
    let footer_crc = crc(&bytes[footer..footer + 60]);
    bytes[footer + 60..footer + 64].copy_from_slice(&footer_crc.to_le_bytes());
    std::fs::write(index, bytes).unwrap();
}

#[test]
fn an_index_header_that_breaks_the_format_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.nt");
    // Asserted only, so the index has no propositions HDT.
    common::write_file(
        &input,
        format!(
            "<http://ex/s> <http://ex/p> <http://ex/o> .\n\
             <http://ex/e> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> <http://ex/o> )>> .\n"
        )
        .as_bytes(),
    );
    let hdt = create(dir.path(), &[&input], "a.hdt");
    assert!(!tt_path(&hdt, ".props.hdt").exists());
    validate(&hdt, dir.path());
    let pristine = std::fs::read(tt_path(&hdt, ".tt")).unwrap();
    for (what, edit) in [
        (
            "minor version",
            Box::new(|header: &mut [u8]| header[10] = 1) as Box<dyn FnOnce(&mut [u8])>,
        ),
        (
            "propositions HDT fields",
            Box::new(|header: &mut [u8]| header[120] = 1),
        ),
        (
            "propositions HDT fields",
            Box::new(|header: &mut [u8]| header[192] = 1),
        ),
    ] {
        std::fs::write(tt_path(&hdt, ".tt"), &pristine).unwrap();
        corrupt_header(&tt_path(&hdt, ".tt"), edit);
        let output = hdtc(&["dump", "-q", hdt.to_str().unwrap()]);
        assert!(!output.status.success(), "{what} accepted");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(what), "{what}: {stderr}");
    }
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

    // The depth count does not leak across the parser's recovery. Each of
    // these lines leaves two triple terms open when it fails, and has no `.`
    // to end it; a counter that outlived recovery would reach the limit long
    // before the last line, which is exactly at it.
    let mut lines = String::new();
    for _ in 0..40 {
        lines.push_str(
            "<http://ex/a> <http://ex/b> <<( <http://ex/s> <http://ex/p> <<( <http://ex/s> \"\n",
        );
    }
    let mut at_limit = "<http://ex/o>".to_owned();
    for _ in 0..64 {
        at_limit = format!("<<( <http://ex/s> <http://ex/p> {at_limit} )>>");
    }
    lines.push_str(&format!("<http://ex/c> <http://ex/d> {at_limit} .\n"));
    let input = dir.path().join("leak.nt");
    common::write_file(&input, lines.as_bytes());
    let hdt = create(dir.path(), &[&input], "leak.hdt");
    assert_eq!(
        dump(&hdt),
        vec![format!("<http://ex/c> <http://ex/d> {at_limit} .")]
    );

    // Reifying a triple whose object is at the limit makes a triple term one
    // level past it, so the reifier is refused where it is written: no
    // dangling reifier is left behind, and the next statement is kept.
    for (name, statement) in [
        (
            "annotated.ttl",
            format!("<http://ex/a> <http://ex/b> {at_limit} {{| <http://ex/q> <http://ex/r> |}} ."),
        ),
        (
            "named.ttl",
            format!(
                "<http://ex/a> <http://ex/b> {at_limit} ~ <http://ex/r1> {{| <http://ex/q> <http://ex/r> |}} ."
            ),
        ),
        (
            "reified.ttl",
            format!("<< <http://ex/a> <http://ex/b> {at_limit} >> <http://ex/q> <http://ex/z> ."),
        ),
    ] {
        let input = dir.path().join(name);
        common::write_file(
            &input,
            format!("{statement}\n<http://ex/ok> <http://ex/p> <http://ex/o> .\n").as_bytes(),
        );
        let hdt = create(dir.path(), &[&input], &format!("{name}.hdt"));
        let lines = dump(&hdt);
        assert!(
            lines
                .iter()
                .all(|line| !line.contains(RDF_REIFIES) && !line.contains("<http://ex/q>")),
            "{name}: {lines:?}"
        );
        assert!(
            lines.contains(&"<http://ex/ok> <http://ex/p> <http://ex/o> .".to_owned()),
            "{name}: {lines:?}"
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

    // Their documents are a segment of their own, after data.hdt's, so a
    // query leaves them out by each segment's column bounds rather than by
    // testing documents (`docs/text-index-format.md` §5.3).
    let text_dir = PathBuf::from(format!("{}.text", hdt.display()));
    let segments = tantivy::Index::open_in_dir(&text_dir)
        .unwrap()
        .searchable_segment_metas()
        .unwrap();
    assert_eq!(segments.len(), 2, "{segments:?}");

    // Through the library, those literals are opt-in: a caller resolving hits
    // through data.hdt alone never matches, scores or counts them.
    use hdtc::format::{TextQuery, TextSearcher};
    let searcher = TextSearcher::open(&PathBuf::from(format!("{}.text", hdt.display()))).unwrap();
    for (propositions, expected) in [(false, 0usize), (true, 1usize)] {
        let query = TextQuery {
            text: "rumoured".to_owned(),
            propositions,
            ..TextQuery::default()
        };
        assert_eq!(searcher.count(&query).unwrap(), expected, "{propositions}");
        assert_eq!(searcher.search(&query, 10).unwrap().len(), expected);
        let ranked = searcher.search_up_to(&query, 10, 100).unwrap();
        assert_eq!(
            (ranked.hits.len(), ranked.examined),
            (expected, expected as u64)
        );
        assert_eq!(
            searcher
                .scan_matching_objects(&query, None, 10)
                .unwrap()
                .object_ids
                .len(),
            expected
        );
    }
    // A literal of data.hdt is found either way.
    for propositions in [false, true] {
        let query = TextQuery {
            text: "asserted".to_owned(),
            propositions,
            ..TextQuery::default()
        };
        assert_eq!(searcher.count(&query).unwrap(), 1, "{propositions}");
    }
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

/// What lenient ingest stores inside an unasserted triple term survives the
/// propositions HDT's build unchanged, though no RDF syntax spells all of it:
/// IRIs and datatype IRIs holding `>`, `\`, a space or a quote, a blank-node
/// label ending in `.`, a language tag with `_`, and IRIs spelled like a blank
/// node or a literal.
#[test]
fn unasserted_triple_terms_keep_whatever_lenient_ingest_stored() {
    let dir = tempfile::tempdir().unwrap();
    let ntriples = dir.path().join("in.nt");
    common::write_file(
        &ntriples,
        format!(
            "<http://ex/e1> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> <http://ex/c d> )>> .\n\
             <http://ex/e2> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> \"y\"^^<http://ex/d\\u0022t> )>> .\n\
             <http://ex/e3> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> \"x\"@zh-classical )>> .\n"
        )
        .as_bytes(),
    );
    let xml = dir.path().join("in.rdf");
    common::write_file(
        &xml,
        concat!(
            "<?xml version=\"1.0\"?>\n",
            "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" ",
            "xmlns:ex=\"http://ex/\" rdf:version=\"1.2\">\n",
            "<rdf:Description rdf:about=\"http://ex/e4\"><rdf:reifies rdf:parseType=\"Triple\">",
            "<rdf:Description rdf:about=\"http://ex/a&gt;b\"><ex:p rdf:resource=\"http://ex/c\\d\"/>",
            "</rdf:Description></rdf:reifies></rdf:Description>\n",
            "<rdf:Description rdf:about=\"http://ex/e5\"><rdf:reifies rdf:parseType=\"Triple\">",
            "<rdf:Description rdf:nodeID=\"x.\"><ex:p xml:lang=\"en_US\">v</ex:p>",
            "</rdf:Description></rdf:reifies></rdf:Description>\n",
            "<rdf:Description rdf:about=\"http://ex/e6\"><rdf:reifies rdf:parseType=\"Triple\">",
            "<rdf:Description rdf:about=\"http://ex/s\"><ex:p rdf:datatype=\"http://ex/d&gt;t\">w</ex:p>",
            "</rdf:Description></rdf:reifies></rdf:Description>\n",
            "</rdf:RDF>\n",
        )
        .as_bytes(),
    );
    // IRIs spelled like a blank node or a literal: the dictionary stores
    // them as one, and every path names the triple term from that spelling.
    // Two are also asserted, so data.hdt decodes them; the last differs
    // from `<"a"@EN>` only in a capital, and must not share its IRI.
    let lookalikes = dir.path().join("lookalikes.nt");
    common::write_file(
        &lookalikes,
        format!(
            "<http://ex/e1> <{RDF_REIFIES}> <<( <_:x> <http://ex/p> <http://ex/o> )>> .\n\
             <http://ex/e2> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> <\\u0022x> )>> .\n\
             <http://ex/e3> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> <\\u0022a\\u0022@EN> )>> .\n\
             <http://ex/e4> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> \
             <\\u0022a\\u0022^^\\u003Chttp://www.w3.org/2001/XMLSchema#string\\u003E> )>> .\n\
             <http://ex/e5> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/q> <\\u0022b\\u0022> )>> .\n\
             <http://ex/s> <http://ex/q> <\\u0022b\\u0022> .\n\
             <http://ex/e6> <{RDF_REIFIES}> <<( <_:y> <http://ex/q> <http://ex/o> )>> .\n\
             <_:y> <http://ex/q> <http://ex/o> .\n\
             <http://ex/e7> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> \"a\"@en )>> .\n\
             <http://ex/e8> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> <\\u0022@en> )>> .\n\
             <http://ex/e9> <{RDF_REIFIES}> <<( <http://ex/s> <http://ex/p> \
             <\\u0022^^\\u003Chttp://ex/d\\u003E> )>> .\n\
             <http://ex/e10> <{RDF_REIFIES}> <<( <\\u0022q\\u0022> <\\u0022p\\u0022> <http://ex/o> )>> .\n"
        )
        .as_bytes(),
    );
    for (input, name, expected) in [
        (
            &lookalikes,
            "lookalikes.hdt",
            vec![
                "<<( _:x <http://ex/p>",
                "\"a\"@EN",
                "XMLSchema#string>",
                "_:y <http://ex/q>",
            ],
        ),
        (
            &ntriples,
            "nt.hdt",
            vec!["<http://ex/c d>", "^^<http://ex/d\"t>", "@zh-classical"],
        ),
        (
            &xml,
            "rdf.hdt",
            vec![
                "<http://ex/a>b>",
                "<http://ex/c\\d>",
                "_:x.",
                "@en_us",
                "^^<http://ex/d>t>",
            ],
        ),
    ] {
        let hdt = create(dir.path(), &[input], name);
        assert!(tt_path(&hdt, ".props.hdt").exists(), "{name}");
        validate(&hdt, dir.path());
        let lines = dump(&hdt);
        let rows = if name == "lookalikes.hdt" { 12 } else { 3 };
        assert_eq!(lines.len(), rows, "{name}: {lines:?}");
        if name == "lookalikes.hdt" {
            // A search finds them as the build stored them: the subject by its
            // spelling, the predicate as it is.
            assert_eq!(
                search(&hdt, "? ? <<( \"q\" \"p\" ? )>>", &[]).len(),
                1,
                "{lines:?}"
            );
        }
        for needle in expected {
            assert!(
                lines.iter().any(|l| l.contains(needle)),
                "{name}: {needle} in {lines:?}"
            );
        }
    }
}

/// A few triple terms among many triples: the data layers are stored sparse
/// (`docs/triple-terms-format.md` §8.1) and the propositions HDT's dense, and
/// every pattern inside a triple term answers exactly the reifiers whose
/// triples match it, by every plan, the count included.
#[test]
fn sparse_layers_answer_every_inner_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let iri = |kind: &str, n: u64| format!("<http://ex/{kind}{n}>");
    let mut source = String::new();
    let mut triples = Vec::new();
    for i in 0..3_000u64 {
        let triple = (iri("s", i % 300), iri("p", i % 10), iri("o", (i * 7) % 200));
        source.push_str(&format!("{} {} {} .\n", triple.0, triple.1, triple.2));
        triples.push(triple);
    }
    // Reifiers and the triple each names: asserted ones from the data, and
    // unasserted ones about objects the data does not hold.
    let mut reified = Vec::new();
    for i in 0..40u64 {
        let triple = triples[(i * 73 % 3_000) as usize].clone();
        reified.push((iri("r", i), triple));
    }
    for i in 0..15u64 {
        let triple = (iri("s", i), iri("p", i % 10), iri("u", i));
        reified.push((iri("n", i), triple));
    }
    for (reifier, (s, p, o)) in &reified {
        source.push_str(&format!(
            "{reifier} <{RDF_REIFIES}> <<( {s} {p} {o} )>> .\n"
        ));
    }
    let input = dir.path().join("in.nt");
    common::write_file(&input, source.as_bytes());
    let hdt = create(dir.path(), &[&input], "sparse.hdt");
    validate(&hdt, dir.path());

    // The data layers are sparse; the propositions HDT's, every position
    // marked, are dense.
    let (_, sections) = hdtc::format::read_triple_term_directory(&tt_path(&hdt, ".tt")).unwrap();
    let kinds = |component: u32| -> Vec<u32> {
        sections
            .iter()
            .filter(|s| s.section_type >> 8 == component)
            .map(|s| s.section_type & 0xFF)
            .collect()
    };
    for component in [0x11, 0x12, 0x13] {
        assert!(
            kinds(component).contains(&0x08),
            "{component:#x}: {:?}",
            kinds(component)
        );
    }
    for component in [0x21, 0x22, 0x23] {
        assert!(
            kinds(component).contains(&0x04),
            "{component:#x}: {:?}",
            kinds(component)
        );
    }

    // Every inner pattern a reified triple fixes, wildcards in each mix.
    let mut patterns = vec![("?".to_owned(), "?".to_owned(), "?".to_owned())];
    for (_, (s, p, o)) in reified.iter().step_by(4) {
        for mask in 1..8u8 {
            let pick = |bit: u8, term: &String| {
                if mask & bit != 0 {
                    term.clone()
                } else {
                    "?".to_owned()
                }
            };
            patterns.push((pick(1, s), pick(2, p), pick(4, o)));
        }
    }
    for (s, p, o) in patterns {
        let expected: Vec<String> = reified
            .iter()
            .filter(|(_, (rs, rp, ro))| {
                (s == "?" || &s == rs) && (p == "?" || &p == rp) && (o == "?" || &o == ro)
            })
            .map(|(reifier, _)| reifier.clone())
            .collect();
        let query = format!("? ? <<( {s} {p} {o} )>>");
        let mut found: Vec<String> = search(&hdt, &query, &[])
            .iter()
            .map(|line| line.split(' ').next().unwrap().to_owned())
            .collect();
        found.sort();
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        assert_eq!(found, expected_sorted, "{query}");
        let counted = search(&hdt, &query, &["--count"]);
        assert_eq!(counted, vec![expected.len().to_string()], "{query} --count");
        // With the outer subject bound, the plan may start from it.
        if let Some(first) = expected.first() {
            let bound = format!("{first} ? <<( {s} {p} {o} )>>");
            assert_eq!(search(&hdt, &bound, &[]).len(), 1, "{bound}");
        }
    }
}
