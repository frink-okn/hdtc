//! Integration tests for `hdtc header`.
//!
//! Covers dumping the header, replacing/augmenting it from an RDF file,
//! renaming the dataset IRI, the conflict guard on reserved predicates, and
//! the `--output` flag-combination rules. Modified files are re-dumped and
//! re-searched to confirm they remain valid, readable HDT.

mod common;

use common::{REPRESENTATIVE_NT, write_file};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Create an HDT from `REPRESENTATIVE_NT` with the given dataset IRI.
fn make_hdt(temp_dir: &Path, dataset_uri: &str) -> PathBuf {
    let nt_path = temp_dir.join("input.nt");
    write_file(&nt_path, REPRESENTATIVE_NT.as_bytes());

    let hdt_path = temp_dir.join("data.hdt");
    let work_dir = temp_dir.join("work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            nt_path.to_str().unwrap(),
            "-o",
            hdt_path.to_str().unwrap(),
            "--dataset-uri",
            dataset_uri,
            "--temp-dir",
            work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc create");
    assert!(
        output.status.success(),
        "hdtc create failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    hdt_path
}

/// Run `hdtc header` with the given args. Returns (success, stdout, stderr).
fn run_header(args: &[&str]) -> (bool, String, String) {
    let mut full = vec!["header"];
    full.extend_from_slice(args);
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(&full)
        .output()
        .expect("Failed to execute hdtc header");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Dump the header of `hdt_path` to a string (asserts success).
fn dump_header(hdt_path: &Path) -> String {
    let (ok, stdout, stderr) = run_header(&[hdt_path.to_str().unwrap()]);
    assert!(ok, "header dump failed: {stderr}");
    stdout
}

/// Assert the HDT file is still readable by round-tripping a full dump.
fn assert_readable(hdt_path: &Path) {
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["dump", hdt_path.to_str().unwrap()])
        .output()
        .expect("Failed to execute hdtc dump");
    assert!(
        output.status.success(),
        "modified HDT is not readable: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // 8 triples in REPRESENTATIVE_NT.
    let lines = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count();
    assert_eq!(lines, 8, "expected 8 triples after modification");
}

#[test]
fn dump_contains_managed_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");

    let header = dump_header(&hdt);
    assert!(header.contains("http://rdfs.org/ns/void#triples"));
    assert!(header.contains("\"8\""));
    assert!(header.contains("http://example.org/ds"));
}

#[test]
fn add_appends_descriptive_triples() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");

    let meta = temp.path().join("meta.nt");
    write_file(
        &meta,
        b"<http://example.org/ds> <http://purl.org/dc/terms/title> \"My Dataset\" .\n",
    );
    let out = temp.path().join("out.hdt");

    let (ok, _, stderr) = run_header(&[
        hdt.to_str().unwrap(),
        "--add",
        meta.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
    ]);
    assert!(ok, "header --add failed: {stderr}");

    let header = dump_header(&out);
    // Managed metadata retained AND the new descriptive triple present.
    assert!(header.contains("http://rdfs.org/ns/void#triples"));
    assert!(header.contains("My Dataset"));
    assert_readable(&out);
}

#[test]
fn replace_keeps_managed_drops_old_descriptive() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");

    // First add a descriptive triple.
    let meta1 = temp.path().join("meta1.nt");
    write_file(
        &meta1,
        b"<http://example.org/ds> <http://purl.org/dc/terms/title> \"First\" .\n",
    );
    let with_meta = temp.path().join("with_meta.hdt");
    let (ok, _, e) = run_header(&[
        hdt.to_str().unwrap(),
        "--add",
        meta1.to_str().unwrap(),
        "--output",
        with_meta.to_str().unwrap(),
    ]);
    assert!(ok, "{e}");

    // Now replace the descriptive metadata.
    let meta2 = temp.path().join("meta2.nt");
    write_file(
        &meta2,
        b"<http://example.org/ds> <http://purl.org/dc/terms/title> \"Second\" .\n",
    );
    let replaced = temp.path().join("replaced.hdt");
    let (ok, _, e) = run_header(&[
        with_meta.to_str().unwrap(),
        "--replace",
        meta2.to_str().unwrap(),
        "--output",
        replaced.to_str().unwrap(),
    ]);
    assert!(ok, "{e}");

    let header = dump_header(&replaced);
    assert!(header.contains("Second"), "new descriptive triple present");
    assert!(!header.contains("First"), "old descriptive triple dropped");
    assert!(
        header.contains("http://rdfs.org/ns/void#triples"),
        "managed block retained"
    );
    assert_readable(&replaced);
}

#[test]
fn dataset_uri_reroots_subject() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/old");
    let out = temp.path().join("renamed.hdt");

    let (ok, _, e) = run_header(&[
        hdt.to_str().unwrap(),
        "--dataset-uri",
        "http://example.org/new",
        "--output",
        out.to_str().unwrap(),
    ]);
    assert!(ok, "{e}");

    let header = dump_header(&out);
    assert!(header.contains("http://example.org/new"));
    assert!(!header.contains("http://example.org/old"));
    assert_readable(&out);
}

#[test]
fn reserved_predicate_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");

    let bad = temp.path().join("bad.nt");
    write_file(
        &bad,
        b"<http://example.org/ds> <http://rdfs.org/ns/void#triples> \"999\" .\n",
    );
    let out = temp.path().join("out.hdt");

    let (ok, _, stderr) = run_header(&[
        hdt.to_str().unwrap(),
        "--add",
        bad.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
    ]);
    assert!(!ok, "expected failure on reserved predicate");
    assert!(
        stderr.contains("reserved"),
        "error should mention reserved predicate: {stderr}"
    );
    assert!(!out.exists(), "no output should be written on error");
}

#[test]
fn modify_without_output_errors() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");

    let (ok, _, stderr) = run_header(&[
        hdt.to_str().unwrap(),
        "--dataset-uri",
        "http://example.org/new",
    ]);
    assert!(!ok, "expected failure without --output");
    assert!(stderr.contains("--output"), "error should mention --output");
}

#[test]
fn output_without_modification_errors() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");
    let out = temp.path().join("out.hdt");

    let (ok, _, stderr) = run_header(&[hdt.to_str().unwrap(), "--output", out.to_str().unwrap()]);
    assert!(!ok, "expected failure: --output with no modification flag");
    assert!(stderr.contains("--output"));
}

#[test]
fn dataset_uri_reroots_object_occurrences() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/old");

    // Add a triple that references the dataset IRI in the OBJECT position.
    let meta = temp.path().join("meta.nt");
    write_file(
        &meta,
        b"<http://example.org/other> <http://www.w3.org/2002/07/owl#sameAs> <http://example.org/old> .\n",
    );
    let with_ref = temp.path().join("with_ref.hdt");
    let (ok, _, e) = run_header(&[
        hdt.to_str().unwrap(),
        "--add",
        meta.to_str().unwrap(),
        "--output",
        with_ref.to_str().unwrap(),
    ]);
    assert!(ok, "{e}");

    let renamed = temp.path().join("renamed.hdt");
    let (ok, _, e) = run_header(&[
        with_ref.to_str().unwrap(),
        "--dataset-uri",
        "http://example.org/new",
        "--output",
        renamed.to_str().unwrap(),
    ]);
    assert!(ok, "{e}");

    let header = dump_header(&renamed);
    assert!(
        !header.contains("http://example.org/old"),
        "old IRI must be gone from subject AND object positions:\n{header}"
    );
    assert!(header.contains("owl#sameAs> <http://example.org/new>"));
    assert_readable(&renamed);
}

#[test]
fn invalid_dataset_uri_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");
    let out = temp.path().join("out.hdt");

    let (ok, _, stderr) = run_header(&[
        hdt.to_str().unwrap(),
        "--dataset-uri",
        "http://example.org/a b", // illegal space
        "--output",
        out.to_str().unwrap(),
    ]);
    assert!(!ok, "expected failure on malformed dataset IRI");
    assert!(
        stderr.contains("Invalid --dataset-uri"),
        "error should name the bad IRI: {stderr}"
    );
    assert!(!out.exists(), "no corrupt output should be written");
}

#[test]
fn add_with_relative_iri_input_stays_readable() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");

    // Turtle with a relative IRI subject and no @base; must resolve to an
    // absolute IRI so the output header stays valid N-Triples.
    let meta = temp.path().join("meta.ttl");
    write_file(
        &meta,
        b"<> <http://purl.org/dc/terms/title> \"Relative subject\" .\n",
    );
    let out = temp.path().join("out.hdt");

    let (ok, _, e) = run_header(&[
        hdt.to_str().unwrap(),
        "--add",
        meta.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
    ]);
    assert!(ok, "header --add with relative IRI failed: {e}");

    let header = dump_header(&out);
    assert!(header.contains("Relative subject"));
    // No empty/relative IRI leaked into the header.
    assert!(
        !header.contains("<> "),
        "relative IRI should be resolved:\n{header}"
    );
    assert_readable(&out);
}

#[test]
fn repeated_add_keeps_blank_nodes_distinct() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");

    // First add introduces a blank node labeled `x`.
    let meta1 = temp.path().join("meta1.nt");
    write_file(&meta1, b"_:x <http://example.org/p1> \"first\" .\n");
    let step1 = temp.path().join("step1.hdt");
    let (ok, _, e) = run_header(&[
        hdt.to_str().unwrap(),
        "--add",
        meta1.to_str().unwrap(),
        "--output",
        step1.to_str().unwrap(),
    ]);
    assert!(ok, "{e}");

    // Second add reuses label `x` for an UNRELATED blank node.
    let meta2 = temp.path().join("meta2.nt");
    write_file(&meta2, b"_:x <http://example.org/p2> \"second\" .\n");
    let step2 = temp.path().join("step2.hdt");
    let (ok, _, e) = run_header(&[
        step1.to_str().unwrap(),
        "--add",
        meta2.to_str().unwrap(),
        "--output",
        step2.to_str().unwrap(),
    ]);
    assert!(ok, "{e}");

    let header = dump_header(&step2);
    // The two blank nodes must not have been merged onto a single label: find
    // the blank-node subject of each predicate and assert they differ.
    let subj_of = |pred: &str| -> String {
        header
            .lines()
            .find(|l| l.contains(pred))
            .and_then(|l| l.split_whitespace().next())
            .unwrap_or("")
            .to_string()
    };
    let s1 = subj_of("http://example.org/p1");
    let s2 = subj_of("http://example.org/p2");
    assert!(s1.starts_with("_:") && s2.starts_with("_:"), "{header}");
    assert_ne!(s1, s2, "distinct blank nodes were merged:\n{header}");
    assert_readable(&step2);
}

#[test]
fn replace_and_add_are_mutually_exclusive() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_hdt(temp.path(), "http://example.org/ds");
    let meta = temp.path().join("meta.nt");
    write_file(&meta, b"<http://example.org/ds> <http://x.org/p> \"v\" .\n");
    let out = temp.path().join("out.hdt");

    let (ok, _, _) = run_header(&[
        hdt.to_str().unwrap(),
        "--replace",
        meta.to_str().unwrap(),
        "--add",
        meta.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
    ]);
    assert!(!ok, "clap should reject --replace together with --add");
}
