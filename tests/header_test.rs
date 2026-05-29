mod common;

use common::{REPRESENTATIVE_TRIPLE_COUNT, run_hdtc_to_path, run_hdtc_to_path_with_args, write_file};
use std::path::Path;
use std::process::Command;

/// Run hdtc with the given args, returning (success, stdout, stderr).
fn run_hdtc(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(args)
        .output()
        .expect("Failed to execute hdtc");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    eprintln!("hdtc {args:?} stderr:\n{stderr}");
    (output.status.success(), stdout, stderr)
}

/// Dump the full triple data (tab-delimited) and count the triples, to confirm
/// the dictionary/triples sections survived a header rewrite intact.
fn data_triple_count(hdt: &Path) -> usize {
    let (ok, stdout, stderr) = run_hdtc(&["dump", hdt.to_str().unwrap()]);
    assert!(ok, "hdtc dump failed: {stderr}");
    stdout.lines().filter(|l| !l.trim().is_empty()).count()
}

/// The triple-count predicate hdtc reads from the header; `replace` preserves it.
const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";

/// A purely descriptive generated-metadata predicate that `replace` drops.
const VOID_DISTINCT_SUBJECTS: &str = "http://rdfs.org/ns/void#distinctSubjects";

#[test]
fn test_header_dump_shows_generated_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let nt = dir.path().join("data.nt");
    write_file(&nt, common::REPRESENTATIVE_NT.as_bytes());
    let hdt = run_hdtc_to_path(dir.path(), &[nt.as_path()], "data.hdt");

    let (ok, stdout, _) = run_hdtc(&["header", "dump", hdt.to_str().unwrap()]);
    assert!(ok);
    assert!(stdout.contains("http://rdfs.org/ns/void#Dataset"));
    assert!(stdout.contains(VOID_TRIPLES));
    assert!(stdout.contains("http://purl.org/HDT/hdt#Dataset"));
}

#[test]
fn test_header_replace_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let nt = dir.path().join("data.nt");
    write_file(&nt, common::REPRESENTATIVE_NT.as_bytes());
    let hdt = run_hdtc_to_path(dir.path(), &[nt.as_path()], "data.hdt");

    let meta = dir.path().join("meta.nt");
    write_file(
        &meta,
        b"<http://example.org/dataset> <http://purl.org/dc/terms/title> \"My Dataset\" .\n\
          <http://example.org/dataset> <http://purl.org/dc/terms/creator> \"Ada\" .\n",
    );

    let (ok, _, stderr) = run_hdtc(&[
        "header",
        "replace",
        hdt.to_str().unwrap(),
        "-i",
        meta.to_str().unwrap(),
    ]);
    assert!(ok, "replace failed: {stderr}");

    // Header now holds the input triples...
    let (ok, header, _) = run_hdtc(&["header", "dump", hdt.to_str().unwrap()]);
    assert!(ok);
    assert!(header.contains("dc/terms/title"));
    assert!(header.contains("My Dataset"));
    // ...descriptive generated metadata is gone...
    assert!(
        !header.contains(VOID_DISTINCT_SUBJECTS),
        "descriptive generated metadata should be gone after replace:\n{header}"
    );
    // ...but the triple count is preserved so the file stays readable.
    assert!(
        header.contains(VOID_TRIPLES),
        "triple count should be carried over on replace:\n{header}"
    );

    // Dictionary + triples untouched: full data still dumps every triple.
    assert_eq!(data_triple_count(&hdt), REPRESENTATIVE_TRIPLE_COUNT);
}

#[test]
fn test_header_replace_count_is_file_owned() {
    let dir = tempfile::tempdir().unwrap();
    let nt = dir.path().join("data.nt");
    write_file(&nt, common::REPRESENTATIVE_NT.as_bytes());
    let hdt = run_hdtc_to_path(dir.path(), &[nt.as_path()], "data.hdt");

    // Input tries to assert a bogus triple count; it must be ignored in favor
    // of the file's real count.
    let meta = dir.path().join("meta.nt");
    write_file(
        &meta,
        format!(
            "<http://example.org/dataset> <http://purl.org/dc/terms/title> \"My Dataset\" .\n\
             <http://example.org/dataset> <{VOID_TRIPLES}> \"999\" .\n"
        )
        .as_bytes(),
    );

    let (ok, _, stderr) = run_hdtc(&[
        "header",
        "replace",
        hdt.to_str().unwrap(),
        "-i",
        meta.to_str().unwrap(),
    ]);
    assert!(ok, "replace failed: {stderr}");

    let (ok, header, _) = run_hdtc(&["header", "dump", hdt.to_str().unwrap()]);
    assert!(ok);
    // The real count (8) wins; the bogus "999" is dropped.
    assert!(
        header.contains(&format!("<{VOID_TRIPLES}> \"{REPRESENTATIVE_TRIPLE_COUNT}\"")),
        "real triple count should be preserved:\n{header}"
    );
    assert!(!header.contains("\"999\""), "bogus input count should be dropped:\n{header}");
    assert_eq!(
        header.matches(VOID_TRIPLES).count(),
        1,
        "exactly one void:triples statement expected:\n{header}"
    );
    assert_eq!(data_triple_count(&hdt), REPRESENTATIVE_TRIPLE_COUNT);
}

#[test]
fn test_header_augment_unions_triples() {
    let dir = tempfile::tempdir().unwrap();
    let nt = dir.path().join("data.nt");
    write_file(&nt, common::REPRESENTATIVE_NT.as_bytes());
    let hdt = run_hdtc_to_path(dir.path(), &[nt.as_path()], "data.hdt");

    let meta = dir.path().join("meta.nt");
    write_file(
        &meta,
        b"<http://example.org/dataset> <http://purl.org/dc/terms/title> \"My Dataset\" .\n",
    );

    let (ok, _, stderr) = run_hdtc(&[
        "header",
        "augment",
        hdt.to_str().unwrap(),
        "-i",
        meta.to_str().unwrap(),
    ]);
    assert!(ok, "augment failed: {stderr}");

    let (ok, header, _) = run_hdtc(&["header", "dump", hdt.to_str().unwrap()]);
    assert!(ok);
    // Both the original generated metadata and the new triple are present.
    assert!(header.contains(VOID_TRIPLES), "original metadata should survive augment");
    assert!(header.contains("dc/terms/title"), "added triple should be present");

    assert_eq!(data_triple_count(&hdt), REPRESENTATIVE_TRIPLE_COUNT);
}

#[test]
fn test_header_output_flag_leaves_source_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let nt = dir.path().join("data.nt");
    write_file(&nt, common::REPRESENTATIVE_NT.as_bytes());
    let hdt = run_hdtc_to_path(dir.path(), &[nt.as_path()], "data.hdt");
    let new_hdt = dir.path().join("with-meta.hdt");

    let meta = dir.path().join("meta.nt");
    write_file(
        &meta,
        b"<http://example.org/dataset> <http://purl.org/dc/terms/title> \"My Dataset\" .\n",
    );

    let (ok, _, stderr) = run_hdtc(&[
        "header",
        "replace",
        hdt.to_str().unwrap(),
        "-i",
        meta.to_str().unwrap(),
        "-o",
        new_hdt.to_str().unwrap(),
    ]);
    assert!(ok, "replace -o failed: {stderr}");

    // Source header is unchanged...
    let (_, src_header, _) = run_hdtc(&["header", "dump", hdt.to_str().unwrap()]);
    assert!(src_header.contains(VOID_DISTINCT_SUBJECTS));
    assert!(!src_header.contains("dc/terms/title"));

    // ...and the new file has the replaced header plus intact data.
    let (_, new_header, _) = run_hdtc(&["header", "dump", new_hdt.to_str().unwrap()]);
    assert!(new_header.contains("dc/terms/title"));
    assert!(!new_header.contains(VOID_DISTINCT_SUBJECTS));
    assert_eq!(data_triple_count(&new_hdt), REPRESENTATIVE_TRIPLE_COUNT);
}

#[test]
fn test_header_rewrite_preserves_existing_index() {
    let dir = tempfile::tempdir().unwrap();
    let nt = dir.path().join("data.nt");
    write_file(&nt, common::REPRESENTATIVE_NT.as_bytes());
    let hdt = run_hdtc_to_path_with_args(dir.path(), &[nt.as_path()], "data.hdt", &["--index"]);

    let index = hdt.with_extension("hdt.index.v1-1");
    let index_before = std::fs::read(&index).expect("index should exist after --index");

    let meta = dir.path().join("meta.nt");
    write_file(
        &meta,
        b"<http://example.org/dataset> <http://purl.org/dc/terms/title> \"My Dataset\" .\n",
    );

    let (ok, _, stderr) = run_hdtc(&[
        "header",
        "replace",
        hdt.to_str().unwrap(),
        "-i",
        meta.to_str().unwrap(),
    ]);
    assert!(ok, "replace failed: {stderr}");

    // The sidecar index is keyed off dictionary/triples, which were copied
    // verbatim, so it is byte-for-byte unchanged and still valid.
    let index_after = std::fs::read(&index).expect("index should still exist");
    assert_eq!(index_before, index_after, "index must be untouched by header rewrite");

    // A search that relies on the index still works.
    let (ok, _, stderr) = run_hdtc(&[
        "search",
        hdt.to_str().unwrap(),
        "--query",
        "? <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?",
    ]);
    assert!(ok, "search after header rewrite failed: {stderr}");
}
