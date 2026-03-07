//! Integration tests for `hdtc search`.
//!
//! Covers all triple-pattern query types: `???`, `S??`, `SP?`, `S?O`, `SPO`,
//! `?P?`, `??O`, `?PO`.

mod common;

use common::{REPRESENTATIVE_NT, write_file};
use std::collections::HashSet;
use std::path::Path;
use std::process::Command;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create an HDT file from `REPRESENTATIVE_NT` in `temp_dir` and return its path.
fn make_representative_hdt(temp_dir: &Path) -> std::path::PathBuf {
    let nt_path = temp_dir.join("input.nt");
    write_file(&nt_path, REPRESENTATIVE_NT.as_bytes());

    let hdt_path = temp_dir.join("data.hdt");
    let work_dir = temp_dir.join("work");

    let status = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            nt_path.to_str().unwrap(),
            "-o",
            hdt_path.to_str().unwrap(),
            "--base-uri",
            "http://example.org/dataset",
            "--temp-dir",
            work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc create");

    assert!(
        status.status.success(),
        "hdtc create failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );

    hdt_path
}

/// Run `hdtc search` and return (success, stdout, stderr).
fn run_search(hdt_path: &Path, query: &str, extra_args: &[&str]) -> (bool, String, String) {
    let mut args = vec!["search", hdt_path.to_str().unwrap(), "--query", query];
    args.extend_from_slice(extra_args);

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(&args)
        .output()
        .expect("Failed to execute hdtc search");

    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Parse tab-delimited N-Triples output from `hdtc search` into a set of triple strings.
///
/// Each line has the form: `S\tP\tO\t.\n`. We treat each line as a canonical
/// triple key: `"S P O"` (using space for easy comparison).
///
/// We do NOT use an RDF parser here — we rely on the structural correctness of
/// the output format (each field is a well-formed N-Triples term).
fn parse_tab_triples(output: &str) -> HashSet<String> {
    output
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            // strip trailing \t.
            let line = line.strip_suffix("\t.").unwrap_or(line);
            let parts: Vec<&str> = line.splitn(3, '\t').collect();
            assert_eq!(
                parts.len(),
                3,
                "Expected 3 tab-separated fields in line: {line:?}"
            );
            format!("{} {} {}", parts[0], parts[1], parts[2])
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `???` returns all 8 triples from the representative dataset.
#[test]
fn test_search_scan_all() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? ? ?", &[]);
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        8,
        "Expected 8 triples from ??? scan, got {}: {triples:#?}",
        triples.len()
    );
}

/// `--count` flag prints the count and nothing else to stdout.
#[test]
fn test_search_count_all() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? ? ?", &["--count"]);
    assert!(ok, "hdtc search failed: {stderr}");
    let count: u64 = stdout.trim().parse().expect("Expected a number in stdout");
    assert_eq!(count, 8, "Expected count=8, got {count}");
}

/// `--limit 3` stops after 3 results.
#[test]
fn test_search_limit() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? ? ?", &["--limit", "3"]);
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        3,
        "Expected 3 triples with --limit 3, got {}",
        triples.len()
    );
}

/// `--offset 2 --limit 3` skips first 2 matches, then returns next 3.
#[test]
fn test_search_offset_with_limit() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? ? ?", &["--offset", "2", "--limit", "3"]);
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        3,
        "Expected 3 triples with --offset 2 --limit 3, got {}",
        triples.len()
    );
}

/// `--offset` past the end returns no rows.
#[test]
fn test_search_offset_past_end() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? ? ?", &["--offset", "100"]);
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(triples.len(), 0, "Expected 0 triples with large offset");
}

/// `S??` returns all triples with alice as subject (5 triples).
#[test]
fn test_search_subject_bound() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "<http://example.org/alice> ? ?", &[]);
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        5,
        "Expected 5 triples for alice, got {}: {triples:#?}",
        triples.len()
    );
    // All results must have alice as subject
    for triple in &triples {
        assert!(
            triple.starts_with("<http://example.org/alice>"),
            "Expected alice subject, got: {triple}"
        );
    }
}

/// `SP?` returns triples with alice as subject and `label` as predicate (2 triples).
#[test]
fn test_search_subject_predicate_bound() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "<http://example.org/alice> <http://example.org/label> ?",
        &[],
    );
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        2,
        "Expected 2 label triples for alice, got {}: {triples:#?}",
        triples.len()
    );
    for triple in &triples {
        assert!(triple.contains("<http://example.org/label>"));
    }
}

/// `S?O` returns triples where alice knows bob (1 triple).
#[test]
fn test_search_subject_object_bound() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "<http://example.org/alice> ? <http://example.org/bob>",
        &[],
    );
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple (alice ? bob), got {}: {triples:#?}",
        triples.len()
    );
}

/// `SPO` exact match returns exactly 1 triple.
#[test]
fn test_search_exact() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "<http://example.org/alice> <http://example.org/knows> <http://example.org/bob>",
        &[],
    );
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for SPO exact match, got {}: {triples:#?}",
        triples.len()
    );
}

/// A subject not in the dictionary returns 0 results (not an error).
#[test]
fn test_search_subject_not_found() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "<http://example.org/nobody> ? ?", &[]);
    assert!(ok, "hdtc search should succeed even with zero results: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        0,
        "Expected 0 results for unknown subject, got {triples:#?}"
    );
}

/// Subject not found with `--count` prints `0`.
#[test]
fn test_search_subject_not_found_count_outputs_zero() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "<http://example.org/nobody> ? ?",
        &["--count"],
    );
    assert!(ok, "hdtc search should succeed even with zero results: {stderr}");
    assert_eq!(
        stdout.trim(),
        "0",
        "Expected --count to print 0 for non-existent subject"
    );
}

/// A predicate not in the dictionary returns 0 results (not an error).
#[test]
fn test_search_predicate_not_found_returns_zero() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "<http://example.org/alice> <http://example.org/unknown> ?",
        &[],
    );
    assert!(ok, "hdtc search should succeed even with zero results: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        0,
        "Expected 0 results for unknown predicate, got {triples:#?}"
    );
}

/// Exact match for a triple that does not exist returns 0 results.
#[test]
fn test_search_exact_not_found() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "<http://example.org/alice> <http://example.org/knows> <http://example.org/alice>",
        &[],
    );
    assert!(ok, "hdtc search should succeed even with zero results: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        0,
        "Expected 0 results for non-existent triple, got {triples:#?}"
    );
}

/// `S?O` with a lang-tagged literal object (S?O pattern, Phase 1 scope).
#[test]
fn test_search_subject_literal_object() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    // alice ? "Alice"@en — subject+object bound (S?O)
    let (ok, stdout, stderr) =
        run_search(&hdt, "<http://example.org/alice> ? \"Alice\"@en", &[]);
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for alice ? \"Alice\"@en, got {}: {triples:#?}",
        triples.len()
    );
}

/// `S?O` with a typed literal object (S?O pattern, Phase 1 scope).
#[test]
fn test_search_subject_typed_literal_object() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    // alice ? "30"^^xsd:integer — subject+object bound (S?O)
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "<http://example.org/alice> ? \"30\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        &[],
    );
    assert!(ok, "hdtc search failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for alice ? \"30\"^^xsd:integer, got {}: {triples:#?}",
        triples.len()
    );
}

/// `??O` without an index and without `--no-index` returns an error.
#[test]
fn test_search_object_bound_requires_index() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, _stdout, stderr) = run_search(&hdt, "? ? \"Alice\"@en", &[]);
    assert!(!ok, "Expected hdtc search to fail for ??O without index");
    assert!(
        stderr.contains("index"),
        "Error message should mention index requirement: {stderr}"
    );
}

/// `--count` with `--limit` warns on stderr and counts all.
#[test]
fn test_search_count_with_limit_warns() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? ? ?", &["--count", "--limit", "3"]);
    assert!(ok, "hdtc search failed: {stderr}");
    let count: u64 = stdout.trim().parse().expect("Expected a number");
    // count should be total (8), not limited
    assert_eq!(count, 8, "Expected count=8 regardless of --limit when --count is set");
    assert!(
        stderr.contains("ignored") || stderr.contains("warn") || stderr.to_lowercase().contains("limit"),
        "Expected warning about --limit being ignored with --count: {stderr}"
    );
}

/// `--count` with `--offset` warns on stderr and counts all.
#[test]
fn test_search_count_with_offset_warns() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? ? ?", &["--count", "--offset", "3"]);
    assert!(ok, "hdtc search failed: {stderr}");
    let count: u64 = stdout.trim().parse().expect("Expected a number");
    assert_eq!(count, 8, "Expected count=8 regardless of --offset when --count is set");
    assert!(
        stderr.contains("ignored") || stderr.contains("warn") || stderr.to_lowercase().contains("offset"),
        "Expected warning about --offset being ignored with --count: {stderr}"
    );
}

/// `--output` writes triples to a file; stdout is empty.
#[test]
fn test_search_output_to_file() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let out = temp.path().join("results.nt");

    let (ok, stdout, stderr) = run_search(
        &hdt,
        "<http://example.org/alice> ? ?",
        &["--output", out.to_str().unwrap()],
    );
    assert!(ok, "hdtc search failed: {stderr}");
    assert!(stdout.is_empty(), "Expected no stdout output when --output is given");

    let content = std::fs::read_to_string(&out).unwrap();
    let triples = parse_tab_triples(&content);
    assert_eq!(triples.len(), 5, "Expected 5 triples in output file, got {triples:#?}");
}

/// `--count --output` writes the count to the file; stdout is empty.
#[test]
fn test_search_count_output_to_file() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let out = temp.path().join("count.txt");

    let (ok, stdout, stderr) = run_search(
        &hdt,
        "? ? ?",
        &["--count", "--output", out.to_str().unwrap()],
    );
    assert!(ok, "hdtc search failed: {stderr}");
    assert!(stdout.is_empty(), "Expected no stdout when --count --output is set");

    let content = std::fs::read_to_string(&out).unwrap();
    let count: u64 = content.trim().parse().expect("Expected a number in output file");
    assert_eq!(count, 8, "Expected count=8 in output file, got {count}");
}

// ---------------------------------------------------------------------------
// Phase 2 helpers and tests: `?P?` via predicate index
// ---------------------------------------------------------------------------

/// Create an HDT file AND its index in `temp_dir`.
fn make_representative_hdt_with_index(temp_dir: &Path) -> std::path::PathBuf {
    let hdt = make_representative_hdt(temp_dir);

    let status = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["index", hdt.to_str().unwrap()])
        .output()
        .expect("Failed to execute hdtc index");

    assert!(
        status.status.success(),
        "hdtc index failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );

    hdt
}

/// Create a skewed HDT+index where one object has a very large group.
///
/// Returns `(hdt_path, common_object_term, hot_predicate_term, expected_hot_count_on_common)`.
fn make_skewed_hdt_with_index(temp_dir: &Path) -> (std::path::PathBuf, String, String, u64) {
    let common_object = "<http://example.org/commonObject>".to_string();
    let hot_predicate = "<http://example.org/pHot>".to_string();

    // Ensure object-group size > 4096 so search_object_bound chunking is exercised.
    let common_group_size = 10_000u64;
    let mut expected_hot_on_common = 0u64;

    let mut nt = String::new();

    // Large group for one object, with mixed predicates.
    for i in 0..common_group_size {
        let subj = format!("<http://example.org/s{i}>");
        let pred = if i % 64 == 0 {
            expected_hot_on_common += 1;
            hot_predicate.clone()
        } else {
            format!("<http://example.org/p{}>", i % 31)
        };
        nt.push_str(&format!("{subj} {pred} {common_object} .\n"));
    }

    // Make hot predicate globally very frequent across many other objects so
    // `count(P)` is much larger than count(commonObject), encouraging ?PO
    // routing to the object-index path.
    for i in 0..50_000u64 {
        let subj = format!("<http://example.org/h{i}>");
        let obj = format!("<http://example.org/o{}>", i % 20_000);
        nt.push_str(&format!("{subj} {hot_predicate} {obj} .\n"));
    }

    let nt_path = temp_dir.join("skewed.nt");
    write_file(&nt_path, nt.as_bytes());

    let hdt_path = temp_dir.join("skewed.hdt");
    let work_dir = temp_dir.join("work-skewed");

    let create = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            nt_path.to_str().unwrap(),
            "-o",
            hdt_path.to_str().unwrap(),
            "--base-uri",
            "http://example.org/dataset",
            "--temp-dir",
            work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc create for skewed dataset");

    assert!(
        create.status.success(),
        "hdtc create failed for skewed dataset: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let index = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["index", hdt_path.to_str().unwrap()])
        .output()
        .expect("Failed to execute hdtc index for skewed dataset");

    assert!(
        index.status.success(),
        "hdtc index failed for skewed dataset: {}",
        String::from_utf8_lossy(&index.stderr)
    );

    (
        hdt_path,
        common_object,
        hot_predicate,
        expected_hot_on_common,
    )
}

/// `?P?` with the `knows` predicate returns exactly 2 triples (alice→bob, bob→alice).
#[test]
fn test_search_predicate_bound_knows() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? <http://example.org/knows> ?", &[]);
    assert!(ok, "hdtc search ?P? failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        2,
        "Expected 2 triples for ?-knows-?, got {}: {triples:#?}",
        triples.len()
    );
    for triple in &triples {
        assert!(
            triple.contains("<http://example.org/knows>"),
            "Expected knows predicate in: {triple}"
        );
    }
}

/// `?P?` with the `age` predicate returns exactly 1 triple.
#[test]
fn test_search_predicate_bound_age() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? <http://example.org/age> ?", &[]);
    assert!(ok, "hdtc search ?P? failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for ?-age-?, got {}: {triples:#?}",
        triples.len()
    );
}

/// `?P?` with the `label` predicate returns exactly 2 triples (two language tags).
#[test]
fn test_search_predicate_bound_label() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? <http://example.org/label> ?", &[]);
    assert!(ok, "hdtc search ?P? failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        2,
        "Expected 2 label triples, got {}: {triples:#?}",
        triples.len()
    );
    for triple in &triples {
        assert!(triple.contains("<http://example.org/label>"));
    }
}

/// `?P?` with the `name` predicate returns exactly 2 triples.
#[test]
fn test_search_predicate_bound_name() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? <http://example.org/name> ?", &[]);
    assert!(ok, "hdtc search ?P? failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        2,
        "Expected 2 name triples, got {}: {triples:#?}",
        triples.len()
    );
}

/// `?P?` with a predicate not in the dictionary returns 0 results (not an error).
#[test]
fn test_search_predicate_bound_unknown() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? <http://example.org/nonexistent> ?", &[]);
    assert!(ok, "hdtc search should succeed with 0 results: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        0,
        "Expected 0 results for unknown predicate, got {triples:#?}"
    );
}

/// `?P?` with `--count` emits only the count.
#[test]
fn test_search_predicate_bound_count() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? <http://example.org/knows> ?", &["--count"]);
    assert!(ok, "hdtc search ?P? --count failed: {stderr}");
    let count: u64 = stdout.trim().parse().expect("Expected a number in stdout");
    assert_eq!(count, 2, "Expected count=2 for ?-knows-?, got {count}");
}

/// `?P?` with `--limit 1` stops after 1 result.
#[test]
fn test_search_predicate_bound_limit() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? <http://example.org/knows> ?", &["--limit", "1"]);
    assert!(ok, "hdtc search ?P? --limit failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple with --limit 1, got {}: {triples:#?}",
        triples.len()
    );
}

/// `?P?` with `--offset 1` skips the first match and returns one remaining row.
#[test]
fn test_search_predicate_bound_offset() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? <http://example.org/knows> ?", &["--offset", "1"]);
    assert!(ok, "hdtc search ?P? --offset failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for ?-knows-? with --offset 1, got {}: {triples:#?}",
        triples.len()
    );
}

/// `?P?` with `--no-index` falls back to sequential scan and still returns correct results.
#[test]
fn test_search_predicate_bound_no_index_fallback() {
    let temp = tempfile::tempdir().unwrap();
    // Note: no index created here — --no-index forces sequential fallback.
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? <http://example.org/knows> ?", &["--no-index"]);
    assert!(ok, "hdtc search ?P? --no-index failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        2,
        "Expected 2 triples with --no-index fallback, got {}: {triples:#?}",
        triples.len()
    );
}

/// `?P?` without an index and without `--no-index` returns an error.
#[test]
fn test_search_predicate_bound_requires_index() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, _stdout, stderr) = run_search(&hdt, "? <http://example.org/knows> ?", &[]);
    assert!(!ok, "Expected hdtc search to fail for ?P? without index");
    assert!(
        stderr.contains("index"),
        "Error message should mention index requirement: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Phase 3 tests: `??O` via object index
// ---------------------------------------------------------------------------

/// `??O` with a shared URI object returns correct results.
#[test]
fn test_search_object_bound_shared_uri() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? ? <http://example.org/alice>", &[]);
    assert!(ok, "hdtc search ??O failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for ??-alice, got {}: {triples:#?}",
        triples.len()
    );
    for triple in &triples {
        assert!(
            triple.contains("<http://example.org/alice>"),
            "Expected alice as object in: {triple}"
        );
    }
}

/// `??O` with an object-only URI returns correct results.
#[test]
fn test_search_object_bound_uri_only() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? ? <http://example.org/Thing>", &[]);
    assert!(ok, "hdtc search ??O failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for ??-Thing, got {}: {triples:#?}",
        triples.len()
    );
}

/// `??O` with a language-tagged literal returns correct results.
#[test]
fn test_search_object_bound_lang_literal() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(&hdt, "? ? \"Alice\"@en", &[]);
    assert!(ok, "hdtc search ??O failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for ??-\"Alice\"@en, got {}: {triples:#?}",
        triples.len()
    );
}

/// `??O` with a typed literal returns correct results.
#[test]
fn test_search_object_bound_typed_literal() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "? ? \"30\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        &[],
    );
    assert!(ok, "hdtc search ??O failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for ??-\"30\"^^xsd:integer, got {}: {triples:#?}",
        triples.len()
    );
}

/// `??O` with unknown object returns 0 results (not an error).
#[test]
fn test_search_object_bound_unknown() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? ? <http://example.org/nonexistent>", &[]);
    assert!(ok, "hdtc search should succeed with 0 results: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        0,
        "Expected 0 results for unknown object, got {triples:#?}"
    );
}

/// `??O` with `--count`.
#[test]
fn test_search_object_bound_count() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? ? <http://example.org/alice>", &["--count"]);
    assert!(ok, "hdtc search ??O --count failed: {stderr}");
    let count: u64 = stdout.trim().parse().expect("Expected a number in stdout");
    assert_eq!(count, 1, "Expected count=1 for ??-alice, got {count}");
}

/// `??O` with `--no-index` falls back to sequential scan.
#[test]
fn test_search_object_bound_no_index_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) =
        run_search(&hdt, "? ? <http://example.org/alice>", &["--no-index"]);
    assert!(ok, "hdtc search ??O --no-index failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple with --no-index fallback, got {}: {triples:#?}",
        triples.len()
    );
}

// ---------------------------------------------------------------------------
// Phase 3+4 tests: `?PO` via selectivity routing
// ---------------------------------------------------------------------------

/// `?PO` returns correct results.
#[test]
fn test_search_predicate_object_bound() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "? <http://example.org/knows> <http://example.org/alice>",
        &[],
    );
    assert!(ok, "hdtc search ?PO failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for ?-knows-alice, got {}: {triples:#?}",
        triples.len()
    );
}

/// `?PO` with a literal object returns correct results.
#[test]
fn test_search_predicate_object_bound_literal() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "? <http://example.org/name> \"Alice\"",
        &[],
    );
    assert!(ok, "hdtc search ?PO failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple for ?-name-\"Alice\", got {}: {triples:#?}",
        triples.len()
    );
}

/// `?PO` with unknown predicate returns 0 results.
#[test]
fn test_search_predicate_object_bound_unknown_pred() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "? <http://example.org/nonexistent> <http://example.org/alice>",
        &[],
    );
    assert!(ok, "hdtc search should succeed with 0 results: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(triples.len(), 0, "Expected 0 results for unknown predicate");
}

/// `?PO` with unknown object returns 0 results.
#[test]
fn test_search_predicate_object_bound_unknown_obj() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "? <http://example.org/knows> <http://example.org/nonexistent>",
        &[],
    );
    assert!(ok, "hdtc search should succeed with 0 results: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(triples.len(), 0, "Expected 0 results for unknown object");
}

/// `?PO` with `--count`.
#[test]
fn test_search_predicate_object_bound_count() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt_with_index(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "? <http://example.org/knows> <http://example.org/alice>",
        &["--count"],
    );
    assert!(ok, "hdtc search ?PO --count failed: {stderr}");
    let count: u64 = stdout.trim().parse().expect("Expected a number in stdout");
    assert_eq!(count, 1, "Expected count=1 for ?-knows-alice, got {count}");
}

/// `?PO` with `--no-index` falls back to sequential scan.
#[test]
fn test_search_predicate_object_bound_no_index_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, stdout, stderr) = run_search(
        &hdt,
        "? <http://example.org/knows> <http://example.org/alice>",
        &["--no-index"],
    );
    assert!(ok, "hdtc search ?PO --no-index failed: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        1,
        "Expected 1 triple with --no-index fallback, got {}: {triples:#?}",
        triples.len()
    );
}

/// `?PO` without an index and without `--no-index` returns an error.
#[test]
fn test_search_predicate_object_bound_requires_index() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let (ok, _stdout, stderr) = run_search(
        &hdt,
        "? <http://example.org/knows> <http://example.org/alice>",
        &[],
    );
    assert!(!ok, "Expected hdtc search to fail for ?PO without index");
    assert!(
        stderr.contains("index"),
        "Error message should mention index requirement: {stderr}"
    );
}

/// Stress-style regression: large object group is processed correctly with
/// chunked ??O streaming (group size > 4096) without full in-memory group load.
#[test]
fn test_search_object_bound_large_group_chunked() {
    let temp = tempfile::tempdir().unwrap();
    let (hdt, common_object, _hot_predicate, _expected_hot_on_common) =
        make_skewed_hdt_with_index(temp.path());

    let query = format!("? ? {common_object}");
    let (ok, stdout, stderr) = run_search(&hdt, &query, &["--count"]);
    assert!(ok, "hdtc search ??O --count failed on skewed data: {stderr}");

    let count: u64 = stdout.trim().parse().expect("Expected numeric count");
    assert_eq!(
        count, 10_000,
        "Expected full object-group count for common object"
    );

    // Also verify limit path on same large group.
    let (ok, stdout, stderr) = run_search(&hdt, &query, &["--limit", "25"]);
    assert!(ok, "hdtc search ??O --limit failed on skewed data: {stderr}");
    let triples = parse_tab_triples(&stdout);
    assert_eq!(
        triples.len(),
        25,
        "Expected limited results from large ??O group"
    );
}

/// Stress-style regression: `?PO` stays correct under skewed selectivity where
/// the predicate is globally hot but only sparsely matches the queried object.
#[test]
fn test_search_predicate_object_bound_skewed_selectivity() {
    let temp = tempfile::tempdir().unwrap();
    let (hdt, common_object, hot_predicate, expected_hot_on_common) =
        make_skewed_hdt_with_index(temp.path());

    let query = format!("? {hot_predicate} {common_object}");
    let (ok, stdout, stderr) = run_search(&hdt, &query, &["--count"]);
    assert!(ok, "hdtc search ?PO --count failed on skewed data: {stderr}");

    let count: u64 = stdout.trim().parse().expect("Expected numeric count");
    assert_eq!(
        count, expected_hot_on_common,
        "Expected ?PO count on skewed selectivity dataset"
    );
}

/// Dump output is tab-delimited and parseable as N-Triples.
#[test]
fn test_dump_tab_delimited() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = make_representative_hdt(temp.path());
    let output_nt = temp.path().join("dumped.nt");

    let status = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "dump",
            hdt.to_str().unwrap(),
            "-o",
            output_nt.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc dump");

    assert!(
        status.status.success(),
        "hdtc dump failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );

    let content = std::fs::read_to_string(&output_nt).unwrap();
    // Each line should contain tabs (our new format).
    let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 8, "Expected 8 triples in dump output");
    for line in &lines {
        assert!(
            line.contains('\t'),
            "Expected tab-delimited format in dump output, got: {line:?}"
        );
        assert!(
            line.ends_with("\t."),
            "Expected line to end with '\\t.' in dump output, got: {line:?}"
        );
    }
}
