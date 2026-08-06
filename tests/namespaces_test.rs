//! Integration tests for `hdtc namespaces`.

mod common;

use common::{run_hdtc_to_path, write_file};
use serde_json::Value;
use std::path::Path;
use std::process::Command;

const NAMESPACE_NT: &str = r#"<http://example.org/s1> <http://example.org/p> <http://example.org/o1> .
<http://example.org/shared> <http://example.org/shared> <http://example.org/shared> .
<http://example.org/s1> <http://other.example/p> <http://other.example/o> .
_:blank <urn:test:p> "literal" .
<http://example.org/s1> <mailto:predicate@example.org> <http://example.org/o1> .
"#;

fn build_fixture(temp_dir: &Path) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let input = temp_dir.join("namespaces.nt");
    write_file(&input, NAMESPACE_NT.as_bytes());
    let hdt = run_hdtc_to_path(temp_dir, &[&input], "namespaces.hdt");

    let yaml = temp_dir.join("prefixes.yaml");
    write_file(
        &yaml,
        br#"broad: "http://"
duplicate: "http://example.org/"
ex: "http://wrong.example/"
missing: "http://missing.example/"
urn: "urn:test:"
"#,
    );
    let json = temp_dir.join("overrides.json");
    write_file(
        &json,
        br#"{"ex":"http://example.org/","other":"http://other.example/"}"#,
    );
    (hdt, yaml, json)
}

fn run_namespaces(hdt: &Path, yaml: &Path, json: &Path, extra: &[&str]) -> std::process::Output {
    let mut args = vec![
        "namespaces",
        hdt.to_str().unwrap(),
        "--prefixes",
        yaml.to_str().unwrap(),
        "--prefixes",
        json.to_str().unwrap(),
    ];
    args.extend_from_slice(extra);
    Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(args)
        .output()
        .expect("run hdtc namespaces")
}

#[test]
fn reports_role_counts_residuals_and_graph_distinct_union() {
    let temp = tempfile::tempdir().unwrap();
    let (hdt, yaml, json) = build_fixture(temp.path());
    let output = run_namespaces(&hdt, &yaml, &json, &[]);
    assert!(
        output.status.success(),
        "namespaces failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["roles"]["subject"]["distinct_iris"], 2);
    assert_eq!(document["roles"]["subject"]["matched"], 2);
    assert_eq!(document["roles"]["subject"]["residual"], 0);
    assert_eq!(document["roles"]["predicate"]["distinct_iris"], 5);
    assert_eq!(document["roles"]["predicate"]["matched"], 4);
    assert_eq!(document["roles"]["predicate"]["residual"], 1);
    assert_eq!(document["roles"]["object"]["distinct_iris"], 3);
    assert_eq!(document["roles"]["object"]["matched"], 3);
    assert_eq!(document["roles"]["object"]["residual"], 0);

    let rows = document["namespaces"].as_array().unwrap();
    let prefixes: Vec<_> = rows
        .iter()
        .map(|row| row["prefix"].as_str().unwrap())
        .collect();
    assert_eq!(prefixes, ["broad", "duplicate", "ex", "other", "urn"]);

    let row = |prefix: &str| rows.iter().find(|row| row["prefix"] == prefix).unwrap();
    assert_eq!(row("broad")["subject"], 2);
    assert_eq!(row("broad")["predicate"], 3);
    assert_eq!(row("broad")["object"], 3);
    assert_eq!(row("broad")["distinct_iris"], 6);
    assert_eq!(row("broad")["example"], "http://example.org/o1");

    // shared occurs in subject and object roles and is also a predicate. The
    // graph-wide count is the four unique example.org IRIs, not 2 + 2 + 2.
    assert_eq!(row("ex")["subject"], 2);
    assert_eq!(row("ex")["predicate"], 2);
    assert_eq!(row("ex")["object"], 2);
    assert_eq!(row("ex")["distinct_iris"], 4);
    assert_eq!(row("duplicate")["distinct_iris"], 4);
    assert_eq!(row("other")["distinct_iris"], 2);
    assert_eq!(row("urn")["distinct_iris"], 1);
    assert_eq!(row("urn")["predicate"], 1);
    assert_eq!(row("ex")["namespace"], "http://example.org/");
    assert!(
        document["prefix_table"]["version"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
}

#[test]
fn writes_yaml_without_examples_to_a_file() {
    let temp = tempfile::tempdir().unwrap();
    let (hdt, yaml, json) = build_fixture(temp.path());
    let result_path = temp.path().join("namespaces.yaml");
    let output = run_namespaces(
        &hdt,
        &yaml,
        &json,
        &[
            "--format",
            "yaml",
            "--no-example",
            "--output",
            result_path.to_str().unwrap(),
        ],
    );
    assert!(
        output.status.success(),
        "namespaces failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());

    let bytes = std::fs::read(result_path).unwrap();
    let document: serde_yaml::Value = serde_yaml::from_slice(&bytes).unwrap();
    let rows = document["namespaces"].as_sequence().unwrap();
    assert!(rows.iter().all(|row| row.get("example").is_none()));
    assert_eq!(rows[2]["distinct_iris"].as_u64(), Some(4));
}

#[test]
fn rejects_non_mapping_prefix_tables() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("one.nt");
    write_file(
        &input,
        b"<http://example/s> <http://example/p> <http://example/o> .\n",
    );
    let hdt = run_hdtc_to_path(temp.path(), &[&input], "one.hdt");
    let prefixes = temp.path().join("bad.json");
    write_file(&prefixes, br#"["http://example/"]"#);

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "namespaces",
            hdt.to_str().unwrap(),
            "--prefixes",
            prefixes.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Invalid JSON prefix table"));
}
