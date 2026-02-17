//! Compatibility tests: verify hdtc output is readable by independent HDT implementations.
//!
//! Layer 1: `hdt` Rust crate (runs on every `cargo test`)
//! Layer 2: hdt-java tools (gated behind `#[ignore]`, requires Java)

mod common;

use common::{run_hdtc_to_path, write_file, REPRESENTATIVE_NT, REPRESENTATIVE_TRIPLE_COUNT};
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

/// Generate the representative HDT file in a temp directory and return the path.
fn generate_representative_hdt() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let nt_path = temp.path().join("input.nt");
    write_file(&nt_path, REPRESENTATIVE_NT.as_bytes());
    let hdt_path = run_hdtc_to_path(temp.path(), &[&nt_path], "representative.hdt");
    (temp, hdt_path)
}

// ---------------------------------------------------------------------------
// Layer 1: hdt Rust crate
// ---------------------------------------------------------------------------

/// The most critical test: the `hdt` crate can read our HDT file at all.
#[test]
fn test_hdt_crate_reads_hdtc_output() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).expect("open HDT file");
    let _hdt = hdt::Hdt::read(BufReader::new(file)).expect("hdt crate should read our HDT");
}

/// Total triple count matches our expected count.
#[test]
fn test_hdt_crate_total_triple_count() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let count = hdt.triples_with_pattern(None, None, None).count();
    assert_eq!(
        count, REPRESENTATIVE_TRIPLE_COUNT,
        "expected {REPRESENTATIVE_TRIPLE_COUNT} triples, got {count}"
    );
}

/// Query a specific URI-URI-URI triple: alice knows bob.
#[test]
fn test_hdt_crate_specific_uri_triple() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("http://example.org/alice"),
            Some("http://example.org/knows"),
            Some("http://example.org/bob"),
        )
        .collect();
    assert_eq!(results.len(), 1, "alice-knows-bob should return exactly 1 triple");
}

/// Query a plain literal: alice's name is "Alice".
#[test]
fn test_hdt_crate_plain_literal() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("http://example.org/alice"),
            Some("http://example.org/name"),
            Some("\"Alice\""),
        )
        .collect();
    assert_eq!(results.len(), 1, "alice-name-Alice should return exactly 1 triple");
}

/// Query a typed literal: alice's age is "30"^^xsd:integer.
#[test]
fn test_hdt_crate_typed_literal() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("http://example.org/alice"),
            Some("http://example.org/age"),
            Some("\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        )
        .collect();
    assert_eq!(results.len(), 1, "alice-age-30^^xsd:integer should return 1 triple");
}

/// Query a language-tagged literal: alice's label is "Alice"@en.
#[test]
fn test_hdt_crate_language_tagged_literal() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("http://example.org/alice"),
            Some("http://example.org/label"),
            Some("\"Alice\"@en"),
        )
        .collect();
    assert_eq!(results.len(), 1, "alice-label-Alice@en should return 1 triple");
}

/// Query a blank node triple. hdtc disambiguates blank nodes with a per-file prefix (f0_).
#[test]
fn test_hdt_crate_blank_node() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    // Our blank node _:b1 becomes _:f0_b1 after hdtc's disambiguation.
    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("_:f0_b1"),
            Some("http://example.org/type"),
            Some("http://example.org/Thing"),
        )
        .collect();
    assert_eq!(results.len(), 1, "blank node triple should return 1 triple");
}

/// Wildcard predicate: all "knows" triples should return 2 (alice->bob, bob->alice).
#[test]
fn test_hdt_crate_wildcard_predicate() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(None, Some("http://example.org/knows"), None)
        .collect();
    assert_eq!(results.len(), 2, "?-knows-? should return 2 triples");
}

/// Verify all triples are present by collecting and sorting them.
#[test]
fn test_hdt_crate_all_triples_content() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let mut triples: Vec<[String; 3]> = hdt
        .triples_with_pattern(None, None, None)
        .map(|t: [Arc<str>; 3]| [t[0].to_string(), t[1].to_string(), t[2].to_string()])
        .collect();
    triples.sort();

    let mut expected = [
        ["http://example.org/alice", "http://example.org/name", "\"Alice\""],
        ["http://example.org/alice", "http://example.org/knows", "http://example.org/bob"],
        ["http://example.org/bob", "http://example.org/name", "\"Bob\""],
        ["http://example.org/bob", "http://example.org/knows", "http://example.org/alice"],
        [
            "http://example.org/alice",
            "http://example.org/age",
            "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        ],
        ["http://example.org/alice", "http://example.org/label", "\"Alice\"@en"],
        ["http://example.org/alice", "http://example.org/label", "\"Alicia\"@es"],
        ["_:f0_b1", "http://example.org/type", "http://example.org/Thing"],
    ];
    expected.sort();

    assert_eq!(triples.len(), expected.len(), "triple count mismatch");
    for (actual, exp) in triples.iter().zip(expected.iter()) {
        assert_eq!(
            actual, exp,
            "triple mismatch: got {:?}, expected {:?}",
            actual, exp
        );
    }
}

// ---------------------------------------------------------------------------
// Layer 2: hdt-java (requires Java, gated behind #[ignore])
// ---------------------------------------------------------------------------

use std::path::Path;
use std::process::Command;
use std::sync::Once;

const HDT_JAVA_VERSION: &str = "3.0.10";

static DOWNLOAD_ONCE: Once = Once::new();

/// Ensure the hdt-java distribution is downloaded and extracted.
/// Returns the path to the hdt-java directory containing `lib/` and `bin/`.
fn ensure_hdt_java() -> PathBuf {
    let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-data")
        .join("hdt-java");

    let marker = base_dir.join(".downloaded");

    DOWNLOAD_ONCE.call_once(|| {
        if marker.exists() {
            return;
        }

        std::fs::create_dir_all(&base_dir).expect("create hdt-java dir");

        let url = format!(
            "https://github.com/rdfhdt/hdt-java/releases/download/v{HDT_JAVA_VERSION}/rdfhdt.tar.gz"
        );

        eprintln!("Downloading hdt-java from {url}...");

        let mut curl = Command::new("curl")
            .args(["-fsSL", &url])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("Failed to start curl");

        let curl_stdout = curl.stdout.take().unwrap();

        let tar_output = Command::new("tar")
            .args(["xzf", "-", "--strip-components=1"])
            .current_dir(&base_dir)
            .stdin(curl_stdout)
            .output()
            .expect("Failed to extract hdt-java tarball");

        curl.wait().expect("Failed to wait for curl");

        assert!(
            tar_output.status.success(),
            "tar extraction failed: {}",
            String::from_utf8_lossy(&tar_output.stderr)
        );

        // Write marker file
        std::fs::write(&marker, "ok").expect("write marker");
        eprintln!("hdt-java downloaded to {}", base_dir.display());
    });

    assert!(marker.exists(), "hdt-java download marker not found");
    base_dir
}

/// Build the Java classpath from the hdt-java lib directory.
fn hdt_java_classpath(hdt_java_dir: &Path) -> String {
    let lib_dir = hdt_java_dir.join("lib");
    let jars: Vec<String> = std::fs::read_dir(&lib_dir)
        .expect("read hdt-java lib dir")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "jar") {
                Some(path.to_str().unwrap().to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(!jars.is_empty(), "No jar files found in {}", lib_dir.display());
    jars.join(":")
}

/// hdt-java HDTVerify: structural integrity check.
#[test]
#[ignore]
fn test_hdtjava_verify() {
    let hdt_java_dir = ensure_hdt_java();
    let (_temp, hdt_path) = generate_representative_hdt();
    let classpath = hdt_java_classpath(&hdt_java_dir);

    let output = Command::new("java")
        .args([
            "-cp",
            &classpath,
            "org.rdfhdt.hdt.tools.HDTVerify",
            hdt_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to run HDTVerify");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("HDTVerify stdout:\n{stdout}");
    eprintln!("HDTVerify stderr:\n{stderr}");
    assert!(
        output.status.success(),
        "HDTVerify failed with exit code {:?}\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );
}

/// hdt-java HDT2RDF round-trip: convert HDT back to N-Triples and compare.
#[test]
#[ignore]
fn test_hdtjava_roundtrip_ntriples() {
    let hdt_java_dir = ensure_hdt_java();
    let (temp, hdt_path) = generate_representative_hdt();
    let classpath = hdt_java_classpath(&hdt_java_dir);
    let output_nt = temp.path().join("roundtrip.nt");

    let output = Command::new("java")
        .args([
            "-cp",
            &classpath,
            "org.rdfhdt.hdt.tools.HDT2RDF",
            hdt_path.to_str().unwrap(),
            output_nt.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to run HDT2RDF");

    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("HDT2RDF stderr:\n{stderr}");
    assert!(
        output.status.success(),
        "HDT2RDF failed with exit code {:?}\nstderr: {stderr}",
        output.status.code()
    );

    // Read and sort actual output lines
    let actual_content = std::fs::read_to_string(&output_nt).expect("read roundtrip output");
    let mut actual_lines: Vec<&str> = actual_content
        .lines()
        .filter(|l| !l.is_empty())
        .collect();
    actual_lines.sort();

    // Build expected N-Triples with hdtc's blank node disambiguation (f0_ prefix)
    let expected_nt = REPRESENTATIVE_NT.replace("_:b1", "_:f0_b1");
    let mut expected_lines: Vec<&str> = expected_nt
        .lines()
        .filter(|l| !l.is_empty())
        .collect();
    expected_lines.sort();

    // hdt-java may output blank nodes with its own prefix, so normalize blank node IDs.
    // We compare structure but allow different blank node prefixes.
    let normalize_bnode = |line: &str| -> String {
        // Replace any _:XXXX pattern with a canonical form for comparison
        let mut result = line.to_string();
        if let Some(start) = result.find("_:") {
            let end = result[start + 2..]
                .find(|c: char| c.is_whitespace())
                .map_or(result.len(), |e| start + 2 + e);
            result.replace_range(start..end, "_:BNODE");
        }
        result
    };

    let mut actual_normalized: Vec<String> = actual_lines.iter().map(|l| normalize_bnode(l)).collect();
    let mut expected_normalized: Vec<String> = expected_lines.iter().map(|l| normalize_bnode(l)).collect();
    actual_normalized.sort();
    expected_normalized.sort();

    assert_eq!(
        actual_normalized.len(),
        expected_normalized.len(),
        "line count mismatch: got {}, expected {}",
        actual_normalized.len(),
        expected_normalized.len()
    );

    for (actual, expected) in actual_normalized.iter().zip(expected_normalized.iter()) {
        assert_eq!(
            actual, expected,
            "N-Triples mismatch:\n  actual:   {actual}\n  expected: {expected}"
        );
    }
}
