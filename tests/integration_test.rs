mod common;

use common::write_file;
use std::io::Write;
use std::path::Path;
use std::process::Command;

/// Helper: run hdtc on given input files and return (success, stderr, hdt_bytes).
fn run_hdtc(temp_dir: &Path, inputs: &[&Path], hdt_name: &str) -> (bool, String, Vec<u8>) {
    let hdt_path = temp_dir.join(hdt_name);
    let work_dir = temp_dir.join("work");

    // Build args with "create" subcommand as first argument
    let mut args: Vec<String> = vec!["create".to_string()];
    args.extend(
        inputs
            .iter()
            .map(|p| p.to_str().unwrap().to_string())
    );
    args.extend([
        "-o".to_string(),
        hdt_path.to_str().unwrap().to_string(),
        "--base-uri".to_string(),
        "http://example.org/dataset".to_string(),
        "--temp-dir".to_string(),
        work_dir.to_str().unwrap().to_string(),
    ]);

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(&args)
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    eprintln!("hdtc stderr:\n{stderr}");

    let hdt_bytes = if hdt_path.exists() {
        std::fs::read(&hdt_path).unwrap()
    } else {
        Vec::new()
    };

    (output.status.success(), stderr, hdt_bytes)
}

/// Helper: count occurrences of the $HDT magic in the HDT bytes.
/// There should be exactly 4: Global, Header, Dictionary, Triples.
fn count_hdt_magic(data: &[u8]) -> usize {
    let magic = b"$HDT";
    data.windows(4).filter(|w| *w == magic).count()
}

/// Helper: find the position of a null byte in data starting at offset.
fn find_null(data: &[u8], offset: usize) -> usize {
    data[offset..]
        .iter()
        .position(|&b| b == 0)
        .map(|p| offset + p)
        .expect("No null terminator found")
}

/// Helper: parse a Control Information block at the given offset.
/// Returns (control_type, format_string, properties_string, offset_after_crc16).
fn parse_control_info(data: &[u8], offset: usize) -> (u8, String, String, usize) {
    assert_eq!(&data[offset..offset + 4], b"$HDT", "Missing $HDT magic");
    let control_type = data[offset + 4];
    let format_end = find_null(data, offset + 5);
    let format = String::from_utf8(data[offset + 5..format_end].to_vec()).unwrap();
    let props_end = find_null(data, format_end + 1);
    let props = String::from_utf8(data[format_end + 1..props_end].to_vec()).unwrap();
    // Skip null + 2 bytes CRC16
    let after = props_end + 1 + 2;
    (control_type, format, props, after)
}

// =============================================================================
// Original tests
// =============================================================================

/// Create a temp N-Triples file and convert it to HDT using the hdtc binary.
#[test]
fn test_end_to_end_ntriples_to_hdt() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create a small N-Triples file
    let nt_path = temp_dir.path().join("test.nt");
    let hdt_path = temp_dir.path().join("test.hdt");

    let mut f = std::fs::File::create(&nt_path).unwrap();
    writeln!(
        f,
        r#"<http://example.org/alice> <http://example.org/name> "Alice" ."#
    )
    .unwrap();
    writeln!(
        f,
        r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> ."#
    )
    .unwrap();
    writeln!(
        f,
        r#"<http://example.org/bob> <http://example.org/name> "Bob" ."#
    )
    .unwrap();
    writeln!(
        f,
        r#"<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> ."#
    )
    .unwrap();
    writeln!(f, r#"<http://example.org/alice> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> ."#).unwrap();
    f.flush().unwrap();
    drop(f);

    // Run hdtc
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            nt_path.to_str().unwrap(),
            "-o",
            hdt_path.to_str().unwrap(),
            "--base-uri",
            "http://example.org/dataset",
            "--temp-dir",
            temp_dir.path().join("work").to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    // Print stderr for debugging
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("hdtc stderr:\n{stderr}");

    assert!(output.status.success(), "hdtc failed: {stderr}");

    // Verify HDT file exists and is non-empty
    let metadata = std::fs::metadata(&hdt_path).expect("HDT file should exist");
    assert!(metadata.len() > 0, "HDT file should be non-empty");

    // Verify it starts with $HDT magic
    let hdt_bytes = std::fs::read(&hdt_path).unwrap();
    assert!(
        hdt_bytes.starts_with(b"$HDT"),
        "HDT file should start with $HDT magic bytes"
    );

    eprintln!("HDT file size: {} bytes", metadata.len());
}

/// Test with multiple input files and blank node disambiguation.
#[test]
fn test_multiple_files_blank_node_disambiguation() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Two files both using _:b1
    let nt1_path = temp_dir.path().join("file1.nt");
    let nt2_path = temp_dir.path().join("file2.nt");
    let hdt_path = temp_dir.path().join("multi.hdt");

    let mut f1 = std::fs::File::create(&nt1_path).unwrap();
    writeln!(f1, r#"_:b1 <http://example.org/p> "from file 1" ."#).unwrap();
    drop(f1);

    let mut f2 = std::fs::File::create(&nt2_path).unwrap();
    writeln!(f2, r#"_:b1 <http://example.org/p> "from file 2" ."#).unwrap();
    drop(f2);

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            nt1_path.to_str().unwrap(),
            nt2_path.to_str().unwrap(),
            "-o",
            hdt_path.to_str().unwrap(),
            "--temp-dir",
            temp_dir.path().join("work").to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("hdtc stderr:\n{stderr}");
    assert!(output.status.success(), "hdtc failed: {stderr}");

    // Verify HDT file produced with correct magic
    let hdt_bytes = std::fs::read(&hdt_path).unwrap();
    assert!(hdt_bytes.starts_with(b"$HDT"));

    // Verify stderr reports 2 triples (blank nodes disambiguated -> different subjects)
    assert!(
        stderr.contains("2 triples"),
        "Expected '2 triples' in stderr, got:\n{stderr}"
    );
}

/// Test that a term appearing as both predicate and subject works correctly.
/// In RDF, a property URI can also appear as a subject or object.
#[test]
fn test_predicate_also_used_as_subject() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");
    let hdt_path = temp_dir.path().join("test.hdt");

    let mut f = std::fs::File::create(&nt_path).unwrap();
    // <likes> is used as a predicate...
    writeln!(
        f,
        r#"<http://example.org/alice> <http://example.org/likes> <http://example.org/bob> ."#
    )
    .unwrap();
    // ...and also as a subject (describing the property itself)
    writeln!(
        f,
        r#"<http://example.org/likes> <http://www.w3.org/2000/01/rdf-schema#label> "likes" ."#
    )
    .unwrap();
    // Another normal triple
    writeln!(
        f,
        r#"<http://example.org/bob> <http://example.org/likes> <http://example.org/alice> ."#
    )
    .unwrap();
    f.flush().unwrap();
    drop(f);

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            nt_path.to_str().unwrap(),
            "-o",
            hdt_path.to_str().unwrap(),
            "--temp-dir",
            temp_dir.path().join("work").to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("hdtc stderr:\n{stderr}");
    assert!(output.status.success(), "hdtc failed: {stderr}");

    // All 3 triples should be encoded (no lookup failures)
    assert!(
        stderr.contains("3 triples"),
        "Expected '3 triples' in stderr, got:\n{stderr}"
    );

    // Verify no lookup failures were logged
    assert!(
        !stderr.contains("not found in dictionary"),
        "Should have no lookup failures, got:\n{stderr}"
    );

    let hdt_bytes = std::fs::read(&hdt_path).unwrap();
    assert!(hdt_bytes.starts_with(b"$HDT"));
}

// =============================================================================
// RDF format tests
// =============================================================================

/// Test Turtle format input.
#[test]
fn test_turtle_format() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ttl_path = temp_dir.path().join("test.ttl");

    let ttl_content = r#"@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:name "Alice" ;
         foaf:knows ex:bob .

ex:bob foaf:name "Bob" ;
       foaf:knows ex:alice .
"#;
    write_file(&ttl_path, ttl_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[ttl_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on Turtle input: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("4 triples"),
        "Expected 4 triples from Turtle, got:\n{stderr}"
    );
}

/// Test RDF/XML format input.
#[test]
fn test_rdfxml_format() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rdf_path = temp_dir.path().join("test.rdf");

    let rdfxml_content = r#"<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/alice">
    <ex:name>Alice</ex:name>
    <ex:knows rdf:resource="http://example.org/bob"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/bob">
    <ex:name>Bob</ex:name>
  </rdf:Description>
</rdf:RDF>
"#;
    write_file(&rdf_path, rdfxml_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[rdf_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on RDF/XML input: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("3 triples"),
        "Expected 3 triples from RDF/XML, got:\n{stderr}"
    );
}

/// Test N-Quads format input (in triples mode, graph component is dropped).
#[test]
fn test_nquads_triples_mode() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nq_path = temp_dir.path().join("test.nq");

    let nq_content = r#"<http://example.org/alice> <http://example.org/name> "Alice" <http://example.org/graph1> .
<http://example.org/bob> <http://example.org/name> "Bob" <http://example.org/graph2> .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
"#;
    write_file(&nq_path, nq_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nq_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on N-Quads input: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    // All 3 quads should produce 3 triples (graph dropped in triples mode)
    assert!(
        stderr.contains("3 triples"),
        "Expected 3 triples from N-Quads, got:\n{stderr}"
    );
}

/// Test N3 (Notation3) format input.
#[test]
fn test_n3_format() {
    let temp_dir = tempfile::tempdir().unwrap();
    let n3_path = temp_dir.path().join("test.n3");

    // N3 is a superset of Turtle
    let n3_content = r#"@prefix ex: <http://example.org/> .

ex:alice ex:name "Alice" .
ex:alice ex:knows ex:bob .
ex:bob ex:name "Bob" .
"#;
    write_file(&n3_path, n3_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[n3_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on N3 input: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("3 triples"),
        "Expected 3 triples from N3, got:\n{stderr}"
    );
}

// =============================================================================
// Compressed input tests
// =============================================================================

/// Test gzip-compressed N-Triples input.
#[test]
fn test_gzip_compressed_input() {
    use flate2::write::GzEncoder;
    use flate2::Compression;

    let temp_dir = tempfile::tempdir().unwrap();
    let gz_path = temp_dir.path().join("test.nt.gz");

    let nt_content = r#"<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/bob> <http://example.org/name> "Bob" .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
"#;

    let f = std::fs::File::create(&gz_path).unwrap();
    let mut encoder = GzEncoder::new(f, Compression::default());
    encoder.write_all(nt_content.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[gz_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on gzip input: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("3 triples"),
        "Expected 3 triples from gzip input, got:\n{stderr}"
    );
}

/// Test bzip2-compressed N-Triples input.
#[test]
fn test_bzip2_compressed_input() {
    use bzip2::write::BzEncoder;
    use bzip2::Compression;

    let temp_dir = tempfile::tempdir().unwrap();
    let bz2_path = temp_dir.path().join("test.nt.bz2");

    let nt_content = r#"<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/bob> <http://example.org/name> "Bob" .
"#;

    let f = std::fs::File::create(&bz2_path).unwrap();
    let mut encoder = BzEncoder::new(f, Compression::default());
    encoder.write_all(nt_content.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[bz2_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on bzip2 input: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("2 triples"),
        "Expected 2 triples from bzip2 input, got:\n{stderr}"
    );
}

/// Test xz-compressed N-Triples input.
#[test]
fn test_xz_compressed_input() {
    use xz2::write::XzEncoder;

    let temp_dir = tempfile::tempdir().unwrap();
    let xz_path = temp_dir.path().join("test.nt.xz");

    let nt_content = r#"<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/bob> <http://example.org/name> "Bob" .
"#;

    let f = std::fs::File::create(&xz_path).unwrap();
    let mut encoder = XzEncoder::new(f, 6);
    encoder.write_all(nt_content.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[xz_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on xz input: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("2 triples"),
        "Expected 2 triples from xz input, got:\n{stderr}"
    );
}

// =============================================================================
// Edge case tests
// =============================================================================

/// Test that duplicate triples are eliminated.
#[test]
fn test_duplicate_triple_elimination() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    // Same triple repeated 5 times
    let nt_content = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> .
"#;
    write_file(&nt_path, nt_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    // Should deduplicate to exactly 2 unique triples
    assert!(
        stderr.contains("2 triples"),
        "Expected 2 triples after dedup, got:\n{stderr}"
    );
}

/// Test that duplicate triples across multiple files are eliminated.
#[test]
fn test_duplicate_triples_across_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt1_path = temp_dir.path().join("file1.nt");
    let nt2_path = temp_dir.path().join("file2.nt");

    // Same triple in both files, plus one unique each
    let content1 = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/name> "Alice" .
"#;
    let content2 = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/name> "Bob" .
"#;
    write_file(&nt1_path, content1.as_bytes());
    write_file(&nt2_path, content2.as_bytes());

    let (success, stderr, hdt_bytes) = run_hdtc(
        temp_dir.path(),
        &[nt1_path.as_path(), nt2_path.as_path()],
        "test.hdt",
    );

    assert!(success, "hdtc failed: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    // 3 unique triples (the shared knows triple is deduped)
    assert!(
        stderr.contains("3 triples"),
        "Expected 3 triples after cross-file dedup, got:\n{stderr}"
    );
}

/// Test that malformed input lines are skipped with warnings.
#[test]
fn test_malformed_input_skipped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    let nt_content = r#"<http://example.org/alice> <http://example.org/name> "Alice" .
this is not valid RDF at all
<http://example.org/bob> <http://example.org/name> "Bob" .
another bad line here
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
"#;
    write_file(&nt_path, nt_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc should succeed despite malformed lines: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    // Only 3 valid triples
    assert!(
        stderr.contains("3 triples"),
        "Expected 3 triples (skipping bad lines), got:\n{stderr}"
    );
    // Should warn about skipped errors
    assert!(
        stderr.contains("skipped") || stderr.contains("error") || stderr.contains("Skipping"),
        "Expected parse error warnings in stderr, got:\n{stderr}"
    );
}

/// Test that a single triple produces a valid HDT.
#[test]
fn test_single_triple() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    write_file(
        &nt_path,
        b"<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
    );

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on single triple: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("1 triples"),
        "Expected 1 triple, got:\n{stderr}"
    );
}

/// Test that input from a directory is discovered and processed.
#[test]
fn test_directory_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input_dir = temp_dir.path().join("rdf_files");
    std::fs::create_dir(&input_dir).unwrap();

    // Create multiple files in a subdirectory
    write_file(
        &input_dir.join("a.nt"),
        b"<http://example.org/a> <http://example.org/p> <http://example.org/o1> .\n",
    );
    write_file(
        &input_dir.join("b.nt"),
        b"<http://example.org/b> <http://example.org/p> <http://example.org/o2> .\n",
    );
    // Non-RDF file should be silently skipped
    write_file(&input_dir.join("readme.txt"), b"not RDF");

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[input_dir.as_path()], "test.hdt");

    assert!(success, "hdtc failed on directory input: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("2 triples"),
        "Expected 2 triples from directory, got:\n{stderr}"
    );
}

/// Test mixing different formats: N-Triples and Turtle in the same run.
#[test]
fn test_mixed_formats() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("data.nt");
    let ttl_path = temp_dir.path().join("data.ttl");

    write_file(
        &nt_path,
        b"<http://example.org/alice> <http://example.org/name> \"Alice\" .\n",
    );

    let ttl_content = r#"@prefix ex: <http://example.org/> .
ex:bob ex:name "Bob" .
"#;
    write_file(&ttl_path, ttl_content.as_bytes());

    let (success, stderr, hdt_bytes) = run_hdtc(
        temp_dir.path(),
        &[nt_path.as_path(), ttl_path.as_path()],
        "test.hdt",
    );

    assert!(success, "hdtc failed on mixed formats: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("2 triples"),
        "Expected 2 triples from mixed formats, got:\n{stderr}"
    );
}

/// Test with various literal types: plain, language-tagged, and typed.
#[test]
fn test_literal_types() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    let nt_content = r#"<http://example.org/s> <http://example.org/plain> "hello" .
<http://example.org/s> <http://example.org/lang> "bonjour"@fr .
<http://example.org/s> <http://example.org/typed> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/s> <http://example.org/date> "2024-01-01"^^<http://www.w3.org/2001/XMLSchema#date> .
"#;
    write_file(&nt_path, nt_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on literal types: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("4 triples"),
        "Expected 4 triples with various literals, got:\n{stderr}"
    );
}

// =============================================================================
// Structural verification tests
// =============================================================================

/// Verify the HDT file has the correct section structure:
/// 4 Control Information blocks (Global, Header, Dictionary, Triples)
/// with correct type bytes and format URIs.
#[test]
fn test_hdt_section_structure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    let nt_content = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> .
<http://example.org/alice> <http://example.org/name> "Alice" .
"#;
    write_file(&nt_path, nt_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed: {stderr}");

    // Should contain exactly 4 $HDT magic markers
    assert_eq!(
        count_hdt_magic(&hdt_bytes),
        4,
        "HDT file should have exactly 4 Control Information blocks"
    );

    // Parse Global CI
    let (ct, format, props, offset) = parse_control_info(&hdt_bytes, 0);
    assert_eq!(ct, 1, "Global CI type should be 1");
    assert_eq!(format, "<http://purl.org/HDT/hdt#HDTv1>");
    assert!(
        props.is_empty(),
        "Global CI should have no properties (Java-compatible format)"
    );

    // Parse Header CI
    let (ct, format, _props, offset) = parse_control_info(&hdt_bytes, offset);
    assert_eq!(ct, 2, "Header CI type should be 2");
    assert_eq!(format, "ntriples");

    // Skip header content (read the length property to skip ahead)
    // The header content is N-Triples text between Header CI and Dictionary CI.
    // Find the next $HDT magic after the header CI.
    let dict_offset = hdt_bytes[offset..]
        .windows(4)
        .position(|w| w == b"$HDT")
        .map(|p| offset + p)
        .expect("Should find Dictionary CI");

    // Parse Dictionary CI
    let (ct, format, props, _offset) = parse_control_info(&hdt_bytes, dict_offset);
    assert_eq!(ct, 3, "Dictionary CI type should be 3");
    assert_eq!(format, "<http://purl.org/HDT/hdt#dictionaryFour>");
    assert!(
        props.contains("mapping=1"),
        "Dictionary should use mapping=1"
    );

    // Find Triples CI
    let triples_offset = hdt_bytes[dict_offset + 4..]
        .windows(4)
        .position(|w| w == b"$HDT")
        .map(|p| dict_offset + 4 + p)
        .expect("Should find Triples CI");

    // Parse Triples CI
    let (ct, format, props, _offset) = parse_control_info(&hdt_bytes, triples_offset);
    assert_eq!(ct, 4, "Triples CI type should be 4");
    assert_eq!(format, "<http://purl.org/HDT/hdt#triplesBitmap>");
    assert!(
        props.contains("order=1"),
        "Triples should use SPO order (1)"
    );
    assert!(
        props.contains("numTriples=3"),
        "Expected numTriples=3, got props: {props}"
    );
}

/// Verify the header section contains expected VoID metadata.
#[test]
fn test_header_void_metadata() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    let nt_content = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> .
"#;
    write_file(&nt_path, nt_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed: {stderr}");

    // Find the header content between Header CI and Dictionary CI
    let (_, _, _, header_start) = parse_control_info(&hdt_bytes, 0);
    let (_, _, props, header_content_start) = parse_control_info(&hdt_bytes, header_start);

    // Extract header length from properties
    let length_str = props
        .split(';')
        .find_map(|p| p.strip_prefix("length="))
        .expect("Header CI should have length property");
    let header_length: usize = length_str.parse().unwrap();

    let header_content =
        String::from_utf8(hdt_bytes[header_content_start..header_content_start + header_length].to_vec())
            .unwrap();

    // Verify VoID metadata
    assert!(
        header_content.contains("void#Dataset"),
        "Header should declare void:Dataset type"
    );
    assert!(
        header_content.contains("void#triples"),
        "Header should include void:triples"
    );
    assert!(
        header_content.contains("void#distinctSubjects"),
        "Header should include void:distinctSubjects"
    );
    assert!(
        header_content.contains("void#properties"),
        "Header should include void:properties"
    );
    assert!(
        header_content.contains("void#distinctObjects"),
        "Header should include void:distinctObjects"
    );
    assert!(
        header_content.contains("http://example.org/dataset"),
        "Header should reference the base URI"
    );
}

/// Verify that each PFC dictionary section starts with type byte 0x02.
#[test]
fn test_dictionary_pfc_type_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    let nt_content = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/name> "Bob" .
"#;
    write_file(&nt_path, nt_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed: {stderr}");

    // Find Dictionary CI and its end
    let dict_offset = {
        let mut off = 0;
        for _ in 0..2 {
            // Skip Global and Header CIs
            off = hdt_bytes[off + 4..]
                .windows(4)
                .position(|w| w == b"$HDT")
                .map(|p| off + 4 + p)
                .unwrap();
        }
        off
    };

    let (ct, _, _, dict_data_start) = parse_control_info(&hdt_bytes, dict_offset);
    assert_eq!(ct, 3);

    // The dictionary data starts right after Dictionary CI.
    // The first byte of each PFC section should be 0x02.
    assert_eq!(
        hdt_bytes[dict_data_start], 0x02,
        "First PFC section (shared) should start with type byte 0x02"
    );

    // Verify that at least 4 PFC sections exist (shared, subjects, predicates, objects)
    // by counting 0x02 type bytes at the start of each section.
    // We can't easily find exact boundaries without fully parsing, but we can verify
    // the first one is correct and that the Triples CI follows after dictionary data.
    let triples_offset = hdt_bytes[dict_offset + 4..]
        .windows(4)
        .position(|w| w == b"$HDT")
        .map(|p| dict_offset + 4 + p)
        .expect("Should find Triples CI after dictionary");

    let (ct, _, _, _) = parse_control_info(&hdt_bytes, triples_offset);
    assert_eq!(ct, 4, "Section after dictionary should be Triples (type 4)");
}

/// Verify that the compressed Turtle format combined with gzip works.
#[test]
fn test_compressed_turtle() {
    use flate2::write::GzEncoder;
    use flate2::Compression;

    let temp_dir = tempfile::tempdir().unwrap();
    let gz_path = temp_dir.path().join("test.ttl.gz");

    let ttl_content = r#"@prefix ex: <http://example.org/> .
ex:alice ex:knows ex:bob .
ex:bob ex:knows ex:alice .
"#;

    let f = std::fs::File::create(&gz_path).unwrap();
    let mut encoder = GzEncoder::new(f, Compression::default());
    encoder.write_all(ttl_content.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[gz_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on compressed Turtle: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("2 triples"),
        "Expected 2 triples from compressed Turtle, got:\n{stderr}"
    );
}

/// Verify that an empty dictionary section (e.g., no subject-only terms when all subjects
/// are shared) doesn't cause issues.
#[test]
fn test_all_subjects_shared() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    // Both alice and bob appear as subjects AND objects -> all shared, no subject-only section
    let nt_content = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> .
"#;
    write_file(&nt_path, nt_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("2 triples"),
        "Expected 2 triples, got:\n{stderr}"
    );
    // Verify 4 CI blocks (structure is still complete even with empty sections)
    assert_eq!(count_hdt_magic(&hdt_bytes), 4);
}

/// Verify that a large number of predicates doesn't cause issues.
#[test]
fn test_many_predicates() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    let mut content = String::new();
    for i in 0..50 {
        content.push_str(&format!(
            "<http://example.org/s> <http://example.org/p{i}> <http://example.org/o{i}> .\n"
        ));
    }
    write_file(&nt_path, content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed with many predicates: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("50 triples"),
        "Expected 50 triples, got:\n{stderr}"
    );
}

/// Test with Unicode content in literals.
#[test]
fn test_unicode_literals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("test.nt");

    let nt_content = "<http://example.org/s> <http://example.org/name> \"\u{00E9}l\u{00E8}ve\" .\n\
                      <http://example.org/s> <http://example.org/jp> \"\u{65E5}\u{672C}\u{8A9E}\"@ja .\n\
                      <http://example.org/s> <http://example.org/emoji> \"\u{1F600}\" .\n";
    write_file(&nt_path, nt_content.as_bytes());

    let (success, stderr, hdt_bytes) =
        run_hdtc(temp_dir.path(), &[nt_path.as_path()], "test.hdt");

    assert!(success, "hdtc failed on Unicode input: {stderr}");
    assert!(hdt_bytes.starts_with(b"$HDT"));
    assert!(
        stderr.contains("3 triples"),
        "Expected 3 triples with Unicode, got:\n{stderr}"
    );
}

/// Test pipeline edge case: exactly 1 triple (minimal single batch).
#[test]
fn test_single_triple_pipeline() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("single.nt");

    write_file(&nt_path, b"<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n");

    let hdt_path = temp_dir.path().join("single.hdt");
    let work_dir = temp_dir.path().join("work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(&[
            "create",
            nt_path.to_str().unwrap(),
            "-o", hdt_path.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc failed with single triple: {stderr}");

    let hdt_bytes = std::fs::read(&hdt_path).expect("HDT file should exist");
    assert!(hdt_bytes.starts_with(b"$HDT"), "Output should start with HDT magic");
    assert!(stderr.contains("1 triple"), "Should report exactly 1 triple, got:\n{stderr}");
}

/// Test pipeline edge case: many small batches to exercise backpressure.
/// This creates a dataset that forces multiple batches with tight memory constraints.
#[test]
fn test_many_small_batches_backpressure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("many_batches.nt");

    // Create 100 triples with very small batch size to force many batches
    let mut content = String::new();
    for i in 0..100 {
        content.push_str(&format!(
            "<http://example.org/s{i}> <http://example.org/p> <http://example.org/o{i}> .\n"
        ));
    }
    write_file(&nt_path, content.as_bytes());

    let hdt_path = temp_dir.path().join("many_batches.hdt");
    let work_dir = temp_dir.path().join("work");

    // Use a very small memory limit to force multiple batches and test backpressure
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(&[
            "create",
            nt_path.to_str().unwrap(),
            "-o", hdt_path.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
            "--memory-limit", "10",  // Small limit (10 MB) forces multiple batches
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc failed with many small batches: {stderr}");

    let hdt_bytes = std::fs::read(&hdt_path).expect("HDT file should exist");
    assert!(hdt_bytes.starts_with(b"$HDT"), "Output should start with HDT magic");
    assert!(stderr.contains("100 triples"), "Should report exactly 100 triples, got:\n{stderr}");

    // Verify the result has the expected structure
    assert_eq!(count_hdt_magic(&hdt_bytes), 4, "Should have 4 HDT magic blocks");
}

/// Test pipeline with overlapping terms across multiple files (multi-file batching).
#[test]
fn test_multi_file_term_overlap() {
    let temp_dir = tempfile::tempdir().unwrap();

    // File 1: uses terms A, B, C
    let file1 = temp_dir.path().join("file1.nt");
    write_file(
        &file1,
        b"<http://example.org/A> <http://example.org/B> <http://example.org/C> .\n\
          <http://example.org/A> <http://example.org/B> <http://example.org/D> .\n",
    );

    // File 2: reuses some terms from File 1
    let file2 = temp_dir.path().join("file2.nt");
    write_file(
        &file2,
        b"<http://example.org/A> <http://example.org/E> <http://example.org/C> .\n\
          <http://example.org/F> <http://example.org/B> <http://example.org/G> .\n",
    );

    let hdt_path = temp_dir.path().join("multi_file.hdt");
    let work_dir = temp_dir.path().join("work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(&[
            "create",
            file1.to_str().unwrap(),
            file2.to_str().unwrap(),
            "-o", hdt_path.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc failed with multi-file overlap: {stderr}");

    let hdt_bytes = std::fs::read(&hdt_path).expect("HDT file should exist");
    assert!(hdt_bytes.starts_with(b"$HDT"), "Output should start with HDT magic");
    assert!(stderr.contains("4 triples"), "Should report exactly 4 triples, got:\n{stderr}");
}
