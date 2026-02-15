use std::io::Write;
use std::process::Command;

/// Create a temp N-Triples file and convert it to HDT using the hdtc binary.
#[test]
fn test_end_to_end_ntriples_to_hdt() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create a small N-Triples file
    let nt_path = temp_dir.path().join("test.nt");
    let hdt_path = temp_dir.path().join("test.hdt");

    let mut f = std::fs::File::create(&nt_path).unwrap();
    writeln!(f, r#"<http://example.org/alice> <http://example.org/name> "Alice" ."#).unwrap();
    writeln!(f, r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> ."#).unwrap();
    writeln!(f, r#"<http://example.org/bob> <http://example.org/name> "Bob" ."#).unwrap();
    writeln!(f, r#"<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> ."#).unwrap();
    writeln!(f, r#"<http://example.org/alice> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> ."#).unwrap();
    f.flush().unwrap();
    drop(f);

    // Run hdtc
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
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

    // Verify stderr reports 2 triples (blank nodes disambiguated → different subjects)
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
    writeln!(f, r#"<http://example.org/alice> <http://example.org/likes> <http://example.org/bob> ."#).unwrap();
    // ...and also as a subject (describing the property itself)
    writeln!(f, r#"<http://example.org/likes> <http://www.w3.org/2000/01/rdf-schema#label> "likes" ."#).unwrap();
    // Another normal triple
    writeln!(f, r#"<http://example.org/bob> <http://example.org/likes> <http://example.org/alice> ."#).unwrap();
    f.flush().unwrap();
    drop(f);

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
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
