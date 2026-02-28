mod common;

use common::write_file;
use oxrdf::Term;
use std::collections::HashSet;
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

fn parse_header_num_triples(header_content: &str) -> u64 {
    const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";
    const HDT_TRIPLES_NUM: &str = "http://purl.org/HDT/hdt#triplesnumTriples";

    let parser = oxrdfio::RdfParser::from_format(oxrdfio::RdfFormat::NTriples)
        .for_reader(std::io::Cursor::new(header_content.as_bytes()));

    let mut value_from_void: Option<u64> = None;
    let mut value_from_hdt: Option<u64> = None;

    for quad in parser {
        let quad = quad.expect("Header must be valid N-Triples");
        let predicate = quad.predicate.as_str();

        if predicate != VOID_TRIPLES && predicate != HDT_TRIPLES_NUM {
            continue;
        }

        let Term::Literal(literal) = quad.object else {
            continue;
        };

        let value = literal
            .value()
            .parse::<u64>()
            .expect("Triple-count literal must be numeric");

        if predicate == VOID_TRIPLES {
            value_from_void = Some(value);
        }
        if predicate == HDT_TRIPLES_NUM {
            value_from_hdt = Some(value);
        }
    }

    match (value_from_void, value_from_hdt) {
        (Some(v), Some(h)) => {
            assert_eq!(v, h, "void:triples and hdt:triplesnumTriples must match");
            v
        }
        (Some(v), None) => v,
        (None, Some(h)) => h,
        (None, None) => panic!("Header is missing triple-count metadata"),
    }
}

#[test]
fn test_dump_hdt_to_ntriples() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input_nt = temp_dir.path().join("input.nt");
    let hdt_path = temp_dir.path().join("data.hdt");
    let output_nt = temp_dir.path().join("dumped.nt");

    let content = r#"<http://example.org/s1> <http://example.org/p> <http://example.org/o1> .
<http://example.org/s1> <http://example.org/p> <http://example.org/o2> .
<http://example.org/s2> <http://example.org/p> "literal" .
<http://example.org/s2> <http://example.org/p> "èpsilon" .
<http://example.org/s2> <http://example.org/p> "éclair" .
"#;
    write_file(&input_nt, content.as_bytes());

    let create_output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            input_nt.to_str().unwrap(),
            "-o",
            hdt_path.to_str().unwrap(),
            "--base-uri",
            "http://example.org/dataset",
            "--temp-dir",
            temp_dir.path().join("work").to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc create");
    assert!(
        create_output.status.success(),
        "hdtc create failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let dump_output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "dump",
            hdt_path.to_str().unwrap(),
            "-o",
            output_nt.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc dump");
    assert!(
        dump_output.status.success(),
        "hdtc dump failed: {}",
        String::from_utf8_lossy(&dump_output.stderr)
    );

    let parse_all = |path: &Path| -> HashSet<String> {
        let file = std::fs::File::open(path).unwrap();
        let reader = std::io::BufReader::new(file);
        let parser = oxrdfio::RdfParser::from_format(oxrdfio::RdfFormat::NTriples).for_reader(reader);
        parser
            .map(|q| q.unwrap().to_string())
            .collect::<HashSet<_>>()
    };

    let expected = parse_all(&input_nt);
    let actual = parse_all(&output_nt);
    assert_eq!(expected, actual, "Dumped N-Triples should match input graph");
}

/// Comprehensive round-trip test for dump: create HDT from N-Triples, dump back,
/// verify the graph is identical. Covers all term types and escaping edge cases.
#[test]
fn test_dump_round_trip_comprehensive() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input_nt = temp_dir.path().join("input.nt");
    let hdt_path = temp_dir.path().join("data.hdt");
    let output_nt = temp_dir.path().join("dumped.nt");

    // Build input covering all term types and escaping edge cases.
    // Each line is a valid N-Triples statement.
    let content = [
        // IRIs as subject, predicate, object
        r#"<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> ."#,
        // Blank node as subject
        r#"_:b0 <http://example.org/p1> <http://example.org/o2> ."#,
        // Blank node as object
        r#"<http://example.org/s1> <http://example.org/p1> _:b1 ."#,
        // Shared term: same IRI as both subject and object
        r#"<http://example.org/shared> <http://example.org/p1> <http://example.org/shared> ."#,
        // Multiple predicates for one subject
        r#"<http://example.org/s1> <http://example.org/p2> <http://example.org/o3> ."#,
        // Simple literal
        r#"<http://example.org/s2> <http://example.org/p1> "hello world" ."#,
        // Typed literal (integer)
        r#"<http://example.org/s2> <http://example.org/p1> "42"^^<http://www.w3.org/2001/XMLSchema#integer> ."#,
        // Typed literal (date)
        r#"<http://example.org/s2> <http://example.org/p1> "2024-01-15"^^<http://www.w3.org/2001/XMLSchema#date> ."#,
        // Language-tagged literal
        r#"<http://example.org/s2> <http://example.org/p1> "bonjour"@fr ."#,
        r#"<http://example.org/s2> <http://example.org/p1> "hello"@en ."#,
        // Unicode in literal value
        r#"<http://example.org/s2> <http://example.org/p1> "café résumé naïve" ."#,
        // Unicode in IRI
        r#"<http://example.org/s2> <http://example.org/p1> <http://example.org/König> ."#,
        // Literal with escaped quote
        r#"<http://example.org/s3> <http://example.org/p1> "she said \"hi\"" ."#,
        // Literal with escaped backslash
        r#"<http://example.org/s3> <http://example.org/p1> "path\\to\\file" ."#,
        // Literal with escaped newline
        r#"<http://example.org/s3> <http://example.org/p1> "line1\nline2" ."#,
        // Literal with escaped carriage return
        r#"<http://example.org/s3> <http://example.org/p1> "before\rafter" ."#,
        // Literal with escaped tab
        r#"<http://example.org/s3> <http://example.org/p1> "col1\tcol2" ."#,
        // Literal with multiple escapes combined
        r#"<http://example.org/s3> <http://example.org/p1> "line1\nline2\ttab\\slash" ."#,
        // Typed literal with escapes in value
        r#"<http://example.org/s3> <http://example.org/p1> "line1\nline2"^^<http://www.w3.org/2001/XMLSchema#string> ."#,
        // Language-tagged literal with escapes in value
        r#"<http://example.org/s3> <http://example.org/p1> "line1\nline2"@en ."#,
        // Literal containing @ in value (not a language tag)
        r#"<http://example.org/s3> <http://example.org/p1> "user@example.com" ."#,
        // Empty literal
        r#"<http://example.org/s4> <http://example.org/p1> "" ."#,
        // Long IRI
        r#"<http://example.org/very/long/path/to/resource/with/many/segments> <http://example.org/p1> "ok" ."#,
        // CJK characters
        r#"<http://example.org/s4> <http://example.org/p1> "日本語テスト" ."#,
        // Emoji
        r#"<http://example.org/s4> <http://example.org/p1> "hello 🌍" ."#,
    ]
    .join("\n")
        + "\n";

    write_file(&input_nt, content.as_bytes());

    let create_output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            input_nt.to_str().unwrap(),
            "-o",
            hdt_path.to_str().unwrap(),
            "--base-uri",
            "http://example.org/dataset",
            "--temp-dir",
            temp_dir.path().join("work").to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc create");
    assert!(
        create_output.status.success(),
        "hdtc create failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let dump_output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "dump",
            hdt_path.to_str().unwrap(),
            "-o",
            output_nt.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc dump");
    assert!(
        dump_output.status.success(),
        "hdtc dump failed: {}",
        String::from_utf8_lossy(&dump_output.stderr)
    );

    // Parse both files and compare as sets of triples
    let parse_all = |path: &Path| -> HashSet<String> {
        let file = std::fs::File::open(path).unwrap();
        let reader = std::io::BufReader::new(file);
        let parser =
            oxrdfio::RdfParser::from_format(oxrdfio::RdfFormat::NTriples).for_reader(reader);
        parser
            .map(|q| {
                let q = q.unwrap();
                q.to_string()
            })
            .collect::<HashSet<_>>()
    };

    let expected = parse_all(&input_nt);
    let actual = parse_all(&output_nt);

    // Check counts first for a clearer error message
    assert_eq!(
        expected.len(),
        actual.len(),
        "Triple count mismatch: expected {}, got {}",
        expected.len(),
        actual.len()
    );

    // Find missing and extra triples for detailed diagnostics
    let missing: Vec<_> = expected.difference(&actual).collect();
    let extra: Vec<_> = actual.difference(&expected).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "Round-trip mismatch!\nMissing from dump output:\n{}\nExtra in dump output:\n{}",
        missing
            .iter()
            .map(|s| format!("  {s}"))
            .collect::<Vec<_>>()
            .join("\n"),
        extra
            .iter()
            .map(|s| format!("  {s}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
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
        props.contains("elements="),
        "Dictionary should include elements property"
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

    // Triple count is encoded in header metadata (Java-compatible), not triples CI.
    let (_, _, _, header_start) = parse_control_info(&hdt_bytes, 0);
    let (_, _, header_props, header_content_start) = parse_control_info(&hdt_bytes, header_start);
    let header_length: usize = header_props
        .split(';')
        .find_map(|p| p.strip_prefix("length="))
        .expect("Header CI should have length property")
        .parse()
        .expect("Header length should be numeric");
    let header_content = String::from_utf8(
        hdt_bytes[header_content_start..header_content_start + header_length].to_vec(),
    )
    .expect("Header should be valid UTF-8");
    let header_num_triples = parse_header_num_triples(&header_content);
    assert_eq!(header_num_triples, 3, "Header triple count should be 3");
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
        .args([
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
        .args([
            "create",
            nt_path.to_str().unwrap(),
            "-o", hdt_path.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
            "--memory-limit", "10M",  // Small limit (10 MB) forces multiple batches
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
        .args([
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

// =============================================================================
// Index creation tests
// =============================================================================

/// Run `hdtc index` on an existing HDT file, return the index file bytes.
fn run_hdtc_index(hdt_path: &Path, temp_dir: &Path) -> Vec<u8> {
    let work_dir = temp_dir.join("index_work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "index",
            hdt_path.to_str().unwrap(),
            "--temp-dir",
            work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc index");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    eprintln!("hdtc index stderr:\n{stderr}");
    assert!(
        output.status.success(),
        "hdtc index failed: {stderr}"
    );

    let index_path = hdt_path.with_extension("hdt.index.v1-1");
    assert!(index_path.exists(), "Index file should exist at {}", index_path.display());
    std::fs::read(&index_path).expect("Failed to read index file")
}

/// Parse a VByte value from a byte slice, returning (value, bytes_consumed).
fn parse_vbyte(data: &[u8]) -> (u64, usize) {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in data.iter().enumerate() {
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 != 0 {
            return (value, i + 1);
        }
        shift += 7;
    }
    panic!("Unterminated VByte");
}

/// Parse bitmap preamble at offset: type(1) + VByte(num_bits) + CRC8(1).
/// Returns (num_bits, data_start_offset, data_bytes, section_end_offset).
fn parse_bitmap_preamble(data: &[u8], offset: usize) -> (u64, usize, usize, usize) {
    assert_eq!(data[offset], 0x01, "Expected bitmap type byte 0x01");
    let (num_bits, vbyte_len) = parse_vbyte(&data[offset + 1..]);
    let data_start = offset + 1 + vbyte_len + 1; // +1 for CRC8
    let data_bytes = num_bits.div_ceil(8) as usize;
    let section_end = data_start + data_bytes + 4; // +4 for CRC32C
    (num_bits, data_start, data_bytes, section_end)
}

/// Parse log array preamble at offset: type(1) + bpe(1) + VByte(num_entries) + CRC8(1).
/// Returns (bits_per_entry, num_entries, data_start_offset, section_end_offset).
fn parse_logarray_preamble(data: &[u8], offset: usize) -> (u8, u64, usize, usize) {
    assert_eq!(data[offset], 0x01, "Expected log array type byte 0x01");
    let bpe = data[offset + 1];
    let (num_entries, vbyte_len) = parse_vbyte(&data[offset + 2..]);
    let data_start = offset + 2 + vbyte_len + 1; // +1 for CRC8
    let total_bits = num_entries * bpe as u64;
    let data_bytes = total_bits.div_ceil(8) as usize;
    let section_end = data_start + data_bytes + 4; // +4 for CRC32C
    (bpe, num_entries, data_start, section_end)
}

/// Read a single bit from packed bitmap data.
fn read_bitmap_bit(data: &[u8], data_start: usize, bit_index: u64) -> bool {
    let byte_idx = data_start + (bit_index / 8) as usize;
    let bit_in_byte = bit_index % 8;
    (data[byte_idx] >> bit_in_byte) & 1 == 1
}

/// Read a single entry from packed log array data.
fn read_logarray_entry(data: &[u8], data_start: usize, bpe: u8, entry_index: u64) -> u64 {
    let bit_offset = entry_index * bpe as u64;
    let byte_start = data_start + (bit_offset / 8) as usize;
    let bit_in_byte = (bit_offset % 8) as u32;

    // Read up to 16 bytes to cover any cross-word boundary
    let mut buf = [0u8; 16];
    let available = data.len().saturating_sub(byte_start).min(16);
    buf[..available].copy_from_slice(&data[byte_start..byte_start + available]);
    let wide = u128::from_le_bytes(buf);
    let mask = (1u128 << bpe) - 1;
    ((wide >> bit_in_byte) & mask) as u64
}

/// Decode all entries from a bitmap section.
fn decode_bitmap(data: &[u8], offset: usize) -> (Vec<bool>, usize) {
    let (num_bits, data_start, _, section_end) = parse_bitmap_preamble(data, offset);
    let bits: Vec<bool> = (0..num_bits)
        .map(|i| read_bitmap_bit(data, data_start, i))
        .collect();
    (bits, section_end)
}

/// Decode all entries from a log array section.
fn decode_logarray(data: &[u8], offset: usize) -> (Vec<u64>, usize) {
    let (bpe, num_entries, data_start, section_end) = parse_logarray_preamble(data, offset);
    let entries: Vec<u64> = (0..num_entries)
        .map(|i| read_logarray_entry(data, data_start, bpe, i))
        .collect();
    (entries, section_end)
}

/// Test that `hdtc index` creates a structurally valid and semantically correct
/// index file for a dataset with multiple subjects, predicates, and objects.
///
/// Dataset (6 triples, 3 subjects, 2 predicates, 4 objects):
///   <s1> <p1> <o1> .   <s1> <p1> <o2> .   <s1> <p2> <o3> .
///   <s2> <p1> <o1> .   <s2> <p2> <o4> .
///   <s3> <p1> <o2> .
///
/// This exercises:
/// - Multiple objects per (S,P) pair → multi-entry groups in bitmapIndexZ
/// - Multiple predicates per subject → predicate index grouping
/// - Object appearing in multiple triples → merged OPS groups
#[test]
fn test_index_creation_structural_and_semantic() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("index_test.nt");

    // Use short URIs that sort lexicographically in a predictable order
    write_file(
        &nt_path,
        b"<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .\n\
          <http://example.org/s1> <http://example.org/p1> <http://example.org/o2> .\n\
          <http://example.org/s1> <http://example.org/p2> <http://example.org/o3> .\n\
          <http://example.org/s2> <http://example.org/p1> <http://example.org/o1> .\n\
          <http://example.org/s2> <http://example.org/p2> <http://example.org/o4> .\n\
          <http://example.org/s3> <http://example.org/p1> <http://example.org/o2> .\n",
    );

    // Create HDT
    let hdt_path = temp_dir.path().join("index_test.hdt");
    let work_dir = temp_dir.path().join("work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
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

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        output.status.success(),
        "hdtc create failed: {stderr}"
    );

    // Create index
    let index_bytes = run_hdtc_index(&hdt_path, temp_dir.path());

    // ── Parse the index file ──

    // Control Info
    let (ci_type, ci_format, ci_props, ci_end) = parse_control_info(&index_bytes, 0);
    assert_eq!(ci_type, 5, "Control type should be INDEX (5)");
    assert_eq!(
        ci_format, "<http://purl.org/HDT/hdt#indexFoQ>",
        "Index format URI mismatch"
    );
    assert!(
        ci_props.contains("numTriples=6"),
        "Should have numTriples=6 in properties, got: {ci_props}"
    );

    // Section 1: bitmapIndexZ
    let (bitmap_index_z, s1_end) = decode_bitmap(&index_bytes, ci_end);
    assert_eq!(
        bitmap_index_z.len(),
        6,
        "bitmapIndexZ should have num_triples={} bits",
        6
    );

    // Section 2: indexZ
    let (index_z, s2_end) = decode_logarray(&index_bytes, s1_end);
    assert_eq!(
        index_z.len(),
        6,
        "indexZ should have num_triples={} entries",
        6
    );

    // Section 3: predicateIndex bitmap
    let (pred_bitmap, s3_end) = decode_bitmap(&index_bytes, s2_end);

    // Section 4: predicateIndex sequence
    let (pred_seq, s4_end) = decode_logarray(&index_bytes, s3_end);

    // Section 5: predicateCount
    let (pred_count, s5_end) = decode_logarray(&index_bytes, s4_end);

    // All sections should consume the file exactly
    assert_eq!(
        s5_end,
        index_bytes.len(),
        "Index sections should consume the file exactly (consumed {}, file len {})",
        s5_end,
        index_bytes.len()
    );

    // ── Structural invariants ──

    // bitmapIndexZ: last bit in each object group is set (true), others are false
    assert!(
        *bitmap_index_z.last().unwrap(),
        "Last bit of bitmapIndexZ must be set"
    );

    // The number of set bits in bitmapIndexZ equals the number of distinct objects
    let num_object_groups = bitmap_index_z.iter().filter(|&&b| b).count();
    // Our dataset has 4 distinct objects (o1, o2, o3, o4)
    assert_eq!(
        num_object_groups, 4,
        "bitmapIndexZ should have one group boundary per distinct object"
    );

    // indexZ values should all be valid Y-positions (S-P pair indices)
    // Our dataset has 5 S-P pairs: (s1,p1), (s1,p2), (s2,p1), (s2,p2), (s3,p1)
    let num_sp_pairs = 5u64;
    for (i, &pos_y) in index_z.iter().enumerate() {
        assert!(
            pos_y < num_sp_pairs,
            "indexZ[{i}]={pos_y} should be < num_sp_pairs={num_sp_pairs}"
        );
    }

    // predicateIndex bitmap + sequence should have same length = num_sp_pairs
    assert_eq!(
        pred_bitmap.len() as u64, num_sp_pairs,
        "predicateIndex bitmap should have num_sp_pairs bits"
    );
    assert_eq!(
        pred_seq.len() as u64, num_sp_pairs,
        "predicateIndex sequence should have num_sp_pairs entries"
    );

    // predicateCount should have one entry per predicate
    // Our dataset has 2 predicates (p1, p2)
    assert_eq!(pred_count.len(), 2, "predicateCount should have 2 entries (one per predicate)");

    // Sum of predicateCount should equal num_sp_pairs
    let count_sum: u64 = pred_count.iter().sum();
    assert_eq!(
        count_sum, num_sp_pairs,
        "Sum of predicateCount should equal num_sp_pairs"
    );

    // ── Semantic checks ──

    // predicateCount values: p1 has 3 S-P pairs (s1-p1, s2-p1, s3-p1), p2 has 2 (s1-p2, s2-p2)
    assert_eq!(pred_count[0], 3, "predicate 1 should have 3 S-P pairs");
    assert_eq!(pred_count[1], 2, "predicate 2 should have 2 S-P pairs");

    // predicateIndex bitmap: 2 groups, first has 3 entries, second has 2
    // Bitmap: [false, false, true, false, true]
    //          ^^^^^^^^^^^^  ^^^^  ^^^^^^^^^^^
    //            pred 1             pred 2
    assert_eq!(pred_bitmap, vec![false, false, true, false, true],
        "predicateIndex bitmap should mark group boundaries at positions 2 and 4");

    // predicateIndex sequence: Y-positions of S-P pairs grouped by predicate
    // pred 1 (at Y-positions 0, 2, 4 for s1-p1, s2-p1, s3-p1):
    //   seq[0..3] should be {0, 2, 4} in sorted order
    // pred 2 (at Y-positions 1, 3 for s1-p2, s2-p2):
    //   seq[3..5] should be {1, 3} in sorted order
    assert_eq!(&pred_seq[0..3], &[0, 2, 4],
        "predicate 1 Y-positions should be [0, 2, 4]");
    assert_eq!(&pred_seq[3..5], &[1, 3],
        "predicate 2 Y-positions should be [1, 3]");

    // Verify OPS index semantics:
    // Our SPO triples with dictionary IDs (subjects s1<s2<s3, predicates p1<p2, objects o1<o2<o3<o4):
    //   S-P pairs (Y-positions): 0=(s1,p1), 1=(s1,p2), 2=(s2,p1), 3=(s2,p2), 4=(s3,p1)
    //   Objects per pair: 0→{o1,o2}, 1→{o3}, 2→{o1}, 3→{o4}, 4→{o2}
    //
    // OPS order groups by object:
    //   o1: (s1,p1)@Y=0, (s2,p1)@Y=2  → indexZ=[0,2], bitmapIndexZ=[false,true]
    //   o2: (s1,p1)@Y=0, (s3,p1)@Y=4  → indexZ=[0,4], bitmapIndexZ=[false,true]
    //   o3: (s1,p2)@Y=1               → indexZ=[1],   bitmapIndexZ=[true]
    //   o4: (s2,p2)@Y=3               → indexZ=[3],   bitmapIndexZ=[true]
    //
    // Combined: indexZ = [0, 2, 0, 4, 1, 3]
    //           bitmapIndexZ = [false, true, false, true, true, true]
    assert_eq!(
        index_z,
        vec![0, 2, 0, 4, 1, 3],
        "indexZ should map OPS-ordered triples to their Y-positions"
    );
    assert_eq!(
        bitmap_index_z,
        vec![false, true, false, true, true, true],
        "bitmapIndexZ should mark object group boundaries"
    );
}

/// Test index creation with a larger dataset and tiny memory limit to force
/// multiple external sort chunks. Exercises the parallel merge tree path.
///
/// With 2000 triples and 1MB memory limit:
/// - OPS sort budget = 768KB → ~24K ObjectPosEntry (32 bytes each) per chunk → ~83 chunks
/// - Pred sort budget = 256KB → ~8K PredicateEntry (16 bytes each) per chunk
/// - This well exceeds the PARALLEL_MERGE_THRESHOLD of 16.
#[test]
fn test_index_creation_many_sort_chunks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_path = temp_dir.path().join("big_index_test.nt");

    // Generate 2000 triples: 100 subjects × 4 predicates × 5 objects each
    let num_subjects = 100;
    let num_predicates = 4;
    let objects_per_pair = 5;
    let num_triples = num_subjects * num_predicates * objects_per_pair;
    let num_sp_pairs = num_subjects * num_predicates;

    let mut content = String::new();
    for s in 0..num_subjects {
        for p in 0..num_predicates {
            for o in 0..objects_per_pair {
                let obj_id = p * objects_per_pair + o; // 20 distinct objects
                content.push_str(&format!(
                    "<http://example.org/s{s:03}> <http://example.org/p{p}> <http://example.org/o{obj_id:02}> .\n"
                ));
            }
        }
    }
    write_file(&nt_path, content.as_bytes());

    // Create HDT
    let hdt_path = temp_dir.path().join("big_index_test.hdt");
    let work_dir = temp_dir.path().join("work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
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

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc create failed: {stderr}");

    // Create index with tiny memory limit to force many sort chunks
    let index_work = temp_dir.path().join("index_work");
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "index",
            hdt_path.to_str().unwrap(),
            "--temp-dir",
            index_work.to_str().unwrap(),
            "--memory-limit",
            "1M",
        ])
        .output()
        .expect("Failed to execute hdtc index");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    eprintln!("hdtc index stderr:\n{stderr}");
    assert!(output.status.success(), "hdtc index failed: {stderr}");

    // Verify parallel merge was used (should be in debug/trace output)
    // Even if not logged at INFO level, the structural checks below validate correctness.

    let index_path = hdt_path.with_extension("hdt.index.v1-1");
    let index_bytes = std::fs::read(&index_path).expect("Index file should exist");

    // Parse and verify
    let (_ci_type, _ci_format, ci_props, ci_end) = parse_control_info(&index_bytes, 0);
    assert!(
        ci_props.contains(&format!("numTriples={num_triples}")),
        "Should have numTriples={num_triples}, got: {ci_props}"
    );

    let (bitmap_index_z, s1_end) = decode_bitmap(&index_bytes, ci_end);
    assert_eq!(
        bitmap_index_z.len(),
        num_triples,
        "bitmapIndexZ should have {num_triples} bits"
    );

    let (index_z, s2_end) = decode_logarray(&index_bytes, s1_end);
    assert_eq!(
        index_z.len(),
        num_triples,
        "indexZ should have {num_triples} entries"
    );

    let (pred_bitmap, s3_end) = decode_bitmap(&index_bytes, s2_end);
    let (pred_seq, s4_end) = decode_logarray(&index_bytes, s3_end);
    let (pred_count, s5_end) = decode_logarray(&index_bytes, s4_end);

    assert_eq!(
        s5_end,
        index_bytes.len(),
        "All sections should consume file exactly"
    );

    // Structural invariants
    assert!(*bitmap_index_z.last().unwrap());

    // All indexZ values must be valid Y-positions
    for (i, &pos_y) in index_z.iter().enumerate() {
        assert!(
            pos_y < num_sp_pairs as u64,
            "indexZ[{i}]={pos_y} should be < num_sp_pairs={num_sp_pairs}"
        );
    }

    // No zero values in indexZ (objects are 1-based, so every triple maps to a valid Y-position)
    // This is the key check that would have caught the wikidata bug.
    let zero_count = index_z.iter().filter(|&&v| v == 0).count();
    // Some entries may legitimately map to Y-position 0, but not an unreasonable fraction.
    // For this dataset, Y=0 is just (s000,p0), used by 5 triples out of 2000.
    assert!(
        zero_count <= objects_per_pair,
        "Too many indexZ entries map to Y=0: got {zero_count}, expected at most {objects_per_pair}"
    );

    // Predicate index invariants
    assert_eq!(
        pred_bitmap.len(),
        num_sp_pairs,
        "predicateIndex bitmap should have {num_sp_pairs} bits"
    );
    assert_eq!(
        pred_seq.len(),
        num_sp_pairs,
        "predicateIndex sequence should have {num_sp_pairs} entries"
    );
    assert_eq!(
        pred_count.len(),
        num_predicates,
        "predicateCount should have {num_predicates} entries"
    );

    let count_sum: u64 = pred_count.iter().sum();
    assert_eq!(count_sum, num_sp_pairs as u64);

    // Each predicate should have exactly num_subjects S-P pairs
    for (i, &count) in pred_count.iter().enumerate() {
        assert_eq!(
            count, num_subjects as u64,
            "predicate {} should have {} S-P pairs, got {}",
            i + 1,
            num_subjects,
            count
        );
    }

    // The number of distinct object groups = number of distinct objects = 20
    let num_object_groups = bitmap_index_z.iter().filter(|&&b| b).count();
    assert_eq!(
        num_object_groups,
        (num_predicates * objects_per_pair),
        "bitmapIndexZ should have 20 object groups"
    );
}

// =============================================================================
// HDT input tests
// =============================================================================

/// Helper: create an HDT file from N-Triples content, return the HDT path.
fn create_hdt_from_ntriples(temp_dir: &Path, nt_content: &[u8], name: &str) -> std::path::PathBuf {
    let nt_path = temp_dir.join(format!("{name}.nt"));
    write_file(&nt_path, nt_content);
    let hdt_path = temp_dir.join(format!("{name}.hdt"));
    let work_dir = temp_dir.join(format!("{name}_work"));

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            nt_path.to_str().unwrap(),
            "-o", hdt_path.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc create failed for {name}: {stderr}");
    hdt_path
}

/// Helper: read all triples from an HDT file using the `hdt` crate.
fn read_hdt_triples(hdt_path: &Path) -> HashSet<(String, String, String)> {
    let file = std::fs::File::open(hdt_path).expect("open HDT file");
    let hdt = hdt::Hdt::read(std::io::BufReader::new(file)).expect("read HDT");
    hdt.triples_with_pattern(None, None, None)
        .map(|[s, p, o]| (s.to_string(), p.to_string(), o.to_string()))
        .collect()
}

/// Test using an HDT file as sole input to create a new HDT file.
/// The output should contain all the same triples.
#[test]
fn test_hdt_input_sole_source() {
    let temp_dir = tempfile::tempdir().unwrap();
    let nt_content = b"\
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/name> \"Bob\" .
<http://example.org/alice> <http://example.org/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .
";

    // Create source HDT
    let source_hdt = create_hdt_from_ntriples(temp_dir.path(), nt_content, "source");

    // Use it as input to create a new HDT
    let output_hdt = temp_dir.path().join("output.hdt");
    let work_dir = temp_dir.path().join("output_work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            source_hdt.to_str().unwrap(),
            "-o", output_hdt.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc failed with HDT input: {stderr}");
    assert!(stderr.contains("HDT input"), "Should log HDT input scanning");

    // Verify output has the same triples
    let source_triples = read_hdt_triples(&source_hdt);
    let output_triples = read_hdt_triples(&output_hdt);
    assert_eq!(source_triples, output_triples, "HDT round-trip should preserve all triples");
    assert_eq!(output_triples.len(), 3);
}

/// Test merging an HDT file with additional N-Triples.
/// The output should contain triples from both sources.
#[test]
fn test_hdt_input_merged_with_ntriples() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create an HDT with triples about alice
    let alice_nt = b"\
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/name> \"Alice\" .
";
    let alice_hdt = create_hdt_from_ntriples(temp_dir.path(), alice_nt, "alice");

    // Additional N-Triples about bob (with overlapping term: bob)
    let bob_nt_path = temp_dir.path().join("bob.nt");
    write_file(
        &bob_nt_path,
        b"<http://example.org/bob> <http://example.org/name> \"Bob\" .\n\
          <http://example.org/bob> <http://example.org/age> \"25\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n",
    );

    // Merge HDT + N-Triples
    let output_hdt = temp_dir.path().join("merged.hdt");
    let work_dir = temp_dir.path().join("merge_work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            alice_hdt.to_str().unwrap(),
            bob_nt_path.to_str().unwrap(),
            "-o", output_hdt.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc failed merging HDT + NT: {stderr}");

    // Verify merged output
    let triples = read_hdt_triples(&output_hdt);
    assert_eq!(triples.len(), 4, "Merged output should have 4 triples");

    // Check specific triples exist from both sources
    assert!(triples.iter().any(|(s, p, _)| s.contains("alice") && p.contains("knows")),
        "Should contain alice-knows-bob from HDT");
    assert!(triples.iter().any(|(s, p, _)| s.contains("bob") && p.contains("age")),
        "Should contain bob-age from N-Triples");
}

/// Test merging two HDT files together.
#[test]
fn test_hdt_input_two_hdt_files() {
    let temp_dir = tempfile::tempdir().unwrap();

    let hdt1 = create_hdt_from_ntriples(
        temp_dir.path(),
        b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .\n",
        "hdt1",
    );

    let hdt2 = create_hdt_from_ntriples(
        temp_dir.path(),
        b"<http://example.org/c> <http://example.org/p> <http://example.org/d> .\n",
        "hdt2",
    );

    let output_hdt = temp_dir.path().join("merged2.hdt");
    let work_dir = temp_dir.path().join("merge2_work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            hdt1.to_str().unwrap(),
            hdt2.to_str().unwrap(),
            "-o", output_hdt.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc failed merging two HDTs: {stderr}");

    let triples = read_hdt_triples(&output_hdt);
    assert_eq!(triples.len(), 2, "Merged output should have 2 triples");
}

/// Test that duplicate triples across HDT and N-Triples are deduplicated.
#[test]
fn test_hdt_input_deduplication() {
    let temp_dir = tempfile::tempdir().unwrap();

    let shared_triple = b"<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n";

    // Create HDT with the triple
    let hdt_path = create_hdt_from_ntriples(temp_dir.path(), shared_triple, "dup");

    // N-Triples file with the same triple plus one more
    let nt_path = temp_dir.path().join("dup.nt");
    write_file(
        &nt_path,
        b"<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n\
          <http://example.org/s> <http://example.org/p> <http://example.org/o2> .\n",
    );

    let output_hdt = temp_dir.path().join("dedup.hdt");
    let work_dir = temp_dir.path().join("dedup_work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            hdt_path.to_str().unwrap(),
            nt_path.to_str().unwrap(),
            "-o", output_hdt.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc failed with dedup: {stderr}");

    let triples = read_hdt_triples(&output_hdt);
    assert_eq!(triples.len(), 2, "Duplicate triple should be deduplicated, expected 2 unique triples");
}

/// Test blank node disambiguation across two HDT inputs.
/// Two HDT files each contain `_:b1` — these should be treated as distinct blank nodes.
#[test]
fn test_hdt_input_blank_node_disambiguation() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Both files use blank node `_:b1` but they should be disambiguated
    let hdt1 = create_hdt_from_ntriples(
        temp_dir.path(),
        b"_:b1 <http://example.org/name> \"Alice\" .\n",
        "bnode1",
    );
    let hdt2 = create_hdt_from_ntriples(
        temp_dir.path(),
        b"_:b1 <http://example.org/name> \"Bob\" .\n",
        "bnode2",
    );

    let output_hdt = temp_dir.path().join("bnode_merged.hdt");
    let work_dir = temp_dir.path().join("bnode_work");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            hdt1.to_str().unwrap(),
            hdt2.to_str().unwrap(),
            "-o", output_hdt.to_str().unwrap(),
            "--base-uri", "http://example.org/dataset",
            "--temp-dir", work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "hdtc failed merging HDTs with blank nodes: {stderr}");

    let triples = read_hdt_triples(&output_hdt);
    // The two _:b1 nodes should be disambiguated into two distinct blank nodes,
    // resulting in 2 triples (not 1 if they were incorrectly merged).
    assert_eq!(triples.len(), 2,
        "Blank nodes from different HDT files should be disambiguated, expected 2 triples but got: {:?}",
        triples);

    // Verify the two subjects are different blank nodes
    let subjects: HashSet<_> = triples.iter().map(|(s, _, _)| s.clone()).collect();
    assert_eq!(subjects.len(), 2,
        "Should have 2 distinct blank node subjects, got: {:?}", subjects);
}
