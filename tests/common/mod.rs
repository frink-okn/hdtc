#![allow(unused)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Representative N-Triples dataset covering all term types:
/// URIs, blank nodes, plain literals, typed literals, language-tagged literals.
pub const REPRESENTATIVE_NT: &str = r#"<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/name> "Bob" .
<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> .
<http://example.org/alice> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/alice> <http://example.org/label> "Alice"@en .
<http://example.org/alice> <http://example.org/label> "Alicia"@es .
_:b1 <http://example.org/type> <http://example.org/Thing> .
"#;

/// Expected triple count for REPRESENTATIVE_NT.
pub const REPRESENTATIVE_TRIPLE_COUNT: usize = 8;

/// Write content to a file.
pub fn write_file(path: &Path, content: &[u8]) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content).unwrap();
    f.flush().unwrap();
}

/// Run hdtc on given input files and return the path to the generated HDT file.
/// Panics if hdtc fails.
pub fn run_hdtc_to_path(temp_dir: &Path, inputs: &[&Path], hdt_name: &str) -> PathBuf {
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

    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("hdtc stderr:\n{stderr}");
    assert!(output.status.success(), "hdtc failed: {stderr}");

    hdt_path
}
