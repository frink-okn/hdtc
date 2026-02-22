//! Folder-based HDT roundtrip tests, one test entry per file.
//!
//! Drop any RDF file into `tests/data/roundtrip/` and it appears automatically
//! as a named test case:
//!
//! ```
//! test basic.nt ... ok
//! test my-dataset.ttl ... FAILED
//! ```
//!
//! For each file the test:
//!   1. Converts it to HDT using hdtc.
//!   2. Converts the HDT back to N-Triples using hdt-java's HDT2RDF.
//!   3. Compares the original and recovered RDF using Apache Jena's
//!      `rdfcompare` (proper graph-isomorphism check).
//!
//! Tests are marked `ignored` by default because they require Java.
//! Run with:
//!   cargo test --test roundtrip_test -- --include-ignored
//!   cargo test --test roundtrip_test -- --include-ignored basic.nt

mod common;

use common::{
    ensure_hdt_java, ensure_jena, hdt_java_classpath, run_hdtc_to_path, run_rdfcompare,
};
use std::path::{Path, PathBuf};
use std::process::Command;

// ---------------------------------------------------------------------------
// Harness entry point
// ---------------------------------------------------------------------------

fn main() {
    let args = libtest_mimic::Arguments::from_args();

    let roundtrip_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("roundtrip");

    let tests = if roundtrip_dir.exists() {
        collect_tests(&roundtrip_dir)
    } else {
        eprintln!(
            "Note: {} not found; no roundtrip tests loaded.",
            roundtrip_dir.display()
        );
        vec![]
    };

    libtest_mimic::run(&args, tests).exit();
}

/// Discover RDF files in `dir` and return one ignored `Trial` per file.
fn collect_tests(dir: &Path) -> Vec<libtest_mimic::Trial> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read roundtrip directory")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && is_rdf_file(p))
        .collect();
    files.sort(); // deterministic order across platforms

    files
        .into_iter()
        .map(|path| {
            let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
            libtest_mimic::Trial::test(name, move || {
                let hdt_java_dir = ensure_hdt_java();
                let classpath = hdt_java_classpath(&hdt_java_dir);
                let jena_dir = ensure_jena();
                run_roundtrip(&classpath, &jena_dir, &path).map_err(|e| e.into())
            })
            .with_ignored_flag(true)
        })
        .collect()
}

/// Check whether a path has a recognised RDF file extension.
fn is_rdf_file(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    matches!(
        ext.as_str(),
        "nt" | "ntriples"
            | "nq"
            | "nquads"
            | "ttl"
            | "turtle"
            | "trig"
            | "rdf"
            | "xml"
            | "owl"
            | "jsonld"
            | "json"
            | "n3"
    )
}

// ---------------------------------------------------------------------------
// Roundtrip logic
// ---------------------------------------------------------------------------

/// Run one roundtrip: hdtc → HDT → hdt-java HDT2RDF → Jena rdfcompare.
fn run_roundtrip(classpath: &str, jena_dir: &Path, input_path: &Path) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;

    // hdtc: RDF → HDT
    let hdt_path = run_hdtc_to_path(temp.path(), &[input_path], "test.hdt");

    // hdt-java: HDT → N-Triples
    let output_nt = temp.path().join("roundtrip.nt");
    let output = Command::new("java")
        .args([
            "-cp",
            classpath,
            "org.rdfhdt.hdt.tools.HDT2RDF",
            hdt_path.to_str().unwrap(),
            output_nt.to_str().unwrap(),
        ])
        .output()
        .map_err(|e| format!("Failed to launch HDT2RDF: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "HDT2RDF failed (exit {:?}):\n{stderr}",
            output.status.code()
        ));
    }

    // Jena rdfcompare: graph-isomorphism check
    run_rdfcompare(jena_dir, input_path, &output_nt)
}
