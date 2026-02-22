#![allow(unused)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;

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
    run_hdtc_to_path_with_args(temp_dir, inputs, hdt_name, &[])
}

/// Run hdtc on given input files and return the path to the generated HDT file,
/// appending additional CLI arguments.
/// Panics if hdtc fails.
pub fn run_hdtc_to_path_with_args(
    temp_dir: &Path,
    inputs: &[&Path],
    hdt_name: &str,
    extra_args: &[&str],
) -> PathBuf {
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
    args.extend(extra_args.iter().map(|arg| arg.to_string()));

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(&args)
        .output()
        .expect("Failed to execute hdtc");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "hdtc failed:\n{stderr}");

    hdt_path
}

// ---------------------------------------------------------------------------
// hdt-java download helpers (shared by compat_test and roundtrip_test)
// ---------------------------------------------------------------------------

pub const HDT_JAVA_VERSION: &str = "3.0.10";

static DOWNLOAD_ONCE: Once = Once::new();

/// Ensure the hdt-java distribution is downloaded and extracted.
/// Returns the path to the hdt-java directory containing `lib/` and `bin/`.
/// The download happens at most once per test binary invocation.
pub fn ensure_hdt_java() -> PathBuf {
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

        std::fs::write(&marker, "ok").expect("write marker");
        eprintln!("hdt-java downloaded to {}", base_dir.display());
    });

    assert!(marker.exists(), "hdt-java download marker not found");
    base_dir
}

/// Build the Java classpath from the hdt-java lib directory.
pub fn hdt_java_classpath(hdt_java_dir: &Path) -> String {
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

// ---------------------------------------------------------------------------
// Apache Jena download helpers (rdfcompare for graph-isomorphism checks)
// ---------------------------------------------------------------------------

pub const JENA_VERSION: &str = "6.0.0";

static JENA_DOWNLOAD_ONCE: Once = Once::new();

/// Ensure the Apache Jena CLI distribution is downloaded and extracted.
/// Returns the path to the Jena directory containing `bin/` and `lib/`.
/// The download happens at most once per test binary invocation.
///
/// Note: Jena 6.x requires Java 21+.
pub fn ensure_jena() -> PathBuf {
    let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-data")
        .join("jena");

    let marker = base_dir.join(".downloaded");

    JENA_DOWNLOAD_ONCE.call_once(|| {
        if marker.exists() {
            return;
        }

        std::fs::create_dir_all(&base_dir).expect("create jena dir");

        let url = format!(
            "https://dlcdn.apache.org/jena/binaries/apache-jena-{JENA_VERSION}.tar.gz"
        );

        eprintln!("Downloading Apache Jena from {url}...");

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
            .expect("Failed to extract Jena tarball");

        curl.wait().expect("Failed to wait for curl");

        assert!(
            tar_output.status.success(),
            "tar extraction failed: {}",
            String::from_utf8_lossy(&tar_output.stderr)
        );

        std::fs::write(&marker, "ok").expect("write marker");
        eprintln!("Apache Jena downloaded to {}", base_dir.display());
    });

    assert!(marker.exists(), "Jena download marker not found");
    base_dir
}

/// Compare two RDF files for graph isomorphism using Jena's `rdfcompare`.
///
/// Returns `Ok(())` if the two files contain identical RDF graphs (modulo
/// blank node naming and serialisation order), or `Err` with diagnostic
/// output otherwise.
pub fn run_rdfcompare(jena_dir: &Path, file1: &Path, file2: &Path) -> Result<(), String> {
    let script = jena_dir.join("bin").join("rdfcompare");

    let output = Command::new("bash")
        .arg(script.to_str().unwrap())
        .arg(file1.to_str().unwrap())
        .arg(file2.to_str().unwrap())
        .output()
        .map_err(|e| format!("Failed to launch rdfcompare: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if output.status.success() && stdout.contains("models are equal") {
        Ok(())
    } else {
        Err(format!(
            "rdfcompare: models are not equal (exit {:?})\nstdout: {stdout}\nstderr: {stderr}",
            output.status.code()
        ))
    }
}
