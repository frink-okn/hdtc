use std::process::Command;

#[test]
fn cli_reports_package_version_on_stdout() {
    let expected = format!("hdtc {}\n", env!("CARGO_PKG_VERSION"));

    for flag in ["--version", "-V"] {
        let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
            .arg(flag)
            .output()
            .unwrap_or_else(|error| panic!("run hdtc {flag}: {error}"));

        assert!(output.status.success(), "hdtc {flag} failed");
        assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
        assert!(output.stderr.is_empty(), "hdtc {flag} wrote to stderr");
    }
}

#[test]
fn lowercase_v_remains_the_verbose_flag() {
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["-v", "search", "--help"])
        .output()
        .expect("run hdtc -v search --help");

    assert!(output.status.success(), "-v was not accepted as verbose");
    assert!(output.stderr.is_empty());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .starts_with("Search an HDT/sidecar")
    );
}

#[test]
fn max_term_bytes_reaches_the_parser_on_create_and_header() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("big.ttl");
    let big = "x".repeat(2 * 1024 * 1024);
    std::fs::write(
        &input,
        format!(
            "<http://example.org/s> <http://example.org/big> \"{big}\" .\n\
             <http://example.org/s> <http://example.org/p> <http://example.org/o> .\n"
        ),
    )
    .unwrap();
    let small = dir.path().join("small.hdt");
    let full = dir.path().join("full.hdt");

    // Below the literal: the build fails, names the flag with the bound it had,
    // and publishes nothing.
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["create", "--max-term-bytes", "1M", "-o"])
        .arg(&small)
        .arg(&input)
        .output()
        .expect("run hdtc create");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "create succeeded under a 1M bound"
    );
    assert!(
        stderr.contains("--max-term-bytes (1048576 bytes)"),
        "{stderr}"
    );
    assert!(!small.exists(), "a failed build left an output file");

    // The default bound covers it.
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["create", "-o"])
        .arg(&full)
        .arg(&input)
        .output()
        .expect("run hdtc create");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // The header command takes the same flag for the RDF it reads.
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["header", "--max-term-bytes", "1M", "--add"])
        .arg(&input)
        .arg("-o")
        .arg(dir.path().join("with-header.hdt"))
        .arg(&full)
        .output()
        .expect("run hdtc header");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "header succeeded under a 1M bound"
    );
    assert!(
        stderr.contains("--max-term-bytes (1048576 bytes)"),
        "{stderr}"
    );
}

#[test]
fn a_header_term_past_the_old_bound_stays_readable_everywhere() {
    // `header --add` accepts a term up to its own --max-term-bytes; every
    // reader of the header must then accept it too. Dump, search (the HDT
    // reader), index, and re-import each parse the header on their own path.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data.nt");
    std::fs::write(
        &data,
        "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
    )
    .unwrap();
    let base = dir.path().join("base.hdt");
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["create", "-o"])
        .arg(&base)
        .arg(&data)
        .output()
        .expect("run hdtc create");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let big = "x".repeat(17 * 1024 * 1024);
    let extra = dir.path().join("extra.ttl");
    std::fs::write(
        &extra,
        format!("<http://example.org/meta> <http://example.org/note> \"{big}\" .\n"),
    )
    .unwrap();
    let with = dir.path().join("with.hdt");
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["header", "--max-term-bytes", "32M", "--add"])
        .arg(&extra)
        .arg("-o")
        .arg(&with)
        .arg(&base)
        .output()
        .expect("run hdtc header --add");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let dumped = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .arg("header")
        .arg(&with)
        .output()
        .expect("run hdtc header");
    assert!(
        dumped.status.success(),
        "{}",
        String::from_utf8_lossy(&dumped.stderr)
    );
    assert!(
        dumped.stdout.len() > 17 * 1024 * 1024,
        "dumped header lacks the big term"
    );

    let searched = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .arg("search")
        .arg(&with)
        .args(["--query", "? ? ?", "--count"])
        .output()
        .expect("run hdtc search");
    assert!(
        searched.status.success(),
        "{}",
        String::from_utf8_lossy(&searched.stderr)
    );

    let indexed = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .arg("index")
        .arg(&with)
        .output()
        .expect("run hdtc index");
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );

    let reimported = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["create", "-o"])
        .arg(dir.path().join("again.hdt"))
        .arg(&with)
        .output()
        .expect("run hdtc create from an HDT");
    assert!(
        reimported.status.success(),
        "{}",
        String::from_utf8_lossy(&reimported.stderr)
    );

    let edited = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .arg("header")
        .arg(&with)
        .args(["--dataset-uri", "http://example.org/renamed", "-o"])
        .arg(dir.path().join("renamed.hdt"))
        .output()
        .expect("run hdtc header --dataset-uri");
    assert!(
        edited.status.success(),
        "{}",
        String::from_utf8_lossy(&edited.stderr)
    );
}
