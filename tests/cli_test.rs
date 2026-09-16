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

#[test]
fn header_add_refuses_input_the_readers_would_reject() {
    // A space in an IRI passes a lenient parse. Every reader of the header is
    // strict, so accepting it would write a file nothing can open.
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

    let bad = dir.path().join("bad.nt");
    std::fs::write(
        &bad,
        "<http://example.org/meta> <http://example.org/note> <http://example.org/a b> .\n",
    )
    .unwrap();
    let with = dir.path().join("with.hdt");
    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["header", "--add"])
        .arg(&bad)
        .arg("-o")
        .arg(&with)
        .arg(&base)
        .output()
        .expect("run hdtc header --add");
    assert!(
        !output.status.success(),
        "header --add accepted an invalid IRI"
    );
    assert!(!with.exists(), "a refused header edit left an output file");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("bad.nt"), "{stderr}");
    // It is a syntax error, and is called one: not a failed read.
    assert!(stderr.contains("invalid RDF"), "{stderr}");
    assert!(!stderr.contains("read error"), "{stderr}");
}

#[test]
fn a_failing_input_stops_its_siblings_and_names_the_cause() {
    // Three inputs parsed by three workers; the first fails at once on a term
    // past the bound while the others are still under way. The others stop
    // and report nothing, and the build's error is the cause, not a count of
    // outcomes that came up short because they stopped.
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("bad.ttl");
    let big = "x".repeat(2 * 1024 * 1024);
    std::fs::write(
        &bad,
        format!("<http://example.org/s> <http://example.org/big> \"{big}\" .\n"),
    )
    .unwrap();
    let line = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n";
    let sibling_a = dir.path().join("a.nt");
    let sibling_b = dir.path().join("b.nt");
    std::fs::write(&sibling_a, line.repeat(200_000)).unwrap();
    std::fs::write(&sibling_b, line.repeat(200_000)).unwrap();
    let out = dir.path().join("out.hdt");

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            "--max-term-bytes",
            "1M",
            "--parse-file-workers",
            "3",
            "-o",
        ])
        .arg(&out)
        .arg(&bad)
        .arg(&sibling_a)
        .arg(&sibling_b)
        .output()
        .expect("run hdtc create");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "create succeeded past the bound");
    assert!(!out.exists(), "a failed build left an output file");
    assert!(stderr.contains("Parser failed for file index"), "{stderr}");
    assert!(stderr.contains("bad.ttl"), "{stderr}");
    assert!(
        stderr.contains("--max-term-bytes (1048576 bytes)"),
        "{stderr}"
    );
    assert!(!stderr.contains("outcomes mismatch"), "{stderr}");
    assert!(!stderr.contains("parse abandoned"), "{stderr}");
}
