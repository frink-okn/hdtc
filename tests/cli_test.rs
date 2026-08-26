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
