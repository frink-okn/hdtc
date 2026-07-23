use oxrdfio::{RdfFormat, RdfParser};
use std::collections::HashSet;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const FIVE_QUADS: &str = r#"<urn:a> <urn:b> <urn:c> .
<urn:a> <urn:b> <urn:c> <urn:g1> .
<urn:a> <urn:b> <urn:d> .
<urn:x> <urn:y> <urn:z> <urn:g1> .
<urn:x> <urn:y> <urn:z> <urn:g2> .
"#;

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(args)
        .output()
        .expect("run hdtc")
}

fn create_sidecar(input: &Path, output: &Path, work: &Path, extra: &[&str]) -> Output {
    let mut args = vec![
        "create",
        input.to_str().unwrap(),
        "--output",
        output.to_str().unwrap(),
        "-m",
        "quads",
        "--temp-dir",
        work.to_str().unwrap(),
        "--memory-limit",
        "1M",
    ];
    args.extend_from_slice(extra);
    run(&args)
}

fn search(hdt: &Path, query: &str, work: &Path, extra: &[&str]) -> Output {
    let mut args = vec![
        "search",
        hdt.to_str().unwrap(),
        "--query",
        query,
        "--temp-dir",
        work.to_str().unwrap(),
        "--memory-limit",
        "1M",
    ];
    args.extend_from_slice(extra);
    run(&args)
}

fn sidecar_path(hdt: &Path) -> PathBuf {
    PathBuf::from(format!("{}.graphs", hdt.display()))
}

fn parse_quads(bytes: &[u8]) -> HashSet<String> {
    RdfParser::from_format(RdfFormat::NQuads)
        .for_reader(Cursor::new(bytes))
        .map(|quad| quad.unwrap().to_string())
        .collect()
}

fn parse_triples(bytes: &[u8]) -> HashSet<String> {
    RdfParser::from_format(RdfFormat::NTriples)
        .for_reader(Cursor::new(bytes))
        .map(|triple| triple.unwrap().to_string())
        .collect()
}

#[test]
fn normative_roundtrip_has_three_triples_and_five_memberships() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("graphs.nq");
    let hdt = temp.path().join("data.hdt");
    let dumped = temp.path().join("dumped.nq");
    fs::write(&input, FIVE_QUADS).unwrap();

    let output = create_sidecar(&input, &hdt, &temp.path().join("work"), &[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let sidecar = fs::read(sidecar_path(&hdt)).unwrap();
    assert_eq!(&sidecar[0..8], b"$HDTGRPH");
    assert_eq!(u64::from_le_bytes(sidecar[24..32].try_into().unwrap()), 3);
    assert_eq!(u64::from_le_bytes(sidecar[32..40].try_into().unwrap()), 2);
    assert_eq!(u64::from_le_bytes(sidecar[40..48].try_into().unwrap()), 5);

    let dump = run(&[
        "dump",
        hdt.to_str().unwrap(),
        "--graph-view",
        "dataset",
        "--output",
        dumped.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("dump-work").to_str().unwrap(),
        "--memory-limit",
        "200M",
    ]);
    assert!(
        dump.status.success(),
        "{}",
        String::from_utf8_lossy(&dump.stderr)
    );
    assert_eq!(
        parse_quads(FIVE_QUADS.as_bytes()),
        parse_quads(&fs::read(dumped).unwrap())
    );

    let validate = run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("validate-work").to_str().unwrap(),
        "--memory-limit",
        "200M",
    ]);
    assert!(
        validate.status.success(),
        "{}",
        String::from_utf8_lossy(&validate.stderr)
    );
}

#[test]
fn hdt_input_preserves_sidecar_and_adds_graph_map() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("graphs.nq");
    let source = temp.path().join("source.hdt");
    let merged = temp.path().join("merged.hdt");
    let dumped = temp.path().join("merged.nq");
    fs::write(&input, FIVE_QUADS).unwrap();
    let source_output = create_sidecar(&input, &source, &temp.path().join("source-work"), &[]);
    assert!(source_output.status.success());

    let mapping = format!("{}=urn:component", source.display());
    let merge = create_sidecar(
        &source,
        &merged,
        &temp.path().join("merge-work"),
        &["--input-sidecars", "require", "--graph-map", &mapping],
    );
    assert!(
        merge.status.success(),
        "{}",
        String::from_utf8_lossy(&merge.stderr)
    );

    let dump = run(&[
        "dump",
        merged.to_str().unwrap(),
        "--graph-view",
        "dataset",
        "--output",
        dumped.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("dump-work").to_str().unwrap(),
        "--memory-limit",
        "200M",
    ]);
    assert!(
        dump.status.success(),
        "{}",
        String::from_utf8_lossy(&dump.stderr)
    );
    let actual = parse_quads(&fs::read(dumped).unwrap());
    let mut expected = parse_quads(FIVE_QUADS.as_bytes());
    expected.extend(parse_quads(
        b"<urn:a> <urn:b> <urn:c> <urn:component> .\n\
          <urn:a> <urn:b> <urn:d> <urn:component> .\n\
          <urn:x> <urn:y> <urn:z> <urn:component> .\n",
    ));
    assert_eq!(actual, expected);
}

#[test]
fn require_rejects_absence_and_wrong_hdt_sidecar_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let plain_input = temp.path().join("plain.nt");
    let plain = temp.path().join("plain.hdt");
    fs::write(&plain_input, "<urn:s> <urn:p> <urn:o> .\n").unwrap();
    let plain_output = run(&[
        "create",
        plain_input.to_str().unwrap(),
        "--output",
        plain.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("plain-work").to_str().unwrap(),
    ]);
    assert!(plain_output.status.success());

    let required = create_sidecar(
        &plain,
        &temp.path().join("required.hdt"),
        &temp.path().join("required-work"),
        &["--input-sidecars", "require"],
    );
    assert!(!required.status.success());
    assert!(String::from_utf8_lossy(&required.stderr).contains("Required graph sidecar not found"));

    let graph_input = temp.path().join("graph.nq");
    let graph_hdt = temp.path().join("graph.hdt");
    fs::write(&graph_input, "<urn:x> <urn:p> <urn:y> <urn:g> .\n").unwrap();
    let graph_output = create_sidecar(
        &graph_input,
        &graph_hdt,
        &temp.path().join("graph-work"),
        &[],
    );
    assert!(graph_output.status.success());
    fs::copy(sidecar_path(&graph_hdt), sidecar_path(&plain)).unwrap();

    let validate = run(&[
        "validate",
        plain.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("validate-work").to_str().unwrap(),
        "--memory-limit",
        "200M",
    ]);
    assert!(!validate.status.success());
}

#[test]
fn default_only_sidecar_has_g0_and_drop_removes_stale_sidecar() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("default.nt");
    let hdt = temp.path().join("default.hdt");
    fs::write(
        &input,
        "<urn:a> <urn:p> <urn:b> .\n<urn:c> <urn:p> <urn:d> .\n",
    )
    .unwrap();
    let sidecar_output = create_sidecar(&input, &hdt, &temp.path().join("sidecar-work"), &[]);
    assert!(sidecar_output.status.success());
    let bytes = fs::read(sidecar_path(&hdt)).unwrap();
    assert_eq!(u64::from_le_bytes(bytes[24..32].try_into().unwrap()), 2);
    assert_eq!(u64::from_le_bytes(bytes[32..40].try_into().unwrap()), 0);
    assert_eq!(u64::from_le_bytes(bytes[40..48].try_into().unwrap()), 2);

    let drop_output = run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        hdt.to_str().unwrap(),
        "--mode",
        "triples",
        "--temp-dir",
        temp.path().join("drop-work").to_str().unwrap(),
        "--memory-limit",
        "200M",
    ]);
    assert!(
        drop_output.status.success(),
        "{}",
        String::from_utf8_lossy(&drop_output.stderr)
    );
    assert!(!sidecar_path(&hdt).exists());
}

#[test]
fn default_and_explicit_triples_modes_reject_graph_assignment_options() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nt");
    let hdt = temp.path().join("output.hdt");
    fs::write(&input, "<urn:s> <urn:p> <urn:o> .\n").unwrap();
    let mapping = format!("{}=urn:graph", input.display());

    for mode_args in [&[][..], &["--mode", "triples"][..]] {
        let mut args = vec![
            "create",
            input.to_str().unwrap(),
            "--output",
            hdt.to_str().unwrap(),
            "--graph-map",
            &mapping,
        ];
        args.extend_from_slice(mode_args);
        let output = run(&args);

        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("require --mode quads"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!hdt.exists());
    }
}

#[test]
fn search_arity_selects_valid_triples_or_quads_output() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("graphs.nq");
    let hdt = temp.path().join("data.hdt");
    fs::write(&input, FIVE_QUADS).unwrap();
    let created = create_sidecar(&input, &hdt, &temp.path().join("create-work"), &[]);
    assert!(created.status.success());

    let triples = search(&hdt, "? ? ?", &temp.path().join("triple-search"), &[]);
    assert!(
        triples.status.success(),
        "{}",
        String::from_utf8_lossy(&triples.stderr)
    );
    assert_eq!(parse_triples(&triples.stdout).len(), 3);

    let quads = search(&hdt, "? ? ? ?", &temp.path().join("quad-search"), &[]);
    assert!(
        quads.status.success(),
        "{}",
        String::from_utf8_lossy(&quads.stderr)
    );
    assert_eq!(
        parse_quads(&quads.stdout),
        parse_quads(FIVE_QUADS.as_bytes())
    );
    assert!(
        String::from_utf8_lossy(&quads.stdout)
            .lines()
            .any(|line| line == "<urn:a>\t<urn:b>\t<urn:c>\t.")
    );
}

#[test]
fn quad_search_filters_graphs_triples_and_counts_memberships() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("graphs.nq");
    let hdt = temp.path().join("data.hdt");
    fs::write(&input, FIVE_QUADS).unwrap();
    let created = create_sidecar(&input, &hdt, &temp.path().join("create-work"), &[]);
    assert!(created.status.success());

    let default = search(
        &hdt,
        "? ? ? default",
        &temp.path().join("default-search"),
        &[],
    );
    assert!(default.status.success());
    assert_eq!(
        parse_quads(&default.stdout),
        parse_quads(
            b"<urn:a> <urn:b> <urn:c> .\n\
              <urn:a> <urn:b> <urn:d> .\n"
        )
    );
    assert!(
        String::from_utf8_lossy(&default.stdout)
            .lines()
            .all(|line| line.split('\t').count() == 4),
        "default-graph results must have no fourth RDF term"
    );

    let named = search(
        &hdt,
        "? ? ? <urn:g1>",
        &temp.path().join("named-search"),
        &[],
    );
    assert!(named.status.success());
    assert_eq!(
        parse_quads(&named.stdout),
        parse_quads(
            b"<urn:a> <urn:b> <urn:c> <urn:g1> .\n\
              <urn:x> <urn:y> <urn:z> <urn:g1> .\n"
        )
    );

    let filtered = search(
        &hdt,
        "<urn:a> <urn:b> <urn:c> ?",
        &temp.path().join("filtered-search"),
        &[],
    );
    assert!(filtered.status.success());
    assert_eq!(
        parse_quads(&filtered.stdout),
        parse_quads(
            b"<urn:a> <urn:b> <urn:c> .\n\
              <urn:a> <urn:b> <urn:c> <urn:g1> .\n"
        )
    );

    for (query, expected) in [("? ? ? ?", "5\n"), ("? ? ? default", "2\n")] {
        let counted = search(&hdt, query, &temp.path().join("count-search"), &["--count"]);
        assert!(counted.status.success());
        assert_eq!(String::from_utf8_lossy(&counted.stdout), expected);
    }
}

#[test]
fn four_position_search_requires_a_graph_sidecar() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("plain.nt");
    let hdt = temp.path().join("plain.hdt");
    fs::write(&input, "<urn:s> <urn:p> <urn:o> .\n").unwrap();
    let created = run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("create-work").to_str().unwrap(),
    ]);
    assert!(created.status.success());

    let searched = search(&hdt, "? ? ? ?", &temp.path().join("search-work"), &[]);
    assert!(!searched.status.success());
    assert!(
        String::from_utf8_lossy(&searched.stderr).contains("requires graph sidecar"),
        "{}",
        String::from_utf8_lossy(&searched.stderr)
    );
}
