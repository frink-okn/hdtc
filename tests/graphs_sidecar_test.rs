use oxrdf::GraphName;
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

const FOUR_KIND_EXPECTED_QUADS: &str = r#"<urn:rdf-default> <urn:p> <urn:o> .
<urn:shared> <urn:p> <urn:o> .
<urn:rdf-named> <urn:p> <urn:o> <urn:g-rdf> .
<urn:shared> <urn:p> <urn:o> <urn:g-shared> .
<urn:hdt-default> <urn:p> <urn:o> .
<urn:hdt-named> <urn:p> <urn:o> <urn:g-hdt> .
<urn:hdt-quad-default> <urn:p> <urn:o> .
"#;

const FOUR_KIND_EXPECTED_TRIPLES: &str = r#"<urn:rdf-default> <urn:p> <urn:o> .
<urn:shared> <urn:p> <urn:o> .
<urn:rdf-named> <urn:p> <urn:o> .
<urn:hdt-default> <urn:p> <urn:o> .
<urn:hdt-named> <urn:p> <urn:o> .
<urn:hdt-quad-default> <urn:p> <urn:o> .
"#;

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(args)
        .output()
        .expect("run hdtc")
}

fn create(inputs: &[&Path], output: &Path, work: &Path, mode: &str, extra: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_hdtc"));
    command.arg("create");
    for input in inputs {
        command.arg(input);
    }
    command.args([
        "--output",
        output.to_str().unwrap(),
        "--mode",
        mode,
        "--temp-dir",
        work.to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]);
    command.args(extra);
    command.output().expect("run hdtc create")
}

fn create_sidecar(input: &Path, output: &Path, work: &Path, extra: &[&str]) -> Output {
    create(&[input], output, work, "quads", extra)
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

fn dump(hdt: &Path, graph_view: &str, work: &Path) -> Output {
    run(&[
        "dump",
        hdt.to_str().unwrap(),
        "--graph-view",
        graph_view,
        "--temp-dir",
        work.to_str().unwrap(),
        "--memory-limit",
        "200M",
    ])
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn sidecar_counts(hdt: &Path) -> (u64, u64, u64) {
    let sidecar = fs::read(sidecar_path(hdt)).unwrap();
    assert_eq!(&sidecar[0..8], b"$HDTGRPH");
    (
        u64::from_le_bytes(sidecar[24..32].try_into().unwrap()),
        u64::from_le_bytes(sidecar[32..40].try_into().unwrap()),
        u64::from_le_bytes(sidecar[40..48].try_into().unwrap()),
    )
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

fn parse_graph_names(bytes: &[u8]) -> HashSet<String> {
    RdfParser::from_format(RdfFormat::NQuads)
        .for_reader(Cursor::new(bytes))
        .map(|quad| match quad.unwrap().graph_name {
            GraphName::DefaultGraph => "default".to_string(),
            GraphName::NamedNode(node) => node.to_string(),
            GraphName::BlankNode(node) => node.to_string(),
        })
        .collect()
}

fn make_four_kind_inputs(root: &Path) -> Vec<PathBuf> {
    let rdf_nt = root.join("mixed-rdf.nt");
    let rdf_nq = root.join("mixed-rdf.nq");
    fs::write(
        &rdf_nt,
        "<urn:rdf-default> <urn:p> <urn:o> .\n\
         <urn:shared> <urn:p> <urn:o> .\n",
    )
    .unwrap();
    fs::write(
        &rdf_nq,
        "<urn:rdf-named> <urn:p> <urn:o> <urn:g-rdf> .\n\
         <urn:shared> <urn:p> <urn:o> <urn:g-shared> .\n",
    )
    .unwrap();

    let plain_source = root.join("plain-source.nt");
    let plain_hdt = root.join("plain-source.hdt");
    fs::write(
        &plain_source,
        "<urn:hdt-default> <urn:p> <urn:o> .\n\
         <urn:shared> <urn:p> <urn:o> .\n",
    )
    .unwrap();
    assert_success(&create(
        &[&plain_source],
        &plain_hdt,
        &root.join("plain-source-work"),
        "triples",
        &[],
    ));
    assert!(!sidecar_path(&plain_hdt).exists());

    let quad_source = root.join("quad-source.nq");
    let quad_hdt = root.join("quad-source.hdt");
    fs::write(
        &quad_source,
        "<urn:hdt-named> <urn:p> <urn:o> <urn:g-hdt> .\n\
         <urn:shared> <urn:p> <urn:o> <urn:g-shared> .\n\
         <urn:hdt-quad-default> <urn:p> <urn:o> .\n",
    )
    .unwrap();
    assert_success(&create_sidecar(
        &quad_source,
        &quad_hdt,
        &root.join("quad-source-work"),
        &[],
    ));

    vec![rdf_nt, rdf_nq, plain_hdt, quad_hdt]
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

#[test]
fn multiple_nquads_inputs_in_triples_mode_drop_graphs_and_deduplicate_union() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first.nq");
    let second = temp.path().join("second.nq");
    let hdt = temp.path().join("triples.hdt");
    fs::write(
        &first,
        "<urn:a> <urn:p> <urn:o> <urn:g1> .\n\
         <urn:a> <urn:p> <urn:o> <urn:g2> .\n\
         <urn:b> <urn:p> <urn:o> .\n",
    )
    .unwrap();
    fs::write(
        &second,
        "<urn:a> <urn:p> <urn:o> <urn:g3> .\n\
         <urn:b> <urn:p> <urn:o> <urn:g4> .\n\
         <urn:c> <urn:p> <urn:o> <urn:g1> .\n",
    )
    .unwrap();

    let created = create(
        &[&first, &second],
        &hdt,
        &temp.path().join("create-work"),
        "triples",
        &[],
    );
    assert_success(&created);
    assert!(!sidecar_path(&hdt).exists());

    let dumped = dump(&hdt, "union", &temp.path().join("dump-work"));
    assert_success(&dumped);
    assert_eq!(
        parse_triples(&dumped.stdout),
        parse_triples(
            b"<urn:a> <urn:p> <urn:o> .\n\
              <urn:b> <urn:p> <urn:o> .\n\
              <urn:c> <urn:p> <urn:o> .\n"
        )
    );
}

#[test]
fn mixed_ntriples_and_nquads_rdf_inputs_preserve_all_memberships() {
    let temp = tempfile::tempdir().unwrap();
    let triples = temp.path().join("triples.nt");
    let quads = temp.path().join("quads.nq");
    let hdt = temp.path().join("mixed.hdt");
    fs::write(
        &triples,
        "<urn:a> <urn:p> <urn:x> .\n\
         <urn:shared> <urn:p> <urn:value> .\n",
    )
    .unwrap();
    fs::write(
        &quads,
        "<urn:b> <urn:p> <urn:y> <urn:g1> .\n\
         <urn:shared> <urn:p> <urn:value> <urn:g2> .\n\
         <urn:b> <urn:p> <urn:y> .\n\
         <urn:shared> <urn:p> <urn:value> .\n",
    )
    .unwrap();

    let created = create(
        &[&triples, &quads],
        &hdt,
        &temp.path().join("create-work"),
        "quads",
        &[],
    );
    assert_success(&created);
    assert_eq!(sidecar_counts(&hdt), (3, 2, 5));

    let dumped = dump(&hdt, "dataset", &temp.path().join("dump-work"));
    assert_success(&dumped);
    assert_eq!(
        parse_quads(&dumped.stdout),
        parse_quads(
            b"<urn:a> <urn:p> <urn:x> .\n\
              <urn:shared> <urn:p> <urn:value> .\n\
              <urn:b> <urn:p> <urn:y> <urn:g1> .\n\
              <urn:shared> <urn:p> <urn:value> <urn:g2> .\n\
              <urn:b> <urn:p> <urn:y> .\n"
        )
    );
}

#[test]
fn four_input_kinds_merge_in_quads_mode_with_sidecars_preserved() {
    let temp = tempfile::tempdir().unwrap();
    let inputs = make_four_kind_inputs(temp.path());
    let input_refs: Vec<&Path> = inputs.iter().map(PathBuf::as_path).collect();
    let hdt = temp.path().join("mixed-quads.hdt");

    let created = create(
        &input_refs,
        &hdt,
        &temp.path().join("merge-work"),
        "quads",
        &["--input-sidecars", "preserve"],
    );
    assert_success(&created);
    assert_eq!(sidecar_counts(&hdt), (6, 3, 7));

    let dataset = dump(&hdt, "dataset", &temp.path().join("dataset-work"));
    assert_success(&dataset);
    assert_eq!(
        parse_quads(&dataset.stdout),
        parse_quads(FOUR_KIND_EXPECTED_QUADS.as_bytes())
    );

    let union = dump(&hdt, "union", &temp.path().join("union-work"));
    assert_success(&union);
    assert_eq!(
        parse_triples(&union.stdout),
        parse_triples(FOUR_KIND_EXPECTED_TRIPLES.as_bytes())
    );

    let validated = run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("validate-work").to_str().unwrap(),
        "--memory-limit",
        "200M",
    ]);
    assert_success(&validated);
}

#[test]
fn four_input_kinds_merge_in_triples_mode_drop_every_graph_layer() {
    let temp = tempfile::tempdir().unwrap();
    let inputs = make_four_kind_inputs(temp.path());
    let input_refs: Vec<&Path> = inputs.iter().map(PathBuf::as_path).collect();
    let hdt = temp.path().join("mixed-triples.hdt");

    let created = create(
        &input_refs,
        &hdt,
        &temp.path().join("merge-work"),
        "triples",
        &[],
    );
    assert_success(&created);
    assert!(!sidecar_path(&hdt).exists());

    let union = dump(&hdt, "union", &temp.path().join("union-work"));
    assert_success(&union);
    assert_eq!(
        parse_triples(&union.stdout),
        parse_triples(FOUR_KIND_EXPECTED_TRIPLES.as_bytes())
    );
}

#[test]
fn dropping_an_input_sidecar_places_its_union_in_the_default_graph() {
    let temp = tempfile::tempdir().unwrap();
    let source_nq = temp.path().join("source.nq");
    let source_hdt = temp.path().join("source.hdt");
    let output_hdt = temp.path().join("dropped.hdt");
    fs::write(
        &source_nq,
        "<urn:a> <urn:p> <urn:o> <urn:g1> .\n\
         <urn:a> <urn:p> <urn:o> <urn:g2> .\n\
         <urn:b> <urn:p> <urn:o> .\n",
    )
    .unwrap();
    assert_success(&create_sidecar(
        &source_nq,
        &source_hdt,
        &temp.path().join("source-work"),
        &[],
    ));

    let created = create(
        &[&source_hdt],
        &output_hdt,
        &temp.path().join("drop-work"),
        "quads",
        &["--input-sidecars", "drop"],
    );
    assert_success(&created);
    assert_eq!(sidecar_counts(&output_hdt), (2, 0, 2));

    let dataset = dump(&output_hdt, "dataset", &temp.path().join("dump-work"));
    assert_success(&dataset);
    assert_eq!(
        parse_quads(&dataset.stdout),
        parse_quads(
            b"<urn:a> <urn:p> <urn:o> .\n\
              <urn:b> <urn:p> <urn:o> .\n"
        )
    );
}

#[test]
fn rdf_graph_maps_are_additive_and_default_graph_is_fallback_only() {
    let temp = tempfile::tempdir().unwrap();
    let mapped = temp.path().join("mapped.nq");
    let unmapped = temp.path().join("unmapped.nt");
    let hdt = temp.path().join("assigned.hdt");
    fs::write(
        &mapped,
        "<urn:m1> <urn:p> <urn:o> <urn:explicit> .\n\
         <urn:m2> <urn:p> <urn:o> .\n",
    )
    .unwrap();
    fs::write(&unmapped, "<urn:u> <urn:p> <urn:o> .\n").unwrap();
    let mapping = format!("{}=urn:mapped", mapped.display());

    let created = create(
        &[&mapped, &unmapped],
        &hdt,
        &temp.path().join("create-work"),
        "quads",
        &["--graph-map", &mapping, "--default-graph", "urn:fallback"],
    );
    assert_success(&created);
    assert_eq!(sidecar_counts(&hdt), (3, 3, 4));

    let dataset = dump(&hdt, "dataset", &temp.path().join("dump-work"));
    assert_success(&dataset);
    assert_eq!(
        parse_quads(&dataset.stdout),
        parse_quads(
            b"<urn:m1> <urn:p> <urn:o> <urn:explicit> .\n\
              <urn:m1> <urn:p> <urn:o> <urn:mapped> .\n\
              <urn:m2> <urn:p> <urn:o> <urn:mapped> .\n\
              <urn:u> <urn:p> <urn:o> <urn:fallback> .\n"
        )
    );
}

#[test]
fn empty_quads_input_produces_a_valid_empty_dataset_sidecar() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("empty.nq");
    let hdt = temp.path().join("empty.hdt");
    fs::write(&input, "").unwrap();

    let created = create_sidecar(&input, &hdt, &temp.path().join("create-work"), &[]);
    assert_success(&created);
    assert_eq!(sidecar_counts(&hdt), (0, 0, 0));

    let dataset = dump(&hdt, "dataset", &temp.path().join("dump-work"));
    assert_success(&dataset);
    assert!(dataset.stdout.is_empty());

    let validated = run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("validate-work").to_str().unwrap(),
        "--memory-limit",
        "200M",
    ]);
    assert_success(&validated);
}

#[test]
fn blank_graph_names_are_disambiguated_across_rdf_files() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first.nq");
    let second = temp.path().join("second.nq");
    let hdt = temp.path().join("blank-graphs.hdt");
    fs::write(&first, "<urn:a> <urn:p> <urn:o> _:g .\n").unwrap();
    fs::write(&second, "<urn:b> <urn:p> <urn:o> _:g .\n").unwrap();

    let created = create(
        &[&first, &second],
        &hdt,
        &temp.path().join("create-work"),
        "quads",
        &[],
    );
    assert_success(&created);
    assert_eq!(sidecar_counts(&hdt), (2, 2, 2));

    let dataset = dump(&hdt, "dataset", &temp.path().join("dump-work"));
    assert_success(&dataset);
    let graph_names = parse_graph_names(&dataset.stdout);
    assert_eq!(graph_names.len(), 2);
    assert!(graph_names.iter().all(|name| name.starts_with("_:")));
}
