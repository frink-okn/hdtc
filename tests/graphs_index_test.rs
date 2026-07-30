use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const QUADS: &str = r#"<urn:a> <urn:p1> <urn:o1> .
<urn:a> <urn:p1> <urn:o1> <urn:g1> .
<urn:a> <urn:p2> <urn:o2> <urn:g2> .
<urn:b> <urn:p1> <urn:o2> <urn:g1> .
<urn:b> <urn:p2> <urn:o1> <urn:g2> .
"#;

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(args)
        .output()
        .expect("run hdtc")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn graph_index_path(hdt: &Path) -> PathBuf {
    PathBuf::from(format!("{}.graphs.idx", hdt.display()))
}

fn permutation_path(hdt: &Path) -> PathBuf {
    PathBuf::from(format!("{}.perm", hdt.display()))
}

fn standard_index_path(hdt: &Path) -> PathBuf {
    hdt.with_extension("hdt.index.v1-1")
}

/// Return the stable dictionary-and-triples suffix bound by graph artifacts.
/// The preceding HDT header contains a build timestamp and is intentionally
/// excluded from artifact identity.
fn hdt_data_suffix(hdt: &Path) -> Vec<u8> {
    let sidecar = fs::read(format!("{}.graphs", hdt.display())).unwrap();
    let length = u64::from_le_bytes(sidecar[48..56].try_into().unwrap()) as usize;
    let bytes = fs::read(hdt).unwrap();
    bytes[bytes.len() - length..].to_vec()
}

#[test]
fn integrated_create_matches_standalone_indexes() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nq");
    let integrated = temp.path().join("integrated.hdt");
    let standalone = temp.path().join("standalone.hdt");
    fs::write(&input, QUADS).unwrap();

    assert_success(&run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        integrated.to_str().unwrap(),
        "--mode",
        "quads",
        "--graphs-index",
        "--perm",
        "--index",
        "--perm-position-maps",
        "pos,ops",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert_success(&run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        standalone.to_str().unwrap(),
        "--mode",
        "quads",
        "--perm",
        "--index",
        "--perm-position-maps",
        "pos,ops",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert_success(&run(&[
        "graphs-index",
        standalone.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));

    assert_eq!(hdt_data_suffix(&integrated), hdt_data_suffix(&standalone));
    assert_eq!(
        fs::read(format!("{}.graphs", integrated.display())).unwrap(),
        fs::read(format!("{}.graphs", standalone.display())).unwrap()
    );
    assert_eq!(
        fs::read(permutation_path(&integrated)).unwrap(),
        fs::read(permutation_path(&standalone)).unwrap()
    );
    assert_eq!(
        fs::read(standard_index_path(&integrated)).unwrap(),
        fs::read(standard_index_path(&standalone)).unwrap()
    );
    assert_eq!(
        fs::read(graph_index_path(&integrated)).unwrap(),
        fs::read(graph_index_path(&standalone)).unwrap()
    );
    assert_success(&run(&[
        "validate",
        integrated.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
}

#[test]
fn integrated_graph_index_requires_quads_mode() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nt");
    let hdt = temp.path().join("data.hdt");
    fs::write(&input, "<urn:s> <urn:p> <urn:o> .\n").unwrap();
    let output = run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        hdt.to_str().unwrap(),
        "--graphs-index",
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("require --mode quads"));
    assert!(!hdt.exists());
}

#[test]
fn integrated_graph_index_handles_empty_and_sorted_spool_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let empty = temp.path().join("empty.nq");
    let empty_hdt = temp.path().join("empty.hdt");
    fs::write(&empty, "").unwrap();
    assert_success(&run(&[
        "create",
        empty.to_str().unwrap(),
        "--output",
        empty_hdt.to_str().unwrap(),
        "--mode",
        "quads",
        "--graphs-index",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert_success(&run(&[
        "validate",
        empty_hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));

    let many = temp.path().join("many-graphs.nq");
    let many_hdt = temp.path().join("many-graphs.hdt");
    let mut quads = String::new();
    for graph in 0..20 {
        quads.push_str(&format!(
            "<urn:s{graph}> <urn:p> <urn:o{graph}> <urn:g{graph}> .\n"
        ));
    }
    fs::write(&many, quads).unwrap();
    assert_success(&run(&[
        "create",
        many.to_str().unwrap(),
        "--output",
        many_hdt.to_str().unwrap(),
        "--mode",
        "quads",
        "--graphs-index",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert_success(&run(&[
        "validate",
        many_hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
}

/// Both integrated layer spools must emit the same bytes.
///
/// `--memory-limit` is what selects between them, so the assertions on the
/// build log are load-bearing: without them a change to the direct-spool
/// budget would silently turn this into two runs of the same branch.
#[test]
fn integrated_direct_and_sorted_layer_spools_agree() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nq");
    let mut quads = String::new();
    for triple in 0..300 {
        for graph in 1..7 {
            if triple % graph == 0 {
                quads.push_str(&format!(
                    "<urn:s{}> <urn:p{}> <urn:o{}> <urn:g{graph}> .\n",
                    triple % 50,
                    triple % 7,
                    triple % 80
                ));
            }
        }
        quads.push_str(&format!(
            "<urn:s{}> <urn:p{}> <urn:o{}> .\n",
            triple % 50,
            triple % 7,
            triple % 80
        ));
    }
    fs::write(&input, quads).unwrap();

    let build = |output: &Path, memory_limit: &str| {
        run(&[
            "create",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--mode",
            "quads",
            "--graphs-index",
            "--perm",
            "--perm-position-maps",
            "pos,ops",
            "--temp-dir",
            temp.path().to_str().unwrap(),
            "--memory-limit",
            memory_limit,
        ])
    };

    let sorted_hdt = temp.path().join("sorted.hdt");
    let direct_hdt = temp.path().join("direct.hdt");
    let sorted = build(&sorted_hdt, "1M");
    let direct = build(&direct_hdt, "64M");
    assert_success(&sorted);
    assert_success(&direct);

    let sorted_log = String::from_utf8_lossy(&sorted.stderr).into_owned();
    let direct_log = String::from_utf8_lossy(&direct.stderr).into_owned();
    assert!(
        sorted_log.contains("integrated direct-spool resource limit"),
        "expected the external membership-sort fallback:\n{sorted_log}"
    );
    assert!(
        direct_log.contains("integrated graph-index memberships directly by layer"),
        "expected the direct per-layer spool:\n{direct_log}"
    );

    for suffix in ["", ".graphs", ".graphs.idx", ".perm"] {
        let sorted_artifact = format!("{}{suffix}", sorted_hdt.display());
        let direct_artifact = format!("{}{suffix}", direct_hdt.display());
        assert_eq!(
            fs::read(&sorted_artifact).unwrap(),
            fs::read(&direct_artifact).unwrap(),
            "layer spools disagree on {sorted_artifact}"
        );
    }

    assert_success(&run(&[
        "validate",
        direct_hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));
}

/// The direct spool gives every non-empty layer its own zstd frame and
/// concatenates them in graph order when it finishes, so exercise it with
/// enough layers for that concatenation to matter and check the result against
/// the standalone builder.
#[test]
fn integrated_direct_layer_spool_spans_many_layers() {
    const GRAPHS: usize = 40;
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nq");
    let integrated = temp.path().join("integrated.hdt");
    let standalone = temp.path().join("standalone.hdt");
    let mut quads = String::new();
    for triple in 0..200 {
        for graph in 1..=GRAPHS {
            if triple % graph == 0 {
                quads.push_str(&format!(
                    "<urn:s{}> <urn:p{}> <urn:o{}> <urn:g{graph}> .\n",
                    triple % 30,
                    triple % 5,
                    triple % 60
                ));
            }
        }
    }
    fs::write(&input, quads).unwrap();

    let integrated_output = run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        integrated.to_str().unwrap(),
        "--mode",
        "quads",
        "--graphs-index",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "256M",
    ]);
    assert_success(&integrated_output);
    let integrated_log = String::from_utf8_lossy(&integrated_output.stderr).into_owned();
    assert!(
        integrated_log.contains("integrated graph-index memberships directly by layer"),
        "expected the direct per-layer spool:\n{integrated_log}"
    );

    assert_success(&run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        standalone.to_str().unwrap(),
        "--mode",
        "quads",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "256M",
    ]));
    assert_success(&run(&[
        "graphs-index",
        standalone.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "256M",
    ]));

    assert_eq!(
        fs::read(graph_index_path(&integrated)).unwrap(),
        fs::read(graph_index_path(&standalone)).unwrap()
    );
    assert_success(&run(&[
        "validate",
        integrated.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "256M",
    ]));
}

/// `graphs-index` picks its layer strategy from the sidecar's memberships per
/// triple. High multiplicity routes to the mapping strategy, which the
/// integrated builder never uses, so compare the two against each other: they
/// reach graph-major permuted positions by different routes and must agree.
#[test]
fn standalone_mapping_strategy_matches_integrated_decorated() {
    const GRAPHS: usize = 20;
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nq");
    let integrated = temp.path().join("integrated.hdt");
    let standalone = temp.path().join("standalone.hdt");
    let mut quads = String::new();
    for triple in 0..60 {
        for graph in 1..=GRAPHS {
            quads.push_str(&format!(
                "<urn:s{}> <urn:p{}> <urn:o{triple}> <urn:g{graph}> .\n",
                triple % 7,
                triple % 3
            ));
        }
    }
    fs::write(&input, quads).unwrap();

    let create = |output: &Path, extra: &[&str]| {
        let mut args = vec![
            "create",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--mode",
            "quads",
            "--temp-dir",
            temp.path().to_str().unwrap(),
            "--memory-limit",
            "64M",
        ];
        args.extend_from_slice(extra);
        run(&args)
    };
    assert_success(&create(&integrated, &["--graphs-index"]));
    assert_success(&create(&standalone, &[]));

    let indexed = run(&[
        "graphs-index",
        standalone.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]);
    assert_success(&indexed);
    let log = String::from_utf8_lossy(&indexed.stderr).into_owned();
    assert!(
        log.contains("mapping permuted positions"),
        "expected the mapping layer strategy:\n{log}"
    );

    assert_eq!(
        fs::read(graph_index_path(&integrated)).unwrap(),
        fs::read(graph_index_path(&standalone)).unwrap()
    );
    assert_success(&run(&[
        "validate",
        standalone.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));
}

/// The sidecar transposes by k-way merging its layers, falling back to an
/// external sort when they no longer fit as concurrent iterators. The budget is
/// what selects between them, so pin the branch and compare the bytes.
#[test]
fn graph_index_transpose_strategies_agree() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nq");
    let merged_hdt = temp.path().join("merged.hdt");
    let sorted_hdt = temp.path().join("sorted.hdt");
    let mut quads = String::new();
    for triple in 0..400 {
        quads.push_str(&format!(
            "<urn:s{}> <urn:p{}> <urn:o{triple}> <urn:g{}> .\n",
            triple % 25,
            triple % 5,
            triple % 3
        ));
    }
    fs::write(&input, quads).unwrap();

    for output in [&merged_hdt, &sorted_hdt] {
        assert_success(&run(&[
            "create",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--mode",
            "quads",
            "--temp-dir",
            temp.path().to_str().unwrap(),
            "--memory-limit",
            "64M",
        ]));
    }

    let index = |hdt: &Path, memory_limit: &str| {
        run(&[
            "graphs-index",
            hdt.to_str().unwrap(),
            "--temp-dir",
            temp.path().to_str().unwrap(),
            "--memory-limit",
            memory_limit,
        ])
    };
    let merged = index(&merged_hdt, "64M");
    let sorted = index(&sorted_hdt, "1M");
    assert_success(&merged);
    assert_success(&sorted);

    let merged_log = String::from_utf8_lossy(&merged.stderr).into_owned();
    let sorted_log = String::from_utf8_lossy(&sorted.stderr).into_owned();
    assert!(
        merged_log.contains("Transposing graph layers by k-way merge"),
        "expected the k-way layer merge:\n{merged_log}"
    );
    assert!(
        sorted_log.contains("transposing by external sort"),
        "expected the external-sort transpose fallback:\n{sorted_log}"
    );

    assert_eq!(
        fs::read(graph_index_path(&merged_hdt)).unwrap(),
        fs::read(graph_index_path(&sorted_hdt)).unwrap()
    );
}

#[test]
fn builds_and_strictly_validates_all_v1_structures() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nq");
    let hdt = temp.path().join("data.hdt");
    fs::write(&input, QUADS).unwrap();

    assert_success(&run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        hdt.to_str().unwrap(),
        "--mode",
        "quads",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert_success(&run(&[
        "graphs-index",
        hdt.to_str().unwrap(),
        "--transpose-ids",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));

    let bytes = fs::read(graph_index_path(&hdt)).unwrap();
    assert_eq!(&bytes[..8], b"$HDTGIDX");
    assert_eq!(
        u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        0b1111
    );
    assert_eq!(u64::from_le_bytes(bytes[24..32].try_into().unwrap()), 4);
    assert_eq!(u64::from_le_bytes(bytes[32..40].try_into().unwrap()), 2);
    assert_eq!(u64::from_le_bytes(bytes[40..48].try_into().unwrap()), 5);

    assert_success(&run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));

    let index_path = graph_index_path(&hdt);
    let mut corrupted = fs::read(&index_path).unwrap();
    let directory_offset = u64::from_le_bytes(corrupted[64..72].try_into().unwrap()) as usize;
    let section_count = u32::from_le_bytes(corrupted[80..84].try_into().unwrap()) as usize;
    let bitmap = (0..section_count)
        .map(|index| directory_offset + index * 64)
        .find(|offset| {
            u32::from_le_bytes(corrupted[*offset..*offset + 4].try_into().unwrap()) == 0x0302
        })
        .unwrap();
    let bitmap_offset =
        u64::from_le_bytes(corrupted[bitmap + 8..bitmap + 16].try_into().unwrap()) as usize;
    corrupted[bitmap_offset] ^= 1;
    fs::write(&index_path, corrupted).unwrap();
    let rejected = run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("section CRC mismatch"));

    // Replacing either parent must retire the derived index before publishing.
    assert_success(&run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        hdt.to_str().unwrap(),
        "--mode",
        "quads",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert!(!graph_index_path(&hdt).exists());
}

#[test]
fn transpose_rank_directories_cross_subblock_boundaries() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("many.nt");
    let hdt = temp.path().join("data.hdt");
    let mut triples = String::new();
    for index in 0..600 {
        triples.push_str(&format!("<urn:s{index}> <urn:p> <urn:o{index}> .\n"));
    }
    fs::write(&input, triples).unwrap();
    assert_success(&run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        hdt.to_str().unwrap(),
        "--mode",
        "quads",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert_success(&run(&[
        "graphs-index",
        hdt.to_str().unwrap(),
        "--no-layers",
        "--transpose-ids",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert_success(&run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
}

#[test]
fn can_build_a_bitmap_only_transpose() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.nq");
    let hdt = temp.path().join("data.hdt");
    fs::write(&input, QUADS).unwrap();
    assert_success(&run(&[
        "create",
        input.to_str().unwrap(),
        "--output",
        hdt.to_str().unwrap(),
        "--mode",
        "quads",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert_success(&run(&[
        "graphs-index",
        hdt.to_str().unwrap(),
        "--no-layers",
        "--transpose-ranks",
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    let bytes = fs::read(graph_index_path(&hdt)).unwrap();
    assert_eq!(
        u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        0b0100
    );
    assert_success(&run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
}
