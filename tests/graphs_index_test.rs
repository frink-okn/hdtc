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
