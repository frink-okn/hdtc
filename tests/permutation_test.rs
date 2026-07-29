//! End-to-end tests for the normative `.hdt.perm` format.

mod common;

use common::{REPRESENTATIVE_NT, write_file};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(args)
        .output()
        .expect("run hdtc")
}

fn assert_ok(output: &Output) {
    assert!(
        output.status.success(),
        "hdtc failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn fixture(temp: &Path, extra: &[&str]) -> PathBuf {
    let input = temp.join("input.nt");
    let hdt = temp.join("data.hdt");
    let work = temp.join("create-work");
    write_file(&input, REPRESENTATIVE_NT.as_bytes());
    let mut args = vec![
        "create",
        input.to_str().unwrap(),
        "-o",
        hdt.to_str().unwrap(),
        "--temp-dir",
        work.to_str().unwrap(),
        "--memory-limit",
        "64M",
    ];
    args.extend_from_slice(extra);
    assert_ok(&run(&args));
    hdt
}

fn perm_path(hdt: &Path) -> PathBuf {
    PathBuf::from(format!("{}.perm", hdt.display()))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn section_types(bytes: &[u8]) -> Vec<u32> {
    let directory = read_u64(bytes, 88) as usize;
    let count = read_u32(bytes, 104) as usize;
    (0..count)
        .map(|index| read_u32(bytes, directory + index * 64))
        .collect()
}

#[test]
fn create_and_standalone_emit_identical_optional_map_profile() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path(), &["--perm", "--perm-position-maps", "pos,ops"]);
    let perm = perm_path(&hdt);
    let integrated = fs::read(&perm).unwrap();
    assert_eq!(&integrated[..8], b"$HDTPERM");
    assert_eq!(read_u32(&integrated, 104), 22);
    let types = section_types(&integrated);
    assert!(types.contains(&0x0109));
    assert!(types.contains(&0x0209));

    let validate_work = temp.path().join("validate-work");
    assert_ok(&run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        validate_work.to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));

    let standalone_work = temp.path().join("standalone-work");
    assert_ok(&run(&[
        "perm",
        hdt.to_str().unwrap(),
        "--position-maps",
        "pos,ops",
        "--temp-dir",
        standalone_work.to_str().unwrap(),
        "--memory-limit",
        "1M",
    ]));
    assert_eq!(fs::read(&perm).unwrap(), integrated);
}

#[test]
fn core_profile_is_discovered_by_search() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path(), &["--perm"]);
    let bytes = fs::read(perm_path(&hdt)).unwrap();
    assert_eq!(read_u32(&bytes, 104), 20);
    assert!(!section_types(&bytes).contains(&0x0109));
    assert!(!section_types(&bytes).contains(&0x0209));

    for (query, expected) in [
        ("? <http://example.org/knows> ?", 2),
        ("? ? <http://example.org/alice>", 1),
        ("? <http://example.org/knows> <http://example.org/bob>", 1),
    ] {
        let output = run(&["search", hdt.to_str().unwrap(), "--query", query, "--count"]);
        assert_ok(&output);
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            expected.to_string()
        );
    }

    // Text occurrence resolution is object-rooted too: a permutation index is
    // sufficient even when no FoQ sidecar exists.
    assert!(!hdt.with_extension("hdt.index.v1-1").exists());
    assert_ok(&run(&[
        "text",
        hdt.to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));
    let output = run(&[
        "search",
        hdt.to_str().unwrap(),
        "--text",
        "Alice",
        "--count",
    ]);
    assert_ok(&output);
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .unwrap()
            > 0
    );
}

#[test]
fn automatic_discovery_prefers_permutation_over_foq_for_predicate_queries() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path(), &["--perm", "--index"]);
    let foq = hdt.with_extension("hdt.index.v1-1");
    assert!(foq.exists());
    fs::write(&foq, b"deliberately broken FoQ index").unwrap();

    let query = "? <http://example.org/knows> ?";
    let output = run(&["search", hdt.to_str().unwrap(), "--query", query, "--count"]);
    assert_ok(&output);
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "2");

    let explicit = run(&[
        "search",
        hdt.to_str().unwrap(),
        "--query",
        query,
        "--index",
        foq.to_str().unwrap(),
        "--count",
    ]);
    assert!(!explicit.status.success());
}

#[test]
fn empty_core_profile_is_canonical_and_valid() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("empty.nt");
    write_file(&input, b"");
    let hdt = temp.path().join("empty.hdt");
    let work = temp.path().join("work");
    assert_ok(&run(&[
        "create",
        input.to_str().unwrap(),
        "-o",
        hdt.to_str().unwrap(),
        "--perm",
        "--temp-dir",
        work.to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));
    let bytes = fs::read(perm_path(&hdt)).unwrap();
    assert_eq!(read_u64(&bytes, 24), 0);
    assert_eq!(read_u32(&bytes, 104), 20);
    let directory = read_u64(&bytes, 88) as usize;
    for index in 0..20 {
        let entry = &bytes[directory + index * 64..directory + (index + 1) * 64];
        assert_eq!(read_u64(entry, 8), 0, "empty section offset");
        assert_eq!(read_u64(entry, 16), 0, "empty section length");
        assert_eq!(read_u32(entry, 36), 0, "empty section CRC");
    }
    assert_ok(&run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("validate").to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));
}

#[test]
fn one_triple_position_maps_use_zero_width() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("one.nt");
    write_file(
        &input,
        b"<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
    );
    let hdt = temp.path().join("one.hdt");
    assert_ok(&run(&[
        "create",
        input.to_str().unwrap(),
        "-o",
        hdt.to_str().unwrap(),
        "--perm",
        "--perm-position-maps",
        "pos,ops",
        "--temp-dir",
        temp.path().join("work").to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));
    let bytes = fs::read(perm_path(&hdt)).unwrap();
    let directory = read_u64(&bytes, 88) as usize;
    let count = read_u32(&bytes, 104) as usize;
    for index in 0..count {
        let entry = &bytes[directory + index * 64..directory + (index + 1) * 64];
        if matches!(read_u32(entry, 0), 0x0109 | 0x0209) {
            assert_eq!(read_u64(entry, 24), 1);
            assert_eq!(entry[32], 0);
            assert_eq!(read_u64(entry, 8), 0);
            assert_eq!(read_u64(entry, 16), 0);
        }
    }
    assert_ok(&run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("validate").to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));
}

#[test]
fn strict_validation_rejects_payload_corruption() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = fixture(temp.path(), &["--perm"]);
    let perm = perm_path(&hdt);
    let bytes = fs::read(&perm).unwrap();
    let directory = read_u64(&bytes, 88) as usize;
    let payload_offset = read_u64(&bytes[directory..directory + 64], 8);
    assert!(payload_offset > 0);
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&perm)
        .unwrap();
    file.seek(SeekFrom::Start(payload_offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(payload_offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.flush().unwrap();

    let output = run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("validate").to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("CRC32C mismatch"));
}

#[test]
fn rank_directories_cross_superblock_boundaries() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("large.nt");
    let mut data = String::new();
    for id in 0..4_100 {
        data.push_str(&format!(
            "<http://example.org/s{id:04}> <http://example.org/p{id:04}> <http://example.org/o{id:04}> .\n"
        ));
    }
    write_file(&input, data.as_bytes());
    let hdt = temp.path().join("large.hdt");
    assert_ok(&run(&[
        "create",
        input.to_str().unwrap(),
        "-o",
        hdt.to_str().unwrap(),
        "--perm",
        "--temp-dir",
        temp.path().join("work").to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));
    assert_ok(&run(&[
        "validate",
        hdt.to_str().unwrap(),
        "--temp-dir",
        temp.path().join("validate").to_str().unwrap(),
        "--memory-limit",
        "64M",
    ]));
    for query in [
        "? <http://example.org/p4095> ?",
        "? <http://example.org/p4096> ?",
        "? ? <http://example.org/o4099>",
    ] {
        let output = run(&["search", hdt.to_str().unwrap(), "--query", query, "--count"]);
        assert_ok(&output);
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "1");
    }
}
