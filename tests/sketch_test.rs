mod common;

use common::{run_hdtc_to_path, write_file};
use crc::{CRC_32_ISCSI, Crc};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use xorf::{BinaryFuse16Ref, Filter, FilterRef};
use xxhash_rust::xxh64::xxh64;

const SKETCH_NT: &str = r#"<http://example.org/a> <http://example.org/p> <http://example.org/shared> .
<http://example.org/shared> <http://example.org/p> <http://example.org/object> .
<http://example.org/subject-only> <http://example.org/p> "literal" .
_:subject-blank <http://example.org/p> <http://example.org/object> .
<http://example.org/a> <http://example.org/p2> _:object-blank .
"#;

const SUBJECT_IRIS: [&str; 3] = [
    "http://example.org/a",
    "http://example.org/shared",
    "http://example.org/subject-only",
];
const OBJECT_IRIS: [&str; 2] = ["http://example.org/object", "http://example.org/shared"];

fn build_fixture(temp: &Path) -> PathBuf {
    let input = temp.join("input.nt");
    write_file(&input, SKETCH_NT.as_bytes());
    run_hdtc_to_path(temp, &[&input], "data.hdt")
}

fn run_sketch(hdt: &Path, args: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_hdtc"));
    command.arg("sketch").arg(hdt).args(args);
    command.output().expect("run hdtc sketch")
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

/// Check the shared envelope and return the embedded source digest.
fn assert_common_envelope(bytes: &[u8], magic: &[u8; 4], role: u8, key_count: u64) -> [u8; 32] {
    assert!(bytes.len() >= 60);
    assert_eq!(&bytes[0..4], magic);
    assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 1);
    assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 1);
    assert_eq!(bytes[8], 1);
    assert_eq!(bytes[9], role);
    assert_eq!(&bytes[10..16], &[0; 6]);
    assert_eq!(read_u64(bytes, 16), key_count);

    let payload_end = bytes.len() - 4;
    let expected_crc = Crc::<u32>::new(&CRC_32_ISCSI).checksum(&bytes[..payload_end]);
    assert_eq!(read_u32(bytes, payload_end), expected_crc);
    bytes[24..56].try_into().unwrap()
}

fn expected_hashes(iris: &[&str]) -> Vec<u64> {
    let mut hashes: Vec<u64> = iris.iter().map(|iri| xxh64(iri.as_bytes(), 0)).collect();
    hashes.sort_unstable();
    hashes
}

#[test]
fn sketch_writes_standard_role_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());

    let output = run_sketch(&hdt, &["--k", "16"]);
    assert!(
        output.status.success(),
        "sketch failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let filters = temp.path().join("filters");
    let mut digests = Vec::new();

    for (stem, role, iris) in [
        ("subjects", 0, SUBJECT_IRIS.as_slice()),
        ("objects", 1, OBJECT_IRIS.as_slice()),
    ] {
        let expected = expected_hashes(iris);
        let minhash = fs::read(filters.join(format!("{stem}.minhash"))).unwrap();
        digests.push(assert_common_envelope(
            &minhash,
            b"KGFM",
            role,
            expected.len() as u64,
        ));
        assert_eq!(read_u32(&minhash, 56), 16);
        assert_eq!(read_u32(&minhash, 60), expected.len() as u32);
        assert_eq!(minhash[64], 0);
        assert_eq!(&minhash[65..72], &[0; 7]);
        let minima: Vec<u64> = minhash[72..minhash.len() - 4]
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        assert_eq!(minima, expected);

        let filter = fs::read(filters.join(format!("{stem}.filter"))).unwrap();
        digests.push(assert_common_envelope(
            &filter,
            b"KGFF",
            role,
            expected.len() as u64,
        ));
        assert_eq!(filter[56], 16);
        assert_eq!(&filter[57..64], &[0; 7]);
        // Reserved by docs/sketch-format.md §5.1; readers may require zero.
        assert_eq!(read_u32(&filter, 84), 0);
        let fingerprint_len = read_u64(&filter, 88) as usize;
        assert_eq!(filter.len(), 96 + fingerprint_len * 2 + 4);
        let filter_ref =
            BinaryFuse16Ref::from_dma(&filter[64..84], &filter[96..96 + fingerprint_len * 2]);
        for hash in expected {
            assert!(filter_ref.contains(&hash), "false negative for {hash:#x}");
        }
    }

    // Every artifact of a run is bound to the same source HDT.
    assert!(digests.windows(2).all(|pair| pair[0] == pair[1]));
    // ...and that binding is not the naive whole-file digest.
    let whole_file: [u8; 32] = Sha256::digest(fs::read(&hdt).unwrap()).into();
    assert_ne!(digests[0], whole_file);

    assert_eq!(fs::read_dir(filters).unwrap().count(), 4);
}

#[test]
fn sketch_digest_ignores_header_rewrites() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());

    // A header rewrite copies the dictionary and triples verbatim, so sketches
    // of both files describe exactly the same IRIs and must agree on identity.
    let extra = temp.path().join("extra.nt");
    write_file(
        &extra,
        b"<http://example.org/ds> <http://purl.org/dc/terms/title> \"retitled\" .\n",
    );
    let rewritten = temp.path().join("rewritten.hdt");
    let header = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .arg("header")
        .arg(&hdt)
        .args(["--add", extra.to_str().unwrap()])
        .args(["--output", rewritten.to_str().unwrap()])
        .output()
        .expect("run hdtc header");
    assert!(
        header.status.success(),
        "header rewrite failed:\n{}",
        String::from_utf8_lossy(&header.stderr)
    );
    assert_ne!(fs::read(&hdt).unwrap(), fs::read(&rewritten).unwrap());

    let original_dir = temp.path().join("original-filters");
    let rewritten_dir = temp.path().join("rewritten-filters");
    for (source, dir) in [(&hdt, &original_dir), (&rewritten, &rewritten_dir)] {
        let output = run_sketch(source, &["--output-dir", dir.to_str().unwrap()]);
        assert!(
            output.status.success(),
            "sketch failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert_eq!(
        fs::read(original_dir.join("subjects.filter")).unwrap(),
        fs::read(rewritten_dir.join("subjects.filter")).unwrap()
    );
    assert_eq!(
        fs::read(original_dir.join("objects.minhash")).unwrap(),
        fs::read(rewritten_dir.join("objects.minhash")).unwrap()
    );
}

#[test]
fn sketch_can_select_one_role_and_binary_fuse_16() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    let output_dir = temp.path().join("subject-sketch");

    let output = run_sketch(
        &hdt,
        &[
            "--output-dir",
            output_dir.to_str().unwrap(),
            "--roles",
            "subjects",
            "--filter-bits",
            "16",
            "--k",
            "2",
        ],
    );
    assert!(
        output.status.success(),
        "sketch failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(output_dir.join("subjects.filter").is_file());
    assert!(output_dir.join("subjects.minhash").is_file());
    assert!(!output_dir.join("objects.filter").exists());
    assert!(!output_dir.join("objects.minhash").exists());

    let minhash = fs::read(output_dir.join("subjects.minhash")).unwrap();
    assert_eq!(read_u32(&minhash, 56), 2);
    assert_eq!(read_u32(&minhash, 60), 2);
    assert_eq!(minhash[64], 1);

    let filter = fs::read(output_dir.join("subjects.filter")).unwrap();
    assert_eq!(filter[56], 16);
    let fingerprint_len = read_u64(&filter, 88) as usize;
    assert_eq!(filter.len(), 96 + fingerprint_len * 2 + 4);
    let filter_ref =
        BinaryFuse16Ref::from_dma(&filter[64..84], &filter[96..96 + fingerprint_len * 2]);
    for hash in expected_hashes(&SUBJECT_IRIS) {
        assert!(filter_ref.contains(&hash), "false negative for {hash:#x}");
    }
}

#[test]
fn sketch_can_select_the_objects_role_alone() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    let output_dir = temp.path().join("object-sketch");

    let output = run_sketch(
        &hdt,
        &[
            "--output-dir",
            output_dir.to_str().unwrap(),
            "--roles",
            "objects",
            "--k",
            "16",
        ],
    );
    assert!(
        output.status.success(),
        "sketch failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(!output_dir.join("subjects.filter").exists());
    assert!(!output_dir.join("subjects.minhash").exists());
    assert_eq!(fs::read_dir(&output_dir).unwrap().count(), 2);

    // The shared section still reaches the objects role when subjects is off.
    let expected = expected_hashes(&OBJECT_IRIS);
    let minhash = fs::read(output_dir.join("objects.minhash")).unwrap();
    assert_common_envelope(&minhash, b"KGFM", 1, expected.len() as u64);
    let minima: Vec<u64> = minhash[72..minhash.len() - 4]
        .chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    assert_eq!(minima, expected);

    let filter = fs::read(output_dir.join("objects.filter")).unwrap();
    assert_eq!(filter[56], 16);
    let fingerprint_len = read_u64(&filter, 88) as usize;
    let filter_ref =
        BinaryFuse16Ref::from_dma(&filter[64..84], &filter[96..96 + fingerprint_len * 2]);
    for hash in expected {
        assert!(filter_ref.contains(&hash), "false negative for {hash:#x}");
    }
}

#[test]
fn sketch_uses_an_explicit_temp_dir() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    let scratch = temp.path().join("scratch/keys");

    let output = run_sketch(&hdt, &["--temp-dir", scratch.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "sketch failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(scratch.is_dir(), "temp dir was not created");
    // Key files are unlinked as they are created, so nothing should remain.
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains(&format!("Temp directory: {}", scratch.display()))
    );
}

#[test]
fn sketch_rejects_a_memory_limit_that_cannot_build_a_filter() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    let output_dir = temp.path().join("too-small");

    // The default k alone reserves more than the whole budget, so no key fits.
    let output = run_sketch(
        &hdt,
        &["--output-dir", output_dir.to_str().unwrap(), "-m", "1M"],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("increase the limit or reduce --k"),
        "unexpected error:\n{stderr}"
    );
    // The budget is refused during the scan, before anything is published.
    assert_eq!(fs::read_dir(&output_dir).unwrap().count(), 0);
}

#[test]
fn sketch_refuses_to_replace_existing_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    let output_dir = temp.path().join("filters");

    let first = run_sketch(&hdt, &[]);
    assert!(first.status.success());
    let original = fs::read(output_dir.join("subjects.filter")).unwrap();

    let second = run_sketch(&hdt, &[]);
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("Refusing to replace"));
    assert_eq!(
        fs::read(output_dir.join("subjects.filter")).unwrap(),
        original
    );
}

/// The frozen conformance vectors of docs/sketch-format.md §9.2, §9.3 and §9.5,
/// end to end through the CLI. These bytes are published for other
/// implementations to verify against, so they may only change together with the
/// specification.
#[test]
fn sketch_reproduces_the_frozen_conformance_vectors() {
    let temp = tempfile::tempdir().unwrap();

    let toy = temp.path().join("toy.nt");
    let mut nt = String::new();
    for index in 0..100 {
        nt.push_str(&format!(
            "<https://example.org/resource/{index:03}> <http://example.org/p> \"x\" .\n"
        ));
    }
    write_file(&toy, nt.as_bytes());
    let toy_hdt = run_hdtc_to_path(temp.path(), &[&toy], "toy.hdt");

    let toy_dir = temp.path().join("toy-filters");
    let output = run_sketch(
        &toy_hdt,
        &[
            "--k",
            "16",
            "--filter-bits",
            "8",
            "--output-dir",
            toy_dir.to_str().unwrap(),
        ],
    );
    assert!(
        output.status.success(),
        "sketch failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // §9.2 saturated MinHash.
    let minhash = fs::read(toy_dir.join("subjects.minhash")).unwrap();
    assert_common_envelope(&minhash, b"KGFM", 0, 100);
    assert_eq!(read_u32(&minhash, 56), 16, "k");
    assert_eq!(read_u32(&minhash, 60), 16, "stored_count");
    assert_eq!(minhash[64], 1, "saturated");
    assert_eq!(minhash.len(), 204, "§9.2 file size");
    let minima: Vec<u64> = minhash[72..minhash.len() - 4]
        .chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    assert_eq!(
        minima,
        vec![
            0x0095_2d96_04e1_cf2c,
            0x00cc_3131_e8f7_a0c5,
            0x01f3_52d0_f9d5_cf80,
            0x06bf_0ecf_32e0_b062,
            0x07d9_a683_f85c_7cb8,
            0x07fa_5da3_a095_2933,
            0x0a72_4817_be71_ee72,
            0x0da9_8875_b72f_df91,
            0x0f98_c196_c842_182a,
            0x0fd0_e9cd_6b8a_a629,
            0x12c1_9725_ea52_303f,
            0x146e_55f2_b679_e888,
            0x1527_5b37_06d1_e3af,
            0x15ff_25a1_0102_039d,
            0x1940_b18c_45c1_e8a5,
            0x1967_6957_cb8c_7933,
        ]
    );

    // §9.3 filter geometry.
    let filter = fs::read(toy_dir.join("subjects.filter")).unwrap();
    assert_common_envelope(&filter, b"KGFF", 0, 100);
    assert_eq!(filter[56], 8, "variant");
    assert_eq!(read_u64(&filter, 64), 0x910a_2dec_8902_5cc1, "seed");
    assert_eq!(read_u32(&filter, 72), 64, "segment_length");
    assert_eq!(read_u32(&filter, 76), 63, "segment_length_mask");
    assert_eq!(read_u32(&filter, 80), 64, "segment_count_length");
    assert_eq!(read_u64(&filter, 88), 192, "fingerprint_len");
    assert_eq!(filter.len(), 292, "§9.3 file size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&filter[96..filter.len() - 4])),
        "4684db4089f6c89f7609e1cd00d8246e02835f64a1377ca14cc63ee491df9960"
    );

    // §9.2 exact (unsaturated) MinHash.
    let small = temp.path().join("small.nt");
    let mut nt = String::new();
    for index in 0..5 {
        nt.push_str(&format!(
            "<https://example.org/resource/{index:03}> <http://example.org/p> \"x\" .\n"
        ));
    }
    write_file(&small, nt.as_bytes());
    let small_hdt = run_hdtc_to_path(temp.path(), &[&small], "small.hdt");
    let small_dir = temp.path().join("small-filters");
    let output = run_sketch(
        &small_hdt,
        &["--k", "16", "--output-dir", small_dir.to_str().unwrap()],
    );
    assert!(output.status.success());

    let minhash = fs::read(small_dir.join("subjects.minhash")).unwrap();
    assert_common_envelope(&minhash, b"KGFM", 0, 5);
    assert_eq!(read_u32(&minhash, 60), 5, "stored_count");
    assert_eq!(minhash[64], 0, "saturated");
    assert_eq!(minhash.len(), 116, "§9.2 exact-case file size");
    let minima: Vec<u64> = minhash[72..minhash.len() - 4]
        .chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    assert_eq!(
        minima,
        vec![
            0x00cc_3131_e8f7_a0c5,
            0x0da9_8875_b72f_df91,
            0x35c5_f517_a376_fed8,
            0x45c6_4ad7_8fde_51e4,
            0xaf5a_5827_fae0_76d7,
        ]
    );

    // §9.5 empty role: the objects role of the toy set is all literals.
    let empty_filter = fs::read(toy_dir.join("objects.filter")).unwrap();
    assert_common_envelope(&empty_filter, b"KGFF", 1, 0);
    assert_eq!(read_u32(&empty_filter, 72), 4, "segment_length");
    assert_eq!(read_u32(&empty_filter, 76), 3, "segment_length_mask");
    assert_eq!(read_u32(&empty_filter, 80), 4, "segment_count_length");
    assert_eq!(read_u64(&empty_filter, 88), 12, "fingerprint_len");
    assert_eq!(empty_filter.len(), 112, "§9.5 file size");
    assert!(
        empty_filter[96..108].iter().all(|byte| *byte == 0),
        "empty filter fingerprints are all zero"
    );
    let empty_minhash = fs::read(toy_dir.join("objects.minhash")).unwrap();
    assert_eq!(empty_minhash.len(), 76, "§9.5 file size");
}

/// §5.3 invariant 5: `fingerprint_len == segment_count_length + 2 *
/// segment_length` is what bounds the three probe indices, so a reader that
/// validated the header can skip bounds checks. Every file we emit must satisfy
/// it, at every size and both widths.
#[test]
fn emitted_filters_satisfy_the_probe_index_bound() {
    let temp = tempfile::tempdir().unwrap();
    for (count, label) in [(0usize, "empty"), (1, "one"), (5, "small"), (900, "large")] {
        let input = temp.path().join(format!("{label}.nt"));
        let mut nt = String::from("_:b <http://example.org/p> \"x\" .\n");
        for index in 0..count {
            nt.push_str(&format!(
                "<https://example.org/r/{index:04}> <http://example.org/p> \"x\" .\n"
            ));
        }
        write_file(&input, nt.as_bytes());
        let hdt = run_hdtc_to_path(temp.path(), &[&input], &format!("{label}.hdt"));

        for bits in ["8", "16"] {
            let dir = temp.path().join(format!("{label}-{bits}"));
            let output = run_sketch(
                &hdt,
                &[
                    "--k",
                    "16",
                    "--filter-bits",
                    bits,
                    "--output-dir",
                    dir.to_str().unwrap(),
                ],
            );
            assert!(output.status.success());

            let filter = fs::read(dir.join("subjects.filter")).unwrap();
            let segment_length = read_u32(&filter, 72) as u64;
            let mask = read_u32(&filter, 76) as u64;
            let segment_count_length = read_u32(&filter, 80) as u64;
            let fingerprint_len = read_u64(&filter, 88);

            assert!(
                (4..=262_144).contains(&segment_length) && segment_length.is_power_of_two(),
                "{label}/{bits}: segment_length {segment_length}"
            );
            assert_eq!(mask, segment_length - 1, "{label}/{bits}: mask");
            assert_eq!(
                segment_count_length % segment_length,
                0,
                "{label}/{bits}: segment_count_length not a multiple"
            );
            assert_eq!(
                fingerprint_len,
                segment_count_length + 2 * segment_length,
                "{label}/{bits}: probe index bound violated"
            );
            assert_eq!(read_u32(&filter, 84), 0, "{label}/{bits}: reserved");
        }
    }
}

#[test]
fn sketch_emits_well_formed_empty_roles() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("blank-only.nt");
    write_file(&input, b"_:subject <http://example.org/p> \"literal\" .\n");
    let hdt = run_hdtc_to_path(temp.path(), &[&input], "blank-only.hdt");

    for (bits, dir) in [("8", "filters-8"), ("16", "filters-16")] {
        let filters = temp.path().join(dir);
        let output = run_sketch(
            &hdt,
            &[
                "--k",
                "2",
                "--filter-bits",
                bits,
                "--output-dir",
                filters.to_str().unwrap(),
            ],
        );
        assert!(
            output.status.success(),
            "sketch failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );

        for (stem, role) in [("subjects", 0), ("objects", 1)] {
            let minhash = fs::read(filters.join(format!("{stem}.minhash"))).unwrap();
            assert_common_envelope(&minhash, b"KGFM", role, 0);
            assert_eq!(read_u32(&minhash, 60), 0);
            assert_eq!(minhash[64], 0);
            assert_eq!(minhash.len(), 76);

            let filter = fs::read(filters.join(format!("{stem}.filter"))).unwrap();
            assert_common_envelope(&filter, b"KGFF", role, 0);
            assert_eq!(filter[56], bits.parse::<u8>().unwrap());
            let fingerprint_len = read_u64(&filter, 88) as usize;
            assert_eq!(fingerprint_len, 12);
            let width = usize::from(filter[56]) / 8;
            assert_eq!(filter.len(), 96 + fingerprint_len * width + 4);
        }
    }
}
