mod common;

use common::{run_hdtc_to_path, write_file};
use crc::{CRC_32_ISCSI, Crc};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use xxhash_rust::xxh64::xxh64;

const KEYSET_NT: &str = r#"<http://example.org/a> <http://example.org/p> <http://example.org/shared> .
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
const SUBJECT_ONLY_IRIS: [&str; 2] = ["http://example.org/a", "http://example.org/subject-only"];
const OBJECT_ONLY_IRIS: [&str; 1] = ["http://example.org/object"];
const SHARED_IRIS: [&str; 1] = ["http://example.org/shared"];
const PREDICATE_IRIS: [&str; 2] = ["http://example.org/p", "http://example.org/p2"];

const HEADER_LEN: usize = 96;

fn build_fixture(temp: &Path) -> PathBuf {
    let input = temp.join("input.nt");
    write_file(&input, KEYSET_NT.as_bytes());
    run_hdtc_to_path(temp, &[&input], "data.hdt")
}

fn run_keyset(hdt: &Path, args: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_hdtc"));
    command.arg("keyset").arg(hdt).args(args);
    command.output().expect("run hdtc keyset")
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn expected_keys(iris: &[&str]) -> Vec<u64> {
    let mut keys: Vec<u64> = iris.iter().map(|iri| xxh64(iri.as_bytes(), 0)).collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// A `.keys` reader written from docs/keyset-format.md §4, applying every §4.4
/// validation rule. Decoding through the specification rather than through the
/// encoder is the point: if the two drift apart, an independent implementation
/// reading the document would disagree with what hdtc emits, and this is what
/// notices.
struct Keyset {
    role: u8,
    encoding: u8,
    low_width: u8,
    key_count: u64,
    min_key: u64,
    max_key: u64,
    source_digest: [u8; 32],
    payload: Vec<u8>,
    keys: Vec<u64>,
    file_len: usize,
}

fn read_keyset(path: &Path) -> Keyset {
    let bytes = fs::read(path).expect("read .keys file");
    assert!(bytes.len() >= HEADER_LEN + 4);

    // Rules 1-7.
    assert_eq!(&bytes[0..8], b"KGFKEYS\0", "magic");
    assert_eq!(u16::from_le_bytes(bytes[8..10].try_into().unwrap()), 1);
    assert_eq!(u16::from_le_bytes(bytes[10..12].try_into().unwrap()), 1);
    assert_eq!(bytes[12], 1, "hash_id = XXH64 seed 0");
    let (role, encoding, low_width) = (bytes[13], bytes[14], bytes[15]);
    assert!(role <= 5, "role");
    assert!(encoding <= 1, "encoding");
    let key_count = read_u64(&bytes, 16);
    let (min_key, max_key) = (read_u64(&bytes, 24), read_u64(&bytes, 32));
    let payload_len = read_u64(&bytes, 40);
    assert_eq!(&bytes[80..96], &[0u8; 16], "reserved must be zero");

    // Rule 8.
    assert_eq!(
        bytes.len() as u64,
        HEADER_LEN as u64 + payload_len + 4,
        "declared file length"
    );
    let crc_at = bytes.len() - 4;
    assert_eq!(
        u32::from_le_bytes(bytes[crc_at..].try_into().unwrap()),
        Crc::<u32>::new(&CRC_32_ISCSI).checksum(&bytes[..crc_at]),
        "CRC32C trailer"
    );

    // Rule 9, in exact (u128) arithmetic, and the decode.
    let payload = bytes[HEADER_LEN..HEADER_LEN + payload_len as usize].to_vec();
    let keys = if encoding == 0 {
        assert_eq!(low_width, 0);
        assert_eq!(u128::from(payload_len), u128::from(key_count) * 8);
        (0..key_count as usize)
            .map(|index| read_u64(&payload, index * 8))
            .collect()
    } else {
        decode_elias_fano(&payload, key_count, low_width, payload_len)
    };

    // Rule 10, plus the SHOULD-level well-formedness check.
    assert_eq!(keys.len() as u64, key_count);
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]), "ascending");
    if key_count == 0 {
        assert_eq!((min_key, max_key), (0, 0));
    } else {
        assert_eq!((min_key, max_key), (keys[0], *keys.last().unwrap()));
    }

    Keyset {
        role,
        encoding,
        low_width,
        key_count,
        min_key,
        max_key,
        source_digest: bytes[48..80].try_into().unwrap(),
        payload,
        keys,
        file_len: bytes.len(),
    }
}

fn decode_elias_fano(payload: &[u8], key_count: u64, low_width: u8, payload_len: u64) -> Vec<u64> {
    if key_count == 0 {
        assert_eq!((low_width, payload_len), (0, 0));
        return Vec::new();
    }
    let l = u32::from(low_width);
    assert_eq!(
        u32::from(low_width),
        63 - key_count.ilog2(),
        "§4.3 sizing rule"
    );
    let low_words = (u128::from(key_count) * u128::from(low_width)).div_ceil(64);
    let high_bits = u128::from(key_count) + (1u128 << (64 - l));
    let high_words = high_bits.div_ceil(64);
    assert_eq!(
        u128::from(payload_len),
        (low_words + high_words) * 8,
        "§4.3 payload length"
    );

    let word = |index: u128| read_u64(payload, index as usize * 8);
    let low = |index: u64| -> u64 {
        if l == 0 {
            return 0;
        }
        let start = index * u64::from(l);
        let (first, offset) = (u128::from(start / 64), start % 64);
        let mut value = word(first) >> offset;
        if offset + u64::from(l) > 64 {
            value |= word(first + 1) << (64 - offset);
        }
        if l == 64 {
            value
        } else {
            value & ((1u64 << l) - 1)
        }
    };

    let mut keys = Vec::with_capacity(key_count as usize);
    let mut found = 0u64;
    for position in 0..high_bits {
        if word(low_words + position / 64) >> (position % 64) & 1 == 1 {
            let high = (position - u128::from(found)) as u64;
            keys.push((high << l) | low(found));
            found += 1;
            if found == key_count {
                break;
            }
        }
    }
    assert_eq!(
        found, key_count,
        "high-bits vector holds key_count set bits"
    );
    keys
}

#[test]
fn keyset_writes_the_disjoint_default_roles() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());

    let output = run_keyset(&hdt, &[]);
    assert!(output.status.success(), "{output:?}");

    let dir = temp.path().join("keysets");
    for role in ["subjects", "objects", "predicates", "terms"] {
        assert!(!dir.join(format!("{role}.keys")).exists(), "{role}");
    }

    let subjects_only = read_keyset(&dir.join("subjects-only.keys"));
    assert_eq!(subjects_only.role, 4);
    assert_eq!(subjects_only.encoding, 1, "elias-fano is the default");
    assert_eq!(subjects_only.keys, expected_keys(&SUBJECT_ONLY_IRIS));

    let objects_only = read_keyset(&dir.join("objects-only.keys"));
    assert_eq!(objects_only.role, 5);
    assert_eq!(objects_only.keys, expected_keys(&OBJECT_ONLY_IRIS));

    let shared = read_keyset(&dir.join("shared.keys"));
    assert_eq!(shared.role, 3);
    assert_eq!(shared.keys, expected_keys(&SHARED_IRIS));

    // The three default sections are pairwise disjoint and bind to one build.
    assert!(
        subjects_only
            .keys
            .iter()
            .all(|key| !objects_only.keys.contains(key) && !shared.keys.contains(key))
    );
    assert!(
        objects_only
            .keys
            .iter()
            .all(|key| !shared.keys.contains(key))
    );
    assert_eq!(subjects_only.source_digest, objects_only.source_digest);
    assert_eq!(subjects_only.source_digest, shared.source_digest);

    // The overlapping subject/object views are losslessly reconstructed.
    let mut subjects = subjects_only.keys.clone();
    subjects.extend_from_slice(&shared.keys);
    subjects.sort_unstable();
    assert_eq!(subjects, expected_keys(&SUBJECT_IRIS));
    let mut objects = objects_only.keys.clone();
    objects.extend_from_slice(&shared.keys);
    objects.sort_unstable();
    assert_eq!(objects, expected_keys(&OBJECT_IRIS));
}

#[test]
fn composite_and_predicate_roles_have_the_documented_membership() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());

    let output = run_keyset(&hdt, &["--roles", "subjects,objects,predicates"]);
    assert!(output.status.success(), "{output:?}");

    let dir = temp.path().join("keysets");
    let subjects = read_keyset(&dir.join("subjects.keys"));
    assert_eq!(subjects.role, 0);
    assert_eq!(subjects.keys, expected_keys(&SUBJECT_IRIS));
    let objects = read_keyset(&dir.join("objects.keys"));
    assert_eq!(objects.role, 1);
    assert_eq!(objects.keys, expected_keys(&OBJECT_IRIS));
    let predicates = read_keyset(&dir.join("predicates.keys"));
    assert_eq!(predicates.role, 2);
    assert_eq!(predicates.keys, expected_keys(&PREDICATE_IRIS));
    assert_eq!(predicates.key_count, 2);
}

#[test]
fn removed_terms_role_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());

    let output = run_keyset(&hdt, &["--roles", "terms"]);
    assert!(!output.status.success(), "terms must no longer be accepted");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("invalid value 'terms'"),
        "{output:?}"
    );
}

#[test]
fn both_encodings_hold_the_same_key_set() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());

    assert!(
        run_keyset(&hdt, &["-o", temp.path().join("ef").to_str().unwrap()])
            .status
            .success()
    );
    let raw = run_keyset(
        &hdt,
        &[
            "-o",
            temp.path().join("raw").to_str().unwrap(),
            "--encoding",
            "raw",
            "--roles",
            "subjects-only,objects-only,shared",
        ],
    );
    assert!(raw.status.success(), "{raw:?}");

    for role in ["subjects-only", "objects-only", "shared"] {
        let ef = read_keyset(&temp.path().join(format!("ef/{role}.keys")));
        let raw = read_keyset(&temp.path().join(format!("raw/{role}.keys")));
        assert_eq!(ef.encoding, 1);
        assert_eq!(raw.encoding, 0);
        assert_eq!(raw.low_width, 0, "low_width is 0 for the raw encoding");
        assert_eq!(ef.keys, raw.keys, "{role}: encodings must agree");
        assert_eq!(ef.source_digest, raw.source_digest);
        assert_eq!(raw.payload.len(), raw.key_count as usize * 8);
    }
}

/// The digest covers the Dictionary-and-Triples suffix only, so rewriting the
/// header leaves a published key set valid (§6).
#[test]
fn keyset_digest_ignores_header_rewrites() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    assert!(run_keyset(&hdt, &[]).status.success());
    let before = read_keyset(&temp.path().join("keysets/subjects-only.keys")).source_digest;

    let renamed = temp.path().join("renamed.hdt");
    let header = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["header", hdt.to_str().unwrap()])
        .args(["--dataset-uri", "http://example.org/renamed"])
        .args(["-o", renamed.to_str().unwrap()])
        .output()
        .expect("run hdtc header");
    assert!(header.status.success(), "{header:?}");

    let output = run_keyset(
        &renamed,
        &["-o", temp.path().join("after").to_str().unwrap()],
    );
    assert!(output.status.success(), "{output:?}");
    let after = read_keyset(&temp.path().join("after/subjects-only.keys"));
    assert_eq!(after.source_digest, before, "header edits do not change it");
}

/// A key set and a sketch of the same HDT describe the same set, because they
/// share one term-to-key convention (§3). A regression in either would show up
/// here first.
#[test]
fn keyset_agrees_with_the_sketch_of_the_same_role() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());

    assert!(run_keyset(&hdt, &["--roles", "subjects"]).status.success());
    let sketch = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["sketch", hdt.to_str().unwrap()])
        .args(["-o", temp.path().join("filters").to_str().unwrap()])
        .args(["--k", "16"])
        .output()
        .expect("run hdtc sketch");
    assert!(sketch.status.success(), "{sketch:?}");

    let keys = read_keyset(&temp.path().join("keysets/subjects.keys"));
    let minhash = fs::read(temp.path().join("filters/subjects.minhash")).unwrap();
    let stored = u32::from_le_bytes(minhash[60..64].try_into().unwrap()) as usize;
    let minima: Vec<u64> = (0..stored)
        .map(|index| read_u64(&minhash, 72 + index * 8))
        .collect();

    // The role is small enough that the sketch is unsaturated and therefore
    // exact, so it must be the key set itself.
    assert_eq!(minhash[64], 0, "unsaturated");
    assert_eq!(minima, keys.keys);
    // And both artifacts bind to the same HDT build.
    assert_eq!(&minhash[24..56], keys.source_digest.as_slice());
}

#[test]
fn keyset_emits_a_well_formed_empty_role() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("blank.nt");
    // No qualifying object IRI: the only object is a literal.
    write_file(
        &input,
        b"<http://example.org/s> <http://example.org/p> \"only a literal\" .\n",
    );
    let hdt = run_hdtc_to_path(temp.path(), &[&input], "blank.hdt");

    for encoding in ["elias-fano", "raw"] {
        let dir = temp.path().join(encoding);
        let output = run_keyset(
            &hdt,
            &[
                "-o",
                dir.to_str().unwrap(),
                "--encoding",
                encoding,
                "--roles",
                "objects-only",
            ],
        );
        assert!(output.status.success(), "{output:?}");

        let objects = read_keyset(&dir.join("objects-only.keys"));
        assert_eq!(objects.key_count, 0, "an empty role is not an error");
        assert_eq!((objects.min_key, objects.max_key), (0, 0));
        assert_eq!(objects.low_width, 0);
        assert!(objects.keys.is_empty());
        assert_eq!(objects.file_len, 100, "§8.6: header plus CRC trailer");
    }
}

#[test]
fn keyset_refuses_to_replace_existing_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    assert!(run_keyset(&hdt, &[]).status.success());

    let shared = temp.path().join("keysets/shared.keys");
    let before = fs::read(&shared).unwrap();
    let output = run_keyset(&hdt, &[]);
    assert!(!output.status.success(), "a second run must fail");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Refusing to replace"),
        "{output:?}"
    );
    assert_eq!(fs::read(&shared).unwrap(), before, "left untouched");
}

/// The key set is built by an external sort, so `--memory-limit` bounds memory
/// without bounding the key count. A limit far below what the role needs must
/// spill and still emit byte-identical output — otherwise the limit is a
/// ceiling on what can be published, which is the thing this design exists to
/// avoid.
#[test]
fn a_tiny_memory_limit_spills_and_emits_identical_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("many.nt");
    // Enough IRIs to overflow the 1 MiB per-role sort budget floor several
    // times over: the budget holds ~131k keys, so 200k spills to 2 chunks.
    let mut nt = String::with_capacity(24 << 20);
    for index in 0..200_000u32 {
        nt.push_str(&format!(
            "<https://example.org/s/{index}> <https://example.org/p> <https://example.org/o/{index}> .\n"
        ));
    }
    write_file(&input, nt.as_bytes());
    let hdt = run_hdtc_to_path(temp.path(), &[&input], "many.hdt");

    let spilled = temp.path().join("spilled");
    let resident = temp.path().join("resident");
    for (dir, limit) in [(&spilled, "1M"), (&resident, "4G")] {
        let output = run_keyset(&hdt, &["-o", dir.to_str().unwrap(), "-m", limit]);
        assert!(output.status.success(), "at -m {limit}: {output:?}");
    }

    for role in ["subjects-only", "objects-only"] {
        let small = read_keyset(&spilled.join(format!("{role}.keys")));
        let large = read_keyset(&resident.join(format!("{role}.keys")));
        assert_eq!(small.key_count, 200_000, "{role}");
        assert_eq!(
            small.keys, large.keys,
            "{role}: the sort path must not matter"
        );
        assert_eq!(
            fs::read(spilled.join(format!("{role}.keys"))).unwrap(),
            fs::read(resident.join(format!("{role}.keys"))).unwrap(),
            "{role}: byte-identical regardless of --memory-limit"
        );
    }
}

#[test]
fn keyset_uses_an_explicit_temp_dir() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    let scratch = temp.path().join("scratch");

    let output = run_keyset(&hdt, &["--temp-dir", scratch.to_str().unwrap()]);
    assert!(output.status.success(), "{output:?}");
    assert!(scratch.is_dir(), "the temp dir is created");
    assert_eq!(
        fs::read_dir(&scratch).unwrap().count(),
        0,
        "temporary key files are cleaned up"
    );
}

/// docs/keyset-format.md §8. These bytes are fully determined by the key set
/// and the encoding — there is no construction freedom to excuse a difference —
/// so an implementation that does not reproduce them is non-conforming.
#[test]
fn keyset_reproduces_the_frozen_conformance_vectors() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("resources.nt");
    let mut nt = String::new();
    for index in 0..100 {
        nt.push_str(&format!(
            "<https://example.org/resource/{index:03}> <https://example.org/p> \"lit\" .\n"
        ));
    }
    write_file(&input, nt.as_bytes());
    let hdt100 = run_hdtc_to_path(temp.path(), &[&input], "r100.hdt");

    let input5 = temp.path().join("five.nt");
    write_file(
        &input5,
        nt.lines().take(5).collect::<Vec<_>>().join("\n").as_bytes(),
    );
    let hdt5 = run_hdtc_to_path(temp.path(), &[&input5], "r5.hdt");

    let build = |hdt: &Path, name: &str, encoding: &str| -> Keyset {
        let dir = temp.path().join(name);
        let output = run_keyset(
            hdt,
            &[
                "-o",
                dir.to_str().unwrap(),
                "--encoding",
                encoding,
                "--roles",
                "subjects",
            ],
        );
        assert!(output.status.success(), "{output:?}");
        read_keyset(&dir.join("subjects.keys"))
    };

    // §8.1 — the same five keys frozen as the sketch spec's small-set vector.
    const SMALL_SET: [u64; 5] = [
        0x00cc_3131_e8f7_a0c5,
        0x0da9_8875_b72f_df91,
        0x35c5_f517_a376_fed8,
        0x45c6_4ad7_8fde_51e4,
        0xaf5a_5827_fae0_76d7,
    ];

    // §8.2 — raw.
    let raw5 = build(&hdt5, "raw5", "raw");
    assert_eq!(raw5.keys, SMALL_SET);
    assert_eq!((raw5.encoding, raw5.low_width), (0, 0));
    assert_eq!(raw5.payload.len(), 40);
    assert_eq!(raw5.file_len, 140);
    assert_eq!(raw5.min_key, SMALL_SET[0]);
    assert_eq!(raw5.max_key, SMALL_SET[4]);
    assert_eq!(
        format!("{:x}", Sha256::digest(&raw5.payload)),
        "05f9f50f4f6d5ac7b569175cfab754e1d2414f786fe67ec5d9a97962f66f2182"
    );
    assert_eq!(
        &raw5.payload[..16],
        &[
            0xC5, 0xA0, 0xF7, 0xE8, 0x31, 0x31, 0xCC, 0x00, 0x91, 0xDF, 0x2F, 0xB7, 0x75, 0x88,
            0xA9, 0x0D
        ],
        "the §8.2 hex transcription"
    );

    // §8.3 — Elias-Fano, and the §8.4 high-bits trace.
    let ef5 = build(&hdt5, "ef5", "elias-fano");
    assert_eq!(ef5.keys, SMALL_SET);
    assert_eq!((ef5.encoding, ef5.low_width), (1, 61));
    assert_eq!(ef5.payload.len(), 48);
    assert_eq!(ef5.file_len, 148);
    assert_eq!(
        format!("{:x}", Sha256::digest(&ef5.payload)),
        "1254a33e82fb1082a0c302380d0f5425fc8b3937f392ab5de90773c5a44246dc"
    );
    assert_eq!(
        read_u64(&ef5.payload, 40),
        0x22B,
        "§8.4: bits 0, 1, 3, 5, 9 of the high-bits vector"
    );
    assert_eq!(
        &ef5.payload[32..40],
        &[0x07, 0xAE, 0x7F, 0x82, 0xA5, 0xF5, 0x00, 0x00],
        "the last low-array word"
    );

    // §8.5 — the 100-key set, at both encodings.
    let ef100 = build(&hdt100, "ef100", "elias-fano");
    assert_eq!(ef100.key_count, 100);
    assert_eq!(ef100.min_key, 0x0095_2d96_04e1_cf2c);
    assert_eq!(ef100.max_key, 0xfe83_c15d_566b_9855);
    assert_eq!((ef100.low_width, ef100.payload.len()), (57, 752));
    assert_eq!(ef100.file_len, 852);
    assert_eq!(
        format!("{:x}", Sha256::digest(&ef100.payload)),
        "a601e77c1f48929b7c9ecc6cc8dd47849123d0d07b42efed73b1666d502056b3"
    );

    let raw100 = build(&hdt100, "raw100", "raw");
    assert_eq!((raw100.low_width, raw100.payload.len()), (0, 800));
    assert_eq!(raw100.file_len, 900);
    assert_eq!(
        format!("{:x}", Sha256::digest(&raw100.payload)),
        "e07ccb2322f490bd3cae2eca78c8a5d01a0d80bb715714f9475617cb66c665d0"
    );
    assert_eq!(raw100.keys, ef100.keys, "the encodings hold one key set");

    // The first 16 keys are the sketch spec's §9.2 toy-set minima, which is the
    // cross-check that both artifact families share one convention.
    assert_eq!(
        &ef100.keys[..4],
        &[
            0x0095_2d96_04e1_cf2c,
            0x00cc_3131_e8f7_a0c5,
            0x01f3_52d0_f9d5_cf80,
            0x06bf_0ecf_32e0_b062,
        ]
    );
}
