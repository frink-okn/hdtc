//! Compatibility tests: verify hdtc output is readable by independent HDT implementations.
//!
//! Layer 1: `hdt` Rust crate (runs on every `cargo test`)
//! Layer 2: hdt-java tools (gated behind `#[ignore]`, requires Java)

mod common;

use common::{run_hdtc_to_path, write_file, REPRESENTATIVE_NT, REPRESENTATIVE_TRIPLE_COUNT};
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

/// Generate the representative HDT file in a temp directory and return the path.
fn generate_representative_hdt() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let nt_path = temp.path().join("input.nt");
    write_file(&nt_path, REPRESENTATIVE_NT.as_bytes());
    let hdt_path = run_hdtc_to_path(temp.path(), &[&nt_path], "representative.hdt");
    (temp, hdt_path)
}

// ---------------------------------------------------------------------------
// Layer 1: hdt Rust crate
// ---------------------------------------------------------------------------

/// The most critical test: the `hdt` crate can read our HDT file at all.
#[test]
fn test_hdt_crate_reads_hdtc_output() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).expect("open HDT file");
    let _hdt = hdt::Hdt::read(BufReader::new(file)).expect("hdt crate should read our HDT");
}

/// Total triple count matches our expected count.
#[test]
fn test_hdt_crate_total_triple_count() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let count = hdt.triples_with_pattern(None, None, None).count();
    assert_eq!(
        count, REPRESENTATIVE_TRIPLE_COUNT,
        "expected {REPRESENTATIVE_TRIPLE_COUNT} triples, got {count}"
    );
}

/// Query a specific URI-URI-URI triple: alice knows bob.
#[test]
fn test_hdt_crate_specific_uri_triple() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("http://example.org/alice"),
            Some("http://example.org/knows"),
            Some("http://example.org/bob"),
        )
        .collect();
    assert_eq!(results.len(), 1, "alice-knows-bob should return exactly 1 triple");
}

/// Query a plain literal: alice's name is "Alice".
#[test]
fn test_hdt_crate_plain_literal() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("http://example.org/alice"),
            Some("http://example.org/name"),
            Some("\"Alice\""),
        )
        .collect();
    assert_eq!(results.len(), 1, "alice-name-Alice should return exactly 1 triple");
}

/// Query a typed literal: alice's age is "30"^^xsd:integer.
#[test]
fn test_hdt_crate_typed_literal() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("http://example.org/alice"),
            Some("http://example.org/age"),
            Some("\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        )
        .collect();
    assert_eq!(results.len(), 1, "alice-age-30^^xsd:integer should return 1 triple");
}

/// Query a language-tagged literal: alice's label is "Alice"@en.
#[test]
fn test_hdt_crate_language_tagged_literal() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("http://example.org/alice"),
            Some("http://example.org/label"),
            Some("\"Alice\"@en"),
        )
        .collect();
    assert_eq!(results.len(), 1, "alice-label-Alice@en should return 1 triple");
}

/// Query a blank node triple.
/// For single-file input, hdtc now preserves original blank node IDs.
#[test]
fn test_hdt_crate_blank_node() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(
            Some("_:b1"),
            Some("http://example.org/type"),
            Some("http://example.org/Thing"),
        )
        .collect();
    assert_eq!(results.len(), 1, "blank node triple should return 1 triple");
}

/// Wildcard predicate: all "knows" triples should return 2 (alice->bob, bob->alice).
#[test]
fn test_hdt_crate_wildcard_predicate() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let results: Vec<_> = hdt
        .triples_with_pattern(None, Some("http://example.org/knows"), None)
        .collect();
    assert_eq!(results.len(), 2, "?-knows-? should return 2 triples");
}

/// Verify all triples are present by collecting and sorting them.
#[test]
fn test_hdt_crate_all_triples_content() {
    let (_temp, hdt_path) = generate_representative_hdt();
    let file = std::fs::File::open(&hdt_path).unwrap();
    let hdt = hdt::Hdt::read(BufReader::new(file)).unwrap();

    let mut triples: Vec<[String; 3]> = hdt
        .triples_with_pattern(None, None, None)
        .map(|t: [Arc<str>; 3]| [t[0].to_string(), t[1].to_string(), t[2].to_string()])
        .collect();
    triples.sort();

    let mut expected = [
        ["http://example.org/alice", "http://example.org/name", "\"Alice\""],
        ["http://example.org/alice", "http://example.org/knows", "http://example.org/bob"],
        ["http://example.org/bob", "http://example.org/name", "\"Bob\""],
        ["http://example.org/bob", "http://example.org/knows", "http://example.org/alice"],
        [
            "http://example.org/alice",
            "http://example.org/age",
            "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        ],
        ["http://example.org/alice", "http://example.org/label", "\"Alice\"@en"],
        ["http://example.org/alice", "http://example.org/label", "\"Alicia\"@es"],
        ["_:b1", "http://example.org/type", "http://example.org/Thing"],
    ];
    expected.sort();

    assert_eq!(triples.len(), expected.len(), "triple count mismatch");
    for (actual, exp) in triples.iter().zip(expected.iter()) {
        assert_eq!(
            actual, exp,
            "triple mismatch: got {:?}, expected {:?}",
            actual, exp
        );
    }
}

// ---------------------------------------------------------------------------
// Layer 2: hdt-java (requires Java, gated behind #[ignore])
// ---------------------------------------------------------------------------

use std::path::Path;
use std::process::Command;
use std::sync::Once;

const HDT_JAVA_VERSION: &str = "3.0.10";

static DOWNLOAD_ONCE: Once = Once::new();

/// Ensure the hdt-java distribution is downloaded and extracted.
/// Returns the path to the hdt-java directory containing `lib/` and `bin/`.
fn ensure_hdt_java() -> PathBuf {
    let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-data")
        .join("hdt-java");

    let marker = base_dir.join(".downloaded");

    DOWNLOAD_ONCE.call_once(|| {
        if marker.exists() {
            return;
        }

        std::fs::create_dir_all(&base_dir).expect("create hdt-java dir");

        let url = format!(
            "https://github.com/rdfhdt/hdt-java/releases/download/v{HDT_JAVA_VERSION}/rdfhdt.tar.gz"
        );

        eprintln!("Downloading hdt-java from {url}...");

        let mut curl = Command::new("curl")
            .args(["-fsSL", &url])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("Failed to start curl");

        let curl_stdout = curl.stdout.take().unwrap();

        let tar_output = Command::new("tar")
            .args(["xzf", "-", "--strip-components=1"])
            .current_dir(&base_dir)
            .stdin(curl_stdout)
            .output()
            .expect("Failed to extract hdt-java tarball");

        curl.wait().expect("Failed to wait for curl");

        assert!(
            tar_output.status.success(),
            "tar extraction failed: {}",
            String::from_utf8_lossy(&tar_output.stderr)
        );

        // Write marker file
        std::fs::write(&marker, "ok").expect("write marker");
        eprintln!("hdt-java downloaded to {}", base_dir.display());
    });

    assert!(marker.exists(), "hdt-java download marker not found");
    base_dir
}

/// Build the Java classpath from the hdt-java lib directory.
fn hdt_java_classpath(hdt_java_dir: &Path) -> String {
    let lib_dir = hdt_java_dir.join("lib");
    let jars: Vec<String> = std::fs::read_dir(&lib_dir)
        .expect("read hdt-java lib dir")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "jar") {
                Some(path.to_str().unwrap().to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(!jars.is_empty(), "No jar files found in {}", lib_dir.display());
    jars.join(":")
}

/// Run hdt-java's rdf2hdt conversion using the downloaded distribution.
fn run_hdt_java_rdf2hdt(hdt_java_dir: &Path, input_rdf: &Path, output_hdt: &Path) {
    let script = hdt_java_dir.join("bin").join("rdf2hdt.sh");

    let output = Command::new("bash")
        .arg(script.to_str().expect("valid rdf2hdt script path"))
        .arg(input_rdf.to_str().expect("valid input RDF path"))
        .arg(output_hdt.to_str().expect("valid output HDT path"))
        .output()
        .expect("Failed to run hdt-java rdf2hdt.sh");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("rdf2hdt.sh stdout:\n{stdout}");
    eprintln!("rdf2hdt.sh stderr:\n{stderr}");

    assert!(
        output.status.success(),
        "hdt-java rdf2hdt failed with exit code {:?}\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );
    assert!(
        output_hdt.exists(),
        "hdt-java rdf2hdt did not produce output file: {}",
        output_hdt.display()
    );
}

#[derive(Debug)]
struct HdtSectionRanges {
    dict_start: usize,
    triples_start: usize,
}

#[derive(Debug)]
struct Span {
    name: &'static str,
    start: usize,
    end: usize,
}

#[derive(Debug)]
struct ControlInfoView {
    start: usize,
    end: usize,
    control_type: u8,
    format: String,
    properties: std::collections::BTreeMap<String, String>,
}

fn parse_null_terminated_utf8(bytes: &[u8], start: usize) -> (String, usize) {
    let mut idx = start;
    while idx < bytes.len() && bytes[idx] != 0 {
        idx += 1;
    }
    assert!(idx < bytes.len(), "unterminated string at offset {start}");
    let text = std::str::from_utf8(&bytes[start..idx])
        .expect("control info string must be UTF-8")
        .to_string();
    (text, idx + 1)
}

fn parse_properties(props: &str) -> std::collections::BTreeMap<String, String> {
    let mut map = std::collections::BTreeMap::new();
    for entry in props.split(';').filter(|p| !p.is_empty()) {
        if let Some((k, v)) = entry.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}

fn decode_vbyte_at(bytes: &[u8], start: usize) -> (u64, usize) {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut idx = start;
    loop {
        assert!(idx < bytes.len(), "truncated VByte at offset {start}");
        let byte = bytes[idx];
        value |= ((byte & 0x7F) as u64) << shift;
        idx += 1;
        if byte & 0x80 != 0 {
            return (value, idx - start);
        }
        shift += 7;
        assert!(shift < 64, "VByte exceeds u64 at offset {start}");
    }
}

fn parse_logarray_len_at(bytes: &[u8], start: usize) -> usize {
    assert!(start + 3 <= bytes.len(), "truncated LogArray at offset {start}");
    let _type_byte = bytes[start];
    let bits_per_entry = bytes[start + 1] as usize;
    let (num_entries, num_entries_len) = decode_vbyte_at(bytes, start + 2);
    let preamble_len = 1 + 1 + num_entries_len + 1;
    let data_bits = (num_entries as usize) * bits_per_entry;
    let data_len = data_bits.div_ceil(8);
    let total_len = preamble_len + data_len + 4;
    assert!(start + total_len <= bytes.len(), "truncated LogArray payload at offset {start}");
    total_len
}

fn decode_logarray_values(bytes: &[u8]) -> Vec<u64> {
    assert!(!bytes.is_empty(), "empty LogArray component");
    assert_eq!(bytes[0], 1, "unexpected LogArray type byte");

    let bits_per_entry = bytes[1] as usize;
    let (num_entries, num_entries_len) = decode_vbyte_at(bytes, 2);
    let num_entries = num_entries as usize;

    let preamble_len = 1 + 1 + num_entries_len + 1;
    let data_len = (num_entries * bits_per_entry).div_ceil(8);
    let data_start = preamble_len;
    let data_end = data_start + data_len;
    assert!(data_end + 4 <= bytes.len(), "truncated LogArray component bytes");

    let data = &bytes[data_start..data_end];

    let mut values = Vec::with_capacity(num_entries);
    for index in 0..num_entries {
        let bit_pos = index * bits_per_entry;
        let byte_index = bit_pos / 8;
        let bit_offset = bit_pos % 8;

        let mut scratch = [0u8; 16];
        let available = data.len().saturating_sub(byte_index).min(16);
        scratch[..available].copy_from_slice(&data[byte_index..byte_index + available]);
        let chunk = u128::from_le_bytes(scratch);

        let value = if bits_per_entry == 0 {
            0
        } else if bits_per_entry == 64 {
            ((chunk >> bit_offset) & (u64::MAX as u128)) as u64
        } else {
            let mask = (1u128 << bits_per_entry) - 1;
            ((chunk >> bit_offset) & mask) as u64
        };
        values.push(value);
    }

    values
}

fn decode_bitmap_bits(bytes: &[u8]) -> Vec<bool> {
    assert!(!bytes.is_empty(), "empty Bitmap component");
    assert_eq!(bytes[0], 1, "unexpected Bitmap type byte");

    let (num_bits, num_bits_len) = decode_vbyte_at(bytes, 1);
    let num_bits = num_bits as usize;
    let preamble_len = 1 + num_bits_len + 1;
    let data_len = num_bits.div_ceil(8);
    let data_start = preamble_len;
    let data_end = data_start + data_len;
    assert!(data_end + 4 <= bytes.len(), "truncated Bitmap component bytes");

    let data = &bytes[data_start..data_end];
    let mut bits = Vec::with_capacity(num_bits);
    for i in 0..num_bits {
        let byte = data[i / 8];
        bits.push(((byte >> (i % 8)) & 1) == 1);
    }
    bits
}

fn decode_pfc_strings(bytes: &[u8]) -> Vec<String> {
    assert!(!bytes.is_empty(), "empty PFC component");
    assert_eq!(bytes[0], 2, "unexpected PFC type byte");

    let (count, count_len) = decode_vbyte_at(bytes, 1);
    let (buffer_len, buffer_len_len) = decode_vbyte_at(bytes, 1 + count_len);
    let (block_size, block_size_len) = decode_vbyte_at(bytes, 1 + count_len + buffer_len_len);
    let count = count as usize;
    let buffer_len = buffer_len as usize;
    let block_size = block_size as usize;

    let preamble_len = 1 + count_len + buffer_len_len + block_size_len + 1;
    let log_array_start = preamble_len;
    let log_array_len = parse_logarray_len_at(bytes, log_array_start);
    let payload_start = log_array_start + log_array_len;
    let payload_end = payload_start + buffer_len;
    assert!(payload_end + 4 <= bytes.len(), "truncated PFC payload bytes");

    let buf = &bytes[payload_start..payload_end];
    let mut strings = Vec::with_capacity(count);
    let mut pos = 0usize;
    let mut prev = String::new();

    for i in 0..count {
        if i % block_size == 0 {
            let end = buf[pos..]
                .iter()
                .position(|&b| b == 0)
                .expect("missing null terminator in PFC block start");
            let s = std::str::from_utf8(&buf[pos..pos + end])
                .expect("PFC string not valid UTF-8")
                .to_string();
            pos += end + 1;
            prev = s.clone();
            strings.push(s);
        } else {
            let (shared, shared_len) = decode_vbyte_at(buf, pos);
            pos += shared_len;
            let end = buf[pos..]
                .iter()
                .position(|&b| b == 0)
                .expect("missing null terminator in PFC suffix");
            let suffix = std::str::from_utf8(&buf[pos..pos + end])
                .expect("PFC suffix not valid UTF-8");
            pos += end + 1;

            let shared = shared as usize;
            let mut s = String::with_capacity(shared + suffix.len());
            s.push_str(&prev[..shared]);
            s.push_str(suffix);
            prev = s.clone();
            strings.push(s);
        }
    }

    strings
}

fn parse_bitmap_len_at(bytes: &[u8], start: usize) -> usize {
    assert!(start + 2 <= bytes.len(), "truncated Bitmap at offset {start}");
    let _type_byte = bytes[start];
    let (num_bits, num_bits_len) = decode_vbyte_at(bytes, start + 1);
    let preamble_len = 1 + num_bits_len + 1;
    let data_len = (num_bits as usize).div_ceil(8);
    let total_len = preamble_len + data_len + 4;
    assert!(start + total_len <= bytes.len(), "truncated Bitmap payload at offset {start}");
    total_len
}

fn parse_pfc_len_at(bytes: &[u8], start: usize) -> usize {
    assert!(start + 2 <= bytes.len(), "truncated PFC section at offset {start}");
    let _type_byte = bytes[start];

    let (_count, count_len) = decode_vbyte_at(bytes, start + 1);
    let (buffer_len, buffer_len_len) = decode_vbyte_at(bytes, start + 1 + count_len);
    let (_block_size, block_size_len) = decode_vbyte_at(bytes, start + 1 + count_len + buffer_len_len);

    let preamble_len = 1 + count_len + buffer_len_len + block_size_len + 1;
    let log_array_start = start + preamble_len;
    let log_array_len = parse_logarray_len_at(bytes, log_array_start);

    let payload_start = log_array_start + log_array_len;
    let payload_len = buffer_len as usize + 4;
    let total_len = preamble_len + log_array_len + payload_len;
    assert!(payload_start + payload_len <= bytes.len(), "truncated PFC payload at offset {start}");
    total_len
}

fn parse_control_info_at(bytes: &[u8], start: usize) -> ControlInfoView {
    assert!(bytes.len() >= start + 7, "truncated HDT control info at offset {start}");
    assert_eq!(&bytes[start..start + 4], b"$HDT", "missing $HDT magic at offset {start}");

    let control_type = bytes[start + 4];
    let (format, idx_after_format) = parse_null_terminated_utf8(bytes, start + 5);
    let (props_str, idx_after_props) = parse_null_terminated_utf8(bytes, idx_after_format);
    assert!(
        idx_after_props + 1 < bytes.len(),
        "truncated control info CRC16 at offset {start}"
    );

    ControlInfoView {
        start,
        end: idx_after_props + 2,
        control_type,
        format,
        properties: parse_properties(&props_str),
    }
}

fn parse_control_info_len(bytes: &[u8], start: usize) -> (u8, usize) {
    let ci = parse_control_info_at(bytes, start);
    (ci.control_type, ci.end - ci.start)
}

fn read_header_length(bytes: &[u8], header_ci_start: usize) -> usize {
    let ci = parse_control_info_at(bytes, header_ci_start);
    assert_eq!(
        ci.control_type, 2,
        "expected header control type at offset {header_ci_start}"
    );
    ci.properties
        .get("length")
        .and_then(|s| s.parse::<usize>().ok())
        .expect("header control info missing length property")
}

fn locate_dict_and_triples(bytes: &[u8]) -> HdtSectionRanges {
    let (global_type, global_len) = parse_control_info_len(bytes, 0);
    assert_eq!(global_type, 1, "expected global control type at start of file");

    let header_ci_start = global_len;
    let (header_type, header_ci_len) = parse_control_info_len(bytes, header_ci_start);
    assert_eq!(header_type, 2, "expected header control type");

    let header_len = read_header_length(bytes, header_ci_start);
    let dict_start = header_ci_start + header_ci_len + header_len;

    let (dict_type, _dict_ci_len) = parse_control_info_len(bytes, dict_start);
    assert_eq!(dict_type, 3, "expected dictionary control type");

    let mut triples_start = None;
    let mut idx = dict_start + 4;
    while idx + 5 <= bytes.len() {
        if &bytes[idx..idx + 4] == b"$HDT" && bytes[idx + 4] == 4 {
            triples_start = Some(idx);
            break;
        }
        idx += 1;
    }

    let triples_start = triples_start.expect("failed to locate triples control info marker");
    HdtSectionRanges {
        dict_start,
        triples_start,
    }
}

fn first_diff_offset(left: &[u8], right: &[u8]) -> Option<usize> {
    let min_len = left.len().min(right.len());
    for i in 0..min_len {
        if left[i] != right[i] {
            return Some(i);
        }
    }
    if left.len() != right.len() {
        Some(min_len)
    } else {
        None
    }
}

fn hex_window(data: &[u8], center: usize, radius: usize) -> String {
    let start = center.saturating_sub(radius);
    let end = (center + radius + 1).min(data.len());
    let mut out = String::new();
    for (i, b) in data[start..end].iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&format!("{:02x}", b));
    }
    format!("offsets {start}..{end}: {out}")
}

fn format_control_info(ci: &ControlInfoView) -> String {
    let mut props: Vec<String> = ci
        .properties
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    props.sort();
    format!(
        "type={}, format={}, properties=[{}]",
        ci.control_type,
        ci.format,
        props.join(", ")
    )
}

fn section_diff_report(
    section_name: &str,
    hdtc_section: &[u8],
    java_section: &[u8],
    hdtc_ci: &ControlInfoView,
    java_ci: &ControlInfoView,
) -> Option<String> {
    let diff = first_diff_offset(hdtc_section, java_section)?;

    let hdtc_byte = hdtc_section.get(diff).copied();
    let java_byte = java_section.get(diff).copied();

    Some(format!(
        "{section_name} mismatch at relative offset {diff}\n  hdtc len={} java len={}\n  hdtc ci: {}\n  java ci: {}\n  hdtc byte={:?} java byte={:?}\n  hdtc window: {}\n  java window: {}",
        hdtc_section.len(),
        java_section.len(),
        format_control_info(hdtc_ci),
        format_control_info(java_ci),
        hdtc_byte,
        java_byte,
        hex_window(hdtc_section, diff, 16),
        hex_window(java_section, diff, 16),
    ))
}

fn find_component<'a>(components: &'a [Span], name: &str) -> &'a Span {
    components
        .iter()
        .find(|s| s.name == name)
        .expect("missing component")
}

fn parse_dictionary_components(bytes: &[u8], dict_start: usize, triples_start: usize) -> Vec<Span> {
    let dict_ci = parse_control_info_at(bytes, dict_start);
    let mut spans = vec![Span {
        name: "DictionaryControlInfo",
        start: dict_ci.start,
        end: dict_ci.end,
    }];

    let mut pos = dict_ci.end;
    for name in ["SharedPFC", "SubjectsPFC", "PredicatesPFC", "ObjectsPFC"] {
        let len = parse_pfc_len_at(bytes, pos);
        spans.push(Span {
            name,
            start: pos,
            end: pos + len,
        });
        pos += len;
    }

    assert_eq!(pos, triples_start, "dictionary component boundaries do not reach triples marker exactly");
    spans
}

fn parse_triples_components(bytes: &[u8], triples_start: usize) -> Vec<Span> {
    let triples_ci = parse_control_info_at(bytes, triples_start);
    let mut spans = vec![Span {
        name: "TriplesControlInfo",
        start: triples_ci.start,
        end: triples_ci.end,
    }];

    let mut pos = triples_ci.end;
    for name in ["BitmapY", "BitmapZ", "ArrayY", "ArrayZ"] {
        let len = if name.starts_with("Bitmap") {
            parse_bitmap_len_at(bytes, pos)
        } else {
            parse_logarray_len_at(bytes, pos)
        };
        spans.push(Span {
            name,
            start: pos,
            end: pos + len,
        });
        pos += len;
    }

    assert_eq!(pos, bytes.len(), "triples component boundaries do not reach file end exactly");
    spans
}

fn component_diff_report(
    section_name: &str,
    component_name: &str,
    left: &[u8],
    right: &[u8],
) -> Option<String> {
    let diff = first_diff_offset(left, right)?;
    Some(format!(
        "{section_name} component {component_name} mismatch at relative offset {diff} (left len {}, right len {})\n  left window: {}\n  right window: {}",
        left.len(),
        right.len(),
        hex_window(left, diff, 16),
        hex_window(right, diff, 16),
    ))
}

/// hdt-java HDTVerify: structural integrity check.
#[test]
#[ignore]
fn test_hdtjava_verify() {
    let hdt_java_dir = ensure_hdt_java();
    let (_temp, hdt_path) = generate_representative_hdt();
    let classpath = hdt_java_classpath(&hdt_java_dir);

    let output = Command::new("java")
        .args([
            "-cp",
            &classpath,
            "org.rdfhdt.hdt.tools.HDTVerify",
            hdt_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to run HDTVerify");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("HDTVerify stdout:\n{stdout}");
    eprintln!("HDTVerify stderr:\n{stderr}");
    assert!(
        output.status.success(),
        "HDTVerify failed with exit code {:?}\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );
}

/// hdt-java HDT2RDF round-trip: convert HDT back to N-Triples and compare.
#[test]
#[ignore]
fn test_hdtjava_roundtrip_ntriples() {
    let hdt_java_dir = ensure_hdt_java();
    let (temp, hdt_path) = generate_representative_hdt();
    let classpath = hdt_java_classpath(&hdt_java_dir);
    let output_nt = temp.path().join("roundtrip.nt");

    let output = Command::new("java")
        .args([
            "-cp",
            &classpath,
            "org.rdfhdt.hdt.tools.HDT2RDF",
            hdt_path.to_str().unwrap(),
            output_nt.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to run HDT2RDF");

    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("HDT2RDF stderr:\n{stderr}");
    assert!(
        output.status.success(),
        "HDT2RDF failed with exit code {:?}\nstderr: {stderr}",
        output.status.code()
    );

    // Read and sort actual output lines
    let actual_content = std::fs::read_to_string(&output_nt).expect("read roundtrip output");
    let mut actual_lines: Vec<&str> = actual_content
        .lines()
        .filter(|l| !l.is_empty())
        .collect();
    actual_lines.sort();

    let expected_nt = REPRESENTATIVE_NT.to_string();
    let mut expected_lines: Vec<&str> = expected_nt
        .lines()
        .filter(|l| !l.is_empty())
        .collect();
    expected_lines.sort();

    // hdt-java may output blank nodes with its own prefix, so normalize blank node IDs.
    // We compare structure but allow different blank node prefixes.
    let normalize_bnode = |line: &str| -> String {
        // Replace any _:XXXX pattern with a canonical form for comparison
        let mut result = line.to_string();
        if let Some(start) = result.find("_:") {
            let end = result[start + 2..]
                .find(|c: char| c.is_whitespace())
                .map_or(result.len(), |e| start + 2 + e);
            result.replace_range(start..end, "_:BNODE");
        }
        result
    };

    let mut actual_normalized: Vec<String> = actual_lines.iter().map(|l| normalize_bnode(l)).collect();
    let mut expected_normalized: Vec<String> = expected_lines.iter().map(|l| normalize_bnode(l)).collect();
    actual_normalized.sort();
    expected_normalized.sort();

    assert_eq!(
        actual_normalized.len(),
        expected_normalized.len(),
        "line count mismatch: got {}, expected {}",
        actual_normalized.len(),
        expected_normalized.len()
    );

    for (actual, expected) in actual_normalized.iter().zip(expected_normalized.iter()) {
        assert_eq!(
            actual, expected,
            "N-Triples mismatch:\n  actual:   {actual}\n  expected: {expected}"
        );
    }
}

/// Generate HDT with hdtc and hdt-java from the same RDF input and compare
/// parsed dictionary/triples structures for semantic equivalence.
#[test]
#[ignore]
fn test_hdtjava_exact_dictionary_and_triples_bytes() {
    let hdt_java_dir = ensure_hdt_java();
    let temp = tempfile::tempdir().expect("create temp dir");

    let input_nt = temp.path().join("input.nt");
    write_file(&input_nt, REPRESENTATIVE_NT.as_bytes());

    let hdtc_hdt = run_hdtc_to_path(temp.path(), &[&input_nt], "hdtc.hdt");
    let java_hdt = temp.path().join("java.hdt");
    run_hdt_java_rdf2hdt(&hdt_java_dir, &input_nt, &java_hdt);

    let hdtc_bytes = std::fs::read(&hdtc_hdt).expect("read hdtc HDT bytes");
    let java_bytes = std::fs::read(&java_hdt).expect("read hdt-java HDT bytes");

    let hdtc_ranges = locate_dict_and_triples(&hdtc_bytes);
    let java_ranges = locate_dict_and_triples(&java_bytes);

    let hdtc_dict_components = parse_dictionary_components(&hdtc_bytes, hdtc_ranges.dict_start, hdtc_ranges.triples_start);
    let java_dict_components = parse_dictionary_components(&java_bytes, java_ranges.dict_start, java_ranges.triples_start);
    let hdtc_triples_components = parse_triples_components(&hdtc_bytes, hdtc_ranges.triples_start);
    let java_triples_components = parse_triples_components(&java_bytes, java_ranges.triples_start);

    for component in ["SharedPFC", "SubjectsPFC", "PredicatesPFC", "ObjectsPFC"] {
        let hc = find_component(&hdtc_dict_components, component);
        let jc = find_component(&java_dict_components, component);
        let left = decode_pfc_strings(&hdtc_bytes[hc.start..hc.end]);
        let right = decode_pfc_strings(&java_bytes[jc.start..jc.end]);
        assert_eq!(left, right, "Decoded dictionary component {component} differs");
    }

    for component in ["BitmapY", "BitmapZ"] {
        let hc = find_component(&hdtc_triples_components, component);
        let jc = find_component(&java_triples_components, component);
        let left = decode_bitmap_bits(&hdtc_bytes[hc.start..hc.end]);
        let right = decode_bitmap_bits(&java_bytes[jc.start..jc.end]);
        assert_eq!(left, right, "Decoded triples component {component} differs");
    }

    for component in ["ArrayY", "ArrayZ"] {
        let hc = find_component(&hdtc_triples_components, component);
        let jc = find_component(&java_triples_components, component);
        let left = decode_logarray_values(&hdtc_bytes[hc.start..hc.end]);
        let right = decode_logarray_values(&java_bytes[jc.start..jc.end]);
        assert_eq!(left, right, "Decoded triples component {component} differs");
    }
}

/// Diagnostic parity test with detailed section metadata + byte-window diff.
/// Run this when exact parity fails to quickly identify root causes.
#[test]
#[ignore]
fn test_hdtjava_exact_dictionary_and_triples_diagnostics() {
    let hdt_java_dir = ensure_hdt_java();
    let temp = tempfile::tempdir().expect("create temp dir");

    let input_nt = temp.path().join("input.nt");
    write_file(&input_nt, REPRESENTATIVE_NT.as_bytes());

    let hdtc_hdt = run_hdtc_to_path(temp.path(), &[&input_nt], "hdtc.hdt");
    let java_hdt = temp.path().join("java.hdt");
    run_hdt_java_rdf2hdt(&hdt_java_dir, &input_nt, &java_hdt);

    let hdtc_bytes = std::fs::read(&hdtc_hdt).expect("read hdtc HDT bytes");
    let java_bytes = std::fs::read(&java_hdt).expect("read hdt-java HDT bytes");

    let hdtc_ranges = locate_dict_and_triples(&hdtc_bytes);
    let java_ranges = locate_dict_and_triples(&java_bytes);

    let hdtc_dict_ci = parse_control_info_at(&hdtc_bytes, hdtc_ranges.dict_start);
    let java_dict_ci = parse_control_info_at(&java_bytes, java_ranges.dict_start);
    let hdtc_triples_ci = parse_control_info_at(&hdtc_bytes, hdtc_ranges.triples_start);
    let java_triples_ci = parse_control_info_at(&java_bytes, java_ranges.triples_start);

    let hdtc_dict = &hdtc_bytes[hdtc_ranges.dict_start..hdtc_ranges.triples_start];
    let java_dict = &java_bytes[java_ranges.dict_start..java_ranges.triples_start];
    let hdtc_triples = &hdtc_bytes[hdtc_ranges.triples_start..];
    let java_triples = &java_bytes[java_ranges.triples_start..];

    let hdtc_dict_components = parse_dictionary_components(&hdtc_bytes, hdtc_ranges.dict_start, hdtc_ranges.triples_start);
    let java_dict_components = parse_dictionary_components(&java_bytes, java_ranges.dict_start, java_ranges.triples_start);
    let hdtc_triples_components = parse_triples_components(&hdtc_bytes, hdtc_ranges.triples_start);
    let java_triples_components = parse_triples_components(&java_bytes, java_ranges.triples_start);

    let mut reports = Vec::new();
    if let Some(r) = section_diff_report("Dictionary section", hdtc_dict, java_dict, &hdtc_dict_ci, &java_dict_ci) {
        reports.push(r);
    }
    if let Some(r) = section_diff_report("Triples section", hdtc_triples, java_triples, &hdtc_triples_ci, &java_triples_ci) {
        reports.push(r);
    }

    for component in ["SharedPFC", "SubjectsPFC", "PredicatesPFC", "ObjectsPFC"] {
        let hc = find_component(&hdtc_dict_components, component);
        let jc = find_component(&java_dict_components, component);
        let left = &hdtc_bytes[hc.start..hc.end];
        let right = &java_bytes[jc.start..jc.end];
        if let Some(r) = component_diff_report("Dictionary", component, left, right) {
            reports.push(r);
        }
    }

    for component in ["BitmapY", "BitmapZ", "ArrayY", "ArrayZ"] {
        let hc = find_component(&hdtc_triples_components, component);
        let jc = find_component(&java_triples_components, component);
        let left = &hdtc_bytes[hc.start..hc.end];
        let right = &java_bytes[jc.start..jc.end];
        if let Some(r) = component_diff_report("Triples", component, left, right) {
            reports.push(r);
        }
    }

    let hdtc_array_y_span = find_component(&hdtc_triples_components, "ArrayY");
    let java_array_y_span = find_component(&java_triples_components, "ArrayY");
    let hdtc_array_y_bytes = &hdtc_bytes[hdtc_array_y_span.start..hdtc_array_y_span.end];
    let java_array_y_bytes = &java_bytes[java_array_y_span.start..java_array_y_span.end];
    let hdtc_array_y = decode_logarray_values(hdtc_array_y_bytes);
    let java_array_y = decode_logarray_values(java_array_y_bytes);

    if hdtc_array_y != java_array_y {
        reports.push(format!(
            "Decoded ArrayY values differ\n  hdtc: {:?}\n  java: {:?}",
            hdtc_array_y, java_array_y
        ));
    } else if first_diff_offset(hdtc_array_y_bytes, java_array_y_bytes).is_some() {
        reports.push(format!(
            "Decoded ArrayY values are identical ({:?}), but raw ArrayY bytes differ; this indicates a non-semantic LogArray encoding difference (e.g., trailing padding bits and resulting CRC32C).",
            hdtc_array_y
        ));
    }

    assert!(
        reports.is_empty(),
        "HDT parity diagnostics:\n{}",
        reports.join("\n\n")
    );
}
