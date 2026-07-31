//! The library façade, exercised the way a downstream crate uses it.
//!
//! `src/format.rs` is hdtc's published surface: KGF's `kgf-store` maps the same
//! files this crate writes and depends on these re-exports for section location,
//! identity, and the `.hdt.perm` directory (KGF doc 20 §20.4). Linking hdtc as a
//! library is therefore a contract, and this test is what fails when the contract
//! is broken — a private module, a renamed export, or a directory accessor that
//! stops describing the file well enough to map it.

mod common;

use common::{REPRESENTATIVE_NT, write_file};
use hdtc::format::{
    PermutationComponent, PermutationIndex, PermutationSectionKind, packed_len,
    permutation_index_path, scan_hdt_sections, sha256_to_end,
};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;

const CORE_KINDS: [PermutationSectionKind; 8] = [
    PermutationSectionKind::ArrayY,
    PermutationSectionKind::BitmapY,
    PermutationSectionKind::ArrayZ,
    PermutationSectionKind::BitmapZ,
    PermutationSectionKind::BitmapYSuperrank,
    PermutationSectionKind::BitmapYSubrank,
    PermutationSectionKind::BitmapZSuperrank,
    PermutationSectionKind::BitmapZSubrank,
];

fn build_fixture(temp: &Path) -> PathBuf {
    let input = temp.join("input.nt");
    let hdt = temp.join("data.hdt");
    write_file(&input, REPRESENTATIVE_NT.as_bytes());

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            input.to_str().unwrap(),
            "-o",
            hdt.to_str().unwrap(),
            "--temp-dir",
            temp.join("work").to_str().unwrap(),
            "--memory-limit",
            "64M",
            "--perm",
        ])
        .output()
        .expect("run hdtc");
    assert!(
        output.status.success(),
        "hdtc create failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    hdt
}

#[test]
fn the_permutation_directory_describes_every_region_well_enough_to_map_it() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    let perm = permutation_index_path(&hdt);
    assert!(perm.exists(), "`create` should emit {}", perm.display());

    let index = PermutationIndex::open(&perm, &hdt).expect("open permutation index");
    let header = index.header();
    let sections = index.sections();

    assert_eq!(index.path(), perm.as_path());
    assert!(header.triples > 0);
    assert!(header.subjects > 0 && header.predicates > 0 && header.objects > 0);
    assert_eq!(
        header.pos_pairs, header.ops_pairs,
        "POS and OPS index the same set of (predicate, object) pairs"
    );

    // Twenty core sections, ascending and unique — §5.1.
    assert!(sections.len() >= 20, "got {} sections", sections.len());
    assert_eq!(header.section_count as usize, sections.len());
    assert!(
        sections
            .windows(2)
            .all(|w| w[0].section_type < w[1].section_type),
        "the directory must be ascending and duplicate-free"
    );
    for component in [PermutationComponent::Pos, PermutationComponent::Ops] {
        for kind in CORE_KINDS {
            let want = component.section_type(kind);
            assert!(
                sections.iter().any(|s| s.section_type == want),
                "missing section {want:#06x}",
            );
        }
    }
    // SPO carries directories only: its bitmaps live in the HDT (§7.3).
    for &kind in &CORE_KINDS[4..] {
        let want = PermutationComponent::Spo.section_type(kind);
        let section = sections
            .iter()
            .find(|s| s.section_type == want)
            .unwrap_or_else(|| panic!("missing SPO directory section {want:#06x}"));
        assert!(
            section.indexed_bits > 0,
            "an SPO directory must record the length of the HDT bitmap it indexes"
        );
    }

    // The mapped-load guarantee (§2.1): every payload is 64-byte aligned and
    // lies wholly inside the file, so a reader may load a u64 anywhere in it.
    let file_len = std::fs::metadata(&perm).unwrap().len();
    assert_eq!(header.file_size, file_len);
    for section in sections {
        if section.length == 0 {
            continue;
        }
        assert_eq!(
            section.offset % 64,
            0,
            "section {:#06x} is not 64-byte aligned",
            section.section_type
        );
        assert!(
            section.offset + section.length <= file_len,
            "section {:#06x} runs past end of file",
            section.section_type
        );
        assert!(section.entry_count > 0);
    }
}

/// The HDT-side half of the same contract: a mapped reader's entire open path is
/// this scan, so every section it reports must be locatable and consistent with
/// what the sidecar's builder recorded about the same file (KGF doc 20 §20.4).
#[test]
fn the_hdt_scan_describes_every_section_well_enough_to_map_it() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    let file_len = std::fs::metadata(&hdt).unwrap().len();

    let mut reader = BufReader::new(File::open(&hdt).unwrap());
    let sections = scan_hdt_sections(&mut reader).expect("scan HDT sections");

    // The walk accounts for the whole file, and the sections it names follow one
    // another in order — with the dictionary's and triples' control info in the
    // gaps a scan does not describe.
    assert_eq!(sections.end(), file_len, "the scan must reach end of file");
    assert!(sections.header_offset < sections.data_offset);
    assert!(sections.data_offset < sections.shared.section_start);
    assert_eq!(sections.shared.section_end, sections.subjects.section_start);
    assert_eq!(
        sections.subjects.section_end,
        sections.predicates.section_start
    );
    assert_eq!(
        sections.predicates.section_end,
        sections.objects.section_start
    );
    assert!(sections.objects.section_end < sections.bitmap_y.section_start);
    assert_eq!(
        sections.bitmap_y.section_end,
        sections.bitmap_z.section_start
    );
    assert_eq!(
        sections.bitmap_z.section_end,
        sections.array_y.section_start
    );
    assert_eq!(sections.array_y.section_end, sections.array_z.section_start);

    // Every payload is inside the file and as long as its shape implies, which
    // is what a mapped reader validates its views against.
    for bitmap in [sections.bitmap_y, sections.bitmap_z] {
        assert!(bitmap.section_start < bitmap.data_start);
        assert_eq!(bitmap.data_length, bitmap.num_bits.div_ceil(8));
        assert!(bitmap.data_start + bitmap.data_length <= file_len);
    }
    for array in [sections.array_y, sections.array_z] {
        assert!(array.section_start < array.data_start);
        assert!(array.bits_per_entry > 0 && array.bits_per_entry <= 64);
        assert_eq!(
            array.data_length,
            packed_len(array.num_entries, array.bits_per_entry).unwrap()
        );
        assert!(array.data_start + array.data_length <= file_len);
    }
    for section in [
        sections.shared,
        sections.subjects,
        sections.predicates,
        sections.objects,
    ] {
        assert!(section.block_size > 0);
        assert_eq!(
            section.offsets.num_entries,
            section.string_count.div_ceil(section.block_size) + 1,
            "one block offset per block, plus the sentinel"
        );
        assert_eq!(section.offsets.section_end, section.buffer_start);
        assert!(section.buffer_start + section.buffer_length <= file_len);
    }

    // The counts a scan derives are the ones the permutation builder recorded
    // from its own read of this file — an independent check on both.
    let index = PermutationIndex::open(&permutation_index_path(&hdt), &hdt).expect("open sidecar");
    let header = index.header();
    assert_eq!(sections.num_triples(), header.triples);
    assert_eq!(sections.num_sp_pairs(), sections.bitmap_y.num_bits);
    assert_eq!(
        sections.shared.string_count + sections.subjects.string_count,
        header.subjects
    );
    assert_eq!(
        sections.shared.string_count + sections.objects.string_count,
        header.objects
    );
    assert_eq!(sections.predicates.string_count, header.predicates);

    // The sidecar's SPO directories index these bitmaps, so their recorded bit
    // lengths must be the ones the scan found: kinds 5–6 cover BitmapY, 7–8
    // BitmapZ (`docs/permutation-index-format.md` §7.3).
    for (kind, bits) in [
        (
            PermutationSectionKind::BitmapYSuperrank,
            sections.bitmap_y.num_bits,
        ),
        (
            PermutationSectionKind::BitmapYSubrank,
            sections.bitmap_y.num_bits,
        ),
        (
            PermutationSectionKind::BitmapZSuperrank,
            sections.bitmap_z.num_bits,
        ),
        (
            PermutationSectionKind::BitmapZSubrank,
            sections.bitmap_z.num_bits,
        ),
    ] {
        let want = PermutationComponent::Spo.section_type(kind);
        let section = index
            .sections()
            .iter()
            .find(|s| s.section_type == want)
            .unwrap_or_else(|| panic!("missing SPO directory section {want:#06x}"));
        assert_eq!(
            section.indexed_bits, bits,
            "SPO directory {want:#06x} indexes a different bitmap length than the scan found"
        );
    }

    // And `data_offset` is the byte the identity digest starts at: hashing from
    // there must reproduce the digest the sidecar bound itself with.
    let mut source = BufReader::new(File::open(&hdt).unwrap());
    source.seek(SeekFrom::Start(sections.data_offset)).unwrap();
    assert_eq!(
        sha256_to_end(&mut source).unwrap(),
        header.source_digest,
        "the scan's data_offset must be where identity digests begin"
    );
}
