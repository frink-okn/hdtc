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
use hdtc::format::{PermutationIndex, permutation_index_path};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Section types are `(component << 8) | kind`; see `docs/permutation-index-format.md` §5.
const COMPONENT_POS: u32 = 0x01;
const COMPONENT_OPS: u32 = 0x02;
const COMPONENT_SPO: u32 = 0x03;

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
    for component in [COMPONENT_POS, COMPONENT_OPS] {
        for kind in 1..=8u32 {
            assert!(
                sections
                    .iter()
                    .any(|s| s.section_type == (component << 8) | kind),
                "missing section {:#06x}",
                (component << 8) | kind
            );
        }
    }
    // SPO carries directories only: its bitmaps live in the HDT (§7.3).
    for kind in 5..=8u32 {
        let want = (COMPONENT_SPO << 8) | kind;
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
