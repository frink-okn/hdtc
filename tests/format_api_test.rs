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
    ELIAS_FANO_HEADER_SIZE, EliasFanoHeader, GRAPH_ARRAY_CONTAINER_MAX,
    GRAPH_BITMAP_CONTAINER_BYTES, GRAPH_CHUNK_ENTRY_SIZE, GRAPH_LAYER_ENTRY_SIZE,
    GRAPH_POSITION_CHUNK_SHIFT, GraphChunkContainer, GraphChunkEntry, GraphIndex,
    GraphIndexOpenError, GraphIndexSectionKind, GraphLayerEncoding, GraphLayerEntry,
    GraphSidecarReader, KeyRole, KeysetEncoding, KeysetHeader, KeysetOpenError, ParsedLiteral,
    PermutationComponent, PermutationIndex, PermutationIndexOpenError, PermutationSectionKind,
    PfcSectionHeader, PfcSectionIterator, SketchBody, SketchHeader, SketchKind, SketchOpenError,
    encode_literal, graph_index_path, graph_sidecar_path, keyset_path, packed_len, parse_literal,
    permutation_index_path, read_keyset_header, read_sketch_header, scan_hdt_sections,
    scan_pfc_section, sha256_to_end, sketch_path,
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
    build_fixture_from(temp, REPRESENTATIVE_NT)
}

fn build_fixture_from(temp: &Path, source: &str) -> PathBuf {
    let input = temp.join("input.nt");
    let hdt = temp.join("data.hdt");
    write_file(&input, source.as_bytes());

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

fn build_graph_fixture_from(temp: &Path, source: &str) -> PathBuf {
    let input = temp.join("input.nq");
    let hdt = temp.join("data.hdt");
    write_file(&input, source.as_bytes());

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            input.to_str().unwrap(),
            "-o",
            hdt.to_str().unwrap(),
            "--mode",
            "quads",
            "--graphs-index",
            "--temp-dir",
            temp.join("work").to_str().unwrap(),
            "--memory-limit",
            "64M",
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
fn dictionary_artifact_headers_are_available_through_the_public_facade() {
    fn assert_public_error<E: std::error::Error>() {}
    assert_public_error::<SketchOpenError>();
    assert_public_error::<KeysetOpenError>();

    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());
    let filters = temp.path().join("filters");
    let keysets = temp.path().join("keysets");

    let sketch = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .arg("sketch")
        .arg(&hdt)
        .args(["--k", "16", "--roles", "subjects,objects", "--output-dir"])
        .arg(&filters)
        .arg("--temp-dir")
        .arg(temp.path().join("sketch-work"))
        .output()
        .expect("run hdtc sketch");
    assert!(
        sketch.status.success(),
        "hdtc sketch failed:\n{}",
        String::from_utf8_lossy(&sketch.stderr)
    );

    let keyset = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .arg("keyset")
        .arg(&hdt)
        .args([
            "--roles",
            "subjects-only,objects-only,shared",
            "--output-dir",
        ])
        .arg(&keysets)
        .arg("--temp-dir")
        .arg(temp.path().join("keyset-work"))
        .output()
        .expect("run hdtc keyset");
    assert!(
        keyset.status.success(),
        "hdtc keyset failed:\n{}",
        String::from_utf8_lossy(&keyset.stderr)
    );

    let subjects_filter: SketchHeader = read_sketch_header(&sketch_path(
        &filters,
        SketchKind::Filter,
        KeyRole::Subjects,
    ))
    .expect("read subjects filter header");
    let objects_filter: SketchHeader =
        read_sketch_header(&sketch_path(&filters, SketchKind::Filter, KeyRole::Objects))
            .expect("read objects filter header");
    for (header, role) in [
        (&subjects_filter, KeyRole::Subjects),
        (&objects_filter, KeyRole::Objects),
    ] {
        assert_eq!(header.kind, SketchKind::Filter);
        assert_eq!(header.role, role);
        assert_eq!(header.format_version, 1);
        assert_eq!(header.convention_id, 1);
        assert_eq!(header.hash_id, 1);
        assert!(matches!(header.body, SketchBody::Filter { .. }));
    }
    for role in [KeyRole::Subjects, KeyRole::Objects] {
        let minhash = read_sketch_header(&sketch_path(&filters, SketchKind::MinHash, role))
            .expect("read MinHash header");
        assert_eq!(minhash.kind, SketchKind::MinHash);
        assert_eq!(minhash.role, role);
        assert_eq!(minhash.format_version, 1);
        assert_eq!(minhash.convention_id, 1);
        assert_eq!(minhash.hash_id, 1);
        assert!(matches!(minhash.body, SketchBody::MinHash { k: 16, .. }));
    }

    let subjects_only: KeysetHeader =
        read_keyset_header(&keyset_path(&keysets, KeyRole::SubjectsOnly)).unwrap();
    let objects_only = read_keyset_header(&keyset_path(&keysets, KeyRole::ObjectsOnly)).unwrap();
    let shared = read_keyset_header(&keyset_path(&keysets, KeyRole::Shared)).unwrap();
    for (header, role) in [
        (&subjects_only, KeyRole::SubjectsOnly),
        (&objects_only, KeyRole::ObjectsOnly),
        (&shared, KeyRole::Shared),
    ] {
        assert_eq!(header.role, role);
        assert_eq!(header.format_version, 1);
        assert_eq!(header.convention_id, 1);
        assert_eq!(header.hash_id, 1);
        assert_eq!(header.encoding, KeysetEncoding::EliasFano);
        assert_eq!(header.source_digest, subjects_filter.source_digest);
    }

    assert_eq!(
        shared.key_count + subjects_only.key_count,
        subjects_filter.key_count
    );
    assert_eq!(
        shared.key_count + objects_only.key_count,
        objects_filter.key_count
    );
}

#[test]
fn open_distinguishes_a_foreign_hdt_from_a_malformed_sidecar() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let first_hdt = build_fixture(first.path());
    let second_hdt = build_fixture_from(
        second.path(),
        "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
    );

    let error = PermutationIndex::open(&permutation_index_path(&first_hdt), &second_hdt)
        .expect_err("a sidecar from another HDT must not bind");
    assert!(matches!(error, PermutationIndexOpenError::Binding { .. }));

    let sidecar = permutation_index_path(&first_hdt);
    let bytes = std::fs::read(&sidecar).unwrap();
    let truncated = first.path().join("truncated.hdt.perm");
    std::fs::write(&truncated, &bytes[..300]).unwrap();
    let error = PermutationIndex::open(&truncated, &first_hdt)
        .expect_err("a truncated sidecar must be classified separately");
    assert!(matches!(error, PermutationIndexOpenError::Sidecar { .. }));
}

#[test]
fn graph_open_distinguishes_foreign_parents_from_a_malformed_index() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let first_hdt = build_graph_fixture_from(first.path(), "<urn:s> <urn:p> <urn:o> <urn:g> .\n");
    let second_hdt = build_graph_fixture_from(
        second.path(),
        concat!(
            "<urn:s> <urn:p> <urn:o> <urn:g> .\n",
            "<urn:extra> <urn:p> <urn:o> <urn:other> .\n",
        ),
    );

    let error = match GraphIndex::open(&graph_index_path(&first_hdt), &second_hdt) {
        Err(error) => error,
        Ok(_) => panic!("an index from another HDT must not bind"),
    };
    assert!(matches!(error, GraphIndexOpenError::Binding { .. }));

    let index = graph_index_path(&first_hdt);
    let bytes = std::fs::read(&index).unwrap();
    let truncated = first.path().join("truncated.hdt.graphs.idx");
    std::fs::write(&truncated, &bytes[..300]).unwrap();
    let error = match GraphIndex::open(&truncated, &first_hdt) {
        Err(error) => error,
        Ok(_) => panic!("a truncated graph index must be classified separately"),
    };
    assert!(matches!(error, GraphIndexOpenError::Index { .. }));
}

/// The graphs half of the same contract. A mapped reader locates the sidecar's
/// dictionary and layer directory from the header, the index's layer sets and
/// transpose from the section directory, and decodes each layer's records
/// lazily from the bytes those offsets name — so every offset must be
/// resolvable against the file, and every record decodable, for every
/// encoding a layer can have.
#[test]
fn the_graph_artifacts_describe_every_region_well_enough_to_map_them() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_graph_fixture_from(temp.path(), &synthetic_quads());
    let sidecar_path = graph_sidecar_path(&hdt);
    let index_path = graph_index_path(&hdt);

    let sidecar = GraphSidecarReader::open(&sidecar_path, &hdt).expect("open sidecar");
    let header = *sidecar.header();
    let sidecar_bytes = std::fs::read(&sidecar_path).unwrap();
    assert_eq!(header.sidecar_size, sidecar_bytes.len() as u64);
    assert_eq!(header.triple_count, sidecar.triple_count());
    assert_eq!(header.named_graph_count, 3);
    assert!(header.membership_count > header.triple_count);
    assert!(sidecar.hdt_data_offset() > 0);

    // The graph dictionary is one standard PFC section, so the ordinary
    // scanner locates it and its count is the named-graph count.
    let mut cursor = std::io::Cursor::new(&sidecar_bytes[..]);
    cursor
        .seek(SeekFrom::Start(header.dictionary_offset))
        .unwrap();
    let dictionary = scan_pfc_section(&mut cursor, "graph dictionary").expect("scan dictionary");
    assert_eq!(dictionary.string_count, header.named_graph_count);
    assert_eq!(dictionary.section_start, header.dictionary_offset);
    assert!(dictionary.section_end <= header.directory_offset);

    let mut encodings = Vec::new();
    let sidecar_layers = check_layer_set(
        &sidecar_bytes,
        header.directory_offset,
        header.named_graph_count,
        header.triple_count,
        &mut encodings,
    );
    assert_eq!(
        sidecar_layers
            .iter()
            .map(|entry| entry.member_count)
            .sum::<u64>(),
        header.membership_count,
        "layer counts sum to M"
    );
    assert!(
        sidecar_layers
            .iter()
            .filter(|entry| entry.member_count > 0)
            .all(|entry| {
                entry.primary_offset >= header.layers_offset
                    && entry.primary_offset < header.layers_offset + header.layers_length
            }),
        "every non-empty layer lives inside the layers span"
    );

    // Both re-keyed layer sets, framed exactly like the sidecar's, and no
    // transpose unless the header says so.
    let index = GraphIndex::directory(&index_path, &hdt).expect("open index directory");
    let index_bytes = std::fs::read(&index_path).unwrap();
    assert_eq!(index.path(), index_path.as_path());
    assert_eq!(index.header().file_size, index_bytes.len() as u64);
    assert_eq!(index.header().triples, header.triple_count);
    assert_eq!(index.header().named_graphs, header.named_graph_count);
    assert_eq!(index.header().memberships, header.membership_count);
    assert_eq!(index.header().source_digest, header.source_digest);
    assert!(index.header().has_pos_layers() && index.header().has_ops_layers());
    assert_eq!(
        index.header().section_count as usize,
        index.sections().len()
    );
    assert!(
        index
            .sections()
            .windows(2)
            .all(|w| w[0].section_type < w[1].section_type)
    );
    for section in index.sections() {
        assert!(section.kind().is_some(), "{:#06x}", section.section_type);
        if section.length > 0 {
            assert_eq!(section.offset % 64, 0);
            assert!(section.offset + section.length <= index_bytes.len() as u64);
        }
    }
    for (directory, region) in [
        (
            GraphIndexSectionKind::PosLayerDirectory,
            GraphIndexSectionKind::PosLayerRegion,
        ),
        (
            GraphIndexSectionKind::OpsLayerDirectory,
            GraphIndexSectionKind::OpsLayerRegion,
        ),
    ] {
        let directory = index.section(directory).expect("layer directory");
        assert_eq!(directory.entry_count, header.named_graph_count + 1);
        let region = index.section(region).expect("layer region");
        let layers = check_layer_set(
            &index_bytes,
            directory.offset,
            header.named_graph_count,
            header.triple_count,
            &mut encodings,
        );
        for (graph, layer) in layers.iter().enumerate() {
            assert_eq!(
                layer.member_count, sidecar_layers[graph].member_count,
                "a layer set holds the sidecar's memberships in another space"
            );
            if layer.member_count > 0 {
                assert!(
                    layer.primary_offset >= region.offset
                        && layer.primary_offset < region.offset + region.length
                );
            }
        }
    }
    let transposed = index.header().has_membership_ranks();
    assert_eq!(
        index
            .section(GraphIndexSectionKind::TransposeBitmap)
            .is_some(),
        transposed
    );
    assert_eq!(
        index
            .section(GraphIndexSectionKind::TransposeArray)
            .is_some(),
        index.header().has_membership_ids()
    );

    encodings.sort();
    encodings.dedup();
    assert_eq!(
        encodings,
        vec![
            GraphLayerEncoding::DenseChunks,
            GraphLayerEncoding::SparseChunks,
            GraphLayerEncoding::EliasFano,
        ],
        "the synthetic fixture must reach every encoding a reader has to decode"
    );
}

/// Decode a layer directory from the bytes at `directory_offset` and walk
/// every record each layer names, so a wrong offset or an undecodable record
/// fails here rather than in a downstream reader.
fn check_layer_set(
    bytes: &[u8],
    directory_offset: u64,
    named_graphs: u64,
    triples: u64,
    encodings: &mut Vec<GraphLayerEncoding>,
) -> Vec<GraphLayerEntry> {
    let mut layers = Vec::new();
    for graph in 0..=named_graphs {
        let start = (directory_offset + graph * GRAPH_LAYER_ENTRY_SIZE as u64) as usize;
        let entry: &[u8; GRAPH_LAYER_ENTRY_SIZE] = bytes[start..start + GRAPH_LAYER_ENTRY_SIZE]
            .try_into()
            .unwrap();
        let layer = GraphLayerEntry::parse(entry);
        let encoding = layer.layer_encoding().expect("a version-1 encoding");
        if layer.member_count == 0 {
            assert_eq!(layer.minimum_position, triples);
            assert_eq!(layer.maximum_position_exclusive, 0);
            layers.push(layer);
            continue;
        }
        encodings.push(encoding);
        assert!(layer.minimum_position < layer.maximum_position_exclusive);
        assert!(layer.maximum_position_exclusive <= triples);
        assert_eq!(layer.primary_offset % 64, 0);
        let primary_end = layer.primary_offset + layer.primary_length;
        assert!(primary_end <= bytes.len() as u64);
        match encoding {
            GraphLayerEncoding::DenseChunks | GraphLayerEncoding::SparseChunks => {
                assert_eq!(
                    layer.primary_length,
                    layer.item_count_a * GRAPH_CHUNK_ENTRY_SIZE as u64
                );
                let mut rank = 0;
                for index in 0..layer.item_count_a {
                    let start =
                        (layer.primary_offset + index * GRAPH_CHUNK_ENTRY_SIZE as u64) as usize;
                    let chunk: &[u8; GRAPH_CHUNK_ENTRY_SIZE] = bytes
                        [start..start + GRAPH_CHUNK_ENTRY_SIZE]
                        .try_into()
                        .unwrap();
                    let chunk = GraphChunkEntry::parse(chunk);
                    assert_eq!(chunk.rank_before, rank);
                    assert!(chunk.key < triples.div_ceil(1 << GRAPH_POSITION_CHUNK_SHIFT));
                    match chunk.container().expect("a version-1 container") {
                        GraphChunkContainer::Empty => assert_eq!(chunk.cardinality, 0),
                        GraphChunkContainer::Array => {
                            assert!(chunk.cardinality <= GRAPH_ARRAY_CONTAINER_MAX);
                            assert_eq!(chunk.payload_length, chunk.cardinality * 2);
                        }
                        GraphChunkContainer::Bitmap => {
                            assert!(chunk.cardinality > GRAPH_ARRAY_CONTAINER_MAX);
                            assert_eq!(chunk.payload_length, GRAPH_BITMAP_CONTAINER_BYTES);
                        }
                    }
                    if chunk.cardinality > 0 {
                        assert_eq!(chunk.payload_offset % 8, 0);
                        assert!(
                            chunk.payload_offset + u64::from(chunk.payload_length)
                                <= bytes.len() as u64
                        );
                    }
                    rank += u64::from(chunk.cardinality);
                }
                assert_eq!(rank, layer.member_count);
                if encoding == GraphLayerEncoding::SparseChunks {
                    assert_eq!(layer.secondary_length, layer.parameter * 8);
                    assert!(layer.secondary_offset + layer.secondary_length <= bytes.len() as u64);
                }
            }
            GraphLayerEncoding::EliasFano => {
                assert_eq!(layer.primary_length, ELIAS_FANO_HEADER_SIZE as u64);
                let start = layer.primary_offset as usize;
                let raw: &[u8; ELIAS_FANO_HEADER_SIZE] = bytes
                    [start..start + ELIAS_FANO_HEADER_SIZE]
                    .try_into()
                    .unwrap();
                let ef = EliasFanoHeader::parse(raw).expect("a valid Elias-Fano header");
                assert_eq!(ef.universe, triples);
                assert_eq!(ef.members, layer.member_count);
                assert_eq!(ef.upper_bits, ef.high_buckets + ef.members);
                assert_eq!(ef.superrank_count, ef.upper_bits.div_ceil(4096) + 1);
                assert_eq!(ef.subrank_count, ef.upper_bits.div_ceil(512));
                for (offset, length) in [
                    (ef.lower_offset, ef.lower_length),
                    (ef.superrank_offset, ef.superrank_length),
                    (ef.subrank_offset, ef.subrank_length),
                    (ef.upper_offset, ef.upper_length),
                ] {
                    if length > 0 {
                        assert_eq!(offset % 8, 0);
                        assert!(offset + length <= bytes.len() as u64);
                    }
                }
                let mut corrupt = *raw;
                corrupt[0] ^= 1;
                assert!(EliasFanoHeader::parse(&corrupt).is_err());
            }
        }
        layers.push(layer);
    }
    layers
}

/// Three graphs over one universe, at densities that make the writer pick a
/// different encoding for each: everything (dense chunks with bitmap
/// containers), every three-hundredth triple (Elias–Fano), and a short run in
/// the middle (sparse chunks with an array container). The universe spans
/// three position chunks; with one chunk a dense directory is always the
/// cheapest structure and nothing else is ever chosen.
fn synthetic_quads() -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for i in 0..140_000u32 {
        let subject = format!("<urn:s{}>", i / 4);
        let predicate = format!("<urn:p{}>", i % 4);
        let object = format!("<urn:o{i}>");
        writeln!(out, "{subject} {predicate} {object} <urn:g:all> .").unwrap();
        if i % 300 == 0 {
            writeln!(out, "{subject} {predicate} {object} <urn:g:sparse> .").unwrap();
        }
        if (70_000..70_040).contains(&i) {
            writeln!(out, "{subject} {predicate} {object} <urn:g:run> .").unwrap();
        }
    }
    out
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

/// The dictionary's spelling of a term, in both published directions.
///
/// A downstream reader resolving a request term to an id has to write the term
/// the way the builder wrote it, then read back what it finds. So the contract
/// is not just that [`parse_literal`] and [`encode_literal`] invert each other
/// — it is that both agree with the bytes actually in a built dictionary. A
/// divergence here returns *fewer rows* rather than an error: the lookup misses
/// a term that is present, and nothing downstream can tell.
#[test]
fn both_directions_of_a_dictionary_term_agree_with_the_bytes_hdtc_wrote() {
    let temp = tempfile::tempdir().unwrap();
    let hdt = build_fixture(temp.path());

    let mut reader = BufReader::new(File::open(&hdt).unwrap());
    let sections = scan_hdt_sections(&mut reader).expect("scan HDT sections");

    reader
        .seek(SeekFrom::Start(sections.objects.section_start))
        .unwrap();
    let header = PfcSectionHeader::read_from(&mut reader, "objects").expect("objects preamble");
    let terms: Vec<Vec<u8>> = PfcSectionIterator::new(&mut reader, header, "objects")
        .collect::<Result<_, _>>()
        .expect("decode objects section");

    let (mut plain, mut tagged, mut typed, mut iris) = (0, 0, 0, 0);
    for term in &terms {
        let parsed: ParsedLiteral<'_> = match parse_literal(term) {
            Some(parsed) => parsed,
            None => {
                iris += 1;
                continue;
            }
        };
        let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).expect("UTF-8 term part");
        let language = parsed.language.map(&text);
        let datatype = parsed.datatype.map(&text);
        match (&language, &datatype) {
            (Some(_), _) => tagged += 1,
            (None, Some(_)) => typed += 1,
            (None, None) => plain += 1,
        }

        let rewritten = encode_literal(
            &text(parsed.value),
            language.as_deref(),
            datatype.as_deref(),
        );
        assert_eq!(
            rewritten.as_bytes(),
            term.as_slice(),
            "re-encoding {} does not reproduce the stored term",
            String::from_utf8_lossy(term)
        );
    }

    // Not vacuous: the fixture must have exercised every literal shape, or a
    // rewrite rule could be wrong for a shape no term happened to have.
    assert!(
        plain > 0 && tagged > 0 && typed > 0 && iris > 0,
        "the fixture must cover plain, tagged and typed literals and a non-literal, saw {plain}/{tagged}/{typed}/{iris}"
    );
}

/// The text-index surface a downstream server needs, exercised the same way.
///
/// KGF's `o.text` resolves hits through the OPS permutation, so what it needs
/// from here is narrow and load-bearing: query and rank, the manifest the
/// service descriptor republishes, and a binding check — because an index built
/// from a different HDT returns object IDs that resolve to real terms in the
/// wrong dictionary, which no checksum on the query path would catch.
#[test]
fn the_text_surface_queries_describes_and_binds() {
    use hdtc::format::{
        TextMatchPage, TextQuery, TextScanPosition, TextSearch, TextSearcher,
        default_text_index_path, verify_text_index_binding,
    };

    let temp = tempfile::tempdir().expect("temp dir");
    let hdt = build_fixture(temp.path());
    let index = default_text_index_path(&hdt);
    assert!(
        index
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with(".text"),
        "the default path is what a bundle is laid out around"
    );

    let built = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(["text", hdt.to_str().unwrap()])
        .output()
        .expect("run hdtc text");
    assert!(
        built.status.success(),
        "hdtc text failed: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    assert!(index.is_dir(), "a text index is a directory, not a file");

    let searcher = TextSearcher::open(&index).expect("open the index");

    // The manifest is what a consumer republishes rather than restating: how
    // much was indexed, and what was left out.
    let manifest = searcher.manifest();
    assert!(manifest.indexed_docs > 0, "the fixture has literals");
    assert!(manifest.literals_scanned >= manifest.indexed_docs);

    // A hit is an object dictionary ID and nothing else, which is what makes
    // it resolvable through a permutation the caller already has.
    let hits = searcher
        .search(
            &TextQuery {
                text: searcher
                    .analyze("Alice")
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "alice".to_owned()),
                ..TextQuery::default()
            },
            10,
        )
        .expect("search");
    assert!(!hits.is_empty(), "the fixture holds a literal to find");
    assert!(hits.iter().all(|hit| hit.object_id > 0));

    let query = TextQuery {
        text: "alice".to_owned(),
        ..TextQuery::default()
    };
    let total = searcher.count(&query).expect("count") as u64;
    assert!(total > 1, "the fixture holds several literals naming Alice");

    // A server can bound scoring itself rather than only the retained heap.
    let ranked: TextSearch = searcher
        .search_up_to(&query, 10, 1)
        .expect("bounded search");
    assert_eq!(ranked.examined, 1);
    assert!(!ranked.complete);
    assert_eq!(ranked.hits.len(), 1);

    // And it can scan the same matching object set in bounded resumable pages
    // for an exact count after applying its own statement-pattern constraints.
    let mut position: Option<TextScanPosition> = None;
    let mut scanned = Vec::new();
    loop {
        let page: TextMatchPage = searcher
            .scan_matching_objects(&query, position, 1)
            .expect("scan matching objects");
        scanned.extend(page.object_ids);
        if page.complete {
            break;
        }
        let next = page.next.expect("an incomplete scan has a position");
        position = Some(TextScanPosition::decode(next.encode()));
    }
    assert_eq!(scanned.len() as u64, total);

    // A bounded count agrees with the unbounded one below the bound, and says
    // so when it stops early — which is what lets a caller with a published
    // work budget spend it rather than learn the cost afterwards.
    assert_eq!(
        searcher
            .count_up_to(&query, total + 1)
            .expect("count_up_to"),
        (total, false),
        "a bound above the total is exact"
    );
    assert_eq!(
        searcher.count_up_to(&query, total).expect("count_up_to"),
        (total, true),
        "a bound at the total stops on the last one"
    );
    assert_eq!(
        searcher.count_up_to(&query, 1).expect("count_up_to"),
        (1, true)
    );
    assert_eq!(
        searcher.count_up_to(&query, 0).expect("count_up_to"),
        (0, true)
    );

    // And the binding: this index against its own HDT, and against another.
    verify_text_index_binding(&index, &hdt).expect("an index binds to its own source");

    let other_temp = tempfile::tempdir().expect("temp dir");
    let other = build_fixture_from(
        other_temp.path(),
        "<http://example.org/x> <http://example.org/p> \"a different graph\" .\n",
    );
    let error = verify_text_index_binding(&index, &other)
        .expect_err("an index must not bind to an HDT it was not built from");
    assert!(
        error.to_string().contains("binding mismatch"),
        "unexpected error: {error}"
    );
}
