//! Strict validation of the triple-term artifacts.
//!
//! What a reader trusts without checking, this checks:
//!
//! - the index is bound to exactly these HDTs (full digests) and every payload
//!   matches its checksum;
//! - each run covers exactly the triple-term IRIs of its dictionary;
//! - every IRI names the proposition it maps to: the proposition is decoded,
//!   nested terms included, and minted again ([`super::iri::iri_for`]);
//! - every asserted proposition is a triple of `data.hdt`, and every
//!   unasserted one a triple of the propositions HDT;
//! - every set layer bit marks the triple that is the target of the IRI its
//!   reverse entry names — so, by the run check, the triple that IRI names —
//!   and each layer has exactly one bit per IRI it should cover.

use super::format::{self, Section};
use super::iri::{Component, Proposition, TT_NAMESPACE, iri_for};
use super::reader::{Layer, LayerCursor, Order, Source, Target, TripleTermIndex};
use crate::hdt::artifacts::SourceIdentity;
use crate::hdt::reader::{BitmapTriplesScanner, DictionaryResolver, open_hdt};
use crate::io::crc_utils::CRC32C_ALGO;
use crate::permutation::{PermutationComponent, PermutationIndex, scan_hdt};
use anyhow::{Context, Result, bail, ensure};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;

fn component_of(
    index: &TripleTermIndex,
    source: Source,
    dictionaries: &mut Dictionaries,
    object: u64,
    depth: usize,
) -> Result<Component> {
    ensure!(
        depth < super::iri::MAX_NESTING,
        "triple terms nested too deeply"
    );
    if let Some(nested) = index.target(source, object)? {
        return Ok(Component::Triple(Box::new(proposition_of(
            index,
            dictionaries,
            nested,
            depth + 1,
        )?)));
    }
    let mut buf = Vec::new();
    dictionaries
        .get(source)?
        .object_term_raw(object, &mut buf)?;
    Ok(Component::from_dictionary(String::from_utf8(buf)?))
}

struct Dictionaries {
    data: DictionaryResolver,
    props: Option<DictionaryResolver>,
}

impl Dictionaries {
    fn get(&mut self, source: Source) -> Result<&mut DictionaryResolver> {
        match source {
            Source::Data => Ok(&mut self.data),
            Source::Propositions => self
                .props
                .as_mut()
                .context("unasserted triple term without a propositions HDT"),
        }
    }
}

fn proposition_of(
    index: &TripleTermIndex,
    dictionaries: &mut Dictionaries,
    target: Target,
    depth: usize,
) -> Result<Proposition> {
    let source = target.source();
    let mut buf = Vec::new();
    let dictionary = dictionaries.get(source)?;
    dictionary.subject_term_raw(target.subject, &mut buf)?;
    let subject = Component::from_dictionary(String::from_utf8(buf.clone())?);
    dictionary.predicate_term(target.predicate, &mut buf)?;
    let predicate = String::from_utf8(buf)?;
    let object = component_of(index, source, dictionaries, target.object, depth)?;
    Ok(Proposition {
        subject,
        predicate,
        object,
    })
}

fn check_payloads(path: &Path, sections: &[Section]) -> Result<()> {
    let file = File::open(path)?;
    for section in sections {
        if section.length == 0 {
            continue;
        }
        let mut reader = BufReader::with_capacity(256 * 1024, &file);
        reader.seek(SeekFrom::Start(section.offset))?;
        let mut digest = CRC32C_ALGO.digest();
        let mut remaining = section.length;
        let mut buffer = vec![0u8; 64 * 1024];
        while remaining > 0 {
            let take = remaining.min(buffer.len() as u64) as usize;
            reader.read_exact(&mut buffer[..take])?;
            digest.update(&buffer[..take]);
            remaining -= take as u64;
        }
        ensure!(
            digest.finalize() == section.payload_crc,
            "section {:#06x} fails its checksum",
            section.section_type
        );
        // Every payload is bits — a bitmap, or packed entries — and the bits
        // past its last entry in its last byte are zero
        // (permutation-index-format.md §2), so equal contents are equal bytes.
        let used = section.entry_count * u64::from(section.bits_per_entry);
        let remainder = (used % 8) as u8;
        if remainder != 0 {
            let mut last = [0u8; 1];
            file.read_exact_at(&mut last, section.offset + section.length - 1)?;
            ensure!(
                last[0] >> remainder == 0,
                "section {:#06x} has nonzero unused high bits",
                section.section_type
            );
        }
    }
    Ok(())
}

type TripleStream<'a> = Box<dyn Iterator<Item = Result<(u64, u64, u64)>> + 'a>;

/// The triples of one HDT in one order, with their positions.
fn ordered_triples<'a>(hdt: &'a Path, perm: &'a Path, order: Order) -> Result<TripleStream<'a>> {
    match order {
        Order::Spo => {
            let metadata = scan_hdt(hdt)?;
            let mut scanner = BitmapTriplesScanner::new(&metadata.offsets, hdt)?;
            Ok(Box::new(std::iter::from_fn(move || {
                scanner.next_triple().transpose()
            })))
        }
        Order::Pos | Order::Ops => {
            let index = PermutationIndex::open(perm, hdt)?;
            let component = if order == Order::Pos {
                PermutationComponent::Pos
            } else {
                PermutationComponent::Ops
            };
            let iter = index.all_triples(component)?;
            Ok(Box::new(iter))
        }
    }
}

/// Check one layer against its HDT, read in the layer's order: each mark is
/// on a triple whose reverse entry names it.
///
/// The run check has already minted every IRI from its target's proposition,
/// so a reverse entry names a position's triple exactly when the entry's
/// target is that triple — a packed read, with no dictionary lookup and no
/// hashing per position. The marks are read through the layer's own cursor,
/// in either encoding (§8.1), so a sparse layer's positions are checked as
/// decoded: strictly increasing, each on a triple of the HDT.
fn check_layer(
    index: &TripleTermIndex,
    source: Source,
    order: Order,
    hdt: &Path,
    expected: u64,
) -> Result<()> {
    let layer: Layer = index.layer(source, order)?;
    let triples_total = match source {
        Source::Data => index.header().data.triples,
        Source::Propositions => index.header().props.triples,
    };
    ensure!(
        layer.len() == triples_total,
        "{source:?} {order:?} layer covers {} positions, the HDT has {triples_total}",
        layer.len()
    );
    let population = layer.population(index.file())?;
    ensure!(
        population == expected && layer.marked() == expected,
        "{source:?} {order:?} layer marks {population} triples, expected {expected}"
    );
    let perm = crate::permutation::canonical_path(hdt);
    let mut marks = LayerCursor::from_layer(index.path(), &layer, 0..layer.len())?;
    let mut next = marks.next()?;
    let mut rank = 0u64;
    for (position, triple) in ordered_triples(hdt, &perm, order)?.enumerate() {
        let Some((marked, data_term)) = next else {
            break;
        };
        let position = position as u64;
        ensure!(
            marked >= position,
            "{source:?} {order:?} layer's marks are out of order at rank {rank}"
        );
        if marked > position {
            continue;
        }
        let (s, p, o) = triple?;
        let target = index.target(Source::Data, data_term)?.with_context(|| {
            format!("{source:?} {order:?} reverse entry {rank} is no triple-term IRI")
        })?;
        ensure!(
            (target.subject, target.predicate, target.object) == (s, p, o)
                && target.asserted == (source == Source::Data),
            "{source:?} {order:?} layer position {position} holds ({s}, {p}, {o}), \
             but its reverse entry {data_term} names another proposition"
        );
        rank += 1;
        next = marks.next()?;
    }
    ensure!(
        next.is_none() && rank == expected,
        "{source:?} {order:?} layer marks positions past its HDT's triples"
    );
    Ok(())
}

/// Recompute every layer's rank directory and compare it with the stored one.
fn check_rank_directories(index: &TripleTermIndex, temp_dir: &Path) -> Result<()> {
    use crate::permutation::{RawBitmap, build_rank_directories};
    let file = File::open(index.path())?;
    let mut sources = vec![Source::Data];
    if index.has_propositions() {
        sources.push(Source::Propositions);
    }
    for source in sources {
        for order in [Order::Spo, Order::Pos, Order::Ops] {
            let layer = index.layer(source, order)?;
            if layer.len() == 0 {
                continue;
            }
            let (superrank, subrank) = build_rank_directories(
                &RawBitmap {
                    path: index.path().to_path_buf(),
                    data_offset: layer.bitmap.offset,
                    // The bitmap the directory indexes: the layer, or a
                    // sparse layer's high bits.
                    bits: layer.bitmap.entry_count,
                },
                temp_dir,
            )?;
            for (recomputed, stored) in [(superrank, &layer.superrank), (subrank, &layer.subrank)] {
                let expected = std::fs::read(&recomputed)?;
                let mut actual = vec![0u8; stored.length as usize];
                file.read_exact_at(&mut actual, stored.offset)?;
                ensure!(
                    expected == actual,
                    "{source:?} {order:?} layer's rank directory disagrees with its bitmap"
                );
            }
        }
    }
    Ok(())
}

/// Every target is a triple of the HDT its flag names; the propositions HDT
/// holds exactly the unasserted targets; and none of those is in `data.hdt`.
fn check_targets(
    index: &TripleTermIndex,
    hdt_path: &Path,
    props_path: &Path,
    temp_dir: &Path,
    memory_limit: usize,
) -> Result<()> {
    use super::build::{Row, Sorted, place};
    let mut asserted = Sorted::<Row<3>>::new(temp_dir, memory_limit / 4);
    let mut unasserted = Sorted::<Row<3>>::new(temp_dir, memory_limit / 4);
    for source in [Source::Data, Source::Propositions] {
        for object in index.run(source) {
            let target = index.target(source, object)?.expect("in run");
            let row = Row([target.subject, target.predicate, target.object]);
            if target.asserted {
                asserted.push(row)?;
            } else {
                unasserted.push(row)?;
            }
        }
    }
    // Merge-join each set (deduplicated by the sort) with its HDT's SPO stream.
    let join = |rows: Sorted<Row<3>>, hdt: &Path| -> Result<u64> {
        let (_sorter, rows) = rows.finish()?;
        let mut count = 0u64;
        let mut rows = rows.peekable();
        if rows.peek().is_none() {
            return Ok(0);
        }
        let metadata = scan_hdt(hdt)?;
        let mut scanner = BitmapTriplesScanner::new(&metadata.offsets, hdt)?;
        let mut current = scanner.next_triple()?;
        for row in rows {
            let [s, p, o] = row?.0;
            while current.is_some_and(|triple| triple < (s, p, o)) {
                current = scanner.next_triple()?;
            }
            ensure!(
                current == Some((s, p, o)),
                "a triple term's proposition ({s}, {p}, {o}) is not a triple of {}",
                hdt.display()
            );
            count += 1;
        }
        Ok(count)
    };
    join(asserted, hdt_path)?;
    if index.has_propositions() {
        let stored = join(unasserted, props_path)?;
        ensure!(
            stored == index.header().props.triples,
            "the propositions HDT holds {} triples but {stored} are unasserted propositions",
            index.header().props.triples
        );
        // None of them is asserted: read them back as records and look for
        // them in data.hdt the way the build does.
        let spool_path = temp_dir.join("tt-validate-props.zst");
        let mut out = super::spool::SpoolWriter::create(spool_path)?;
        let (_, mut props) = open_hdt(props_path, memory_limit / 4)?;
        let metadata = scan_hdt(props_path)?;
        let mut scanner = BitmapTriplesScanner::new(&metadata.offsets, props_path)?;
        let mut buf = Vec::new();
        while let Some((s, p, o)) = scanner.next_triple()? {
            props.subject_term_raw(s, &mut buf)?;
            let subject = String::from_utf8(buf.clone())?;
            props.predicate_term(p, &mut buf)?;
            let predicate = String::from_utf8(buf.clone())?;
            props.object_term_raw(o, &mut buf)?;
            let object = String::from_utf8(buf.clone())?;
            out.push(&super::PropositionRecord {
                iri: String::new(),
                subject,
                predicate,
                object,
            })?;
        }
        let spool = out.finish()?;
        let (found, _) = place(hdt_path, &spool, None, temp_dir, memory_limit / 2, false)?;
        spool.remove();
        let (_sorter, mut found) = found.finish()?;
        ensure!(
            found.next().is_none(),
            "a triple of the propositions HDT is asserted in {}",
            hdt_path.display()
        );
    } else {
        let (_sorter, mut rows) = unasserted.finish()?;
        ensure!(
            rows.next().is_none(),
            "unasserted triple terms without a propositions HDT"
        );
    }
    Ok(())
}

/// Validate the triple-term index and propositions HDT beside `hdt_path`
/// strictly (`docs/triple-terms-format.md` §12): both digests, every payload,
/// every run entry minted again, every layer bit, every target. Sorts spill
/// into `temp_dir`, which is created if missing; `memory_limit` bounds them.
pub fn validate_triple_terms(hdt_path: &Path, temp_dir: &Path, memory_limit: usize) -> Result<()> {
    std::fs::create_dir_all(temp_dir)
        .with_context(|| format!("Failed to create {}", temp_dir.display()))?;
    let index = TripleTermIndex::open(hdt_path)?;
    let header = index.header().clone();
    let props_path = format::propositions_path(hdt_path);

    ensure!(
        SourceIdentity::capture(hdt_path)?.digest() == &header.data.digest,
        "triple-term index digest does not match {}",
        hdt_path.display()
    );
    if index.has_propositions() {
        ensure!(
            SourceIdentity::capture(&props_path)?.digest() == &header.props.digest,
            "triple-term index digest does not match {}",
            props_path.display()
        );
        crate::permutation::validate_permutation_index(
            &crate::permutation::canonical_path(&props_path),
            &props_path,
            temp_dir,
            memory_limit,
        )?;
    } else {
        ensure!(
            !props_path.exists(),
            "{} exists but the index does not cover it",
            props_path.display()
        );
    }
    check_payloads(index.path(), index.sections())?;
    check_rank_directories(&index, temp_dir)?;
    check_targets(&index, hdt_path, &props_path, temp_dir, memory_limit)?;

    let mut dictionaries = Dictionaries {
        data: open_hdt(hdt_path, memory_limit / 2)?.1,
        props: if index.has_propositions() {
            Some(open_hdt(&props_path, memory_limit / 4)?.1)
        } else {
            None
        },
    };

    // Runs cover exactly the dictionaries' triple-term IRIs.
    for source in [Source::Data, Source::Propositions] {
        let run = index.run(source);
        let Ok(dictionary) = dictionaries.get(source) else {
            ensure!(
                run.is_empty(),
                "propositions run without a propositions HDT"
            );
            continue;
        };
        for (section, role) in [
            (&mut dictionary.shared, "subject"),
            (&mut dictionary.subjects, "subject"),
            (&mut dictionary.predicates, "predicate"),
        ] {
            ensure!(
                section.prefix_range(TT_NAMESPACE.as_bytes())?.is_empty(),
                "a triple-term IRI is a {role}"
            );
        }
        let local = dictionary.objects.prefix_range(TT_NAMESPACE.as_bytes())?;
        let offset = dictionary.shared.string_count;
        let expected = if local.is_empty() {
            0..0
        } else {
            offset + local.start..offset + local.end
        };
        ensure!(
            run == expected,
            "{source:?} run {run:?} does not match the dictionary's triple-term IRIs {expected:?}"
        );
    }

    // Every IRI names its proposition, and the propositions are where the
    // targets say. Count how many IRIs of data.hdt each layer must mark.
    let mut data_layer_expected = 0u64;
    let mut props_layer_expected = 0u64;
    let mut buf = Vec::new();
    for source in [Source::Data, Source::Propositions] {
        for object in index.run(source) {
            let target = index.target(source, object)?.expect("in run");
            let proposition = proposition_of(&index, &mut dictionaries, target, 0)?;
            let iri = iri_for(&proposition);
            dictionaries
                .get(source)?
                .object_term_raw(object, &mut buf)?;
            ensure!(
                buf == iri.as_bytes(),
                "triple-term IRI {} decodes to a term named {iri}",
                String::from_utf8_lossy(&buf)
            );
            // Distinct IRIs are distinct propositions: each was just minted
            // from its own proposition and found equal to a different string.
            if source == Source::Data {
                if target.asserted {
                    data_layer_expected += 1;
                } else {
                    props_layer_expected += 1;
                }
            }
        }
    }

    // Layers: each marked position's triple is named by its reverse entry.
    for order in [Order::Spo, Order::Pos, Order::Ops] {
        check_layer(&index, Source::Data, order, hdt_path, data_layer_expected)?;
        if index.has_propositions() {
            check_layer(
                &index,
                Source::Propositions,
                order,
                &props_path,
                props_layer_expected,
            )?;
        } else if props_layer_expected > 0 {
            bail!("unasserted triple terms without a propositions HDT");
        }
    }
    Ok(())
}
