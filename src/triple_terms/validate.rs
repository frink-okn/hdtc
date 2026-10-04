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
//! - every set layer bit marks a triple whose IRI is the one its reverse entry
//!   names, and each layer has exactly one bit per IRI it should cover.

use super::format::{self, Section};
use super::iri::{Component, Proposition, TT_NAMESPACE, iri_for};
use super::reader::{Layer, Order, Source, Target, TripleTermIndex};
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
    ensure!(depth < 64, "triple terms nested too deeply");
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
    Ok(term_component(String::from_utf8(buf)?))
}

fn term_component(term: String) -> Component {
    if let Some(label) = term.strip_prefix("_:") {
        Component::Blank(label.to_owned())
    } else if term.starts_with('"') {
        Component::Literal(term)
    } else {
        Component::Iri(term)
    }
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
    let subject = term_component(String::from_utf8(buf.clone())?);
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

fn check_layer(
    index: &TripleTermIndex,
    dictionaries: &mut Dictionaries,
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
    let population = layer.rank1(index.file(), layer.len())?;
    ensure!(
        population == expected && layer.reverse.entry_count == expected,
        "{source:?} {order:?} layer marks {population} triples, expected {expected}"
    );
    let perm = crate::permutation::canonical_path(hdt);
    let mut data_buf = Vec::new();
    let mut rank = 0u64;
    let mut bitmap = vec![0u8; layer.len().div_ceil(8) as usize];
    if !bitmap.is_empty() {
        index
            .file()
            .read_exact_at(&mut bitmap, layer.bitmap.offset)?;
    }
    for (position, triple) in ordered_triples(hdt, &perm, order)?.enumerate() {
        let (s, p, o) = triple?;
        if bitmap[position / 8] >> (position % 8) & 1 == 0 {
            continue;
        }
        let target = Target {
            subject: s,
            predicate: p,
            object: o,
            asserted: source == Source::Data,
        };
        let iri = iri_for(&proposition_of(index, dictionaries, target, 0)?);
        let data_term = layer.reverse(index.file(), rank)?;
        dictionaries
            .data
            .object_term_raw(data_term, &mut data_buf)
            .with_context(|| format!("{source:?} {order:?} reverse entry {rank}"))?;
        ensure!(
            data_buf == iri.as_bytes(),
            "{source:?} {order:?} layer position {position} names {} but is {iri}",
            String::from_utf8_lossy(&data_buf)
        );
        rank += 1;
    }
    ensure!(rank == expected, "{source:?} {order:?} layer is truncated");
    Ok(())
}

/// Validate the triple-term index and propositions HDT beside `hdt_path`.
pub fn validate_triple_terms(hdt_path: &Path, temp_dir: &Path, memory_limit: usize) -> Result<()> {
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
        ensure!(
            dictionary
                .shared
                .prefix_range(TT_NAMESPACE.as_bytes())?
                .is_empty(),
            "a triple-term IRI is a subject"
        );
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
        check_layer(
            &index,
            &mut dictionaries,
            Source::Data,
            order,
            hdt_path,
            data_layer_expected,
        )?;
        if index.has_propositions() {
            check_layer(
                &index,
                &mut dictionaries,
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
