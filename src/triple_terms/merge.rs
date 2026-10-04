//! Merging HDT inputs that carry triple terms.
//!
//! A merge cannot copy the inputs' triple-term artifacts: positions change, and
//! a term unasserted in one input may be asserted in another, which moves its
//! proposition out of the propositions HDT altogether. So each input's
//! propositions are read back as records — the same records the parser writes
//! — renamed the way the merge renames that input's blank nodes, and the
//! merged result goes through the same finishing pass as parsed input
//! ([`super::build::finish`]).
//!
//! Reading back is streaming. An input's asserted propositions are the marked
//! triples of its SPO layer, read beside its SPO stream; its unasserted ones are
//! the marked triples of its propositions HDT, read the same way; and the few
//! propositions named only from inside another proposition are decoded through
//! the propositions HDT's run.

use super::PropositionRecord;
use super::format;
use super::iri::{TT_NAMESPACE, prefix_labels};
use super::reader::{LayerCursor, Order, Source, TripleTermIndex};
use super::spool::{PropositionSpool, SpoolWriter};
use crate::hdt::reader::{BitmapTriplesScanner, DictionaryResolver, open_hdt};
use crate::permutation::scan_hdt;
use anyhow::{Context, Result, ensure};
use std::path::Path;

/// Rename one dictionary string as the merge renames its input: blank nodes
/// take the input's prefix, and so do the labels inside a blank-node-bearing
/// triple-term IRI.
fn rename(term: Vec<u8>, prefix: Option<&str>) -> Result<String> {
    let term = match prefix {
        Some(prefix) if term.starts_with(b"_:") => {
            let mut renamed = Vec::with_capacity(term.len() + prefix.len());
            renamed.extend_from_slice(b"_:");
            renamed.extend_from_slice(prefix.as_bytes());
            renamed.extend_from_slice(&term[2..]);
            renamed
        }
        Some(prefix) => prefix_labels(&term, prefix).unwrap_or(term),
        None => term,
    };
    Ok(String::from_utf8(term)?)
}

/// Whether an HDT's dictionary holds any triple-term IRI, in any section.
pub(crate) fn has_triple_term_iris(dictionary: &mut DictionaryResolver) -> Result<bool> {
    let prefix = TT_NAMESPACE.as_bytes();
    Ok(!dictionary.shared.prefix_range(prefix)?.is_empty()
        || !dictionary.subjects.prefix_range(prefix)?.is_empty()
        || !dictionary.predicates.prefix_range(prefix)?.is_empty()
        || !dictionary.objects.prefix_range(prefix)?.is_empty())
}

/// Whether an HDT input to a merge carries triple terms. An input whose
/// dictionary holds triple-term IRIs without the index that defines them is
/// refused: its triple terms could only be merged as opaque IRIs.
pub(crate) fn input_has_triple_terms(hdt: &Path) -> Result<bool> {
    if format::canonical_path(hdt).exists() {
        return Ok(true);
    }
    let (_, mut dictionary) = open_hdt(hdt, 64 << 20)?;
    ensure!(
        !has_triple_term_iris(&mut dictionary)?,
        "HDT input {} contains RDF 1.2 triple terms but its triple-term index {} is missing",
        hdt.display(),
        format::canonical_path(hdt).display()
    );
    Ok(false)
}

/// Write the records of the marked triples of one HDT's SPO layer.
fn layer_records(
    index: &TripleTermIndex,
    source: Source,
    hdt: &Path,
    dictionary: &mut DictionaryResolver,
    data: &mut DictionaryResolver,
    prefix: Option<&str>,
    out: &mut SpoolWriter,
) -> Result<()> {
    let metadata = scan_hdt(hdt)?;
    let mut scanner = BitmapTriplesScanner::new(&metadata.offsets, hdt)?;
    let mut cursor = LayerCursor::new(index, source, Order::Spo, 0..metadata.triples)?;
    // Position of the next triple the scanner yields.
    let mut next = 0u64;
    let mut buf = Vec::new();
    while let Some((marked, data_term)) = cursor.next()? {
        while next < marked {
            scanner.next_triple()?.context("layer outruns its HDT")?;
            next += 1;
        }
        let triple = scanner.next_triple()?.context("layer outruns its HDT")?;
        next += 1;
        data.object_term_raw(data_term, &mut buf)?;
        let iri = rename(buf.clone(), prefix)?;
        dictionary.subject_term_raw(triple.0, &mut buf)?;
        let subject = rename(buf.clone(), prefix)?;
        dictionary.predicate_term(triple.1, &mut buf)?;
        let predicate = String::from_utf8(buf.clone())?;
        dictionary.object_term_raw(triple.2, &mut buf)?;
        let object = rename(buf.clone(), prefix)?;
        out.push(&PropositionRecord {
            iri,
            subject,
            predicate,
            object,
        })?;
    }
    Ok(())
}

/// Read back an HDT input's proposition records, renamed with `prefix` as the
/// merge renames that input. `None` when the input has no triple terms.
pub(crate) fn rederive(
    hdt: &Path,
    prefix: Option<&str>,
    spool_path: std::path::PathBuf,
    memory_limit: usize,
) -> Result<Option<PropositionSpool>> {
    if !input_has_triple_terms(hdt)? {
        return Ok(None);
    }
    let started = std::time::Instant::now();
    let index = TripleTermIndex::open(hdt)?;
    let (_, mut data) = open_hdt(hdt, memory_limit / 2)?;
    let props_path = format::propositions_path(hdt);
    let mut props = if index.has_propositions() {
        Some(open_hdt(&props_path, memory_limit / 4)?.1)
    } else {
        None
    };
    let mut out = SpoolWriter::create(spool_path)?;

    // Asserted propositions named in data.hdt: its marked triples.
    {
        let mut scratch = open_hdt(hdt, memory_limit / 8)?.1;
        layer_records(
            &index,
            Source::Data,
            hdt,
            &mut scratch,
            &mut data,
            prefix,
            &mut out,
        )?;
    }
    if let Some(props) = props.as_mut() {
        // Unasserted propositions named in data.hdt.
        layer_records(
            &index,
            Source::Propositions,
            &props_path,
            props,
            &mut data,
            prefix,
            &mut out,
        )?;
        // Propositions named only inside another proposition: through the
        // propositions HDT's own run. Those also named in data.hdt were written
        // above and are deduplicated with everything else.
        let mut buf = Vec::new();
        for object in index.run(Source::Propositions) {
            let target = index
                .target(Source::Propositions, object)?
                .context("run entry without a target")?;
            props.object_term_raw(object, &mut buf)?;
            let iri = rename(buf.clone(), prefix)?;
            let dictionary = if target.asserted {
                &mut data
            } else {
                &mut *props
            };
            dictionary.subject_term_raw(target.subject, &mut buf)?;
            let subject = rename(buf.clone(), prefix)?;
            dictionary.predicate_term(target.predicate, &mut buf)?;
            let predicate = String::from_utf8(buf.clone())?;
            dictionary.object_term_raw(target.object, &mut buf)?;
            let object = rename(buf.clone(), prefix)?;
            out.push(&PropositionRecord {
                iri,
                subject,
                predicate,
                object,
            })?;
        }
    }
    let spool = out.finish()?;
    tracing::info!(
        "Triple terms: read {} propositions back from {} ({:.1}s)",
        spool.records,
        hdt.display(),
        started.elapsed().as_secs_f64()
    );
    Ok(Some(spool))
}
