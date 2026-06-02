//! `hdtc header` command: dump or modify the RDF triples embedded in an HDT
//! file's header section.
//!
//! The header is an N-Triples blob carrying dataset metadata: void statistics,
//! the HDT format/structure description, and any descriptive triples about the
//! dataset. Because HDT control-info blocks are self-delimiting and no section
//! stores absolute file offsets (and the `.hdt.index.v1-1` file does not
//! reference the header), the header can be resized and the dictionary+triples
//! bytes copied verbatim — no re-encoding and no index invalidation.
//!
//! Modifications parse the existing header back into `oxrdf::Triple`s, operate
//! on those triples, and re-serialize through [`crate::rdf::serialize_triples`]
//! (the same path the HDT writer uses). Working on real triples — rather than
//! raw N-Triples text — keeps the command robust to literal escaping, blank
//! nodes, and IRI quoting.
//!
//! Modes:
//! - no flags  → dump the header N-Triples to stdout
//! - --replace → keep the hdtc-managed (data-derived) triples, swap the
//!   descriptive metadata for the triples in an RDF file
//! - --add     → append the triples from an RDF file to the header
//! - --dataset-uri → rewrite every occurrence of the current dataset IRI
//!   (subject, predicate or object) to a new IRI
//!
//! Any modification writes to `--output`; the original file is never changed.

use crate::hdt::header_vocab::{HDT_DATASET, HDT_NS, RDF_TYPE, VOID_DATASET, VOID_NS, VOID_STAT_LOCALS};
use crate::io::{ControlInfo, ControlType};
use crate::rdf::parser::parse_rdf_to_triples;
use crate::rdf::{discover_inputs, serialize_triples};
use anyhow::{Context, Result, bail};
use oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};
use std::collections::HashSet;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Entry point for the `header` command. Validates the flag combination, then
/// either dumps the header to stdout or writes a modified HDT file.
pub fn run_header_command(
    hdt_path: &Path,
    replace: Option<&Path>,
    add: Option<&Path>,
    dataset_uri: Option<&str>,
    output: Option<&Path>,
) -> Result<()> {
    if !hdt_path.exists() {
        bail!("HDT file not found: {}", hdt_path.display());
    }

    let modifying = replace.is_some() || add.is_some() || dataset_uri.is_some();
    match (modifying, output) {
        (true, None) => {
            bail!("--output is required when modifying the header (the original file is never changed)")
        }
        (false, Some(_)) => {
            bail!("--output is only used when modifying the header; with no modification flags the header is dumped to stdout")
        }
        _ => {}
    }

    let file = std::fs::File::open(hdt_path)
        .with_context(|| format!("Failed to open {}", hdt_path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);

    let (global_ci, header_ci, header_text) = read_header(&mut reader)?;

    if !modifying {
        let stdout = io::stdout();
        let mut lock = stdout.lock();
        lock.write_all(header_text.as_bytes())?;
        lock.flush()?;
        return Ok(());
    }

    // Parse the existing header into triples, apply the requested edits, and
    // re-serialize. Everything that can fail (IRI validation, reserved-predicate
    // rejection) happens here, before any output file is created.
    let existing =
        parse_ntriples_text(&header_text).context("Failed to parse existing header as N-Triples")?;
    let new_triples = build_new_header(existing, replace, add, dataset_uri)?;
    let new_header = serialize_triples(&new_triples)?;

    // `reader` is positioned at the start of the dictionary section; copy the
    // remaining bytes verbatim after the rewritten header.
    let output = output.expect("checked above");
    write_modified_hdt(output, &global_ci, &header_ci, &new_header, &mut reader)
        .with_context(|| format!("Failed to write {}", output.display()))?;
    tracing::info!("Modified HDT written: {}", output.display());
    Ok(())
}

/// Read the global control info, the header control info, and the header
/// N-Triples blob, leaving the reader positioned at the start of the
/// dictionary section. The header control info is returned so a rewrite can
/// preserve any properties it carries beyond `length`.
fn read_header<R: Read>(reader: &mut R) -> Result<(ControlInfo, ControlInfo, String)> {
    let global_ci = ControlInfo::read_from(reader).context("Failed to read global control info")?;
    if global_ci.control_type != ControlType::Global {
        bail!("Expected global control info at start of HDT file");
    }

    let header_ci = ControlInfo::read_from(reader).context("Failed to read header control info")?;
    if header_ci.control_type != ControlType::Header {
        bail!("Expected header control info");
    }
    let header_len: usize = header_ci
        .get_property("length")
        .and_then(|s| s.parse().ok())
        .context("Missing or invalid header length in control info")?;

    let mut buf = vec![0u8; header_len];
    reader
        .read_exact(&mut buf)
        .context("Failed to read header section")?;
    let text = String::from_utf8(buf).context("Header content is not valid UTF-8")?;

    Ok((global_ci, header_ci, text))
}

/// Assemble the new header triples from the existing header, the optional input
/// RDF file (replace/add), and the optional dataset-IRI rename.
fn build_new_header(
    existing: Vec<Triple>,
    replace: Option<&Path>,
    add: Option<&Path>,
    dataset_uri: Option<&str>,
) -> Result<Vec<Triple>> {
    // Validate the new dataset IRI up front so a malformed value can't corrupt
    // the header (and make the file unreadable) only to fail on reopen.
    let new_dataset = dataset_uri.map(make_dataset_node).transpose()?;

    let managed = collect_managed_bnodes(&existing);

    // `--replace` keeps only the hdtc-managed (data-derived) triples; `--add`
    // and a bare `--dataset-uri` keep the whole existing header.
    let mut triples: Vec<Triple> = if replace.is_some() {
        existing.iter().filter(|t| is_managed(t, &managed)).cloned().collect()
    } else {
        existing.clone()
    };

    if let Some(path) = replace.or(add) {
        // Offset the blank-node disambiguation index past any `fN_` blank nodes
        // already present so a later --add can't merge its blank nodes with ones
        // a prior --add wrote into the header.
        let blank_offset = next_blank_index(&existing);
        triples.extend(read_input_triples(path, blank_offset)?);
    }

    if let Some(new_node) = new_dataset {
        let old = match dataset_iris(&existing).as_slice() {
            [one] => one.clone(),
            [] => bail!(
                "Could not determine the current dataset IRI (no rdf:type hdt:Dataset/void:Dataset triple in the header)"
            ),
            many => bail!(
                "Header declares {} dataset IRIs ({}); --dataset-uri is ambiguous. Edit the header so exactly one subject has rdf:type hdt:Dataset/void:Dataset.",
                many.len(),
                many.join(", ")
            ),
        };
        reroot_triples(&mut triples, &old, &new_node);
    }

    Ok(triples)
}

/// Parse an RDF input file into triples, rejecting any triple that asserts a
/// reserved (hdtc-managed) predicate.
///
/// `blank_offset` shifts the blank-node disambiguation index so parsed blank
/// nodes can't collide with `fN_` blank nodes already in the header.
fn read_input_triples(path: &Path, blank_offset: usize) -> Result<Vec<Triple>> {
    let discovered = discover_inputs(std::slice::from_ref(&path.to_path_buf()))?;
    if discovered.rdf_inputs.is_empty() {
        bail!("Input is not a recognized RDF file: {}", path.display());
    }

    let mut out = Vec::new();
    for (idx, input) in discovered.rdf_inputs.iter().enumerate() {
        // Resolve relative IRIs against the input file's own location, mirroring
        // `create`; otherwise relative/empty IRIs would corrupt the header.
        let base = std::fs::canonicalize(&input.path)
            .ok()
            .map(|p| format!("file://{}", p.display()));
        let prefix = format!("f{}_", blank_offset + idx);
        let parsed = parse_rdf_to_triples(input, base.as_deref(), &prefix)
            .with_context(|| format!("Failed to parse {}", input.path.display()))?;
        if parsed.named_graph_seen {
            tracing::warn!(
                "{}: graph names are ignored; only triples are written to the header",
                input.path.display()
            );
        }
        for triple in parsed.triples {
            if is_reserved_predicate(triple.predicate.as_str()) {
                bail!(
                    "Input triple uses reserved hdtc-managed predicate <{}>; these data-derived statistics cannot be set via the header command",
                    triple.predicate.as_str()
                );
            }
            out.push(triple);
        }
    }
    Ok(out)
}

/// Write the global control info, a rewritten header, and the verbatim
/// remainder (dictionary + triples) read from `rest`. The original header
/// control info is reused (only its `length` is updated) so any properties it
/// carried are preserved.
fn write_modified_hdt<R: Read>(
    output: &Path,
    global_ci: &ControlInfo,
    header_ci: &ControlInfo,
    header_text: &str,
    rest: &mut R,
) -> Result<()> {
    let file = std::fs::File::create(output)?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);

    global_ci.write_to(&mut writer)?;

    let mut header_ci = header_ci.clone();
    header_ci.set_property("length", header_text.len().to_string());
    header_ci.write_to(&mut writer)?;
    writer.write_all(header_text.as_bytes())?;

    io::copy(rest, &mut writer).context("Failed to copy dictionary and triples sections")?;
    writer.flush()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Triple classification helpers
// ---------------------------------------------------------------------------

/// Parse an N-Triples string into oxrdf triples.
///
/// Parsing is strict (not lenient): a malformed line in the existing header is
/// surfaced as an error rather than silently dropped, so a rewrite can never
/// quietly discard header content it failed to understand.
fn parse_ntriples_text(text: &str) -> Result<Vec<Triple>> {
    let parser = oxrdfio::RdfParser::from_format(oxrdfio::RdfFormat::NTriples)
        .for_reader(text.as_bytes());

    let mut triples = Vec::new();
    for quad in parser {
        let quad = quad.context("Invalid N-Triples in HDT header")?;
        triples.push(Triple::new(quad.subject, quad.predicate, quad.object));
    }
    Ok(triples)
}

/// A predicate is reserved if it is one of the void statistics predicates or
/// lives in the HDT namespace (these are derived from the data / encoding).
fn is_reserved_predicate(predicate: &str) -> bool {
    predicate.starts_with(HDT_NS)
        || predicate
            .strip_prefix(VOID_NS)
            .is_some_and(|local| VOID_STAT_LOCALS.contains(&local))
}

/// Collect the blank nodes that hold the HDT format/statistics/publication
/// description — they appear as blank-node objects of reserved predicates.
fn collect_managed_bnodes(triples: &[Triple]) -> HashSet<String> {
    let mut bnodes = HashSet::new();
    for triple in triples {
        if is_reserved_predicate(triple.predicate.as_str())
            && let Term::BlankNode(b) = &triple.object
        {
            bnodes.insert(b.as_str().to_string());
        }
    }
    bnodes
}

/// Decide whether a header triple is part of the hdtc-managed block.
fn is_managed(triple: &Triple, managed_bnodes: &HashSet<String>) -> bool {
    if is_reserved_predicate(triple.predicate.as_str()) {
        return true;
    }
    if let NamedOrBlankNode::BlankNode(b) = &triple.subject
        && managed_bnodes.contains(b.as_str())
    {
        return true;
    }
    if triple.predicate.as_str() == RDF_TYPE
        && let Term::NamedNode(obj) = &triple.object
    {
        return obj.as_str() == HDT_DATASET || obj.as_str() == VOID_DATASET;
    }
    false
}

/// Find the dataset IRIs: the named-node subjects of `rdf:type hdt:Dataset`
/// (or `void:Dataset`) declarations, distinct and in first-seen order.
///
/// A well-formed hdtc header has exactly one, but a header could in principle
/// carry several (e.g. after manual editing); the caller decides what to do
/// with zero or many.
fn dataset_iris(triples: &[Triple]) -> Vec<String> {
    let mut iris: Vec<String> = Vec::new();
    for triple in triples {
        if triple.predicate.as_str() != RDF_TYPE {
            continue;
        }
        let Term::NamedNode(obj) = &triple.object else {
            continue;
        };
        if (obj.as_str() == HDT_DATASET || obj.as_str() == VOID_DATASET)
            && let NamedOrBlankNode::NamedNode(s) = &triple.subject
        {
            let iri = s.as_str();
            if !iris.iter().any(|existing| existing == iri) {
                iris.push(iri.to_string());
            }
        }
    }
    iris
}

/// Rewrite every occurrence of `old` (a bare IRI) — in subject, predicate, or
/// object position — to `new`, leaving blank nodes and literals untouched.
fn reroot_triples(triples: &mut [Triple], old: &str, new: &NamedNode) {
    for triple in triples.iter_mut() {
        if matches!(&triple.subject, NamedOrBlankNode::NamedNode(n) if n.as_str() == old) {
            triple.subject = NamedOrBlankNode::NamedNode(new.clone());
        }
        if triple.predicate.as_str() == old {
            triple.predicate = new.clone();
        }
        if matches!(&triple.object, Term::NamedNode(n) if n.as_str() == old) {
            triple.object = Term::NamedNode(new.clone());
        }
    }
}

/// Validate and build the new dataset IRI node. A malformed IRI is rejected
/// here (before any output is written) because embedding it would corrupt the
/// header and make the file unreadable on reopen. Validation is delegated to
/// `oxrdf::NamedNode`, the same check the serializer applies.
fn make_dataset_node(iri: &str) -> Result<NamedNode> {
    if iri.is_empty() {
        bail!("--dataset-uri must not be empty");
    }
    NamedNode::new(iri).map_err(|e| anyhow::anyhow!("Invalid --dataset-uri {iri:?}: {e}"))
}

/// Parse the numeric index `N` from an `fN_…` disambiguated blank-node label.
fn blank_fn_index(label: &str) -> Option<usize> {
    let rest = label.strip_prefix('f')?;
    let (digits, _) = rest.split_once('_')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<usize>().ok()
}

/// The next safe blank-node disambiguation index: one greater than the largest
/// `fN_` label already present among the header's blank nodes (0 if none).
fn next_blank_index(triples: &[Triple]) -> usize {
    let mut max: Option<usize> = None;
    for triple in triples {
        let labels = [
            match &triple.subject {
                NamedOrBlankNode::BlankNode(b) => Some(b.as_str()),
                _ => None,
            },
            match &triple.object {
                Term::BlankNode(b) => Some(b.as_str()),
                _ => None,
            },
        ];
        for label in labels.into_iter().flatten() {
            if let Some(n) = blank_fn_index(label) {
                max = Some(max.map_or(n, |m| m.max(n)));
            }
        }
    }
    max.map_or(0, |n| n + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A header resembling what `build_header_ntriples` produces, as triples.
    fn sample_header(dataset: &str) -> Vec<Triple> {
        let text = format!(
            "<{dataset}> <{RDF_TYPE}> <{HDT_DATASET}> .\n\
             <{dataset}> <{RDF_TYPE}> <{VOID_DATASET}> .\n\
             <{dataset}> <{VOID_NS}triples> \"100\" .\n\
             <{dataset}> <{HDT_NS}formatInformation> _:format .\n\
             _:format <{HDT_NS}dictionary> _:dictionary .\n\
             _:format <{HDT_NS}triples> _:triples .\n\
             <{dataset}> <{HDT_NS}publicationInformation> _:pub .\n\
             _:dictionary <http://purl.org/dc/terms/format> <{HDT_NS}dictionaryFour> .\n\
             _:triples <{HDT_NS}triplesnumTriples> \"100\" .\n\
             _:pub <http://purl.org/dc/terms/issued> \"2024-01-01\" .\n"
        );
        parse_ntriples_text(&text).unwrap()
    }

    fn triple(s: &str, p: &str, o: &str) -> Triple {
        parse_ntriples_text(&format!("{s} {p} {o} .\n")).unwrap().remove(0)
    }

    #[test]
    fn classifies_managed_and_descriptive() {
        let dataset = "http://example.org/ds";
        let triples = sample_header(dataset);
        let bnodes = collect_managed_bnodes(&triples);

        // All hdtc-generated triples are managed.
        for t in &triples {
            assert!(is_managed(t, &bnodes), "should be managed: {t}");
        }

        // A user-supplied descriptive triple is not managed, including a
        // non-HDT rdf:type and a Dublin Core title.
        let title = triple(
            &format!("<{dataset}>"),
            "<http://purl.org/dc/terms/title>",
            "\"My dataset\"",
        );
        let dcat = triple(
            &format!("<{dataset}>"),
            &format!("<{RDF_TYPE}>"),
            "<http://www.w3.org/ns/dcat#Dataset>",
        );
        assert!(!is_managed(&title, &bnodes));
        assert!(!is_managed(&dcat, &bnodes));
    }

    #[test]
    fn finds_dataset_iri() {
        let triples = sample_header("http://example.org/ds");
        assert_eq!(dataset_iris(&triples), vec!["http://example.org/ds".to_string()]);
    }

    #[test]
    fn dataset_iris_dedups_and_reports_multiple() {
        // No dataset declaration → empty.
        let empty = vec![triple(
            "<http://ex/s>",
            "<http://purl.org/dc/terms/title>",
            "\"T\"",
        )];
        assert!(dataset_iris(&empty).is_empty());

        // The two rdf:type declarations in sample_header share one subject, so
        // it is reported once, not twice.
        let one = sample_header("http://example.org/ds");
        assert_eq!(dataset_iris(&one).len(), 1);

        // Two distinct dataset subjects are both reported, in first-seen order.
        let mut two = sample_header("http://example.org/a");
        two.push(triple(
            "<http://example.org/b>",
            &format!("<{RDF_TYPE}>"),
            &format!("<{HDT_DATASET}>"),
        ));
        assert_eq!(
            dataset_iris(&two),
            vec!["http://example.org/a".to_string(), "http://example.org/b".to_string()]
        );
    }

    #[test]
    fn reserved_predicate_detection() {
        assert!(is_reserved_predicate("http://purl.org/HDT/hdt#triplesnumTriples"));
        assert!(is_reserved_predicate("http://rdfs.org/ns/void#triples"));
        assert!(!is_reserved_predicate("http://purl.org/dc/terms/title"));
        assert!(!is_reserved_predicate(RDF_TYPE));
    }

    #[test]
    fn rename_reroots_subject_and_object_occurrences() {
        let old = "http://example.org/ds";
        let new_iri = "http://example.org/new";
        let mut triples = sample_header(old);
        triples.push(triple(
            &format!("<{old}>"),
            "<http://purl.org/dc/terms/title>",
            "\"T\"",
        ));
        // The dataset IRI also appears as an OBJECT here.
        triples.push(triple(
            "<http://example.org/other>",
            "<http://www.w3.org/2002/07/owl#sameAs>",
            &format!("<{old}>"),
        ));

        reroot_triples(&mut triples, old, &NamedNode::new_unchecked(new_iri));
        let out = serialize_triples(&triples).unwrap();

        assert!(!out.contains(&format!("<{old}>")), "no old IRI should remain");
        // Managed and descriptive subject-position triples are re-rooted.
        assert!(out.contains(&format!("<{new_iri}> <{VOID_NS}triples> \"100\" .")));
        assert!(out.contains(&format!(
            "<{new_iri}> <http://purl.org/dc/terms/title> \"T\" ."
        )));
        // Object-position occurrence is re-rooted too.
        assert!(out.contains(&format!(
            "<http://example.org/other> <http://www.w3.org/2002/07/owl#sameAs> <{new_iri}> ."
        )));
        // Blank-node-subject triples are untouched.
        assert!(out.contains(&format!("_:format <{HDT_NS}dictionary> _:dictionary .")));
    }

    #[test]
    fn make_dataset_node_rejects_illegal_chars() {
        assert!(make_dataset_node("http://example.org/ok").is_ok());
        assert!(make_dataset_node("http://example.org/a b").is_err()); // space
        assert!(make_dataset_node("urn:x>y").is_err()); // angle bracket
        assert!(make_dataset_node("").is_err()); // empty
        assert!(make_dataset_node("http://ex/\"q").is_err()); // quote
    }

    #[test]
    fn next_blank_index_advances_past_existing() {
        let none = sample_header("http://example.org/ds");
        // Managed blank nodes (format, dictionary, …) don't match the `fN_`
        // scheme, so the next index starts at 0.
        assert_eq!(next_blank_index(&none), 0);

        let with_f = vec![
            triple("_:f0_x", "<http://ex/p>", "\"v\""),
            triple("<http://ex/s>", "<http://ex/q>", "_:f3_y"),
        ];
        assert_eq!(next_blank_index(&with_f), 4);
    }

    #[test]
    fn replace_keeps_managed_drops_descriptive() {
        let dataset = "http://example.org/ds";
        let mut triples = sample_header(dataset);
        triples.push(triple(
            &format!("<{dataset}>"),
            "<http://purl.org/dc/terms/title>",
            "\"Old\"",
        ));
        let bnodes = collect_managed_bnodes(&triples);

        let kept: Vec<&Triple> = triples.iter().filter(|t| is_managed(t, &bnodes)).collect();

        // The descriptive title is dropped; the managed triple count is kept.
        assert!(!kept.iter().any(|t| t.predicate.as_str().contains("dc/terms/title")));
        assert!(kept.iter().any(|t| t.predicate.as_str() == format!("{VOID_NS}triples")));
    }
}
