//! `hdtc header` command: dump or modify the RDF triples embedded in an HDT
//! file's header section.
//!
//! The header is a plain N-Triples blob (one triple per line) carrying dataset
//! metadata: void statistics, the HDT format/structure description, and any
//! descriptive triples about the dataset. Because HDT control-info blocks are
//! self-delimiting and no section stores absolute file offsets (and the
//! `.hdt.index.v1-1` file does not reference the header), the header can be
//! resized and the dictionary+triples bytes copied verbatim — no re-encoding
//! and no index invalidation.
//!
//! Modes:
//! - no flags  → dump the header N-Triples to stdout
//! - --replace → keep the hdtc-managed (data-derived) triples, swap the
//!   descriptive metadata for the triples in an RDF file
//! - --add     → append the triples from an RDF file to the header
//! - --dataset-uri → rewrite every occurrence of the current dataset IRI
//!   (subject or object) to a new IRI
//!
//! Any modification writes to `--output`; the original file is never changed.

use crate::hdt::reader::{write_nt_object, write_nt_subject};
use crate::io::{ControlInfo, ControlType};
use crate::rdf::{ParseOptions, discover_inputs, stream_quads_with_options};
use anyhow::{Context, Result, bail};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

const VOID_NS: &str = "http://rdfs.org/ns/void#";
const HDT_NS: &str = "http://purl.org/HDT/hdt#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const HDT_DATASET: &str = "http://purl.org/HDT/hdt#Dataset";
const VOID_DATASET: &str = "http://rdfs.org/ns/void#Dataset";

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

    let new_header = build_new_header(&header_text, replace, add, dataset_uri)?;

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

/// Assemble the new header text from the existing header, the optional input
/// RDF file (replace/add), and the optional dataset-IRI rename.
fn build_new_header(
    header_text: &str,
    replace: Option<&Path>,
    add: Option<&Path>,
    dataset_uri: Option<&str>,
) -> Result<String> {
    let existing: Vec<&str> = header_text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();

    // Validate the new dataset IRI up front so a malformed value can't corrupt
    // the header (and make the file unreadable) only to fail on reopen.
    if let Some(new_iri) = dataset_uri {
        validate_iri(new_iri)?;
    }

    let managed_bnodes = collect_managed_bnodes(&existing);

    let mut lines: Vec<String> = if replace.is_some() {
        existing
            .iter()
            .filter(|l| is_managed(l, &managed_bnodes))
            .map(|l| l.to_string())
            .collect()
    } else {
        existing.iter().map(|l| l.to_string()).collect()
    };

    if let Some(path) = replace.or(add) {
        // Offset the parser's blank-node disambiguation index past any `fN_`
        // blank nodes already present so a later --add can't merge its blank
        // nodes with ones a prior --add wrote into the header.
        let blank_offset = next_blank_index(&existing);
        lines.extend(read_input_triples(path, blank_offset)?);
    }

    if let Some(new_iri) = dataset_uri {
        let old_iri = current_dataset_iri(&existing)
            .context("Could not determine the current dataset IRI (no rdf:type hdt:Dataset/void:Dataset triple in the header)")?;
        let old_token = format!("<{old_iri}>");
        let new_token = format!("<{new_iri}>");
        for line in &mut lines {
            *line = reroot_line(line, &old_token, &new_token);
        }
    }

    let mut out = lines.join("\n");
    out.push('\n');
    Ok(out)
}

/// Parse an RDF input file and serialize each triple to an N-Triples line,
/// rejecting any triple that asserts a reserved (hdtc-managed) predicate.
///
/// `blank_offset` shifts the blank-node disambiguation index so parsed blank
/// nodes can't collide with `fN_` blank nodes already in the header.
fn read_input_triples(path: &Path, blank_offset: usize) -> Result<Vec<String>> {
    let discovered = discover_inputs(std::slice::from_ref(&path.to_path_buf()))?;
    if discovered.rdf_inputs.is_empty() {
        bail!("Input is not a recognized RDF file: {}", path.display());
    }

    let mut out = Vec::new();
    let options = ParseOptions::default();
    for (idx, input) in discovered.rdf_inputs.iter().enumerate() {
        // Resolve relative IRIs against the input file's own location, mirroring
        // `create`; otherwise relative/empty IRIs would corrupt the header.
        let base = std::fs::canonicalize(&input.path)
            .ok()
            .map(|p| format!("file://{}", p.display()));
        stream_quads_with_options(input, blank_offset + idx, true, base.as_deref(), &options, |quad| {
            if is_reserved_predicate(&quad.predicate) {
                bail!(
                    "Input triple uses reserved hdtc-managed predicate <{}>; these data-derived statistics cannot be set via the header command",
                    quad.predicate
                );
            }
            let mut line = Vec::new();
            write_nt_subject(&mut line, quad.subject.as_bytes())?;
            line.push(b' ');
            // Predicates are always IRIs; write_nt_subject wraps IRIs in <>.
            write_nt_subject(&mut line, quad.predicate.as_bytes())?;
            line.push(b' ');
            write_nt_object(&mut line, quad.object.as_bytes())?;
            line.extend_from_slice(b" .");
            out.push(String::from_utf8(line).expect("N-Triples serialization is valid UTF-8"));
            Ok(())
        })
        .with_context(|| format!("Failed to parse {}", input.path.display()))?;
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
// Header line classification helpers
// ---------------------------------------------------------------------------

/// Split an N-Triples line into its leading subject token and the remainder
/// (predicate, object and trailing ` .`).
fn split_subject(line: &str) -> Option<(&str, &str)> {
    let line = line.trim();
    let (subject, rest) = line.split_once(char::is_whitespace)?;
    Some((subject, rest.trim_start()))
}

/// Split an N-Triples line into (subject, predicate, object-and-dot) tokens.
fn split_triple(line: &str) -> Option<(&str, &str, &str)> {
    let (subject, rest) = split_subject(line)?;
    let (predicate, object_part) = rest.split_once(char::is_whitespace)?;
    Some((subject, predicate, object_part.trim()))
}

/// Strip the trailing ` .` from the object portion to recover the object term.
fn object_term(object_part: &str) -> &str {
    object_part
        .trim_end()
        .strip_suffix('.')
        .unwrap_or(object_part)
        .trim_end()
}

/// Recover the bare IRI from a `<...>` token.
fn iri_of(token: &str) -> Option<&str> {
    token.strip_prefix('<').and_then(|t| t.strip_suffix('>'))
}

/// Reject IRIs containing characters illegal in an N-Triples IRIREF. Embedding
/// such an IRI would corrupt the header and make the file unreadable on reopen.
fn validate_iri(iri: &str) -> Result<()> {
    if iri.is_empty() {
        bail!("--dataset-uri must not be empty");
    }
    for c in iri.chars() {
        if (c as u32) <= 0x20 || matches!(c, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\') {
            bail!(
                "Invalid --dataset-uri {iri:?}: contains {c:?}, which is not allowed in an N-Triples IRI"
            );
        }
    }
    Ok(())
}

/// Rewrite one N-Triples line, replacing the subject, predicate, and/or object
/// token wherever it exactly equals `old_token` (`<old_iri>`). Returns the line
/// unchanged if it can't be parsed as `S P O .`.
fn reroot_line(line: &str, old_token: &str, new_token: &str) -> String {
    let trimmed = line.trim();
    let Some(body) = trimmed.strip_suffix('.') else {
        return line.to_string();
    };
    let body = body.trim_end();
    let Some((subject, rest)) = body.split_once(char::is_whitespace) else {
        return line.to_string();
    };
    let Some((predicate, object)) = rest.trim_start().split_once(char::is_whitespace) else {
        return line.to_string();
    };

    let swap = |term: &str| -> String {
        if term == old_token {
            new_token.to_string()
        } else {
            term.to_string()
        }
    };
    format!(
        "{} {} {} .",
        swap(subject),
        swap(predicate),
        swap(object.trim())
    )
}

/// Parse the numeric index `N` from a `_:fN_…` disambiguated blank-node label.
fn blank_fn_index(term: &str) -> Option<usize> {
    let label = term.strip_prefix("_:")?;
    let rest = label.strip_prefix('f')?;
    let (digits, _) = rest.split_once('_')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<usize>().ok()
}

/// The next safe blank-node disambiguation index: one greater than the largest
/// `fN_` prefix already present among the header's blank nodes (0 if none).
fn next_blank_index(lines: &[&str]) -> usize {
    let mut max: Option<usize> = None;
    for line in lines {
        if let Some((subject, _predicate, object_part)) = split_triple(line) {
            for term in [subject, object_term(object_part)] {
                if let Some(n) = blank_fn_index(term) {
                    max = Some(max.map_or(n, |m| m.max(n)));
                }
            }
        }
    }
    max.map_or(0, |n| n + 1)
}

/// A predicate is reserved if it is one of the void statistics predicates or
/// lives in the HDT namespace (these are derived from the data / encoding).
fn is_reserved_predicate(predicate: &str) -> bool {
    predicate.starts_with(HDT_NS)
        || predicate.strip_prefix(VOID_NS).is_some_and(|local| {
            matches!(
                local,
                "triples" | "properties" | "distinctSubjects" | "distinctObjects"
            )
        })
}

/// Collect the blank nodes that hold the HDT format/statistics/publication
/// description — they appear as blank-node objects of reserved predicates.
fn collect_managed_bnodes(lines: &[&str]) -> Vec<String> {
    let mut bnodes = Vec::new();
    for line in lines {
        let Some((_, predicate, object_part)) = split_triple(line) else {
            continue;
        };
        let Some(pred_iri) = iri_of(predicate) else {
            continue;
        };
        if !is_reserved_predicate(pred_iri) {
            continue;
        }
        let object = object_term(object_part);
        if object.starts_with("_:") && !bnodes.iter().any(|b| b == object) {
            bnodes.push(object.to_string());
        }
    }
    bnodes
}

/// Decide whether a header line is part of the hdtc-managed block.
fn is_managed(line: &str, managed_bnodes: &[String]) -> bool {
    let Some((subject, predicate, object_part)) = split_triple(line) else {
        return false;
    };
    let Some(pred_iri) = iri_of(predicate) else {
        return false;
    };

    if is_reserved_predicate(pred_iri) {
        return true;
    }
    if managed_bnodes.iter().any(|b| b == subject) {
        return true;
    }
    if pred_iri == RDF_TYPE
        && let Some(obj_iri) = iri_of(object_term(object_part))
    {
        return obj_iri == HDT_DATASET || obj_iri == VOID_DATASET;
    }
    false
}

/// Find the current dataset IRI: the subject of the `rdf:type hdt:Dataset`
/// (or `void:Dataset`) declaration.
fn current_dataset_iri(lines: &[&str]) -> Option<String> {
    for line in lines {
        let Some((subject, predicate, object_part)) = split_triple(line) else {
            continue;
        };
        if iri_of(predicate) != Some(RDF_TYPE) {
            continue;
        }
        match iri_of(object_term(object_part)) {
            Some(HDT_DATASET) | Some(VOID_DATASET) => return iri_of(subject).map(str::to_string),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A header resembling what `build_header_ntriples` produces.
    fn sample_header(dataset: &str) -> String {
        format!(
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
        )
    }

    #[test]
    fn classifies_managed_and_descriptive() {
        let dataset = "http://example.org/ds";
        let header = sample_header(dataset);
        let lines: Vec<&str> = header.lines().map(str::trim).collect();
        let bnodes = collect_managed_bnodes(&lines);

        // All hdtc-generated lines are managed.
        for line in &lines {
            assert!(is_managed(line, &bnodes), "should be managed: {line}");
        }

        // A user-supplied descriptive triple is not managed, including a
        // non-HDT rdf:type and a Dublin Core title.
        let title = format!("<{dataset}> <http://purl.org/dc/terms/title> \"My dataset\" .");
        let dcat = format!("<{dataset}> <{RDF_TYPE}> <http://www.w3.org/ns/dcat#Dataset> .");
        assert!(!is_managed(&title, &bnodes));
        assert!(!is_managed(&dcat, &bnodes));
    }

    #[test]
    fn finds_dataset_iri() {
        let header = sample_header("http://example.org/ds");
        let lines: Vec<&str> = header.lines().map(str::trim).collect();
        assert_eq!(
            current_dataset_iri(&lines).as_deref(),
            Some("http://example.org/ds")
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
        let new = "http://example.org/new";
        let header = sample_header(old)
            + &format!("<{old}> <http://purl.org/dc/terms/title> \"T\" .\n")
            // The dataset IRI also appears as an OBJECT here.
            + &format!("<http://example.org/other> <http://www.w3.org/2002/07/owl#sameAs> <{old}> .\n");

        let out = build_new_header(&header, None, None, Some(new)).unwrap();

        assert!(!out.contains(&format!("<{old}>")), "no old IRI should remain");
        // Managed and descriptive subject-position triples are re-rooted.
        assert!(out.contains(&format!("<{new}> <{VOID_NS}triples> \"100\" .")));
        assert!(out.contains(&format!(
            "<{new}> <http://purl.org/dc/terms/title> \"T\" ."
        )));
        // Object-position occurrence is re-rooted too.
        assert!(out.contains(&format!(
            "<http://example.org/other> <http://www.w3.org/2002/07/owl#sameAs> <{new}> ."
        )));
        // Blank-node-subject triples are untouched.
        assert!(out.contains(&format!("_:format <{HDT_NS}dictionary> _:dictionary .")));
    }

    #[test]
    fn reroot_line_preserves_literal_with_spaces() {
        let line = "<http://ex/old> <http://purl.org/dc/terms/title> \"A long title.\" .";
        let out = reroot_line(line, "<http://ex/old>", "<http://ex/new>");
        assert_eq!(
            out,
            "<http://ex/new> <http://purl.org/dc/terms/title> \"A long title.\" ."
        );
    }

    #[test]
    fn validate_iri_rejects_illegal_chars() {
        assert!(validate_iri("http://example.org/ok").is_ok());
        assert!(validate_iri("http://example.org/a b").is_err()); // space
        assert!(validate_iri("urn:x>y").is_err()); // angle bracket
        assert!(validate_iri("").is_err()); // empty
        assert!(validate_iri("http://ex/\"q").is_err()); // quote
    }

    #[test]
    fn next_blank_index_advances_past_existing() {
        let none = ["<http://ex/s> <http://ex/p> _:format ."];
        assert_eq!(next_blank_index(&none), 0);

        let with_f = [
            "_:f0_x <http://ex/p> \"v\" .",
            "<http://ex/s> <http://ex/q> _:f3_y .",
        ];
        assert_eq!(next_blank_index(&with_f), 4);
    }

    #[test]
    fn replace_keeps_managed_drops_descriptive() {
        let dataset = "http://example.org/ds";
        let header = sample_header(dataset)
            + &format!("<{dataset}> <http://purl.org/dc/terms/title> \"Old\" .\n");
        let lines: Vec<&str> = header.lines().map(str::trim).collect();
        let bnodes = collect_managed_bnodes(&lines);

        let kept: Vec<&str> = lines
            .iter()
            .copied()
            .filter(|l| is_managed(l, &bnodes))
            .collect();

        // The descriptive title is dropped; the managed triple count is kept.
        assert!(!kept.iter().any(|l| l.contains("dc/terms/title")));
        assert!(kept.iter().any(|l| l.contains("void#triples")));
    }
}
