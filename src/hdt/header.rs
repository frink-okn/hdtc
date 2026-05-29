//! Inspect and rewrite the N-Triples metadata embedded in an HDT file's header.
//!
//! An HDT file is a sequence of self-sized sections, each preceded by a
//! [`ControlInfo`] block. The Header section is an N-Triples RDF block whose
//! ControlInfo carries a single positional `length` property giving the byte
//! length of the N-Triples payload that follows it:
//!
//! ```text
//! Global ControlInfo
//! Header ControlInfo  (length=<bytes>)
//! <length bytes of N-Triples>
//! Dictionary ControlInfo …
//! Triples ControlInfo …
//! ```
//!
//! Rewriting the header changes the payload's byte length, which invalidates
//! both the `length` property *and* the Header ControlInfo's trailing CRC16. We
//! never patch in place: instead we re-emit the Global and Header ControlInfo
//! (going through [`ControlInfo::write_to`], which recomputes the CRC16) and
//! stream-copy the Dictionary and Triples sections verbatim. Because those
//! sections are byte-for-byte identical, any existing `.hdt.index` sidecar stays
//! valid.

use crate::io::{ControlInfo, ControlType};
use crate::rdf::serialize_triples;
use anyhow::{Context, Result, bail};
use oxrdf::Triple;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Predicates hdtc reads from the header to recover the triple count
/// (`reader.rs`/`input_adapter.rs`). At least one must remain present or the
/// rewritten HDT cannot be reopened.
const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";
const HDT_TRIPLES_NUM: &str = "http://purl.org/HDT/hdt#triplesnumTriples";

/// True if `predicate` states the dataset's triple count.
fn is_count_predicate(predicate: &str) -> bool {
    matches!(predicate, VOID_TRIPLES | HDT_TRIPLES_NUM)
}

/// True if any triple states the dataset's triple count.
fn has_triple_count(triples: &[Triple]) -> bool {
    triples.iter().any(|t| is_count_predicate(t.predicate.as_str()))
}

/// Clone out the triple-count statements from a triple list.
fn take_count_triples(triples: &[Triple]) -> Vec<Triple> {
    triples
        .iter()
        .filter(|t| is_count_predicate(t.predicate.as_str()))
        .cloned()
        .collect()
}

/// Drop any triple-count statements from `triples`, returning whether any were
/// removed. The count is file-owned, so it is never taken from user input.
fn strip_count_triples(triples: &mut Vec<Triple>) -> bool {
    let before = triples.len();
    triples.retain(|t| !is_count_predicate(t.predicate.as_str()));
    before != triples.len()
}

/// Print the header triples to `output` (or stdout) as N-Triples. Returns the
/// number of triples written.
pub fn dump(hdt_path: &Path, output: Option<&Path>) -> Result<u64> {
    let (_global_ci, header_text, _reader) = read_header(hdt_path)?;
    let count = header_text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count() as u64;

    match output {
        Some(path) => {
            std::fs::write(path, header_text.as_bytes())
                .with_context(|| format!("Failed to write {}", path.display()))?;
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            stdout.write_all(header_text.as_bytes())?;
            stdout.flush()?;
        }
    }

    Ok(count)
}

/// Replace the header triples wholesale with the triples from `input_path`.
/// Returns the number of triples written.
pub fn replace(hdt_path: &Path, input_path: &Path, output: Option<&Path>) -> Result<u64> {
    let (global_ci, old_text, reader) = read_header(hdt_path)?;

    // The triple count is a fact about the stored data (which is copied
    // verbatim), not editorial metadata. It is always taken from the original
    // header and never from user input, so a replaced HDT stays readable and
    // the count can't be set to a wrong value.
    let original = parse_ntriples_text(&old_text)
        .context("Failed to parse existing header as N-Triples")?;
    let original_counts = take_count_triples(&original);

    // No existing-header collision possible, so blank-node labels are preserved
    // for a single input file (auto-prefixed only across multiple files).
    let mut triples = parse_input_triples(input_path, None)?;
    if strip_count_triples(&mut triples) {
        tracing::warn!(
            "Ignoring triple-count statement(s) in the input header; the count is taken from \
             the HDT data"
        );
    }

    if original_counts.is_empty() {
        tracing::warn!(
            "Existing header has no triple count to carry over; the rewritten HDT may not be \
             reopenable by hdtc"
        );
    } else {
        triples.extend(original_counts);
    }

    let payload = serialize_triples(&triples)?;

    finish_rewrite(global_ci, reader, hdt_path, output, &payload)?;
    Ok(triples.len() as u64)
}

/// Merge the triples from `input_path` into the existing header triples (set
/// union, deduplicated). Returns the total number of triples written.
pub fn augment(hdt_path: &Path, input_path: &Path, output: Option<&Path>) -> Result<u64> {
    let (global_ci, old_text, reader) = read_header(hdt_path)?;

    // Existing header triples keep their original blank-node labels (they are
    // internally self-consistent). Input blank nodes are prefixed so they can
    // never be conflated with the header's `_:format`, `_:dictionary`, etc.
    let existing = parse_ntriples_text(&old_text)
        .context("Failed to parse existing header as N-Triples")?;
    let mut added = parse_input_triples(input_path, Some("in"))?;

    // The header's own triple count wins; never take a (possibly conflicting)
    // count from the input, which would risk a mismatch the reader rejects.
    if strip_count_triples(&mut added) {
        tracing::warn!(
            "Ignoring triple-count statement(s) in the input; the header's existing count is kept"
        );
    }

    let mut seen: HashSet<Triple> = HashSet::new();
    let mut merged: Vec<Triple> = Vec::with_capacity(existing.len() + added.len());
    for triple in existing.into_iter().chain(added) {
        if seen.insert(triple.clone()) {
            merged.push(triple);
        }
    }

    if !has_triple_count(&merged) {
        tracing::warn!(
            "Existing header has no triple count; the augmented HDT may not be reopenable by hdtc"
        );
    }

    let payload = serialize_triples(&merged)?;
    finish_rewrite(global_ci, reader, hdt_path, output, &payload)?;
    Ok(merged.len() as u64)
}

/// Open `hdt_path`, read and validate the Global + Header ControlInfo, and
/// return the Global ControlInfo, the header's N-Triples payload, and a reader
/// positioned at the start of the Dictionary section.
fn read_header(hdt_path: &Path) -> Result<(ControlInfo, String, BufReader<File>)> {
    let file = File::open(hdt_path)
        .with_context(|| format!("Failed to open HDT file {}", hdt_path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);

    let global_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read global control info")?;
    if global_ci.control_type != ControlType::Global {
        bail!("Expected global control info at start of HDT file");
    }

    let header_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read header control info")?;
    if header_ci.control_type != ControlType::Header {
        bail!("Expected header control info");
    }
    let header_len: usize = header_ci
        .get_property("length")
        .and_then(|s| s.parse().ok())
        .context("Missing or invalid header length in control info")?;

    let mut header_buf = vec![0u8; header_len];
    reader
        .read_exact(&mut header_buf)
        .context("Failed to read header section")?;
    let header_text =
        String::from_utf8(header_buf).context("Header content is not valid UTF-8")?;

    Ok((global_ci, header_text, reader))
}

/// Write a new HDT file with `payload` as the header, copying the rest of the
/// source (Dictionary + Triples) verbatim from `reader`.
///
/// The output is written to a temporary file in the destination directory and
/// atomically renamed into place, so an in-place rewrite never truncates the
/// file it is still reading.
fn finish_rewrite(
    global_ci: ControlInfo,
    mut reader: BufReader<File>,
    hdt_path: &Path,
    output: Option<&Path>,
    payload: &str,
) -> Result<()> {
    let dest = output.unwrap_or(hdt_path);
    let dir = match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };

    let mut tmp = tempfile::NamedTempFile::new_in(dir)
        .with_context(|| format!("Failed to create temp file in {}", dir.display()))?;

    {
        let mut writer = BufWriter::with_capacity(256 * 1024, tmp.as_file_mut());

        // Global ControlInfo — re-emitting the struct reproduces it verbatim.
        global_ci.write_to(&mut writer)?;

        // Fresh Header ControlInfo with the recomputed length (and CRC16).
        let mut header_ci = ControlInfo::new(ControlType::Header, "ntriples");
        header_ci.set_property("length", payload.len().to_string());
        header_ci.write_to(&mut writer)?;

        writer.write_all(payload.as_bytes())?;

        // Dictionary + Triples sections, byte-for-byte.
        std::io::copy(&mut reader, &mut writer)
            .context("Failed to copy dictionary/triples sections")?;

        writer.flush()?;
    }

    tmp.persist(dest)
        .with_context(|| format!("Failed to write {}", dest.display()))?;

    Ok(())
}

/// Parse the discovered RDF file(s) at `path` into a deduplicated list of
/// triples (RDF graphs are sets), preserving first-seen order.
///
/// When `force_prefix` is `Some`, every file's blank nodes are prefixed with
/// `<base><idx>_`. When `None`, blank nodes are prefixed only if more than one
/// file is discovered (so a single file keeps its original labels).
fn parse_input_triples(path: &Path, force_prefix: Option<&str>) -> Result<Vec<Triple>> {
    let discovered = crate::rdf::discover_inputs(std::slice::from_ref(&path.to_path_buf()))?;
    if !discovered.hdt_inputs.is_empty() {
        bail!(
            "Header input must be RDF, not an HDT file: {}",
            path.display()
        );
    }
    let inputs = discovered.rdf_inputs;
    let multi = inputs.len() > 1;

    let mut seen: HashSet<Triple> = HashSet::new();
    let mut out: Vec<Triple> = Vec::new();

    for (idx, input) in inputs.iter().enumerate() {
        let prefix = match force_prefix {
            Some(base) => format!("{base}{idx}_"),
            None if multi => format!("f{idx}_"),
            None => String::new(),
        };
        let parsed = crate::rdf::parser::parse_rdf_to_triples(input, None, &prefix)?;
        if parsed.named_graph_seen {
            tracing::warn!(
                "{}: graph names are ignored; only triples are written to the header",
                input.path.display()
            );
        }
        for triple in parsed.triples {
            if seen.insert(triple.clone()) {
                out.push(triple);
            }
        }
    }

    Ok(out)
}

/// Parse an in-memory N-Triples string into oxrdf triples (lenient).
fn parse_ntriples_text(text: &str) -> Result<Vec<Triple>> {
    let parser = oxrdfio::RdfParser::from_format(oxrdfio::RdfFormat::NTriples)
        .lenient()
        .for_reader(text.as_bytes());

    let mut triples = Vec::new();
    for quad in parser {
        let quad = quad.context("Invalid N-Triples in HDT header")?;
        triples.push(Triple::new(quad.subject, quad.predicate, quad.object));
    }
    Ok(triples)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{BlankNode, NamedNode};

    #[test]
    fn test_parse_ntriples_text_blank_nodes() {
        let text = "_:a <http://example.org/p> _:b .\n";
        let triples = parse_ntriples_text(text).unwrap();
        assert_eq!(triples.len(), 1);
        assert!(matches!(
            triples[0].subject,
            oxrdf::NamedOrBlankNode::BlankNode(_)
        ));
    }

    #[test]
    fn test_parse_input_triples_dedups() {
        let mut f = tempfile::Builder::new().suffix(".nt").tempfile().unwrap();
        writeln!(f, "<http://example.org/s> <http://example.org/p> <http://example.org/o> .").unwrap();
        writeln!(f, "<http://example.org/s> <http://example.org/p> <http://example.org/o> .").unwrap();
        writeln!(f, "<http://example.org/s> <http://example.org/p> <http://example.org/o2> .").unwrap();
        f.flush().unwrap();

        let triples = parse_input_triples(f.path(), None).unwrap();
        assert_eq!(triples.len(), 2, "duplicate triple should be collapsed");
    }

    #[test]
    fn test_augment_blank_node_prefixing() {
        // The input reuses the label `_:format`, which the real header also uses.
        // Prefixing keeps them distinct so they are not conflated.
        let blank = oxrdf::Term::BlankNode(BlankNode::new_unchecked("format"));
        let triples = vec![Triple::new(
            BlankNode::new_unchecked("format"),
            NamedNode::new_unchecked("http://example.org/p"),
            blank,
        )];
        let text = serialize_triples(&triples).unwrap();

        let mut f = tempfile::Builder::new().suffix(".nt").tempfile().unwrap();
        f.write_all(text.as_bytes()).unwrap();
        f.flush().unwrap();

        let parsed = parse_input_triples(f.path(), Some("in")).unwrap();
        assert_eq!(parsed.len(), 1);
        let oxrdf::NamedOrBlankNode::BlankNode(b) = &parsed[0].subject else {
            panic!("expected blank node subject");
        };
        assert!(
            b.as_str().starts_with("in0_"),
            "blank label should be prefixed, got {}",
            b.as_str()
        );
    }
}
