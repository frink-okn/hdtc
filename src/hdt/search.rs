//! HDT triple-pattern search engine (Phase 1).
//!
//! Supports the following query patterns without an index file:
//!
//! - `???`  — scan all triples
//! - `S??`  — subject bound
//! - `SP?`  — subject + predicate bound
//! - `S?O`  — subject + object bound
//! - `SPO`  — exact triple lookup
//!
//! Patterns requiring the index file (`?P?`, `??O`, `?PO`) are planned for
//! Phase 2 and Phase 3 and will return a clear error for now.

use crate::hdt::reader::{BitmapTriplesScanner, open_hdt, write_triple_tab};
use anyhow::{Context, Result, bail};
use std::io::Write;
use std::path::Path;

// ---------------------------------------------------------------------------
// Query term and pattern
// ---------------------------------------------------------------------------

/// A single position in a triple pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryTerm {
    /// `?` or `*` — matches any value.
    Wildcard,
    /// A bound term represented as raw HDT dictionary bytes.
    ///
    /// - IRIs: plain UTF-8 IRI string (no `<>` brackets)
    /// - Blank nodes: `_:id`
    /// - Literals: `"value"`, `"value"@lang`, or `"value"^^<type>`
    Bound(Vec<u8>),
}

/// Classified query pattern based on which positions are bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternKind {
    /// `???` — no constraints, full scan.
    ScanAll,
    /// `S??` — subject bound only.
    SubjectBound,
    /// `SP?` — subject and predicate bound.
    SubjectPredicateBound,
    /// `S?O` — subject and object bound.
    SubjectObjectBound,
    /// `SPO` — exact triple.
    Exact,
    /// `?P?` — predicate bound only (requires index — Phase 2).
    PredicateBound,
    /// `??O` — object bound only (requires index — Phase 3).
    ObjectBound,
    /// `?PO` — predicate and object bound (requires index — Phase 3/4).
    PredicateObjectBound,
}

impl PatternKind {
    fn from_terms(s: &QueryTerm, p: &QueryTerm, o: &QueryTerm) -> Self {
        match (s, p, o) {
            (QueryTerm::Wildcard, QueryTerm::Wildcard, QueryTerm::Wildcard) => Self::ScanAll,
            (QueryTerm::Bound(_), QueryTerm::Wildcard, QueryTerm::Wildcard) => Self::SubjectBound,
            (QueryTerm::Bound(_), QueryTerm::Bound(_), QueryTerm::Wildcard) => {
                Self::SubjectPredicateBound
            }
            (QueryTerm::Bound(_), QueryTerm::Wildcard, QueryTerm::Bound(_)) => {
                Self::SubjectObjectBound
            }
            (QueryTerm::Bound(_), QueryTerm::Bound(_), QueryTerm::Bound(_)) => Self::Exact,
            (QueryTerm::Wildcard, QueryTerm::Bound(_), QueryTerm::Wildcard) => {
                Self::PredicateBound
            }
            (QueryTerm::Wildcard, QueryTerm::Wildcard, QueryTerm::Bound(_)) => Self::ObjectBound,
            (QueryTerm::Wildcard, QueryTerm::Bound(_), QueryTerm::Bound(_)) => {
                Self::PredicateObjectBound
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Query parser
// ---------------------------------------------------------------------------

/// Parse a triple pattern query string into three `QueryTerm`s.
///
/// The format is three whitespace-separated N-Triples terms. A `?` or `*`
/// in any position is treated as a wildcard. Example:
///
/// ```text
/// <http://example.org/alice> ? ?
/// ? <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?
/// ? ? "Alice"@en
/// ```
///
/// Literal values in the query are in N-Triples syntax and are unescaped
/// before comparison with the raw HDT dictionary bytes.
pub fn parse_query(query: &str) -> Result<(QueryTerm, QueryTerm, QueryTerm)> {
    let bytes = query.as_bytes();
    let mut pos = 0;
    let mut terms: Vec<QueryTerm> = Vec::with_capacity(3);

    while terms.len() < 3 {
        // Skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }

        let term = parse_one_term(bytes, &mut pos)
            .with_context(|| format!("Failed to parse term {} in query", terms.len() + 1))?;
        terms.push(term);
    }

    // Skip trailing whitespace and verify nothing left
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if pos < bytes.len() {
        bail!(
            "Unexpected trailing content in query after 3 terms: {:?}",
            &query[pos..]
        );
    }

    if terms.len() != 3 {
        bail!(
            "Query must have exactly 3 terms (got {}): {:?}",
            terms.len(),
            query
        );
    }

    let o = terms.pop().unwrap();
    let p = terms.pop().unwrap();
    let s = terms.pop().unwrap();
    Ok((s, p, o))
}

fn parse_one_term(bytes: &[u8], pos: &mut usize) -> Result<QueryTerm> {
    match bytes[*pos] {
        b'?' | b'*' => {
            *pos += 1;
            Ok(QueryTerm::Wildcard)
        }
        b'<' => {
            // IRI: read until matching '>'
            let start = *pos + 1;
            *pos += 1;
            while *pos < bytes.len() && bytes[*pos] != b'>' {
                *pos += 1;
            }
            if *pos >= bytes.len() {
                bail!("Unclosed IRI '<' in query");
            }
            let iri = bytes[start..*pos].to_vec();
            *pos += 1; // consume '>'
            Ok(QueryTerm::Bound(iri))
        }
        b'_' if bytes.get(*pos + 1) == Some(&b':') => {
            // Blank node: read until whitespace
            let start = *pos;
            while *pos < bytes.len() && !bytes[*pos].is_ascii_whitespace() {
                *pos += 1;
            }
            Ok(QueryTerm::Bound(bytes[start..*pos].to_vec()))
        }
        b'"' => {
            // Literal: parse N-Triples literal and unescape the value
            parse_nt_literal(bytes, pos)
        }
        b => bail!(
            "Unexpected character {:?} in query at position {}",
            b as char,
            pos
        ),
    }
}

/// Parse an N-Triples literal at `bytes[*pos..]`, advance `*pos` past it,
/// and return the HDT dictionary representation (with unescaped raw value).
///
/// N-Triples syntax: `"value"`, `"value"@lang`, `"value"^^<type>`.
/// The value portion uses N-Triples escape sequences that we unescape.
fn parse_nt_literal(bytes: &[u8], pos: &mut usize) -> Result<QueryTerm> {
    debug_assert_eq!(bytes[*pos], b'"');

    // Parse the value, unescaping N-Triples escape sequences.
    let mut value: Vec<u8> = Vec::new();
    *pos += 1; // skip opening '"'

    loop {
        if *pos >= bytes.len() {
            bail!("Unterminated literal in query");
        }
        match bytes[*pos] {
            b'"' => {
                *pos += 1; // consume closing '"'
                break;
            }
            b'\\' => {
                *pos += 1;
                if *pos >= bytes.len() {
                    bail!("Unterminated escape sequence in literal");
                }
                let escaped = match bytes[*pos] {
                    b'\\' => b'\\',
                    b'"' => b'"',
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'b' => 0x08,
                    b'f' => 0x0C,
                    b'u' => {
                        *pos += 1;
                        let hex = parse_hex_digits(bytes, pos, 4)?;
                        push_codepoint(&mut value, hex)?;
                        continue;
                    }
                    b'U' => {
                        *pos += 1;
                        let hex = parse_hex_digits(bytes, pos, 8)?;
                        push_codepoint(&mut value, hex)?;
                        continue;
                    }
                    c => bail!("Unknown escape sequence \\{}", c as char),
                };
                value.push(escaped);
                *pos += 1;
            }
            b => {
                value.push(b);
                *pos += 1;
            }
        }
    }

    // Check for suffix: @lang or ^^<type>
    let mut suffix: Vec<u8> = Vec::new();
    if *pos < bytes.len() && bytes[*pos] == b'@' {
        // Language tag
        suffix.push(b'@');
        *pos += 1;
        while *pos < bytes.len()
            && (bytes[*pos].is_ascii_alphanumeric() || bytes[*pos] == b'-')
        {
            suffix.push(bytes[*pos]);
            *pos += 1;
        }
    } else if *pos + 1 < bytes.len() && bytes[*pos] == b'^' && bytes[*pos + 1] == b'^' {
        // Datatype IRI
        suffix.extend_from_slice(b"^^");
        *pos += 2;
        if *pos >= bytes.len() || bytes[*pos] != b'<' {
            bail!("Expected '<' after '^^' in typed literal");
        }
        suffix.push(b'<');
        *pos += 1;
        while *pos < bytes.len() && bytes[*pos] != b'>' {
            suffix.push(bytes[*pos]);
            *pos += 1;
        }
        if *pos >= bytes.len() {
            bail!("Unclosed datatype IRI in literal");
        }
        suffix.push(b'>');
        *pos += 1; // consume '>'
    }

    // Reconstruct HDT literal: "raw_value" + optional suffix
    let mut hdt_literal: Vec<u8> = Vec::with_capacity(1 + value.len() + 1 + suffix.len());
    hdt_literal.push(b'"');
    hdt_literal.extend_from_slice(&value);
    hdt_literal.push(b'"');
    hdt_literal.extend_from_slice(&suffix);

    Ok(QueryTerm::Bound(hdt_literal))
}

fn parse_hex_digits(bytes: &[u8], pos: &mut usize, n: usize) -> Result<u32> {
    if *pos + n > bytes.len() {
        bail!("Truncated Unicode escape sequence");
    }
    let hex_str = std::str::from_utf8(&bytes[*pos..*pos + n])
        .map_err(|_| anyhow::anyhow!("Non-UTF-8 in Unicode escape"))?;
    let value = u32::from_str_radix(hex_str, 16)
        .with_context(|| format!("Invalid hex digits in Unicode escape: {hex_str}"))?;
    *pos += n;
    Ok(value)
}

fn push_codepoint(buf: &mut Vec<u8>, codepoint: u32) -> Result<()> {
    let ch = char::from_u32(codepoint)
        .with_context(|| format!("Invalid Unicode codepoint: U+{codepoint:04X}"))?;
    let mut tmp = [0u8; 4];
    buf.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
    Ok(())
}

// ---------------------------------------------------------------------------
// Main search function
// ---------------------------------------------------------------------------

/// Search an HDT file for triples matching a triple pattern.
///
/// - `query`: space-separated triple pattern in N-Triples syntax (`?` as wildcard)
/// - `output`: `None` = stdout, `Some(path)` = write to file
/// - `count_only`: if true, emit only the count (nothing to stdout except the number)
/// - `limit`: stop after this many results (`None` = no limit)
/// - `memory_limit`: budget for the PFC block caches
///
/// Returns the count of matching triples.
pub fn search_hdt_streaming(
    hdt_path: &Path,
    query: &str,
    output: Option<&Path>,
    count_only: bool,
    limit: Option<u64>,
    memory_limit: usize,
) -> Result<u64> {
    let (s_term, p_term, o_term) =
        parse_query(query).with_context(|| format!("Invalid query: {query:?}"))?;

    let kind = PatternKind::from_terms(&s_term, &p_term, &o_term);

    // Phase 1 only covers patterns that don't need an index.
    match kind {
        PatternKind::PredicateBound | PatternKind::ObjectBound | PatternKind::PredicateObjectBound => {
            bail!(
                "Pattern {:?} requires an index file. Run `hdtc index <HDT_FILE>` first, \
                 then retry (index-based queries are coming in Phase 2/3).",
                kind
            );
        }
        _ => {}
    }

    let (offsets, mut dictionary) = open_hdt(hdt_path, memory_limit)
        .with_context(|| format!("Failed to open HDT file {}", hdt_path.display()))?;

    // Resolve bound terms to dictionary IDs. If any bound term is not found,
    // the result set is empty — return immediately (not an error).
    let s_id: Option<u64> = match &s_term {
        QueryTerm::Wildcard => None,
        QueryTerm::Bound(bytes) => match dictionary.locate_subject(bytes)? {
            Some(id) => Some(id),
            None => return Ok(0), // not found → zero results
        },
    };

    let p_id: Option<u64> = match &p_term {
        QueryTerm::Wildcard => None,
        QueryTerm::Bound(bytes) => match dictionary.locate_predicate(bytes)? {
            Some(id) => Some(id),
            None => return Ok(0),
        },
    };

    let o_id: Option<u64> = match &o_term {
        QueryTerm::Wildcard => None,
        QueryTerm::Bound(bytes) => match dictionary.locate_object(bytes)? {
            Some(id) => Some(id),
            None => return Ok(0),
        },
    };

    let mut scanner = BitmapTriplesScanner::new(&offsets, hdt_path)
        .context("Failed to create BitmapTriples scanner")?;

    // Open the output destination once; --count writes the count here too.
    let mut writer = crate::hdt::reader::make_writer(output)?;

    let mut count = 0u64;
    let mut subject_buf = Vec::new();
    let mut predicate_buf = Vec::new();
    let mut object_buf = Vec::new();

    // Only resolve subject/predicate when the ID changes — avoids redundant
    // dictionary lookups for the common case of many triples per subject.
    // In SPO order, consecutive triples frequently share subject and predicate.
    let mut prev_s = 0u64;
    let mut prev_p = 0u64;

    while let Some((s, p, o)) = scanner.next_triple()? {
        // Subject-bound early exit: once we pass the target subject, stop.
        if let Some(target_s) = s_id {
            if s > target_s {
                break;
            }
            if s < target_s {
                continue;
            }
        }

        // Apply predicate and object filters.
        if let Some(target_p) = p_id
            && p != target_p
        {
            continue;
        }
        if let Some(target_o) = o_id
            && o != target_o
        {
            continue;
        }

        // Match found.
        count += 1;

        if !count_only {
            if s != prev_s {
                dictionary.subject_term(s, &mut subject_buf)?;
                prev_s = s;
            }
            if p != prev_p {
                dictionary.predicate_term(p, &mut predicate_buf)?;
                prev_p = p;
            }
            dictionary.object_term(o, &mut object_buf)?;
            write_triple_tab(&mut writer, &subject_buf, &predicate_buf, &object_buf)?;
        }

        if let Some(lim) = limit
            && count >= lim
        {
            break;
        }
    }

    if count_only {
        writeln!(writer, "{count}")?;
    }
    writer.flush()?;

    Ok(count)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_wildcard_query() {
        let (s, p, o) = parse_query("? ? ?").unwrap();
        assert_eq!(s, QueryTerm::Wildcard);
        assert_eq!(p, QueryTerm::Wildcard);
        assert_eq!(o, QueryTerm::Wildcard);
    }

    #[test]
    fn test_parse_star_wildcard() {
        let (s, p, o) = parse_query("* * *").unwrap();
        assert_eq!(s, QueryTerm::Wildcard);
        assert_eq!(p, QueryTerm::Wildcard);
        assert_eq!(o, QueryTerm::Wildcard);
    }

    #[test]
    fn test_parse_iri_subject() {
        let (s, p, o) = parse_query("<http://example.org/alice> ? ?").unwrap();
        assert_eq!(
            s,
            QueryTerm::Bound(b"http://example.org/alice".to_vec())
        );
        assert_eq!(p, QueryTerm::Wildcard);
        assert_eq!(o, QueryTerm::Wildcard);
    }

    #[test]
    fn test_parse_iri_predicate() {
        let (s, p, o) =
            parse_query("? <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?").unwrap();
        assert_eq!(s, QueryTerm::Wildcard);
        assert_eq!(
            p,
            QueryTerm::Bound(
                b"http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_vec()
            )
        );
        assert_eq!(o, QueryTerm::Wildcard);
    }

    #[test]
    fn test_parse_simple_literal_object() {
        let (s, p, o) = parse_query("? ? \"Alice\"").unwrap();
        assert_eq!(s, QueryTerm::Wildcard);
        assert_eq!(p, QueryTerm::Wildcard);
        assert_eq!(o, QueryTerm::Bound(b"\"Alice\"".to_vec()));
    }

    #[test]
    fn test_parse_lang_tagged_literal() {
        let (s, p, o) = parse_query("? ? \"Alice\"@en").unwrap();
        assert_eq!(o, QueryTerm::Bound(b"\"Alice\"@en".to_vec()));
        assert_eq!(s, QueryTerm::Wildcard);
        assert_eq!(p, QueryTerm::Wildcard);
    }

    #[test]
    fn test_parse_typed_literal() {
        let (s, p, o) =
            parse_query("? ? \"42\"^^<http://www.w3.org/2001/XMLSchema#integer>").unwrap();
        assert_eq!(
            o,
            QueryTerm::Bound(
                b"\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_vec()
            )
        );
        assert_eq!(s, QueryTerm::Wildcard);
        assert_eq!(p, QueryTerm::Wildcard);
    }

    #[test]
    fn test_parse_literal_with_escape() {
        // "\n" in the query should be unescaped to a real newline in the HDT bytes
        let (_, _, o) = parse_query("? ? \"line1\\nline2\"").unwrap();
        assert_eq!(o, QueryTerm::Bound(b"\"line1\nline2\"".to_vec()));
    }

    #[test]
    fn test_parse_blank_node() {
        let (s, p, o) = parse_query("_:b0 ? ?").unwrap();
        assert_eq!(s, QueryTerm::Bound(b"_:b0".to_vec()));
        assert_eq!(p, QueryTerm::Wildcard);
        assert_eq!(o, QueryTerm::Wildcard);
    }

    #[test]
    fn test_parse_all_bound() {
        let (s, p, o) = parse_query(
            "<http://example.org/s> <http://example.org/p> <http://example.org/o>",
        )
        .unwrap();
        assert_eq!(s, QueryTerm::Bound(b"http://example.org/s".to_vec()));
        assert_eq!(p, QueryTerm::Bound(b"http://example.org/p".to_vec()));
        assert_eq!(o, QueryTerm::Bound(b"http://example.org/o".to_vec()));
    }

    #[test]
    fn test_parse_too_few_terms() {
        assert!(parse_query("? ?").is_err());
    }

    #[test]
    fn test_parse_too_many_terms() {
        assert!(parse_query("? ? ? ?").is_err());
    }

    #[test]
    fn test_pattern_kind_scan_all() {
        let (s, p, o) = parse_query("? ? ?").unwrap();
        assert_eq!(PatternKind::from_terms(&s, &p, &o), PatternKind::ScanAll);
    }

    #[test]
    fn test_pattern_kind_subject_bound() {
        let (s, p, o) = parse_query("<http://example.org/s> ? ?").unwrap();
        assert_eq!(
            PatternKind::from_terms(&s, &p, &o),
            PatternKind::SubjectBound
        );
    }

    #[test]
    fn test_pattern_kind_sp_bound() {
        let (s, p, o) =
            parse_query("<http://example.org/s> <http://example.org/p> ?").unwrap();
        assert_eq!(
            PatternKind::from_terms(&s, &p, &o),
            PatternKind::SubjectPredicateBound
        );
    }

    #[test]
    fn test_pattern_kind_exact() {
        let (s, p, o) = parse_query(
            "<http://example.org/s> <http://example.org/p> <http://example.org/o>",
        )
        .unwrap();
        assert_eq!(PatternKind::from_terms(&s, &p, &o), PatternKind::Exact);
    }

    #[test]
    fn test_pattern_kind_predicate_bound() {
        let (s, p, o) = parse_query("? <http://example.org/p> ?").unwrap();
        assert_eq!(
            PatternKind::from_terms(&s, &p, &o),
            PatternKind::PredicateBound
        );
    }
}
