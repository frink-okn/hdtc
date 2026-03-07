//! HDT triple-pattern search engine.
//!
//! Supports the following query patterns without an index file:
//!
//! - `???`  — scan all triples
//! - `S??`  — subject bound
//! - `SP?`  — subject + predicate bound
//! - `S?O`  — subject + object bound
//! - `SPO`  — exact triple lookup
//!
//! Supports with a `.hdt.index.v1-1` sidecar index:
//!
//! - `?P?`  — predicate bound, uses predicateIndex
//! - `??O`  — object bound, uses bitmapIndexZ + indexZ
//! - `?PO`  — predicate + object bound, auto-routes via selectivity

use crate::hdt::index_reader::{
    bitmap_index_z_group_stats, open_index, open_index_section, read_index_z_range,
    read_predicate_count, ObjectGroupStats,
};
use crate::hdt::reader::{BitmapTriplesScanner, DictionaryResolver, HdtSectionOffsets, open_hdt, write_triple_tab};
use crate::io::{StreamingBitmapDecoder, StreamingLogArrayDecoder};
use anyhow::{Context, Result, bail};
use std::fs::File;
use std::io::{BufReader, SeekFrom, Write};
use std::io::Seek;
use std::path::{Path, PathBuf};

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
    /// `?P?` — predicate bound only (requires index).
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
// Index path resolution
// ---------------------------------------------------------------------------

fn resolve_index_path(hdt_path: &Path, explicit: Option<&Path>) -> PathBuf {
    if let Some(p) = explicit {
        p.to_path_buf()
    } else {
        hdt_path.with_extension("hdt.index.v1-1")
    }
}

fn write_zero_count_if_needed(output: Option<&Path>, count_only: bool) -> Result<()> {
    if !count_only {
        return Ok(());
    }
    let mut writer = crate::hdt::reader::make_writer(output)?;
    writeln!(writer, "0")?;
    writer.flush()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// ?P? search using predicate index
// ---------------------------------------------------------------------------

/// Search for all triples matching `? <pred_id> ?` using the predicate index.
///
/// Opens five streaming decoders simultaneously — `predicateIndex.bitmap`,
/// `predicateIndex.seq`, `BitmapY`, `BitmapZ`, and `ArrayZ` — and interleaves
/// their reads in a single forward pass.  No intermediate buffer proportional
/// to the predicate's frequency is allocated; memory is O(1).
///
/// The predIndex group for `pred_id` is scanned entry by entry.  Each `pos_y`
/// value obtained from `predicateIndex.seq` is used immediately to advance
/// BitmapY/BitmapZ/ArrayZ and emit the matching object triples before
/// moving on to the next predIndex entry.
///
/// When `object_filter` is set, only triples whose object matches are emitted.
/// Objects within each (S,P) pair are sorted in SPO order, so once we see an
/// object greater than the target, the rest of that pair can be skipped.
///
/// Writes triples to `writer` unless `count_only` is true. Returns the triple count.
#[allow(clippy::too_many_arguments)]
fn search_predicate_bound(
    hdt_path: &Path,
    index_path: &Path,
    pred_id: u64,
    object_filter: Option<u64>,
    offsets: &HdtSectionOffsets,
    dictionary: &mut DictionaryResolver,
    writer: &mut crate::hdt::reader::OutputWriter,
    count_only: bool,
    offset: Option<u64>,
    limit: Option<u64>,
) -> Result<u64> {
    let idx = open_index(index_path)
        .with_context(|| format!("Failed to read index file {}", index_path.display()))?;

    let mut p_buf = Vec::new();
    let mut predicate_resolved = false;

    let open_at = |section_offset: u64| -> Result<BufReader<File>> {
        let mut f = File::open(hdt_path)
            .with_context(|| format!("Failed to open {}", hdt_path.display()))?;
        f.seek(SeekFrom::Start(section_offset))?;
        Ok(BufReader::with_capacity(256 * 1024, f))
    };

    // Open all five streaming decoders. ArrayY is not needed — we already know
    // the predicate ID. predicateIndex.bitmap/seq guide the BitmapY/BitmapZ/ArrayZ scan.
    let mut pred_bitmap = StreamingBitmapDecoder::new(
        open_index_section(index_path, idx.pred_bitmap_start)?,
    )
    .context("Failed to open predicateIndex.bitmap decoder")?;

    let mut pred_seq = StreamingLogArrayDecoder::new(
        open_index_section(index_path, idx.pred_seq_start)?,
    )
    .context("Failed to open predicateIndex.seq decoder")?;

    let mut bitmap_y = StreamingBitmapDecoder::new(open_at(offsets.by_start)?)
        .context("Failed to open BitmapY decoder")?;
    let mut bitmap_z = StreamingBitmapDecoder::new(open_at(offsets.bz_start)?)
        .context("Failed to open BitmapZ decoder")?;
    let mut array_z = StreamingLogArrayDecoder::new(open_at(offsets.az_start)?)
        .context("Failed to open ArrayZ decoder")?;

    // Predicate groups in predicateIndex are 1-based and delimited by 1-bits.
    // Scan forward through groups 1..(pred_id-1), then process group pred_id.
    let mut groups_passed = 0u64;
    let mut in_target = pred_id == 1;
    let mut by_pos = 0u64; // (S,P) pairs consumed so far in BitmapY
    let mut subject = 1u64;
    let mut count = 0u64;
    let mut remaining_offset = offset.unwrap_or(0);
    let mut s_buf = Vec::new();
    let mut o_buf = Vec::new();
    let mut prev_s = 0u64;

    'scan: loop {
        let Some(pred_bit) = pred_bitmap.next_bit()? else {
            break;
        };
        let Some(pos_y) = pred_seq.next_entry()? else {
            break;
        };

        if !in_target {
            // Still scanning pre-target groups; a 1-bit marks the end of a group.
            if pred_bit {
                groups_passed += 1;
                if groups_passed == pred_id - 1 {
                    in_target = true;
                }
            }
            continue;
        }

        // We're in predicate P's group.  Advance BitmapY + BitmapZ from
        // `by_pos` to `pos_y`, skipping all objects for intervening (S,P) pairs.
        while by_pos < pos_y {
            loop {
                let bz_bit = bitmap_z.next_bit()?.with_context(|| {
                    format!("BitmapZ ended early skipping to pos_y {pos_y} (at {by_pos})")
                })?;
                array_z.next_entry()?.with_context(|| {
                    format!("ArrayZ ended early skipping to pos_y {pos_y} (at {by_pos})")
                })?;
                if bz_bit {
                    break;
                }
            }
            let by_bit = bitmap_y.next_bit()?.with_context(|| {
                format!("BitmapY ended early skipping to pos_y {pos_y} (at {by_pos})")
            })?;
            if by_bit {
                subject += 1;
            }
            by_pos += 1;
        }

        // Emit all objects for the (S,P) pair at `pos_y`.
        loop {
            let object = array_z
                .next_entry()?
                .with_context(|| format!("ArrayZ ended early at target pos_y {pos_y}"))?;
            let bz_bit = bitmap_z
                .next_bit()?
                .with_context(|| format!("BitmapZ ended early at target pos_y {pos_y}"))?;

            // Object filter for ?PO via ?P? path: objects are sorted within
            // each (S,P) pair, so skip past target = done with this pair.
            if let Some(target_obj) = object_filter {
                if object > target_obj {
                    // Past target in sorted order — consume rest of this pair
                    if !bz_bit {
                        loop {
                            array_z.next_entry()?;
                            if bitmap_z.next_bit()?.unwrap_or(true) {
                                break;
                            }
                        }
                    }
                    break; // next pos_y
                }
                if object != target_obj {
                    if bz_bit {
                        break;
                    }
                    continue;
                }
            }

            if remaining_offset > 0 {
                remaining_offset -= 1;
                if bz_bit {
                    break;
                }
                continue;
            }

            count += 1;
            if !count_only {
                if !predicate_resolved {
                    dictionary
                        .predicate_term(pred_id, &mut p_buf)
                        .with_context(|| format!("Failed to resolve predicate ID {pred_id}"))?;
                    predicate_resolved = true;
                }
                if subject != prev_s {
                    dictionary
                        .subject_term(subject, &mut s_buf)
                        .with_context(|| format!("Failed to resolve subject ID {subject}"))?;
                    prev_s = subject;
                }
                dictionary
                    .object_term(object, &mut o_buf)
                    .with_context(|| format!("Failed to resolve object ID {object}"))?;
                write_triple_tab(writer, &s_buf, &p_buf, &o_buf)?;
            }

            if let Some(lim) = limit
                && count >= lim
            {
                break 'scan;
            }

            if bz_bit {
                break;
            }
        }

        // Consume the BitmapY bit for `pos_y` to keep the subject counter current.
        let by_bit = bitmap_y.next_bit()?.with_context(|| {
            format!("BitmapY ended early after emitting pos_y {pos_y}")
        })?;
        if by_bit {
            subject += 1;
        }
        by_pos += 1;

        if pred_bit {
            break 'scan; // end of predicate P's group
        }
    }

    Ok(count)
}

// ---------------------------------------------------------------------------
// ??O search using object index
// ---------------------------------------------------------------------------

/// Search for all triples matching `? ? <obj_id>` using the object index.
///
/// Locates object-group boundaries from `bitmapIndexZ`, then streams indexZ
/// entries in bounded chunks. For each entry, it performs coordinated scans
/// over BitmapY + ArrayY to recover (subject, predicate) pairs.
///
/// `indexZ` is in OPS order within each object group, so `pos_y` may decrease
/// across predicate boundaries. When that happens, BitmapY/ArrayY decoders are
/// reset and rescanned forward. This keeps memory bounded without materializing
/// or sorting entire object groups in memory.
///
/// Optionally filters by predicate when `pred_filter` is set (for `?PO`
/// via the ??O path).
///
/// Writes triples to `writer` unless `count_only` is true. Returns the triple count.
#[allow(clippy::too_many_arguments)]
fn search_object_bound(
    hdt_path: &Path,
    index_path: &Path,
    obj_id: u64,
    subject_filter: Option<u64>,
    pred_filter: Option<u64>,
    offsets: &HdtSectionOffsets,
    dictionary: &mut DictionaryResolver,
    writer: &mut crate::hdt::reader::OutputWriter,
    count_only: bool,
    offset: Option<u64>,
    limit: Option<u64>,
    memory_limit: usize,
    precomputed_group: Option<ObjectGroupStats>,
) -> Result<u64> {
    let idx = open_index(index_path)
        .with_context(|| format!("Failed to read index file {}", index_path.display()))?;

    // Step 1–2: Find object group boundaries in bitmapIndexZ.
    let group = match precomputed_group {
        Some(g) => g,
        None => match bitmap_index_z_group_stats(index_path, idx.bitmap_index_z_start, obj_id)? {
            Some(g) => g,
            None => return Ok(0), // object group doesn't exist
        },
    };

    // Step 3: Open BitmapY + ArrayY streaming decoders.
    // Does NOT need BitmapZ or ArrayZ — the object is already known and
    // each pos_y identifies a unique (S,P) pair.
    let open_at = |section_offset: u64| -> Result<BufReader<File>> {
        let mut f = File::open(hdt_path)
            .with_context(|| format!("Failed to open {}", hdt_path.display()))?;
        f.seek(SeekFrom::Start(section_offset))?;
        Ok(BufReader::with_capacity(256 * 1024, f))
    };

    let mut bitmap_y = StreamingBitmapDecoder::new(open_at(offsets.by_start)?)
        .context("Failed to open BitmapY decoder")?;
    let mut array_y = StreamingLogArrayDecoder::new(open_at(offsets.ay_start)?)
        .context("Failed to open ArrayY decoder")?;

    // Step 4: Coordinated streaming scan over indexZ entries.
    // indexZ is OPS-ordered within each object group, so pos_y can decrease
    // when predicate changes. We process in chunks and reset the forward scan
    // only when necessary, keeping memory bounded by chunk size.
    //
    // PERF: Each reset rescans BitmapY from the start up to the new pos_y.
    // For a group with K predicate sub-groups, there are K-1 resets; worst-case
    // work is O(K × n_sp / 8) bytes. The alternative — load all pos_y values,
    // sort them, do one forward scan — uses O(group_size) memory, which violates
    // the streaming budget for large groups. For typical data the predicate
    // sub-groups are large enough that resets are rare.
    let entries_per_chunk = (memory_limit / std::mem::size_of::<u64>()).clamp(4096, 262_144) as u64;

    let mut by_pos = 0u64;
    let mut subject = 1u64;
    let mut count = 0u64;
    let mut remaining_offset = offset.unwrap_or(0);
    let mut s_buf = Vec::new();
    let mut p_buf = Vec::new();
    let mut o_buf = Vec::new();
    let mut prev_s = 0u64;
    let mut o_resolved = false;
    let mut read_offset = 0u64;

    'scan: while read_offset < group.size {
        let chunk_len = (group.size - read_offset).min(entries_per_chunk);
        let entries = read_index_z_range(
            index_path,
            idx.index_z_start,
            group.start + read_offset,
            chunk_len,
        )?;
        read_offset += chunk_len;

        for target_pos_y in entries {
            if target_pos_y < by_pos {
                bitmap_y = StreamingBitmapDecoder::new(open_at(offsets.by_start)?)
                    .context("Failed to reset BitmapY decoder")?;
                array_y = StreamingLogArrayDecoder::new(open_at(offsets.ay_start)?)
                    .context("Failed to reset ArrayY decoder")?;
                by_pos = 0;
                subject = 1;
            }

            // Advance BitmapY + ArrayY from by_pos to target_pos_y
            while by_pos < target_pos_y {
                let by_bit = bitmap_y.next_bit()?.with_context(|| {
                    format!("BitmapY ended early advancing to pos_y {target_pos_y} (at {by_pos})")
                })?;
                array_y.next_entry()?.with_context(|| {
                    format!("ArrayY ended early advancing to pos_y {target_pos_y} (at {by_pos})")
                })?;
                if by_bit {
                    subject += 1;
                }
                by_pos += 1;
            }

            // Read predicate at target_pos_y
            let pred = array_y
                .next_entry()?
                .with_context(|| format!("ArrayY ended early at target pos_y {target_pos_y}"))?;

            // Read BitmapY bit at target_pos_y (consumed after emission)
            let by_bit = bitmap_y
                .next_bit()?
                .with_context(|| format!("BitmapY ended early at target pos_y {target_pos_y}"))?;
            by_pos += 1;

            // Apply optional subject filter (for S?O via ??O path)
            if let Some(target_subject) = subject_filter
                && subject != target_subject
            {
                if by_bit {
                    subject += 1;
                }
                continue;
            }

            // Apply optional predicate filter (for ?PO via ??O path)
            if let Some(target_pred) = pred_filter
                && pred != target_pred
            {
                if by_bit {
                    subject += 1;
                }
                continue;
            }

            // Handle offset
            if remaining_offset > 0 {
                remaining_offset -= 1;
                if by_bit {
                    subject += 1;
                }
                continue;
            }

            // Emit triple
            count += 1;
            if !count_only {
                if !o_resolved {
                    dictionary
                        .object_term(obj_id, &mut o_buf)
                        .with_context(|| format!("Failed to resolve object ID {obj_id}"))?;
                    o_resolved = true;
                }
                if subject != prev_s {
                    dictionary
                        .subject_term(subject, &mut s_buf)
                        .with_context(|| format!("Failed to resolve subject ID {subject}"))?;
                    prev_s = subject;
                }
                dictionary
                    .predicate_term(pred, &mut p_buf)
                    .with_context(|| format!("Failed to resolve predicate ID {pred}"))?;
                write_triple_tab(writer, &s_buf, &p_buf, &o_buf)?;
            }

            if let Some(lim) = limit
                && count >= lim
            {
                break 'scan;
            }

            if by_bit {
                subject += 1;
            }
        }
    }

    Ok(count)
}

// ---------------------------------------------------------------------------
// Main search function
// ---------------------------------------------------------------------------

/// Search an HDT file for triples matching a triple pattern.
///
/// - `query`: space-separated triple pattern in N-Triples syntax (`?` as wildcard)
/// - `output`: `None` = stdout, `Some(path)` = write to file
/// - `count_only`: if true, emit only the count (nothing to stdout except the number)
/// - `limit`: stop after this many results (`None` = no limit; ignored when `count_only`)
/// - `offset`: skip this many matching results before emitting/counting
/// - `memory_limit`: budget for the PFC block caches
/// - `index_path`: explicit index file path; `None` = auto-derive as `<hdt>.hdt.index.v1-1`
/// - `no_index`: if true, skip the index and fall back to sequential scan for all patterns
///
/// Returns the count of matching triples.
#[allow(clippy::too_many_arguments)]
pub fn search_hdt_streaming(
    hdt_path: &Path,
    query: &str,
    output: Option<&Path>,
    count_only: bool,
    limit: Option<u64>,
    offset: Option<u64>,
    memory_limit: usize,
    index_path: Option<&Path>,
    no_index: bool,
) -> Result<u64> {
    let (s_term, p_term, o_term) =
        parse_query(query).with_context(|| format!("Invalid query: {query:?}"))?;

    let kind = PatternKind::from_terms(&s_term, &p_term, &o_term);

    let (offsets, mut dictionary) = open_hdt(hdt_path, memory_limit)
        .with_context(|| format!("Failed to open HDT file {}", hdt_path.display()))?;

    // Resolve bound terms to dictionary IDs. If any bound term is not found,
    // the result set is empty — return immediately (not an error).
    let s_id: Option<u64> = match &s_term {
        QueryTerm::Wildcard => None,
        QueryTerm::Bound(bytes) => match dictionary.locate_subject(bytes)? {
            Some(id) => Some(id),
            None => {
                write_zero_count_if_needed(output, count_only)?;
                return Ok(0); // not found → zero results
            }
        },
    };

    let p_id: Option<u64> = match &p_term {
        QueryTerm::Wildcard => None,
        QueryTerm::Bound(bytes) => match dictionary.locate_predicate(bytes)? {
            Some(id) => Some(id),
            None => {
                write_zero_count_if_needed(output, count_only)?;
                return Ok(0);
            }
        },
    };

    let o_id: Option<u64> = match &o_term {
        QueryTerm::Wildcard => None,
        QueryTerm::Bound(bytes) => match dictionary.locate_object(bytes)? {
            Some(id) => Some(id),
            None => {
                write_zero_count_if_needed(output, count_only)?;
                return Ok(0);
            }
        },
    };

    // Phase 2: predicate-bound query — use the predicate index when available.
    if kind == PatternKind::PredicateBound && !no_index {
        let pred_id = p_id.expect("p_id must be set for PredicateBound");
        let eff_index = resolve_index_path(hdt_path, index_path);

        if !eff_index.exists() {
            bail!(
                "Pattern ?P? requires an index file.\n\
                 Expected: {}\n\
                 Run `hdtc index {}` to create one, \
                 or pass `--no-index` to fall back to a sequential scan.",
                eff_index.display(),
                hdt_path.display()
            );
        }

        let mut writer = crate::hdt::reader::make_writer(output)?;
        let count = search_predicate_bound(
            hdt_path,
            &eff_index,
            pred_id,
            None, // no object filter for plain ?P?
            &offsets,
            &mut dictionary,
            &mut writer,
            count_only,
            offset,
            limit,
        )?;
        if count_only {
            writeln!(writer, "{count}")?;
        }
        writer.flush()?;
        return Ok(count);
    }

    // Object-bound query — use the object index when available.
    if kind == PatternKind::ObjectBound && !no_index {
        let obj_id = o_id.expect("o_id must be set for ObjectBound");
        let eff_index = resolve_index_path(hdt_path, index_path);

        if !eff_index.exists() {
            bail!(
                "Pattern ??O requires an index file.\n\
                 Expected: {}\n\
                 Run `hdtc index {}` to create one, \
                 or pass `--no-index` to fall back to a sequential scan.",
                eff_index.display(),
                hdt_path.display()
            );
        }

        let mut writer = crate::hdt::reader::make_writer(output)?;
        let count = search_object_bound(
            hdt_path,
            &eff_index,
            obj_id,
            None,
            None, // no predicate filter for plain ??O
            &offsets,
            &mut dictionary,
            &mut writer,
            count_only,
            offset,
            limit,
            memory_limit,
            None,
        )?;
        if count_only {
            writeln!(writer, "{count}")?;
        }
        writer.flush()?;
        return Ok(count);
    }

    // Predicate+object-bound query — route via ??O or ?P? based on selectivity.
    if kind == PatternKind::PredicateObjectBound && !no_index {
        let pred_id = p_id.expect("p_id must be set for PredicateObjectBound");
        let obj_id = o_id.expect("o_id must be set for PredicateObjectBound");
        let eff_index = resolve_index_path(hdt_path, index_path);

        if !eff_index.exists() {
            bail!(
                "Pattern ?PO requires an index file.\n\
                 Expected: {}\n\
                 Run `hdtc index {}` to create one, \
                 or pass `--no-index` to fall back to a sequential scan.",
                eff_index.display(),
                hdt_path.display()
            );
        }

        let idx = open_index(&eff_index)?;
        let obj_group = bitmap_index_z_group_stats(&eff_index, idx.bitmap_index_z_start, obj_id)?;
        let Some(obj_group) = obj_group else {
            write_zero_count_if_needed(output, count_only)?;
            return Ok(0);
        };

        // Selectivity routing: compare estimated work by `count(P)` vs `count(O)`.
        // - `count(P)` comes from predicateCount (SP-pair count for predicate).
        // - `count(O)` is the object group size in bitmapIndexZ/indexZ.
        let use_predicate_path = {
            match read_predicate_count(&eff_index, idx.pred_count_start) {
                Ok(pred_count) => {
                    let n_predicates = pred_count.len();
                    if n_predicates > 0 {
                        let count_p = pred_count.get(pred_id - 1);
                        // Strict less-than: when counts are equal, the object
                        // path opens fewer decoders (BitmapY+ArrayY vs five),
                        // so prefer it in the tie case.
                        count_p < obj_group.size
                    } else {
                        obj_group.size > 0
                    }
                }
                Err(_) => false, // default to ??O path
            }
        };

        let mut writer = crate::hdt::reader::make_writer(output)?;
        let count = if use_predicate_path {
            search_predicate_bound(
                hdt_path,
                &eff_index,
                pred_id,
                Some(obj_id),
                &offsets,
                &mut dictionary,
                &mut writer,
                count_only,
                offset,
                limit,
            )?
        } else {
            search_object_bound(
                hdt_path,
                &eff_index,
                obj_id,
                None,
                Some(pred_id),
                &offsets,
                &mut dictionary,
                &mut writer,
                count_only,
                offset,
                limit,
                memory_limit,
                Some(obj_group),
            )?
        };
        if count_only {
            writeln!(writer, "{count}")?;
        }
        writer.flush()?;
        return Ok(count);
    }

    // Subject+object-bound query — default S-bound scan, but if object group is
    // tiny the object-index path can be cheaper.
    if kind == PatternKind::SubjectObjectBound && !no_index {
        let subject_id = s_id.expect("s_id must be set for SubjectObjectBound");
        let obj_id = o_id.expect("o_id must be set for SubjectObjectBound");
        let eff_index = resolve_index_path(hdt_path, index_path);

        if eff_index.exists() {
            let idx = open_index(&eff_index)?;
            if let Some(obj_group) =
                bitmap_index_z_group_stats(&eff_index, idx.bitmap_index_z_start, obj_id)?
            {
                // Keep this conservative: only route through ??O path when object
                // fanout is small enough to avoid expensive repeated rescans.
                if obj_group.size <= 4096 {
                    let mut writer = crate::hdt::reader::make_writer(output)?;
                    let count = search_object_bound(
                        hdt_path,
                        &eff_index,
                        obj_id,
                        Some(subject_id),
                        None,
                        &offsets,
                        &mut dictionary,
                        &mut writer,
                        count_only,
                        offset,
                        limit,
                        memory_limit,
                        Some(obj_group),
                    )?;
                    if count_only {
                        writeln!(writer, "{count}")?;
                    }
                    writer.flush()?;
                    return Ok(count);
                }
            }
        }
    }

    // For index-using patterns with --no-index, or S-bound/scan-all patterns:
    // fall through to the sequential scan below.

    let mut scanner = BitmapTriplesScanner::new(&offsets, hdt_path)
        .context("Failed to create BitmapTriples scanner")?;

    // Open the output destination once; --count writes the count here too.
    let mut writer = crate::hdt::reader::make_writer(output)?;

    let mut count = 0u64;
    let mut remaining_offset = offset.unwrap_or(0);
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

        if remaining_offset > 0 {
            remaining_offset -= 1;
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
