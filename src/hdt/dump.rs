use crate::io::crc_utils::crc8;
use crate::io::{
    ControlInfo, ControlType, LogArrayReader, StreamingBitmapDecoder, StreamingLogArrayDecoder,
    decode_vbyte, encode_vbyte, read_vbyte, skip_bitmap_section, skip_log_array_section,
};
use anyhow::{Context, Result, bail};
use oxrdfio::{RdfFormat, RdfParser};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

const PFC_SECTION_TYPE: u8 = 0x02;
const DICTIONARY_FOUR_FORMAT: &str = "<http://purl.org/HDT/hdt#dictionaryFour>";
const TRIPLES_BITMAP_FORMAT: &str = "<http://purl.org/HDT/hdt#triplesBitmap>";

pub fn dump_hdt_to_ntriples_streaming(
    hdt_path: &Path,
    output_path: &Path,
    memory_limit: usize,
) -> Result<u64> {
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
    let header_text = String::from_utf8(header_buf).context("Header content is not valid UTF-8")?;
    let num_triples = parse_num_triples_from_header(&header_text)
        .context("Failed to parse triple count from header metadata")?;

    let dict_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read dictionary control info")?;
    if dict_ci.control_type != ControlType::Dictionary {
        bail!("Expected dictionary control info");
    }
    if dict_ci.format != DICTIONARY_FOUR_FORMAT {
        bail!(
            "Unsupported dictionary format: {} (expected {})",
            dict_ci.format,
            DICTIONARY_FOUR_FORMAT
        );
    }

    // Dump's memory is almost entirely the PFC block cache — the only other
    // allocations are block-offset vectors (tens of MB) and I/O buffers (~1 MB).
    // Reserve a fixed 64 MB for those, then split the rest across 4 sections.
    const RESERVED_BYTES: usize = 64 * 1024 * 1024;
    let cache_budget_per_section = memory_limit.saturating_sub(RESERVED_BYTES) / 4;

    let mut dictionary = DictionaryResolver {
        shared: PfcSectionIndex::read_from(&mut reader, hdt_path, "shared", cache_budget_per_section)?,
        subjects: PfcSectionIndex::read_from(&mut reader, hdt_path, "subjects", cache_budget_per_section)?,
        predicates: PfcSectionIndex::read_from(&mut reader, hdt_path, "predicates", cache_budget_per_section)?,
        objects: PfcSectionIndex::read_from(&mut reader, hdt_path, "objects", cache_budget_per_section)?,
    };

    let triples_ci =
        ControlInfo::read_from(&mut reader).context("Failed to read triples control info")?;
    if triples_ci.control_type != ControlType::Triples {
        bail!(
            "Expected triples control info, found {:?}",
            triples_ci.control_type
        );
    }
    if triples_ci.format != TRIPLES_BITMAP_FORMAT {
        bail!(
            "Unsupported triples format: {} (expected {})",
            triples_ci.format,
            TRIPLES_BITMAP_FORMAT
        );
    }

    let (by_start, by_bits) = skip_bitmap_section(&mut reader).context("Failed to scan BitmapY")?;
    let (bz_start, _bz_bits) =
        skip_bitmap_section(&mut reader).context("Failed to scan BitmapZ")?;
    let (ay_start, ay_entries, _ay_bpe) =
        skip_log_array_section(&mut reader).context("Failed to scan ArrayY")?;
    let (az_start, az_entries, _az_bpe) =
        skip_log_array_section(&mut reader).context("Failed to scan ArrayZ")?;

    if az_entries != num_triples {
        bail!(
            "ArrayZ size mismatch: header has {num_triples} triples but ArrayZ has {az_entries} entries"
        );
    }
    if by_bits != ay_entries {
        bail!(
            "BitmapY/ArrayY mismatch: BitmapY has {by_bits} bits but ArrayY has {ay_entries} entries"
        );
    }

    drop(reader);

    let open_at = |offset: u64| -> Result<BufReader<File>> {
        let mut f = File::open(hdt_path)?;
        f.seek(SeekFrom::Start(offset))?;
        Ok(BufReader::with_capacity(256 * 1024, f))
    };

    let mut bitmap_y_dec = StreamingBitmapDecoder::new(open_at(by_start)?)
        .context("Failed to create BitmapY decoder")?;
    let mut bitmap_z_dec = StreamingBitmapDecoder::new(open_at(bz_start)?)
        .context("Failed to create BitmapZ decoder")?;
    let mut array_y_dec = StreamingLogArrayDecoder::new(open_at(ay_start)?)
        .context("Failed to create ArrayY decoder")?;
    let mut array_z_dec = StreamingLogArrayDecoder::new(open_at(az_start)?)
        .context("Failed to create ArrayZ decoder")?;

    let output_file = File::create(output_path)
        .with_context(|| format!("Failed to create output file {}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, output_file);

    let num_sp_pairs = ay_entries;
    let mut current_subject = 1u64;
    let mut pos_y = 0u64;
    let mut current_predicate = if num_sp_pairs > 0 {
        array_y_dec
            .next_entry()?
            .context("ArrayY unexpectedly empty")?
    } else {
        0
    };

    // Reusable buffers for term bytes — avoids per-triple allocation.
    let mut subject_buf = Vec::new();
    let mut predicate_buf = Vec::new();
    let mut object_buf = Vec::new();

    if num_triples > 0 {
        dictionary.subject_term(current_subject, &mut subject_buf)?;
    }
    if num_sp_pairs > 0 {
        dictionary.predicate_term(current_predicate, &mut predicate_buf)?;
    }

    let mut triples_written = 0u64;

    for pos_z in 0..num_triples {
        let object = array_z_dec
            .next_entry()?
            .with_context(|| format!("ArrayZ ended early at position {pos_z}"))?;

        if current_predicate == 0 {
            bail!("Invalid predicate ID 0 at triple position {pos_z}");
        }
        if object == 0 {
            bail!("Invalid object ID 0 at triple position {pos_z}");
        }

        dictionary.object_term(object, &mut object_buf)?;

        // Write subject (IRI or blank node)
        write_ntriples_subject(&mut writer, &subject_buf)?;
        writer.write_all(b" ")?;
        // Write predicate (always an IRI)
        writer.write_all(b"<")?;
        writer.write_all(&predicate_buf)?;
        writer.write_all(b"> ")?;
        // Write object (IRI, blank node, or literal)
        write_ntriples_object(&mut writer, &object_buf)?;
        writer.write_all(b" .\n")?;
        triples_written += 1;

        let bz_bit = bitmap_z_dec
            .next_bit()?
            .with_context(|| format!("BitmapZ ended early at position {pos_z}"))?;

        if bz_bit {
            let by_bit = bitmap_y_dec
                .next_bit()?
                .with_context(|| format!("BitmapY ended early at position {pos_y}"))?;

            if by_bit {
                current_subject += 1;
                if pos_z + 1 < num_triples {
                    dictionary.subject_term(current_subject, &mut subject_buf)?;
                }
            }

            pos_y += 1;
            if pos_y < num_sp_pairs {
                current_predicate = array_y_dec
                    .next_entry()?
                    .with_context(|| format!("ArrayY ended early at pos_y {pos_y}"))?;
                dictionary.predicate_term(current_predicate, &mut predicate_buf)?;
            }
        }
    }

    if pos_y != num_sp_pairs {
        bail!("Bitmap boundary count mismatch: got {pos_y}, expected {num_sp_pairs}");
    }

    bitmap_y_dec
        .finish()
        .context("BitmapY CRC verification failed")?;
    bitmap_z_dec
        .finish()
        .context("BitmapZ CRC verification failed")?;
    array_y_dec
        .finish()
        .context("ArrayY CRC verification failed")?;
    array_z_dec
        .finish()
        .context("ArrayZ CRC verification failed")?;

    writer.flush()?;

    Ok(triples_written)
}

/// Write a subject term (IRI or blank node) in N-Triples format.
fn write_ntriples_subject(w: &mut impl Write, term: &[u8]) -> std::io::Result<()> {
    if term.starts_with(b"_:") {
        w.write_all(term)
    } else {
        w.write_all(b"<")?;
        w.write_all(term)?;
        w.write_all(b">")
    }
}

/// Write an object term (IRI, blank node, or literal) in N-Triples format.
/// Literal values are escaped for N-Triples (HDT stores raw unescaped UTF-8).
fn write_ntriples_object(w: &mut impl Write, term: &[u8]) -> std::io::Result<()> {
    if term.starts_with(b"\"") {
        write_ntriples_literal(w, term)
    } else if term.starts_with(b"_:") {
        w.write_all(term)
    } else {
        w.write_all(b"<")?;
        w.write_all(term)?;
        w.write_all(b">")
    }
}

/// Write a literal in N-Triples format with proper escaping of the value portion.
///
/// HDT stores literals as: `"raw value"`, `"raw value"@lang`, or `"raw value"^^<datatype>`.
/// The value portion may contain raw `"`, `\`, newlines, etc. that must be escaped for N-Triples.
/// The datatype IRI and language tag are written verbatim (they don't need escaping).
fn write_ntriples_literal(w: &mut impl Write, term: &[u8]) -> std::io::Result<()> {
    debug_assert!(term.first() == Some(&b'"'));

    // Find where the raw value ends and the suffix begins.
    // Scan backwards for the boundary marker: "^^< (typed), "@ (lang-tagged), or final " (simple).
    let (value_end, suffix_start) = find_literal_boundary(term);
    let value = &term[1..value_end];

    w.write_all(b"\"")?;
    write_escaped_literal_value(w, value)?;
    w.write_all(b"\"")?;
    if suffix_start < term.len() {
        w.write_all(&term[suffix_start..])?;
    }
    Ok(())
}

/// Find the boundary between the raw value and the suffix in an HDT literal.
///
/// Returns `(value_end, suffix_start)` where:
/// - `value` is `term[1..value_end]` (between opening `"` and closing `"`)
/// - `suffix` is `term[suffix_start..]` (e.g. `^^<datatype>` or `@lang`, empty for simple literals)
///
/// HDT stores literal values unescaped, so embedded `"` in the value are
/// indistinguishable from the structural closing `"` in pathological cases.
/// This parser handles the three suffix forms by dispatching on the last byte:
/// - `>` → typed literal: scan backwards for `"^^<` (robust because IRIs cannot contain `<`)
/// - valid BCP-47 char → language tag: scan backwards for `[a-zA-Z0-9-]+` then verify `@"`
/// - `"` → simple literal
fn find_literal_boundary(term: &[u8]) -> (usize, usize) {
    let len = term.len();
    if len < 2 {
        return (len, len);
    }

    match term[len - 1] {
        // Typed literal: ends with `>`, look for `"^^<` scanning backwards.
        // IRIs cannot contain `<`, so the first `<` from the right with `"^^` before it
        // is unambiguous.
        b'>' => {
            let mut i = len - 2;
            while i >= 4 {
                if term[i] == b'<'
                    && term[i - 1] == b'^'
                    && term[i - 2] == b'^'
                    && term[i - 3] == b'"'
                {
                    return (i - 3, i - 2);
                }
                i -= 1;
            }
        }
        // Simple literal: closing `"` is the last character.
        b'"' => return (len - 1, len),
        // Possibly language-tagged: scan backwards for valid BCP-47 tag characters,
        // then verify `@"` delimiter. This is more robust than scanning for `"@`
        // because language tags are restricted to [a-zA-Z0-9-], so we won't match
        // an `@` embedded in the value unless everything after it also looks like
        // a valid tag.
        b if b.is_ascii_alphanumeric() || b == b'-' => {
            let mut tag_start = len - 1;
            while tag_start > 0
                && (term[tag_start - 1].is_ascii_alphanumeric() || term[tag_start - 1] == b'-')
            {
                tag_start -= 1;
            }
            if tag_start >= 2 && term[tag_start - 1] == b'@' && term[tag_start - 2] == b'"' {
                return (tag_start - 2, tag_start - 1);
            }
        }
        _ => {}
    }

    // Fallback (shouldn't happen with valid HDT data)
    (len, len)
}

/// Write a literal value with N-Triples escaping.
///
/// Per the W3C N-Triples spec, STRING_LITERAL_QUOTE only allows characters in
/// `[#x20-#x21] | [#x23-#x5B] | [#x5D-#x10FFFF]` plus ECHAR/UCHAR escapes.
/// This means all bytes below 0x20 (control characters), `"` (0x22), and `\` (0x5C)
/// must be escaped. In the common case where no escaping is needed, this is a
/// single `write_all` call.
fn write_escaped_literal_value(w: &mut impl Write, value: &[u8]) -> std::io::Result<()> {
    let mut start = 0;
    for (i, &b) in value.iter().enumerate() {
        let escape: &[u8] = match b {
            b'\\' => b"\\\\",
            b'"' => b"\\\"",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x08 => b"\\b",
            0x0C => b"\\f",
            // Other control characters (0x00-0x07, 0x0B, 0x0E-0x1F) need \uXXXX
            0x00..=0x1F => {
                if start < i {
                    w.write_all(&value[start..i])?;
                }
                write!(w, "\\u{b:04X}")?;
                start = i + 1;
                continue;
            }
            _ => continue,
        };
        if start < i {
            w.write_all(&value[start..i])?;
        }
        w.write_all(escape)?;
        start = i + 1;
    }
    if start < value.len() {
        w.write_all(&value[start..])?;
    }
    Ok(())
}

fn parse_num_triples_from_header(header: &str) -> Result<u64> {
    const VOID_TRIPLES: &str = "http://rdfs.org/ns/void#triples";
    const HDT_TRIPLES_NUM: &str = "http://purl.org/HDT/hdt#triplesnumTriples";

    let mut value_from_void: Option<u64> = None;
    let mut value_from_hdt: Option<u64> = None;

    let parser =
        RdfParser::from_format(RdfFormat::NTriples).for_reader(Cursor::new(header.as_bytes()));

    for quad_result in parser {
        let quad = quad_result.context("Invalid N-Triples in HDT header metadata")?;
        let predicate = quad.predicate.as_str();

        if predicate != VOID_TRIPLES && predicate != HDT_TRIPLES_NUM {
            continue;
        }

        let oxrdf::Term::Literal(literal) = quad.object else {
            continue;
        };

        let parsed = literal.value().parse::<u64>().with_context(|| {
            format!("Invalid numeric triple-count literal: {}", literal.value())
        })?;

        if predicate == VOID_TRIPLES {
            value_from_void = Some(parsed);
        }
        if predicate == HDT_TRIPLES_NUM {
            value_from_hdt = Some(parsed);
        }
    }

    match (value_from_void, value_from_hdt) {
        (Some(v), Some(h)) if v != h => {
            bail!(
                "Header triple-count mismatch between void:triples ({v}) and hdt:triplesnumTriples ({h})"
            )
        }
        (Some(v), Some(_)) => Ok(v),
        (Some(v), None) => Ok(v),
        (None, Some(h)) => Ok(h),
        (None, None) => bail!("Header metadata missing triple-count predicate"),
    }
}

struct DictionaryResolver {
    shared: PfcSectionIndex,
    subjects: PfcSectionIndex,
    predicates: PfcSectionIndex,
    objects: PfcSectionIndex,
}

impl DictionaryResolver {
    fn subject_term(&mut self, subject_id: u64, buf: &mut Vec<u8>) -> Result<()> {
        let shared_count = self.shared.string_count;
        if subject_id == 0 {
            bail!("Invalid subject ID 0");
        }
        if subject_id <= shared_count {
            return self.shared.get_bytes(subject_id, buf);
        }
        let local = subject_id - shared_count;
        self.subjects.get_bytes(local, buf)
    }

    fn predicate_term(&mut self, predicate_id: u64, buf: &mut Vec<u8>) -> Result<()> {
        self.predicates.get_bytes(predicate_id, buf)
    }

    fn object_term(&mut self, object_id: u64, buf: &mut Vec<u8>) -> Result<()> {
        let shared_count = self.shared.string_count;
        if object_id == 0 {
            bail!("Invalid object ID 0");
        }
        if object_id <= shared_count {
            return self.shared.get_bytes(object_id, buf);
        }
        let local = object_id - shared_count;
        self.objects.get_bytes(local, buf)
    }
}

struct PfcSectionIndex {
    section_name: &'static str,
    string_count: u64,
    block_size: u64,
    offsets: Vec<u64>,
    string_buf_start: u64,
    reader: BufReader<File>,
    block_cache: HashMap<u64, Vec<Vec<u8>>>,
    cache_order: VecDeque<u64>,
    cache_capacity: usize,
}

impl PfcSectionIndex {
    fn read_from<R: Read + Seek>(
        reader: &mut R,
        hdt_path: &Path,
        section_name: &'static str,
        cache_budget: usize,
    ) -> Result<Self> {
        let mut preamble = Vec::new();

        let mut section_type = [0u8; 1];
        reader.read_exact(&mut section_type)?;
        if section_type[0] != PFC_SECTION_TYPE {
            bail!(
                "Invalid dictionary section type for {section_name}: expected 0x{PFC_SECTION_TYPE:02x}, got 0x{:02x}",
                section_type[0]
            );
        }
        preamble.push(section_type[0]);

        let string_count = read_vbyte(reader)
            .with_context(|| format!("Invalid string count VByte for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(string_count));
        let buffer_length = read_vbyte(reader)
            .with_context(|| format!("Invalid buffer length VByte for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(buffer_length));
        let block_size = read_vbyte(reader)
            .with_context(|| format!("Invalid block size VByte for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(block_size));
        if block_size == 0 {
            bail!("Invalid block size 0 in {section_name} section");
        }

        let mut crc8_buf = [0u8; 1];
        reader.read_exact(&mut crc8_buf)?;
        let expected_crc8 = crc8(&preamble);
        if crc8_buf[0] != expected_crc8 {
            bail!(
                "PFC preamble CRC8 mismatch in {section_name}: expected {expected_crc8:#04x}, got {:#04x}",
                crc8_buf[0]
            );
        }

        let offsets_reader = LogArrayReader::read_from(reader)
            .with_context(|| format!("Failed to read block offsets for {section_name}"))?;
        let offset_count = offsets_reader.len();

        let expected_blocks = if string_count == 0 {
            0
        } else {
            string_count.div_ceil(block_size)
        };
        let expected_offsets = expected_blocks + 1;
        if offset_count != expected_offsets {
            bail!(
                "Unexpected offset count in {section_name}: got {offset_count}, expected {expected_offsets}"
            );
        }

        let mut offsets = Vec::with_capacity(offset_count as usize);
        for i in 0..offset_count {
            offsets.push(offsets_reader.get(i));
        }

        if offsets.last().copied().unwrap_or(0) != buffer_length {
            bail!(
                "PFC sentinel mismatch in {section_name}: last offset {} != buffer length {buffer_length}",
                offsets.last().copied().unwrap_or(0)
            );
        }

        let string_buf_start = reader.stream_position()?;
        reader
            .seek(SeekFrom::Current(buffer_length as i64 + 4))
            .with_context(|| format!("Failed to skip string buffer for {section_name}"))?;

        // Estimate ~2KB per decoded block (block_size strings × ~128 bytes avg).
        // Clamp to at least 64 blocks so small budgets still function.
        const ESTIMATED_BLOCK_BYTES: usize = 2048;
        let cache_capacity = (cache_budget / ESTIMATED_BLOCK_BYTES).max(64);

        tracing::debug!(
            "{section_name}: cache_capacity={cache_capacity} blocks (budget={cache_budget} bytes)"
        );

        let file = File::open(hdt_path)?;
        Ok(Self {
            section_name,
            string_count,
            block_size,
            offsets,
            string_buf_start,
            reader: BufReader::with_capacity(64 * 1024, file),
            block_cache: HashMap::new(),
            cache_order: VecDeque::new(),
            cache_capacity,
        })
    }

    fn get_bytes(&mut self, id: u64, buf: &mut Vec<u8>) -> Result<()> {
        if id == 0 || id > self.string_count {
            bail!(
                "{} ID out of range: {id} (valid range: 1..={})",
                self.section_name,
                self.string_count
            );
        }

        let zero_based = id - 1;
        let block_index = zero_based / self.block_size;
        let entry_in_block = (zero_based % self.block_size) as usize;

        if !self.block_cache.contains_key(&block_index) {
            let block = self.decode_block(block_index)?;
            self.block_cache.insert(block_index, block);
            self.cache_order.push_back(block_index);
            while self.cache_order.len() > self.cache_capacity {
                if let Some(evicted) = self.cache_order.pop_front() {
                    self.block_cache.remove(&evicted);
                }
            }
        }

        let entry = self
            .block_cache
            .get(&block_index)
            .and_then(|v| v.get(entry_in_block))
            .with_context(|| {
                format!(
                    "Decoded block too short in {} at block {}, entry {}",
                    self.section_name, block_index, entry_in_block
                )
            })?;
        buf.clear();
        buf.extend_from_slice(entry);
        Ok(())
    }

    fn decode_block(&mut self, block_index: u64) -> Result<Vec<Vec<u8>>> {
        let start = self
            .offsets
            .get(block_index as usize)
            .copied()
            .with_context(|| {
                format!(
                    "Missing block offset {block_index} in {}",
                    self.section_name
                )
            })?;
        let end = self
            .offsets
            .get(block_index as usize + 1)
            .copied()
            .with_context(|| {
                format!(
                    "Missing block offset {} in {}",
                    block_index + 1,
                    self.section_name
                )
            })?;

        if end < start {
            bail!(
                "Invalid block offsets in {}: end {} < start {}",
                self.section_name,
                end,
                start
            );
        }

        let block_len = (end - start) as usize;
        let mut data = vec![0u8; block_len];
        self.reader
            .seek(SeekFrom::Start(self.string_buf_start + start))?;
        self.reader.read_exact(&mut data)?;

        let base = block_index * self.block_size;
        let max_entries = (self.string_count - base).min(self.block_size) as usize;
        let mut entries = Vec::with_capacity(max_entries);

        let mut pos = 0usize;
        let mut prev_bytes = Vec::<u8>::new();
        for i in 0..max_entries {
            if pos >= data.len() {
                bail!(
                    "Unexpected end of block in {} at entry {}",
                    self.section_name,
                    i
                );
            }

            if i == 0 {
                let rel_end = data[pos..].iter().position(|&b| b == 0).with_context(|| {
                    format!(
                        "Missing null terminator in {} block {}",
                        self.section_name, block_index
                    )
                })?;
                let end_pos = pos + rel_end;
                let term_bytes = data[pos..end_pos].to_vec();
                pos = end_pos + 1;
                prev_bytes = term_bytes.clone();
                entries.push(term_bytes);
                continue;
            }

            let (shared, consumed) = decode_vbyte(&data[pos..])?;
            pos += consumed;
            let rel_end = data[pos..].iter().position(|&b| b == 0).with_context(|| {
                format!(
                    "Missing null terminator in {} block {}",
                    self.section_name, block_index
                )
            })?;
            let end_pos = pos + rel_end;
            let suffix = &data[pos..end_pos];
            pos = end_pos + 1;

            let shared = shared as usize;
            if shared > prev_bytes.len() {
                bail!(
                    "Invalid shared prefix length {} in {} block {} (prev len {})",
                    shared,
                    self.section_name,
                    block_index,
                    prev_bytes.len()
                );
            }

            let mut value_bytes = Vec::with_capacity(shared + suffix.len());
            value_bytes.extend_from_slice(&prev_bytes[..shared]);
            value_bytes.extend_from_slice(suffix);
            prev_bytes = value_bytes.clone();
            entries.push(value_bytes);
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_to_string(f: impl Fn(&mut Vec<u8>) -> std::io::Result<()>) -> String {
        let mut buf = Vec::new();
        f(&mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_write_subject_iri() {
        let result = write_to_string(|w| write_ntriples_subject(w, b"http://example.org/s"));
        assert_eq!(result, "<http://example.org/s>");
    }

    #[test]
    fn test_write_subject_blank_node() {
        let result = write_to_string(|w| write_ntriples_subject(w, b"_:b0"));
        assert_eq!(result, "_:b0");
    }

    #[test]
    fn test_write_object_iri() {
        let result = write_to_string(|w| write_ntriples_object(w, b"http://example.org/o"));
        assert_eq!(result, "<http://example.org/o>");
    }

    #[test]
    fn test_write_object_blank_node() {
        let result = write_to_string(|w| write_ntriples_object(w, b"_:b1"));
        assert_eq!(result, "_:b1");
    }

    #[test]
    fn test_write_literal_simple() {
        let result = write_to_string(|w| write_ntriples_object(w, b"\"hello\""));
        assert_eq!(result, "\"hello\"");
    }

    #[test]
    fn test_write_literal_typed() {
        let result = write_to_string(|w| {
            write_ntriples_object(w, b"\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>")
        });
        assert_eq!(
            result,
            "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
    }

    #[test]
    fn test_write_literal_language() {
        let result = write_to_string(|w| write_ntriples_object(w, b"\"bonjour\"@fr"));
        assert_eq!(result, "\"bonjour\"@fr");
    }

    #[test]
    fn test_write_literal_embedded_quote() {
        // HDT stores: "he said "hi"" (raw quotes inside)
        // N-Triples output must escape: "he said \"hi\""
        let result = write_to_string(|w| write_ntriples_object(w, b"\"he said \"hi\"\""));
        assert_eq!(result, r#""he said \"hi\"""#);
    }

    #[test]
    fn test_write_literal_with_newline() {
        // HDT stores raw newline byte
        let result = write_to_string(|w| write_ntriples_object(w, b"\"line1\nline2\""));
        assert_eq!(result, "\"line1\\nline2\"");
    }

    #[test]
    fn test_write_literal_with_backslash() {
        let result = write_to_string(|w| write_ntriples_object(w, b"\"path\\to\\file\""));
        assert_eq!(result, "\"path\\\\to\\\\file\"");
    }

    #[test]
    fn test_write_literal_with_cr_and_tab() {
        let result = write_to_string(|w| write_ntriples_object(w, b"\"a\rb\tc\""));
        assert_eq!(result, "\"a\\rb\\tc\"");
    }

    #[test]
    fn test_write_literal_with_backspace_and_formfeed() {
        let result = write_to_string(|w| write_ntriples_object(w, b"\"a\x08b\x0Cc\""));
        assert_eq!(result, "\"a\\bb\\fc\"");
    }

    #[test]
    fn test_write_literal_with_other_control_chars() {
        // NUL (0x00), BEL (0x07), vertical tab (0x0B) need \uXXXX escaping
        let result = write_to_string(|w| write_ntriples_object(w, b"\"a\x00b\x07c\x0Bd\""));
        assert_eq!(result, "\"a\\u0000b\\u0007c\\u000Bd\"");
    }

    #[test]
    fn test_write_literal_typed_with_escapes() {
        // Value contains a newline, typed literal
        let input = b"\"line1\nline2\"^^<http://www.w3.org/2001/XMLSchema#string>";
        let result = write_to_string(|w| write_ntriples_object(w, input));
        assert_eq!(
            result,
            "\"line1\\nline2\"^^<http://www.w3.org/2001/XMLSchema#string>"
        );
    }

    #[test]
    fn test_write_literal_language_with_escapes() {
        let input = b"\"line1\nline2\"@en";
        let result = write_to_string(|w| write_ntriples_object(w, input));
        assert_eq!(result, "\"line1\\nline2\"@en");
    }

    #[test]
    fn test_write_literal_unicode() {
        // UTF-8 characters pass through unchanged
        let result =
            write_to_string(|w| write_ntriples_object(w, "\"èpsilon\"".as_bytes()));
        assert_eq!(result, "\"èpsilon\"");
    }

    #[test]
    fn test_find_boundary_typed() {
        let term = b"\"value\"^^<http://example.org/type>";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"value");
        assert_eq!(&term[ss..], b"^^<http://example.org/type>");
    }

    #[test]
    fn test_find_boundary_language() {
        let term = b"\"value\"@en";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"value");
        assert_eq!(&term[ss..], b"@en");
    }

    #[test]
    fn test_find_boundary_simple() {
        let term = b"\"value\"";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"value");
        assert_eq!(ss, term.len());
    }

    #[test]
    fn test_find_boundary_value_containing_at() {
        // Value contains "@" but the real language tag is at the end
        let term = b"\"email@host\"@en";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"email@host");
        assert_eq!(&term[ss..], b"@en");
    }

    #[test]
    fn test_find_boundary_value_ending_with_at_no_lang_tag() {
        // Simple literal whose value ends with "@" — not a language tag because
        // there are no BCP-47 characters after the "@".
        let term = b"\"user@\"";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"user@");
        assert_eq!(ss, term.len()); // simple literal, no suffix
    }

    #[test]
    fn test_find_boundary_value_with_at_non_tag_suffix() {
        // Value contains "@" followed by characters that include non-BCP-47 chars
        // (dots, underscores). The last byte is '.', which is not alphanumeric or '-',
        // so this is treated as a simple literal ending with '"'.
        // HDT wouldn't normally produce this, but it tests the robustness of the parser.
        let term = b"\"user@host.com\"";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"user@host.com");
        assert_eq!(ss, term.len()); // simple literal
    }

    #[test]
    fn test_find_boundary_multiple_at_signs() {
        // Value has multiple "@" signs; only the last valid "@ + BCP-47 tag should match.
        let term = b"\"a@fake\"@de";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"a@fake");
        assert_eq!(&term[ss..], b"@de");
    }
}
