use crate::io::crc_utils::crc8;
use crate::io::{
    ControlInfo, ControlType, LogArrayReader, StreamingBitmapDecoder, StreamingLogArrayDecoder,
};
use anyhow::{Context, Result, bail};
use oxrdf::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, Triple};
use oxrdfio::{RdfFormat, RdfParser, RdfSerializer};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

const PFC_SECTION_TYPE: u8 = 0x02;
const DICTIONARY_FOUR_FORMAT: &str = "<http://purl.org/HDT/hdt#dictionaryFour>";
const TRIPLES_BITMAP_FORMAT: &str = "<http://purl.org/HDT/hdt#triplesBitmap>";

pub fn dump_hdt_to_ntriples_streaming(hdt_path: &Path, output_path: &Path) -> Result<u64> {
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

    let mut dictionary = DictionaryResolver {
        shared: PfcSectionIndex::read_from(&mut reader, hdt_path, "shared")?,
        subjects: PfcSectionIndex::read_from(&mut reader, hdt_path, "subjects")?,
        predicates: PfcSectionIndex::read_from(&mut reader, hdt_path, "predicates")?,
        objects: PfcSectionIndex::read_from(&mut reader, hdt_path, "objects")?,
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
    let output_writer = BufWriter::with_capacity(256 * 1024, output_file);
    let mut serializer = RdfSerializer::from_format(RdfFormat::NTriples).for_writer(output_writer);

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
    let mut current_subject_text = if num_triples > 0 {
        dictionary.subject_term(current_subject)?
    } else {
        String::new()
    };
    let mut current_predicate_text = if num_sp_pairs > 0 {
        dictionary.predicate_term(current_predicate)?
    } else {
        String::new()
    };

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

        let object_text = dictionary.object_term(object)?;

        let triple = Triple::new(
            parse_subject(&current_subject_text)
                .with_context(|| format!("Invalid subject term for ID {current_subject}"))?,
            NamedNode::new(current_predicate_text.as_str())
                .with_context(|| format!("Invalid predicate IRI for ID {current_predicate}"))?,
            parse_object(&object_text)
                .with_context(|| format!("Invalid object term for ID {object}"))?,
        );
        serializer.serialize_triple(&triple)?;
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
                    current_subject_text = dictionary.subject_term(current_subject)?;
                }
            }

            pos_y += 1;
            if pos_y < num_sp_pairs {
                current_predicate = array_y_dec
                    .next_entry()?
                    .with_context(|| format!("ArrayY ended early at pos_y {pos_y}"))?;
                current_predicate_text = dictionary.predicate_term(current_predicate)?;
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

    let mut writer = serializer.finish()?;
    writer.flush()?;

    Ok(triples_written)
}

fn parse_subject(term: &str) -> Result<NamedOrBlankNode> {
    if let Some(id) = term.strip_prefix("_:") {
        Ok(BlankNode::new(id)?.into())
    } else {
        Ok(NamedNode::new(term)?.into())
    }
}

fn parse_object(term: &str) -> Result<Term> {
    if let Some(id) = term.strip_prefix("_:") {
        Ok(Term::from(BlankNode::new(id)?))
    } else if term.starts_with('"') {
        Ok(Term::from(parse_hdt_literal(term)?))
    } else {
        Ok(Term::from(NamedNode::new(term)?))
    }
}

fn parse_hdt_literal(term: &str) -> Result<Literal> {
    if !term.starts_with('"') {
        bail!("Invalid literal serialization");
    }

    if term.ends_with('>')
        && let Some(marker_pos) = term.rfind("\"^^<")
    {
        let value = &term[1..marker_pos];
        let datatype = &term[marker_pos + 4..term.len() - 1];
        let datatype_node = NamedNode::new(datatype)
            .with_context(|| format!("Invalid datatype IRI in literal: {datatype}"))?;
        return Ok(Literal::new_typed_literal(value, datatype_node));
    }

    if let Some(marker_pos) = term.rfind("\"@") {
        let value = &term[1..marker_pos];
        let language = &term[marker_pos + 2..];
        return Literal::new_language_tagged_literal(value, language)
            .context("Invalid language-tagged literal");
    }

    if term.len() >= 2 && term.ends_with('"') {
        let value = &term[1..term.len() - 1];
        return Ok(Literal::new_simple_literal(value));
    }

    bail!("Invalid literal serialization")
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

        let Term::Literal(literal) = quad.object else {
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

fn skip_bitmap_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64)> {
    let section_start = reader.stream_position()?;

    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;

    let num_bits = read_vbyte_from_reader(reader)?;

    let mut crc8 = [0u8; 1];
    reader.read_exact(&mut crc8)?;

    let data_bytes = num_bits.div_ceil(8);
    reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

    Ok((section_start, num_bits))
}

fn skip_log_array_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64, u8)> {
    let section_start = reader.stream_position()?;

    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;

    let mut bits_byte = [0u8; 1];
    reader.read_exact(&mut bits_byte)?;
    let bits_per_entry = bits_byte[0];

    let num_entries = read_vbyte_from_reader(reader)?;

    let mut crc8 = [0u8; 1];
    reader.read_exact(&mut crc8)?;

    let total_bits = num_entries * bits_per_entry as u64;
    let data_bytes = total_bits.div_ceil(8);
    reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

    Ok((section_start, num_entries, bits_per_entry))
}

fn read_vbyte_from_reader<R: Read>(reader: &mut R) -> Result<u64> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut byte_buf = [0u8; 1];

    loop {
        reader.read_exact(&mut byte_buf)?;
        let byte = byte_buf[0];
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 != 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            bail!("Invalid VByte: value exceeds u64 range");
        }
    }
}

fn read_vbyte_tracking<R: Read>(reader: &mut R, tracking: &mut Vec<u8>) -> Result<u64> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut byte_buf = [0u8; 1];

    loop {
        reader.read_exact(&mut byte_buf)?;
        let byte = byte_buf[0];
        tracking.push(byte);
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 != 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            bail!("Invalid VByte: value exceeds u64 range");
        }
    }
}

fn decode_vbyte_slice(data: &[u8]) -> Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0u32;

    for (i, byte) in data.iter().enumerate() {
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 != 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            bail!("Invalid VByte in PFC block: value exceeds u64 range");
        }
    }

    bail!("Unexpected end of block while decoding VByte");
}

struct DictionaryResolver {
    shared: PfcSectionIndex,
    subjects: PfcSectionIndex,
    predicates: PfcSectionIndex,
    objects: PfcSectionIndex,
}

impl DictionaryResolver {
    fn subject_term(&mut self, subject_id: u64) -> Result<String> {
        let shared_count = self.shared.string_count;
        if subject_id == 0 {
            bail!("Invalid subject ID 0");
        }
        if subject_id <= shared_count {
            return self.shared.get(subject_id);
        }
        let local = subject_id - shared_count;
        self.subjects.get(local)
    }

    fn predicate_term(&mut self, predicate_id: u64) -> Result<String> {
        self.predicates.get(predicate_id)
    }

    fn object_term(&mut self, object_id: u64) -> Result<String> {
        let shared_count = self.shared.string_count;
        if object_id == 0 {
            bail!("Invalid object ID 0");
        }
        if object_id <= shared_count {
            return self.shared.get(object_id);
        }
        let local = object_id - shared_count;
        self.objects.get(local)
    }
}

struct PfcSectionIndex {
    section_name: &'static str,
    string_count: u64,
    block_size: u64,
    offsets: Vec<u64>,
    string_buf_start: u64,
    reader: BufReader<File>,
    block_cache: HashMap<u64, Vec<String>>,
    cache_order: VecDeque<u64>,
}

impl PfcSectionIndex {
    fn read_from<R: Read + Seek>(
        reader: &mut R,
        hdt_path: &Path,
        section_name: &'static str,
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

        let string_count = read_vbyte_tracking(reader, &mut preamble)
            .with_context(|| format!("Invalid string count VByte for {section_name}"))?;
        let buffer_length = read_vbyte_tracking(reader, &mut preamble)
            .with_context(|| format!("Invalid buffer length VByte for {section_name}"))?;
        let block_size = read_vbyte_tracking(reader, &mut preamble)
            .with_context(|| format!("Invalid block size VByte for {section_name}"))?;
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
        })
    }

    fn get(&mut self, id: u64) -> Result<String> {
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
            const CACHE_CAPACITY: usize = 4096;
            while self.cache_order.len() > CACHE_CAPACITY {
                if let Some(evicted) = self.cache_order.pop_front() {
                    self.block_cache.remove(&evicted);
                }
            }
        }

        self.block_cache
            .get(&block_index)
            .and_then(|v| v.get(entry_in_block))
            .cloned()
            .with_context(|| {
                format!(
                    "Decoded block too short in {} at block {}, entry {}",
                    self.section_name, block_index, entry_in_block
                )
            })
    }

    fn decode_block(&mut self, block_index: u64) -> Result<Vec<String>> {
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
                let value = std::str::from_utf8(&term_bytes)
                    .with_context(|| {
                        format!(
                            "Non-UTF8 term in {} block {}",
                            self.section_name, block_index
                        )
                    })?
                    .to_string();
                pos = end_pos + 1;
                prev_bytes = term_bytes;
                entries.push(value);
                continue;
            }

            let (shared, consumed) = decode_vbyte_slice(&data[pos..])?;
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
            let value = String::from_utf8(value_bytes.clone()).with_context(|| {
                format!(
                    "Non-UTF8 reconstructed term in {} block {}",
                    self.section_name, block_index
                )
            })?;
            prev_bytes = value_bytes;
            entries.push(value);
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_hdt_literal;

    #[test]
    fn test_parse_hdt_literal_simple() {
        let lit = parse_hdt_literal("\"hello\"").unwrap();
        assert_eq!(lit.value(), "hello");
        assert_eq!(
            lit.datatype().as_str(),
            "http://www.w3.org/2001/XMLSchema#string"
        );
        assert!(lit.language().is_none());
    }

    #[test]
    fn test_parse_hdt_literal_typed() {
        let lit = parse_hdt_literal("\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>").unwrap();
        assert_eq!(lit.value(), "30");
        assert_eq!(
            lit.datatype().as_str(),
            "http://www.w3.org/2001/XMLSchema#integer"
        );
        assert!(lit.language().is_none());
    }

    #[test]
    fn test_parse_hdt_literal_language() {
        let lit = parse_hdt_literal("\"bonjour\"@fr").unwrap();
        assert_eq!(lit.value(), "bonjour");
        assert_eq!(lit.language(), Some("fr"));
    }

    #[test]
    fn test_parse_hdt_literal_embedded_quote() {
        let lit = parse_hdt_literal("\"he said \"hi\"\"").unwrap();
        assert_eq!(lit.value(), "he said \"hi\"");
    }
}
