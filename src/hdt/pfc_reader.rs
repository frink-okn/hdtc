//! Sequential PFC (Plain Front Coding) section reader.
//!
//! Iterates all terms in a PFC dictionary section in lexicographic order,
//! decoding blocks sequentially. O(block_size) memory — no seeking needed.

use crate::io::crc_utils::crc8;
use crate::io::{LogArrayReader, decode_vbyte, encode_vbyte, read_vbyte};
use anyhow::{Context, Result, bail};
use std::io::Read;

const PFC_SECTION_TYPE: u8 = 0x02;

/// Metadata about a PFC section, read from the preamble.
pub struct PfcSectionHeader {
    pub string_count: u64,
    pub block_size: u64,
    /// Block offsets into the string buffer.
    pub offsets: Vec<u64>,
    /// Total byte length of the string buffer (for CRC skip).
    pub buffer_length: u64,
}

impl PfcSectionHeader {
    /// Read the PFC section preamble and block offsets from a reader.
    ///
    /// After this call, the reader is positioned at the start of the string
    /// data buffer, ready for sequential block decoding.
    pub fn read_from<R: Read>(reader: &mut R, section_name: &str) -> Result<Self> {
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

        let string_count =
            read_vbyte(reader).with_context(|| format!("Invalid string count for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(string_count));
        let buffer_length = read_vbyte(reader)
            .with_context(|| format!("Invalid buffer length for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(buffer_length));
        let block_size = read_vbyte(reader)
            .with_context(|| format!("Invalid block size for {section_name}"))?;
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

        Ok(Self {
            string_count,
            block_size,
            offsets,
            buffer_length,
        })
    }
}

/// Sequential iterator over all terms in a PFC section.
///
/// Yields terms in lexicographic order (1-based IDs: 1, 2, 3, ...).
/// Reads blocks linearly from the underlying reader — no seeking.
pub struct PfcSectionIterator<R: Read> {
    reader: R,
    section_name: String,
    string_count: u64,
    block_size: u64,
    offsets: Vec<u64>,

    // Current iteration state
    strings_yielded: u64,
    current_block: Vec<Vec<u8>>,
    current_block_index: usize,
    blocks_decoded: u64,
    total_blocks: u64,
    bytes_consumed: u64,
}

impl<R: Read> PfcSectionIterator<R> {
    /// Create a new sequential PFC iterator.
    ///
    /// The reader must be positioned at the start of the PFC string data buffer
    /// (i.e., right after the `PfcSectionHeader` has been read).
    pub fn new(reader: R, header: &PfcSectionHeader, section_name: &str) -> Self {
        let total_blocks = if header.string_count == 0 {
            0
        } else {
            header.string_count.div_ceil(header.block_size)
        };

        Self {
            reader,
            section_name: section_name.to_string(),
            string_count: header.string_count,
            block_size: header.block_size,
            offsets: header.offsets.clone(),
            strings_yielded: 0,
            current_block: Vec::new(),
            current_block_index: 0,
            blocks_decoded: 0,
            total_blocks,
            bytes_consumed: 0,
        }
    }

    /// Decode the next block from the reader.
    fn decode_next_block(&mut self) -> Result<Vec<Vec<u8>>> {
        let block_index = self.blocks_decoded;
        let start = self.offsets.get(block_index as usize).copied().with_context(|| {
            format!(
                "Missing block offset {} in {}",
                block_index, self.section_name
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
        self.reader.read_exact(&mut data)?;
        self.bytes_consumed += block_len as u64;

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
            prev_bytes.clone_from(&value_bytes);
            entries.push(value_bytes);
        }

        Ok(entries)
    }

}

impl<R: Read> Iterator for PfcSectionIterator<R> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.strings_yielded >= self.string_count {
            return None;
        }

        // Need to decode a new block?
        if self.current_block_index >= self.current_block.len() {
            if self.blocks_decoded >= self.total_blocks {
                return None;
            }
            match self.decode_next_block() {
                Ok(block) => {
                    self.current_block = block;
                    self.current_block_index = 0;
                    self.blocks_decoded += 1;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        let term = self.current_block[self.current_block_index].clone();
        self.current_block_index += 1;
        self.strings_yielded += 1;
        Some(Ok(term))
    }
}

/// Read and skip a PFC section, returning its string count.
///
/// Useful when you need to skip past a section without decoding all terms.
/// Uses `Seek` to skip the string buffer efficiently without allocation.
pub fn skip_pfc_section<R: Read + std::io::Seek>(
    reader: &mut R,
    section_name: &str,
) -> Result<u64> {
    let header = PfcSectionHeader::read_from(reader, section_name)?;
    let string_count = header.string_count;

    // Skip the string buffer + CRC32C
    reader.seek(std::io::SeekFrom::Current(header.buffer_length as i64 + 4))?;

    Ok(string_count)
}
