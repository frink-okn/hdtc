//! Sequential PFC (Plain Front Coding) section reader.
//!
//! Iterates all terms in a PFC dictionary section in lexicographic order,
//! decoding blocks sequentially. O(block_size) memory. A reader that can seek
//! may start mid-section (`PfcSectionIterator::seek_to`); reading is still
//! sequential from there.

use crate::io::crc_utils::crc8;
use crate::io::{
    LogArrayReader, LogArraySection, decode_vbyte, encode_vbyte, read_vbyte, read_vbyte_recording,
    scan_log_array_section, verify_preamble_crc8,
};
use anyhow::{Context, Result, anyhow, bail, ensure};
use std::io::{Read, Seek, SeekFrom};

const PFC_SECTION_TYPE: u8 = 0x02;

/// Said of a front-coded block that does not decode. A term holding U+0000
/// reads as a short term and then as garbage: the reader ends the term at the
/// `0x00` and decodes the rest of it as the next entry's shared-prefix length,
/// or, in a block's last entry, leaves it over.
pub(crate) const EMBEDDED_NUL_HINT: &str =
    "a term in it may hold U+0000, which hdtc up to 1.3.1 wrote unchanged; rebuild the HDT";

/// Decode one front-coded block of `entries` terms, handing each to `visit`
/// in order. Each term is decoded in place over its predecessor in `term`, so
/// decoding allocates nothing per term.
///
/// The block must hold exactly its entries: bytes left over are an error, so
/// a term holding U+0000 in a block's last entry is not read back silently
/// cut short. A decoding error names the section, block and entry, with
/// [`EMBEDDED_NUL_HINT`]; an error from `visit` is passed through unchanged.
pub(crate) fn decode_block(
    section_name: &str,
    block_index: u64,
    data: &[u8],
    entries: u64,
    term: &mut Vec<u8>,
    mut visit: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let malformed = |what: std::fmt::Arguments| {
        anyhow!("{section_name} block {block_index} does not decode: {what}; {EMBEDDED_NUL_HINT}")
    };
    let mut pos = 0usize;
    for i in 0..entries {
        if pos >= data.len() {
            return Err(malformed(format_args!("it ends before entry {i}")));
        }
        if i == 0 {
            term.clear();
        } else {
            let (shared, consumed) = decode_vbyte(&data[pos..]).map_err(|e| {
                malformed(format_args!(
                    "entry {i} has a malformed shared-prefix length ({e})"
                ))
            })?;
            pos += consumed;
            let shared = usize::try_from(shared).unwrap_or(usize::MAX);
            if shared > term.len() {
                return Err(malformed(format_args!(
                    "entry {i} shares {shared} bytes with a previous term of {}",
                    term.len()
                )));
            }
            term.truncate(shared);
        }
        let Some(len) = data[pos..].iter().position(|&b| b == 0) else {
            return Err(malformed(format_args!("entry {i} has no terminator")));
        };
        term.extend_from_slice(&data[pos..pos + len]);
        pos += len + 1;
        visit(term)?;
    }
    if pos != data.len() {
        return Err(malformed(format_args!(
            "{} bytes follow its last entry",
            data.len() - pos
        )));
    }
    Ok(())
}

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

        let string_count = read_vbyte(reader)
            .with_context(|| format!("Invalid string count for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(string_count));
        let buffer_length = read_vbyte(reader)
            .with_context(|| format!("Invalid buffer length for {section_name}"))?;
        preamble.extend_from_slice(&encode_vbyte(buffer_length));
        let block_size =
            read_vbyte(reader).with_context(|| format!("Invalid block size for {section_name}"))?;
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
/// Reads blocks linearly from the underlying reader; the only seek is the
/// optional initial `seek_to`.
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
    ///
    /// Takes the header by value: `offsets` holds one `u64` per block (0.5 bytes
    /// per term at the default block size), so cloning it would double a
    /// dictionary-sized allocation.
    pub fn new(reader: R, header: PfcSectionHeader, section_name: &str) -> Self {
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
            offsets: header.offsets,
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
        let start = self
            .offsets
            .get(block_index as usize)
            .copied()
            .with_context(|| {
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
        let count = (self.string_count - base).min(self.block_size);
        let mut entries = Vec::with_capacity(count as usize);
        let mut term = Vec::new();
        decode_block(
            &self.section_name,
            block_index,
            &data,
            count,
            &mut term,
            |value| {
                entries.push(value.to_vec());
                Ok(())
            },
        )?;
        Ok(entries)
    }
}

impl<R: Read + Seek> PfcSectionIterator<R> {
    /// Position a fresh iterator at the term with zero-based `index`, without
    /// decoding the blocks before it: one seek to that term's block, then the
    /// terms before it within the block. For reading a run in the middle of a
    /// section, such as the triple-term IRIs, sequentially.
    pub fn seek_to(&mut self, index: u64) -> Result<()> {
        ensure!(
            self.strings_yielded == 0 && self.blocks_decoded == 0,
            "{} can only be positioned before it is read",
            self.section_name
        );
        if index == 0 {
            return Ok(());
        }
        if index >= self.string_count {
            self.strings_yielded = self.string_count;
            return Ok(());
        }
        let block = index / self.block_size;
        let offset =
            self.offsets.get(block as usize).copied().with_context(|| {
                format!("Missing block offset {block} in {}", self.section_name)
            })?;
        self.reader.seek(SeekFrom::Current(
            i64::try_from(offset).context("PFC block offset is too large to seek")?,
        ))?;
        self.bytes_consumed = offset;
        self.blocks_decoded = block;
        self.strings_yielded = block * self.block_size;
        for _ in self.strings_yielded..index {
            self.next().transpose()?;
        }
        Ok(())
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

/// Where a PFC section's parts are and what shape they have, from preambles.
///
/// Produced by [`scan_pfc_section`], which reads no payload byte — neither a
/// block offset nor a term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PfcSection {
    /// Offset of the section, i.e. of its type byte.
    pub section_start: u64,
    /// Terms in the section; ids run `1..=string_count`.
    pub string_count: u64,
    /// Terms per block. Only a block's first term is stored uncompressed.
    pub block_size: u64,
    /// The `LogArray` of block start offsets into the string buffer, with a
    /// sentinel entry holding [`buffer_length`](Self::buffer_length).
    pub offsets: LogArraySection,
    /// Offset of the front-coded string buffer.
    pub buffer_start: u64,
    /// Length of the string buffer in bytes.
    pub buffer_length: u64,
    /// Offset just past the buffer's CRC32C — where the next section begins.
    pub section_end: u64,
}

/// Locate a PFC section's block offsets and string buffer from its preambles,
/// leaving `reader` past the section.
///
/// Verifies both preamble CRC8s and that the block-offset count matches the
/// string count, but reads **no payload**: on a large dictionary the
/// block-offset array alone runs to millions of entries, which is why
/// [`PfcSectionHeader::read_from`] — which materializes it — must not be on a
/// mapped reader's open path (KGF doc 20 §20.4). The sentinel check that reader
/// performs needs a payload read and so belongs to verification rather than to
/// open.
pub fn scan_pfc_section<R: Read + Seek>(reader: &mut R, section_name: &str) -> Result<PfcSection> {
    let section_start = reader.stream_position()?;

    let mut preamble = vec![0u8; 1];
    reader.read_exact(&mut preamble)?;
    ensure!(
        preamble[0] == PFC_SECTION_TYPE,
        "invalid dictionary section type for {section_name}: expected {PFC_SECTION_TYPE:#04x}, got {:#04x}",
        preamble[0]
    );
    let string_count = read_vbyte_recording(reader, &mut preamble)?;
    let buffer_length = read_vbyte_recording(reader, &mut preamble)?;
    let block_size = read_vbyte_recording(reader, &mut preamble)?;
    ensure!(block_size > 0, "invalid {section_name} PFC block size 0");
    verify_preamble_crc8(reader, &preamble, &format!("{section_name} PFC"))?;

    let offsets = scan_log_array_section(reader)
        .with_context(|| format!("Failed to scan {section_name} PFC block offsets"))?;
    let expected_offsets = string_count
        .div_ceil(block_size)
        .checked_add(1)
        .context("PFC block-offset count overflow")?;
    ensure!(
        offsets.num_entries == expected_offsets,
        "unexpected {section_name} PFC block-offset count: got {}, expected {expected_offsets}",
        offsets.num_entries
    );

    let buffer_start = reader.stream_position()?;
    let skip = buffer_length
        .checked_add(4)
        .context("PFC string buffer length overflow")?;
    let section_end = reader.seek(SeekFrom::Current(
        i64::try_from(skip).context("PFC string buffer is too large to seek")?,
    ))?;

    Ok(PfcSection {
        section_start,
        string_count,
        block_size,
        offsets,
        buffer_start,
        buffer_length,
        section_end,
    })
}

/// Read and skip a PFC section, returning its string count.
///
/// Useful when you need to skip past a section without decoding all terms.
/// A thin wrapper over [`scan_pfc_section`]; use that when the offsets matter.
pub fn skip_pfc_section<R: Read + Seek>(reader: &mut R, section_name: &str) -> Result<u64> {
    Ok(scan_pfc_section(reader, section_name)?.string_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dictionary::pfc::{PfcEncoder, TERMS_WITH_NUL};
    use std::io::Cursor;

    fn iterate(terms: &[&str]) -> Result<Vec<Vec<u8>>> {
        let mut encoder = PfcEncoder::new();
        for term in terms {
            encoder.push_unchecked(*term);
        }
        let mut section = Vec::new();
        encoder.write_to(&mut section).unwrap();
        let mut reader = Cursor::new(section);
        let header = PfcSectionHeader::read_from(&mut reader, "objects").unwrap();
        PfcSectionIterator::new(reader, header, "objects").collect()
    }

    /// A term holding U+0000, written unchanged, reads as "alpha" and then an
    /// entry that does not decode; the error names where.
    #[test]
    fn an_embedded_nul_is_reported_with_its_section_block_and_entry() {
        let message = iterate(&TERMS_WITH_NUL).unwrap_err().to_string();
        assert!(
            message.starts_with(
                "objects block 0 does not decode: entry 2 has a malformed shared-prefix length"
            ),
            "{message}"
        );
        assert!(message.ends_with(EMBEDDED_NUL_HINT), "{message}");
    }

    /// In a block's last entry, U+0000 leaves the rest of the term over
    /// rather than spoiling a next entry; that is refused too, not read back
    /// as a term cut short.
    #[test]
    fn an_embedded_nul_in_a_blocks_last_entry_is_reported() {
        let message = iterate(&["alpha\0tail"]).unwrap_err().to_string();
        assert_eq!(
            message,
            format!(
                "objects block 0 does not decode: 5 bytes follow its last entry; {EMBEDDED_NUL_HINT}"
            )
        );
    }

    /// Positioning at any term yields exactly the section's suffix from it,
    /// across block boundaries and at both ends.
    #[test]
    fn a_positioned_iterator_reads_the_suffix_from_any_term() {
        let terms: Vec<String> = (0..23).map(|i| format!("term-{i:03}")).collect();
        let mut encoder = PfcEncoder::with_block_size(4);
        for term in &terms {
            encoder.push(term);
        }
        let mut section = Vec::new();
        encoder.write_to(&mut section).unwrap();
        for start in 0..=terms.len() {
            let mut reader = Cursor::new(section.clone());
            let header = PfcSectionHeader::read_from(&mut reader, "test").unwrap();
            let mut iterator = PfcSectionIterator::new(reader, header, "test");
            iterator.seek_to(start as u64).unwrap();
            let read: Vec<String> = iterator
                .map(|term| String::from_utf8(term.unwrap()).unwrap())
                .collect();
            assert_eq!(read, terms[start..], "from {start}");
        }
    }

    /// The scan must describe a section the encoder just wrote, and land exactly
    /// on what follows it — the property every walk built from it depends on.
    #[test]
    fn scanning_a_section_locates_its_parts_and_the_next_section() {
        for terms in [&[][..], &["alpha", "beta", "gamma"][..]] {
            let mut encoder = PfcEncoder::with_block_size(2);
            for term in terms {
                encoder.push(*term);
            }
            let mut section = Vec::new();
            encoder.write_to(&mut section).unwrap();
            let section_length = section.len() as u64;
            section.extend_from_slice(b"next");

            let mut reader = Cursor::new(section);
            let scanned = scan_pfc_section(&mut reader, "test").unwrap();

            assert_eq!(scanned.section_start, 0);
            assert_eq!(scanned.string_count, terms.len() as u64);
            assert_eq!(scanned.block_size, 2);
            assert_eq!(
                scanned.offsets.num_entries,
                (terms.len() as u64).div_ceil(2) + 1,
                "one offset per block plus the sentinel"
            );
            // The three parts are contiguous: preamble, block offsets, buffer.
            assert!(scanned.offsets.section_start > scanned.section_start);
            assert_eq!(
                scanned.offsets.section_end, scanned.buffer_start,
                "the string buffer follows the block offsets"
            );
            assert_eq!(scanned.section_end, section_length);
            assert_eq!(reader.position(), section_length);

            let mut next = [0u8; 4];
            reader.read_exact(&mut next).unwrap();
            assert_eq!(&next, b"next");
        }
    }

    /// A materializing read of the same section must agree with the scan, since
    /// the two parse the same preamble for different consumers.
    #[test]
    fn the_scan_and_the_materializing_reader_agree() {
        let mut encoder = PfcEncoder::with_block_size(4);
        for term in ["alpha", "beta", "delta", "epsilon", "gamma"] {
            encoder.push(term);
        }
        let mut section = Vec::new();
        encoder.write_to(&mut section).unwrap();

        let scanned = scan_pfc_section(&mut Cursor::new(section.clone()), "test").unwrap();
        let header = PfcSectionHeader::read_from(&mut Cursor::new(section), "test").unwrap();

        assert_eq!(scanned.string_count, header.string_count);
        assert_eq!(scanned.block_size, header.block_size);
        assert_eq!(scanned.buffer_length, header.buffer_length);
        assert_eq!(scanned.offsets.num_entries, header.offsets.len() as u64);
    }
}
