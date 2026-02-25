//! Plain Front Coding (PFC) dictionary section encoder and decoder for HDT.
//!
//! PFC encodes a sorted list of strings using front coding in blocks:
//! - First string in each block: stored verbatim + null terminator
//! - Subsequent strings: VByte(shared_prefix_length) + suffix + null terminator
//!
//! Section layout:
//! type_byte(0x02) + VByte(string_count) + VByte(buffer_length) + VByte(block_size) + CRC8
//! + LogArray(block_offsets) + Buffer(encoded_strings) + CRC32C

use crate::io::crc_utils::{crc8, Crc32cWriter, CRC32C_ALGO};
#[cfg(test)]
use crate::io::crc_utils::crc32c;
use crate::io::log_array::{LogArrayWriter, StreamingLogArrayEncoder};
use crate::io::vbyte::encode_vbyte;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// PFC dictionary section type byte, written before each section.
const PFC_SECTION_TYPE: u8 = 0x02;

/// Default number of strings per block.
const DEFAULT_BLOCK_SIZE: usize = 16;

/// Encoder for building a PFC dictionary section (used in tests).
#[cfg(test)]
pub struct PfcEncoder {
    block_size: usize,
    strings: Vec<String>,
}

#[cfg(test)]
impl PfcEncoder {
    pub fn new() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            strings: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn with_block_size(block_size: usize) -> Self {
        assert!(block_size > 0, "Block size must be > 0");
        Self {
            block_size,
            strings: Vec::new(),
        }
    }

    /// Add a string to the dictionary section.
    /// Strings MUST be added in sorted order.
    pub fn push(&mut self, s: impl Into<String>) {
        let s = s.into();
        debug_assert!(
            self.strings.last().is_none_or(|prev| prev.as_str() < s.as_str()),
            "Strings must be added in sorted order"
        );
        self.strings.push(s);
    }

    /// Serialize the PFC section to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // First, encode all strings into a buffer and collect block offsets
        let mut buffer = Vec::new();
        let mut block_offsets: Vec<u64> = Vec::new();

        for (i, s) in self.strings.iter().enumerate() {
            if i % self.block_size == 0 {
                // Start of a new block: record offset, write string verbatim
                block_offsets.push(buffer.len() as u64);
                buffer.extend_from_slice(s.as_bytes());
                buffer.push(0x00); // null terminator
            } else {
                // Within a block: compute shared prefix with previous string
                let prev = &self.strings[i - 1];
                let shared = common_prefix_len(prev, s);
                buffer.extend_from_slice(&encode_vbyte(shared as u64));
                buffer.extend_from_slice(&s.as_bytes()[shared..]);
                buffer.push(0x00); // null terminator
            }
        }

        // Write preamble: type_byte + VByte(string_count) + VByte(buffer_length) + VByte(block_size)
        let mut preamble = Vec::new();
        preamble.push(PFC_SECTION_TYPE);
        preamble.extend_from_slice(&encode_vbyte(self.strings.len() as u64));
        preamble.extend_from_slice(&encode_vbyte(buffer.len() as u64));
        preamble.extend_from_slice(&encode_vbyte(self.block_size as u64));

        // Write preamble + CRC8 (CRC covers type byte + VByte fields)
        writer.write_all(&preamble)?;
        let crc = crc8(&preamble);
        writer.write_all(&[crc])?;

        // Write block offsets as LogArray (with sentinel = buffer_length at end)
        if !block_offsets.is_empty() {
            let sentinel = buffer.len() as u64;
            let max_offset = sentinel.max(1);
            let mut log_array = LogArrayWriter::for_max_value(max_offset);
            for &offset in &block_offsets {
                log_array.push(offset);
            }
            log_array.push(sentinel);
            log_array.write_to(writer)?;
        } else {
            // Empty section: write LogArray with sentinel entry (0) using 0 bits per entry,
            // matching hdt-java's format for empty PFC sections.
            let mut log_array = LogArrayWriter::new(0);
            log_array.push(0);
            log_array.write_to(writer)?;
        }

        // Write encoded string buffer + CRC32C
        writer.write_all(&buffer)?;
        let crc = crc32c(&buffer);
        writer.write_all(&crc.to_le_bytes())?;

        Ok(())
    }
}

#[cfg(test)]
impl Default for PfcEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of finishing a streaming PFC encoder.
pub struct PfcSectionFile {
    /// Path to the complete PFC section file (preamble + LogArray + buffer + CRC32C).
    pub path: PathBuf,
    /// Total size of the section file in bytes.
    pub size: u64,
}

/// Streaming PFC encoder that writes blocks to temp files as they fill.
///
/// Unlike `PfcEncoder` which holds all strings in memory, this encoder uses O(block_size)
/// memory regardless of dictionary size. Encoded string data and block offsets are written
/// to temp files incrementally. On `finish()`, the complete PFC section is assembled.
pub struct StreamingPfcEncoder {
    block_size: usize,
    string_count: u64,
    total_buffer_bytes: u64,

    /// Current block of strings (at most block_size entries).
    current_block: Vec<String>,
    /// Last string pushed (for sort-order validation across blocks).
    last_string: Option<String>,

    /// Writer for encoded string data (zstd-compressed).
    string_buf_writer: zstd::Encoder<'static, BufWriter<File>>,
    /// Writer for block offsets (zstd-compressed, raw u64 LE).
    offsets_writer: zstd::Encoder<'static, BufWriter<File>>,
    /// Number of block offsets written.
    num_offsets: u64,

    /// Incremental CRC32C over the string buffer.
    string_buf_crc: crc::Digest<'static, u32>,

    /// Paths for intermediate temp files.
    string_buf_path: PathBuf,
    offsets_path: PathBuf,
    /// Directory for the final output file.
    output_dir: PathBuf,
    /// Section name for temp file naming.
    section_name: String,
}

impl StreamingPfcEncoder {
    /// Create a new streaming PFC encoder that writes to temp files in `temp_dir`.
    ///
    /// `section_name` is used for temp file naming (e.g., "shared", "subjects").
    pub fn new(temp_dir: &Path, section_name: &str) -> io::Result<Self> {
        let string_buf_path = temp_dir.join(format!("pfc_{section_name}_strings.tmp"));
        let offsets_path = temp_dir.join(format!("pfc_{section_name}_offsets.tmp"));

        let string_buf_file = File::create(&string_buf_path)?;
        let offsets_file = File::create(&offsets_path)?;

        let string_buf_encoder = zstd::Encoder::new(BufWriter::new(string_buf_file), 3)?;
        let offsets_encoder = zstd::Encoder::new(BufWriter::new(offsets_file), 3)?;

        Ok(Self {
            block_size: DEFAULT_BLOCK_SIZE,
            string_count: 0,
            total_buffer_bytes: 0,
            current_block: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
            last_string: None,
            string_buf_writer: string_buf_encoder,
            offsets_writer: offsets_encoder,
            num_offsets: 0,
            string_buf_crc: CRC32C_ALGO.digest(),
            string_buf_path,
            offsets_path,
            output_dir: temp_dir.to_path_buf(),
            section_name: section_name.to_string(),
        })
    }

    /// Add a string to the dictionary section.
    ///
    /// Strings MUST be added in sorted order.
    pub fn push(&mut self, s: &str) -> io::Result<()> {
        debug_assert!(
            self.last_string.as_ref().is_none_or(|prev| prev.as_str() < s),
            "Strings must be added in sorted order"
        );

        self.current_block.push(s.to_string());

        if self.current_block.len() == self.block_size {
            self.flush_block()?;
        }

        Ok(())
    }

    /// Flush the current block to the string buffer temp file.
    fn flush_block(&mut self) -> io::Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // Record block offset
        self.offsets_writer
            .write_all(&self.total_buffer_bytes.to_le_bytes())?;
        self.num_offsets += 1;

        // Encode block
        for (i, s) in self.current_block.iter().enumerate() {
            if i == 0 {
                // First string in block: written verbatim + null terminator
                let bytes = s.as_bytes();
                self.string_buf_writer.write_all(bytes)?;
                self.string_buf_writer.write_all(&[0x00])?;
                self.string_buf_crc.update(bytes);
                self.string_buf_crc.update(&[0x00]);
                self.total_buffer_bytes += bytes.len() as u64 + 1;
            } else {
                // Within block: VByte(shared_prefix_len) + suffix + null
                let prev = &self.current_block[i - 1];
                let shared = common_prefix_len(prev, s);
                let vbyte = encode_vbyte(shared as u64);
                let suffix = &s.as_bytes()[shared..];

                self.string_buf_writer.write_all(&vbyte)?;
                self.string_buf_writer.write_all(suffix)?;
                self.string_buf_writer.write_all(&[0x00])?;
                self.string_buf_crc.update(&vbyte);
                self.string_buf_crc.update(suffix);
                self.string_buf_crc.update(&[0x00]);
                self.total_buffer_bytes += vbyte.len() as u64 + suffix.len() as u64 + 1;
            }
        }

        self.string_count += self.current_block.len() as u64;
        self.last_string = self.current_block.last().cloned();
        self.current_block.clear();

        Ok(())
    }

    /// Finish encoding and assemble the complete PFC section file.
    ///
    /// Returns the path and size of the output file. Cleans up intermediate temp files.
    pub fn finish(mut self) -> io::Result<PfcSectionFile> {
        // Flush any remaining partial block
        self.flush_block()?;

        // Finalize zstd encoders (flush + write end frame)
        self.string_buf_writer.finish()?;
        self.offsets_writer.finish()?;

        // Finalize string buffer CRC
        let string_buf_crc = self.string_buf_crc.finalize();

        // Create output file for the complete PFC section
        let output_path = self
            .output_dir
            .join(format!("pfc_{}_section.tmp", self.section_name));
        let output_file = File::create(&output_path)?;
        let mut writer = BufWriter::new(output_file);

        // 1. Write preamble + CRC8
        let mut preamble = Vec::new();
        preamble.push(PFC_SECTION_TYPE);
        preamble.extend_from_slice(&encode_vbyte(self.string_count));
        preamble.extend_from_slice(&encode_vbyte(self.total_buffer_bytes));
        preamble.extend_from_slice(&encode_vbyte(self.block_size as u64));
        writer.write_all(&preamble)?;
        writer.write_all(&[crc8(&preamble)])?;

        // 2. Write block offsets as LogArray (with sentinel)
        if self.num_offsets > 0 {
            let sentinel = self.total_buffer_bytes;
            let max_offset = sentinel.max(1);
            let num_entries = self.num_offsets + 1; // offsets + sentinel

            // Write LogArray preamble
            let bits_per_entry = crate::io::log_array::bits_for(max_offset);
            let mut la_preamble = Vec::new();
            la_preamble.push(1u8); // TYPE_LOG
            la_preamble.push(bits_per_entry);
            la_preamble.extend_from_slice(&encode_vbyte(num_entries));
            writer.write_all(&la_preamble)?;
            writer.write_all(&[crc8(&la_preamble)])?;

            // Stream offsets through StreamingLogArrayEncoder wrapped in Crc32cWriter
            let crc_writer = Crc32cWriter::new(&mut writer);
            let mut la_encoder = StreamingLogArrayEncoder::new(bits_per_entry, crc_writer);

            // Read offsets back from zstd-compressed temp file
            let offsets_file = File::open(&self.offsets_path)?;
            let mut offsets_reader = zstd::Decoder::new(BufReader::new(offsets_file))?;
            let mut offset_buf = [0u8; 8];
            for _ in 0..self.num_offsets {
                offsets_reader.read_exact(&mut offset_buf)?;
                let offset = u64::from_le_bytes(offset_buf);
                la_encoder.push(offset)?;
            }
            // Push sentinel
            la_encoder.push(sentinel)?;

            let (_num_entries, _bits, crc_writer) = la_encoder.finish()?;
            crc_writer.finalize_and_write()?;
        } else {
            // Empty section: LogArray with sentinel entry (0) using 0 bits per entry
            let mut log_array = LogArrayWriter::new(0);
            log_array.push(0);
            log_array.write_to(&mut writer)?;
        }

        // 3. Decompress string buffer from temp file and copy to output + CRC32C
        {
            let string_buf_file = File::open(&self.string_buf_path)?;
            let mut reader = zstd::Decoder::new(BufReader::new(string_buf_file))?;
            io::copy(&mut reader, &mut writer)?;
        }
        writer.write_all(&string_buf_crc.to_le_bytes())?;

        writer.flush()?;
        drop(writer);

        // Get output file size
        let size = std::fs::metadata(&output_path)?.len();

        // Clean up intermediate temp files
        let _ = std::fs::remove_file(&self.string_buf_path);
        let _ = std::fs::remove_file(&self.offsets_path);

        Ok(PfcSectionFile {
            path: output_path,
            size,
        })
    }
}

/// Compute the length of the common prefix between two strings (in bytes).
fn common_prefix_len(a: &str, b: &str) -> usize {
    a.as_bytes()
        .iter()
        .zip(b.as_bytes())
        .take_while(|(x, y)| x == y)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::log_array::LogArrayReader;
    use crate::io::vbyte::decode_vbyte;
    use std::io::{Cursor, Read};

    /// Decoder for reading a PFC dictionary section (test-only for now).
    struct PfcDecoder {
        strings: Vec<String>,
    }

    impl PfcDecoder {
        fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
            let mut preamble_buf = Vec::new();

            // Read type byte (0x02 = PFC)
            let mut type_byte = [0u8; 1];
            reader.read_exact(&mut type_byte)?;
            preamble_buf.push(type_byte[0]);

            let string_count = read_vbyte_tracking(reader, &mut preamble_buf)?;
            let buffer_length = read_vbyte_tracking(reader, &mut preamble_buf)?;
            let _block_size = read_vbyte_tracking(reader, &mut preamble_buf)?;

            let mut crc_byte = [0u8; 1];
            reader.read_exact(&mut crc_byte)?;
            let expected_crc = crc8(&preamble_buf);
            if crc_byte[0] != expected_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "PFC preamble CRC8 mismatch",
                ));
            }

            let block_offsets = LogArrayReader::read_from(reader)?;

            let mut buffer = vec![0u8; buffer_length as usize];
            reader.read_exact(&mut buffer)?;

            let mut crc_buf = [0u8; 4];
            reader.read_exact(&mut crc_buf)?;
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32c(&buffer);
            if stored_crc != computed_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "PFC buffer CRC32C mismatch",
                ));
            }

            let mut strings = Vec::with_capacity(string_count as usize);
            let mut pos = 0usize;
            let mut prev_string = String::new();

            for i in 0..string_count as usize {
                let is_block_start = is_at_block_offset(&block_offsets, pos as u64);

                if is_block_start || i == 0 {
                    let end = buffer[pos..]
                        .iter()
                        .position(|&b| b == 0)
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "Missing null terminator in PFC")
                        })?;
                    let s = String::from_utf8(buffer[pos..pos + end].to_vec()).map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, e)
                    })?;
                    pos += end + 1;
                    prev_string = s.clone();
                    strings.push(s);
                } else {
                    let (shared_len, vbyte_size) = decode_vbyte(&buffer[pos..])?;
                    pos += vbyte_size;

                    let end = buffer[pos..]
                        .iter()
                        .position(|&b| b == 0)
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "Missing null terminator in PFC")
                        })?;
                    let suffix = &buffer[pos..pos + end];
                    pos += end + 1;

                    let shared_len = shared_len as usize;
                    let mut s = String::with_capacity(shared_len + suffix.len());
                    s.push_str(&prev_string[..shared_len]);
                    s.push_str(
                        std::str::from_utf8(suffix)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                    );
                    prev_string = s.clone();
                    strings.push(s);
                }
            }

            Ok(Self { strings })
        }

        fn len(&self) -> usize {
            self.strings.len()
        }

        fn get(&self, index: usize) -> Option<&str> {
            self.strings.get(index).map(|s| s.as_str())
        }
    }

    fn is_at_block_offset(offsets: &LogArrayReader, pos: u64) -> bool {
        for i in 0..offsets.len() {
            if offsets.get(i) == pos {
                return true;
            }
        }
        false
    }

    fn read_vbyte_tracking<R: Read>(reader: &mut R, buf: &mut Vec<u8>) -> io::Result<u64> {
        let mut value: u64 = 0;
        let mut shift = 0u32;
        let mut byte_buf = [0u8; 1];

        loop {
            reader.read_exact(&mut byte_buf)?;
            let byte = byte_buf[0];
            buf.push(byte);
            value |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 != 0 {
                // MSB=1: last byte
                return Ok(value);
            }
            shift += 7;
            if shift >= 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "VByte value exceeds u64 range",
                ));
            }
        }
    }

    #[test]
    fn test_common_prefix_len() {
        assert_eq!(common_prefix_len("", ""), 0);
        assert_eq!(common_prefix_len("abc", "abd"), 2);
        assert_eq!(common_prefix_len("abc", "abc"), 3);
        assert_eq!(common_prefix_len("abc", "xyz"), 0);
        assert_eq!(
            common_prefix_len("http://example.org/a", "http://example.org/b"),
            19
        );
    }

    #[test]
    fn test_roundtrip_empty() {
        let encoder = PfcEncoder::new();
        let mut buf = Vec::new();
        encoder.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoder = PfcDecoder::read_from(&mut cursor).unwrap();
        assert_eq!(decoder.len(), 0);
    }

    #[test]
    fn test_roundtrip_single_string() {
        let mut encoder = PfcEncoder::new();
        encoder.push("http://example.org/resource1");

        let mut buf = Vec::new();
        encoder.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoder = PfcDecoder::read_from(&mut cursor).unwrap();
        assert_eq!(decoder.len(), 1);
        assert_eq!(decoder.get(0), Some("http://example.org/resource1"));
    }

    #[test]
    fn test_roundtrip_within_one_block() {
        let mut encoder = PfcEncoder::with_block_size(16);
        let strings = vec![
            "http://example.org/a",
            "http://example.org/b",
            "http://example.org/c",
        ];
        for s in &strings {
            encoder.push(*s);
        }

        let mut buf = Vec::new();
        encoder.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoder = PfcDecoder::read_from(&mut cursor).unwrap();
        assert_eq!(decoder.len(), strings.len());
        for (i, s) in strings.iter().enumerate() {
            assert_eq!(decoder.get(i), Some(*s), "mismatch at index {i}");
        }
    }

    #[test]
    fn test_roundtrip_multiple_blocks() {
        let mut encoder = PfcEncoder::with_block_size(3);
        let strings: Vec<String> = (0..10)
            .map(|i| format!("http://example.org/resource{i:03}"))
            .collect();
        for s in &strings {
            encoder.push(s.as_str());
        }

        let mut buf = Vec::new();
        encoder.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoder = PfcDecoder::read_from(&mut cursor).unwrap();
        assert_eq!(decoder.len(), strings.len());
        for (i, s) in strings.iter().enumerate() {
            assert_eq!(decoder.get(i), Some(s.as_str()), "mismatch at index {i}");
        }
    }

    #[test]
    fn test_roundtrip_diverse_strings() {
        let mut encoder = PfcEncoder::with_block_size(4);
        let mut strings = vec![
            "\"literal value\"",
            "\"literal with lang\"@en",
            "\"typed literal\"^^<http://www.w3.org/2001/XMLSchema#date>",
            "http://example.org/resource1",
            "http://example.org/resource2",
            "http://example.org/resource3",
            "_:blank1",
            "_:blank2",
        ];
        strings.sort();
        for s in &strings {
            encoder.push(*s);
        }

        let mut buf = Vec::new();
        encoder.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoder = PfcDecoder::read_from(&mut cursor).unwrap();
        assert_eq!(decoder.len(), strings.len());
        for (i, s) in strings.iter().enumerate() {
            assert_eq!(decoder.get(i), Some(*s), "mismatch at index {i}");
        }
    }

    #[test]
    fn test_front_coding_efficiency() {
        // Strings with long common prefixes should compress well
        let mut encoder = PfcEncoder::with_block_size(4);
        let strings: Vec<String> = (0..8)
            .map(|i| format!("http://www.example.org/very/long/path/resource{i}"))
            .collect();
        for s in &strings {
            encoder.push(s.as_str());
        }

        let mut buf = Vec::new();
        encoder.write_to(&mut buf).unwrap();

        // Encoded size should be much less than the sum of string lengths
        let total_raw: usize = strings.iter().map(|s| s.len()).sum();
        assert!(
            buf.len() < total_raw,
            "PFC encoding should be smaller than raw: {} >= {total_raw}",
            buf.len()
        );
    }

    #[test]
    fn test_crc_corruption_preamble() {
        let mut encoder = PfcEncoder::new();
        encoder.push("test");

        let mut buf = Vec::new();
        encoder.write_to(&mut buf).unwrap();

        // Corrupt first byte (string count VByte)
        buf[0] ^= 0xFF;

        let mut cursor = Cursor::new(&buf);
        assert!(PfcDecoder::read_from(&mut cursor).is_err());
    }

    // --- StreamingPfcEncoder tests ---

    /// Helper: compare streaming encoder output against in-memory PfcEncoder (bit-exact).
    fn assert_streaming_matches_inmemory(strings: &[&str]) {
        let temp_dir = tempfile::TempDir::new().unwrap();

        // In-memory encoder
        let mut inmem = PfcEncoder::new();
        for &s in strings {
            inmem.push(s);
        }
        let mut inmem_buf = Vec::new();
        inmem.write_to(&mut inmem_buf).unwrap();

        // Streaming encoder
        let mut streaming = StreamingPfcEncoder::new(temp_dir.path(), "test").unwrap();
        for &s in strings {
            streaming.push(s).unwrap();
        }
        let section_file = streaming.finish().unwrap();

        // Read the streaming output
        let streaming_buf = std::fs::read(&section_file.path).unwrap();

        assert_eq!(
            inmem_buf, streaming_buf,
            "Streaming encoder output differs from in-memory encoder (len {} vs {})",
            inmem_buf.len(),
            streaming_buf.len()
        );
        assert_eq!(section_file.size, streaming_buf.len() as u64);

        // Also verify the streaming output is decodable
        let mut cursor = Cursor::new(&streaming_buf);
        let decoder = PfcDecoder::read_from(&mut cursor).unwrap();
        assert_eq!(decoder.len(), strings.len());
        for (i, &s) in strings.iter().enumerate() {
            assert_eq!(decoder.get(i), Some(s), "mismatch at index {i}");
        }
    }

    #[test]
    fn test_streaming_empty() {
        assert_streaming_matches_inmemory(&[]);
    }

    #[test]
    fn test_streaming_single_string() {
        assert_streaming_matches_inmemory(&["http://example.org/resource1"]);
    }

    #[test]
    fn test_streaming_within_one_block() {
        assert_streaming_matches_inmemory(&[
            "http://example.org/a",
            "http://example.org/b",
            "http://example.org/c",
        ]);
    }

    #[test]
    fn test_streaming_exactly_one_block() {
        // Exactly 16 strings = one full block
        let strings: Vec<String> = (0..16)
            .map(|i| format!("http://example.org/r{i:04}"))
            .collect();
        let refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        assert_streaming_matches_inmemory(&refs);
    }

    #[test]
    fn test_streaming_multiple_blocks() {
        // 40 strings = 2 full blocks + 8 remaining
        let strings: Vec<String> = (0..40)
            .map(|i| format!("http://example.org/resource{i:04}"))
            .collect();
        let refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        assert_streaming_matches_inmemory(&refs);
    }

    #[test]
    fn test_streaming_block_plus_one() {
        // 17 strings = 1 full block + 1 remaining
        let strings: Vec<String> = (0..17)
            .map(|i| format!("http://example.org/r{i:04}"))
            .collect();
        let refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        assert_streaming_matches_inmemory(&refs);
    }

    #[test]
    fn test_streaming_diverse_strings() {
        let mut strings = vec![
            "\"literal value\"",
            "\"literal with lang\"@en",
            "\"typed literal\"^^<http://www.w3.org/2001/XMLSchema#date>",
            "http://example.org/resource1",
            "http://example.org/resource2",
            "http://example.org/resource3",
            "_:blank1",
            "_:blank2",
        ];
        strings.sort();
        assert_streaming_matches_inmemory(&strings);
    }

    #[test]
    fn test_streaming_many_blocks() {
        // 200 strings across many blocks
        let strings: Vec<String> = (0..200)
            .map(|i| format!("http://example.org/term{i:06}"))
            .collect();
        let refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        assert_streaming_matches_inmemory(&refs);
    }
}
