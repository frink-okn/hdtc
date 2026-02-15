//! Plain Front Coding (PFC) dictionary section encoder and decoder for HDT.
//!
//! PFC encodes a sorted list of strings using front coding in blocks:
//! - First string in each block: stored verbatim + null terminator
//! - Subsequent strings: VByte(shared_prefix_length) + suffix + null terminator
//!
//! Section layout:
//! VByte(string_count) + VByte(buffer_length) + CRC8
//! + LogArray(block_offsets) + Buffer(encoded_strings) + CRC32C

use crate::io::crc_utils::{crc8, crc32c};
use crate::io::log_array::LogArrayWriter;
use crate::io::vbyte::encode_vbyte;
use std::io::{self, Write};

/// Default number of strings per block.
const DEFAULT_BLOCK_SIZE: usize = 16;

/// Encoder for building a PFC dictionary section.
pub struct PfcEncoder {
    block_size: usize,
    strings: Vec<String>,
}

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
            self.strings.last().map_or(true, |prev| prev.as_str() < s.as_str()),
            "Strings must be added in sorted order"
        );
        self.strings.push(s);
    }

    /// Number of strings added.
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// Whether no strings have been added.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
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

        // Write preamble: VByte(string_count) + VByte(buffer_length)
        let mut preamble = Vec::new();
        preamble.extend_from_slice(&encode_vbyte(self.strings.len() as u64));
        preamble.extend_from_slice(&encode_vbyte(buffer.len() as u64));

        // Write preamble + CRC8
        writer.write_all(&preamble)?;
        let crc = crc8(&preamble);
        writer.write_all(&[crc])?;

        // Write block offsets as LogArray
        if !block_offsets.is_empty() {
            let max_offset = *block_offsets.last().unwrap();
            let mut log_array = LogArrayWriter::for_max_value(max_offset.max(1));
            for &offset in &block_offsets {
                log_array.push(offset);
            }
            log_array.write_to(writer)?;
        } else {
            // Empty array - still need to write a valid LogArray
            let log_array = LogArrayWriter::new(1);
            log_array.write_to(writer)?;
        }

        // Write encoded string buffer + CRC32C
        writer.write_all(&buffer)?;
        let crc = crc32c(&buffer);
        writer.write_all(&crc.to_le_bytes())?;

        Ok(())
    }
}

impl Default for PfcEncoder {
    fn default() -> Self {
        Self::new()
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
            let string_count = read_vbyte_tracking(reader, &mut preamble_buf)?;
            let buffer_length = read_vbyte_tracking(reader, &mut preamble_buf)?;

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
            if byte & 0x80 == 0 {
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
            "\"typed literal\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "<http://example.org/resource1>",
            "<http://example.org/resource2>",
            "<http://example.org/resource3>",
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
}
