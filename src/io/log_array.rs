//! LogArray (Log64) - bit-packed integer arrays for HDT.
//!
//! Stores a sequence of unsigned integers using a fixed number of bits per entry,
//! where the bit width is `ceil(log2(max_value + 1))`.
//!
//! Binary format:
//! - Preamble: format byte (TYPE_LOG = 1) + VByte(num_entries) + VByte(bits_per_entry) + CRC8
//! - Data: bit-packed entries in little-endian u64 words + CRC32C

use crate::io::crc_utils::{crc8, crc32c};
use crate::io::vbyte::{encode_vbyte, decode_vbyte};
use std::io::{self, Read, Write};

/// Format type byte for LogArray.
const TYPE_LOG: u8 = 1;

/// Compute the number of bits needed to represent a value.
/// Returns 1 for value 0 (need at least 1 bit).
pub fn bits_for(max_value: u64) -> u8 {
    if max_value == 0 {
        1
    } else {
        (64 - max_value.leading_zeros()) as u8
    }
}

/// Number of u64 words needed to store `count` entries at `bits` bits each.
fn words_needed(count: u64, bits: u8) -> u64 {
    let total_bits = count * bits as u64;
    (total_bits + 63) / 64
}

/// Writer for building a LogArray incrementally.
pub struct LogArrayWriter {
    entries: Vec<u64>,
    bits_per_entry: u8,
}

impl LogArrayWriter {
    /// Create a new LogArrayWriter with the specified bits per entry.
    pub fn new(bits_per_entry: u8) -> Self {
        assert!(bits_per_entry > 0 && bits_per_entry <= 64);
        Self {
            entries: Vec::new(),
            bits_per_entry,
        }
    }

    /// Create a LogArrayWriter with bits calculated from the maximum value.
    pub fn for_max_value(max_value: u64) -> Self {
        Self::new(bits_for(max_value))
    }

    /// Add a value to the array.
    pub fn push(&mut self, value: u64) {
        debug_assert!(
            self.bits_per_entry == 64 || value < (1u64 << self.bits_per_entry),
            "Value {value} does not fit in {} bits",
            self.bits_per_entry
        );
        self.entries.push(value);
    }

    /// Number of entries added so far.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the array is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Bits per entry.
    pub fn bits_per_entry(&self) -> u8 {
        self.bits_per_entry
    }

    /// Serialize the LogArray to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Build preamble: type + VByte(num_entries) + VByte(bits_per_entry)
        let mut preamble = Vec::new();
        preamble.push(TYPE_LOG);
        preamble.extend_from_slice(&encode_vbyte(self.entries.len() as u64));
        preamble.extend_from_slice(&encode_vbyte(self.bits_per_entry as u64));

        // Write preamble + CRC8
        writer.write_all(&preamble)?;
        let crc = crc8(&preamble);
        writer.write_all(&[crc])?;

        // Pack entries into u64 words
        let num_words = words_needed(self.entries.len() as u64, self.bits_per_entry) as usize;
        let mut words = vec![0u64; num_words];

        for (i, &value) in self.entries.iter().enumerate() {
            let bit_pos = i as u64 * self.bits_per_entry as u64;
            let word_idx = (bit_pos / 64) as usize;
            let bit_offset = (bit_pos % 64) as u32;

            words[word_idx] |= value << bit_offset;

            // Handle overflow into next word
            if bit_offset + self.bits_per_entry as u32 > 64 && word_idx + 1 < num_words {
                words[word_idx + 1] |= value >> (64 - bit_offset);
            }
        }

        // Serialize words as little-endian bytes
        let mut data = Vec::with_capacity(num_words * 8);
        for &word in &words {
            data.extend_from_slice(&word.to_le_bytes());
        }

        // Write data + CRC32C
        writer.write_all(&data)?;
        let crc = crc32c(&data);
        writer.write_all(&crc.to_le_bytes())?;

        Ok(())
    }
}

/// Reader for decoding a LogArray from bytes.
pub struct LogArrayReader {
    words: Vec<u64>,
    num_entries: u64,
    bits_per_entry: u8,
}

impl LogArrayReader {
    /// Read a LogArray from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read and verify preamble
        let mut preamble_buf = Vec::new();

        // Read type byte
        let mut type_byte = [0u8; 1];
        reader.read_exact(&mut type_byte)?;
        if type_byte[0] != TYPE_LOG {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected LogArray type byte {TYPE_LOG}, got {}", type_byte[0]),
            ));
        }
        preamble_buf.push(type_byte[0]);

        // Read num_entries VByte
        let num_entries = read_vbyte_tracking(reader, &mut preamble_buf)?;

        // Read bits_per_entry VByte
        let bits_per_entry = read_vbyte_tracking(reader, &mut preamble_buf)? as u8;

        // Read and verify CRC8
        let mut crc_byte = [0u8; 1];
        reader.read_exact(&mut crc_byte)?;
        let expected_crc = crc8(&preamble_buf);
        if crc_byte[0] != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "LogArray preamble CRC8 mismatch: expected {expected_crc:#04x}, got {:#04x}",
                    crc_byte[0]
                ),
            ));
        }

        // Read data words
        let num_words = words_needed(num_entries, bits_per_entry) as usize;
        let data_bytes = num_words * 8;
        let mut data = vec![0u8; data_bytes];
        reader.read_exact(&mut data)?;

        // Read and verify CRC32C
        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);
        let computed_crc = crc32c(&data);
        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("LogArray data CRC32C mismatch: expected {computed_crc:#010x}, got {stored_crc:#010x}"),
            ));
        }

        // Parse words
        let mut words = Vec::with_capacity(num_words);
        for chunk in data.chunks_exact(8) {
            words.push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }

        Ok(Self {
            words,
            num_entries,
            bits_per_entry,
        })
    }

    /// Get the value at the given index.
    pub fn get(&self, index: u64) -> u64 {
        assert!(index < self.num_entries, "Index out of bounds");

        let bit_pos = index * self.bits_per_entry as u64;
        let word_idx = (bit_pos / 64) as usize;
        let bit_offset = (bit_pos % 64) as u32;

        let mask = if self.bits_per_entry == 64 {
            u64::MAX
        } else {
            (1u64 << self.bits_per_entry) - 1
        };

        let mut value = (self.words[word_idx] >> bit_offset) & mask;

        // Handle spanning two words
        if bit_offset + self.bits_per_entry as u32 > 64 {
            let remaining = bit_offset + self.bits_per_entry as u32 - 64;
            let upper_mask = (1u64 << remaining) - 1;
            value |= (self.words[word_idx + 1] & upper_mask) << (64 - bit_offset);
        }

        value
    }

    /// Number of entries in the array.
    pub fn len(&self) -> u64 {
        self.num_entries
    }

    /// Whether the array is empty.
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    /// Bits per entry.
    pub fn bits_per_entry(&self) -> u8 {
        self.bits_per_entry
    }
}

/// Read a VByte value from a reader, appending raw bytes to a tracking buffer.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_bits_for() {
        assert_eq!(bits_for(0), 1);
        assert_eq!(bits_for(1), 1);
        assert_eq!(bits_for(2), 2);
        assert_eq!(bits_for(3), 2);
        assert_eq!(bits_for(4), 3);
        assert_eq!(bits_for(7), 3);
        assert_eq!(bits_for(8), 4);
        assert_eq!(bits_for(255), 8);
        assert_eq!(bits_for(256), 9);
        assert_eq!(bits_for(u64::MAX), 64);
    }

    #[test]
    fn test_empty_array() {
        let writer = LogArrayWriter::new(8);
        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = LogArrayReader::read_from(&mut cursor).unwrap();
        assert_eq!(reader.len(), 0);
        assert!(reader.is_empty());
    }

    #[test]
    fn test_single_entry() {
        let mut writer = LogArrayWriter::new(8);
        writer.push(42);

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = LogArrayReader::read_from(&mut cursor).unwrap();
        assert_eq!(reader.len(), 1);
        assert_eq!(reader.get(0), 42);
    }

    #[test]
    fn test_multiple_entries() {
        let mut writer = LogArrayWriter::for_max_value(100);
        let values = [0, 1, 50, 99, 100];
        for &v in &values {
            writer.push(v);
        }

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = LogArrayReader::read_from(&mut cursor).unwrap();
        assert_eq!(reader.len(), values.len() as u64);
        for (i, &expected) in values.iter().enumerate() {
            assert_eq!(reader.get(i as u64), expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn test_word_boundary_crossing() {
        // Use 13 bits per entry to ensure entries cross u64 word boundaries
        let mut writer = LogArrayWriter::new(13);
        let values: Vec<u64> = (0..20).map(|i| i * 400).collect();
        for &v in &values {
            writer.push(v);
        }

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = LogArrayReader::read_from(&mut cursor).unwrap();
        for (i, &expected) in values.iter().enumerate() {
            assert_eq!(reader.get(i as u64), expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn test_1_bit_entries() {
        let mut writer = LogArrayWriter::new(1);
        let values = [0, 1, 1, 0, 1, 0, 0, 1, 1, 1];
        for &v in &values {
            writer.push(v);
        }

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = LogArrayReader::read_from(&mut cursor).unwrap();
        for (i, &expected) in values.iter().enumerate() {
            assert_eq!(reader.get(i as u64), expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn test_large_values() {
        let mut writer = LogArrayWriter::new(64);
        let values = [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX];
        for &v in &values {
            writer.push(v);
        }

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = LogArrayReader::read_from(&mut cursor).unwrap();
        for (i, &expected) in values.iter().enumerate() {
            assert_eq!(reader.get(i as u64), expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn test_crc_corruption_detected() {
        let mut writer = LogArrayWriter::new(8);
        writer.push(42);

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        // Corrupt a data byte
        let last_data_byte = buf.len() - 5; // before CRC32C
        buf[last_data_byte] ^= 0xFF;

        let mut cursor = Cursor::new(&buf);
        assert!(LogArrayReader::read_from(&mut cursor).is_err());
    }
}
