//! Bitmap - bit sequence storage for HDT.
//!
//! Used by BitmapTriples for the Bp (BitmapY) and Bo (BitmapZ) structures.
//!
//! Binary format:
//! - Preamble: format byte (TYPE_BITMAP = 1) + VByte(num_bits) + CRC8
//! - Data: bits byte-packed in little-endian order + CRC32C
//!
//! Data size is ceil(num_bits / 8) bytes (byte-packed, NOT padded to 64-bit words).

#![allow(dead_code)]

use crate::io::crc_utils::{crc8, crc32c};
use crate::io::vbyte::encode_vbyte;
use std::io::{self, Read, Write};

/// Format type byte for Bitmap (same as LogArray uses type 1, bitmap also uses 1
/// but is distinguished by context).
const TYPE_BITMAP: u8 = 1;

/// Writer for building a Bitmap incrementally.
pub struct BitmapWriter {
    bits: Vec<u64>,
    num_bits: u64,
}

impl BitmapWriter {
    pub fn new() -> Self {
        Self {
            bits: Vec::new(),
            num_bits: 0,
        }
    }

    /// Append a bit (true = 1, false = 0).
    pub fn push(&mut self, value: bool) {
        let word_idx = (self.num_bits / 64) as usize;
        let bit_idx = (self.num_bits % 64) as u32;

        if word_idx >= self.bits.len() {
            self.bits.push(0);
        }

        if value {
            self.bits[word_idx] |= 1u64 << bit_idx;
        }

        self.num_bits += 1;
    }

    /// Append a 1 bit.
    pub fn push_one(&mut self) {
        self.push(true);
    }

    /// Append a 0 bit.
    pub fn push_zero(&mut self) {
        self.push(false);
    }

    /// Set the most recently pushed bit to the given value.
    /// Panics if the bitmap is empty.
    pub fn set_last(&mut self, value: bool) {
        assert!(self.num_bits > 0, "Cannot set_last on empty bitmap");
        let word_idx = ((self.num_bits - 1) / 64) as usize;
        let bit_idx = ((self.num_bits - 1) % 64) as u32;
        if value {
            self.bits[word_idx] |= 1u64 << bit_idx;
        } else {
            self.bits[word_idx] &= !(1u64 << bit_idx);
        }
    }

    /// Number of bits in the bitmap.
    pub fn len(&self) -> u64 {
        self.num_bits
    }

    /// Whether the bitmap is empty.
    pub fn is_empty(&self) -> bool {
        self.num_bits == 0
    }

    /// Serialize the Bitmap to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Build preamble: type + VByte(num_bits)
        let mut preamble = Vec::new();
        preamble.push(TYPE_BITMAP);
        preamble.extend_from_slice(&encode_vbyte(self.num_bits));

        // Write preamble + CRC8
        writer.write_all(&preamble)?;
        let crc = crc8(&preamble);
        writer.write_all(&[crc])?;

        // Serialize as byte-packed data (not padded to 64-bit word boundaries)
        let num_words = if self.num_bits == 0 {
            0
        } else {
            ((self.num_bits - 1) / 64 + 1) as usize
        };
        let data_byte_count = (self.num_bits.div_ceil(8)) as usize;

        let mut data = Vec::with_capacity(data_byte_count);
        for i in 0..num_words {
            let word = if i < self.bits.len() { self.bits[i] } else { 0 };
            let word_bytes = word.to_le_bytes();
            let remaining = data_byte_count - i * 8;
            let take = remaining.min(8);
            data.extend_from_slice(&word_bytes[..take]);
        }

        // Write data + CRC32C
        writer.write_all(&data)?;
        let crc = crc32c(&data);
        writer.write_all(&crc.to_le_bytes())?;

        Ok(())
    }
}

impl Default for BitmapWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Reader for decoding a Bitmap from bytes.
pub struct BitmapReader {
    words: Vec<u64>,
    num_bits: u64,
}

impl BitmapReader {
    /// Read a Bitmap from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read preamble
        let mut preamble_buf = Vec::new();

        // Type byte
        let mut type_byte = [0u8; 1];
        reader.read_exact(&mut type_byte)?;
        if type_byte[0] != TYPE_BITMAP {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected Bitmap type byte {TYPE_BITMAP}, got {}", type_byte[0]),
            ));
        }
        preamble_buf.push(type_byte[0]);

        // VByte(num_bits) - read byte by byte tracking into preamble_buf
        let num_bits = read_vbyte_tracking(reader, &mut preamble_buf)?;

        // Read and verify CRC8
        let mut crc_byte = [0u8; 1];
        reader.read_exact(&mut crc_byte)?;
        let expected_crc = crc8(&preamble_buf);
        if crc_byte[0] != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Bitmap preamble CRC8 mismatch",
            ));
        }

        // Read byte-packed data
        let num_words = if num_bits == 0 {
            0
        } else {
            ((num_bits - 1) / 64 + 1) as usize
        };
        let data_byte_count = (num_bits.div_ceil(8)) as usize;
        let mut data = vec![0u8; data_byte_count];
        reader.read_exact(&mut data)?;

        // Read and verify CRC32C
        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);
        let computed_crc = crc32c(&data);
        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Bitmap data CRC32C mismatch",
            ));
        }

        // Parse into u64 words (last word may be constructed from fewer than 8 bytes)
        let mut words = Vec::with_capacity(num_words);
        for wi in 0..num_words {
            let start = wi * 8;
            let end = (start + 8).min(data_byte_count);
            let mut word_bytes = [0u8; 8];
            word_bytes[..end - start].copy_from_slice(&data[start..end]);
            words.push(u64::from_le_bytes(word_bytes));
        }

        Ok(Self { words, num_bits })
    }

    /// Get the bit at the given index.
    pub fn get(&self, index: u64) -> bool {
        assert!(index < self.num_bits, "Bitmap index out of bounds");
        let word_idx = (index / 64) as usize;
        let bit_idx = (index % 64) as u32;
        (self.words[word_idx] >> bit_idx) & 1 == 1
    }

    /// Number of bits in the bitmap.
    pub fn len(&self) -> u64 {
        self.num_bits
    }

    /// Whether the bitmap is empty.
    pub fn is_empty(&self) -> bool {
        self.num_bits == 0
    }

    /// Count the number of 1 bits up to and including position `pos` (rank1).
    pub fn rank1(&self, pos: u64) -> u64 {
        assert!(pos < self.num_bits);
        let word_idx = (pos / 64) as usize;
        let bit_idx = (pos % 64) as u32;

        let mut count = 0u64;
        for i in 0..word_idx {
            count += self.words[i].count_ones() as u64;
        }
        // Count bits in the last word up to and including bit_idx
        let mask = if bit_idx == 63 {
            u64::MAX
        } else {
            (1u64 << (bit_idx + 1)) - 1
        };
        count += (self.words[word_idx] & mask).count_ones() as u64;
        count
    }

    /// Find the position of the nth 1-bit (1-indexed).
    /// select1(1) returns the position of the first 1-bit.
    pub fn select1(&self, n: u64) -> Option<u64> {
        if n == 0 {
            return None;
        }
        let mut remaining = n;
        for (word_idx, &word) in self.words.iter().enumerate() {
            let ones = word.count_ones() as u64;
            if remaining <= ones {
                // The answer is in this word
                let mut w = word;
                for bit_pos in 0..64 {
                    if w & 1 == 1 {
                        remaining -= 1;
                        if remaining == 0 {
                            return Some(word_idx as u64 * 64 + bit_pos);
                        }
                    }
                    w >>= 1;
                }
            }
            remaining -= ones;
        }
        None
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_empty_bitmap() {
        let writer = BitmapWriter::new();
        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = BitmapReader::read_from(&mut cursor).unwrap();
        assert_eq!(reader.len(), 0);
        assert!(reader.is_empty());
    }

    #[test]
    fn test_single_bit() {
        let mut writer = BitmapWriter::new();
        writer.push_one();

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = BitmapReader::read_from(&mut cursor).unwrap();
        assert_eq!(reader.len(), 1);
        assert!(reader.get(0));
    }

    #[test]
    fn test_mixed_bits() {
        let pattern = [true, false, true, true, false, false, true, false, true, true];
        let mut writer = BitmapWriter::new();
        for &b in &pattern {
            writer.push(b);
        }

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = BitmapReader::read_from(&mut cursor).unwrap();
        assert_eq!(reader.len(), pattern.len() as u64);
        for (i, &expected) in pattern.iter().enumerate() {
            assert_eq!(reader.get(i as u64), expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn test_cross_word_boundary() {
        let mut writer = BitmapWriter::new();
        // Write 100 bits crossing the 64-bit word boundary
        for i in 0..100u64 {
            writer.push(i % 3 == 0);
        }

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = BitmapReader::read_from(&mut cursor).unwrap();
        assert_eq!(reader.len(), 100);
        for i in 0..100u64 {
            assert_eq!(reader.get(i), i % 3 == 0, "mismatch at index {i}");
        }
    }

    #[test]
    fn test_rank1() {
        // Bitmap: 1 0 1 1 0 1 0 0 1
        let bits = [true, false, true, true, false, true, false, false, true];
        let mut writer = BitmapWriter::new();
        for &b in &bits {
            writer.push(b);
        }

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = BitmapReader::read_from(&mut cursor).unwrap();

        assert_eq!(reader.rank1(0), 1); // bit 0 is 1
        assert_eq!(reader.rank1(1), 1); // bits 0-1: one 1
        assert_eq!(reader.rank1(2), 2); // bits 0-2: two 1s
        assert_eq!(reader.rank1(3), 3); // bits 0-3: three 1s
        assert_eq!(reader.rank1(4), 3);
        assert_eq!(reader.rank1(5), 4);
        assert_eq!(reader.rank1(8), 5);
    }

    #[test]
    fn test_select1() {
        // Bitmap: 1 0 1 1 0 1 0 0 1
        let bits = [true, false, true, true, false, true, false, false, true];
        let mut writer = BitmapWriter::new();
        for &b in &bits {
            writer.push(b);
        }

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let reader = BitmapReader::read_from(&mut cursor).unwrap();

        assert_eq!(reader.select1(0), None);
        assert_eq!(reader.select1(1), Some(0)); // first 1 is at position 0
        assert_eq!(reader.select1(2), Some(2)); // second 1 at position 2
        assert_eq!(reader.select1(3), Some(3)); // third 1 at position 3
        assert_eq!(reader.select1(4), Some(5)); // fourth 1 at position 5
        assert_eq!(reader.select1(5), Some(8)); // fifth 1 at position 8
        assert_eq!(reader.select1(6), None);    // no sixth 1
    }

    #[test]
    fn test_crc_corruption() {
        let mut writer = BitmapWriter::new();
        writer.push_one();
        writer.push_zero();
        writer.push_one();

        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        // Corrupt data (3 bits = 1 byte of data + 4 bytes CRC32C)
        let data_start = buf.len() - 5;
        buf[data_start] ^= 0xFF;

        let mut cursor = Cursor::new(&buf);
        assert!(BitmapReader::read_from(&mut cursor).is_err());
    }
}
