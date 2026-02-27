//! LogArray (Log64) - bit-packed integer arrays for HDT.
//!
//! Stores a sequence of unsigned integers using a fixed number of bits per entry,
//! where the bit width is `ceil(log2(max_value + 1))`.
//!
//! Binary format:
//! - Preamble: format byte (TYPE_LOG = 1) + bits_per_entry (raw byte) + VByte(num_entries) + CRC8
//! - Data: bit-packed entries in little-endian byte-packed format + CRC32C
//!
//! Data size is ceil(total_bits / 8) bytes (byte-packed, NOT padded to 64-bit words).

use crate::io::crc_utils::{crc8, crc32c};
use crate::io::vbyte::encode_vbyte;
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
    total_bits.div_ceil(64)
}

/// Number of bytes needed to store `count` entries at `bits` bits each (byte-packed).
fn bytes_needed(count: u64, bits: u8) -> u64 {
    let total_bits = count * bits as u64;
    total_bits.div_ceil(8)
}

/// Writer for building a LogArray incrementally.
#[allow(dead_code)]
pub struct LogArrayWriter {
    entries: Vec<u64>,
    bits_per_entry: u8,
}

#[allow(dead_code)]
impl LogArrayWriter {
    /// Create a new LogArrayWriter with the specified bits per entry.
    /// A bits_per_entry of 0 is allowed (all entries are implicitly 0).
    pub fn new(bits_per_entry: u8) -> Self {
        assert!(bits_per_entry <= 64);
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

    /// Serialize the LogArray to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Build preamble: type + numbits (raw byte) + VByte(num_entries)
        let mut preamble = Vec::new();
        preamble.push(TYPE_LOG);
        preamble.push(self.bits_per_entry);
        preamble.extend_from_slice(&encode_vbyte(self.entries.len() as u64));

        // Write preamble + CRC8
        writer.write_all(&preamble)?;
        let crc = crc8(&preamble);
        writer.write_all(&[crc])?;

        // Pack entries into u64 words
        let num_words = words_needed(self.entries.len() as u64, self.bits_per_entry) as usize;
        let data_byte_count = bytes_needed(self.entries.len() as u64, self.bits_per_entry) as usize;
        let mut data = Vec::with_capacity(data_byte_count);

        if self.bits_per_entry > 0 {
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

            // Serialize as byte-packed data (not padded to 64-bit word boundaries)
            for (wi, &word) in words.iter().enumerate() {
                let word_bytes = word.to_le_bytes();
                let remaining = data_byte_count - wi * 8;
                let take = remaining.min(8);
                data.extend_from_slice(&word_bytes[..take]);
            }
        }

        // Write data + CRC32C
        writer.write_all(&data)?;
        let crc = crc32c(&data);
        writer.write_all(&crc.to_le_bytes())?;

        Ok(())
    }
}

/// Streaming log array encoder that writes bit-packed integers to a `Write` target
/// as u64 words fill up, using O(1) memory regardless of array size.
///
/// Data is written as byte-packed little-endian words. The caller is responsible
/// for writing the preamble (type + bits_per_entry + VByte(num_entries) + CRC8)
/// before the data and the CRC32C after the data.
///
/// Use `Crc32cWriter` as the inner writer to compute CRC32C incrementally.
pub struct StreamingLogArrayEncoder<W: Write> {
    writer: W,
    bits_per_entry: u8,
    num_entries: u64,
    /// The current word being packed into.
    current_word: u64,
    /// The next word (entries can span two words).
    next_word: u64,
    /// Index of `current_word` in the logical word array.
    word_index: u64,
}

impl<W: Write> StreamingLogArrayEncoder<W> {
    pub fn new(bits_per_entry: u8, writer: W) -> Self {
        assert!(bits_per_entry <= 64);
        Self {
            writer,
            bits_per_entry,
            num_entries: 0,
            current_word: 0,
            next_word: 0,
            word_index: 0,
        }
    }

    pub fn for_max_value(max_value: u64, writer: W) -> Self {
        Self::new(bits_for(max_value), writer)
    }

    /// Add a value to the array, flushing completed words to the writer.
    pub fn push(&mut self, value: u64) -> io::Result<()> {
        if self.bits_per_entry == 0 {
            self.num_entries += 1;
            return Ok(());
        }

        debug_assert!(
            self.bits_per_entry == 64 || value < (1u64 << self.bits_per_entry),
            "Value {value} does not fit in {} bits",
            self.bits_per_entry
        );

        let bit_pos = self.num_entries * self.bits_per_entry as u64;
        let target_word = bit_pos / 64;
        let bit_offset = (bit_pos % 64) as u32;

        debug_assert!(target_word == self.word_index);

        // Pack value into current word
        self.current_word |= value << bit_offset;

        // Handle overflow into next word
        if bit_offset + self.bits_per_entry as u32 > 64 {
            self.next_word |= value >> (64 - bit_offset);
        }

        self.num_entries += 1;

        // Check if current_word is fully committed (next entry starts in a later word)
        let next_bit_pos = self.num_entries * self.bits_per_entry as u64;
        let next_word_idx = next_bit_pos / 64;

        if next_word_idx > self.word_index {
            // Flush current_word (all 8 bytes — it's fully packed)
            self.writer.write_all(&self.current_word.to_le_bytes())?;
            self.current_word = self.next_word;
            self.next_word = 0;
            self.word_index = next_word_idx;
        }

        Ok(())
    }

    /// Bits per entry.
    #[allow(dead_code)]
    pub fn bits_per_entry(&self) -> u8 {
        self.bits_per_entry
    }

    /// Number of entries pushed so far.
    #[allow(dead_code)]
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /// Flush the final partial word and return (num_entries, bits_per_entry, inner_writer).
    pub fn finish(mut self) -> io::Result<(u64, u8, W)> {
        if self.bits_per_entry > 0 && self.num_entries > 0 {
            let total_data_bytes =
                bytes_needed(self.num_entries, self.bits_per_entry) as usize;
            let bytes_already_written = self.word_index as usize * 8;
            let remaining = total_data_bytes - bytes_already_written;
            if remaining > 0 {
                let word_bytes = self.current_word.to_le_bytes();
                self.writer.write_all(&word_bytes[..remaining])?;
            }
        }
        Ok((self.num_entries, self.bits_per_entry, self.writer))
    }
}

/// Streaming log array decoder that reads entries sequentially from a `Read` source
/// using O(1) memory, without loading the entire array into RAM.
///
/// Reads the preamble during construction, then lazily reads data words on demand.
/// CRC32C is verified when `finish()` is called.
pub struct StreamingLogArrayDecoder<R: Read> {
    reader: R,
    bits_per_entry: u8,
    num_entries: u64,
    entries_read: u64,
    /// Current 64-bit word being decoded from.
    current_word: u64,
    /// Next word (needed when entries span word boundaries).
    next_word: u64,
    /// Global bit position within the data stream.
    bit_position: u64,
    /// Index of the word currently in `current_word`.
    current_word_index: u64,
    /// Whether next_word has been loaded.
    has_next_word: bool,
    /// Total number of data words.
    total_words: u64,
    /// Words loaded so far (into current_word or next_word).
    words_loaded: u64,
    /// Mask for extracting an entry: (1 << bits_per_entry) - 1.
    mask: u64,
    /// Total data bytes in the section.
    total_data_bytes: u64,
    /// Data bytes read so far.
    data_bytes_read: u64,
    crc_digest: crc::Digest<'static, u32>,
}

impl<R: Read> StreamingLogArrayDecoder<R> {
    /// Create a streaming decoder by reading the log array preamble.
    pub fn new(mut reader: R) -> io::Result<Self> {
        let mut preamble_buf = Vec::new();

        // Type byte
        let mut type_byte = [0u8; 1];
        reader.read_exact(&mut type_byte)?;
        if type_byte[0] != TYPE_LOG {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected LogArray type byte {TYPE_LOG}, got {}", type_byte[0]),
            ));
        }
        preamble_buf.push(type_byte[0]);

        // bits_per_entry (raw byte)
        let mut bits_byte = [0u8; 1];
        reader.read_exact(&mut bits_byte)?;
        let bits_per_entry = bits_byte[0];
        preamble_buf.push(bits_per_entry);

        // VByte(num_entries)
        let num_entries = read_vbyte_tracking(&mut reader, &mut preamble_buf)?;

        // CRC8
        let mut crc_byte = [0u8; 1];
        reader.read_exact(&mut crc_byte)?;
        let expected_crc = crc8(&preamble_buf);
        if crc_byte[0] != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "LogArray preamble CRC8 mismatch",
            ));
        }

        let mask = if bits_per_entry == 64 {
            u64::MAX
        } else if bits_per_entry == 0 {
            0
        } else {
            (1u64 << bits_per_entry) - 1
        };

        let total_words = words_needed(num_entries, bits_per_entry);
        let total_data_bytes = bytes_needed(num_entries, bits_per_entry);

        let mut decoder = Self {
            reader,
            bits_per_entry,
            num_entries,
            entries_read: 0,
            current_word: 0,
            next_word: 0,
            bit_position: 0,
            current_word_index: 0,
            has_next_word: false,
            total_words,
            words_loaded: 0,
            mask,
            total_data_bytes,
            data_bytes_read: 0,
            crc_digest: crate::io::crc_utils::CRC32C_ALGO.digest(),
        };

        // Pre-load first two words if available
        if total_words > 0 {
            decoder.current_word = decoder.read_word()?;
            decoder.words_loaded = 1;
            if total_words > 1 {
                decoder.next_word = decoder.read_word()?;
                decoder.has_next_word = true;
                decoder.words_loaded = 2;
            }
        }

        Ok(decoder)
    }

    /// Number of entries in the array.
    #[allow(dead_code)]
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /// Bits per entry.
    #[allow(dead_code)]
    pub fn bits_per_entry(&self) -> u8 {
        self.bits_per_entry
    }

    /// Read the next word from the data stream.
    fn read_word(&mut self) -> io::Result<u64> {
        let bytes_remaining = self.total_data_bytes - self.data_bytes_read;
        let bytes_to_read = bytes_remaining.min(8) as usize;
        if bytes_to_read == 0 {
            return Ok(0);
        }
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf[..bytes_to_read])?;
        self.crc_digest.update(&buf[..bytes_to_read]);
        self.data_bytes_read += bytes_to_read as u64;
        Ok(u64::from_le_bytes(buf))
    }

    /// Read the next entry. Returns `None` when all entries have been read.
    pub fn next_entry(&mut self) -> io::Result<Option<u64>> {
        if self.entries_read >= self.num_entries {
            return Ok(None);
        }

        if self.bits_per_entry == 0 {
            self.entries_read += 1;
            return Ok(Some(0));
        }

        let bit_offset = (self.bit_position % 64) as u32;

        let mut value = (self.current_word >> bit_offset) & self.mask;

        // Handle entry spanning two words
        if bit_offset as u8 + self.bits_per_entry > 64 {
            let remaining_bits = bit_offset as u8 + self.bits_per_entry - 64;
            let upper_mask = (1u64 << remaining_bits) - 1;
            value |= (self.next_word & upper_mask) << (64 - bit_offset);
        }

        self.bit_position += self.bits_per_entry as u64;
        self.entries_read += 1;

        // Check if we've moved to the next word
        let new_word_index = self.bit_position / 64;
        if new_word_index > self.current_word_index {
            self.current_word = self.next_word;
            self.current_word_index = new_word_index;
            // Load the next word if available
            if self.words_loaded < self.total_words {
                self.next_word = self.read_word()?;
                self.words_loaded += 1;
                self.has_next_word = true;
            } else {
                self.next_word = 0;
                self.has_next_word = false;
            }
        }

        Ok(Some(value))
    }

    /// Verify CRC32C after all entries have been read.
    pub fn finish(mut self) -> io::Result<R> {
        // Read any remaining data bytes
        while self.data_bytes_read < self.total_data_bytes {
            let bytes_remaining = self.total_data_bytes - self.data_bytes_read;
            let bytes_to_read = bytes_remaining.min(8192) as usize;
            let mut buf = vec![0u8; bytes_to_read];
            self.reader.read_exact(&mut buf)?;
            self.crc_digest.update(&buf);
            self.data_bytes_read += bytes_to_read as u64;
        }

        // Read and verify CRC32C
        let mut crc_buf = [0u8; 4];
        self.reader.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);
        let computed_crc = self.crc_digest.finalize();
        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "LogArray data CRC32C mismatch: expected {computed_crc:#010x}, got {stored_crc:#010x}"
                ),
            ));
        }

        Ok(self.reader)
    }
}

/// Reader for decoding a LogArray from bytes.
#[allow(dead_code)]
pub struct LogArrayReader {
    words: Vec<u64>,
    num_entries: u64,
    bits_per_entry: u8,
}

#[allow(dead_code)]
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

        // Read bits_per_entry (raw byte, not VByte)
        let mut bits_byte = [0u8; 1];
        reader.read_exact(&mut bits_byte)?;
        let bits_per_entry = bits_byte[0];
        preamble_buf.push(bits_per_entry);

        // Read num_entries VByte
        let num_entries = read_vbyte_tracking(reader, &mut preamble_buf)?;

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

        // Read byte-packed data
        let num_words = words_needed(num_entries, bits_per_entry) as usize;
        let data_byte_count = bytes_needed(num_entries, bits_per_entry) as usize;
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
                format!("LogArray data CRC32C mismatch: expected {computed_crc:#010x}, got {stored_crc:#010x}"),
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

        Ok(Self {
            words,
            num_entries,
            bits_per_entry,
        })
    }

    /// Get the value at the given index.
    pub fn get(&self, index: u64) -> u64 {
        assert!(index < self.num_entries, "Index out of bounds");

        if self.bits_per_entry == 0 {
            return 0;
        }

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

    /// Approximate heap memory used by this reader (bytes).
    pub fn heap_size(&self) -> usize {
        self.words.len() * std::mem::size_of::<u64>()
    }

    /// Whether the array is empty.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
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

    /// Helper: assemble a full LogArray from streaming encoder output.
    fn assemble_log_array(num_entries: u64, bits_per_entry: u8, data: &[u8], crc: u32) -> Vec<u8> {
        use crate::io::crc_utils::crc8;
        use crate::io::vbyte::encode_vbyte;

        let mut out = Vec::new();
        let mut preamble = Vec::new();
        preamble.push(1u8); // TYPE_LOG
        preamble.push(bits_per_entry);
        preamble.extend_from_slice(&encode_vbyte(num_entries));
        out.extend_from_slice(&preamble);
        out.push(crc8(&preamble));
        out.extend_from_slice(data);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    #[test]
    fn test_streaming_matches_inmemory_small() {
        use crate::io::crc_utils::Crc32cWriter;

        let mut writer = LogArrayWriter::for_max_value(100);
        let values = [0, 1, 50, 99, 100];
        let data_buf: Vec<u8> = Vec::new();
        let crc_writer = Crc32cWriter::new(data_buf);
        let mut encoder = StreamingLogArrayEncoder::for_max_value(100, crc_writer);

        for &v in &values {
            writer.push(v);
            encoder.push(v).unwrap();
        }

        let mut expected = Vec::new();
        writer.write_to(&mut expected).unwrap();

        let (num_entries, bits, crc_writer) = encoder.finish().unwrap();
        let (data_crc, data_buf) = crc_writer.finalize();
        let assembled = assemble_log_array(num_entries, bits, &data_buf, data_crc);

        assert_eq!(assembled, expected, "streaming log array differs from in-memory");
    }

    #[test]
    fn test_streaming_matches_word_boundary() {
        use crate::io::crc_utils::Crc32cWriter;

        // 13-bit entries cross word boundaries frequently
        let mut writer = LogArrayWriter::new(13);
        let values: Vec<u64> = (0..20).map(|i| i * 400).collect();

        let data_buf: Vec<u8> = Vec::new();
        let crc_writer = Crc32cWriter::new(data_buf);
        let mut encoder = StreamingLogArrayEncoder::new(13, crc_writer);

        for &v in &values {
            writer.push(v);
            encoder.push(v).unwrap();
        }

        let mut expected = Vec::new();
        writer.write_to(&mut expected).unwrap();

        let (num_entries, bits, crc_writer) = encoder.finish().unwrap();
        let (data_crc, data_buf) = crc_writer.finalize();
        let assembled = assemble_log_array(num_entries, bits, &data_buf, data_crc);

        assert_eq!(assembled, expected);
    }

    #[test]
    fn test_streaming_matches_64bit() {
        use crate::io::crc_utils::Crc32cWriter;

        let mut writer = LogArrayWriter::new(64);
        let values = [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX];

        let data_buf: Vec<u8> = Vec::new();
        let crc_writer = Crc32cWriter::new(data_buf);
        let mut encoder = StreamingLogArrayEncoder::new(64, crc_writer);

        for &v in &values {
            writer.push(v);
            encoder.push(v).unwrap();
        }

        let mut expected = Vec::new();
        writer.write_to(&mut expected).unwrap();

        let (num_entries, bits, crc_writer) = encoder.finish().unwrap();
        let (data_crc, data_buf) = crc_writer.finalize();
        let assembled = assemble_log_array(num_entries, bits, &data_buf, data_crc);

        assert_eq!(assembled, expected);
    }

    #[test]
    fn test_streaming_matches_many_entries() {
        use crate::io::crc_utils::Crc32cWriter;

        // 1000 entries at 20 bits — tests many word flushes
        let max_val = (1u64 << 20) - 1;
        let mut writer = LogArrayWriter::for_max_value(max_val);

        let data_buf: Vec<u8> = Vec::new();
        let crc_writer = Crc32cWriter::new(data_buf);
        let mut encoder = StreamingLogArrayEncoder::for_max_value(max_val, crc_writer);

        for i in 0..1000u64 {
            let v = i * 1049 % (max_val + 1); // pseudo-random values
            writer.push(v);
            encoder.push(v).unwrap();
        }

        let mut expected = Vec::new();
        writer.write_to(&mut expected).unwrap();

        let (num_entries, bits, crc_writer) = encoder.finish().unwrap();
        let (data_crc, data_buf) = crc_writer.finalize();
        let assembled = assemble_log_array(num_entries, bits, &data_buf, data_crc);

        assert_eq!(assembled, expected);
    }

    #[test]
    fn test_streaming_decoder_matches_reader() {
        let mut writer = LogArrayWriter::for_max_value(100);
        let values = [0, 1, 50, 99, 100];
        for &v in &values {
            writer.push(v);
        }
        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let reader = LogArrayReader::read_from(&mut Cursor::new(&buf)).unwrap();
        let mut decoder = StreamingLogArrayDecoder::new(Cursor::new(&buf)).unwrap();
        assert_eq!(decoder.num_entries(), values.len() as u64);

        for (i, &expected) in values.iter().enumerate() {
            let val = decoder.next_entry().unwrap().unwrap();
            assert_eq!(val, expected, "decoder mismatch at index {i}");
            assert_eq!(val, reader.get(i as u64), "decoder vs reader mismatch at {i}");
        }

        assert!(decoder.next_entry().unwrap().is_none());
        decoder.finish().unwrap();
    }

    #[test]
    fn test_streaming_decoder_word_boundary() {
        // 13-bit entries frequently cross word boundaries
        let mut writer = LogArrayWriter::new(13);
        let values: Vec<u64> = (0..20).map(|i| i * 400).collect();
        for &v in &values {
            writer.push(v);
        }
        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let reader = LogArrayReader::read_from(&mut Cursor::new(&buf)).unwrap();
        let mut decoder = StreamingLogArrayDecoder::new(Cursor::new(&buf)).unwrap();

        for (i, &expected) in values.iter().enumerate() {
            let val = decoder.next_entry().unwrap().unwrap();
            assert_eq!(val, expected, "decoder mismatch at index {i}");
            assert_eq!(val, reader.get(i as u64), "decoder vs reader at {i}");
        }
        decoder.finish().unwrap();
    }

    #[test]
    fn test_streaming_decoder_64bit() {
        let mut writer = LogArrayWriter::new(64);
        let values = [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX];
        for &v in &values {
            writer.push(v);
        }
        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut decoder = StreamingLogArrayDecoder::new(Cursor::new(&buf)).unwrap();
        for (i, &expected) in values.iter().enumerate() {
            let val = decoder.next_entry().unwrap().unwrap();
            assert_eq!(val, expected, "mismatch at index {i}");
        }
        decoder.finish().unwrap();
    }

    #[test]
    fn test_streaming_decoder_many_entries() {
        let max_val = (1u64 << 20) - 1;
        let mut writer = LogArrayWriter::for_max_value(max_val);
        let values: Vec<u64> = (0..1000).map(|i| i * 1049 % (max_val + 1)).collect();
        for &v in &values {
            writer.push(v);
        }
        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let reader = LogArrayReader::read_from(&mut Cursor::new(&buf)).unwrap();
        let mut decoder = StreamingLogArrayDecoder::new(Cursor::new(&buf)).unwrap();

        for (i, &expected) in values.iter().enumerate() {
            let val = decoder.next_entry().unwrap().unwrap();
            assert_eq!(val, expected, "mismatch at index {i}");
            assert_eq!(val, reader.get(i as u64));
        }
        decoder.finish().unwrap();
    }

    #[test]
    fn test_streaming_decoder_empty() {
        let writer = LogArrayWriter::new(8);
        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut decoder = StreamingLogArrayDecoder::new(Cursor::new(&buf)).unwrap();
        assert_eq!(decoder.num_entries(), 0);
        assert!(decoder.next_entry().unwrap().is_none());
        decoder.finish().unwrap();
    }

    #[test]
    fn test_streaming_decoder_1bit() {
        let mut writer = LogArrayWriter::new(1);
        let values = [0, 1, 1, 0, 1, 0, 0, 1, 1, 1];
        for &v in &values {
            writer.push(v);
        }
        let mut buf = Vec::new();
        writer.write_to(&mut buf).unwrap();

        let mut decoder = StreamingLogArrayDecoder::new(Cursor::new(&buf)).unwrap();
        for (i, &expected) in values.iter().enumerate() {
            let val = decoder.next_entry().unwrap().unwrap();
            assert_eq!(val, expected, "mismatch at index {i}");
        }
        decoder.finish().unwrap();
    }

    /// Test streaming decoder with bpe=33 (typical for large HDT object IDs) at scale.
    /// Exercises all 64 possible bit offsets within a word multiple times.
    /// Uses both LogArrayWriter→Decoder and StreamingEncoder→Decoder paths.
    #[test]
    fn test_streaming_decoder_bpe33_large() {
        let bpe: u8 = 33;
        let max_val = (1u64 << bpe) - 1;
        let n = 10_000u64; // enough for ~156 full 64-entry cycles

        // Generate diverse values including edge cases
        let values: Vec<u64> = (0..n)
            .map(|i| match i % 7 {
                0 => 0,                           // zero
                1 => max_val,                      // max
                2 => 1,                            // min nonzero
                3 => i * 839_471 % (max_val + 1),  // pseudo-random
                4 => max_val - (i % 100),          // near-max
                5 => (i * i) % (max_val + 1),      // quadratic
                _ => (max_val / 2) + (i % 1000),   // mid-range
            })
            .collect();

        // Path A: LogArrayWriter → StreamingLogArrayDecoder
        {
            let mut writer = LogArrayWriter::new(bpe);
            for &v in &values {
                writer.push(v);
            }
            let mut buf = Vec::new();
            writer.write_to(&mut buf).unwrap();

            let reader = LogArrayReader::read_from(&mut Cursor::new(&buf)).unwrap();
            let mut decoder = StreamingLogArrayDecoder::new(Cursor::new(&buf)).unwrap();
            assert_eq!(decoder.num_entries(), n);
            assert_eq!(decoder.bits_per_entry(), bpe);

            for (i, &expected) in values.iter().enumerate() {
                let val = decoder.next_entry().unwrap().unwrap();
                assert_eq!(
                    val, expected,
                    "LogArrayWriter→Decoder mismatch at index {i}: got {val}, expected {expected}"
                );
                assert_eq!(val, reader.get(i as u64), "Reader mismatch at index {i}");
            }
            assert!(decoder.next_entry().unwrap().is_none());
            decoder.finish().unwrap();
        }

        // Path B: StreamingLogArrayEncoder → StreamingLogArrayDecoder
        {
            use crate::io::crc_utils::Crc32cWriter;
            use std::io::BufWriter;

            let data_buf: Vec<u8> = Vec::new();
            let crc_writer = Crc32cWriter::new(BufWriter::new(data_buf));
            let mut encoder = StreamingLogArrayEncoder::new(bpe, crc_writer);

            for &v in &values {
                encoder.push(v).unwrap();
            }
            let (num_entries, bpe_out, crc_writer) = encoder.finish().unwrap();
            assert_eq!(num_entries, n);
            assert_eq!(bpe_out, bpe);

            let (data_crc, buf_writer) = crc_writer.finalize();
            let data: Vec<u8> = buf_writer.into_inner().unwrap();

            // Build complete serialized form: preamble + CRC8 + data + CRC32C
            let mut buf = Vec::new();
            let mut preamble = Vec::new();
            preamble.push(TYPE_LOG);
            preamble.push(bpe);
            preamble.extend_from_slice(&encode_vbyte(n));
            buf.extend_from_slice(&preamble);
            buf.push(crc8(&preamble));
            buf.extend_from_slice(&data);
            buf.extend_from_slice(&data_crc.to_le_bytes());

            let mut decoder = StreamingLogArrayDecoder::new(Cursor::new(&buf)).unwrap();
            for (i, &expected) in values.iter().enumerate() {
                let val = decoder.next_entry().unwrap().unwrap();
                assert_eq!(
                    val, expected,
                    "StreamingEncoder→Decoder mismatch at index {i}: got {val}, expected {expected}"
                );
            }
            assert!(decoder.next_entry().unwrap().is_none());
            decoder.finish().unwrap();
        }
    }

    /// Test streaming decoder with every bpe from 1 to 64 over multiple word boundaries.
    /// Catches bit-extraction bugs specific to certain alignment patterns.
    #[test]
    fn test_streaming_decoder_all_bpe_values() {
        for bpe in 1..=64u8 {
            let max_val = if bpe == 64 { u64::MAX } else { (1u64 << bpe) - 1 };
            // 200 entries exercises ~3 full 64-entry cycles for any bpe
            let values: Vec<u64> = (0..200u64)
                .map(|i| (i * 7919 + 1) % (max_val.min(1_000_000) + 1))
                .collect();

            let mut writer = LogArrayWriter::new(bpe);
            for &v in &values {
                writer.push(v);
            }
            let mut buf = Vec::new();
            writer.write_to(&mut buf).unwrap();

            let mut decoder = StreamingLogArrayDecoder::new(Cursor::new(&buf)).unwrap();
            for (i, &expected) in values.iter().enumerate() {
                let val = decoder.next_entry().unwrap().unwrap();
                assert_eq!(
                    val, expected,
                    "bpe={bpe} mismatch at index {i}: got {val}, expected {expected}"
                );
            }
            decoder.finish().unwrap();
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
