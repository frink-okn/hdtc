//! Forward-only bit packing into little-endian `u64` words.
//!
//! Values are packed least-significant-bit first into a sequence of 64-bit
//! little-endian words, straddling word boundaries freely. It is the packing
//! `docs/keyset-format.md` §4.3 specifies for Elias-Fano, and it lives here
//! rather than beside its one caller so that a second format needing the same
//! packing gets the same implementation.

use anyhow::Result;
use std::io::Write;

/// Packs bits into little-endian `u64` words, least-significant bit first,
/// flushing each word as it fills.
pub struct BitPacker<W: Write> {
    writer: W,
    word: u64,
    /// Bits already placed in `word`; always in `0..64`.
    used: u32,
    words: u64,
}

impl<W: Write> BitPacker<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            word: 0,
            used: 0,
            words: 0,
        }
    }

    /// Append the low `width` bits of `value`. `width` may be 0 or 64.
    pub fn push_bits(&mut self, value: u64, width: u32) -> Result<()> {
        debug_assert!(width <= 64);
        if width == 0 {
            return Ok(());
        }
        let masked = if width == 64 {
            value
        } else {
            value & ((1u64 << width) - 1)
        };
        let free = 64 - self.used; // 1..=64, since `used` is always < 64
        self.word |= masked << self.used;
        if width < free {
            self.used += width;
        } else {
            self.flush_word()?;
            let remaining = width - free;
            if remaining > 0 {
                // `free < width <= 64` here, so `free <= 63` and the shift is
                // in range.
                self.word = masked >> free;
                self.used = remaining;
            }
        }
        Ok(())
    }

    pub fn push_zeros(&mut self, mut count: u64) -> Result<()> {
        while count > 0 {
            let chunk = count.min(u64::from(64 - self.used)) as u32;
            self.push_bits(0, chunk)?;
            count -= u64::from(chunk);
        }
        Ok(())
    }

    fn flush_word(&mut self) -> Result<()> {
        self.writer.write_all(&self.word.to_le_bytes())?;
        self.word = 0;
        self.used = 0;
        self.words += 1;
        Ok(())
    }

    /// Flush any partial word, zero-padded, and report the words written.
    pub fn finish(mut self) -> Result<u64> {
        if self.used > 0 {
            self.flush_word()?;
        }
        Ok(self.words)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_packer_writes_whole_little_endian_words() {
        let mut buffer = Vec::new();
        let mut packer = BitPacker::new(&mut buffer);
        // 0b101 then 61 zero bits fills exactly one word.
        packer.push_bits(0b101, 3).unwrap();
        packer.push_zeros(61).unwrap();
        assert_eq!(packer.finish().unwrap(), 1);
        assert_eq!(buffer, 5u64.to_le_bytes());

        // A value straddling the word boundary splits across both words.
        let mut buffer = Vec::new();
        let mut packer = BitPacker::new(&mut buffer);
        packer.push_zeros(60).unwrap();
        packer.push_bits(0b1111_1111, 8).unwrap();
        assert_eq!(packer.finish().unwrap(), 2);
        assert_eq!(
            u64::from_le_bytes(buffer[0..8].try_into().unwrap()),
            0xF000_0000_0000_0000
        );
        assert_eq!(
            u64::from_le_bytes(buffer[8..16].try_into().unwrap()),
            0b1111
        );
    }
}
