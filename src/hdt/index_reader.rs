//! HDT index file reader (.hdt.index.v1-1).
//!
//! Scans an index file and records byte offsets of its sections so that
//! streaming decoders can be opened at those positions for query execution.
//!
//! Index file layout (in order):
//! ```text
//! ControlInfo         — type=INDEX, format="<http://purl.org/HDT/hdt#indexFoQ>",
//!                       properties: numTriples, order
//! bitmapIndexZ        — n_triples bits; 1-bit = last entry of each object group
//! indexZ              — n_triples entries; pos_y values sorted by (object, predicate, pos_y)
//! predicateIndex.bitmap  — n_sp bits; 1-bit = last pos_y for current predicate group
//! predicateIndex.seq     — n_sp entries; pos_y values sorted by (predicate, pos_y)
//! predicateCount      — LogArray; count of SP pairs per predicate
//! ```

use crate::io::{
    ControlInfo, ControlType, LogArrayReader, read_vbyte, skip_bitmap_section,
    skip_log_array_section,
};
use anyhow::{Context, Result, bail};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

const INDEX_FORMAT: &str = "<http://purl.org/HDT/hdt#indexFoQ>";

/// Byte offsets of sections within an HDT index file.
#[derive(Debug, Clone, Copy)]
pub struct IndexSectionOffsets {
    /// Start offset of bitmapIndexZ section (object groups).
    pub bitmap_index_z_start: u64,
    /// Start offset of indexZ section (pos_y values sorted by object).
    pub index_z_start: u64,
    /// Start offset of predicateIndex.bitmap section.
    pub pred_bitmap_start: u64,
    /// Start offset of predicateIndex.seq section.
    pub pred_seq_start: u64,
    /// Start offset of predicateCount section.
    pub pred_count_start: u64,
}

/// Object-group metadata within `indexZ`.
#[derive(Debug, Clone, Copy)]
pub struct ObjectGroupStats {
    /// Inclusive start entry index in `indexZ`.
    pub start: u64,
    /// Number of entries in the group (`end - start + 1`).
    pub size: u64,
}

/// Open an HDT index file, verify its structure, and return section offsets.
pub fn open_index(index_path: &Path) -> Result<IndexSectionOffsets> {
    let file = File::open(index_path)
        .with_context(|| format!("Failed to open index file {}", index_path.display()))?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);

    let ci = ControlInfo::read_from(&mut reader)
        .context("Failed to read index control info")?;

    if ci.control_type != ControlType::Index {
        bail!(
            "Expected index control info (type=INDEX), got {:?} in {}",
            ci.control_type,
            index_path.display()
        );
    }
    if ci.format != INDEX_FORMAT {
        bail!(
            "Unsupported index format: {} (expected {}) in {}",
            ci.format,
            INDEX_FORMAT,
            index_path.display()
        );
    }

    // Record start of bitmapIndexZ, then skip past it
    let bitmap_index_z_start = reader
        .stream_position()
        .context("Failed to get bitmapIndexZ section offset")?;
    skip_bitmap_section(&mut reader)
        .context("Failed to skip bitmapIndexZ section")?;

    // Record start of indexZ, then skip past it
    let index_z_start = reader
        .stream_position()
        .context("Failed to get indexZ section offset")?;
    skip_log_array_section(&mut reader)
        .context("Failed to skip indexZ section")?;

    // Record start of predicateIndex.bitmap
    let pred_bitmap_start = reader
        .stream_position()
        .context("Failed to get pred bitmap section offset")?;
    skip_bitmap_section(&mut reader)
        .context("Failed to skip predicateIndex.bitmap section")?;

    // Record start of predicateIndex.seq
    let pred_seq_start = reader
        .stream_position()
        .context("Failed to get pred seq section offset")?;
    skip_log_array_section(&mut reader)
        .context("Failed to skip predicateIndex.seq section")?;

    // Record start of predicateCount
    let pred_count_start = reader
        .stream_position()
        .context("Failed to get predicateCount section offset")?;

    Ok(IndexSectionOffsets {
        bitmap_index_z_start,
        index_z_start,
        pred_bitmap_start,
        pred_seq_start,
        pred_count_start,
    })
}

/// Read the predicateCount LogArray from the index file.
///
/// Returns a `LogArrayReader` with `len() == |P|` entries, where
/// `get(i)` returns the number of (S,P) pairs for predicate `i+1`.
pub fn read_predicate_count(
    index_path: &Path,
    pred_count_start: u64,
) -> Result<LogArrayReader> {
    let mut reader = open_index_section(index_path, pred_count_start)?;
    LogArrayReader::read_from(&mut reader)
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to read predicateCount LogArray from index")
}

/// Find object group boundaries in bitmapIndexZ using a streaming scan.
///
/// Returns `Some((group_start, group_end))` where the object group for
/// `obj_id` spans indexZ positions `[group_start, group_end]` (inclusive).
/// Returns `None` if the object group doesn't exist (obj_id too large).
///
/// O(1) memory — reads word-by-word (64 bits per read) with `count_ones()`
/// to skip entire words.  No data is retained after the scan completes.
pub fn bitmap_index_z_group_range(
    index_path: &Path,
    bitmap_index_z_start: u64,
    obj_id: u64,
) -> Result<Option<(u64, u64)>> {
    if obj_id == 0 {
        return Ok(None);
    }
    let mut f = File::open(index_path)
        .with_context(|| format!("Failed to open index file {}", index_path.display()))?;
    f.seek(SeekFrom::Start(bitmap_index_z_start))?;
    let mut reader = BufReader::with_capacity(64 * 1024, f);

    // Bitmap preamble: type(1) + VByte(num_bits) + CRC8(1)
    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;
    let num_bits = read_vbyte(&mut reader)?;
    let mut _crc = [0u8; 1];
    reader.read_exact(&mut _crc)?;
    // reader is now at start of bitmap data

    let total_data_bytes = num_bits.div_ceil(8);
    let target_prev = obj_id.saturating_sub(1);
    let mut cumulative_ones = 0u64;
    let mut bits_processed = 0u64;
    let mut data_bytes_read = 0u64;
    let mut group_start: Option<u64> = if obj_id == 1 { Some(0) } else { None };

    while bits_processed < num_bits {
        let bytes_remaining = total_data_bytes - data_bytes_read;
        let bytes_to_read = bytes_remaining.min(8) as usize;
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf[..bytes_to_read])?;
        data_bytes_read += bytes_to_read as u64;
        let word = u64::from_le_bytes(buf);

        let bits_in_word = (num_bits - bits_processed).min(64) as u32;
        // Mask off unused bits in the last word
        let masked = if bits_in_word < 64 {
            word & ((1u64 << bits_in_word) - 1)
        } else {
            word
        };
        let ones_in_word = masked.count_ones() as u64;

        // Check if select1(obj_id - 1) falls in this word.
        // Boundary case: when target_prev == cumulative_ones, the previous
        // group's end was exactly before this word, so this group's start is
        // the first bit of this word.
        if group_start.is_none() {
            if target_prev == cumulative_ones {
                group_start = Some(bits_processed);
            } else if cumulative_ones + ones_in_word >= target_prev {
                let need = target_prev - cumulative_ones;
                group_start = Some(nth_set_bit(masked, need, bits_processed) + 1);
            }
        }

        // Check if select1(obj_id) falls in this word
        if cumulative_ones + ones_in_word >= obj_id {
            let need = obj_id - cumulative_ones;
            let group_end = nth_set_bit(masked, need, bits_processed);
            return Ok(Some((group_start.unwrap(), group_end)));
        }

        cumulative_ones += ones_in_word;
        bits_processed += bits_in_word as u64;
    }

    Ok(None) // obj_id-th 1-bit not found
}

/// Find object group boundaries and size in bitmapIndexZ.
pub fn bitmap_index_z_group_stats(
    index_path: &Path,
    bitmap_index_z_start: u64,
    obj_id: u64,
) -> Result<Option<ObjectGroupStats>> {
    let Some((start, end)) = bitmap_index_z_group_range(index_path, bitmap_index_z_start, obj_id)?
    else {
        return Ok(None);
    };
    Ok(Some(ObjectGroupStats {
        start,
        size: end - start + 1,
    }))
}

/// Find the position of the n-th 1-bit (1-indexed) within a word.
/// `base` is the global bit position of bit 0 of this word.
fn nth_set_bit(word: u64, n: u64, base: u64) -> u64 {
    debug_assert!(n >= 1 && n <= word.count_ones() as u64);
    let mut remaining = n;
    let mut w = word;
    for bit_pos in 0..64u64 {
        if w & 1 == 1 {
            remaining -= 1;
            if remaining == 0 {
                return base + bit_pos;
            }
        }
        w >>= 1;
    }
    unreachable!("nth set bit not found in word");
}

/// Read a contiguous range of entries from the indexZ LogArray section on disk.
///
/// Parses only the LogArray preamble, then seeks directly to the byte offset
/// containing the requested entries and extracts values via bit manipulation.
/// Memory: O(count) for the result plus a small temporary read buffer.
pub fn read_index_z_range(
    index_path: &Path,
    index_z_start: u64,
    start_index: u64,
    count: u64,
) -> Result<Vec<u64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut f = File::open(index_path)
        .with_context(|| format!("Failed to open index file {}", index_path.display()))?;
    f.seek(SeekFrom::Start(index_z_start))?;

    // Parse LogArray preamble: type(1) + bpe(1) + VByte(num_entries) + CRC8(1)
    let mut header = [0u8; 2];
    f.read_exact(&mut header)?;
    let bpe = header[1];

    let mut preamble_len: u64 = 2;
    let mut byte_buf = [0u8; 1];
    loop {
        f.read_exact(&mut byte_buf)?;
        preamble_len += 1;
        if byte_buf[0] & 0x80 != 0 {
            break;
        }
    }
    // CRC8 byte: account for it in the offset calculation but don't read it —
    // we use an absolute seek below so the file position doesn't matter here.
    preamble_len += 1;

    let data_start = index_z_start + preamble_len;

    if bpe == 0 {
        return Ok(vec![0; count as usize]);
    }

    let mask: u64 = if bpe == 64 {
        u64::MAX
    } else {
        (1u64 << bpe) - 1
    };

    // Compute word-aligned byte range to read from the data section.
    // Aligning to 8-byte words ensures our local words match the original
    // LogArray word layout, making bit extraction straightforward.
    let first_bit = start_index * bpe as u64;
    let last_bit = (start_index + count) * bpe as u64 - 1;
    let first_word = first_bit / 64;
    let last_word = last_bit / 64;
    // +1 extra word in case the last entry spans a word boundary
    let words_to_read = (last_word - first_word + 2) as usize;

    let seek_byte = first_word * 8;
    f.seek(SeekFrom::Start(data_start + seek_byte))?;
    let mut raw = vec![0u8; words_to_read * 8];
    // Read available bytes; remainder stays zero (safe for trailing entries)
    let mut total_read = 0;
    while total_read < raw.len() {
        let n = f.read(&mut raw[total_read..])?;
        if n == 0 {
            break;
        }
        total_read += n;
    }

    // Reconstruct u64 words from the byte buffer
    let mut words = Vec::with_capacity(words_to_read);
    for wi in 0..words_to_read {
        let s = wi * 8;
        words.push(u64::from_le_bytes(raw[s..s + 8].try_into().unwrap()));
    }

    // Extract entries using the same bit-extraction logic as LogArrayReader::get()
    let mut result = Vec::with_capacity(count as usize);
    for i in 0..count {
        let bit_pos = (start_index + i) * bpe as u64;
        let w = (bit_pos / 64 - first_word) as usize;
        let off = (bit_pos % 64) as u32;

        let mut v = (words[w] >> off) & mask;
        if off + bpe as u32 > 64 && w + 1 < words.len() {
            let rem = off + bpe as u32 - 64;
            let upper_mask = (1u64 << rem) - 1;
            v |= (words[w + 1] & upper_mask) << (64 - off);
        }
        result.push(v);
    }

    Ok(result)
}

/// BufReader positioned at a given offset in the index file.
pub(crate) fn open_index_section(
    index_path: &Path,
    offset: u64,
) -> Result<BufReader<File>> {
    let mut f = File::open(index_path)
        .with_context(|| format!("Failed to open index file {}", index_path.display()))?;
    f.seek(SeekFrom::Start(offset))
        .with_context(|| format!("Failed to seek to offset {offset} in index file"))?;
    Ok(BufReader::with_capacity(256 * 1024, f))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::BitmapWriter;

    fn write_bitmap_file(bits: &[bool]) -> tempfile::NamedTempFile {
        let mut bmp = BitmapWriter::new();
        for &bit in bits {
            bmp.push(bit);
        }

        let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
        bmp.write_to(tmp.as_file_mut())
            .expect("write bitmap section");
        tmp
    }

    #[test]
    fn test_bitmap_index_z_group_range_word_boundary() {
        // 1-bits at positions 10, 63, 70.
        // For obj_id=3, the previous group ends exactly at word boundary 63,
        // so group_start must be 64 (first bit of next word).
        let mut bits = vec![false; 71];
        bits[10] = true;
        bits[63] = true;
        bits[70] = true;
        let tmp = write_bitmap_file(&bits);

        let range = bitmap_index_z_group_range(tmp.path(), 0, 3)
            .expect("group range")
            .expect("group exists");
        assert_eq!(range, (64, 70));
    }

    #[test]
    fn test_bitmap_index_z_group_stats_empty_for_zero_id() {
        let bits = vec![true, false, true];
        let tmp = write_bitmap_file(&bits);

        let stats = bitmap_index_z_group_stats(tmp.path(), 0, 0).expect("stats call");
        assert!(stats.is_none());
    }
}
