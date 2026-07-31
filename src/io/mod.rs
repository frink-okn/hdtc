pub(crate) mod bitmap;
pub(crate) mod bitpack;
pub(crate) mod control;
pub(crate) mod crc_utils;
pub(crate) mod log_array;
pub(crate) mod vbyte;

#[allow(unused_imports)]
pub use bitmap::{BitmapReader, BitmapWriter, StreamingBitmapDecoder, StreamingBitmapEncoder};
pub use bitpack::BitPacker;
pub use control::{ControlInfo, ControlType};
#[allow(unused_imports)]
pub use log_array::{
    LogArrayReader, LogArrayWriter, StreamingLogArrayDecoder, StreamingLogArrayEncoder,
};
pub use vbyte::{decode_vbyte, encode_vbyte, read_vbyte};

use crate::io::crc_utils::crc8;
use anyhow::{Context, Result, ensure};
use std::io::{Read, Seek, SeekFrom};

/// Section type byte of a Bitmap and of a LogArray (they share the value; which
/// one a section is follows from where it sits).
const TYPE_BITMAP: u8 = 1;
const TYPE_LOG: u8 = 1;

/// Bytes occupied by `count` packed entries of `width` bits each.
///
/// The rounding convention every packed region in every format this crate
/// writes shares: entries are bit-packed end to end and the region is padded to
/// a whole byte. Errors rather than overflowing, because `count` and `width`
/// come from a file being parsed.
pub fn packed_len(count: u64, width: u8) -> Result<u64> {
    count
        .checked_mul(u64::from(width))
        .context("packed section bit-length overflow")
        .map(|bits| bits.div_ceil(8))
}

/// Where a Bitmap section's payload is and how long it is, from its preamble.
///
/// Produced by [`scan_bitmap_section`], which reads no payload byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitmapSection {
    /// Offset of the section, i.e. of its type byte.
    pub section_start: u64,
    /// Offset of the packed bits, past the preamble and its CRC8.
    pub data_start: u64,
    /// Payload length in bytes.
    pub data_length: u64,
    /// Bits in the bitmap.
    pub num_bits: u64,
    /// Offset just past the payload's CRC32C — where the next section begins.
    pub section_end: u64,
}

/// Where a LogArray section's payload is and what shape it has, from its
/// preamble.
///
/// Produced by [`scan_log_array_section`], which reads no payload byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogArraySection {
    /// Offset of the section, i.e. of its type byte.
    pub section_start: u64,
    /// Offset of the packed entries, past the preamble and its CRC8.
    pub data_start: u64,
    /// Payload length in bytes.
    pub data_length: u64,
    /// Number of entries.
    pub num_entries: u64,
    /// Bits per entry, `0..=64`.
    pub bits_per_entry: u8,
    /// Offset just past the payload's CRC32C — where the next section begins.
    pub section_end: u64,
}

/// Locate a Bitmap section from its preamble, leaving `reader` past the section.
///
/// Verifies the preamble's CRC8 and touches no payload byte, so the cost is a
/// handful of bytes however large the bitmap is. This is the form a mapped
/// reader needs: it reports where the payload starts rather than only where the
/// section does (KGF doc 20 §20.4).
pub fn scan_bitmap_section<R: Read + Seek>(reader: &mut R) -> Result<BitmapSection> {
    let section_start = reader.stream_position()?;

    let mut preamble = vec![0u8; 1];
    reader.read_exact(&mut preamble)?;
    ensure!(preamble[0] == TYPE_BITMAP, "invalid Bitmap section type");
    let num_bits = read_vbyte_recording(reader, &mut preamble)?;
    verify_preamble_crc8(reader, &preamble, "Bitmap")?;

    let data_start = reader.stream_position()?;
    let data_length = num_bits.div_ceil(8);
    let section_end = skip_payload(reader, data_length)?;

    Ok(BitmapSection {
        section_start,
        data_start,
        data_length,
        num_bits,
        section_end,
    })
}

/// Locate a LogArray section from its preamble, leaving `reader` past the
/// section. See [`scan_bitmap_section`] for why this form exists.
pub fn scan_log_array_section<R: Read + Seek>(reader: &mut R) -> Result<LogArraySection> {
    let section_start = reader.stream_position()?;

    let mut preamble = vec![0u8; 2];
    reader.read_exact(&mut preamble)?;
    ensure!(preamble[0] == TYPE_LOG, "invalid LogArray section type");
    let bits_per_entry = preamble[1];
    ensure!(
        bits_per_entry <= 64,
        "invalid LogArray entry width {bits_per_entry}"
    );
    let num_entries = read_vbyte_recording(reader, &mut preamble)?;
    verify_preamble_crc8(reader, &preamble, "LogArray")?;

    let data_start = reader.stream_position()?;
    let data_length = packed_len(num_entries, bits_per_entry)?;
    let section_end = skip_payload(reader, data_length)?;

    Ok(LogArraySection {
        section_start,
        data_start,
        data_length,
        num_entries,
        bits_per_entry,
        section_end,
    })
}

/// Skip past a Bitmap section, returning `(section_start_offset, num_bits)`.
pub fn skip_bitmap_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64)> {
    let section = scan_bitmap_section(reader)?;
    Ok((section.section_start, section.num_bits))
}

/// Skip past a LogArray section, returning `(section_start_offset, num_entries, bits_per_entry)`.
pub fn skip_log_array_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64, u8)> {
    let section = scan_log_array_section(reader)?;
    Ok((
        section.section_start,
        section.num_entries,
        section.bits_per_entry,
    ))
}

/// Read a VByte, appending the bytes it consumed to `bytes` so that a preamble
/// can be checksummed after the fact.
pub(crate) fn read_vbyte_recording<R: Read>(reader: &mut R, bytes: &mut Vec<u8>) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        bytes.push(byte[0]);
        value |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 != 0 {
            return Ok(value);
        }
        shift += 7;
        ensure!(shift < 64, "invalid VByte in section preamble");
    }
}

/// Consume the CRC8 that follows a preamble and check it against `preamble`.
pub(crate) fn verify_preamble_crc8<R: Read>(
    reader: &mut R,
    preamble: &[u8],
    section_name: &str,
) -> Result<()> {
    let mut stored = [0u8; 1];
    reader.read_exact(&mut stored)?;
    let expected = crc8(preamble);
    ensure!(
        stored[0] == expected,
        "{section_name} preamble CRC8 mismatch: expected {expected:#04x}, got {:#04x}",
        stored[0]
    );
    Ok(())
}

/// Seek past a payload of `data_length` bytes and its CRC32C, returning the
/// position of the next section.
fn skip_payload<R: Seek>(reader: &mut R, data_length: u64) -> Result<u64> {
    /// Every payload is followed by a CRC32C.
    const PAYLOAD_CRC_BYTES: u64 = 4;
    let skip = data_length
        .checked_add(PAYLOAD_CRC_BYTES)
        .context("section length overflow")?;
    Ok(reader.seek(SeekFrom::Current(
        i64::try_from(skip).context("section is too large to seek")?,
    ))?)
}
