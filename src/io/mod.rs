pub(crate) mod vbyte;
pub(crate) mod crc_utils;
pub(crate) mod log_array;
pub(crate) mod bitmap;
pub(crate) mod control;

#[allow(unused_imports)]
pub use log_array::{LogArrayWriter, LogArrayReader, StreamingLogArrayEncoder, StreamingLogArrayDecoder};
#[allow(unused_imports)]
pub use bitmap::{BitmapWriter, BitmapReader, StreamingBitmapEncoder, StreamingBitmapDecoder};
pub use control::{ControlInfo, ControlType};
pub use vbyte::{decode_vbyte, encode_vbyte, read_vbyte};

use anyhow::Result;
use std::io::{Read, Seek, SeekFrom};

/// Skip past a Bitmap section, returning `(section_start_offset, num_bits)`.
pub fn skip_bitmap_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64)> {
    let section_start = reader.stream_position()?;

    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;

    let num_bits = read_vbyte(reader)?;

    let mut crc8 = [0u8; 1];
    reader.read_exact(&mut crc8)?;

    let data_bytes = num_bits.div_ceil(8);
    reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

    Ok((section_start, num_bits))
}

/// Skip past a LogArray section, returning `(section_start_offset, num_entries, bits_per_entry)`.
pub fn skip_log_array_section<R: Read + Seek>(reader: &mut R) -> Result<(u64, u64, u8)> {
    let section_start = reader.stream_position()?;

    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;

    let mut bits_byte = [0u8; 1];
    reader.read_exact(&mut bits_byte)?;
    let bits_per_entry = bits_byte[0];

    let num_entries = read_vbyte(reader)?;

    let mut crc8 = [0u8; 1];
    reader.read_exact(&mut crc8)?;

    let total_bits = num_entries * bits_per_entry as u64;
    let data_bytes = total_bits.div_ceil(8);
    reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

    Ok((section_start, num_entries, bits_per_entry))
}
