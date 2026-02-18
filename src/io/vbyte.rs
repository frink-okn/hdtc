//! Variable-byte (VByte) integer encoding for HDT.
//!
//! Each byte uses 7 data bits (little-endian order). The MSB is a termination
//! bit: 1 means this is the last byte, 0 means more bytes follow.

#[cfg(test)]
use std::io;
#[cfg(test)]
use std::io::{Read, Write};

/// Encode a u64 value as VByte, returning the bytes.
pub fn encode_vbyte(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(10);
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value > 0 {
            bytes.push(byte); // MSB=0: more bytes follow
        } else {
            bytes.push(byte | 0x80); // MSB=1: last byte
            break;
        }
    }
    bytes
}

/// Decode a VByte-encoded value from a byte slice.
/// Returns (value, bytes_consumed).
#[cfg(test)]
pub fn decode_vbyte(data: &[u8]) -> io::Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0u32;

    for (i, &byte) in data.iter().enumerate() {
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 != 0 {
            // MSB=1: last byte
            return Ok((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "VByte value exceeds u64 range",
            ));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "Unexpected end of data in VByte",
    ))
}

/// Write a VByte-encoded value to a writer. Returns bytes written.
#[cfg(test)]
pub fn write_vbyte<W: Write>(writer: &mut W, value: u64) -> io::Result<usize> {
    let bytes = encode_vbyte(value);
    writer.write_all(&bytes)?;
    Ok(bytes.len())
}

/// Read a VByte-encoded value from a reader.
#[cfg(test)]
pub fn read_vbyte<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut buf = [0u8; 1];

    loop {
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
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
    fn test_encode_zero() {
        // 0 → single byte with MSB=1 (last): 0x80
        assert_eq!(encode_vbyte(0), vec![0x80]);
    }

    #[test]
    fn test_encode_small() {
        assert_eq!(encode_vbyte(1), vec![0x81]);
        assert_eq!(encode_vbyte(127), vec![0xFF]);
    }

    #[test]
    fn test_encode_two_bytes() {
        // 128: byte0 = 0x00 (more), byte1 = 0x81 (last, value=1)
        assert_eq!(encode_vbyte(128), vec![0x00, 0x81]);
        // 255: byte0 = 0x7F (more), byte1 = 0x81 (last, value=1)
        assert_eq!(encode_vbyte(255), vec![0x7F, 0x81]);
    }

    #[test]
    fn test_encode_large() {
        // 16384 = 0x4000: byte0 = 0x00, byte1 = 0x00, byte2 = 0x81
        assert_eq!(encode_vbyte(16384), vec![0x00, 0x00, 0x81]);
    }

    #[test]
    fn test_roundtrip_decode() {
        for &value in &[0u64, 1, 127, 128, 255, 256, 16383, 16384, 1_000_000, u64::MAX] {
            let encoded = encode_vbyte(value);
            let (decoded, consumed) = decode_vbyte(&encoded).unwrap();
            assert_eq!(decoded, value, "roundtrip failed for {value}");
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn test_roundtrip_reader_writer() {
        for &value in &[0u64, 1, 127, 128, 16384, 1_000_000, u64::MAX] {
            let mut buf = Vec::new();
            write_vbyte(&mut buf, value).unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = read_vbyte(&mut cursor).unwrap();
            assert_eq!(decoded, value, "reader/writer roundtrip failed for {value}");
        }
    }

    #[test]
    fn test_decode_multiple_values() {
        let mut buf = Vec::new();
        write_vbyte(&mut buf, 42).unwrap();
        write_vbyte(&mut buf, 300).unwrap();
        write_vbyte(&mut buf, 0).unwrap();

        let mut cursor = Cursor::new(&buf);
        assert_eq!(read_vbyte(&mut cursor).unwrap(), 42);
        assert_eq!(read_vbyte(&mut cursor).unwrap(), 300);
        assert_eq!(read_vbyte(&mut cursor).unwrap(), 0);
    }

    #[test]
    fn test_decode_truncated() {
        // Byte with continuation bit (MSB=0) but no following byte
        let data = [0x00u8];
        let mut cursor = Cursor::new(&data[..]);
        assert!(read_vbyte(&mut cursor).is_err());
    }

    #[test]
    fn test_hdt_java_compatibility() {
        // Verify we encode 333430 the same way hdt-java does: 76 2c 94
        let encoded = encode_vbyte(333430);
        assert_eq!(encoded, vec![0x76, 0x2c, 0x94]);
    }
}
