//! CRC checksum utilities for HDT.
//!
//! HDT uses three CRC variants:
//! - CRC8-CCITT (poly 0x07): after metadata preambles in arrays/bitmaps
//! - CRC16-ANSI (poly 0x8005): after Control Information blocks
//! - CRC32C (poly 0x1EDC6F41): after data payloads

use crc::{Crc, CRC_32_ISCSI};
#[cfg(test)]
use std::io::{self, Write};

// CRC8-CCITT: polynomial 0x07
const CRC8_ALGO: Crc<u8> = Crc::<u8>::new(&crc::Algorithm {
    width: 8,
    poly: 0x07,
    init: 0x00,
    refin: false,
    refout: false,
    xorout: 0x00,
    check: 0x00,
    residue: 0x00,
});

// CRC16-ANSI: polynomial 0x8005
const CRC16_ALGO: Crc<u16> = Crc::<u16>::new(&crc::Algorithm {
    width: 16,
    poly: 0x8005,
    init: 0x0000,
    refin: true,
    refout: true,
    xorout: 0x0000,
    check: 0x0000,
    residue: 0x0000,
});

// CRC32C (Castagnoli): polynomial 0x1EDC6F41
// This is the same as CRC_32_ISCSI in the crc crate
const CRC32C_ALGO: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Compute CRC8-CCITT over the given data.
pub fn crc8(data: &[u8]) -> u8 {
    CRC8_ALGO.checksum(data)
}

/// Compute CRC16-ANSI over the given data.
pub fn crc16(data: &[u8]) -> u16 {
    CRC16_ALGO.checksum(data)
}

/// Compute CRC32C over the given data.
pub fn crc32c(data: &[u8]) -> u32 {
    CRC32C_ALGO.checksum(data)
}

/// A writer wrapper that incrementally computes a CRC8 over all bytes written.
#[cfg(test)]
pub struct Crc8Writer<W: Write> {
    inner: W,
    digest: crc::Digest<'static, u8>,
}

#[cfg(test)]
impl<W: Write> Crc8Writer<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            digest: CRC8_ALGO.digest(),
        }
    }

    /// Finalize and write the CRC8 checksum. Returns the inner writer.
    pub fn finalize(mut self) -> io::Result<W> {
        let checksum = self.digest.finalize();
        self.inner.write_all(&[checksum])?;
        Ok(self.inner)
    }
}

#[cfg(test)]
impl<W: Write> Write for Crc8Writer<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.digest.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// A writer wrapper that incrementally computes a CRC32C over all bytes written.
#[cfg(test)]
pub struct Crc32cWriter<W: Write> {
    inner: W,
    digest: crc::Digest<'static, u32>,
}

#[cfg(test)]
impl<W: Write> Crc32cWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            digest: CRC32C_ALGO.digest(),
        }
    }

    /// Finalize and write the CRC32C checksum (little-endian). Returns the inner writer.
    pub fn finalize(mut self) -> io::Result<W> {
        let checksum = self.digest.finalize();
        self.inner.write_all(&checksum.to_le_bytes())?;
        Ok(self.inner)
    }
}

#[cfg(test)]
impl<W: Write> Write for Crc32cWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.digest.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc8_empty() {
        assert_eq!(crc8(&[]), 0x00);
    }

    #[test]
    fn test_crc8_data() {
        // Verify CRC8 is deterministic and non-trivial
        let c1 = crc8(b"hello");
        let c2 = crc8(b"hello");
        let c3 = crc8(b"world");
        assert_eq!(c1, c2);
        assert_ne!(c1, c3);
    }

    #[test]
    fn test_crc16_deterministic() {
        let c1 = crc16(b"test data");
        let c2 = crc16(b"test data");
        assert_eq!(c1, c2);
        assert_ne!(crc16(b"test data"), crc16(b"other data"));
    }

    #[test]
    fn test_crc32c_deterministic() {
        let c1 = crc32c(b"test data");
        let c2 = crc32c(b"test data");
        assert_eq!(c1, c2);
        assert_ne!(crc32c(b"test data"), crc32c(b"other data"));
    }

    #[test]
    fn test_crc8_writer() {
        let mut buf = Vec::new();
        let data = b"test payload";
        let expected_crc = crc8(data);

        {
            let mut writer = Crc8Writer::new(&mut buf);
            writer.write_all(data).unwrap();
            writer.finalize().unwrap();
        }

        assert_eq!(&buf[..data.len()], data);
        assert_eq!(buf[data.len()], expected_crc);
    }

    #[test]
    fn test_crc32c_writer() {
        let mut buf = Vec::new();
        let data = b"test payload";
        let expected_crc = crc32c(data);

        {
            let mut writer = Crc32cWriter::new(&mut buf);
            writer.write_all(data).unwrap();
            writer.finalize().unwrap();
        }

        assert_eq!(&buf[..data.len()], data);
        let crc_bytes = &buf[data.len()..];
        assert_eq!(crc_bytes.len(), 4);
        let stored_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        assert_eq!(stored_crc, expected_crc);
    }

    #[test]
    fn test_crc32c_writer_incremental() {
        // Writing in chunks should produce the same CRC as writing all at once
        let data = b"hello world test";
        let full_crc = crc32c(data);

        let mut buf = Vec::new();
        {
            let mut writer = Crc32cWriter::new(&mut buf);
            writer.write_all(&data[..5]).unwrap();
            writer.write_all(&data[5..11]).unwrap();
            writer.write_all(&data[11..]).unwrap();
            writer.finalize().unwrap();
        }

        let stored_crc = u32::from_le_bytes([
            buf[data.len()],
            buf[data.len() + 1],
            buf[data.len() + 2],
            buf[data.len() + 3],
        ]);
        assert_eq!(stored_crc, full_crc);
    }
}
