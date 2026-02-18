//! Control Information blocks for HDT sections.
//!
//! Every HDT section is preceded by a Control Information block:
//! - Magic: "$HDT" (4 bytes)
//! - Type: 1 byte (1=Global, 2=Header, 3=Dictionary, 4=Triples, 5=Index)
//! - Format: null-terminated URI string
//! - Properties: semicolon-separated key=value pairs, null-terminated
//! - CRC16-ANSI checksum over everything from magic through properties (inclusive)

#![allow(dead_code)]

use crate::io::crc_utils::crc16;
use std::collections::BTreeMap;
use std::io::{self, Read, Write};

/// HDT magic bytes: "$HDT"
const HDT_MAGIC: &[u8; 4] = b"$HDT";

/// Section types in an HDT file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ControlType {
    Global = 1,
    Header = 2,
    Dictionary = 3,
    Triples = 4,
    Index = 5,
}

impl ControlType {
    fn from_byte(b: u8) -> io::Result<Self> {
        match b {
            1 => Ok(Self::Global),
            2 => Ok(Self::Header),
            3 => Ok(Self::Dictionary),
            4 => Ok(Self::Triples),
            5 => Ok(Self::Index),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown control type byte: {b}"),
            )),
        }
    }
}

/// HDT Control Information block.
#[derive(Debug, Clone)]
pub struct ControlInfo {
    pub control_type: ControlType,
    pub format: String,
    pub properties: BTreeMap<String, String>,
}

impl ControlInfo {
    /// Create a new ControlInfo with the given type and format URI.
    pub fn new(control_type: ControlType, format: impl Into<String>) -> Self {
        Self {
            control_type,
            format: format.into(),
            properties: BTreeMap::new(),
        }
    }

    /// Add a property key-value pair.
    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.properties.insert(key.into(), value.into());
    }

    /// Get a property value by key.
    pub fn get_property(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(|s| s.as_str())
    }

    /// Serialize the Control Information to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut data = Vec::new();

        // Magic
        data.extend_from_slice(HDT_MAGIC);

        // Type
        data.push(self.control_type as u8);

        // Format (null-terminated)
        data.extend_from_slice(self.format.as_bytes());
        data.push(0x00);

        // Properties (semicolon-separated, null-terminated)
        let props: Vec<String> = self
            .properties
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        if !props.is_empty() {
            data.extend_from_slice(props.join(";").as_bytes());
            data.push(b';');
        }
        data.push(0x00);

        // Write data + CRC16
        writer.write_all(&data)?;
        let checksum = crc16(&data);
        writer.write_all(&checksum.to_le_bytes())?;

        Ok(())
    }

    /// Read a Control Information block from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut data = Vec::new();

        // Read magic
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != HDT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid HDT magic: expected $HDT, got {:?}", magic),
            ));
        }
        data.extend_from_slice(&magic);

        // Read type
        let mut type_byte = [0u8; 1];
        reader.read_exact(&mut type_byte)?;
        let control_type = ControlType::from_byte(type_byte[0])?;
        data.push(type_byte[0]);

        // Read format (null-terminated)
        let format = read_null_terminated(reader, &mut data)?;

        // Read properties (null-terminated)
        let props_str = read_null_terminated(reader, &mut data)?;

        // Read and verify CRC16
        let mut crc_buf = [0u8; 2];
        reader.read_exact(&mut crc_buf)?;
        let stored_crc = u16::from_le_bytes(crc_buf);
        let computed_crc = crc16(&data);
        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Control Information CRC16 mismatch: expected {computed_crc:#06x}, got {stored_crc:#06x}"
                ),
            ));
        }

        // Parse properties
        let mut properties = BTreeMap::new();
        if !props_str.is_empty() {
            for pair in props_str.split(';') {
                let pair = pair.trim();
                if pair.is_empty() {
                    continue;
                }
                if let Some((key, value)) = pair.split_once('=') {
                    properties.insert(key.to_string(), value.to_string());
                }
            }
        }

        Ok(Self {
            control_type,
            format,
            properties,
        })
    }
}

/// Read a null-terminated string from a reader, appending raw bytes to data buffer.
fn read_null_terminated<R: Read>(reader: &mut R, data: &mut Vec<u8>) -> io::Result<String> {
    let mut bytes = Vec::new();
    let mut buf = [0u8; 1];
    loop {
        reader.read_exact(&mut buf)?;
        data.push(buf[0]);
        if buf[0] == 0x00 {
            break;
        }
        bytes.push(buf[0]);
    }
    String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_roundtrip_global() {
        let mut ci = ControlInfo::new(ControlType::Global, "<http://purl.org/HDT/hdt#HDTv1>");
        ci.set_property("BaseURI", "http://example.org");
        ci.set_property("Software", "hdtc");

        let mut buf = Vec::new();
        ci.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let ci2 = ControlInfo::read_from(&mut cursor).unwrap();

        assert_eq!(ci2.control_type, ControlType::Global);
        assert_eq!(ci2.format, "<http://purl.org/HDT/hdt#HDTv1>");
        assert_eq!(ci2.get_property("BaseURI"), Some("http://example.org"));
        assert_eq!(ci2.get_property("Software"), Some("hdtc"));
    }

    #[test]
    fn test_roundtrip_no_properties() {
        let ci = ControlInfo::new(ControlType::Header, "ntriples");

        let mut buf = Vec::new();
        ci.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let ci2 = ControlInfo::read_from(&mut cursor).unwrap();

        assert_eq!(ci2.control_type, ControlType::Header);
        assert_eq!(ci2.format, "ntriples");
        assert!(ci2.properties.is_empty());
    }

    #[test]
    fn test_magic_bytes() {
        let ci = ControlInfo::new(ControlType::Global, "test");

        let mut buf = Vec::new();
        ci.write_to(&mut buf).unwrap();

        assert_eq!(&buf[0..4], b"$HDT");
    }

    #[test]
    fn test_invalid_magic() {
        let data = b"XXXX\x00test\x00\x00\x00\x00";
        let mut cursor = Cursor::new(&data[..]);
        assert!(ControlInfo::read_from(&mut cursor).is_err());
    }

    #[test]
    fn test_crc_corruption() {
        let ci = ControlInfo::new(ControlType::Dictionary, "<http://purl.org/HDT/hdt#dictionaryFour>");

        let mut buf = Vec::new();
        ci.write_to(&mut buf).unwrap();

        // Corrupt a byte in the format string
        buf[5] ^= 0xFF;

        let mut cursor = Cursor::new(&buf);
        assert!(ControlInfo::read_from(&mut cursor).is_err());
    }

    #[test]
    fn test_all_control_types() {
        for &ct in &[
            ControlType::Global,
            ControlType::Header,
            ControlType::Dictionary,
            ControlType::Triples,
            ControlType::Index,
        ] {
            let ci = ControlInfo::new(ct, "format");
            let mut buf = Vec::new();
            ci.write_to(&mut buf).unwrap();

            let mut cursor = Cursor::new(&buf);
            let ci2 = ControlInfo::read_from(&mut cursor).unwrap();
            assert_eq!(ci2.control_type, ct);
        }
    }
}
