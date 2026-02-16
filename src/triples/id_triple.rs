//! ID triple type for encoding RDF triples as integer IDs.

use crate::sort::Sortable;
use anyhow::Result;
use std::io::{Read, Write};

/// A triple encoded as integer IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdTriple {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64,
}

impl PartialOrd for IdTriple {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IdTriple {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.subject
            .cmp(&other.subject)
            .then(self.predicate.cmp(&other.predicate))
            .then(self.object.cmp(&other.object))
    }
}

impl Sortable for IdTriple {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.subject.to_le_bytes())?;
        writer.write_all(&self.predicate.to_le_bytes())?;
        writer.write_all(&self.object.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; 24];
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        Ok(Some(IdTriple {
            subject: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            predicate: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            object: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        }))
    }

    fn mem_size(&self) -> usize {
        24
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_triple_ordering() {
        let t1 = IdTriple { subject: 1, predicate: 1, object: 1 };
        let t2 = IdTriple { subject: 1, predicate: 1, object: 2 };
        let t3 = IdTriple { subject: 1, predicate: 2, object: 1 };
        let t4 = IdTriple { subject: 2, predicate: 1, object: 1 };

        assert!(t1 < t2);
        assert!(t2 < t3);
        assert!(t3 < t4);
    }

    #[test]
    fn test_id_triple_roundtrip() {
        let triple = IdTriple { subject: 42, predicate: 7, object: 100 };
        let mut buf = Vec::new();
        triple.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), 24);

        let mut cursor = std::io::Cursor::new(&buf);
        let decoded = IdTriple::read_from(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded, triple);
    }
}
