//! OpsTriple - Triple wrapper for OPS (Object-Predicate-Subject) ordering.
//!
//! Used to sort triples in OPS order for index creation. Wraps IdTriple and
//! implements Ord for OPS column priority: Object, then Predicate, then Subject.

use crate::sort::Sortable;
use crate::triples::id_triple::IdTriple;
use anyhow::Result;
use std::cmp::Ordering;
use std::io::{Read, Write};

/// Triple wrapper that sorts in OPS order (Object, Predicate, Subject).
///
/// Used by the external sorter to sort triples for index creation.
/// Implements Sortable for use with ExternalSorter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpsTriple {
    pub object: u64,
    pub predicate: u64,
    pub subject: u64,
}

impl OpsTriple {
    /// Create an OpsTriple from column values.
    pub fn new(object: u64, predicate: u64, subject: u64) -> Self {
        Self {
            object,
            predicate,
            subject,
        }
    }
}

impl From<IdTriple> for OpsTriple {
    fn from(t: IdTriple) -> Self {
        Self {
            object: t.object,
            predicate: t.predicate,
            subject: t.subject,
        }
    }
}

impl From<OpsTriple> for IdTriple {
    fn from(t: OpsTriple) -> Self {
        Self {
            subject: t.subject,
            predicate: t.predicate,
            object: t.object,
        }
    }
}

impl PartialOrd for OpsTriple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OpsTriple {
    fn cmp(&self, other: &Self) -> Ordering {
        // OPS ordering: Object first, then Predicate, then Subject
        self.object
            .cmp(&other.object)
            .then(self.predicate.cmp(&other.predicate))
            .then(self.subject.cmp(&other.subject))
    }
}

impl Sortable for OpsTriple {
    /// Serialize as 3 little-endian u64 values (24 bytes total).
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.object.to_le_bytes())?;
        writer.write_all(&self.predicate.to_le_bytes())?;
        writer.write_all(&self.subject.to_le_bytes())?;
        Ok(())
    }

    /// Deserialize from 3 little-endian u64 values.
    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; 24];
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        Ok(Some(OpsTriple {
            object: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            predicate: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            subject: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
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
    fn test_ops_ordering() {
        let t1 = OpsTriple::new(1, 2, 3);
        let t2 = OpsTriple::new(1, 3, 2);
        let t3 = OpsTriple::new(2, 1, 1);

        // Same object, different predicate
        assert!(t1 < t2);

        // Different object
        assert!(t1 < t3);

        // Same object and predicate, different subject
        let t1a = OpsTriple::new(1, 2, 3);
        let t1b = OpsTriple::new(1, 2, 4);
        assert!(t1a < t1b);
    }

    #[test]
    fn test_conversion_roundtrip() {
        let id_triple = IdTriple {
            subject: 10,
            predicate: 20,
            object: 30,
        };

        let ops = OpsTriple::from(id_triple);
        assert_eq!(ops.object, 30);
        assert_eq!(ops.predicate, 20);
        assert_eq!(ops.subject, 10);

        let back = IdTriple::from(ops);
        assert_eq!(back.subject, 10);
        assert_eq!(back.predicate, 20);
        assert_eq!(back.object, 30);
    }

    #[test]
    fn test_sortable_roundtrip() -> Result<()> {
        let original = OpsTriple::new(100, 200, 300);

        let mut buf = Vec::new();
        original.write_to(&mut buf)?;

        let mut cursor = std::io::Cursor::new(buf);
        let restored = OpsTriple::read_from(&mut cursor)?;

        assert_eq!(restored, Some(original));
        Ok(())
    }
}
