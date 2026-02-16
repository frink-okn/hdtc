//! BitmapTriples construction from sorted SPO ID triples.
//!
//! Builds the four structures:
//! - ArrayY (Sp): predicate ID sequence
//! - BitmapY (Bp): marks last predicate for each subject (1 = last predicate of subject)
//! - ArrayZ (So): object ID sequence
//! - BitmapZ (Bo): marks last object for each (subject,predicate) pair (1 = last object of pair)

use crate::io::{BitmapWriter, LogArrayWriter};
use crate::triples::id_triple::IdTriple;
use anyhow::Result;

/// Result of building BitmapTriples.
pub struct BitmapTriplesData {
    /// Encoded BitmapY (Bp) bytes
    pub bitmap_y: Vec<u8>,
    /// Encoded ArrayY (Sp) bytes
    pub array_y: Vec<u8>,
    /// Encoded BitmapZ (Bo) bytes
    pub bitmap_z: Vec<u8>,
    /// Encoded ArrayZ (So) bytes
    pub array_z: Vec<u8>,
    /// Total number of triples
    pub num_triples: u64,
    /// Maximum subject ID seen
    #[allow(dead_code)]
    pub max_subject: u64,
    /// Maximum predicate ID seen
    #[allow(dead_code)]
    pub max_predicate: u64,
    /// Maximum object ID seen
    #[allow(dead_code)]
    pub max_object: u64,
}

/// Build BitmapTriples from a sorted iterator of ID triples.
///
/// `max_subject`, `max_predicate`, `max_object` must be upper bounds on the
/// corresponding ID values in the input. They are used to determine the bit
/// width for LogArray encoding upfront, enabling single-pass streaming
/// construction without intermediate vectors.
pub fn build_bitmap_triples(
    sorted_triples: impl Iterator<Item = Result<IdTriple>>,
    max_subject: u64,
    max_predicate: u64,
    max_object: u64,
) -> Result<BitmapTriplesData> {
    // Initialize writers with correct bit widths upfront so we can stream
    // directly into them, eliminating the intermediate Vec<u64> / Vec<bool>.
    let mut bitmap_y = BitmapWriter::new();
    let mut array_y = LogArrayWriter::for_max_value(max_predicate.max(1));
    let mut bitmap_z = BitmapWriter::new();
    let max_obj_or_shared = max_object.max(max_subject).max(1);
    let mut array_z = LogArrayWriter::for_max_value(max_obj_or_shared);

    let mut prev_subject: u64 = 0;
    let mut prev_predicate: u64 = 0;
    let mut num_triples: u64 = 0;

    // HDT convention: bit=1 marks the LAST child of a parent node.
    // Since we process triples in order, we mark the end of each group
    // retroactively when the next group starts, and mark the final
    // entries after the loop.
    for result in sorted_triples {
        let triple = result?;

        if triple.subject != prev_subject {
            // New subject
            if num_triples > 0 {
                // Mark end of previous subject's last predicate
                bitmap_y.set_last(true);
                // Mark end of previous (S,P) pair's last object
                bitmap_z.set_last(true);
            }
            bitmap_y.push(false);
            array_y.push(triple.predicate);
            bitmap_z.push(false);
            array_z.push(triple.object);
            prev_subject = triple.subject;
            prev_predicate = triple.predicate;
        } else if triple.predicate != prev_predicate {
            // Same subject, new predicate
            // Mark end of previous (S,P) pair's last object
            bitmap_z.set_last(true);
            bitmap_y.push(false);
            array_y.push(triple.predicate);
            bitmap_z.push(false);
            array_z.push(triple.object);
            prev_predicate = triple.predicate;
        } else {
            // Same subject and predicate, new object
            bitmap_z.push(false);
            array_z.push(triple.object);
        }

        num_triples += 1;
    }

    // Mark end of the final groups
    if num_triples > 0 {
        bitmap_y.set_last(true);
        bitmap_z.set_last(true);
    }

    tracing::info!("BitmapTriples: {num_triples} triples encoded");

    // Serialize to byte buffers
    let mut bitmap_y_buf = Vec::new();
    bitmap_y.write_to(&mut bitmap_y_buf)?;

    let mut bitmap_z_buf = Vec::new();
    bitmap_z.write_to(&mut bitmap_z_buf)?;

    let mut array_y_buf = Vec::new();
    array_y.write_to(&mut array_y_buf)?;

    let mut array_z_buf = Vec::new();
    array_z.write_to(&mut array_z_buf)?;

    Ok(BitmapTriplesData {
        bitmap_y: bitmap_y_buf,
        array_y: array_y_buf,
        bitmap_z: bitmap_z_buf,
        array_z: array_z_buf,
        num_triples,
        max_subject,
        max_predicate,
        max_object,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{BitmapReader, LogArrayReader};
    use std::io::Cursor;

    #[test]
    fn test_single_triple() {
        let triples = vec![Ok(IdTriple {
            subject: 1,
            predicate: 1,
            object: 1,
        })];

        let result = build_bitmap_triples(triples.into_iter(), 1, 1, 1).unwrap();
        assert_eq!(result.num_triples, 1);

        // BitmapY should be [1] (last predicate of subject 1)
        let by = BitmapReader::read_from(&mut Cursor::new(&result.bitmap_y)).unwrap();
        assert_eq!(by.len(), 1);
        assert!(by.get(0));

        // ArrayY should be [1] (predicate 1)
        let ay = LogArrayReader::read_from(&mut Cursor::new(&result.array_y)).unwrap();
        assert_eq!(ay.len(), 1);
        assert_eq!(ay.get(0), 1);

        // BitmapZ should be [1] (last object of (1,1))
        let bz = BitmapReader::read_from(&mut Cursor::new(&result.bitmap_z)).unwrap();
        assert_eq!(bz.len(), 1);
        assert!(bz.get(0));

        // ArrayZ should be [1] (object 1)
        let az = LogArrayReader::read_from(&mut Cursor::new(&result.array_z)).unwrap();
        assert_eq!(az.len(), 1);
        assert_eq!(az.get(0), 1);
    }

    #[test]
    fn test_multiple_triples() {
        // S=1 P=1 O=1
        // S=1 P=1 O=2  (same S,P -> new O)
        // S=1 P=2 O=3  (same S, new P)
        // S=2 P=1 O=1  (new S)
        let triples = vec![
            Ok(IdTriple { subject: 1, predicate: 1, object: 1 }),
            Ok(IdTriple { subject: 1, predicate: 1, object: 2 }),
            Ok(IdTriple { subject: 1, predicate: 2, object: 3 }),
            Ok(IdTriple { subject: 2, predicate: 1, object: 1 }),
        ];

        let result = build_bitmap_triples(triples.into_iter(), 2, 2, 3).unwrap();
        assert_eq!(result.num_triples, 4);

        // BitmapY: [0, 1, 1] (bit=1 marks last predicate of each subject)
        // S=1 has predicates at pos 0,1 -> pos 1 is last
        // S=2 has predicate at pos 2 -> pos 2 is last
        let by = BitmapReader::read_from(&mut Cursor::new(&result.bitmap_y)).unwrap();
        assert_eq!(by.len(), 3);
        assert!(!by.get(0));  // S=1, P=1 (not last pred of S=1)
        assert!(by.get(1));   // S=1, P=2 (last pred of S=1)
        assert!(by.get(2));   // S=2, P=1 (last pred of S=2)

        // ArrayY: [1, 2, 1] (predicates)
        let ay = LogArrayReader::read_from(&mut Cursor::new(&result.array_y)).unwrap();
        assert_eq!(ay.len(), 3);
        assert_eq!(ay.get(0), 1);
        assert_eq!(ay.get(1), 2);
        assert_eq!(ay.get(2), 1);

        // BitmapZ: [0, 1, 1, 1] (bit=1 marks last object of each (S,P) pair)
        // (1,1) has objects at pos 0,1 -> pos 1 is last
        // (1,2) has object at pos 2 -> pos 2 is last
        // (2,1) has object at pos 3 -> pos 3 is last
        let bz = BitmapReader::read_from(&mut Cursor::new(&result.bitmap_z)).unwrap();
        assert_eq!(bz.len(), 4);
        assert!(!bz.get(0));  // (1,1) O=1 (not last)
        assert!(bz.get(1));   // (1,1) O=2 (last obj of (1,1))
        assert!(bz.get(2));   // (1,2) O=3 (last obj of (1,2))
        assert!(bz.get(3));   // (2,1) O=1 (last obj of (2,1))

        // ArrayZ: [1, 2, 3, 1] (objects)
        let az = LogArrayReader::read_from(&mut Cursor::new(&result.array_z)).unwrap();
        assert_eq!(az.len(), 4);
        assert_eq!(az.get(0), 1);
        assert_eq!(az.get(1), 2);
        assert_eq!(az.get(2), 3);
        assert_eq!(az.get(3), 1);
    }
}
