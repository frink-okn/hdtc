//! BitmapTriples construction from sorted SPO ID triples.
//!
//! Builds the four structures:
//! - ArrayY (Sp): predicate ID sequence
//! - BitmapY (Bp): marks first predicate for each subject (1 = new subject)
//! - ArrayZ (So): object ID sequence
//! - BitmapZ (Bo): marks first object for each (subject,predicate) pair (1 = new pair)

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
pub fn build_bitmap_triples(
    sorted_triples: impl Iterator<Item = Result<IdTriple>>,
) -> Result<BitmapTriplesData> {
    let mut bitmap_y = BitmapWriter::new();
    let mut array_y_entries: Vec<u64> = Vec::new();
    let mut bitmap_z = BitmapWriter::new();
    let mut array_z_entries: Vec<u64> = Vec::new();

    let mut prev_subject: u64 = 0;
    let mut prev_predicate: u64 = 0;
    let mut num_triples: u64 = 0;
    let mut max_subject: u64 = 0;
    let mut max_predicate: u64 = 0;
    let mut max_object: u64 = 0;

    for result in sorted_triples {
        let triple = result?;

        max_subject = max_subject.max(triple.subject);
        max_predicate = max_predicate.max(triple.predicate);
        max_object = max_object.max(triple.object);

        if triple.subject != prev_subject {
            // New subject
            if num_triples > 0 {
                // The previous predicate entry ended
            }
            bitmap_y.push_one();
            array_y_entries.push(triple.predicate);
            bitmap_z.push_one();
            array_z_entries.push(triple.object);
            prev_subject = triple.subject;
            prev_predicate = triple.predicate;
        } else if triple.predicate != prev_predicate {
            // Same subject, new predicate
            bitmap_y.push_zero();
            array_y_entries.push(triple.predicate);
            bitmap_z.push_one();
            array_z_entries.push(triple.object);
            prev_predicate = triple.predicate;
        } else {
            // Same subject and predicate, new object
            bitmap_z.push_zero();
            array_z_entries.push(triple.object);
        }

        num_triples += 1;
    }

    tracing::info!("BitmapTriples: {num_triples} triples encoded");

    // Encode ArrayY
    let mut array_y_writer = LogArrayWriter::for_max_value(max_predicate.max(1));
    for &p in &array_y_entries {
        array_y_writer.push(p);
    }
    let mut array_y_buf = Vec::new();
    array_y_writer.write_to(&mut array_y_buf)?;

    // Encode ArrayZ
    let max_obj_or_shared = max_object.max(max_subject).max(1);
    let mut array_z_writer = LogArrayWriter::for_max_value(max_obj_or_shared);
    for &o in &array_z_entries {
        array_z_writer.push(o);
    }
    let mut array_z_buf = Vec::new();
    array_z_writer.write_to(&mut array_z_buf)?;

    // Encode bitmaps
    let mut bitmap_y_buf = Vec::new();
    bitmap_y.write_to(&mut bitmap_y_buf)?;

    let mut bitmap_z_buf = Vec::new();
    bitmap_z.write_to(&mut bitmap_z_buf)?;

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

        let result = build_bitmap_triples(triples.into_iter()).unwrap();
        assert_eq!(result.num_triples, 1);

        // BitmapY should be [1] (one subject)
        let by = BitmapReader::read_from(&mut Cursor::new(&result.bitmap_y)).unwrap();
        assert_eq!(by.len(), 1);
        assert!(by.get(0)); // 1 = new subject

        // ArrayY should be [1] (predicate 1)
        let ay = LogArrayReader::read_from(&mut Cursor::new(&result.array_y)).unwrap();
        assert_eq!(ay.len(), 1);
        assert_eq!(ay.get(0), 1);

        // BitmapZ should be [1] (one SP pair)
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

        let result = build_bitmap_triples(triples.into_iter()).unwrap();
        assert_eq!(result.num_triples, 4);

        // BitmapY: [1, 0, 1] (subject changes at pos 0 and 2)
        let by = BitmapReader::read_from(&mut Cursor::new(&result.bitmap_y)).unwrap();
        assert_eq!(by.len(), 3);
        assert!(by.get(0));   // S=1 start
        assert!(!by.get(1));  // S=1, P=2
        assert!(by.get(2));   // S=2 start

        // ArrayY: [1, 2, 1] (predicates)
        let ay = LogArrayReader::read_from(&mut Cursor::new(&result.array_y)).unwrap();
        assert_eq!(ay.len(), 3);
        assert_eq!(ay.get(0), 1);
        assert_eq!(ay.get(1), 2);
        assert_eq!(ay.get(2), 1);

        // BitmapZ: [1, 0, 1, 1] (new SP pair at pos 0, 2, 3)
        let bz = BitmapReader::read_from(&mut Cursor::new(&result.bitmap_z)).unwrap();
        assert_eq!(bz.len(), 4);
        assert!(bz.get(0));   // S=1,P=1 start
        assert!(!bz.get(1));  // S=1,P=1,O=2
        assert!(bz.get(2));   // S=1,P=2 start
        assert!(bz.get(3));   // S=2,P=1 start

        // ArrayZ: [1, 2, 3, 1] (objects)
        let az = LogArrayReader::read_from(&mut Cursor::new(&result.array_z)).unwrap();
        assert_eq!(az.len(), 4);
        assert_eq!(az.get(0), 1);
        assert_eq!(az.get(1), 2);
        assert_eq!(az.get(2), 3);
        assert_eq!(az.get(3), 1);
    }
}
