//! BitmapTriples decoder - converts encoded BitmapTriples back to (S,P,O) tuples.
//!
//! Implements the AdjacencyList pattern from hdt-java to decode the hierarchical
//! encoding of triples in BitmapTriples format.

use crate::io::{BitmapReader, LogArrayReader};
use crate::triples::id_triple::IdTriple;
use anyhow::Result;

/// Iterator that decodes BitmapTriples structures back to (S,P,O) triples.
///
/// The BitmapTriples encoding is hierarchical:
/// - ArrayY contains predicate IDs for each (S,P) pair
/// - BitmapY marks the last predicate for each subject
/// - ArrayZ contains object IDs for each triple
/// - BitmapZ marks the last object for each (S,P) pair
///
/// The decoder traverses the hierarchy by detecting bitmap boundaries.
pub struct BitmapTriplesDecoder {
    bitmap_y: BitmapReader,
    array_y: LogArrayReader,
    bitmap_z: BitmapReader,
    array_z: LogArrayReader,
    current_pos: u64,      // Position in arrayZ
    current_subject: u64,  // Current subject ID (1-indexed)
    current_y_pos: u64,    // Position in arrayY
    num_triples: u64,      // Total number of triples to decode
}

impl BitmapTriplesDecoder {
    /// Create a new decoder from BitmapTriples structures.
    ///
    /// # Arguments
    /// - `bitmap_y`: Bitmap marking last predicate of each subject
    /// - `array_y`: Sequence of predicate IDs
    /// - `bitmap_z`: Bitmap marking last object of each (S,P) pair
    /// - `array_z`: Sequence of object IDs
    /// - `num_triples`: Total number of triples to decode
    pub fn new(
        bitmap_y: BitmapReader,
        array_y: LogArrayReader,
        bitmap_z: BitmapReader,
        array_z: LogArrayReader,
        num_triples: u64,
    ) -> Self {
        Self {
            bitmap_y,
            array_y,
            bitmap_z,
            array_z,
            current_pos: 0,
            current_subject: 1,  // IDs are 1-indexed
            current_y_pos: 0,
            num_triples,
        }
    }

    /// Get the current predicate from arrayY at the given position.
    fn get_predicate(&self, y_pos: u64) -> u64 {
        self.array_y.get(y_pos)
    }

    /// Get the current object from arrayZ at the given position.
    fn get_object(&self, z_pos: u64) -> u64 {
        self.array_z.get(z_pos)
    }

    /// Check if the given position in bitmapZ marks a boundary (last object of (S,P)).
    fn is_z_boundary(&self, z_pos: u64) -> bool {
        if z_pos >= self.num_triples {
            return true;
        }
        self.bitmap_z.get(z_pos)
    }

    /// Check if the given position in bitmapY marks a boundary (last predicate of S).
    fn is_y_boundary(&self, y_pos: u64) -> bool {
        if y_pos >= self.array_y.len() {
            return true;
        }
        self.bitmap_y.get(y_pos)
    }
}

impl Iterator for BitmapTriplesDecoder {
    type Item = Result<IdTriple>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.num_triples {
            return None;
        }

        // Get predicate and object from current position
        let predicate = self.get_predicate(self.current_y_pos);
        let object = self.get_object(self.current_pos);

        let triple = IdTriple {
            subject: self.current_subject,
            predicate,
            object,
        };

        // Check if we need to advance predicates or subjects
        let is_last_object = self.is_z_boundary(self.current_pos);
        if is_last_object {
            // Last object of current (S,P) pair - advance to next predicate
            self.current_y_pos += 1;

            // Check if we've reached the end of current subject's predicates
            let is_last_predicate = self.current_y_pos > 0 && self.is_y_boundary(self.current_y_pos - 1);
            if is_last_predicate {
                // Advance to next subject
                self.current_subject += 1;
            }
        }

        self.current_pos += 1;

        Some(Ok(triple))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{BitmapWriter, LogArrayWriter};
    use std::io::Cursor;

    fn create_simple_test_structures() -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
        // Create a simple test case: 3 triples
        // S=1, P=1, O=1
        // S=1, P=2, O=2
        // S=2, P=1, O=3
        // Expected encoding:
        // ArrayY: [1, 2, 1]
        // BitmapY: [0, 1, 1] (last predicate of S=1 is at pos 1, last of S=2 is at pos 2)
        // ArrayZ: [1, 2, 3]
        // BitmapZ: [0, 1, 1] (last object of (1,1) is at pos 0, last of (1,2) at pos 1, last of (2,1) at pos 2)

        let mut bitmap_y = BitmapWriter::new();
        bitmap_y.push(false); // pos 0: not last
        bitmap_y.push(true);  // pos 1: last predicate of S=1
        bitmap_y.push(true);  // pos 2: last predicate of S=2
        let mut buf_y = Vec::new();
        bitmap_y.write_to(&mut buf_y).unwrap();

        let mut array_y = LogArrayWriter::for_max_value(2);
        array_y.push(1);
        array_y.push(2);
        array_y.push(1);
        let mut buf_ay = Vec::new();
        array_y.write_to(&mut buf_ay).unwrap();

        let mut bitmap_z = BitmapWriter::new();
        bitmap_z.push(true);  // pos 0: last object of (1,1)
        bitmap_z.push(true);  // pos 1: last object of (1,2)
        bitmap_z.push(true);  // pos 2: last object of (2,1)
        let mut buf_z = Vec::new();
        bitmap_z.write_to(&mut buf_z).unwrap();

        let mut array_z = LogArrayWriter::for_max_value(3);
        array_z.push(1);
        array_z.push(2);
        array_z.push(3);
        let mut buf_az = Vec::new();
        array_z.write_to(&mut buf_az).unwrap();

        (buf_y, buf_ay, buf_z, buf_az)
    }

    #[test]
    fn test_simple_decode() -> Result<()> {
        let (buf_y, buf_ay, buf_z, buf_az) = create_simple_test_structures();

        let bitmap_y = BitmapReader::read_from(&mut Cursor::new(buf_y))?;
        let array_y = LogArrayReader::read_from(&mut Cursor::new(buf_ay))?;
        let bitmap_z = BitmapReader::read_from(&mut Cursor::new(buf_z))?;
        let array_z = LogArrayReader::read_from(&mut Cursor::new(buf_az))?;

        let decoder = BitmapTriplesDecoder::new(bitmap_y, array_y, bitmap_z, array_z, 3);

        let triples: Result<Vec<_>> = decoder.collect();
        let triples = triples?;

        assert_eq!(triples.len(), 3);
        assert_eq!(triples[0].subject, 1);
        assert_eq!(triples[0].predicate, 1);
        assert_eq!(triples[0].object, 1);

        assert_eq!(triples[1].subject, 1);
        assert_eq!(triples[1].predicate, 2);
        assert_eq!(triples[1].object, 2);

        assert_eq!(triples[2].subject, 2);
        assert_eq!(triples[2].predicate, 1);
        assert_eq!(triples[2].object, 3);

        Ok(())
    }

    #[test]
    fn test_single_triple() -> Result<()> {
        let mut bitmap_y = BitmapWriter::new();
        bitmap_y.push(true);
        let mut buf_y = Vec::new();
        bitmap_y.write_to(&mut buf_y)?;

        let mut array_y = LogArrayWriter::for_max_value(1);
        array_y.push(1);
        let mut buf_ay = Vec::new();
        array_y.write_to(&mut buf_ay)?;

        let mut bitmap_z = BitmapWriter::new();
        bitmap_z.push(true);
        let mut buf_z = Vec::new();
        bitmap_z.write_to(&mut buf_z)?;

        let mut array_z = LogArrayWriter::for_max_value(1);
        array_z.push(1);
        let mut buf_az = Vec::new();
        array_z.write_to(&mut buf_az)?;

        let bitmap_y = BitmapReader::read_from(&mut Cursor::new(buf_y))?;
        let array_y = LogArrayReader::read_from(&mut Cursor::new(buf_ay))?;
        let bitmap_z = BitmapReader::read_from(&mut Cursor::new(buf_z))?;
        let array_z = LogArrayReader::read_from(&mut Cursor::new(buf_az))?;

        let decoder = BitmapTriplesDecoder::new(bitmap_y, array_y, bitmap_z, array_z, 1);

        let triples: Result<Vec<_>> = decoder.collect();
        let triples = triples?;

        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].subject, 1);
        assert_eq!(triples[0].predicate, 1);
        assert_eq!(triples[0].object, 1);

        Ok(())
    }
}
