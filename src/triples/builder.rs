//! BitmapTriples construction from sorted SPO ID triples.
//!
//! Builds the four structures:
//! - ArrayY (Sp): predicate ID sequence
//! - BitmapY (Bp): marks last predicate for each subject (1 = last predicate of subject)
//! - ArrayZ (So): object ID sequence
//! - BitmapZ (Bo): marks last object for each (subject,predicate) pair (1 = last object of pair)

use crate::io::crc_utils::Crc32cWriter;
use crate::io::{StreamingBitmapEncoder, StreamingLogArrayEncoder};
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
use crate::io::crc_utils::crc8;
#[cfg(test)]
use crate::io::vbyte::encode_vbyte;
#[cfg(test)]
use crate::io::{BitmapWriter, LogArrayWriter};

/// Result of building BitmapTriples (in-memory, used in tests).
#[cfg(test)]
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
}

/// Build BitmapTriples from a sorted iterator of ID triples (in-memory, used in tests).
#[cfg(test)]
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
    })
}

/// Metadata for a bitmap component written to a temp file.
pub struct StreamingBitmapResult {
    pub path: PathBuf,
    pub num_bits: u64,
}

impl StreamingBitmapResult {
    /// Size of this component when framed into the HDT file:
    /// preamble (type byte + VByte(num_bits)) + CRC8 + raw data + CRC32C.
    pub fn framed_size(&self) -> Result<u64> {
        let data_size = std::fs::metadata(&self.path)
            .with_context(|| format!("Failed to stat {}", self.path.display()))?
            .len();
        // preamble: 1 (type) + vbyte_len(num_bits)
        let preamble_len = 1 + vbyte_encoded_len(self.num_bits);
        Ok(preamble_len as u64 + 1 /* CRC8 */ + data_size + 4 /* CRC32C */)
    }
}

/// Metadata for a log array component written to a temp file.
pub struct StreamingLogArrayResult {
    pub path: PathBuf,
    pub bits_per_entry: u8,
    pub num_entries: u64,
}

impl StreamingLogArrayResult {
    /// Size of this component when framed into the HDT file:
    /// preamble (type byte + bits_per_entry byte + VByte(num_entries)) + CRC8 + raw data + CRC32C.
    pub fn framed_size(&self) -> Result<u64> {
        let data_size = std::fs::metadata(&self.path)
            .with_context(|| format!("Failed to stat {}", self.path.display()))?
            .len();
        // preamble: 1 (type) + 1 (bits_per_entry) + vbyte_len(num_entries)
        let preamble_len = 2 + vbyte_encoded_len(self.num_entries);
        Ok(preamble_len as u64 + 1 /* CRC8 */ + data_size + 4 /* CRC32C */)
    }
}

/// Number of bytes needed to VByte-encode a value.
fn vbyte_encoded_len(value: u64) -> usize {
    if value == 0 {
        return 1;
    }
    let bits = 64 - value.leading_zeros() as usize;
    bits.div_ceil(7)
}

/// Result of streaming BitmapTriples construction to temp files.
pub struct BitmapTriplesFiles {
    pub bitmap_y: StreamingBitmapResult,
    pub array_y: StreamingLogArrayResult,
    pub bitmap_z: StreamingBitmapResult,
    pub array_z: StreamingLogArrayResult,
    pub num_triples: u64,
}

impl BitmapTriplesFiles {
    /// Total encoded size of all four components as they will appear in the HDT file,
    /// including preambles, CRC8, raw data, and CRC32C per component.
    pub fn total_encoded_size(&self) -> Result<u64> {
        Ok(self.bitmap_y.framed_size()?
            + self.bitmap_z.framed_size()?
            + self.array_y.framed_size()?
            + self.array_z.framed_size()?)
    }

    /// Clean up temp files.
    pub fn cleanup(&self) {
        for path in [&self.bitmap_y.path, &self.array_y.path,
                     &self.bitmap_z.path, &self.array_z.path] {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!("Failed to delete triples temp file {}: {}", path.display(), e);
            }
        }
    }
}

/// Build BitmapTriples from a sorted iterator, streaming each component to a temp file.
///
/// This uses O(1) memory for the triples data (vs O(num_triples) for `build_bitmap_triples`).
/// Each component (BitmapY, ArrayY, BitmapZ, ArrayZ) is written as raw packed data
/// to its own temp file, with CRC32C computed incrementally.
pub fn build_bitmap_triples_to_files(
    sorted_triples: impl Iterator<Item = Result<IdTriple>>,
    max_subject: u64,
    max_predicate: u64,
    max_object: u64,
    temp_dir: &Path,
) -> Result<BitmapTriplesFiles> {
    // Create temp files for each component
    let bitmap_y_path = temp_dir.join("bt_bitmap_y.tmp");
    let array_y_path = temp_dir.join("bt_array_y.tmp");
    let bitmap_z_path = temp_dir.join("bt_bitmap_z.tmp");
    let array_z_path = temp_dir.join("bt_array_z.tmp");

    let make_writer = |path: &Path| -> Result<BufWriter<File>> {
        let file = File::create(path)
            .with_context(|| format!("Failed to create temp file {}", path.display()))?;
        Ok(BufWriter::with_capacity(256 * 1024, file))
    };

    // Each component writes: raw packed data bytes (no preamble).
    // CRC32C is computed incrementally via Crc32cWriter wrapper.
    // The preamble + CRC32C framing is written during HDT assembly.
    let mut bitmap_y = StreamingBitmapEncoder::new(
        Crc32cWriter::new(make_writer(&bitmap_y_path)?));
    let mut array_y = StreamingLogArrayEncoder::for_max_value(
        max_predicate.max(1),
        Crc32cWriter::new(make_writer(&array_y_path)?));
    let mut bitmap_z = StreamingBitmapEncoder::new(
        Crc32cWriter::new(make_writer(&bitmap_z_path)?));
    let max_obj_or_shared = max_object.max(max_subject).max(1);
    let mut array_z = StreamingLogArrayEncoder::for_max_value(
        max_obj_or_shared,
        Crc32cWriter::new(make_writer(&array_z_path)?));

    let mut prev_subject: u64 = 0;
    let mut prev_predicate: u64 = 0;
    let mut num_triples: u64 = 0;

    for result in sorted_triples {
        let triple = result?;

        if triple.subject != prev_subject {
            if num_triples > 0 {
                bitmap_y.set_last(true);
                bitmap_z.set_last(true);
            }
            bitmap_y.push(false)?;
            array_y.push(triple.predicate)?;
            bitmap_z.push(false)?;
            array_z.push(triple.object)?;
            prev_subject = triple.subject;
            prev_predicate = triple.predicate;
        } else if triple.predicate != prev_predicate {
            bitmap_z.set_last(true);
            bitmap_y.push(false)?;
            array_y.push(triple.predicate)?;
            bitmap_z.push(false)?;
            array_z.push(triple.object)?;
            prev_predicate = triple.predicate;
        } else {
            bitmap_z.push(false)?;
            array_z.push(triple.object)?;
        }

        num_triples += 1;
    }

    if num_triples > 0 {
        bitmap_y.set_last(true);
        bitmap_z.set_last(true);
    }

    tracing::info!("BitmapTriples: {num_triples} triples encoded (streamed to files)");

    // Finish each encoder: flush final partial word, get CRC, flush file
    let (by_bits, by_crc_writer) = bitmap_y.finish()?;
    let (_by_crc, mut by_buf) = by_crc_writer.finalize();
    by_buf.flush()?;

    let (ay_entries, ay_bits_per_entry, ay_crc_writer) = array_y.finish()?;
    let (_ay_crc, mut ay_buf) = ay_crc_writer.finalize();
    ay_buf.flush()?;

    let (bz_bits, bz_crc_writer) = bitmap_z.finish()?;
    let (_bz_crc, mut bz_buf) = bz_crc_writer.finalize();
    bz_buf.flush()?;

    let (az_entries, az_bits_per_entry, az_crc_writer) = array_z.finish()?;
    let (_az_crc, mut az_buf) = az_crc_writer.finalize();
    az_buf.flush()?;

    Ok(BitmapTriplesFiles {
        bitmap_y: StreamingBitmapResult {
            path: bitmap_y_path,
            num_bits: by_bits,
        },
        array_y: StreamingLogArrayResult {
            path: array_y_path,
            bits_per_entry: ay_bits_per_entry,
            num_entries: ay_entries,
        },
        bitmap_z: StreamingBitmapResult {
            path: bitmap_z_path,
            num_bits: bz_bits,
        },
        array_z: StreamingLogArrayResult {
            path: array_z_path,
            bits_per_entry: az_bits_per_entry,
            num_entries: az_entries,
        },
        num_triples,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{BitmapReader, LogArrayReader};
    use std::io::{Cursor, Read as _};

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

    /// Helper: read a temp file's raw data bytes and reassemble it with preamble + CRC
    /// as a bitmap section, then verify it matches the in-memory version.
    fn read_and_assemble_bitmap(path: &Path, num_bits: u64) -> Vec<u8> {
        let mut data = Vec::new();
        File::open(path).unwrap().read_to_end(&mut data).unwrap();
        let data_crc = crate::io::crc_utils::crc32c(&data);

        let mut out = Vec::new();
        let mut preamble = Vec::new();
        preamble.push(1u8); // TYPE_BITMAP
        preamble.extend_from_slice(&encode_vbyte(num_bits));
        out.extend_from_slice(&preamble);
        out.push(crc8(&preamble));
        out.extend_from_slice(&data);
        out.extend_from_slice(&data_crc.to_le_bytes());
        out
    }

    fn read_and_assemble_log_array(path: &Path, bits_per_entry: u8, num_entries: u64) -> Vec<u8> {
        let mut data = Vec::new();
        File::open(path).unwrap().read_to_end(&mut data).unwrap();
        let data_crc = crate::io::crc_utils::crc32c(&data);

        let mut out = Vec::new();
        let mut preamble = Vec::new();
        preamble.push(1u8); // TYPE_LOG
        preamble.push(bits_per_entry);
        preamble.extend_from_slice(&encode_vbyte(num_entries));
        out.extend_from_slice(&preamble);
        out.push(crc8(&preamble));
        out.extend_from_slice(&data);
        out.extend_from_slice(&data_crc.to_le_bytes());
        out
    }

    #[test]
    fn test_streaming_matches_inmemory() {
        let make_triples = || vec![
            Ok(IdTriple { subject: 1, predicate: 1, object: 1 }),
            Ok(IdTriple { subject: 1, predicate: 1, object: 2 }),
            Ok(IdTriple { subject: 1, predicate: 2, object: 3 }),
            Ok(IdTriple { subject: 2, predicate: 1, object: 1 }),
        ];

        let inmem = build_bitmap_triples(make_triples().into_iter(), 2, 2, 3).unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let files = build_bitmap_triples_to_files(
            make_triples().into_iter(), 2, 2, 3, temp_dir.path(),
        ).unwrap();

        assert_eq!(files.num_triples, inmem.num_triples);

        // Reassemble each component from the temp file and compare
        let by = read_and_assemble_bitmap(&files.bitmap_y.path, files.bitmap_y.num_bits);
        assert_eq!(by, inmem.bitmap_y, "BitmapY mismatch");

        let ay = read_and_assemble_log_array(
            &files.array_y.path, files.array_y.bits_per_entry, files.array_y.num_entries);
        assert_eq!(ay, inmem.array_y, "ArrayY mismatch");

        let bz = read_and_assemble_bitmap(&files.bitmap_z.path, files.bitmap_z.num_bits);
        assert_eq!(bz, inmem.bitmap_z, "BitmapZ mismatch");

        let az = read_and_assemble_log_array(
            &files.array_z.path, files.array_z.bits_per_entry, files.array_z.num_entries);
        assert_eq!(az, inmem.array_z, "ArrayZ mismatch");

        files.cleanup();
    }

    #[test]
    fn test_streaming_single_triple() {
        let make_triple = || vec![Ok(IdTriple { subject: 1, predicate: 1, object: 1 })];

        let inmem = build_bitmap_triples(make_triple().into_iter(), 1, 1, 1).unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let files = build_bitmap_triples_to_files(
            make_triple().into_iter(), 1, 1, 1, temp_dir.path(),
        ).unwrap();

        assert_eq!(files.num_triples, 1);

        let by = read_and_assemble_bitmap(&files.bitmap_y.path, files.bitmap_y.num_bits);
        assert_eq!(by, inmem.bitmap_y);

        let az = read_and_assemble_log_array(
            &files.array_z.path, files.array_z.bits_per_entry, files.array_z.num_entries);
        assert_eq!(az, inmem.array_z);

        files.cleanup();
    }
}
