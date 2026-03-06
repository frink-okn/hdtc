//! HDT index file reader (.hdt.index.v1-1).
//!
//! Scans an index file and records byte offsets of its sections so that
//! streaming decoders can be opened at those positions for query execution.
//!
//! Index file layout (in order):
//! ```text
//! ControlInfo         — type=INDEX, format="<http://purl.org/HDT/hdt#indexFoQ>",
//!                       properties: numTriples, order
//! bitmapIndexZ        — n_triples bits  (used for ??O / ?PO — Phase 3)
//! indexZ              — n_triples entries (used for ??O / ?PO — Phase 3)
//! predicateIndex.bitmap  — n_sp bits; 1-bit = last pos_y for current predicate group
//! predicateIndex.seq     — n_sp entries; pos_y values sorted by (predicate, pos_y)
//! predicateCount      — LogArray; count of SP pairs per predicate
//! ```

use crate::io::{ControlInfo, ControlType, skip_bitmap_section, skip_log_array_section};
use anyhow::{Context, Result, bail};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;

const INDEX_FORMAT: &str = "<http://purl.org/HDT/hdt#indexFoQ>";

/// Byte offsets of sections within an HDT index file.
#[derive(Debug, Clone, Copy)]
pub struct IndexSectionOffsets {
    /// Start offset of predicateIndex.bitmap section.
    pub pred_bitmap_start: u64,
    /// Start offset of predicateIndex.seq section.
    pub pred_seq_start: u64,
}

/// Open an HDT index file, verify its structure, and return section offsets.
///
/// Skips the object-index sections (bitmapIndexZ, indexZ) which are needed
/// only for Phase 3 (`??O`, `?PO` patterns). Records the predicate-index
/// section offsets used for Phase 2 (`?P?` pattern).
pub fn open_index(index_path: &Path) -> Result<IndexSectionOffsets> {
    let file = File::open(index_path)
        .with_context(|| format!("Failed to open index file {}", index_path.display()))?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);

    let ci = ControlInfo::read_from(&mut reader)
        .context("Failed to read index control info")?;

    if ci.control_type != ControlType::Index {
        bail!(
            "Expected index control info (type=INDEX), got {:?} in {}",
            ci.control_type,
            index_path.display()
        );
    }
    if ci.format != INDEX_FORMAT {
        bail!(
            "Unsupported index format: {} (expected {}) in {}",
            ci.format,
            INDEX_FORMAT,
            index_path.display()
        );
    }

    // Skip bitmapIndexZ (Phase 3 object index)
    skip_bitmap_section(&mut reader)
        .context("Failed to skip bitmapIndexZ section")?;

    // Skip indexZ (Phase 3 object index)
    skip_log_array_section(&mut reader)
        .context("Failed to skip indexZ section")?;

    // Record start of predicateIndex.bitmap
    let pred_bitmap_start = reader
        .stream_position()
        .context("Failed to get pred bitmap section offset")?;
    skip_bitmap_section(&mut reader)
        .context("Failed to skip predicateIndex.bitmap section")?;

    // Record start of predicateIndex.seq
    let pred_seq_start = reader
        .stream_position()
        .context("Failed to get pred seq section offset")?;

    Ok(IndexSectionOffsets {
        pred_bitmap_start,
        pred_seq_start,
    })
}

/// BufReader positioned at a given offset in the index file.
pub(crate) fn open_index_section(
    index_path: &Path,
    offset: u64,
) -> Result<BufReader<File>> {
    let mut f = File::open(index_path)
        .with_context(|| format!("Failed to open index file {}", index_path.display()))?;
    f.seek(SeekFrom::Start(offset))
        .with_context(|| format!("Failed to seek to offset {offset} in index file"))?;
    Ok(BufReader::with_capacity(256 * 1024, f))
}
