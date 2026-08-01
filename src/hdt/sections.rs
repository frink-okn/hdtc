//! Structural scan of an HDT file: where every section is, from preambles only.
//!
//! An HDT is a sequence of self-describing sections — global control info,
//! header, dictionary control info and its four PFC sections, triples control
//! info and its four BitmapTriples sections — each of which states its own size.
//! Locating all of them is therefore a walk of about a dozen small reads that
//! touches no payload byte, whatever the size of the file.
//!
//! That property is what makes this the right entry point for two very
//! different readers: this crate's builders, which need the SPO bitmaps'
//! offsets before streaming a sidecar, and out-of-process mapped readers such as
//! KGF's `kgf-store`, whose whole open path is this walk plus a projection of
//! each region onto an `mmap` (KGF doc 20 §20.1, §20.4). Neither may call the
//! materializing readers — [`PfcSectionHeader::read_from`] expands a
//! dictionary-sized block-offset array — so the scan forms
//! ([`scan_pfc_section`], [`scan_bitmap_section`], [`scan_log_array_section`])
//! are what this composes.

use crate::io::{
    BitmapSection, ControlInfo, ControlType, LogArraySection, scan_bitmap_section,
    scan_log_array_section,
};
use anyhow::{Context, Result, ensure};
use std::io::{Read, Seek, SeekFrom};

#[cfg(doc)]
use super::pfc_reader::PfcSectionHeader;
use super::pfc_reader::{PfcSection, scan_pfc_section};

/// Dictionary format URI of the four-section dictionary this crate writes and
/// reads. Recorded in the dictionary control info.
pub const DICTIONARY_FOUR_FORMAT: &str = "<http://purl.org/HDT/hdt#dictionaryFour>";

/// Triples format URI of the BitmapTriples encoding. Recorded in the triples
/// control info.
pub const TRIPLES_BITMAP_FORMAT: &str = "<http://purl.org/HDT/hdt#triplesBitmap>";

/// Value of the triples control info's `order` property for SPO order.
pub const TRIPLES_ORDER_SPO: u64 = 1;

/// Byte offsets and shapes of every section in an HDT file.
///
/// Produced by [`scan_hdt_sections`]. Offsets are absolute within the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HdtSections {
    /// Offset of the header's N-Triples blob, past its control info.
    pub header_offset: u64,
    /// Offset where the dictionary's control info begins — equivalently, the end
    /// of the header.
    ///
    /// Identity digests start here rather than at byte 0, so that a header
    /// rewrite leaves derived artifacts bound to their HDT
    /// (`docs/permutation-index-format.md` §9).
    pub data_offset: u64,
    /// Terms that occur as both subject and object.
    pub shared: PfcSection,
    /// Terms that occur only as subjects.
    pub subjects: PfcSection,
    /// Predicates.
    pub predicates: PfcSection,
    /// Terms that occur only as objects.
    pub objects: PfcSection,
    /// Bitmap over `ArrayY` positions: one bit per (subject, predicate) pair,
    /// set at the last predicate of each subject.
    pub bitmap_y: BitmapSection,
    /// Bitmap over `ArrayZ` positions: one bit per triple, set at the last
    /// object of each (subject, predicate) pair.
    pub bitmap_z: BitmapSection,
    /// Predicate ids, grouped by subject.
    pub array_y: LogArraySection,
    /// Object ids, grouped by (subject, predicate) pair.
    pub array_z: LogArraySection,
}

impl HdtSections {
    /// Triples in the file: one `ArrayZ` entry each.
    pub fn num_triples(&self) -> u64 {
        self.array_z.num_entries
    }

    /// Distinct (subject, predicate) pairs: one `ArrayY` entry each.
    pub fn num_sp_pairs(&self) -> u64 {
        self.array_y.num_entries
    }

    /// Offset just past the last section, which for a well-formed file is its
    /// length. Callers that know the file length should compare the two —
    /// anything else means trailing or truncated bytes.
    pub fn end(&self) -> u64 {
        self.array_z.section_end
    }

    /// Read back the header's N-Triples blob.
    ///
    /// [`scan_hdt_sections`] seeks past the header rather than buffering it,
    /// since most readers only need to know where the data begins. Callers that
    /// want what the header *declares* — `void:triples`, the original size —
    /// come back for it here, once, with the extent the scan already recorded.
    ///
    /// Leaves `reader` at [`data_offset`](Self::data_offset), not at
    /// [`end`](Self::end).
    pub fn read_header<R: Read + Seek>(&self, reader: &mut R) -> Result<String> {
        let length = usize::try_from(self.data_offset - self.header_offset)
            .context("HDT header is too large to buffer")?;
        reader.seek(SeekFrom::Start(self.header_offset))?;
        let mut bytes = vec![0u8; length];
        reader
            .read_exact(&mut bytes)
            .context("Failed to read the HDT header")?;
        String::from_utf8(bytes).context("HDT header is not valid UTF-8")
    }
}

/// Walk an HDT from the start of `reader`, recording where every section is.
///
/// Reads control info blocks and section preambles — around a dozen small reads
/// — and **no payload byte**. Verifies each preamble's CRC8, the dictionary and
/// triples formats, SPO ordering, and that the bitmaps and arrays of each level
/// agree on their lengths. Payload CRC32Cs are not checked: that is a
/// verification pass over the whole file, not a structural scan.
///
/// `reader` is left positioned at [`HdtSections::end`].
pub fn scan_hdt_sections<R: Read + Seek>(reader: &mut R) -> Result<HdtSections> {
    let global =
        ControlInfo::read_from(reader).context("Failed to read HDT global control info")?;
    ensure!(
        global.control_type == ControlType::Global,
        "expected HDT global control info, found {:?}",
        global.control_type
    );

    let header =
        ControlInfo::read_from(reader).context("Failed to read HDT header control info")?;
    ensure!(
        header.control_type == ControlType::Header,
        "expected HDT header control info, found {:?}",
        header.control_type
    );
    let header_length: u64 = header
        .get_property("length")
        .context("HDT header is missing length")?
        .parse()
        .context("invalid HDT header length")?;
    let header_offset = reader.stream_position()?;
    let data_offset = reader.seek(SeekFrom::Current(
        i64::try_from(header_length).context("HDT header is too large to seek")?,
    ))?;

    let dictionary =
        ControlInfo::read_from(reader).context("Failed to read HDT dictionary control info")?;
    ensure!(
        dictionary.control_type == ControlType::Dictionary,
        "expected HDT dictionary control info, found {:?}",
        dictionary.control_type
    );
    ensure!(
        dictionary.format == DICTIONARY_FOUR_FORMAT,
        "unsupported HDT dictionary format: {}",
        dictionary.format
    );
    let shared = scan_pfc_section(reader, "shared")?;
    let subjects = scan_pfc_section(reader, "subjects")?;
    let predicates = scan_pfc_section(reader, "predicates")?;
    let objects = scan_pfc_section(reader, "objects")?;

    let triples =
        ControlInfo::read_from(reader).context("Failed to read HDT triples control info")?;
    ensure!(
        triples.control_type == ControlType::Triples,
        "expected HDT triples control info, found {:?}",
        triples.control_type
    );
    ensure!(
        triples.format == TRIPLES_BITMAP_FORMAT,
        "unsupported HDT triples format: {}",
        triples.format
    );
    let order: u64 = triples
        .get_property("order")
        .context("HDT triples control info is missing order")?
        .parse()
        .context("invalid HDT triples order")?;
    ensure!(
        order == TRIPLES_ORDER_SPO,
        "expected SPO-ordered HDT triples, found order {order}"
    );

    // Written in this order — bitmaps before arrays — matching hdt-java.
    let bitmap_y = scan_bitmap_section(reader).context("Failed to scan BitmapY")?;
    let bitmap_z = scan_bitmap_section(reader).context("Failed to scan BitmapZ")?;
    let array_y = scan_log_array_section(reader).context("Failed to scan ArrayY")?;
    let array_z = scan_log_array_section(reader).context("Failed to scan ArrayZ")?;
    ensure!(
        bitmap_y.num_bits == array_y.num_entries,
        "HDT BitmapY/ArrayY length mismatch: {} bits, {} entries",
        bitmap_y.num_bits,
        array_y.num_entries
    );
    ensure!(
        bitmap_z.num_bits == array_z.num_entries,
        "HDT BitmapZ/ArrayZ length mismatch: {} bits, {} entries",
        bitmap_z.num_bits,
        array_z.num_entries
    );

    Ok(HdtSections {
        header_offset,
        data_offset,
        shared,
        subjects,
        predicates,
        objects,
        bitmap_y,
        bitmap_z,
        array_y,
        array_z,
    })
}
