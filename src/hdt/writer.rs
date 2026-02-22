//! HDT file assembly: writes the complete HDT file from dictionary and triples data.
//!
//! File layout:
//! 1. Global Control Information
//! 2. Header (N-Triples metadata)
//! 3. Dictionary (Control Info + Shared PFC + Subjects PFC + Predicates PFC + Objects PFC)
//! 4. Triples (Control Info + BitmapY + ArrayY + BitmapZ + ArrayZ)

use crate::dictionary::DictCounts;
use crate::io::crc_utils::crc8;
use crate::io::vbyte::encode_vbyte;
use crate::io::{ControlInfo, ControlType};
use crate::triples::BitmapTriplesFiles;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Write a complete HDT file, reading triples data from streaming temp files.
///
/// This avoids holding the entire triples section in memory. Dict sections are
/// still in memory (typically much smaller than triples).
pub fn write_hdt_streaming(
    output_path: &Path,
    base_uri: &str,
    counts: &DictCounts,
    dict_sections: &[Vec<u8>],
    triples: &BitmapTriplesFiles,
    ntriples_size: u64,
) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("Failed to create output file {}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);

    // 1. Global Control Information
    let global_ci = ControlInfo::new(ControlType::Global, "<http://purl.org/HDT/hdt#HDTv1>");
    global_ci.write_to(&mut writer)?;

    // Calculate sizes for header metadata
    let dict_size: u64 = dict_sections.iter().map(|s| s.len() as u64).sum();
    let triples_size: u64 = triples.total_encoded_size()?;
    let hdt_data_size = dict_size + triples_size;

    // 2. Header
    let header_content = build_header_ntriples(
        base_uri,
        counts,
        triples.num_triples,
        dict_size,
        hdt_data_size,
        ntriples_size,
    );
    let mut header_ci = ControlInfo::new(ControlType::Header, "ntriples");
    header_ci.set_property("length", header_content.len().to_string());
    header_ci.write_to(&mut writer)?;
    writer.write_all(header_content.as_bytes())?;

    // 3. Dictionary
    let total_elements = counts.shared + counts.subjects + counts.predicates + counts.objects;
    let mut dict_ci = ControlInfo::new(
        ControlType::Dictionary,
        "<http://purl.org/HDT/hdt#dictionaryFour>",
    );
    dict_ci.set_property("elements", total_elements.to_string());
    dict_ci.write_to(&mut writer)?;

    for section_data in dict_sections {
        writer.write_all(section_data)?;
    }

    // 4. Triples
    let mut triples_ci = ControlInfo::new(
        ControlType::Triples,
        "<http://purl.org/HDT/hdt#triplesBitmap>",
    );
    triples_ci.set_property("order", "1"); // SPO
    triples_ci.write_to(&mut writer)?;

    // Write each component: preamble + CRC8 + data (from temp file) + CRC32C
    // Order: BitmapY, BitmapZ, ArrayY (SeqY), ArrayZ (SeqZ) — matching hdt-java
    write_bitmap_from_file(&mut writer, &triples.bitmap_y.path, triples.bitmap_y.num_bits)?;
    write_bitmap_from_file(&mut writer, &triples.bitmap_z.path, triples.bitmap_z.num_bits)?;
    write_log_array_from_file(
        &mut writer, &triples.array_y.path,
        triples.array_y.bits_per_entry, triples.array_y.num_entries)?;
    write_log_array_from_file(
        &mut writer, &triples.array_z.path,
        triples.array_z.bits_per_entry, triples.array_z.num_entries)?;

    writer.flush()?;

    tracing::info!(
        "HDT file written (streaming): {}",
        output_path.display()
    );

    Ok(())
}

/// Write a Bitmap section from a temp file containing raw packed data.
/// Writes: preamble (type + VByte(num_bits)) + CRC8 + data + CRC32C
pub(crate) fn write_bitmap_from_file<W: Write>(writer: &mut W, path: &Path, num_bits: u64) -> Result<()> {
    // Preamble
    let mut preamble = Vec::new();
    preamble.push(1u8); // TYPE_BITMAP
    preamble.extend_from_slice(&encode_vbyte(num_bits));
    writer.write_all(&preamble)?;
    writer.write_all(&[crc8(&preamble)])?;

    // Copy data from temp file while computing CRC32C
    let data_crc = copy_file_with_crc(writer, path)?;
    writer.write_all(&data_crc.to_le_bytes())?;

    Ok(())
}

/// Write a LogArray section from a temp file containing raw packed data.
/// Writes: preamble (type + bits_per_entry + VByte(num_entries)) + CRC8 + data + CRC32C
pub(crate) fn write_log_array_from_file<W: Write>(
    writer: &mut W, path: &Path, bits_per_entry: u8, num_entries: u64,
) -> Result<()> {
    // Preamble
    let mut preamble = Vec::new();
    preamble.push(1u8); // TYPE_LOG
    preamble.push(bits_per_entry);
    preamble.extend_from_slice(&encode_vbyte(num_entries));
    writer.write_all(&preamble)?;
    writer.write_all(&[crc8(&preamble)])?;

    // Copy data from temp file while computing CRC32C
    let data_crc = copy_file_with_crc(writer, path)?;
    writer.write_all(&data_crc.to_le_bytes())?;

    Ok(())
}

/// Copy a file's contents to a writer, computing CRC32C over the data.
fn copy_file_with_crc<W: Write>(writer: &mut W, path: &Path) -> Result<u32> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open temp file {}", path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);

    let mut buf = [0u8; 64 * 1024];
    let crc_algo = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
    let mut digest = crc_algo.digest();

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        digest.update(&buf[..n]);
        writer.write_all(&buf[..n])?;
    }

    Ok(digest.finalize())
}

/// Build the header section content as N-Triples (Java-compatible format).
fn build_header_ntriples(
    base_uri: &str,
    counts: &DictCounts,
    num_triples: u64,
    dict_size: u64,
    hdt_data_size: u64,
    ntriples_size: u64,
) -> String {
    let mut lines = Vec::new();
    let dataset = format!("<{base_uri}>");
    let void = "http://rdfs.org/ns/void#";
    let rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    let dcterms = "http://purl.org/dc/terms/";
    let hdt_ns = "http://purl.org/HDT/hdt#";

    // Match Java's type declarations (both hdt#Dataset and void#Dataset)
    lines.push(format!(
        "{dataset} <{rdf}type> <{hdt_ns}Dataset> ."
    ));
    lines.push(format!(
        "{dataset} <{rdf}type> <{void}Dataset> ."
    ));

    // Counts (untyped literals to match Java format)
    lines.push(format!(
        "{dataset} <{void}triples> \"{num_triples}\" ."
    ));

    lines.push(format!(
        "{dataset} <{void}properties> \"{}\" .",
        counts.predicates
    ));

    let distinct_subjects = counts.shared + counts.subjects;
    lines.push(format!(
        "{dataset} <{void}distinctSubjects> \"{distinct_subjects}\" ."
    ));

    let distinct_objects = counts.shared + counts.objects;
    lines.push(format!(
        "{dataset} <{void}distinctObjects> \"{distinct_objects}\" ."
    ));

    // Blank node for format information (Java style)
    lines.push(format!(
        "{dataset} <{hdt_ns}formatInformation> _:format ."
    ));
    lines.push(format!(
        "_:format <{hdt_ns}dictionary> _:dictionary ."
    ));
    lines.push(format!(
        "_:format <{hdt_ns}triples> _:triples ."
    ));

    // Blank node for statistical information
    lines.push(format!(
        "{dataset} <{hdt_ns}statisticalInformation> _:statistics ."
    ));

    // Blank node for publication information
    lines.push(format!(
        "{dataset} <{hdt_ns}publicationInformation> _:publicationInformation ."
    ));

    // Dictionary format information
    lines.push(format!(
        "_:dictionary <{dcterms}format> <{hdt_ns}dictionaryFour> ."
    ));
    lines.push(format!(
        "_:dictionary <{hdt_ns}dictionarynumSharedSubjectObject> \"{shared}\" .",
        shared = counts.shared
    ));

    // Dictionary size in bytes (actual encoded size)
    lines.push(format!(
        "_:dictionary <{hdt_ns}dictionarysizeStrings> \"{dict_size}\" ."
    ));

    // Triples format information
    lines.push(format!(
        "_:triples <{dcterms}format> <{hdt_ns}triplesBitmap> ."
    ));
    lines.push(format!(
        "_:triples <{hdt_ns}triplesnumTriples> \"{num_triples}\" ."
    ));
    lines.push(format!(
        "_:triples <{hdt_ns}triplesOrder> \"SPO\" ."
    ));

    // Statistical information (HDT data size in bytes)
    lines.push(format!(
        "_:statistics <{hdt_ns}hdtSize> \"{hdt_data_size}\" ."
    ));

    // Publication information with timestamp (ISO 8601 format)
    let timestamp = generate_timestamp();
    lines.push(format!(
        "_:publicationInformation <{dcterms}issued> \"{timestamp}\" ."
    ));

    // Original N-Triples serialization size
    lines.push(format!(
        "_:statistics <{hdt_ns}originalSize> \"{ntriples_size}\" ."
    ));

    lines.join("\n") + "\n"
}

/// Generate ISO 8601 timestamp for publication info.
fn generate_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Get current time since Unix epoch
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let total_secs = duration.as_secs();

    // Simple date calculation for 1970-2100 range
    // This is a simplified approximation; use chrono for production accuracy
    let days_since_epoch = total_secs / 86400;
    let secs_today = total_secs % 86400;

    let hours = secs_today / 3600;
    let minutes = (secs_today % 3600) / 60;

    // Rough year calculation (doesn't account for leap years perfectly)
    let mut year = 1970;
    let mut remaining_days = days_since_epoch;

    loop {
        let days_in_year = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
            366
        } else {
            365
        };

        if remaining_days < days_in_year as u64 {
            break;
        }

        remaining_days -= days_in_year as u64;
        year += 1;

        if year > 2100 {
            // Fallback for out-of-range dates
            return "2026-02-16T00:00Z".to_string();
        }
    }

    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut month = 1;
    let mut day_of_month = remaining_days + 1;

    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    for (i, &days) in month_days.iter().enumerate() {
        let days_in_month = if i == 1 && is_leap { 29 } else { days };
        if day_of_month <= days_in_month as u64 {
            month = i + 1;
            break;
        }
        day_of_month -= days_in_month as u64;
    }

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}Z",
        year, month, day_of_month, hours, minutes
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_ntriples_content() {
        let counts = DictCounts {
            shared: 10,
            subjects: 5,
            predicates: 3,
            objects: 7,
            graphs: 0,
        };

        let header = build_header_ntriples("http://example.org/dataset", &counts, 100, 150, 200, 1000);

        // Check both dataset types (hdt and void)
        assert!(header.contains("hdt#Dataset"));
        assert!(header.contains("void#Dataset"));

        // Check counts (now untyped literals)
        assert!(header.contains("\"100\""));
        assert!(header.contains("\"15\"")); // distinct subjects = 10 + 5
        assert!(header.contains("\"3\""));  // predicates
        assert!(header.contains("\"17\"")); // distinct objects = 10 + 7

        // Check blank node structures
        assert!(header.contains("_:format"));
        assert!(header.contains("_:dictionary"));
        assert!(header.contains("_:triples"));
        assert!(header.contains("_:statistics"));
        assert!(header.contains("_:publicationInformation"));

        // Check format information
        assert!(header.contains("dictionaryFour"));
        assert!(header.contains("triplesBitmap"));
        assert!(header.contains("SPO"));

        // Check that statistics are present
        assert!(header.contains("\"150\"")); // dict_size
        assert!(header.contains("\"200\"")); // hdt_data_size
        assert!(header.contains("\"1000\"")); // original_input_size
    }
}
