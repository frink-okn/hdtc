//! HDT file assembly: writes the complete HDT file from dictionary and triples data.
//!
//! File layout:
//! 1. Global Control Information
//! 2. Header (N-Triples metadata)
//! 3. Dictionary (Control Info + Shared PFC + Subjects PFC + Predicates PFC + Objects PFC)
//! 4. Triples (Control Info + BitmapY + ArrayY + BitmapZ + ArrayZ)

use crate::dictionary::DictCounts;
use crate::hdt::header_vocab::{
    HDT_DATASET, HDT_NS, RDF_TYPE, VOID_DATASET, VOID_NS, VOID_STAT_LOCALS,
};
use crate::io::crc_utils::crc8;
use crate::io::vbyte::encode_vbyte;
use crate::io::{ControlInfo, ControlType};
use crate::rdf::serialize_triples;
use crate::triples::BitmapTriplesFiles;
use anyhow::{Context, Result};
use oxrdf::{BlankNode, Literal, NamedNode, Term, Triple};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Write a complete HDT file, reading triples and dict sections from temp files.
///
/// This avoids holding either the dictionary or triples in memory.
pub fn write_hdt_streaming(
    output_path: &Path,
    dataset_uri: &str,
    counts: &DictCounts,
    dict_section_paths: &[PathBuf],
    dict_section_sizes: &[u64],
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
    let dict_size: u64 = dict_section_sizes.iter().sum();
    let triples_size: u64 = triples.total_encoded_size()?;
    let hdt_data_size = dict_size + triples_size;

    // 2. Header
    let header_content = build_header_ntriples(
        dataset_uri,
        counts,
        triples.num_triples,
        dict_size,
        hdt_data_size,
        ntriples_size,
    )?;
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

    for section_path in dict_section_paths {
        let section_file = File::open(section_path)
            .with_context(|| format!("Failed to open dict section {}", section_path.display()))?;
        let mut reader = BufReader::new(section_file);
        std::io::copy(&mut reader, &mut writer)
            .with_context(|| format!("Failed to copy dict section {}", section_path.display()))?;
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
    write_bitmap_from_file(
        &mut writer,
        &triples.bitmap_y.path,
        triples.bitmap_y.num_bits,
    )?;
    write_bitmap_from_file(
        &mut writer,
        &triples.bitmap_z.path,
        triples.bitmap_z.num_bits,
    )?;
    write_log_array_from_file(
        &mut writer,
        &triples.array_y.path,
        triples.array_y.bits_per_entry,
        triples.array_y.num_entries,
    )?;
    write_log_array_from_file(
        &mut writer,
        &triples.array_z.path,
        triples.array_z.bits_per_entry,
        triples.array_z.num_entries,
    )?;

    writer.flush()?;

    tracing::info!("HDT file written: {}", output_path.display());

    Ok(())
}

/// Write a Bitmap section from a temp file containing raw packed data.
/// Writes: preamble (type + VByte(num_bits)) + CRC8 + data + CRC32C
pub(crate) fn write_bitmap_from_file<W: Write>(
    writer: &mut W,
    path: &Path,
    num_bits: u64,
) -> Result<()> {
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
    writer: &mut W,
    path: &Path,
    bits_per_entry: u8,
    num_entries: u64,
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
    let file =
        File::open(path).with_context(|| format!("Failed to open temp file {}", path.display()))?;
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
    dataset_uri: &str,
    counts: &DictCounts,
    num_triples: u64,
    dict_size: u64,
    hdt_data_size: u64,
    ntriples_size: u64,
) -> Result<String> {
    let dcterms = "http://purl.org/dc/terms/";

    // `new_unchecked` mirrors the previous string-interpolation behaviour: the
    // dataset base URI is not validated as an IRI here.
    let iri = |s: &str| NamedNode::new_unchecked(s);
    let term_iri = |s: &str| Term::NamedNode(NamedNode::new_unchecked(s));
    let blank = |s: &str| BlankNode::new_unchecked(s);
    let term_blank = |s: &str| Term::BlankNode(BlankNode::new_unchecked(s));
    // Counts are untyped (xsd:string) literals to match the Java format.
    let lit = |s: String| Term::Literal(Literal::new_simple_literal(s));

    let dataset = iri(dataset_uri);
    let distinct_subjects = counts.shared + counts.subjects;
    let distinct_objects = counts.shared + counts.objects;
    let timestamp = generate_timestamp();

    // Value for each data-derived void statistic, keyed by its local name. The
    // statistic triples are built by iterating VOID_STAT_LOCALS (below), so the
    // `header` command's reserved-predicate set cannot drift from what is
    // emitted here — adding a stat means adding it to both this match and the
    // shared list.
    let void_stat_value = |local: &str| -> String {
        match local {
            "triples" => num_triples.to_string(),
            "properties" => counts.predicates.to_string(),
            "distinctSubjects" => distinct_subjects.to_string(),
            "distinctObjects" => distinct_objects.to_string(),
            other => unreachable!("no value for void statistic {other}"),
        }
    };

    let mut triples = vec![
        // Dataset: type declarations (both hdt#Dataset and void#Dataset, Java style)
        Triple::new(dataset.clone(), iri(RDF_TYPE), term_iri(HDT_DATASET)),
        Triple::new(dataset.clone(), iri(RDF_TYPE), term_iri(VOID_DATASET)),
    ];

    // Dataset: VoID counts (untyped string literals to match Java)
    for local in VOID_STAT_LOCALS {
        triples.push(Triple::new(
            dataset.clone(),
            iri(&format!("{VOID_NS}{local}")),
            lit(void_stat_value(local)),
        ));
    }

    triples.extend([
        // Format information: _:format links to the dictionary and triples nodes
        Triple::new(
            dataset.clone(),
            iri(&format!("{HDT_NS}formatInformation")),
            term_blank("format"),
        ),
        Triple::new(
            blank("format"),
            iri(&format!("{HDT_NS}dictionary")),
            term_blank("dictionary"),
        ),
        Triple::new(
            blank("format"),
            iri(&format!("{HDT_NS}triples")),
            term_blank("triples"),
        ),
        // _:dictionary — format, shared subject/object count, encoded size in bytes
        Triple::new(
            blank("dictionary"),
            iri(&format!("{dcterms}format")),
            term_iri(&format!("{HDT_NS}dictionaryFour")),
        ),
        Triple::new(
            blank("dictionary"),
            iri(&format!("{HDT_NS}dictionarynumSharedSubjectObject")),
            lit(counts.shared.to_string()),
        ),
        Triple::new(
            blank("dictionary"),
            iri(&format!("{HDT_NS}dictionarysizeStrings")),
            lit(dict_size.to_string()),
        ),
        // _:triples — format, triple count, ordering
        Triple::new(
            blank("triples"),
            iri(&format!("{dcterms}format")),
            term_iri(&format!("{HDT_NS}triplesBitmap")),
        ),
        Triple::new(
            blank("triples"),
            iri(&format!("{HDT_NS}triplesnumTriples")),
            lit(num_triples.to_string()),
        ),
        Triple::new(
            blank("triples"),
            iri(&format!("{HDT_NS}triplesOrder")),
            lit("SPO".to_string()),
        ),
        // Statistical information: _:statistics — HDT data size and original N-Triples size in bytes
        Triple::new(
            dataset.clone(),
            iri(&format!("{HDT_NS}statisticalInformation")),
            term_blank("statistics"),
        ),
        Triple::new(
            blank("statistics"),
            iri(&format!("{HDT_NS}hdtSize")),
            lit(hdt_data_size.to_string()),
        ),
        Triple::new(
            blank("statistics"),
            iri(&format!("{HDT_NS}originalSize")),
            lit(ntriples_size.to_string()),
        ),
        // Publication information: _:publicationInformation — issue timestamp (ISO 8601)
        Triple::new(
            dataset.clone(),
            iri(&format!("{HDT_NS}publicationInformation")),
            term_blank("publicationInformation"),
        ),
        Triple::new(
            blank("publicationInformation"),
            iri(&format!("{dcterms}issued")),
            lit(timestamp),
        ),
    ]);

    serialize_triples(&triples)
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

        let header =
            build_header_ntriples("http://example.org/dataset", &counts, 100, 150, 200, 1000)
                .unwrap();

        // Check both dataset types (hdt and void)
        assert!(header.contains("hdt#Dataset"));
        assert!(header.contains("void#Dataset"));

        // Check counts (now untyped literals)
        assert!(header.contains("\"100\""));
        assert!(header.contains("\"15\"")); // distinct subjects = 10 + 5
        assert!(header.contains("\"3\"")); // predicates
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
