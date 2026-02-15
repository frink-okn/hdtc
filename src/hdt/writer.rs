//! HDT file assembly: writes the complete HDT file from dictionary and triples data.
//!
//! File layout:
//! 1. Global Control Information
//! 2. Header (N-Triples metadata)
//! 3. Dictionary (Control Info + Shared PFC + Subjects PFC + Predicates PFC + Objects PFC)
//! 4. Triples (Control Info + BitmapY + ArrayY + BitmapZ + ArrayZ)

use crate::dictionary::DictCounts;
use crate::io::{ControlInfo, ControlType};
use crate::triples::builder::BitmapTriplesData;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Write a complete HDT file.
pub fn write_hdt(
    output_path: &Path,
    base_uri: &str,
    counts: &DictCounts,
    dict_sections: &[Vec<u8>],
    triples: &BitmapTriplesData,
) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("Failed to create output file {}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);

    // 1. Global Control Information
    let mut global_ci = ControlInfo::new(ControlType::Global, "<http://purl.org/HDT/hdt#HDTv1>");
    global_ci.set_property("BaseURI", base_uri);
    global_ci.set_property("Software", "hdtc");
    global_ci.write_to(&mut writer)?;

    // 2. Header
    let header_content = build_header_ntriples(base_uri, counts, triples.num_triples);
    let mut header_ci = ControlInfo::new(ControlType::Header, "ntriples");
    header_ci.set_property("length", &header_content.len().to_string());
    header_ci.write_to(&mut writer)?;
    writer.write_all(header_content.as_bytes())?;

    // 3. Dictionary
    let total_elements = counts.shared + counts.subjects + counts.predicates + counts.objects;
    let mut dict_ci = ControlInfo::new(
        ControlType::Dictionary,
        "<http://purl.org/HDT/hdt#dictionaryFour>",
    );
    dict_ci.set_property("mapping", "1");
    dict_ci.set_property("elements", &total_elements.to_string());
    dict_ci.write_to(&mut writer)?;

    // Write dictionary sections: shared, subjects, predicates, objects
    // (Each section includes its own PFC type byte prefix)
    for section_data in dict_sections {
        writer.write_all(section_data)?;
    }

    // 4. Triples
    let mut triples_ci = ControlInfo::new(
        ControlType::Triples,
        "<http://purl.org/HDT/hdt#triplesBitmap>",
    );
    triples_ci.set_property("order", "1"); // SPO
    triples_ci.set_property("numTriples", &triples.num_triples.to_string());
    triples_ci.write_to(&mut writer)?;

    // Write: BitmapY, BitmapZ, SeqY, SeqZ (hdt-java reads in this order)
    writer.write_all(&triples.bitmap_y)?;
    writer.write_all(&triples.bitmap_z)?;
    writer.write_all(&triples.array_y)?;
    writer.write_all(&triples.array_z)?;

    writer.flush()?;

    tracing::info!(
        "HDT file written: {}",
        output_path.display()
    );

    Ok(())
}

/// Build the header section content as N-Triples.
fn build_header_ntriples(base_uri: &str, counts: &DictCounts, num_triples: u64) -> String {
    let mut lines = Vec::new();
    let dataset = format!("<{base_uri}>");
    let void = "http://rdfs.org/ns/void#";
    let rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    let dcterms = "http://purl.org/dc/terms/";
    let hdt_ns = "http://purl.org/HDT/hdt#";

    // Type
    lines.push(format!(
        "{dataset} <{rdf}type> <{void}Dataset> ."
    ));

    // Counts
    lines.push(format!(
        "{dataset} <{void}triples> \"{num_triples}\"^^<http://www.w3.org/2001/XMLSchema#integer> ."
    ));

    let distinct_subjects = counts.shared + counts.subjects;
    lines.push(format!(
        "{dataset} <{void}distinctSubjects> \"{distinct_subjects}\"^^<http://www.w3.org/2001/XMLSchema#integer> ."
    ));

    lines.push(format!(
        "{dataset} <{void}properties> \"{}\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
        counts.predicates
    ));

    let distinct_objects = counts.shared + counts.objects;
    lines.push(format!(
        "{dataset} <{void}distinctObjects> \"{distinct_objects}\"^^<http://www.w3.org/2001/XMLSchema#integer> ."
    ));

    lines.push(format!(
        "{dataset} <{void}entities> \"{}\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
        counts.shared
    ));

    // HDT properties
    lines.push(format!(
        "{dataset} <{hdt_ns}dictionaryFormat> <{hdt_ns}dictionaryFour> ."
    ));
    lines.push(format!(
        "{dataset} <{hdt_ns}triplesFormat> <{hdt_ns}triplesBitmap> ."
    ));

    // Software
    lines.push(format!(
        "{dataset} <{dcterms}source> <{base_uri}> ."
    ));

    lines.join("\n") + "\n"
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

        let header = build_header_ntriples("http://example.org/dataset", &counts, 100);

        assert!(header.contains("void#Dataset"));
        assert!(header.contains("\"100\""));
        assert!(header.contains("\"15\"")); // distinct subjects = 10 + 5
        assert!(header.contains("\"3\""));  // predicates
        assert!(header.contains("\"17\"")); // distinct objects = 10 + 7
        assert!(header.contains("\"10\"")); // shared entities
    }
}
