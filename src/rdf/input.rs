use anyhow::{bail, Result};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Supported RDF serialization formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdfFormat {
    NTriples,
    NQuads,
    Turtle,
    TriG,
    RdfXml,
    JsonLd,
    N3,
}

impl RdfFormat {
    /// Returns true if this format can contain quad (named graph) information.
    #[allow(dead_code)]
    pub fn is_quad_format(self) -> bool {
        matches!(self, RdfFormat::NQuads | RdfFormat::TriG)
    }
}

/// Compression applied to an input file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Bzip2,
    Xz,
}

/// A discovered RDF input file with its detected format and compression.
#[derive(Debug, Clone)]
pub struct RdfInput {
    pub path: PathBuf,
    pub format: RdfFormat,
    pub compression: Compression,
}

/// Detect compression from file extension, returning the compression type
/// and the remaining path (with compression extension stripped) for format detection.
fn detect_compression(path: &Path) -> (Compression, PathBuf) {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match ext.as_str() {
        "gz" => (Compression::Gzip, path.with_extension("")),
        "bz2" => (Compression::Bzip2, path.with_extension("")),
        "xz" => (Compression::Xz, path.with_extension("")),
        _ => (Compression::None, path.to_path_buf()),
    }
}

/// Detect RDF format from file extension.
fn detect_format(path: &Path) -> Option<RdfFormat> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match ext.as_str() {
        "nt" | "ntriples" => Some(RdfFormat::NTriples),
        "nq" | "nquads" => Some(RdfFormat::NQuads),
        "ttl" | "turtle" => Some(RdfFormat::Turtle),
        "trig" => Some(RdfFormat::TriG),
        "rdf" | "xml" | "owl" => Some(RdfFormat::RdfXml),
        "jsonld" | "json" => Some(RdfFormat::JsonLd),
        "n3" => Some(RdfFormat::N3),
        _ => None,
    }
}

/// Discover all RDF input files from a list of paths (files and/or directories).
///
/// Directories are walked recursively. Files with unrecognized extensions are
/// skipped with a warning logged via `tracing`.
pub fn discover_inputs(paths: &[PathBuf]) -> Result<Vec<RdfInput>> {
    let mut inputs = Vec::new();

    for path in paths {
        if !path.exists() {
            bail!("Input path does not exist: {}", path.display());
        }

        if path.is_file() {
            match classify_file(path) {
                Some(input) => inputs.push(input),
                None => {
                    tracing::warn!("Skipping file with unrecognized extension: {}", path.display());
                }
            }
        } else if path.is_dir() {
            for entry in WalkDir::new(path)
                .follow_links(true)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    if let Some(input) = classify_file(entry.path()) {
                        inputs.push(input);
                    }
                    // Silently skip unrecognized files in directories
                }
            }
        } else {
            bail!(
                "Input path is neither a file nor a directory: {}",
                path.display()
            );
        }
    }

    if inputs.is_empty() {
        bail!("No RDF input files found");
    }

    // Sort for deterministic processing order (important for blank node disambiguation)
    inputs.sort_by(|a, b| a.path.cmp(&b.path));

    tracing::info!("Discovered {} RDF input files", inputs.len());
    Ok(inputs)
}

/// Try to classify a file path as an RDF input.
fn classify_file(path: &Path) -> Option<RdfInput> {
    let (compression, base_path) = detect_compression(path);
    let format = detect_format(&base_path)?;

    Some(RdfInput {
        path: path.to_path_buf(),
        format,
        compression,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_format() {
        assert_eq!(detect_format(Path::new("data.nt")), Some(RdfFormat::NTriples));
        assert_eq!(detect_format(Path::new("data.nq")), Some(RdfFormat::NQuads));
        assert_eq!(detect_format(Path::new("data.ttl")), Some(RdfFormat::Turtle));
        assert_eq!(detect_format(Path::new("data.trig")), Some(RdfFormat::TriG));
        assert_eq!(detect_format(Path::new("data.rdf")), Some(RdfFormat::RdfXml));
        assert_eq!(detect_format(Path::new("data.xml")), Some(RdfFormat::RdfXml));
        assert_eq!(detect_format(Path::new("data.jsonld")), Some(RdfFormat::JsonLd));
        assert_eq!(detect_format(Path::new("data.n3")), Some(RdfFormat::N3));
        assert_eq!(detect_format(Path::new("data.txt")), None);
        assert_eq!(detect_format(Path::new("data")), None);
    }

    #[test]
    fn test_detect_compression() {
        let (comp, base) = detect_compression(Path::new("data.nt.gz"));
        assert_eq!(comp, Compression::Gzip);
        assert_eq!(base, Path::new("data.nt"));

        let (comp, base) = detect_compression(Path::new("data.ttl.bz2"));
        assert_eq!(comp, Compression::Bzip2);
        assert_eq!(base, Path::new("data.ttl"));

        let (comp, base) = detect_compression(Path::new("data.nq.xz"));
        assert_eq!(comp, Compression::Xz);
        assert_eq!(base, Path::new("data.nq"));

        let (comp, base) = detect_compression(Path::new("data.nt"));
        assert_eq!(comp, Compression::None);
        assert_eq!(base, Path::new("data.nt"));
    }

    #[test]
    fn test_classify_compressed_file() {
        let input = classify_file(Path::new("data.nt.gz")).unwrap();
        assert_eq!(input.format, RdfFormat::NTriples);
        assert_eq!(input.compression, Compression::Gzip);

        let input = classify_file(Path::new("data.trig.xz")).unwrap();
        assert_eq!(input.format, RdfFormat::TriG);
        assert_eq!(input.compression, Compression::Xz);
    }

    #[test]
    fn test_classify_unknown_extension() {
        assert!(classify_file(Path::new("data.csv")).is_none());
    }

    #[test]
    fn test_quad_format_detection() {
        assert!(RdfFormat::NQuads.is_quad_format());
        assert!(RdfFormat::TriG.is_quad_format());
        assert!(!RdfFormat::NTriples.is_quad_format());
        assert!(!RdfFormat::Turtle.is_quad_format());
        assert!(!RdfFormat::RdfXml.is_quad_format());
    }
}
