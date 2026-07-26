//! The `hdtc-text.meta` manifest: everything about a text index that Tantivy
//! does not model.
//!
//! `docs/text-index-format.md` §4 is normative for this file. It carries the
//! three things a consumer cannot get from the segment files: which HDT the
//! index was built from, which analyzer convention its terms follow, and — the
//! honesty requirement of doc 19 §19.2.4 — exactly what was left out and how
//! much of it there was. An index that silently omits 3% of literals makes a
//! search quietly wrong about coverage, so the omissions are counted and
//! published rather than inferred.
//!
//! The encoding is tab-separated lines rather than JSON so that the file stays
//! greppable and diffable, and so that reading it needs no serialization
//! dependency.

use super::analyzer::{ANALYZER_ID, DatatypeExclusions};
use anyhow::{Context, Result, bail, ensure};
use std::fmt::Write as _;
use std::path::Path;

/// File name of the manifest inside an index directory.
pub const MANIFEST_FILE: &str = "hdtc-text.meta";

/// Manifest schema version, bumped when a line's meaning changes.
pub const MANIFEST_VERSION: u32 = 1;

/// The exact Tantivy release whose segment format this build reads and writes.
///
/// Recorded per index because the segment bytes are Tantivy's, not ours: an
/// index built by a different release may not be readable, and a reader should
/// be able to say so precisely instead of failing on a parse error.
///
/// Taken from Tantivy itself rather than written down here, so that moving the
/// `Cargo.toml` pin cannot leave indexes labelled with a version that did not
/// write them. A mislabelled index is worse than an unreadable one.
pub fn tantivy_version() -> String {
    // Tantivy's `Version` fields are private, so its own rendering —
    // "tantivy v0.26.1, index_format v7" — is the only accessor. The release
    // version is the stricter of the two numbers it carries: two releases can
    // share an index format, and refusing to read across them anyway costs a
    // rebuild rather than a wrong answer.
    let rendered = tantivy::version_string();
    rendered
        .strip_prefix("tantivy v")
        .and_then(|rest| rest.split(',').next())
        .unwrap_or(rendered)
        .to_string()
}

/// One language tag and how many indexed documents carry it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanguageCount {
    pub tag: String,
    pub documents: u64,
}

/// What one text index was built from and what it left out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextManifest {
    pub analyzer_id: u32,
    pub tantivy_version: String,
    /// SHA-256 over the source HDT's dictionary-and-triples suffix, as
    /// `SourceIdentity` computes it for every dictionary-derived artifact.
    pub source_digest: [u8; 32],
    pub max_literal_bytes: u64,
    /// Language untagged literals were stemmed as; `None` when they were left
    /// unstemmed. Recorded because it is an assumption about the data, not a
    /// property of it, and a consumer should be able to read it rather than
    /// infer it from results.
    pub untagged_language: Option<String>,
    /// Literals seen in the object dictionary section, indexed or not.
    pub literals_scanned: u64,
    /// Documents in the index — one per indexed distinct literal.
    pub indexed_docs: u64,
    /// Documents short enough to carry a whole-literal key (§3.7). The rest are
    /// findable but cannot be matched as a whole, so the count is published
    /// rather than left to be inferred.
    pub whole_literal_keys: u64,
    pub excluded_oversize: u64,
    pub excluded_datatype: u64,
    pub excluded_no_tokens: u64,
    pub exclusions: DatatypeExclusions,
    /// Language tags in ascending order, each with its document count.
    pub languages: Vec<LanguageCount>,
}

impl TextManifest {
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "hdtc-text\t{MANIFEST_VERSION}");
        let _ = writeln!(out, "analyzer\t{}", self.analyzer_id);
        let _ = writeln!(out, "tantivy\t{}", self.tantivy_version);
        let _ = writeln!(out, "source_digest\t{}", hex(&self.source_digest));
        let _ = writeln!(out, "max_literal_bytes\t{}", self.max_literal_bytes);
        let _ = writeln!(
            out,
            "untagged_language\t{}",
            self.untagged_language.as_deref().unwrap_or("none")
        );
        let _ = writeln!(out, "literals_scanned\t{}", self.literals_scanned);
        let _ = writeln!(out, "indexed_docs\t{}", self.indexed_docs);
        let _ = writeln!(out, "whole_literal_keys\t{}", self.whole_literal_keys);
        let _ = writeln!(out, "excluded_oversize\t{}", self.excluded_oversize);
        let _ = writeln!(out, "excluded_datatype\t{}", self.excluded_datatype);
        let _ = writeln!(out, "excluded_no_tokens\t{}", self.excluded_no_tokens);
        for iri in self.exclusions.iris() {
            let _ = writeln!(out, "excluded_datatype_iri\t{iri}");
        }
        for language in &self.languages {
            let _ = writeln!(out, "language\t{}\t{}", language.tag, language.documents);
        }
        out
    }

    pub fn parse(text: &str) -> Result<Self> {
        let mut version = None;
        let mut analyzer_id = None;
        let mut written_by = None;
        let mut source_digest = [0u8; 32];
        let mut max_literal_bytes = 0u64;
        let mut untagged_language = None;
        let mut literals_scanned = 0u64;
        let mut indexed_docs = 0u64;
        let mut whole_literal_keys = 0u64;
        let mut excluded_oversize = 0u64;
        let mut excluded_datatype = 0u64;
        let mut excluded_no_tokens = 0u64;
        let mut exclusion_iris = Vec::new();
        let mut languages = Vec::new();

        for (number, line) in text.lines().enumerate() {
            if line.is_empty() {
                continue;
            }
            let mut fields = line.split('\t');
            let key = fields.next().unwrap_or_default();
            let value = fields.next().unwrap_or_default();
            let at = || format!("{MANIFEST_FILE} line {}", number + 1);
            let number_value = |value: &str| -> Result<u64> {
                value
                    .parse::<u64>()
                    .with_context(|| format!("Invalid number in {}", at()))
            };

            match key {
                "hdtc-text" => version = Some(number_value(value)?),
                "analyzer" => analyzer_id = Some(number_value(value)?),
                "tantivy" => written_by = Some(value.to_string()),
                "source_digest" => {
                    source_digest = parse_hex(value).with_context(at)?;
                }
                "max_literal_bytes" => max_literal_bytes = number_value(value)?,
                "untagged_language" => {
                    untagged_language = (value != "none").then(|| value.to_string());
                }
                "literals_scanned" => literals_scanned = number_value(value)?,
                "indexed_docs" => indexed_docs = number_value(value)?,
                "whole_literal_keys" => whole_literal_keys = number_value(value)?,
                "excluded_oversize" => excluded_oversize = number_value(value)?,
                "excluded_datatype" => excluded_datatype = number_value(value)?,
                "excluded_no_tokens" => excluded_no_tokens = number_value(value)?,
                "excluded_datatype_iri" => exclusion_iris.push(value.to_string()),
                "language" => {
                    let documents = fields
                        .next()
                        .with_context(|| format!("Missing document count in {}", at()))?;
                    languages.push(LanguageCount {
                        tag: value.to_string(),
                        documents: number_value(documents)?,
                    });
                }
                // Unknown keys are ignored, so a later version may add lines
                // without making this one refuse the file outright.
                _ => {}
            }
        }

        let version =
            version.context("Not an hdtc text index: no hdtc-text line in the manifest")?;
        ensure!(
            version == u64::from(MANIFEST_VERSION),
            "Unsupported text index manifest version {version} (this build reads version \
             {MANIFEST_VERSION})"
        );
        let analyzer_id = analyzer_id.context("Text index manifest declares no analyzer")?;
        let analyzer_id = u32::try_from(analyzer_id).unwrap_or(u32::MAX);
        ensure!(
            analyzer_id == ANALYZER_ID,
            "Text index was built with analyzer {analyzer_id}, which this build cannot query (it \
             implements analyzer {ANALYZER_ID})"
        );
        let written_by = written_by.context("Text index manifest declares no Tantivy version")?;
        let linked = tantivy_version();
        ensure!(
            written_by == linked,
            "Text index was written by Tantivy {written_by}; this build links Tantivy {linked} \
             and cannot rely on reading its segments. Rebuild with `hdtc text`."
        );
        languages.sort_by(|a, b| a.tag.cmp(&b.tag));

        Ok(Self {
            analyzer_id,
            tantivy_version: written_by,
            source_digest,
            max_literal_bytes,
            untagged_language,
            literals_scanned,
            indexed_docs,
            whole_literal_keys,
            excluded_oversize,
            excluded_datatype,
            excluded_no_tokens,
            exclusions: DatatypeExclusions::from_iris(exclusion_iris),
            languages,
        })
    }

    pub fn read(index_dir: &Path) -> Result<Self> {
        let path = index_dir.join(MANIFEST_FILE);
        let text = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "Failed to read text index manifest {} (is {} an hdtc text index?)",
                path.display(),
                index_dir.display()
            )
        })?;
        Self::parse(&text).with_context(|| format!("Invalid manifest {}", path.display()))
    }

    pub fn write(&self, index_dir: &Path) -> Result<()> {
        let path = index_dir.join(MANIFEST_FILE);
        std::fs::write(&path, self.to_text())
            .with_context(|| format!("Failed to write {}", path.display()))
    }

    /// Total literals not indexed, for the coverage line a summary reports.
    pub fn excluded_total(&self) -> u64 {
        self.excluded_oversize + self.excluded_datatype + self.excluded_no_tokens
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn parse_hex(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 {
        bail!(
            "Expected a 64-character hex digest, got {} characters",
            text.len()
        );
    }
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
            .context("Digest is not hexadecimal")?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> TextManifest {
        TextManifest {
            analyzer_id: ANALYZER_ID,
            tantivy_version: tantivy_version(),
            source_digest: [0xab; 32],
            max_literal_bytes: 4096,
            untagged_language: Some("en".to_string()),
            literals_scanned: 120,
            indexed_docs: 100,
            whole_literal_keys: 95,
            excluded_oversize: 3,
            excluded_datatype: 15,
            excluded_no_tokens: 2,
            exclusions: DatatypeExclusions::from_iris(vec![
                "http://www.w3.org/2001/XMLSchema#integer".to_string(),
                "http://www.w3.org/2001/XMLSchema#date".to_string(),
            ]),
            languages: vec![
                LanguageCount {
                    tag: "en".to_string(),
                    documents: 70,
                },
                LanguageCount {
                    tag: "und".to_string(),
                    documents: 30,
                },
            ],
        }
    }

    /// The version is read out of Tantivy rather than written down, so this is
    /// the one place the pin is asserted. It failing means the `Cargo.toml` pin
    /// moved: that changes the segment bytes every published index carries and
    /// makes existing indexes refuse to open, so it needs a deliberate update
    /// here and in `docs/text-index-format.md` §5, not a silent one.
    #[test]
    fn the_recorded_version_is_the_documented_pin() {
        assert_eq!(tantivy_version(), "0.26.1");
    }

    #[test]
    fn manifest_round_trips_through_its_text_form() {
        let manifest = sample();
        let text = manifest.to_text();
        assert!(text.starts_with("hdtc-text\t1\n"));
        assert!(text.contains("source_digest\tabababab"));
        assert_eq!(TextManifest::parse(&text).unwrap(), manifest);
        assert_eq!(manifest.excluded_total(), 20);
    }

    /// Exclusion IRIs are written in sorted order regardless of how they were
    /// supplied, so two builds with the same configuration produce the same
    /// manifest bytes.
    #[test]
    fn exclusion_iris_are_written_in_a_stable_order() {
        let text = sample().to_text();
        let iris: Vec<&str> = text
            .lines()
            .filter_map(|line| line.strip_prefix("excluded_datatype_iri\t"))
            .collect();
        assert_eq!(
            iris,
            [
                "http://www.w3.org/2001/XMLSchema#date",
                "http://www.w3.org/2001/XMLSchema#integer"
            ]
        );
    }

    #[test]
    fn an_index_this_build_cannot_read_is_named_precisely() {
        let bad_analyzer = sample().to_text().replace("analyzer\t3", "analyzer\t7");
        assert!(
            TextManifest::parse(&bad_analyzer)
                .unwrap_err()
                .to_string()
                .contains("analyzer 7")
        );

        let bad_tantivy = sample().to_text().replace(
            &format!("tantivy\t{}", tantivy_version()),
            "tantivy\t0.22.0",
        );
        let error = TextManifest::parse(&bad_tantivy).unwrap_err().to_string();
        assert!(
            error.contains("0.22.0") && error.contains("hdtc text"),
            "{error}"
        );

        let bad_version = sample().to_text().replace("hdtc-text\t1", "hdtc-text\t9");
        assert!(
            TextManifest::parse(&bad_version)
                .unwrap_err()
                .to_string()
                .contains("manifest version 9")
        );

        assert!(TextManifest::parse("something else\t1\n").is_err());
    }

    /// A later version may add lines; this one must ignore what it does not
    /// know rather than refuse the file.
    #[test]
    fn unknown_lines_are_ignored() {
        let text = format!("{}future_field\t99\n", sample().to_text());
        assert_eq!(TextManifest::parse(&text).unwrap(), sample());
    }

    #[test]
    fn a_malformed_digest_is_refused() {
        let text = sample().to_text().replace(&hex(&[0xab; 32]), "abc");
        assert!(TextManifest::parse(&text).is_err());
    }
}
