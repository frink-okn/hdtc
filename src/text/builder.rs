//! Building a text index from one pass over an HDT's object dictionary.
//!
//! The unit of indexing is a **distinct literal**, not an occurrence, and a
//! document's identity is its HDT object dictionary ID (doc 19 §19.2.2). The
//! consequence worth restating, because it is what makes indexing every literal
//! affordable: the index stores no subject and no predicate at all. A hit is an
//! object ID, and `? ? <that object>` through the OPS index turns it into every
//! (subject, predicate) that uses it.
//!
//! Only the object-only dictionary section is read. A literal can never be an
//! RDF subject, so it is never in the shared section — scanning shared would
//! cost a second decompression pass to find nothing.

use super::analyzer::{
    self, DatatypeExclusions, Exclusion, UNDETERMINED_LANGUAGE, collect_tokens, normalize_language,
    parse_literal, stemmer_language, stemming_tokenizer, tokenizer, whole_literal_key,
};
use super::manifest::{LanguageCount, TextManifest, tantivy_version};
use super::schema::{
    FIELD_LANG, FIELD_OBJECT, FIELD_TEXT, FIELD_TEXT_EXACT, FIELD_TEXT_STEMMED, register_tokenizer,
    text_schema,
};
use crate::hdt::artifacts::SourceIdentity;
use crate::hdt::input_adapter::HdtInputAdapter;
use anyhow::{Context, Result, bail, ensure};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tantivy::merge_policy::NoMergePolicy;
use tantivy::tokenizer::{Language, PreTokenizedString, TextAnalyzer};
use tantivy::{Index, IndexWriter, TantivyDocument};

/// Tantivy refuses a per-thread arena below this, and performs poorly near it.
const MIN_WRITER_BYTES_PER_THREAD: usize = 20 * 1024 * 1024;
/// Tantivy's own ceiling is just under 4 GiB per thread; stay clear of it.
const MAX_WRITER_BYTES_PER_THREAD: usize = 2 * 1024 * 1024 * 1024;
/// More indexing threads than this stop paying for themselves against a single
/// sequential dictionary reader feeding them.
const MAX_WRITER_THREADS: usize = 8;

/// Parameters for one text-index build.
#[derive(Debug, Clone)]
pub struct TextConfig {
    pub hdt_path: PathBuf,
    /// Directory the index is published at; must not already exist.
    pub output_dir: PathBuf,
    pub max_literal_bytes: usize,
    pub exclusions: DatatypeExclusions,
    /// Language to stem untagged literals as; `None` leaves them unstemmed.
    pub untagged_language: Option<String>,
    pub memory_limit: usize,
    pub threads: Option<usize>,
}

/// What a completed build indexed, and what it left out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSummary {
    pub output_dir: PathBuf,
    pub manifest: TextManifest,
    pub index_bytes: u64,
}

/// Build a text index over every qualifying literal in the HDT's dictionary.
///
/// The index is written to a staging directory and renamed into place only
/// after the manifest is written and the source is confirmed unchanged, so an
/// interrupted build never leaves a half-built index under the published name.
pub fn create_text_index(config: &TextConfig) -> Result<TextSummary> {
    ensure!(
        config.max_literal_bytes > 0,
        "--max-literal-bytes must be greater than zero"
    );
    if config.output_dir.exists() {
        bail!(
            "Refusing to replace existing text index: {}",
            config.output_dir.display()
        );
    }

    // Digested first, so every dictionary byte read afterwards is covered by
    // the `ensure_unchanged` below.
    let source = SourceIdentity::capture(&config.hdt_path)?;
    let adapter = HdtInputAdapter::scan(&config.hdt_path)?;

    let staging = StagingDir::create(&config.output_dir)?;
    let index = Index::create_in_dir(staging.path(), text_schema()).with_context(|| {
        format!(
            "Failed to create text index in {}",
            staging.path().display()
        )
    })?;
    register_tokenizer(&index);

    let schema = index.schema();
    let text_field = schema.get_field(FIELD_TEXT)?;
    let stemmed_field = schema.get_field(FIELD_TEXT_STEMMED)?;
    let exact_field = schema.get_field(FIELD_TEXT_EXACT)?;
    let object_field = schema.get_field(FIELD_OBJECT)?;
    let lang_field = schema.get_field(FIELD_LANG)?;
    let mut stemmers = Stemmers::new(config.untagged_language.as_deref())?;
    let mut plain = tokenizer();

    let (threads, budget) = writer_budget(config.memory_limit, config.threads);
    tracing::info!(
        "Indexing with {threads} writer thread(s), {} total arena",
        crate::hdt::artifacts::format_bytes(budget as u64)
    );
    let mut writer: IndexWriter<TantivyDocument> = index
        .writer_with_num_threads(threads, budget)
        .context("Failed to create text index writer")?;
    // The published index is written once and never appended to, so the
    // background merge policy has nothing useful to decide. Disabling it keeps
    // segments stable until the single deliberate merge below — with the policy
    // active, it can consume the very segments that merge was about to name.
    writer.set_merge_policy(Box::new(NoMergePolicy));

    let mut counts = ScanCounts::default();
    let mut languages: HashMap<String, u64> = HashMap::new();

    tracing::info!("Scanning HDT object dictionary for literals");
    // Object IDs in the object-only section continue after the shared section
    // (`docs/text-index-format.md` §2), which is the ID a search resolves.
    let shared_count = adapter.shared_count;
    for (offset, term) in adapter.object_terms()?.enumerate() {
        let term = term?;
        let Some(literal) = parse_literal(&term) else {
            continue;
        };
        counts.literals_scanned += 1;

        match analyzer::classify(&literal, config.max_literal_bytes, &config.exclusions) {
            Some(Exclusion::Oversize) => {
                counts.excluded_oversize += 1;
                continue;
            }
            Some(Exclusion::Datatype) => {
                counts.excluded_datatype += 1;
                continue;
            }
            Some(Exclusion::NoTokens) => {
                counts.excluded_no_tokens += 1;
                continue;
            }
            None => {}
        }

        let object_id = shared_count + offset as u64 + 1;
        let language = literal
            .language
            .map(normalize_language)
            .unwrap_or_else(|| UNDETERMINED_LANGUAGE.to_string());

        let value = String::from_utf8_lossy(literal.value).into_owned();
        let mut document = TantivyDocument::new();
        document.add_u64(object_field, object_id);
        document.add_text(lang_field, &language);
        // The whole-literal key comes from the same plain tokens the `text`
        // field is built from, so "which resource is named this" is asked in
        // the terms the index actually holds.
        let plain_tokens = collect_tokens(&mut plain, &value);
        if let Some(key) = whole_literal_key(plain_tokens.iter().map(|token| token.text.as_str())) {
            document.add_text(exact_field, &key);
            counts.whole_literal_keys += 1;
        }
        // A literal in a language with no Snowball algorithm — or under
        // `--untagged-language none` — is simply left out of the stemmed field.
        // It stays exactly searchable; only the extra recall is unavailable.
        if let Some(analyzer) = stemmers.for_language(&language) {
            let tokens = collect_tokens(analyzer, &value);
            document.add_pre_tokenized_text(
                stemmed_field,
                PreTokenizedString {
                    text: value.clone(),
                    tokens,
                },
            );
        }
        document.add_text(text_field, value);
        writer
            .add_document(document)
            .context("Failed to add a literal to the text index")?;

        *languages.entry(language).or_default() += 1;
        counts.indexed_docs += 1;
        if counts.indexed_docs.is_multiple_of(1_000_000) {
            tracing::info!("Indexed {} literals", counts.indexed_docs);
        }
    }

    tracing::info!(
        "Committing {} documents ({} literals scanned, {} short enough for whole-literal \
         matching)",
        counts.indexed_docs,
        counts.literals_scanned,
        counts.whole_literal_keys
    );
    writer.commit().context("Failed to commit the text index")?;
    merge_to_one_segment(&index, &mut writer)?;
    writer
        .wait_merging_threads()
        .context("Text index merge threads failed")?;

    let mut languages: Vec<LanguageCount> = languages
        .into_iter()
        .map(|(tag, documents)| LanguageCount { tag, documents })
        .collect();
    languages.sort_by(|a, b| a.tag.cmp(&b.tag));

    let manifest = TextManifest {
        analyzer_id: analyzer::ANALYZER_ID,
        tantivy_version: tantivy_version(),
        source_digest: *source.digest(),
        max_literal_bytes: config.max_literal_bytes as u64,
        untagged_language: config.untagged_language.clone(),
        literals_scanned: counts.literals_scanned,
        indexed_docs: counts.indexed_docs,
        whole_literal_keys: counts.whole_literal_keys,
        excluded_oversize: counts.excluded_oversize,
        excluded_datatype: counts.excluded_datatype,
        excluded_no_tokens: counts.excluded_no_tokens,
        exclusions: config.exclusions.clone(),
        languages,
    };
    manifest.write(staging.path())?;
    let index_bytes = directory_bytes(staging.path())?;

    // The dictionary was read by path after the digest was taken. Confirm it is
    // still the file that was digested before this index becomes public.
    source.ensure_unchanged(&config.hdt_path)?;
    staging.publish(&config.output_dir)?;

    Ok(TextSummary {
        output_dir: config.output_dir.clone(),
        manifest,
        index_bytes,
    })
}

/// One stemming analyzer per language seen, built on first use.
///
/// A `TextAnalyzer` carries a token buffer, so it is reused across documents
/// rather than rebuilt per literal — the scan visits millions of them.
struct Stemmers {
    /// Language to stem `und` documents as, already resolved to an algorithm.
    untagged: Option<Language>,
    /// `None` marks a language with no Snowball algorithm, so the lookup is
    /// attempted once rather than once per literal.
    analyzers: HashMap<String, Option<TextAnalyzer>>,
}

impl Stemmers {
    fn new(untagged_language: Option<&str>) -> Result<Self> {
        let untagged = match untagged_language {
            None => None,
            Some(tag) => Some(stemmer_language(tag).with_context(|| {
                format!(
                    "No stemmer for --untagged-language {tag}. Snowball covers ar, da, nl, en, \
                     fi, fr, de, el, hu, it, no, pt, ro, ru, es, sv, ta and tr; pass `none` to \
                     leave untagged literals unstemmed."
                )
            })?),
        };
        Ok(Self {
            untagged,
            analyzers: HashMap::new(),
        })
    }

    fn for_language(&mut self, language: &str) -> Option<&mut TextAnalyzer> {
        let untagged = self.untagged;
        self.analyzers
            .entry(language.to_string())
            .or_insert_with(|| {
                let algorithm = if language == UNDETERMINED_LANGUAGE {
                    untagged
                } else {
                    stemmer_language(language)
                };
                algorithm.map(stemming_tokenizer)
            })
            .as_mut()
    }
}

#[derive(Debug, Default)]
struct ScanCounts {
    literals_scanned: u64,
    indexed_docs: u64,
    excluded_oversize: u64,
    excluded_datatype: u64,
    excluded_no_tokens: u64,
    whole_literal_keys: u64,
}

/// Merge every segment into one, so the published index is a compact artifact
/// rather than a snapshot of whatever the merge policy happened to leave.
fn merge_to_one_segment(index: &Index, writer: &mut IndexWriter<TantivyDocument>) -> Result<()> {
    let segments = index
        .searchable_segment_ids()
        .context("Failed to list text index segments")?;
    if segments.len() > 1 {
        tracing::info!("Merging {} segments", segments.len());
        writer
            .merge(&segments)
            .wait()
            .context("Failed to merge text index segments")?;
    }
    writer
        .garbage_collect_files()
        .wait()
        .context("Failed to remove superseded text index segments")?;
    Ok(())
}

/// Split `memory_limit` into a thread count and the total arena Tantivy divides
/// among them.
///
/// Tantivy enforces a floor per thread, so a small limit buys fewer threads
/// rather than an arena too small to make progress — the same trade the key-set
/// builder makes when it shares one budget across roles.
fn writer_budget(memory_limit: usize, requested_threads: Option<usize>) -> (usize, usize) {
    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let wanted = requested_threads
        .unwrap_or(available)
        .clamp(1, MAX_WRITER_THREADS);
    let budget = memory_limit.max(MIN_WRITER_BYTES_PER_THREAD);
    // Never fewer than one thread, never more than the budget can feed.
    let threads = wanted.min(budget / MIN_WRITER_BYTES_PER_THREAD).max(1);
    let per_thread =
        (budget / threads).clamp(MIN_WRITER_BYTES_PER_THREAD, MAX_WRITER_BYTES_PER_THREAD);
    (threads, per_thread * threads)
}

fn directory_bytes(dir: &Path) -> Result<u64> {
    let mut total = 0;
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("Failed to read {}", dir.display()))?
    {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

/// A directory an index is built into, removed on drop unless it was published.
///
/// The staging directory is a sibling of the target so that publication is a
/// rename within one filesystem, which is atomic where the platform allows it.
struct StagingDir {
    path: PathBuf,
    published: bool,
}

impl StagingDir {
    fn create(target: &Path) -> Result<Self> {
        let parent = target.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}", parent.display()))?;
        let name = target
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| "text".to_string());
        let path = parent.join(format!(".{name}.building.{}", std::process::id()));
        if path.exists() {
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("Failed to clear stale {}", path.display()))?;
        }
        std::fs::create_dir(&path)
            .with_context(|| format!("Failed to create staging directory {}", path.display()))?;
        Ok(Self {
            path,
            published: false,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publish(mut self, target: &Path) -> Result<()> {
        std::fs::rename(&self.path, target).with_context(|| {
            format!(
                "Failed to publish text index {} as {}",
                self.path.display(),
                target.display()
            )
        })?;
        self.published = true;
        Ok(())
    }
}

impl Drop for StagingDir {
    fn drop(&mut self) {
        if !self.published
            && let Err(error) = std::fs::remove_dir_all(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                "Failed to remove staging directory {}: {error}",
                self.path.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The budget rule: honour the limit, respect Tantivy's per-thread floor,
    /// and never end up with zero threads however small the limit is.
    #[test]
    fn writer_budget_respects_the_per_thread_floor() {
        let (threads, total) = writer_budget(4 << 30, Some(4));
        assert_eq!(threads, 4);
        assert_eq!(total, 4 << 30);

        // A limit too small for the requested threads buys fewer threads.
        let (threads, total) = writer_budget(40 * 1024 * 1024, Some(8));
        assert_eq!(threads, 2);
        assert_eq!(total, 40 * 1024 * 1024);

        // Below one thread's floor, the floor wins — Tantivy would refuse less.
        let (threads, total) = writer_budget(1024, Some(8));
        assert_eq!(threads, 1);
        assert_eq!(total, MIN_WRITER_BYTES_PER_THREAD);

        // The thread cap holds even on a very large machine.
        let (threads, _) = writer_budget(64 << 30, Some(64));
        assert_eq!(threads, MAX_WRITER_THREADS);

        // Per-thread arenas stay under Tantivy's own ceiling.
        let (threads, total) = writer_budget(32 << 30, Some(1));
        assert_eq!(threads, 1);
        assert_eq!(total, MAX_WRITER_BYTES_PER_THREAD);
    }

    /// A staging directory that is never published leaves nothing behind, which
    /// is what keeps a failed build from shadowing the index name.
    #[test]
    fn an_unpublished_staging_directory_is_removed() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("data.hdt.text");
        let staged_path = {
            let staging = StagingDir::create(&target).unwrap();
            let path = staging.path().to_path_buf();
            assert!(path.is_dir());
            path
        };
        assert!(!staged_path.exists());
        assert!(!target.exists());
    }

    #[test]
    fn publishing_renames_the_staging_directory_into_place() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("data.hdt.text");
        let staging = StagingDir::create(&target).unwrap();
        std::fs::write(staging.path().join("marker"), b"x").unwrap();
        staging.publish(&target).unwrap();
        assert!(target.join("marker").is_file());
    }
}
