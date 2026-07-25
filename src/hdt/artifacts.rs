//! Machinery shared by the dictionary-derived sidecar artifact families.
//!
//! `hdtc sketch` (`docs/sketch-format.md`) and `hdtc keyset`
//! (`docs/keyset-format.md`) publish different files from the same underlying
//! stream: the distinct 64-bit keys of an HDT dictionary role. Both declare
//! `convention_id = 1`, which asserts that a term becomes a key by exactly one
//! rule — so [`iri_hash`] lives here and is the single definition of that rule
//! for both commands. A divergence between the two would be silent: filters
//! would report members absent and key-set intersections would come out empty,
//! with nothing to detect it.
//!
//! Also here: two ways of getting a role's keys off the heap, and the
//! staged-then-renamed publication both commands use to avoid leaving a partial
//! output directory behind.
//!
//! The two key paths differ because their consumers do. [`KeySorter`]
//! externally sorts to an ascending run on disk, which `hdtc keyset` streams —
//! so a key set of any size builds within any memory limit. [`KeySpool`] merely
//! spools and reads back into memory, which is all `hdtc sketch` can use:
//! binary fuse construction peels a hypergraph over the entire key set and has
//! no streaming form, so it pays for a resident array and enforces a key
//! ceiling in exchange.

use crate::sort::{ExternalSorter, Sortable};
use anyhow::{Context, Result, bail, ensure};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use xxhash_rust::xxh64::xxh64;

/// Map a dictionary term to its 64-bit key, or `None` when the term does not
/// qualify.
///
/// The qualifying rule is `docs/sketch-format.md` §1.2 and
/// `docs/keyset-format.md` §1.2: absolute IRIs only. HDT stores IRIs as their
/// raw UTF-8 bytes, blank nodes with `_:`, and literals with a leading quote.
/// Current hdtc output retains blank nodes; a future documented skolem prefix
/// can be excluded here as well.
pub(crate) fn iri_hash(term: &[u8]) -> Option<u64> {
    if term.starts_with(b"_:") || term.starts_with(b"\"") {
        None
    } else {
        Some(xxh64(term, 0))
    }
}

/// One role's keys, spooled to a temporary file as they are scanned.
///
/// Roles are accumulated concurrently during the single dictionary pass but
/// consumed one at a time, so keeping every role's keys resident would cost
/// several times what the largest role needs on its own.
pub(crate) struct KeySpool {
    label: &'static str,
    writer: BufWriter<File>,
    key_count: u64,
}

impl KeySpool {
    pub(crate) fn new(temp_dir: &Path, label: &'static str) -> Result<Self> {
        let file = tempfile::tempfile_in(temp_dir).with_context(|| {
            format!(
                "Failed to create temporary {label} key file in {}",
                temp_dir.display()
            )
        })?;
        Ok(Self {
            label,
            writer: BufWriter::with_capacity(256 * 1024, file),
            key_count: 0,
        })
    }

    /// Number of qualifying IRIs spooled so far. Keys are not yet deduplicated,
    /// so this counts occurrences, not distinct values.
    pub(crate) fn key_count(&self) -> u64 {
        self.key_count
    }

    pub(crate) fn push(&mut self, hash: u64) -> Result<()> {
        self.writer.write_all(&hash.to_le_bytes())?;
        self.key_count = self
            .key_count
            .checked_add(1)
            .context("IRI key count exceeds u64")?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<SpooledKeys> {
        self.writer.flush()?;
        let file = self
            .writer
            .into_inner()
            .map_err(|error| error.into_error())?;
        let expected_bytes = self
            .key_count
            .checked_mul(8)
            .context("Temporary key file length exceeds u64")?;
        ensure!(
            file.metadata()?.len() == expected_bytes,
            "Temporary {} key file has an unexpected length",
            self.label
        );
        Ok(SpooledKeys {
            label: self.label,
            file,
            key_count: self.key_count,
        })
    }
}

/// A completed key spool, ready to be read back.
pub(crate) struct SpooledKeys {
    label: &'static str,
    file: File,
    key_count: u64,
}

/// What a duplicate key means for the role being read back, which decides how
/// one is reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DuplicateKeys {
    /// The role draws from mutually disjoint dictionary sections, so its terms
    /// are distinct by construction and a duplicate key can only be an XXH64
    /// collision — worth a warning.
    AreCollisions,
    /// The role draws from sections that can repeat an IRI, so duplicates are
    /// ordinary. The HDT Predicates section is a separate ID space whose IRIs
    /// may also appear as subjects or objects, and in real vocabularies they
    /// routinely do. Reporting these as collisions would overstate the
    /// collision rate by orders of magnitude.
    AreExpected,
}

impl SpooledKeys {
    /// Number of qualifying IRIs scanned, before deduplication.
    pub(crate) fn key_count(&self) -> u64 {
        self.key_count
    }

    /// Read the spooled keys back, sorted ascending and deduplicated.
    ///
    /// Both consumers need distinct keys: binary fuse construction is only
    /// defined for them, and a key set is by definition a set. Deduplicating
    /// here makes the input meet that precondition instead of leaving it to
    /// chance.
    pub(crate) fn read_sorted_distinct(&mut self, duplicates: DuplicateKeys) -> Result<Vec<u64>> {
        let count =
            usize::try_from(self.key_count).context("Key count does not fit this platform")?;
        self.file.rewind()?;
        let mut reader = BufReader::with_capacity(256 * 1024, &mut self.file);
        let mut keys = Vec::with_capacity(count);
        let mut bytes = [0u8; 8];
        for _ in 0..count {
            reader
                .read_exact(&mut bytes)
                .with_context(|| format!("Truncated temporary {} key file", self.label))?;
            keys.push(u64::from_le_bytes(bytes));
        }

        keys.sort_unstable();
        let scanned = keys.len();
        keys.dedup();
        let removed = scanned - keys.len();
        if removed > 0 {
            match duplicates {
                DuplicateKeys::AreCollisions => tracing::warn!(
                    "{} XXH64 collision(s) among {} {} IRIs; the colliding IRIs are \
                     indistinguishable in the emitted artifacts",
                    removed,
                    scanned,
                    self.label
                ),
                DuplicateKeys::AreExpected => tracing::debug!(
                    "{} of {} {} IRIs occur in more than one dictionary section",
                    removed,
                    scanned,
                    self.label
                ),
            }
        }
        Ok(keys)
    }
}

// ---------------------------------------------------------------------------
// Source binding
// ---------------------------------------------------------------------------

/// The source HDT's digest, together with enough identity to tell whether the
/// file that was digested is still the file being read.
///
/// A build opens the source several times — to scan the dictionary layout, to
/// digest it, and once per dictionary section — so an HDT replaced mid-build
/// can contribute keys from the new bytes under a digest taken from the old.
/// `source_digest` being advisory (docs/keyset-format.md §6) means it may be
/// *stale*; it does not license it being *wrong about which bytes it covers*,
/// which is the one thing that would make it actively misleading.
///
/// This does not make the build atomic — nothing short of holding the file
/// open throughout would — but it converts a silently mislabelled artifact
/// into a failed run, which is the outcome that matters for files other parties
/// consume.
#[derive(Debug, Clone)]
pub(crate) struct SourceIdentity {
    digest: [u8; 32],
    marks: FileMarks,
}

/// Cheap identity of a file: what a `stat` can tell us.
///
/// On Unix the device and inode catch the common atomic replace (write a
/// temporary, rename over the target) even when the replacement preserves
/// length and timestamp, as `rsync -a` and `cp -p` do.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileMarks {
    len: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl FileMarks {
    fn read(path: &Path) -> Result<Self> {
        let metadata = fs::metadata(path)
            .with_context(|| format!("Failed to stat source HDT {}", path.display()))?;
        Ok(Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            device: std::os::unix::fs::MetadataExt::dev(&metadata),
            #[cfg(unix)]
            inode: std::os::unix::fs::MetadataExt::ino(&metadata),
        })
    }
}

impl SourceIdentity {
    /// Digest the source HDT and record which file was digested.
    pub(crate) fn capture(path: &Path) -> Result<Self> {
        // Marked on both sides of the digest, so a replacement *during* the
        // digest — which would otherwise yield a hash of two different files
        // spliced together — is caught here rather than at publication.
        let before = FileMarks::read(path)?;
        let digest = super::reader::hdt_data_digest(path)?;
        let marks = FileMarks::read(path)?;
        ensure!(
            before == marks,
            "Source HDT {} changed while it was being read; the digest cannot be trusted",
            path.display()
        );
        Ok(Self { digest, marks })
    }

    pub(crate) fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// Fail if the source is no longer the file that was digested.
    ///
    /// Call immediately before publishing: everything between [`capture`] and
    /// here — the dictionary scan and every section read — is then known to
    /// have come from one snapshot.
    ///
    /// [`capture`]: SourceIdentity::capture
    pub(crate) fn ensure_unchanged(&self, path: &Path) -> Result<()> {
        let now = FileMarks::read(path)?;
        ensure!(
            now == self.marks,
            "Source HDT {} changed during the build; refusing to publish artifacts whose keys and \
             source_digest may describe different bytes",
            path.display()
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Externally sorted key runs
// ---------------------------------------------------------------------------

/// One key, as the external sorter stores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Key(u64);

impl Sortable for Key {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut bytes = [0u8; 8];
        match reader.read_exact(&mut bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        Ok(Some(Key(u64::from_le_bytes(bytes))))
    }

    fn mem_size(&self) -> usize {
        8
    }
}

/// A role's keys, externally sorted so that neither the dictionary scan nor the
/// encoding needs them resident.
///
/// Keys are buffered to `memory_budget`, sorted, and spilled to compressed
/// chunk files; [`KeySorter::finish`] k-way merges the chunks, deduplicating as
/// it goes, into one ascending run on disk.
pub(crate) struct KeySorter {
    label: &'static str,
    temp_dir: PathBuf,
    sorter: ExternalSorter,
    buffer: Vec<Key>,
    mem_used: usize,
    scanned: u64,
}

impl KeySorter {
    pub(crate) fn new(temp_dir: &Path, label: &'static str, memory_budget: usize) -> Self {
        Self {
            label,
            temp_dir: temp_dir.to_path_buf(),
            sorter: ExternalSorter::new(temp_dir, memory_budget),
            buffer: Vec::new(),
            mem_used: 0,
            scanned: 0,
        }
    }

    /// Qualifying IRIs pushed so far, before deduplication.
    pub(crate) fn scanned(&self) -> u64 {
        self.scanned
    }

    pub(crate) fn push(&mut self, key: u64) -> Result<()> {
        self.sorter
            .push(Key(key), &mut self.buffer, &mut self.mem_used)?;
        self.scanned = self
            .scanned
            .checked_add(1)
            .context("IRI key count exceeds u64")?;
        Ok(())
    }

    /// Merge every chunk into a single ascending, distinct run on disk.
    ///
    /// The run is stored uncompressed at 8 bytes per distinct key so that the
    /// encoder can stream it — twice, for Elias-Fano, which writes its low-bits
    /// array before its high-bits vector.
    pub(crate) fn finish(mut self, duplicates: DuplicateKeys) -> Result<SortedKeyRun> {
        let merged = self.sorter.finish(&mut self.buffer)?;

        let file = tempfile::tempfile_in(&self.temp_dir).with_context(|| {
            format!(
                "Failed to create temporary {} key run in {}",
                self.label,
                self.temp_dir.display()
            )
        })?;
        let mut writer = BufWriter::with_capacity(256 * 1024, file);
        let (mut key_count, mut min_key, mut max_key) = (0u64, 0u64, 0u64);
        for key in merged {
            let Key(key) = key?;
            if key_count == 0 {
                min_key = key;
            }
            max_key = key;
            writer.write_all(&key.to_le_bytes())?;
            key_count += 1;
        }
        writer.flush()?;
        let file = writer.into_inner().map_err(|error| error.into_error())?;
        // `self`, and with it the ExternalSorter that owns the chunk files, is
        // still alive: the merge above reads those files, and dropping the
        // sorter deletes them.

        let removed = self.scanned - key_count;
        if removed > 0 {
            match duplicates {
                DuplicateKeys::AreCollisions => tracing::warn!(
                    "{} XXH64 collision(s) among {} {} IRIs; the colliding IRIs are \
                     indistinguishable in the emitted artifacts",
                    removed,
                    self.scanned,
                    self.label
                ),
                DuplicateKeys::AreExpected => tracing::debug!(
                    "{} of {} {} IRIs occur in more than one dictionary section",
                    removed,
                    self.scanned,
                    self.label
                ),
            }
        }

        Ok(SortedKeyRun {
            label: self.label,
            file,
            key_count,
            min_key,
            max_key,
        })
    }
}

/// A merged run of ascending, distinct keys on disk, re-readable in one pass.
pub(crate) struct SortedKeyRun {
    label: &'static str,
    file: File,
    key_count: u64,
    min_key: u64,
    max_key: u64,
}

impl SortedKeyRun {
    /// Number of distinct keys — the `key_count` header field.
    pub(crate) fn key_count(&self) -> u64 {
        self.key_count
    }

    /// Smallest and largest key; both `0` for an empty run.
    pub(crate) fn range(&self) -> (u64, u64) {
        (self.min_key, self.max_key)
    }

    /// Stream the run from the beginning.
    pub(crate) fn keys(&mut self) -> Result<KeyRunIter<'_>> {
        self.file.rewind()?;
        Ok(KeyRunIter {
            label: self.label,
            reader: BufReader::with_capacity(256 * 1024, &mut self.file),
            remaining: self.key_count,
        })
    }
}

pub(crate) struct KeyRunIter<'a> {
    label: &'static str,
    reader: BufReader<&'a mut File>,
    remaining: u64,
}

impl Iterator for KeyRunIter<'_> {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut bytes = [0u8; 8];
        match self.reader.read_exact(&mut bytes) {
            Ok(()) => {
                self.remaining -= 1;
                Some(Ok(u64::from_le_bytes(bytes)))
            }
            Err(error) => Some(Err(anyhow::Error::new(error)
                .context(format!("Truncated temporary {} key run", self.label)))),
        }
    }
}

/// A finished artifact written to a temporary file, awaiting publication.
pub(crate) struct StagedArtifact {
    pub(crate) file: NamedTempFile,
    pub(crate) target: PathBuf,
}

pub(crate) fn prepare_output_directory(output_dir: &Path) -> Result<()> {
    if output_dir.exists() {
        ensure!(
            output_dir.is_dir(),
            "Output path is not a directory: {}",
            output_dir.display()
        );
    } else {
        fs::create_dir_all(output_dir).with_context(|| {
            format!("Failed to create output directory {}", output_dir.display())
        })?;
    }
    Ok(())
}

pub(crate) fn ensure_targets_absent(targets: &[PathBuf]) -> Result<()> {
    for target in targets {
        match fs::symlink_metadata(target) {
            Ok(_) => bail!(
                "Refusing to replace existing artifact: {}",
                target.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("Failed to inspect target {}", target.display()));
            }
        }
    }
    Ok(())
}

/// Rename every staged artifact into place, rolling back what was already
/// published if one of them fails.
pub(crate) fn publish_artifacts(staged: Vec<StagedArtifact>) -> Result<()> {
    let mut published = Vec::<PathBuf>::new();
    for artifact in staged {
        let target = artifact.target;
        if let Err(error) = artifact.file.persist_noclobber(&target) {
            for path in published.iter().rev() {
                if let Err(rollback_error) = fs::remove_file(path) {
                    tracing::warn!(
                        "Failed to roll back artifact {}: {}",
                        path.display(),
                        rollback_error
                    );
                }
            }
            return Err(error.error)
                .with_context(|| format!("Failed to publish artifact {}", target.display()));
        }
        published.push(target);
    }
    Ok(())
}

pub(crate) fn format_bytes(bytes: u64) -> String {
    for (suffix, scale) in [
        ("GiB", 1u64 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("B", 1),
    ] {
        if bytes >= scale {
            return format!("{:.2} {suffix}", bytes as f64 / scale as f64);
        }
    }
    "0 B".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_iris_qualify() {
        assert_eq!(
            iri_hash(b"http://example.org/a"),
            Some(xxh64(b"http://example.org/a", 0))
        );
        assert_eq!(iri_hash(b"_:blank"), None);
        assert_eq!(iri_hash(b"\"literal\""), None);
    }

    #[test]
    fn spooled_keys_come_back_sorted_and_distinct() {
        let temp = tempfile::tempdir().unwrap();
        let mut spool = KeySpool::new(temp.path(), "subjects").unwrap();
        for hash in [40, 10, 40, 30, 10] {
            spool.push(hash).unwrap();
        }
        let mut keys = spool.finish().unwrap();
        assert_eq!(keys.key_count(), 5, "occurrences, not distinct values");
        assert_eq!(
            keys.read_sorted_distinct(DuplicateKeys::AreCollisions)
                .unwrap(),
            vec![10, 30, 40]
        );
    }

    /// A source replaced mid-build must fail the run, not publish an artifact
    /// whose keys and `source_digest` describe different bytes.
    #[test]
    fn a_replaced_source_is_caught_before_publication() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("data.hdt");

        // A real HDT is not needed: `capture` digests the dictionary-and-triples
        // suffix, and any well-formed HDT prefix would do — but the identity
        // marks are what this is about, so drive them directly.
        let marks = |bytes: &[u8]| {
            fs::write(&path, bytes).unwrap();
            FileMarks::read(&path).unwrap()
        };

        let original = marks(b"first");
        let identity = SourceIdentity {
            digest: [0x11; 32],
            marks: original.clone(),
        };
        identity.ensure_unchanged(&path).unwrap();

        // Replaced with different content: length differs.
        marks(b"second file");
        let error = identity.ensure_unchanged(&path).unwrap_err();
        assert!(
            error.to_string().contains("changed during the build"),
            "{error}"
        );

        // Replaced by rename with *identical* content and preserved timestamp,
        // the case `rsync -a` and `cp -p` produce. On Unix the inode catches it.
        fs::write(&path, b"first").unwrap();
        let replacement = temp.path().join("replacement");
        fs::write(&replacement, b"first").unwrap();
        let modified = fs::metadata(&path).unwrap().modified().unwrap();
        fs::File::open(&replacement)
            .unwrap()
            .set_modified(modified)
            .unwrap();
        fs::rename(&replacement, &path).unwrap();
        let restated = FileMarks::read(&path).unwrap();
        assert_eq!(restated.len, original.len);
        #[cfg(unix)]
        {
            assert_ne!(restated.inode, original.inode, "rename changes the inode");
            assert!(identity.ensure_unchanged(&path).is_err());
        }
    }

    #[test]
    fn byte_sizes_stay_readable_below_a_gibibyte() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(2_239), "2.19 KiB");
        assert_eq!(format_bytes(8 << 20), "8.00 MiB");
        assert_eq!(format_bytes(4 << 30), "4.00 GiB");
    }
}
