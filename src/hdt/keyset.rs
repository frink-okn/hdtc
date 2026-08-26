//! Exact per-role key sets — the complete sorted set of distinct 64-bit term
//! keys for a dictionary role.
//!
//! The emitted `.keys` files are specified normatively in
//! `docs/keyset-format.md`. That document, not this module, is the authority on
//! the byte layouts, the Elias-Fano sizing rule, and the reader validation
//! rules; keep them in step. The conformance vectors it freezes in §8 are
//! pinned by the tests at the bottom of this file and in `tests/keyset_test.rs`.
//!
//! For the composite subject/object roles, a key set is the exact parent of
//! both sketch artifacts: the `.minhash` is its bottom `k` values, and the
//! `.filter` is built from it. Every key-set role answers membership without a
//! false-positive rate and overlap without an estimator.
//!
//! The command shares `hdtc sketch`'s dictionary scan and term-to-key
//! convention (see [`crate::hdt::artifacts`]) — no new hashing and no new pass
//! over the triples.
//!
//! Nothing here holds the key set in memory. Keys are externally sorted and
//! deduplicated into a run on disk, and both encoders stream that run, so
//! `--memory-limit` bounds the sort buffers rather than the key count: a role
//! of any size builds at any limit.

use super::artifacts::{
    KeyRunIter, KeySorter, SortedKeyRun, SourceIdentity, StagedArtifact, ensure_targets_absent,
    iri_hash, prepare_output_directory, publish_artifacts,
};
use super::input_adapter::HdtInputAdapter;
use crate::io::BitPacker;
use crate::io::crc_utils::{CRC32C_ALGO, Crc32cWriter};
use anyhow::{Context, Result, ensure};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

const MAGIC: &[u8; 8] = b"KGFKEYS\0";
const FORMAT_VERSION: u16 = 1;
/// The convention of `docs/sketch-format.md` §3 — the same term-to-key rule the
/// sketches use, which is what makes the three artifacts comparable.
const CONVENTION_ID: u16 = 1;
const HASH_ID_XXH64: u8 = 1;
const HEADER_LEN: usize = 96;
/// Floor on one role's share of `--memory-limit`, so that a small limit still
/// makes progress rather than spilling a chunk per key.
const MIN_ROLE_SORT_BUDGET: usize = 1 << 20;

/// Parameters for one key-set build.
#[derive(Clone, Copy)]
pub struct KeysetConfig<'a> {
    pub hdt_path: &'a Path,
    pub output_dir: &'a Path,
    pub temp_dir: &'a Path,
    pub roles: &'a [KeyRole],
    pub encoding: KeysetEncoding,
    pub memory_limit: usize,
}

/// A dictionary role a key set can be built for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRole {
    /// Qualifying IRIs in `Shared ∪ Subjects`.
    Subjects,
    /// Qualifying IRIs in `Shared ∪ Objects`.
    Objects,
    /// Qualifying IRIs in the Predicates section.
    Predicates,
    /// Qualifying IRIs in the Shared section.
    Shared,
    /// Qualifying IRIs in the Subjects section.
    SubjectsOnly,
    /// Qualifying IRIs in the Objects section.
    ObjectsOnly,
}

impl KeyRole {
    pub(super) fn id(self) -> u8 {
        match self {
            Self::Subjects => 0,
            Self::Objects => 1,
            Self::Predicates => 2,
            Self::Shared => 3,
            Self::SubjectsOnly => 4,
            Self::ObjectsOnly => 5,
        }
    }

    fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::Subjects),
            1 => Some(Self::Objects),
            2 => Some(Self::Predicates),
            3 => Some(Self::Shared),
            4 => Some(Self::SubjectsOnly),
            5 => Some(Self::ObjectsOnly),
            _ => None,
        }
    }

    pub(super) fn is_sketch_role(self) -> bool {
        matches!(self, Self::Subjects | Self::Objects)
    }

    pub fn file_stem(self) -> &'static str {
        match self {
            Self::Subjects => "subjects",
            Self::Objects => "objects",
            Self::Predicates => "predicates",
            Self::Shared => "shared",
            Self::SubjectsOnly => "subjects-only",
            Self::ObjectsOnly => "objects-only",
        }
    }

    /// Whether the role draws from the shared dictionary section.
    fn takes_shared_section(self) -> bool {
        matches!(self, Self::Subjects | Self::Objects | Self::Shared)
    }

    /// Whether the role draws from the subject-only dictionary section.
    fn takes_subject_section(self) -> bool {
        matches!(self, Self::Subjects | Self::SubjectsOnly)
    }

    /// Whether the role draws from the object-only dictionary section.
    fn takes_object_section(self) -> bool {
        matches!(self, Self::Objects | Self::ObjectsOnly)
    }

    /// Whether the role draws from the predicate dictionary section.
    fn takes_predicate_section(self) -> bool {
        matches!(self, Self::Predicates)
    }
}

/// How a key set's payload is encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeysetEncoding {
    /// Raw sorted `u64` array — 8 bytes per key, `mmap` + binary search.
    Raw,
    /// Elias-Fano — 4.4–5.8 bytes per key, near the information floor.
    EliasFano,
}

impl KeysetEncoding {
    fn id(self) -> u8 {
        match self {
            Self::Raw => 0,
            Self::EliasFano => 1,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::EliasFano => "elias-fano",
        }
    }

    fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::Raw),
            1 => Some(Self::EliasFano),
            _ => None,
        }
    }
}

/// The validated 96-byte header of a `.keys` artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeysetHeader {
    pub format_version: u16,
    pub convention_id: u16,
    pub hash_id: u8,
    pub role: KeyRole,
    pub encoding: KeysetEncoding,
    pub low_width: u8,
    pub key_count: u64,
    pub min_key: u64,
    pub max_key: u64,
    pub payload_len: u64,
    pub source_digest: [u8; 32],
}

/// Why a key-set header could not be opened.
///
/// `NotKeyset` is reserved for a CRC-valid file with a foreign magic value, so
/// callers can distinguish an unrelated format from a malformed `.keys` file.
#[derive(Debug, thiserror::Error)]
pub enum KeysetOpenError {
    #[error("failed to read key-set artifact {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("{path:?} is not a key-set artifact (magic {magic:?})")]
    NotKeyset { path: PathBuf, magic: [u8; 8] },

    #[error("invalid key-set artifact {path:?}: {message}")]
    Invalid { path: PathBuf, message: String },
}

impl KeysetOpenError {
    fn io(path: &Path, source: io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }

    fn invalid(path: &Path, message: impl Into<String>) -> Self {
        Self::Invalid {
            path: path.to_path_buf(),
            message: message.into(),
        }
    }
}

/// Read and validate a `.keys` header.
///
/// The entire file is streamed through CRC32C before any header field is
/// interpreted. Payload bytes are not decoded or retained.
pub fn read_keyset_header(path: &Path) -> std::result::Result<KeysetHeader, KeysetOpenError> {
    let (bytes, file_len) = read_crc_checked_header(path)?;

    let magic: [u8; 8] = bytes[0..8].try_into().unwrap();
    if &magic != MAGIC {
        return Err(KeysetOpenError::NotKeyset {
            path: path.to_path_buf(),
            magic,
        });
    }

    let format_version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    if format_version != FORMAT_VERSION {
        return Err(KeysetOpenError::invalid(
            path,
            format!("unsupported format_version {format_version}"),
        ));
    }
    let convention_id = u16::from_le_bytes(bytes[10..12].try_into().unwrap());
    if convention_id != CONVENTION_ID {
        return Err(KeysetOpenError::invalid(
            path,
            format!("unsupported convention_id {convention_id}"),
        ));
    }
    let hash_id = bytes[12];
    if hash_id != HASH_ID_XXH64 {
        return Err(KeysetOpenError::invalid(
            path,
            format!("unsupported hash_id {hash_id}"),
        ));
    }
    let role = KeyRole::from_id(bytes[13])
        .ok_or_else(|| KeysetOpenError::invalid(path, format!("unsupported role {}", bytes[13])))?;
    let encoding = KeysetEncoding::from_id(bytes[14]).ok_or_else(|| {
        KeysetOpenError::invalid(path, format!("unsupported encoding {}", bytes[14]))
    })?;
    if bytes[80..96].iter().any(|&byte| byte != 0) {
        return Err(KeysetOpenError::invalid(
            path,
            "reserved header bytes are nonzero",
        ));
    }

    let low_width = bytes[15];
    let key_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let min_key = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    let max_key = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
    let payload_len = u64::from_le_bytes(bytes[40..48].try_into().unwrap());
    let source_digest = bytes[48..80].try_into().unwrap();

    let expected_file_len = HEADER_LEN as u128 + u128::from(payload_len) + 4;
    if u128::from(file_len) != expected_file_len {
        return Err(KeysetOpenError::invalid(
            path,
            format!("file length {file_len} does not match header declaration {expected_file_len}"),
        ));
    }

    let expected_payload_len = match encoding {
        KeysetEncoding::Raw => {
            if low_width != 0 {
                return Err(KeysetOpenError::invalid(
                    path,
                    format!("raw encoding requires low_width 0, found {low_width}"),
                ));
            }
            u128::from(key_count) * 8
        }
        KeysetEncoding::EliasFano if key_count == 0 => {
            if low_width != 0 {
                return Err(KeysetOpenError::invalid(
                    path,
                    format!("empty Elias-Fano encoding requires low_width 0, found {low_width}"),
                ));
            }
            0
        }
        KeysetEncoding::EliasFano => {
            let expected_low_width = (63 - key_count.ilog2()) as u8;
            if low_width != expected_low_width {
                return Err(KeysetOpenError::invalid(
                    path,
                    format!(
                        "Elias-Fano low_width {low_width} does not match key_count {key_count} (expected {expected_low_width})"
                    ),
                ));
            }
            let low_bits = u128::from(key_count) * u128::from(low_width);
            let high_bits = u128::from(key_count) + (1u128 << (64 - u32::from(low_width)));
            (low_bits.div_ceil(64) + high_bits.div_ceil(64)) * 8
        }
    };
    if u128::from(payload_len) != expected_payload_len {
        return Err(KeysetOpenError::invalid(
            path,
            format!(
                "payload_len {payload_len} does not match {encoding:?} sizing rule (expected {expected_payload_len})"
            ),
        ));
    }

    if key_count == 0 {
        if min_key != 0 || max_key != 0 {
            return Err(KeysetOpenError::invalid(
                path,
                "an empty key set requires min_key = max_key = 0",
            ));
        }
    } else if min_key > max_key {
        return Err(KeysetOpenError::invalid(
            path,
            format!("min_key {min_key} exceeds max_key {max_key}"),
        ));
    }

    Ok(KeysetHeader {
        format_version,
        convention_id,
        hash_id,
        role,
        encoding,
        low_width,
        key_count,
        min_key,
        max_key,
        payload_len,
        source_digest,
    })
}

/// Return the conventional `.keys` path inside a key-set output directory.
pub fn keyset_path(dir: &Path, role: KeyRole) -> PathBuf {
    dir.join(format!("{}.keys", role.file_stem()))
}

/// Stream a file through CRC32C and retain only its fixed header.
fn read_crc_checked_header(
    path: &Path,
) -> std::result::Result<([u8; HEADER_LEN], u64), KeysetOpenError> {
    let file = File::open(path).map_err(|source| KeysetOpenError::io(path, source))?;
    let file_len = file
        .metadata()
        .map_err(|source| KeysetOpenError::io(path, source))?
        .len();
    if file_len < 4 {
        return Err(KeysetOpenError::invalid(
            path,
            format!("file is too short to contain a CRC32C trailer ({file_len} bytes)"),
        ));
    }

    let data_len = file_len - 4;
    let mut reader = BufReader::new(file);
    let mut digest = CRC32C_ALGO.digest();
    let mut header = [0u8; HEADER_LEN];
    let mut header_filled = 0usize;
    let mut remaining = data_len;
    let mut buffer = [0u8; 64 * 1024];
    while remaining != 0 {
        let wanted = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        let read = reader
            .read(&mut buffer[..wanted])
            .map_err(|source| KeysetOpenError::io(path, source))?;
        if read == 0 {
            return Err(KeysetOpenError::io(
                path,
                io::Error::new(io::ErrorKind::UnexpectedEof, "file changed while reading"),
            ));
        }
        digest.update(&buffer[..read]);
        if header_filled < HEADER_LEN {
            let copied = read.min(HEADER_LEN - header_filled);
            header[header_filled..header_filled + copied].copy_from_slice(&buffer[..copied]);
            header_filled += copied;
        }
        remaining -= read as u64;
    }

    let mut trailer = [0u8; 4];
    reader
        .read_exact(&mut trailer)
        .map_err(|source| KeysetOpenError::io(path, source))?;
    let stored_crc = u32::from_le_bytes(trailer);
    let computed_crc = digest.finalize();
    if stored_crc != computed_crc {
        return Err(KeysetOpenError::invalid(
            path,
            format!("CRC32C mismatch: stored {stored_crc:#010x}, computed {computed_crc:#010x}"),
        ));
    }
    if header_filled < HEADER_LEN {
        return Err(KeysetOpenError::invalid(
            path,
            format!(
                "file is too short for the {HEADER_LEN}-byte header ({file_len} bytes including CRC32C)"
            ),
        ));
    }

    Ok((header, file_len))
}

/// What one role's key set cost to build and to store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeysetRoleSummary {
    pub role: KeyRole,
    /// Distinct keys written — the `key_count` header field.
    pub key_count: u64,
    /// Qualifying IRIs scanned, before collision deduplication.
    pub scanned_iris: u64,
    /// Published file size in bytes.
    pub file_bytes: u64,
}

impl KeysetRoleSummary {
    /// Published bytes per key, the number `docs/keyset-format.md` §5 models.
    pub fn bytes_per_key(&self) -> f64 {
        if self.key_count == 0 {
            0.0
        } else {
            self.file_bytes as f64 / self.key_count as f64
        }
    }
}

/// Counts reported after a successful key-set build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeysetSummary {
    pub files_written: usize,
    pub roles: Vec<KeysetRoleSummary>,
}

/// Generate the selected role key sets, publishing them only after every file
/// has been written successfully.
pub fn create_keysets(config: KeysetConfig<'_>) -> Result<KeysetSummary> {
    ensure!(
        !config.roles.is_empty(),
        "At least one key-set role must be selected"
    );

    prepare_output_directory(config.output_dir)?;
    let targets: Vec<PathBuf> = config
        .roles
        .iter()
        .map(|&role| keyset_path(config.output_dir, role))
        .collect();
    ensure_targets_absent(&targets)?;

    // Digest first, so every byte read afterwards is checked against this
    // snapshot by the `ensure_unchanged` below.
    let source = SourceIdentity::capture(config.hdt_path)?;
    let adapter = HdtInputAdapter::scan(config.hdt_path)?;
    // Every selected role buffers concurrently during the one dictionary pass,
    // so the limit is shared out rather than granted to each.
    let role_budget = role_sort_budget(config.memory_limit, config.roles.len());
    let mut accumulators: Vec<RoleAccumulator> = config
        .roles
        .iter()
        .map(|&role| RoleAccumulator::new(role, config.temp_dir, role_budget))
        .collect();

    tracing::info!("Scanning HDT dictionary for qualifying IRIs");
    // Each dictionary section is read once and fanned out to the selected
    // composite or section roles that draw from it.
    if config.roles.iter().any(|role| role.takes_shared_section()) {
        scan_section(
            adapter.shared_terms()?,
            &mut accumulators,
            KeyRole::takes_shared_section,
        )?;
    }
    if config.roles.iter().any(|role| role.takes_subject_section()) {
        scan_section(
            adapter.subject_terms()?,
            &mut accumulators,
            KeyRole::takes_subject_section,
        )?;
    }
    if config.roles.iter().any(|role| role.takes_object_section()) {
        scan_section(
            adapter.object_terms()?,
            &mut accumulators,
            KeyRole::takes_object_section,
        )?;
    }
    if config
        .roles
        .iter()
        .any(|role| role.takes_predicate_section())
    {
        scan_section(
            adapter.predicate_terms()?,
            &mut accumulators,
            KeyRole::takes_predicate_section,
        )?;
    }

    let mut staged = Vec::with_capacity(config.roles.len());
    let mut roles = Vec::with_capacity(config.roles.len());
    for accumulator in accumulators {
        let role = accumulator.role;
        let scanned_iris = accumulator.keys.scanned();
        tracing::info!("Merging {} keys", role.file_stem());
        // No role draws from overlapping dictionary sections, so a duplicate
        // within one role can only be a 64-bit hash collision.
        let mut run = accumulator.keys.finish()?;
        tracing::info!(
            "Building {} key set from {} distinct keys ({} qualifying IRIs)",
            role.file_stem(),
            run.key_count(),
            scanned_iris
        );

        let mut file = NamedTempFile::new_in(config.output_dir)?;
        let file_bytes = write_keyset_file(
            file.as_file_mut(),
            role,
            config.encoding,
            &mut run,
            source.digest(),
        )?;
        file.as_file().sync_all()?;
        staged.push(StagedArtifact {
            file,
            target: keyset_path(config.output_dir, role),
        });
        roles.push(KeysetRoleSummary {
            role,
            key_count: run.key_count(),
            scanned_iris,
            file_bytes,
        });
    }

    let files_written = staged.len();
    // Everything above read the source by path, several times over. Confirm it
    // is still the file that was digested before these bytes become public.
    source.ensure_unchanged(config.hdt_path)?;
    publish_artifacts(staged)?;
    Ok(KeysetSummary {
        files_written,
        roles,
    })
}

/// Fan one dictionary section out to the accumulators whose role draws from it.
fn scan_section<I>(
    terms: I,
    accumulators: &mut [RoleAccumulator],
    takes: fn(KeyRole) -> bool,
) -> Result<()>
where
    I: Iterator<Item = Result<Vec<u8>>>,
{
    let mut selected: Vec<&mut RoleAccumulator> = accumulators
        .iter_mut()
        .filter(|accumulator| takes(accumulator.role))
        .collect();
    if selected.is_empty() {
        return Ok(());
    }
    for term in terms {
        if let Some(hash) = iri_hash(&term?) {
            for accumulator in &mut selected {
                accumulator.add_hash(hash)?;
            }
        }
    }
    Ok(())
}

struct RoleAccumulator {
    role: KeyRole,
    keys: KeySorter,
}

impl RoleAccumulator {
    fn new(role: KeyRole, temp_dir: &Path, sort_budget: usize) -> Self {
        Self {
            role,
            keys: KeySorter::new(temp_dir, role.file_stem(), sort_budget),
        }
    }

    fn add_hash(&mut self, hash: u64) -> Result<()> {
        self.keys.push(hash)
    }
}

/// One role's share of `--memory-limit` for its sort buffer.
///
/// Halved because the buffer is a `Vec` that doubles its capacity: at 8 bytes
/// per key, resident bytes can reach twice the budget just before a flush, and
/// the limit should bound what is actually held.
fn role_sort_budget(memory_limit: usize, roles: usize) -> usize {
    (memory_limit / roles.max(1) / 2).max(MIN_ROLE_SORT_BUDGET)
}

// ---------------------------------------------------------------------------
// File writing
// ---------------------------------------------------------------------------

/// Write one role's key set, returning the file's total byte length.
///
/// The run is streamed, never materialized: Elias-Fano reads it twice, once for
/// the low-bits array and once for the high-bits vector.
fn write_keyset_file(
    file: &mut File,
    role: KeyRole,
    encoding: KeysetEncoding,
    run: &mut SortedKeyRun,
    source_digest: &[u8; 32],
) -> Result<u64> {
    let key_count = run.key_count();
    let low_width = match encoding {
        KeysetEncoding::Raw => 0,
        KeysetEncoding::EliasFano => elias_fano_low_width(key_count),
    };
    let payload_len = payload_len(encoding, key_count, low_width)?;
    // An empty role states its emptiness with key_count = 0; the range fields
    // carry no information then and are specified as zero.
    let (min_key, max_key) = run.range();

    let mut writer = Crc32cWriter::new(BufWriter::with_capacity(256 * 1024, file));
    writer.write_all(&header(
        role,
        encoding,
        low_width,
        key_count,
        min_key,
        max_key,
        payload_len,
        source_digest,
    ))?;

    let written = write_payload(&mut writer, encoding, key_count, low_width, run)?;
    ensure!(
        written == payload_len,
        "Key-set payload is {written} bytes but the header declares {payload_len}"
    );
    writer.finalize_and_write()?.flush()?;

    Ok(HEADER_LEN as u64 + payload_len + 4)
}

/// A re-readable source of one role's keys.
///
/// Elias-Fano writes its low-bits array before its high-bits vector and each
/// needs every key, so the encoder asks for a fresh ascending stream per pass
/// rather than taking one iterator — which is what keeps a multi-gigabyte key
/// set off the heap.
trait KeySource {
    type Keys<'a>: Iterator<Item = Result<u64>>
    where
        Self: 'a;

    fn keys(&mut self) -> Result<Self::Keys<'_>>;
}

impl KeySource for SortedKeyRun {
    type Keys<'a> = KeyRunIter<'a>;

    fn keys(&mut self) -> Result<Self::Keys<'_>> {
        SortedKeyRun::keys(self)
    }
}

/// Write the payload for `encoding`, returning its byte length.
fn write_payload<W: Write, S: KeySource>(
    writer: &mut W,
    encoding: KeysetEncoding,
    key_count: u64,
    low_width: u8,
    source: &mut S,
) -> Result<u64> {
    match encoding {
        KeysetEncoding::Raw => write_raw_payload(writer, source, key_count),
        KeysetEncoding::EliasFano => write_elias_fano_payload(writer, source, key_count, low_width),
    }
}

/// Streams a key run while enforcing the invariants the payload depends on:
/// strictly ascending (therefore distinct, §1.3) and exactly `expected` keys.
///
/// The external sort already guarantees both. Checking here means a regression
/// upstream fails the build instead of silently publishing an artifact that
/// every conforming reader would reject — or worse, would accept and binary
/// search incorrectly.
struct CheckedKeys<I> {
    inner: I,
    previous: Option<u64>,
    seen: u64,
    expected: u64,
}

impl<I: Iterator<Item = Result<u64>>> CheckedKeys<I> {
    fn new(inner: I, expected: u64) -> Self {
        Self {
            inner,
            previous: None,
            seen: 0,
            expected,
        }
    }

    fn next_key(&mut self) -> Result<Option<u64>> {
        let Some(key) = self.inner.next().transpose()? else {
            ensure!(
                self.seen == self.expected,
                "Key run ended after {} keys but {} were declared",
                self.seen,
                self.expected
            );
            return Ok(None);
        };
        ensure!(
            self.previous.is_none_or(|previous| previous < key),
            "Key run is not strictly ascending at key {}",
            self.seen
        );
        ensure!(
            self.seen < self.expected,
            "Key run holds more than the {} keys declared",
            self.expected
        );
        self.previous = Some(key);
        self.seen += 1;
        Ok(Some(key))
    }
}

#[allow(clippy::too_many_arguments)]
fn header(
    role: KeyRole,
    encoding: KeysetEncoding,
    low_width: u8,
    key_count: u64,
    min_key: u64,
    max_key: u64,
    payload_len: u64,
    source_digest: &[u8; 32],
) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[0..8].copy_from_slice(MAGIC);
    header[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    header[10..12].copy_from_slice(&CONVENTION_ID.to_le_bytes());
    header[12] = HASH_ID_XXH64;
    header[13] = role.id();
    header[14] = encoding.id();
    header[15] = low_width;
    header[16..24].copy_from_slice(&key_count.to_le_bytes());
    header[24..32].copy_from_slice(&min_key.to_le_bytes());
    header[32..40].copy_from_slice(&max_key.to_le_bytes());
    header[40..48].copy_from_slice(&payload_len.to_le_bytes());
    header[48..80].copy_from_slice(source_digest);
    // header[80..96] is reserved and stays zero.
    header
}

fn write_raw_payload<W: Write, S: KeySource>(
    writer: &mut W,
    source: &mut S,
    key_count: u64,
) -> Result<u64> {
    let mut keys = CheckedKeys::new(source.keys()?, key_count);
    let mut written = 0u64;
    while let Some(key) = keys.next_key()? {
        writer.write_all(&key.to_le_bytes())?;
        written += 8;
    }
    Ok(written)
}

// ---------------------------------------------------------------------------
// Elias-Fano
// ---------------------------------------------------------------------------

/// The Elias-Fano low-part width for `n` distinct keys over the 64-bit
/// universe (`docs/keyset-format.md` §4.2).
///
/// `l = 63 - floor(log2(n))`, the standard `floor(log2(u/n))` specialised to
/// `u = 2^64`. It puts the high-bits vector at `n + 2^(64-l)` bits, which stays
/// in `[2n, 3n)` for every `n ≥ 1` — so the whole payload is bounded, and the
/// per-key rate falls as the role grows.
fn elias_fano_low_width(n: u64) -> u8 {
    if n == 0 { 0 } else { (63 - n.ilog2()) as u8 }
}

/// Length of the high-bits vector in bits: `n + 2^(64 - low_width)`.
///
/// Computed in `u128` because `2^(64 - low_width)` reaches the top of the `u64`
/// range for the largest key counts.
fn elias_fano_high_bits(n: u64, low_width: u8) -> u128 {
    u128::from(n) + (1u128 << (64 - u32::from(low_width)))
}

/// Payload length in bytes for the given encoding, in exact arithmetic.
fn payload_len(encoding: KeysetEncoding, n: u64, low_width: u8) -> Result<u64> {
    let bytes: u128 = match encoding {
        KeysetEncoding::Raw => u128::from(n) * 8,
        KeysetEncoding::EliasFano => {
            if n == 0 {
                0
            } else {
                let low_words = (u128::from(n) * u128::from(low_width)).div_ceil(64);
                let high_words = elias_fano_high_bits(n, low_width).div_ceil(64);
                (low_words + high_words) * 8
            }
        }
    };
    u64::try_from(bytes).context("Key-set payload length exceeds u64")
}

/// Write the Elias-Fano payload: the packed low bits, then the high-bits
/// vector.
fn write_elias_fano_payload<W: Write, S: KeySource>(
    writer: &mut W,
    source: &mut S,
    key_count: u64,
    low_width: u8,
) -> Result<u64> {
    if key_count == 0 {
        return Ok(0);
    }
    let shift = u32::from(low_width);

    // Pass one: the packed low bits.
    let mut packer = BitPacker::new(&mut *writer);
    let mut keys = CheckedKeys::new(source.keys()?, key_count);
    while let Some(key) = keys.next_key()? {
        packer.push_bits(key, shift)?;
    }
    drop(keys);
    let low_words = packer.finish()?;

    // Pass two: the high-bits vector. Position `(key >> l) + index` is strictly
    // increasing for ascending keys, so it is written forward with no seeking
    // back. The checked subtraction backs up `CheckedKeys`: on unsorted input
    // it would otherwise wrap in a release build and emit ~2^64 zero bits.
    let mut packer = BitPacker::new(&mut *writer);
    let mut position: u64 = 0;
    let mut index = 0u64;
    let mut keys = CheckedKeys::new(source.keys()?, key_count);
    while let Some(key) = keys.next_key()? {
        let target = (key >> shift) + index;
        let gap = target
            .checked_sub(position)
            .context("Elias-Fano input keys are not sorted ascending")?;
        packer.push_zeros(gap)?;
        packer.push_bits(1, 1)?;
        position = target + 1;
        index += 1;
    }
    drop(keys);
    let high_bits = elias_fano_high_bits(key_count, low_width);
    packer.push_zeros(
        u64::try_from(high_bits - u128::from(position))
            .context("Elias-Fano high-bits vector exceeds u64")?,
    )?;
    let high_words = packer.finish()?;

    Ok((low_words + high_words) * 8)
}

#[cfg(test)]
mod tests {
    use super::*;

    type HeaderMutation = (&'static str, fn(&mut Vec<u8>));

    fn with_crc(mut body: Vec<u8>) -> Vec<u8> {
        body.extend_from_slice(&crate::io::crc_utils::crc32c(&body).to_le_bytes());
        body
    }

    fn write_reader_fixture(temp: &Path, name: &str, body: Vec<u8>) -> PathBuf {
        let path = temp.join(name);
        std::fs::write(&path, with_crc(body)).unwrap();
        path
    }

    fn raw_reader_body() -> Vec<u8> {
        let mut body = header(
            KeyRole::Subjects,
            KeysetEncoding::Raw,
            0,
            2,
            1,
            2,
            16,
            &[0x5a; 32],
        )
        .to_vec();
        body.extend_from_slice(&1u64.to_le_bytes());
        body.extend_from_slice(&2u64.to_le_bytes());
        body
    }

    fn expect_invalid_reader_body(temp: &Path, name: &str, body: Vec<u8>) {
        let path = write_reader_fixture(temp, name, body);
        assert!(
            matches!(
                read_keyset_header(&path),
                Err(KeysetOpenError::Invalid { .. })
            ),
            "{name} should be rejected as an invalid key set"
        );
    }

    #[test]
    fn header_reader_parses_the_frozen_raw_vector() {
        let temp = tempfile::tempdir().unwrap();
        let keys = [
            0x00cc_3131_e8f7_a0c5,
            0x0da9_8875_b72f_df91,
            0x35c5_f517_a376_fed8,
            0x45c6_4ad7_8fde_51e4,
            0xaf5a_5827_fae0_76d7,
        ];
        let mut body = header(
            KeyRole::Subjects,
            KeysetEncoding::Raw,
            0,
            5,
            keys[0],
            keys[4],
            40,
            &[0x5a; 32],
        )
        .to_vec();
        for key in keys {
            body.extend_from_slice(&key.to_le_bytes());
        }
        let path = write_reader_fixture(temp.path(), "subjects.keys", body);

        let parsed = read_keyset_header(&path).unwrap();
        assert_eq!(parsed.format_version, 1);
        assert_eq!(parsed.convention_id, 1);
        assert_eq!(parsed.hash_id, 1);
        assert_eq!(parsed.role, KeyRole::Subjects);
        assert_eq!(parsed.encoding, KeysetEncoding::Raw);
        assert_eq!(parsed.low_width, 0);
        assert_eq!(parsed.key_count, 5);
        assert_eq!(parsed.min_key, keys[0]);
        assert_eq!(parsed.max_key, keys[4]);
        assert_eq!(parsed.payload_len, 40);
        assert_eq!(parsed.source_digest, [0x5a; 32]);
        assert_eq!(keyset_path(temp.path(), KeyRole::Subjects), path);
    }

    #[test]
    fn header_reader_enforces_every_keyset_reader_rule() {
        let temp = tempfile::tempdir().unwrap();

        let mut bad_crc = with_crc(raw_reader_body());
        *bad_crc.last_mut().unwrap() ^= 1;
        let bad_crc_path = temp.path().join("bad-crc.keys");
        std::fs::write(&bad_crc_path, bad_crc).unwrap();
        let error = read_keyset_header(&bad_crc_path).unwrap_err();
        assert!(error.to_string().contains("CRC32C mismatch"));

        let mut foreign = raw_reader_body();
        foreign[0..8].copy_from_slice(b"NOTKEYS\0");
        let foreign_path = write_reader_fixture(temp.path(), "foreign.keys", foreign);
        assert!(matches!(
            read_keyset_header(&foreign_path),
            Err(KeysetOpenError::NotKeyset { .. })
        ));

        let mutations: Vec<HeaderMutation> = vec![
            ("version", |body| body[8] = 2),
            ("convention", |body| body[10] = 2),
            ("hash", |body| body[12] = 2),
            ("role", |body| body[13] = 6),
            ("encoding", |body| body[14] = 2),
            ("reserved", |body| body[80] = 1),
            ("raw-low-width", |body| body[15] = 1),
            ("raw-payload-size", |body| {
                body[16..24].copy_from_slice(&1u64.to_le_bytes())
            }),
            ("range-order", |body| {
                body[24..32].copy_from_slice(&3u64.to_le_bytes())
            }),
            ("file-size", |body| body.push(0)),
        ];
        for (name, mutate) in mutations {
            let mut body = raw_reader_body();
            mutate(&mut body);
            expect_invalid_reader_body(temp.path(), name, body);
        }

        let empty = || {
            header(
                KeyRole::Shared,
                KeysetEncoding::EliasFano,
                0,
                0,
                0,
                0,
                0,
                &[0; 32],
            )
            .to_vec()
        };
        let empty_path = write_reader_fixture(temp.path(), "empty.keys", empty());
        assert_eq!(read_keyset_header(&empty_path).unwrap().key_count, 0);

        let mut bad_empty_width = empty();
        bad_empty_width[15] = 1;
        expect_invalid_reader_body(temp.path(), "empty-width", bad_empty_width);
        let mut bad_empty_range = empty();
        bad_empty_range[24] = 1;
        expect_invalid_reader_body(temp.path(), "empty-range", bad_empty_range);

        let mut elias = header(
            KeyRole::Objects,
            KeysetEncoding::EliasFano,
            61,
            5,
            1,
            2,
            48,
            &[0; 32],
        )
        .to_vec();
        elias.resize(HEADER_LEN + 48, 0);
        let valid_elias = read_keyset_header(&write_reader_fixture(
            temp.path(),
            "valid-elias.keys",
            elias.clone(),
        ))
        .unwrap();
        assert_eq!(valid_elias.encoding, KeysetEncoding::EliasFano);
        assert_eq!(valid_elias.low_width, 61);
        assert_eq!(valid_elias.key_count, 5);
        assert_eq!(valid_elias.payload_len, 48);
        let mut bad_elias_width = elias.clone();
        bad_elias_width[15] = 60;
        expect_invalid_reader_body(temp.path(), "elias-width", bad_elias_width);
        let mut bad_elias_size = elias;
        bad_elias_size[16..24].copy_from_slice(&4u64.to_le_bytes());
        expect_invalid_reader_body(temp.path(), "elias-payload-size", bad_elias_size);

        let mut advisory_digest = raw_reader_body();
        advisory_digest[48..80].fill(0xff);
        let path = write_reader_fixture(temp.path(), "advisory-digest.keys", advisory_digest);
        assert_eq!(read_keyset_header(&path).unwrap().source_digest, [0xff; 32]);
    }

    /// Decode an Elias-Fano payload back to its key list, from the
    /// specification text rather than from the encoder's internals. If the two
    /// drift apart, an independent implementation reading the document will
    /// disagree with what we emit.
    fn decode_elias_fano(payload: &[u8], n: u64, low_width: u8) -> Vec<u64> {
        if n == 0 {
            return Vec::new();
        }
        let l = u32::from(low_width);
        let low_words = (u128::from(n) * u128::from(low_width)).div_ceil(64) as usize;
        let word = |bytes: &[u8], index: usize| -> u64 {
            u64::from_le_bytes(bytes[index * 8..index * 8 + 8].try_into().unwrap())
        };
        let bit = |bytes: &[u8], base: usize, position: u64| -> bool {
            let index = base + (position / 64) as usize;
            word(bytes, index) >> (position % 64) & 1 == 1
        };

        // Low bits: field i occupies bits [i*l, (i+1)*l) of the low stream.
        let low = |index: u64| -> u64 {
            if l == 0 {
                return 0;
            }
            let start = index * u64::from(l);
            let (first, offset) = ((start / 64) as usize, start % 64);
            let mut value = word(payload, first) >> offset;
            if offset + u64::from(l) > 64 {
                value |= word(payload, first + 1) << (64 - offset);
            }
            if l == 64 {
                value
            } else {
                value & ((1u64 << l) - 1)
            }
        };

        // High bits: the i-th set bit sits at position (key_i >> l) + i.
        let high_bits = elias_fano_high_bits(n, low_width);
        let mut keys = Vec::with_capacity(n as usize);
        let mut found = 0u64;
        for position in 0..high_bits as u64 {
            if bit(payload, low_words, position) {
                let high = position - found;
                keys.push((high << l) | low(found));
                found += 1;
                if found == n {
                    break;
                }
            }
        }
        assert_eq!(found, n, "high-bits vector holds n set bits");
        keys
    }

    /// A key source straight from a slice, with none of the external sort's
    /// guarantees, so the encoder's own invariant checks can be exercised.
    struct SliceKeys<'k>(&'k [u64]);

    impl KeySource for SliceKeys<'_> {
        type Keys<'a>
            = std::iter::Map<std::iter::Copied<std::slice::Iter<'a, u64>>, fn(u64) -> Result<u64>>
        where
            Self: 'a;

        fn keys(&mut self) -> Result<Self::Keys<'_>> {
            Ok(self.0.iter().copied().map(Ok as fn(u64) -> Result<u64>))
        }
    }

    /// Encode an in-memory key set through the same streaming path the command
    /// uses.
    fn encode(keys: &[u64], encoding: KeysetEncoding) -> (Vec<u8>, u8) {
        let key_count = keys.len() as u64;
        let low_width = match encoding {
            KeysetEncoding::Raw => 0,
            KeysetEncoding::EliasFano => elias_fano_low_width(key_count),
        };
        let mut payload = Vec::new();
        let written = write_payload(
            &mut payload,
            encoding,
            key_count,
            low_width,
            &mut SliceKeys(keys),
        )
        .unwrap();
        assert_eq!(written, payload.len() as u64);
        assert_eq!(
            written,
            payload_len(encoding, key_count, low_width).unwrap(),
            "declared payload length must match what was written"
        );
        (payload, low_width)
    }

    /// A `SortedKeyRun` over the given keys, for the file-level tests.
    fn run_of(keys: &[u64]) -> SortedKeyRun {
        let temp = tempfile::tempdir().unwrap();
        let mut sorter = KeySorter::new(temp.path(), "subjects", 1 << 20);
        for &key in keys {
            sorter.push(key).unwrap();
        }
        sorter.finish().unwrap()
    }

    /// The §4.2 sizing rule, and the bytes-per-key model of §5 that follows
    /// from it. The table values are the ones doc 18 §18.3 publishes.
    #[test]
    fn elias_fano_sizing_matches_the_published_model() {
        assert_eq!(elias_fano_low_width(0), 0);
        assert_eq!(elias_fano_low_width(1), 63);
        assert_eq!(elias_fano_low_width(2), 62);
        assert_eq!(elias_fano_low_width(1 << 20), 43);

        // doc 18 §18.3's table, which quotes the model to two decimals; the
        // tolerance absorbs its rounding, not ours.
        for (n, expected) in [
            (1_000_000u64, 5.76),
            (10_000_000, 5.34),
            (100_000_000, 4.93),
            (2_300_000_000, 4.36),
        ] {
            let low_width = elias_fano_low_width(n);
            let bytes = payload_len(KeysetEncoding::EliasFano, n, low_width).unwrap();
            let per_key = bytes as f64 / n as f64;
            assert!(
                (per_key - expected).abs() < 0.02,
                "n = {n}: {per_key:.3} bytes/key, expected about {expected}"
            );
            // The high-bits vector never leaves [2n, 3n) bits, which is what
            // bounds the payload for every key count.
            let high = elias_fano_high_bits(n, low_width);
            assert!((2 * u128::from(n)..3 * u128::from(n)).contains(&high));
        }
    }

    #[test]
    fn elias_fano_round_trips_every_key() {
        let keys: Vec<u64> = {
            let mut keys: Vec<u64> = (0..500)
                .map(|index| {
                    xxhash_rust::xxh64::xxh64(
                        format!("https://example.org/resource/{index:03}").as_bytes(),
                        0,
                    )
                })
                .collect();
            keys.sort_unstable();
            keys.dedup();
            keys
        };
        let (payload, low_width) = encode(&keys, KeysetEncoding::EliasFano);
        assert_eq!(
            decode_elias_fano(&payload, keys.len() as u64, low_width),
            keys
        );
    }

    /// The extremes the sizing rule has to survive: a single key, keys at both
    /// ends of the universe, and a dense run that stresses the zero-gap path.
    #[test]
    fn elias_fano_handles_boundary_key_sets() {
        for keys in [
            vec![],
            vec![0u64],
            vec![u64::MAX],
            vec![0, u64::MAX],
            vec![0, 1, 2, 3],
            (0..64u64).collect::<Vec<_>>(),
            vec![0, 1, u64::MAX - 1, u64::MAX],
            (0..37u64).map(|i| i << 58).collect::<Vec<_>>(),
        ] {
            let (payload, low_width) = encode(&keys, KeysetEncoding::EliasFano);
            assert_eq!(
                decode_elias_fano(&payload, keys.len() as u64, low_width),
                keys,
                "round trip for {keys:?}"
            );
        }
    }

    /// A key run that is not a strictly ascending set is a bug upstream, but it
    /// must fail loudly rather than publish a malformed artifact — or, in the
    /// Elias-Fano case, wrap into a ~2^64-bit zero run in a release build.
    #[test]
    fn a_malformed_key_run_is_refused() {
        let cases: [(&str, Vec<u64>, u64); 4] = [
            ("descending", vec![1 << 62, 1], 2),
            ("duplicate", vec![7, 7], 2),
            ("too few keys", vec![1, 2], 3),
            ("too many keys", vec![1, 2, 3], 2),
        ];
        for encoding in [KeysetEncoding::Raw, KeysetEncoding::EliasFano] {
            for (label, keys, declared) in &cases {
                let low_width = match encoding {
                    KeysetEncoding::Raw => 0,
                    KeysetEncoding::EliasFano => elias_fano_low_width(*declared),
                };
                let error = write_payload(
                    &mut Vec::new(),
                    encoding,
                    *declared,
                    low_width,
                    &mut SliceKeys(keys),
                )
                .unwrap_err();
                let message = error.to_string();
                assert!(
                    message.contains("ascending") || message.contains("declared"),
                    "{label} at {encoding:?}: {message}"
                );
            }
        }
    }

    #[test]
    fn raw_payload_is_a_sorted_little_endian_array() {
        let keys = vec![1u64, 0x0102_0304_0506_0708, u64::MAX];
        let (payload, low_width) = encode(&keys, KeysetEncoding::Raw);
        assert_eq!(low_width, 0);
        assert_eq!(payload.len(), 24);
        assert_eq!(&payload[8..16], &[8, 7, 6, 5, 4, 3, 2, 1]);
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(
                u64::from_le_bytes(payload[index * 8..index * 8 + 8].try_into().unwrap()),
                *key
            );
        }
    }

    #[test]
    fn header_has_stable_layout() {
        let digest = [0x5a; 32];
        let header = header(
            KeyRole::Predicates,
            KeysetEncoding::EliasFano,
            43,
            42,
            7,
            9_999,
            256,
            &digest,
        );
        assert_eq!(&header[0..8], b"KGFKEYS\0");
        assert_eq!(u16::from_le_bytes(header[8..10].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(header[10..12].try_into().unwrap()), 1);
        assert_eq!(header[12], 1, "hash_id");
        assert_eq!(header[13], 2, "role = predicates");
        assert_eq!(header[14], 1, "encoding = elias-fano");
        assert_eq!(header[15], 43, "low_width");
        assert_eq!(u64::from_le_bytes(header[16..24].try_into().unwrap()), 42);
        assert_eq!(u64::from_le_bytes(header[24..32].try_into().unwrap()), 7);
        assert_eq!(
            u64::from_le_bytes(header[32..40].try_into().unwrap()),
            9_999
        );
        assert_eq!(u64::from_le_bytes(header[40..48].try_into().unwrap()), 256);
        assert_eq!(&header[48..80], &digest);
        assert_eq!(&header[80..96], &[0; 16], "reserved");
    }

    #[test]
    fn empty_role_writes_a_headers_only_file() {
        for encoding in [KeysetEncoding::Raw, KeysetEncoding::EliasFano] {
            let mut run = run_of(&[]);
            assert_eq!(run.key_count(), 0);
            assert_eq!(run.range(), (0, 0));

            let mut file = tempfile::tempfile().unwrap();
            let bytes =
                write_keyset_file(&mut file, KeyRole::Subjects, encoding, &mut run, &[0; 32])
                    .unwrap();
            assert_eq!(bytes, 100, "96-byte header plus the CRC trailer");
            assert_eq!(file.metadata().unwrap().len(), 100);
        }
    }

    /// The external sort must produce the same ascending, distinct run whether
    /// it fits one in-memory chunk or spills to many — the merge path changes
    /// with the chunk count, and the emitted bytes must not.
    #[test]
    fn spilled_and_resident_sorts_produce_the_same_run() {
        let keys: Vec<u64> = (0..2_000u64)
            .map(|index| xxhash_rust::xxh64::xxh64(&index.to_le_bytes(), 0))
            .collect();
        let mut expected = keys.clone();
        expected.sort_unstable();
        expected.dedup();

        let temp = tempfile::tempdir().unwrap();
        // A 1 KiB budget spills roughly every 128 keys, exercising the parallel
        // merge tree; the whole set fits the 1 MiB budget in one chunk.
        for budget in [1 << 10, 1 << 20] {
            let mut sorter = KeySorter::new(temp.path(), "subjects", budget);
            // Push everything twice: deduplication is the merge's job.
            for key in keys.iter().chain(keys.iter()) {
                sorter.push(*key).unwrap();
            }
            assert_eq!(sorter.scanned(), 4_000);

            let mut run = sorter.finish().unwrap();
            assert_eq!(run.key_count(), expected.len() as u64, "budget {budget}");
            assert_eq!(
                run.range(),
                (expected[0], *expected.last().unwrap()),
                "budget {budget}"
            );
            let read: Vec<u64> = run.keys().unwrap().collect::<Result<_>>().unwrap();
            assert_eq!(read, expected, "budget {budget}");
        }
    }

    #[test]
    fn memory_budget_bounds_the_resident_key_array() {
        // The budget is shared out across the selected roles and halved, and
        // never falls below the floor that keeps the sort making progress.
        assert_eq!(role_sort_budget(4 << 30, 1), (4 << 30) / 2);
        assert_eq!(role_sort_budget(4 << 30, 3), (4 << 30) / 3 / 2);
        assert_eq!(role_sort_budget(1 << 20, 8), MIN_ROLE_SORT_BUDGET);
        assert_eq!(role_sort_budget(0, 0), MIN_ROLE_SORT_BUDGET, "never zero");
    }
}
