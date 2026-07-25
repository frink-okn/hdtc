//! Exact per-role key sets — the complete sorted set of distinct 64-bit term
//! keys for a dictionary role.
//!
//! The emitted `.keys` files are specified normatively in
//! `docs/keyset-format.md`. That document, not this module, is the authority on
//! the byte layouts, the Elias-Fano sizing rule, and the reader validation
//! rules; keep them in step. The conformance vectors it freezes in §8 are
//! pinned by the tests at the bottom of this file and in `tests/keyset_test.rs`.
//!
//! A key set is the exact parent of both sketch artifacts: the `.minhash` is
//! its bottom `k` values, and the `.filter` is built from it. It answers
//! membership without a false-positive rate and overlap without an estimator,
//! at roughly four times the filter's bytes.
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
    DuplicateKeys, KeyRunIter, KeySorter, SortedKeyRun, StagedArtifact, ensure_targets_absent,
    iri_hash, prepare_output_directory, publish_artifacts,
};
use super::input_adapter::HdtInputAdapter;
use super::reader::hdt_data_digest;
use crate::io::crc_utils::Crc32cWriter;
use anyhow::{Context, Result, ensure};
use std::fs::File;
use std::io::{BufWriter, Write};
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
    /// Every qualifying IRI in the dictionary, predicates included.
    ///
    /// Not part of the published role pair — see `docs/keyset-format.md` §1.1.
    /// It exists to measure what a whole-vocabulary key set costs and what it
    /// answers that the role split does not.
    Terms,
}

impl KeyRole {
    fn id(self) -> u8 {
        match self {
            Self::Subjects => 0,
            Self::Objects => 1,
            Self::Terms => 2,
        }
    }

    pub fn file_stem(self) -> &'static str {
        match self {
            Self::Subjects => "subjects",
            Self::Objects => "objects",
            Self::Terms => "terms",
        }
    }

    /// Whether the role draws from the subject-only dictionary section.
    fn takes_subject_section(self) -> bool {
        matches!(self, Self::Subjects | Self::Terms)
    }

    /// Whether the role draws from the object-only dictionary section.
    fn takes_object_section(self) -> bool {
        matches!(self, Self::Objects | Self::Terms)
    }

    /// Whether the role draws from the predicate dictionary section.
    fn takes_predicate_section(self) -> bool {
        matches!(self, Self::Terms)
    }

    /// What a duplicate key means for this role.
    ///
    /// Shared, Subjects, and Objects are mutually disjoint, so a duplicate in
    /// the published roles can only be a hash collision. `Terms` also reads the
    /// Predicates section, which is a separate ID space that routinely repeats
    /// IRIs already present as subjects or objects — 1129 of ubergraph's 1251
    /// predicates do — so duplicates there are ordinary, not collisions.
    fn duplicate_keys(self) -> DuplicateKeys {
        match self {
            Self::Subjects | Self::Objects => DuplicateKeys::AreCollisions,
            Self::Terms => DuplicateKeys::AreExpected,
        }
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
        .map(|&role| artifact_path(config.output_dir, role))
        .collect();
    ensure_targets_absent(&targets)?;

    let adapter = HdtInputAdapter::scan(config.hdt_path)?;
    let source_digest = hdt_data_digest(config.hdt_path)?;
    // Every selected role buffers concurrently during the one dictionary pass,
    // so the limit is shared out rather than granted to each.
    let role_budget = role_sort_budget(config.memory_limit, config.roles.len());
    let mut accumulators: Vec<RoleAccumulator> = config
        .roles
        .iter()
        .map(|&role| RoleAccumulator::new(role, config.temp_dir, role_budget))
        .collect();

    tracing::info!("Scanning HDT dictionary for qualifying IRIs");
    // The shared section belongs to every role, so it is read once and fanned
    // out rather than re-read per role.
    for term in adapter.shared_terms()? {
        if let Some(hash) = iri_hash(&term?) {
            for accumulator in &mut accumulators {
                accumulator.add_hash(hash)?;
            }
        }
    }
    scan_section(
        adapter.subject_terms()?,
        &mut accumulators,
        KeyRole::takes_subject_section,
    )?;
    scan_section(
        adapter.object_terms()?,
        &mut accumulators,
        KeyRole::takes_object_section,
    )?;
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
        let mut run = accumulator.keys.finish(role.duplicate_keys())?;
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
            &source_digest,
        )?;
        file.as_file().sync_all()?;
        staged.push(StagedArtifact {
            file,
            target: artifact_path(config.output_dir, role),
        });
        roles.push(KeysetRoleSummary {
            role,
            key_count: run.key_count(),
            scanned_iris,
            file_bytes,
        });
    }

    let files_written = staged.len();
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

/// The `.keys` path for one role. The single place artifact names are formed,
/// so the no-clobber precheck and publication cannot diverge.
fn artifact_path(output_dir: &Path, role: KeyRole) -> PathBuf {
    output_dir.join(format!("{}.keys", role.file_stem()))
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

/// Packs bits into little-endian `u64` words, least-significant bit first,
/// flushing each word as it fills.
struct BitPacker<W: Write> {
    writer: W,
    word: u64,
    /// Bits already placed in `word`; always in `0..64`.
    used: u32,
    words: u64,
}

impl<W: Write> BitPacker<W> {
    fn new(writer: W) -> Self {
        Self {
            writer,
            word: 0,
            used: 0,
            words: 0,
        }
    }

    /// Append the low `width` bits of `value`. `width` may be 0 or 64.
    fn push_bits(&mut self, value: u64, width: u32) -> Result<()> {
        debug_assert!(width <= 64);
        if width == 0 {
            return Ok(());
        }
        let masked = if width == 64 {
            value
        } else {
            value & ((1u64 << width) - 1)
        };
        let free = 64 - self.used; // 1..=64, since `used` is always < 64
        self.word |= masked << self.used;
        if width < free {
            self.used += width;
        } else {
            self.flush_word()?;
            let remaining = width - free;
            if remaining > 0 {
                // `free < width <= 64` here, so `free <= 63` and the shift is
                // in range.
                self.word = masked >> free;
                self.used = remaining;
            }
        }
        Ok(())
    }

    fn push_zeros(&mut self, mut count: u64) -> Result<()> {
        while count > 0 {
            let chunk = count.min(u64::from(64 - self.used)) as u32;
            self.push_bits(0, chunk)?;
            count -= u64::from(chunk);
        }
        Ok(())
    }

    fn flush_word(&mut self) -> Result<()> {
        self.writer.write_all(&self.word.to_le_bytes())?;
        self.word = 0;
        self.used = 0;
        self.words += 1;
        Ok(())
    }

    /// Flush any partial word, zero-padded, and report the words written.
    fn finish(mut self) -> Result<u64> {
        if self.used > 0 {
            self.flush_word()?;
        }
        Ok(self.words)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        sorter.finish(DuplicateKeys::AreCollisions).unwrap()
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
            KeyRole::Terms,
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
        assert_eq!(header[13], 2, "role = terms");
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

            let mut run = sorter.finish(DuplicateKeys::AreCollisions).unwrap();
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
    fn bit_packer_writes_whole_little_endian_words() {
        let mut buffer = Vec::new();
        let mut packer = BitPacker::new(&mut buffer);
        // 0b101 then 61 zero bits fills exactly one word.
        packer.push_bits(0b101, 3).unwrap();
        packer.push_zeros(61).unwrap();
        assert_eq!(packer.finish().unwrap(), 1);
        assert_eq!(buffer, 5u64.to_le_bytes());

        // A value straddling the word boundary splits across both words.
        let mut buffer = Vec::new();
        let mut packer = BitPacker::new(&mut buffer);
        packer.push_zeros(60).unwrap();
        packer.push_bits(0b1111_1111, 8).unwrap();
        assert_eq!(packer.finish().unwrap(), 2);
        assert_eq!(
            u64::from_le_bytes(buffer[0..8].try_into().unwrap()),
            0xF000_0000_0000_0000
        );
        assert_eq!(
            u64::from_le_bytes(buffer[8..16].try_into().unwrap()),
            0b1111
        );
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
