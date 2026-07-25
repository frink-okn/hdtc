//! Role-specific membership filters and bottom-k overlap sketches.
//!
//! The emitted `.filter` and `.minhash` files are specified normatively in
//! `docs/sketch-format.md`. That document, not this module, is the authority on
//! the byte layouts, the term-to-key derivation, and the membership probe; keep
//! them in step. The conformance vectors it freezes in §9 are pinned by the
//! tests at the bottom of this file.
//!
//! The command scans the shared, subject-only, and object-only PFC dictionary
//! sections once. Qualifying IRI hashes are spooled to temporary files so the
//! roles do not need to be resident together; binary fuse construction then
//! reads one role's keys back at a time.

use super::artifacts::{
    DuplicateKeys, KeySpool, SourceIdentity, SpooledKeys, StagedArtifact, ensure_targets_absent,
    format_bytes, iri_hash, prepare_output_directory, publish_artifacts,
};
use super::input_adapter::HdtInputAdapter;
use super::pfc_reader::PfcSectionIterator;
use crate::io::crc_utils::Crc32cWriter;
use anyhow::{Context, Result, ensure};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use xorf::{BinaryFuse8, BinaryFuse16, DmaSerializable};

const FORMAT_VERSION: u16 = 1;
const CONVENTION_ID: u16 = 1;
const HASH_ID_XXH64: u8 = 1;
const COMMON_HEADER_LEN: usize = 56;
const FILTER_DESCRIPTOR_LEN: usize = 20;
// Conservative BTreeSet cost per retained bottom-k value while scanning.
const MINHASH_BYTES_PER_ENTRY_ESTIMATE: u64 = 32;
/// Largest key count `xorf`'s binary fuse construction can represent.
///
/// Not `u32::MAX * 8 / 9`: construction rounds the ~9/8 capacity *up* to a whole
/// 262,144-entry segment and holds the result in a `u32`. The largest usable
/// array length is therefore `16_383 * 262_144 = 4_294_705_152`, and this is the
/// largest key count whose rounded capacity stays at or below it. One key more
/// rounds to 2^32 and overflows xorf's `u32` multiply — silently in release
/// builds, where Cargo leaves overflow checks off. See
/// `binary_fuse_ceiling_keeps_capacity_in_u32`.
const MAX_BINARY_FUSE_KEYS: u64 = 3_817_515_691;

/// Parameters for one sketch build.
#[derive(Clone, Copy)]
pub struct SketchConfig<'a> {
    pub hdt_path: &'a Path,
    pub output_dir: &'a Path,
    pub temp_dir: &'a Path,
    pub roles: &'a [Role],
    pub k: u32,
    pub filter_bits: u8,
    pub memory_limit: usize,
}

/// Counts reported after a successful sketch build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SketchSummary {
    pub files_written: usize,
    /// Qualifying IRI count per selected role, in the order they were built.
    pub role_counts: Vec<(Role, u64)>,
}

/// A dictionary role a sketch can be built for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Subjects,
    Objects,
}

impl Role {
    fn id(self) -> u8 {
        match self {
            Self::Subjects => 0,
            Self::Objects => 1,
        }
    }

    pub fn file_stem(self) -> &'static str {
        match self {
            Self::Subjects => "subjects",
            Self::Objects => "objects",
        }
    }

    /// The role's own dictionary section, excluding the shared section that
    /// every role draws from.
    fn private_terms(
        self,
        adapter: &HdtInputAdapter,
    ) -> Result<PfcSectionIterator<BufReader<File>>> {
        match self {
            Self::Subjects => adapter.subject_terms(),
            Self::Objects => adapter.object_terms(),
        }
    }

    fn private_count(self, adapter: &HdtInputAdapter) -> u64 {
        match self {
            Self::Subjects => adapter.subjects_count,
            Self::Objects => adapter.objects_count,
        }
    }
}

struct RoleAccumulator {
    role: Role,
    keys: KeySpool,
    /// Largest key count whose filter build still fits `memory_limit`.
    max_keys: u64,
    memory_limit: usize,
    k: usize,
    minima: BTreeSet<u64>,
}

impl RoleAccumulator {
    fn new(role: Role, config: SketchConfig<'_>, max_keys: u64, k: usize) -> Result<Self> {
        Ok(Self {
            role,
            keys: KeySpool::new(config.temp_dir, role.file_stem())?,
            max_keys,
            memory_limit: config.memory_limit,
            k,
            minima: BTreeSet::new(),
        })
    }

    fn add_hash(&mut self, hash: u64) -> Result<()> {
        self.keys.push(hash)?;
        // Checked per key rather than once at the end: the budget then fails as
        // soon as the input is known to be too large, instead of after a full
        // dictionary scan, and it never rejects a role on an over-estimate.
        ensure!(
            self.keys.key_count() <= self.max_keys,
            "{} role has more than {} qualifying IRIs, the most a binary fuse filter can be built from within --memory-limit ({}); increase the limit or reduce --k",
            self.role.file_stem(),
            self.max_keys,
            format_bytes(self.memory_limit as u64)
        );

        if self.minima.len() < self.k {
            self.minima.insert(hash);
        } else if self.minima.last().is_some_and(|largest| hash < *largest) {
            // A duplicate leaves the length unchanged, so only evict on growth.
            if self.minima.insert(hash) {
                self.minima.pop_last();
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<RoleData> {
        let key_count = self.keys.key_count();
        let keys = self.keys.finish()?;

        // BTreeSet iterates in ascending order.
        let minima: Vec<u64> = self.minima.into_iter().collect();
        let expected_minima = key_count.min(self.k as u64) as usize;
        if minima.len() != expected_minima {
            tracing::warn!(
                "{} XXH64 collision(s) among the smallest {} hashes; overlap estimates from this \
                 sketch treat the colliding IRIs as one",
                expected_minima - minima.len(),
                self.role.file_stem()
            );
        }
        Ok(RoleData {
            role: self.role,
            keys,
            minima,
        })
    }
}

struct RoleData {
    role: Role,
    keys: SpooledKeys,
    minima: Vec<u64>,
}

impl RoleData {
    /// Distinct qualifying IRIs in the role — the `key_count` header field.
    fn key_count(&self) -> u64 {
        self.keys.key_count()
    }
}

/// Generate the selected role artifacts and publish them only after every file
/// has been constructed successfully.
pub fn create_sketches(config: SketchConfig<'_>) -> Result<SketchSummary> {
    ensure!(config.k >= 2, "MinHash k must be at least 2");
    ensure!(
        matches!(config.filter_bits, 8 | 16),
        "Filter width must be 8 or 16 bits"
    );
    ensure!(
        !config.roles.is_empty(),
        "At least one sketch role must be selected"
    );

    prepare_output_directory(config.output_dir)?;
    let mut targets = Vec::with_capacity(config.roles.len() * 2);
    for &role in config.roles {
        let (filter, minhash) = artifact_paths(config.output_dir, role);
        targets.push(filter);
        targets.push(minhash);
    }
    ensure_targets_absent(&targets)?;

    let adapter = HdtInputAdapter::scan(config.hdt_path)?;
    let dictionary_counts: Vec<u64> = config
        .roles
        .iter()
        .map(|role| {
            adapter
                .shared_count
                .saturating_add(role.private_count(&adapter))
        })
        .collect();
    ensure_minhash_accumulator_memory(&dictionary_counts, config.k, config.memory_limit)?;

    let k = usize::try_from(config.k).context("MinHash k does not fit this platform")?;
    let max_keys = max_filter_keys(config.k, config.filter_bits, config.memory_limit);
    // Digested up front and rechecked before publication: the source is opened
    // by path again for every dictionary section, so a file replaced mid-build
    // could otherwise pair new keys with the old digest.
    let source = SourceIdentity::capture(config.hdt_path)?;
    let mut accumulators = config
        .roles
        .iter()
        .map(|&role| RoleAccumulator::new(role, config, max_keys, k))
        .collect::<Result<Vec<_>>>()?;

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
    for accumulator in &mut accumulators {
        let terms = accumulator.role.private_terms(&adapter)?;
        for term in terms {
            if let Some(hash) = iri_hash(&term?) {
                accumulator.add_hash(hash)?;
            }
        }
    }

    let mut role_data = accumulators
        .into_iter()
        .map(RoleAccumulator::finish)
        .collect::<Result<Vec<_>>>()?;
    let role_counts = role_data
        .iter()
        .map(|data| (data.role, data.key_count()))
        .collect();

    let mut staged = Vec::with_capacity(config.roles.len() * 2);
    for data in &mut role_data {
        stage_role_artifacts(data, config, source.digest(), &mut staged)?;
    }

    let files_written = staged.len();
    source.ensure_unchanged(config.hdt_path)?;
    publish_artifacts(staged)?;
    Ok(SketchSummary {
        files_written,
        role_counts,
    })
}

/// The `(filter, minhash)` paths for one role. The single place artifact names
/// are formed, so the no-clobber precheck and publication cannot diverge.
fn artifact_paths(output_dir: &Path, role: Role) -> (PathBuf, PathBuf) {
    (
        output_dir.join(format!("{}.filter", role.file_stem())),
        output_dir.join(format!("{}.minhash", role.file_stem())),
    )
}

fn stage_role_artifacts(
    data: &mut RoleData,
    config: SketchConfig<'_>,
    source_digest: &[u8; 32],
    staged: &mut Vec<StagedArtifact>,
) -> Result<()> {
    tracing::info!(
        "Building {} artifacts from {} distinct IRIs",
        data.role.file_stem(),
        data.key_count()
    );
    let (filter_target, minhash_target) = artifact_paths(config.output_dir, data.role);

    let mut filter_file = NamedTempFile::new_in(config.output_dir)?;
    write_filter_file(
        filter_file.as_file_mut(),
        data,
        config.filter_bits,
        source_digest,
    )?;
    filter_file.as_file().sync_all()?;
    staged.push(StagedArtifact {
        file: filter_file,
        target: filter_target,
    });

    let mut minhash_file = NamedTempFile::new_in(config.output_dir)?;
    write_minhash_file(minhash_file.as_file_mut(), data, config.k, source_digest)?;
    minhash_file.as_file().sync_all()?;
    staged.push(StagedArtifact {
        file: minhash_file,
        target: minhash_target,
    });
    Ok(())
}

fn common_header(
    magic: &[u8; 4],
    role: Role,
    key_count: u64,
    source_digest: &[u8; 32],
) -> [u8; COMMON_HEADER_LEN] {
    let mut header = [0u8; COMMON_HEADER_LEN];
    header[0..4].copy_from_slice(magic);
    header[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&CONVENTION_ID.to_le_bytes());
    header[8] = HASH_ID_XXH64;
    header[9] = role.id();
    header[16..24].copy_from_slice(&key_count.to_le_bytes());
    header[24..56].copy_from_slice(source_digest);
    header
}

fn write_filter_file(
    file: &mut File,
    data: &mut RoleData,
    filter_bits: u8,
    source_digest: &[u8; 32],
) -> Result<()> {
    let key_count = data.key_count();
    // Shared, Subjects, and Objects are mutually disjoint, so any duplicate key
    // in a sketch role is a hash collision.
    let keys = data
        .keys
        .read_sorted_distinct(DuplicateKeys::AreCollisions)?;
    let mut writer = Crc32cWriter::new(BufWriter::with_capacity(256 * 1024, file));
    writer.write_all(&common_header(b"KGFF", data.role, key_count, source_digest))?;
    writer.write_all(&[filter_bits])?;
    writer.write_all(&[0u8; 7])?;

    match filter_bits {
        8 => {
            let filter = BinaryFuse8::try_from_iterator(keys.iter().copied())
                .map_err(|error| anyhow::anyhow!("BinaryFuse8 construction failed: {error}"))?;
            write_filter_descriptor(&mut writer, &filter, filter.fingerprints.len())?;
            writer.write_all(&filter.fingerprints)?;
        }
        16 => {
            let filter = BinaryFuse16::try_from_iterator(keys.iter().copied())
                .map_err(|error| anyhow::anyhow!("BinaryFuse16 construction failed: {error}"))?;
            write_filter_descriptor(&mut writer, &filter, filter.fingerprints.len())?;
            for fingerprint in &filter.fingerprints {
                writer.write_all(&fingerprint.to_le_bytes())?;
            }
        }
        _ => unreachable!("filter width validated by caller"),
    }

    writer.finalize_and_write()?.flush()?;
    Ok(())
}

fn write_filter_descriptor<W: Write, F: DmaSerializable>(
    writer: &mut W,
    filter: &F,
    fingerprint_len: usize,
) -> Result<()> {
    ensure!(
        F::DESCRIPTOR_LEN == FILTER_DESCRIPTOR_LEN,
        "Unsupported binary fuse descriptor length {}",
        F::DESCRIPTOR_LEN
    );
    let mut descriptor = [0u8; FILTER_DESCRIPTOR_LEN];
    filter.dma_copy_descriptor_to(&mut descriptor);
    check_filter_geometry(&descriptor, fingerprint_len)?;
    writer.write_all(&descriptor)?;
    // Reserved, and required to be zero (docs/sketch-format.md §5.1). Recording
    // a construction-algorithm identity here would need a format change and a
    // version bump, not a unilateral write into a reserved field.
    writer.write_all(&0u32.to_le_bytes())?;
    writer.write_all(
        &u64::try_from(fingerprint_len)
            .context("Fingerprint array length exceeds u64")?
            .to_le_bytes(),
    )?;
    Ok(())
}

/// Refuse to emit a filter that violates the structural invariants readers rely
/// on (docs/sketch-format.md §5.3).
///
/// Invariant 5 in particular — `fingerprint_len == segment_count_length +
/// 2 * segment_length` — is what bounds the three probe indices, so a reader
/// that validated the header can skip bounds checks. Producing a file that
/// breaks it would hand every downstream reader an out-of-bounds read.
fn check_filter_geometry(
    descriptor: &[u8; FILTER_DESCRIPTOR_LEN],
    fingerprint_len: usize,
) -> Result<()> {
    let segment_length = u32::from_le_bytes(descriptor[8..12].try_into().unwrap());
    let segment_length_mask = u32::from_le_bytes(descriptor[12..16].try_into().unwrap());
    let segment_count_length = u32::from_le_bytes(descriptor[16..20].try_into().unwrap());

    ensure!(
        (4..=262_144).contains(&segment_length) && segment_length.is_power_of_two(),
        "binary fuse segment_length {segment_length} outside the specified range"
    );
    ensure!(
        segment_length_mask == segment_length - 1,
        "binary fuse segment_length_mask {segment_length_mask} does not match segment_length {segment_length}"
    );
    ensure!(
        segment_count_length % segment_length == 0 && segment_count_length >= segment_length,
        "binary fuse segment_count_length {segment_count_length} is not a positive multiple of {segment_length}"
    );
    let expected = u64::from(segment_count_length) + 2 * u64::from(segment_length);
    ensure!(
        fingerprint_len as u64 == expected,
        "binary fuse fingerprint length {fingerprint_len} != segment_count_length + 2*segment_length ({expected})"
    );
    ensure!(
        expected <= u64::from(u32::MAX),
        "binary fuse fingerprint length {expected} exceeds the 32-bit index space"
    );
    Ok(())
}

fn write_minhash_file(
    file: &mut File,
    data: &RoleData,
    k: u32,
    source_digest: &[u8; 32],
) -> Result<()> {
    let stored_count =
        u32::try_from(data.minima.len()).context("MinHash stored value count exceeds u32")?;
    // From stored_count, not key_count (docs/sketch-format.md §6.2). Under an
    // XXH64 collision a role can have key_count >= k with fewer than k distinct
    // keys; deriving this from key_count would then claim a full sketch that is
    // not there, and a consumer taking the saturated branch would read
    // minima[k - 1] past the end.
    let saturated = u8::from(stored_count == k);
    let mut writer = Crc32cWriter::new(BufWriter::with_capacity(256 * 1024, file));
    writer.write_all(&common_header(
        b"KGFM",
        data.role,
        data.key_count(),
        source_digest,
    ))?;
    writer.write_all(&k.to_le_bytes())?;
    writer.write_all(&stored_count.to_le_bytes())?;
    writer.write_all(&[saturated])?;
    writer.write_all(&[0u8; 7])?;
    for minimum in &data.minima {
        writer.write_all(&minimum.to_le_bytes())?;
    }
    writer.finalize_and_write()?.flush()?;
    Ok(())
}

/// `dictionary_counts` holds each selected role's upper bound on distinct
/// terms (its own section plus the shared section).
fn ensure_minhash_accumulator_memory(
    dictionary_counts: &[u64],
    k: u32,
    memory_limit: usize,
) -> Result<()> {
    let maximum_entries = dictionary_counts.iter().fold(0u64, |total, count| {
        total.saturating_add((*count).min(u64::from(k)))
    });
    let estimated = maximum_entries.saturating_mul(MINHASH_BYTES_PER_ENTRY_ESTIMATE);
    ensure!(
        estimated <= memory_limit as u64,
        "Selected MinHash accumulators may need an estimated {}, exceeding --memory-limit ({}); reduce --k or increase the limit",
        format_bytes(estimated),
        format_bytes(memory_limit as u64)
    );
    Ok(())
}

/// Largest key count one role's binary fuse build can take.
///
/// Covers the keys read back from the temporary file, xorf's peel arrays and
/// fingerprints, and the role's retained bottom-k values; capped by the key
/// ceiling of an implementation that indexes its arrays with `u32`.
fn max_filter_keys(k: u32, filter_bits: u8, memory_limit: usize) -> u64 {
    let bytes_per_key = if filter_bits == 8 { 48u64 } else { 50 };
    let minima_bytes = u64::from(k).saturating_mul(MINHASH_BYTES_PER_ENTRY_ESTIMATE);
    let budget = (memory_limit as u64).saturating_sub(minima_bytes);
    (budget / bytes_per_key).min(MAX_BINARY_FUSE_KEYS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek};
    use xxhash_rust::xxh64::xxh64;

    /// Shown when the bytes xorf produces for a fixed key set stop matching the
    /// published vectors — the situation the exact version pin in Cargo.toml
    /// exists to make deliberate rather than accidental.
    const XORF_OUTPUT_CHANGED: &str = "\
xorf now builds a different filter for the same keys, so hdtc emits different \
bytes than docs/sketch-format.md §9.3 publishes.

This is NOT a correctness failure: construction is not normative (§5.4), and the \
new filter is perfectly valid — readers elsewhere are unaffected. What broke is a \
published conformance vector.

To accept the change: regenerate §9.3 (seed, segment_*, fingerprint_len, \
fingerprint digest) and the matching values in this test and in \
tests/sketch_test.rs::sketch_reproduces_the_frozen_conformance_vectors, and note \
the change wherever the vectors are consumed. To reject it: restore the previous \
xorf version in Cargo.toml.

Note this vector only exercises small-n construction where the first seed \
succeeds; it cannot detect a change confined to large inputs or to the retry \
path. The version pin, not this test, is the actual guard.";

    fn test_config<'a>(temp_dir: &'a Path, roles: &'a [Role]) -> SketchConfig<'a> {
        SketchConfig {
            hdt_path: Path::new("unused.hdt"),
            output_dir: Path::new("unused"),
            temp_dir,
            roles,
            k: 3,
            filter_bits: 8,
            memory_limit: 4 << 30,
        }
    }

    #[test]
    fn xxh64_seed_zero_anchor() {
        assert_eq!(xxh64(b"", 0), 0xef46_db37_51d8_e999);
    }

    /// The frozen term→key table, docs/sketch-format.md §9.1.
    #[test]
    fn representative_term_hashes_are_stable() {
        for (term, byte_len, expected) in [
            ("http://example.org/Alice", 24, 0x9a60_9fb4_0498_cf38),
            ("http://example.org/", 19, 0x5ca3_af77_9db3_b833),
            ("https://example.org/a%20b", 25, 0x839f_f1bd_bec2_0449),
            ("https://example.org/a b", 23, 0x3003_d543_7a43_2cc8),
            (
                "https://example.org/path?q=1&r=2#frag",
                37,
                0x8474_26c7_4dd5_1315,
            ),
            ("https://例え.テスト/é/雪", 31, 0x7725_c712_ff13_1331),
            (
                "https://example.org/Ünicode/Ärger",
                35,
                0xc638_9897_ba43_27da,
            ),
            (
                "urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66",
                45,
                0xd295_8fdb_f467_8e1b,
            ),
            (
                "urn:example:long:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                65,
                0xc523_6d32_cb62_97aa,
            ),
        ] {
            assert_eq!(term.len(), byte_len, "UTF-8 byte count for {term:?}");
            assert_eq!(xxh64(term.as_bytes(), 0), expected, "term {term:?}");
        }
    }

    /// §6.2: `saturated` is derived from `stored_count`, never from
    /// `key_count`. A colliding role can have `key_count >= k` while storing
    /// fewer than `k` distinct minima; claiming saturation there would send a
    /// consumer into the §6.3 saturated branch to read `minima[k - 1]` off the
    /// end of the array.
    #[test]
    fn saturation_follows_stored_minima_not_the_iri_count() {
        let temp = tempfile::tempdir().unwrap();
        let roles = [Role::Subjects];
        let config = test_config(temp.path(), &roles);
        let k = 4usize;

        // Six IRIs whose hashes collide down to three distinct keys: key_count
        // reaches k, stored_count cannot.
        let mut accumulator = RoleAccumulator::new(Role::Subjects, config, u64::MAX, k).unwrap();
        for hash in [10, 20, 10, 30, 20, 10] {
            accumulator.add_hash(hash).unwrap();
        }
        let data = accumulator.finish().unwrap();
        assert_eq!(data.key_count(), 6, "six qualifying IRIs were scanned");
        assert_eq!(data.minima, vec![10, 20, 30], "only three distinct keys");

        let mut file = tempfile::tempfile().unwrap();
        write_minhash_file(&mut file, &data, k as u32, &[0; 32]).unwrap();
        let mut bytes = Vec::new();
        file.rewind().unwrap();
        file.read_to_end(&mut bytes).unwrap();

        assert_eq!(
            u32::from_le_bytes(bytes[56..60].try_into().unwrap()),
            4,
            "k"
        );
        assert_eq!(
            u32::from_le_bytes(bytes[60..64].try_into().unwrap()),
            3,
            "stored_count"
        );
        assert_eq!(
            bytes[64], 0,
            "key_count >= k but the sketch is not full, so saturated must be 0"
        );

        // And the ordinary case still saturates.
        let mut accumulator = RoleAccumulator::new(Role::Subjects, config, u64::MAX, k).unwrap();
        for hash in [1, 2, 3, 4, 5] {
            accumulator.add_hash(hash).unwrap();
        }
        let data = accumulator.finish().unwrap();
        let mut file = tempfile::tempfile().unwrap();
        write_minhash_file(&mut file, &data, k as u32, &[0; 32]).unwrap();
        let mut bytes = Vec::new();
        file.rewind().unwrap();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(u32::from_le_bytes(bytes[60..64].try_into().unwrap()), 4);
        assert_eq!(bytes[64], 1, "a full sketch is saturated");
    }

    /// §3.1: terms are hashed as exact codepoints, so the two Unicode normal
    /// forms of one visible string are distinct keys. An implementation that
    /// normalizes silently would pass every other test and still be unable to
    /// compare artifacts with anyone else.
    #[test]
    fn unicode_normal_forms_are_distinct_keys() {
        let nfc = "https://example.org/\u{e9}";
        let nfd = "https://example.org/e\u{301}";
        assert_eq!((nfc.len(), nfd.len()), (22, 23));
        assert_eq!(xxh64(nfc.as_bytes(), 0), 0x059c_b2e7_093f_e708);
        assert_eq!(xxh64(nfd.as_bytes(), 0), 0xf750_5861_366c_a3a5);
    }

    #[test]
    fn bottom_k_is_sorted_distinct_and_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let roles = [Role::Subjects];
        let config = test_config(temp.path(), &roles);
        let mut accumulator = RoleAccumulator::new(Role::Subjects, config, u64::MAX, 3).unwrap();
        for hash in [9, 2, 7, 2, 1, 8] {
            accumulator.add_hash(hash).unwrap();
        }
        let data = accumulator.finish().unwrap();
        assert_eq!(data.key_count(), 6);
        assert_eq!(data.minima, vec![1, 2, 7]);
    }

    #[test]
    fn key_budget_is_enforced_during_the_scan() {
        let temp = tempfile::tempdir().unwrap();
        let roles = [Role::Objects];
        let config = test_config(temp.path(), &roles);
        let mut accumulator = RoleAccumulator::new(Role::Objects, config, 2, 3).unwrap();
        accumulator.add_hash(1).unwrap();
        accumulator.add_hash(2).unwrap();
        let error = accumulator.add_hash(3).unwrap_err();
        assert!(error.to_string().contains("more than 2 qualifying IRIs"));
    }

    #[test]
    fn max_filter_keys_reserves_the_minhash_and_caps_at_the_fuse_ceiling() {
        // 4 GiB budget, k=65536: 2 MiB reserved for the bottom-k, rest at 48 B/key.
        assert_eq!(
            max_filter_keys(65_536, 8, 4 << 30),
            ((4u64 << 30) - 65_536 * 32) / 48
        );
        assert_eq!(max_filter_keys(2, 8, usize::MAX), MAX_BINARY_FUSE_KEYS);
    }

    #[test]
    fn binary_fuse_ceiling_keeps_capacity_in_u32() {
        // Mirrors xorf's sizing: capacity = round(9/8 · keys), rounded up to a
        // whole segment, held in a u32.
        const SEGMENT: u64 = 262_144;
        let capacity = |keys: u64| (keys as f64 * 1.125).round() as u64;
        let usable = (u64::from(u32::MAX) / SEGMENT) * SEGMENT;

        assert!(capacity(MAX_BINARY_FUSE_KEYS).div_ceil(SEGMENT) * SEGMENT <= usable);
        // One key more rounds up to 2^32, past what a u32 array length holds.
        assert!(
            capacity(MAX_BINARY_FUSE_KEYS + 1).div_ceil(SEGMENT) * SEGMENT > u64::from(u32::MAX)
        );
    }

    #[test]
    fn minhash_memory_preflight_combines_selected_roles() {
        let error = ensure_minhash_accumulator_memory(&[30, 40], 100, 2_239).unwrap_err();
        assert!(error.to_string().contains("reduce --k"));

        ensure_minhash_accumulator_memory(&[30], 100, 960).unwrap();
        ensure_minhash_accumulator_memory(&[30, 40], 5, 320).unwrap();
    }

    #[test]
    fn common_header_has_stable_layout() {
        let digest = [0x5a; 32];
        let header = common_header(b"KGFM", Role::Objects, 42, &digest);
        assert_eq!(&header[0..4], b"KGFM");
        assert_eq!(u16::from_le_bytes(header[4..6].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(header[6..8].try_into().unwrap()), 1);
        assert_eq!(header[8], 1);
        assert_eq!(header[9], 1);
        assert_eq!(&header[10..16], &[0; 6]);
        assert_eq!(u64::from_le_bytes(header[16..24].try_into().unwrap()), 42);
        assert_eq!(&header[24..56], &digest);
    }

    #[test]
    fn binary_fuse_and_minhash_reference_vectors_are_stable() {
        let keys: Vec<u64> = (0..100)
            .map(|index| {
                xxh64(
                    format!("https://example.org/resource/{index:03}").as_bytes(),
                    0,
                )
            })
            .collect();
        let mut minima = keys.clone();
        minima.sort_unstable();
        assert_eq!(
            &minima[..16],
            &[
                0x0095_2d96_04e1_cf2c,
                0x00cc_3131_e8f7_a0c5,
                0x01f3_52d0_f9d5_cf80,
                0x06bf_0ecf_32e0_b062,
                0x07d9_a683_f85c_7cb8,
                0x07fa_5da3_a095_2933,
                0x0a72_4817_be71_ee72,
                0x0da9_8875_b72f_df91,
                0x0f98_c196_c842_182a,
                0x0fd0_e9cd_6b8a_a629,
                0x12c1_9725_ea52_303f,
                0x146e_55f2_b679_e888,
                0x1527_5b37_06d1_e3af,
                0x15ff_25a1_0102_039d,
                0x1940_b18c_45c1_e8a5,
                0x1967_6957_cb8c_7933,
            ]
        );

        let filter = BinaryFuse8::try_from(&keys).unwrap();
        let mut descriptor = [0u8; FILTER_DESCRIPTOR_LEN];
        filter.dma_copy_descriptor_to(&mut descriptor);
        assert_eq!(
            descriptor,
            [
                0xc1, 0x5c, 0x02, 0x89, 0xec, 0x2d, 0x0a, 0x91, 0x40, 0x00, 0x00, 0x00, 0x3f, 0x00,
                0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
            ],
            "{XORF_OUTPUT_CHANGED}"
        );
        assert_eq!(filter.fingerprints.len(), 192, "{XORF_OUTPUT_CHANGED}");
        assert_eq!(
            format!("{:x}", Sha256::digest(&filter.fingerprints)),
            "4684db4089f6c89f7609e1cd00d8246e02835f64a1377ca14cc63ee491df9960",
            "{XORF_OUTPUT_CHANGED}"
        );
        // The first row and the three trace indices of the §9.3 hex dump. The
        // digest above pins the array; these pin the transcription, so a typo
        // in the published bytes fails here rather than in someone else's
        // implementation.
        assert_eq!(
            &filter.fingerprints[..16],
            &[
                0x00, 0x00, 0xDA, 0xB1, 0x4C, 0x98, 0x00, 0xD9, 0x00, 0x00, 0x00, 0x72, 0x00, 0x00,
                0x00, 0x19
            ],
            "{XORF_OUTPUT_CHANGED}"
        );
        assert_eq!(
            (
                filter.fingerprints[55],
                filter.fingerprints[118],
                filter.fingerprints[144]
            ),
            (0x39, 0x28, 0xFD),
            "{XORF_OUTPUT_CHANGED}"
        );
        check_filter_geometry(&descriptor, filter.fingerprints.len()).unwrap();
    }

    /// The §5.2 probe, reimplemented here from the specification text rather
    /// than by calling xorf, and checked against the §9.4 worked trace. If this
    /// drifts from the document, an independent implementation reading the
    /// document will disagree with what we emit.
    #[test]
    fn spec_probe_matches_the_frozen_trace() {
        fn mix64(mut k: u64) -> u64 {
            k ^= k >> 33;
            k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
            k ^= k >> 33;
            k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
            k ^= k >> 33;
            k
        }
        fn contains(key: u64, seed: u64, sl: u32, mask: u32, scl: u32, fps: &[u8]) -> (u8, bool) {
            let h = mix64(key.wrapping_add(seed));
            let mut f = (h ^ (h >> 32)) as u8;
            let hi = ((u128::from(h) * u128::from(scl)) >> 64) as u64;
            let h0 = hi as u32;
            let h1 = (h0.wrapping_add(sl)) ^ (((h >> 18) as u32) & mask);
            let h2 = (h0.wrapping_add(sl).wrapping_add(sl)) ^ ((h as u32) & mask);
            f ^= fps[h0 as usize] ^ fps[h1 as usize] ^ fps[h2 as usize];
            (f, f == 0)
        }

        let keys: Vec<u64> = (0..100)
            .map(|i| xxh64(format!("https://example.org/resource/{i:03}").as_bytes(), 0))
            .collect();
        let filter = BinaryFuse8::try_from(&keys).unwrap();
        let (seed, sl, mask, scl) = (0x910a_2dec_8902_5cc1u64, 64u32, 63u32, 64u32);

        // §9.4 member trace.
        let member = xxh64(b"https://example.org/resource/000", 0);
        assert_eq!(member, 0x45c6_4ad7_8fde_51e4);
        assert_eq!(mix64(member.wrapping_add(seed)), 0xdd07_2e4b_6b05_a8a7);
        assert_eq!(
            contains(member, seed, sl, mask, scl, &filter.fingerprints),
            (0x00, true)
        );

        // §9.4 non-member trace.
        let absent = xxh64(b"https://example.org/absent", 0);
        assert_eq!(absent, 0xd5ec_b43f_977b_8759);
        assert_eq!(mix64(absent.wrapping_add(seed)), 0x4439_e9b4_d171_6e65);
        assert_eq!(
            contains(absent, seed, sl, mask, scl, &filter.fingerprints),
            (0x4c, false)
        );

        // The filter's defining property: no false negatives, ever.
        for key in &keys {
            assert!(
                contains(*key, seed, sl, mask, scl, &filter.fingerprints).1,
                "false negative for {key:#018x}"
            );
        }
    }
}
