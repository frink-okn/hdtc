//! K-way merge of partial vocabularies into global dictionary with ID mappings.

use crate::dictionary::pfc::PfcEncoder;
use crate::dictionary::DictCounts;
use crate::pipeline::PartialVocabReader;
use crate::sort::parallel_merge::{
    build_merge_tree, Mergeable, MergeSource, MergeTreeConfig, MergeTreeHandle,
};
use anyhow::{Context, Result};
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::batch_vocab::Roles;

/// Tracks which batch contributed a term and with what roles/local IDs.
#[derive(Clone, Debug)]
pub struct TermBatchInfo {
    pub batch_id: usize,
    pub roles: Roles,
    pub so_local_id: Option<u32>,
    pub p_local_id: Option<u32>,
}

/// ID mapping for a single batch (local ID → global ID).
#[derive(Debug, Clone)]
pub struct IdMapping {
    pub batch_id: usize,
    /// Subject/Object local ID → global ID
    pub so_map: Vec<u64>,
    /// Predicate local ID → global predicate ID
    pub p_map: Vec<u64>,
}

impl IdMapping {
    fn new(batch_id: usize) -> Self {
        Self {
            batch_id,
            so_map: Vec::new(),
            p_map: Vec::new(),
        }
    }

    /// Write ID mapping to a file.
    pub fn write_to_file(&self, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        let mut encoder = zstd::Encoder::new(&mut writer, 3)?;

        // Write batch ID
        encoder.write_all(&(self.batch_id as u32).to_le_bytes())?;

        // Write SO map
        encoder.write_all(&(self.so_map.len() as u32).to_le_bytes())?;
        for &id in &self.so_map {
            encoder.write_all(&id.to_le_bytes())?;
        }

        // Write P map
        encoder.write_all(&(self.p_map.len() as u32).to_le_bytes())?;
        for &id in &self.p_map {
            encoder.write_all(&id.to_le_bytes())?;
        }

        encoder.finish()?;
        Ok(())
    }

    /// Read ID mapping from a file.
    pub fn read_from_file(path: &Path) -> Result<Self> {
        use std::io::{BufReader, Read};

        let file = File::open(path)?;
        let buf_reader = BufReader::new(file);
        let mut decoder = zstd::Decoder::with_buffer(buf_reader)?;

        // Read batch ID
        let mut batch_id_bytes = [0u8; 4];
        decoder.read_exact(&mut batch_id_bytes)?;
        let batch_id = u32::from_le_bytes(batch_id_bytes) as usize;

        // Read SO map
        let mut so_len_bytes = [0u8; 4];
        decoder.read_exact(&mut so_len_bytes)?;
        let so_len = u32::from_le_bytes(so_len_bytes) as usize;

        let mut so_map = Vec::with_capacity(so_len);
        for _ in 0..so_len {
            let mut id_bytes = [0u8; 8];
            decoder.read_exact(&mut id_bytes)?;
            so_map.push(u64::from_le_bytes(id_bytes));
        }

        // Read P map
        let mut p_len_bytes = [0u8; 4];
        decoder.read_exact(&mut p_len_bytes)?;
        let p_len = u32::from_le_bytes(p_len_bytes) as usize;

        let mut p_map = Vec::with_capacity(p_len);
        for _ in 0..p_len {
            let mut id_bytes = [0u8; 8];
            decoder.read_exact(&mut id_bytes)?;
            p_map.push(u64::from_le_bytes(id_bytes));
        }

        Ok(Self {
            batch_id,
            so_map,
            p_map,
        })
    }
}

#[derive(Debug)]
struct StreamEntry {
    term: Vec<u8>,
    roles: Roles,
    so_local_id: Option<u32>,
    p_local_id: Option<u32>,
    source_batch: usize,
}

impl Mergeable for StreamEntry {
    fn merge_cmp(&self, other: &Self) -> Ordering {
        self.term.cmp(&other.term)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        // term_len (u32) + term + roles (u8) + conditional IDs + source_batch (u32)
        writer.write_all(&(self.term.len() as u32).to_le_bytes())?;
        writer.write_all(&self.term)?;
        writer.write_all(&[self.roles.bits()])?;
        if self.roles.intersects(Roles::SUBJECT | Roles::OBJECT) {
            let so_id = self.so_local_id.expect("SO local ID must be present");
            writer.write_all(&so_id.to_le_bytes())?;
        }
        if self.roles.contains(Roles::PREDICATE) {
            let p_id = self.p_local_id.expect("P local ID must be present");
            writer.write_all(&p_id.to_le_bytes())?;
        }
        writer.write_all(&(self.source_batch as u32).to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let term_len = u32::from_le_bytes(len_buf) as usize;

        let mut term = vec![0u8; term_len];
        reader.read_exact(&mut term)?;

        let mut roles_buf = [0u8; 1];
        reader.read_exact(&mut roles_buf)?;
        let roles = Roles::from_bits_truncate(roles_buf[0]);

        let so_local_id = if roles.intersects(Roles::SUBJECT | Roles::OBJECT) {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            Some(u32::from_le_bytes(buf))
        } else {
            None
        };

        let p_local_id = if roles.contains(Roles::PREDICATE) {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            Some(u32::from_le_bytes(buf))
        } else {
            None
        };

        let mut batch_buf = [0u8; 4];
        reader.read_exact(&mut batch_buf)?;
        let source_batch = u32::from_le_bytes(batch_buf) as usize;

        Ok(Some(StreamEntry {
            term,
            roles,
            so_local_id,
            p_local_id,
            source_batch,
        }))
    }
}

const PROVISIONAL_SO_ID_TAG: u64 = 1 << 63;
const PROVISIONAL_SO_ID_MASK: u64 = !PROVISIONAL_SO_ID_TAG;

fn encode_provisional_so_id(local_section_id: u64) -> u64 {
    debug_assert!(local_section_id > 0);
    debug_assert!(local_section_id <= PROVISIONAL_SO_ID_MASK);
    local_section_id | PROVISIONAL_SO_ID_TAG
}

fn is_provisional_so_id(id: u64) -> bool {
    (id & PROVISIONAL_SO_ID_TAG) != 0
}

fn decode_provisional_so_id(id: u64) -> u64 {
    id & PROVISIONAL_SO_ID_MASK
}

fn finalize_provisional_so_ids(id_mappings: &mut [IdMapping], shared_count: u64) -> Result<()> {
    if shared_count > PROVISIONAL_SO_ID_MASK {
        anyhow::bail!(
            "Shared term count {} exceeds provisional ID range",
            shared_count
        );
    }

    for mapping in id_mappings {
        for mapped_id in &mut mapping.so_map {
            if is_provisional_so_id(*mapped_id) {
                let local_section_id = decode_provisional_so_id(*mapped_id);
                *mapped_id = shared_count + local_section_id;
            }
        }
    }

    Ok(())
}

/// Result of vocabulary merge.
pub struct VocabMergeResult {
    pub dict_sections: Vec<Vec<u8>>, // PFC-encoded: [shared, subjects, predicates, objects]
    pub counts: DictCounts,
    #[allow(dead_code)]
    pub predicate_ids: HashMap<String, u64>,
}

/// Merge partial vocabularies into global dictionary.
pub fn merge_vocabularies(
    batch_infos: Vec<(usize, PathBuf)>, // (batch_id, vocab_path)
    temp_dir: &Path,
    memory_budget: usize,
) -> Result<VocabMergeResult> {
    tracing::debug!("Merging {} partial vocabularies", batch_infos.len());

    let mut stream_reader_init_time = Duration::ZERO;
    let mut stream_read_time = Duration::ZERO;
    let mut id_assignment_time = Duration::ZERO;
    let mut so_map_finalize_time = Duration::ZERO;
    let mut pfc_serialize_time = Duration::ZERO;
    let mut mapping_write_time = Duration::ZERO;
    let mut stream_bytes_read = 0u64;

    let mut id_mappings: Vec<IdMapping> = Vec::new();

    for (batch_id, vocab_path) in &batch_infos {
        let init_start = Instant::now();
        tracing::debug!("Opening partial vocab header for batch {}: {:?}", batch_id, vocab_path);
        stream_bytes_read += std::fs::metadata(vocab_path)
            .with_context(|| format!("Failed to stat partial vocab for batch {}", batch_id))?
            .len();
        let reader = PartialVocabReader::open(vocab_path)
            .with_context(|| format!("Failed to open partial vocab for batch {}", batch_id))?;

        // Pre-allocate ID mappings based on max IDs from this batch
        let max_so_id = reader.max_so_id();
        let max_p_id = reader.max_p_id();
        let mut mapping = IdMapping::new(*batch_id);
        mapping.so_map = vec![0u64; (max_so_id + 1) as usize];
        mapping.p_map = vec![0u64; (max_p_id + 1) as usize];
        id_mappings.push(mapping);
        stream_reader_init_time += init_start.elapsed();
    }

    // Initialize PFC encoders for each section
    let mut shared_enc = PfcEncoder::new();
    let mut subjects_enc = PfcEncoder::new();
    let mut predicates_enc = PfcEncoder::new();
    let mut objects_enc = PfcEncoder::new();

    let mut counts = DictCounts::default();
    let mut predicate_ids = HashMap::new();

    // Single-pass streaming merge:
    // - Aggregate term roles across batches
    // - Assign IDs and write dictionary sections immediately
    // - Use provisional SO IDs for subject-only/object-only terms, then finalize once shared count is known
    tracing::debug!("Single-pass merge: assigning global IDs with provisional SO offsets");

    let stream_init_start = Instant::now();
    let stream_channel_capacity = calculate_stage4_stream_channel_capacity(memory_budget);
    tracing::debug!(
        "Stage 4 stream settings: capacity={} (memory budget: {} MiB)",
        stream_channel_capacity,
        memory_budget / 1024 / 1024
    );
    let merge_handle = build_vocab_merge_tree(&batch_infos, stream_channel_capacity, temp_dir)?;
    let stream_rx = &merge_handle.rx;
    stream_reader_init_time += stream_init_start.elapsed();

    let stream_result = (|| -> Result<()> {
        let mut current_term: Option<Vec<u8>> = None;
        let mut merged_roles = Roles::empty();
        let mut batches_with_term: Vec<TermBatchInfo> = Vec::new();

        loop {
            let read_start = Instant::now();
            let recv_item = stream_rx.recv();
            stream_read_time += read_start.elapsed();

            let stream_entry = match recv_item {
                Ok(Ok(entry)) => entry,
                Ok(Err(e)) => return Err(e),
                Err(_) => break,
            };

            let term = stream_entry.term;
            let source_batch = stream_entry.source_batch;
            let roles_in_batch = stream_entry.roles;
            let so_local_id = stream_entry.so_local_id;
            let p_local_id = stream_entry.p_local_id;

            let is_same_term = current_term.as_ref() == Some(&term);

            if !is_same_term
                && let Some(prev_term) = &current_term
            {
                let assign_start = Instant::now();
                assign_global_ids_and_record_mappings(
                    prev_term,
                    merged_roles,
                    &batches_with_term,
                    &mut counts,
                    &mut shared_enc,
                    &mut subjects_enc,
                    &mut predicates_enc,
                    &mut objects_enc,
                    &mut predicate_ids,
                    &mut id_mappings,
                )?;
                id_assignment_time += assign_start.elapsed();

                batches_with_term.clear();
                merged_roles = Roles::empty();
            }

            merged_roles |= roles_in_batch;
            batches_with_term.push(TermBatchInfo {
                batch_id: source_batch,
                roles: roles_in_batch,
                so_local_id,
                p_local_id,
            });
            current_term = Some(term);
        }

        if let Some(term) = current_term {
            let assign_start = Instant::now();
            assign_global_ids_and_record_mappings(
                &term,
                merged_roles,
                &batches_with_term,
                &mut counts,
                &mut shared_enc,
                &mut subjects_enc,
                &mut predicates_enc,
                &mut objects_enc,
                &mut predicate_ids,
                &mut id_mappings,
            )?;
            id_assignment_time += assign_start.elapsed();
        }

        Ok(())
    })();

    merge_handle.join()?;
    stream_result?;

    let so_finalize_start = Instant::now();
    finalize_provisional_so_ids(&mut id_mappings, counts.shared)?;
    so_map_finalize_time += so_finalize_start.elapsed();

    tracing::debug!(
        "Merged vocabulary: {} shared, {} subjects, {} predicates, {} objects",
        counts.shared,
        counts.subjects,
        counts.predicates,
        counts.objects
    );

    // Encode dictionary sections
    let mut dict_sections = Vec::new();
    let serialize_start = Instant::now();

    let mut shared_buf = Vec::new();
    shared_enc.write_to(&mut shared_buf)?;
    dict_sections.push(shared_buf);

    let mut subjects_buf = Vec::new();
    subjects_enc.write_to(&mut subjects_buf)?;
    dict_sections.push(subjects_buf);

    let mut predicates_buf = Vec::new();
    predicates_enc.write_to(&mut predicates_buf)?;
    dict_sections.push(predicates_buf);

    let mut objects_buf = Vec::new();
    objects_enc.write_to(&mut objects_buf)?;
    dict_sections.push(objects_buf);
    pfc_serialize_time += serialize_start.elapsed();

    // Write ID mappings to files
    let mapping_write_start = Instant::now();
    id_mappings.par_iter().try_for_each(|mapping| -> Result<()> {
        let mapping_path = temp_dir.join(format!("id_mapping_{:06}.map.zst", mapping.batch_id));
        mapping
            .write_to_file(&mapping_path)
            .with_context(|| format!("Failed to write ID mapping for batch {}", mapping.batch_id))?;
        tracing::debug!(
            "Wrote ID mapping for batch {}: {} SO entries, {} P entries",
            mapping.batch_id,
            mapping.so_map.len(),
            mapping.p_map.len()
        );
        Ok(())
    })?;
    mapping_write_time += mapping_write_start.elapsed();

    tracing::debug!(
        "Stage 4 timing: stream init {:.3}s/read {:.3}s ({} MB), assign {:.3}s, finalize SO-map {:.3}s, dict serialize {:.3}s, mapping writes {:.3}s",
        stream_reader_init_time.as_secs_f64(),
        stream_read_time.as_secs_f64(),
        stream_bytes_read / (1024 * 1024),
        id_assignment_time.as_secs_f64(),
        so_map_finalize_time.as_secs_f64(),
        pfc_serialize_time.as_secs_f64(),
        mapping_write_time.as_secs_f64(),
    );

    Ok(VocabMergeResult {
        dict_sections,
        counts,
        predicate_ids,
    })
}

/// Calculate the bounded channel capacity for the k-way merge stream.
///
/// `stage4_budget` is the full Stage 4 memory budget (which owns the entire
/// memory limit since Stage 4 runs alone).  We allocate a small fraction for
/// the stream channel; the rest is available for id_mappings, PFC encoders,
/// and per-batch reader threads.
fn calculate_stage4_stream_channel_capacity(stage4_budget: usize) -> usize {
    const MIB: usize = 1024 * 1024;

    // ~5% of the stage budget for the channel, clamped to reasonable bounds.
    let estimated_entry_bytes = 160usize; // term bytes + metadata per StreamEntry
    let queue_budget = (stage4_budget / 20).clamp(8 * MIB, 128 * MIB);
    (queue_budget / estimated_entry_bytes).clamp(64, 4096)
}

/// Build a parallel merge tree over partial vocabulary files.
///
/// Each leaf thread opens a `PartialVocabReader` and maps entries to
/// `StreamEntry` with the correct `source_batch`. When the number of
/// batches exceeds the merge tree's max fan-in, multi-round merging
/// with intermediate temp files is used automatically.
fn build_vocab_merge_tree(
    batch_infos: &[(usize, PathBuf)],
    channel_capacity: usize,
    temp_dir: &Path,
) -> Result<MergeTreeHandle<StreamEntry>> {
    let sources: Vec<MergeSource<StreamEntry>> = batch_infos
        .iter()
        .map(|(batch_id, vocab_path)| {
            let batch_id = *batch_id;
            let vocab_path = vocab_path.clone();
            MergeSource::Factory(Box::new(move || {
                let reader = PartialVocabReader::open(&vocab_path).with_context(|| {
                    format!("Failed to open partial vocab for batch {}", batch_id)
                })?;
                Ok(Box::new(reader.map(move |entry_result| {
                    entry_result.map(|entry| StreamEntry {
                        term: entry.term,
                        roles: entry.roles,
                        so_local_id: entry.so_local_id,
                        p_local_id: entry.p_local_id,
                        source_batch: batch_id,
                    })
                }))
                    as Box<dyn Iterator<Item = Result<StreamEntry>> + Send>)
            }))
        })
        .collect();

    let config = MergeTreeConfig::new(temp_dir)
        .with_channel_capacity(channel_capacity);

    build_merge_tree(sources, &config)
}

/// Assign global IDs and record mappings for a single term.
#[allow(clippy::too_many_arguments)]
fn assign_global_ids_and_record_mappings(
    term: &[u8],
    roles: Roles,
    batches: &[TermBatchInfo],
    counts: &mut DictCounts,
    shared_enc: &mut PfcEncoder,
    subjects_enc: &mut PfcEncoder,
    predicates_enc: &mut PfcEncoder,
    objects_enc: &mut PfcEncoder,
    predicate_ids: &mut HashMap<String, u64>,
    id_mappings: &mut [IdMapping],
) -> Result<()> {
    let term_str = std::str::from_utf8(term)
        .with_context(|| format!("Invalid UTF-8 in term: {:?}", term))?;

    // Handle predicates (separate ID space)
    if roles.contains(Roles::PREDICATE) {
        counts.predicates += 1;
        let global_pred_id = counts.predicates;
        predicates_enc.push(term_str);
        predicate_ids.insert(term_str.to_string(), global_pred_id);

        // Record mapping for each batch that had this predicate
        for info in batches {
            if info.roles.contains(Roles::PREDICATE)
                && let Some(local_p_id) = info.p_local_id
            {
                id_mappings[info.batch_id].p_map[local_p_id as usize] = global_pred_id;
            }
        }
    }

    // Handle subjects/objects (shared ID space)
    if roles.intersects(Roles::SUBJECT | Roles::OBJECT) {
        let global_so_id = if roles.contains(Roles::SUBJECT | Roles::OBJECT) {
            // Shared: appears as both subject and object
            counts.shared += 1;
            shared_enc.push(term_str);
            counts.shared
        } else if roles.contains(Roles::SUBJECT) {
            // Subject-only: use provisional section-local ID, fix offset after stream completes
            counts.subjects += 1;
            subjects_enc.push(term_str);
            encode_provisional_so_id(counts.subjects)
        } else {
            // Object-only: use provisional section-local ID, fix offset after stream completes
            counts.objects += 1;
            objects_enc.push(term_str);
            encode_provisional_so_id(counts.objects)
        };

        // Record mapping for each batch that had this subject/object
        for info in batches {
            if info.roles.intersects(Roles::SUBJECT | Roles::OBJECT)
                && let Some(local_so_id) = info.so_local_id
            {
                id_mappings[info.batch_id].so_map[local_so_id as usize] = global_so_id;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::partial_vocab::{PartialVocabWriter, PartialVocabEntry};
    use tempfile::TempDir;

    const TEST_MEMORY_BUDGET: usize = 512 * 1024 * 1024;

    fn read_mapping_from_temp(temp_path: &Path, batch_id: usize) -> Result<IdMapping> {
        let mapping_path = temp_path.join(format!("id_mapping_{:06}.map.zst", batch_id));
        IdMapping::read_from_file(&mapping_path)
    }

    /// Create a test partial vocabulary file with given entries.
    fn create_test_partial_vocab(
        path: &Path,
        entries: Vec<(&str, Roles, Option<u32>, Option<u32>)>,
    ) -> Result<()> {
        // Calculate max local IDs for the header
        let mut max_so_id = 0u32;
        let mut max_p_id = 0u32;
        for (_, _, so_id, p_id) in &entries {
            if let Some(id) = so_id {
                max_so_id = max_so_id.max(*id);
            }
            if let Some(id) = p_id {
                max_p_id = max_p_id.max(*id);
            }
        }

        let mut writer = PartialVocabWriter::create(path)?;
        writer.write_header(entries.len() as u32, max_so_id, max_p_id)?;

        for (term, roles, so_id, p_id) in entries {
            let entry = PartialVocabEntry::new(term.as_bytes().to_vec(), roles, so_id, p_id);
            writer.write_entry(&entry)?;
        }
        writer.finish()?;
        Ok(())
    }

    /// Test merging two batches with completely different terms.
    #[test]
    fn test_merge_disjoint_vocabularies() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Batch 0: terms a, b, c
        let batch0_path = temp_path.join("batch0.vocab.zst");
        create_test_partial_vocab(
            &batch0_path,
            vec![
                ("a", Roles::SUBJECT, Some(0), None),
                ("b", Roles::OBJECT, Some(1), None),
                ("c", Roles::PREDICATE, None, Some(0)),
            ],
        )?;

        // Batch 1: terms d, e, f
        let batch1_path = temp_path.join("batch1.vocab.zst");
        create_test_partial_vocab(
            &batch1_path,
            vec![
                ("d", Roles::SUBJECT, Some(0), None),
                ("e", Roles::OBJECT, Some(1), None),
                ("f", Roles::PREDICATE, None, Some(0)),
            ],
        )?;

        let batch_infos = vec![(0, batch0_path), (1, batch1_path)];
        let result = merge_vocabularies(batch_infos, temp_path, TEST_MEMORY_BUDGET)?;

        // Verify counts
        assert_eq!(result.counts.shared, 0);
        assert_eq!(result.counts.subjects, 2);
        assert_eq!(result.counts.objects, 2);
        assert_eq!(result.counts.predicates, 2);

        // Verify ID mapping files are correct
        let mapping_0 = read_mapping_from_temp(temp_path, 0)?;
        let mapping_1 = read_mapping_from_temp(temp_path, 1)?;

        // Batch 0 mappings: a→1 (first subject), b→1 (first object, same ID offset), c→1 (first predicate)
        assert_eq!(mapping_0.so_map[0], 1); // a (first subject-only)
        assert_eq!(mapping_0.so_map[1], 1); // b (first object-only)
        assert_eq!(mapping_0.p_map[0], 1);  // c (first predicate)

        // Batch 1 mappings: d→2 (second subject), e→2 (second object, same ID offset), f→2 (second predicate)
        assert_eq!(mapping_1.so_map[0], 2); // d (second subject-only)
        assert_eq!(mapping_1.so_map[1], 2); // e (second object-only)
        assert_eq!(mapping_1.p_map[0], 2);  // f (second predicate)

        Ok(())
    }

    /// Test merging batches with overlapping terms.
    #[test]
    fn test_merge_overlapping_terms() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Batch 0: term "x" as subject, predicate as "p1"
        let batch0_path = temp_path.join("batch0.vocab.zst");
        create_test_partial_vocab(
            &batch0_path,
            vec![
                ("p1", Roles::PREDICATE, None, Some(0)),
                ("x", Roles::SUBJECT, Some(0), None),
            ],
        )?;

        // Batch 1: term "x" as object, predicate as "p1"
        let batch1_path = temp_path.join("batch1.vocab.zst");
        create_test_partial_vocab(
            &batch1_path,
            vec![
                ("p1", Roles::PREDICATE, None, Some(0)),
                ("x", Roles::OBJECT, Some(0), None),
            ],
        )?;

        let batch_infos = vec![(0, batch0_path), (1, batch1_path)];
        let result = merge_vocabularies(batch_infos, temp_path, TEST_MEMORY_BUDGET)?;

        // "x" should be shared (appears as both subject and object)
        // "p1" should be a predicate
        assert_eq!(result.counts.shared, 1); // x
        assert_eq!(result.counts.subjects, 0);
        assert_eq!(result.counts.objects, 0);
        assert_eq!(result.counts.predicates, 1); // p1

        let mapping_0 = read_mapping_from_temp(temp_path, 0)?;
        let mapping_1 = read_mapping_from_temp(temp_path, 1)?;

        // Both batches should map "x" to the same global ID (1, the shared ID)
        assert_eq!(mapping_0.so_map[0], 1); // x from batch 0
        assert_eq!(mapping_1.so_map[0], 1); // x from batch 1

        // Both batches should map "p1" to the same global predicate ID (1)
        assert_eq!(mapping_0.p_map[0], 1); // p1 from batch 0
        assert_eq!(mapping_1.p_map[0], 1); // p1 from batch 1

        Ok(())
    }

    /// Test merging with a term appearing in all three roles across batches.
    #[test]
    fn test_merge_multi_role_term() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Batch 0: "multi" as subject
        let batch0_path = temp_path.join("batch0.vocab.zst");
        create_test_partial_vocab(
            &batch0_path,
            vec![("multi", Roles::SUBJECT, Some(0), None)],
        )?;

        // Batch 1: "multi" as predicate
        let batch1_path = temp_path.join("batch1.vocab.zst");
        create_test_partial_vocab(
            &batch1_path,
            vec![("multi", Roles::PREDICATE, None, Some(0))],
        )?;

        // Batch 2: "multi" as object
        let batch2_path = temp_path.join("batch2.vocab.zst");
        create_test_partial_vocab(
            &batch2_path,
            vec![("multi", Roles::OBJECT, Some(0), None)],
        )?;

        let batch_infos = vec![
            (0, batch0_path),
            (1, batch1_path),
            (2, batch2_path),
        ];
        let result = merge_vocabularies(batch_infos, temp_path, TEST_MEMORY_BUDGET)?;

        // "multi" should be shared (appears as both subject and object)
        // and also as predicate
        assert_eq!(result.counts.shared, 1);
        assert_eq!(result.counts.predicates, 1);

        let mapping_0 = read_mapping_from_temp(temp_path, 0)?;
        let mapping_1 = read_mapping_from_temp(temp_path, 1)?;
        let mapping_2 = read_mapping_from_temp(temp_path, 2)?;

        // Verify mappings for each batch
        // Batch 0: "multi" as subject → global ID 1 (shared section starts at 1)
        assert_eq!(mapping_0.so_map[0], 1);
        // Batch 1: "multi" as predicate → global ID 1
        assert_eq!(mapping_1.p_map[0], 1);
        // Batch 2: "multi" as object → global ID 1 (shared)
        assert_eq!(mapping_2.so_map[0], 1);

        Ok(())
    }

    /// Test merging three batches with complex overlap patterns.
    #[test]
    fn test_merge_three_batches_complex() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Batch 0: a (subject), b (predicate)
        let batch0_path = temp_path.join("batch0.vocab.zst");
        create_test_partial_vocab(
            &batch0_path,
            vec![
                ("a", Roles::SUBJECT, Some(0), None),
                ("b", Roles::PREDICATE, None, Some(0)),
            ],
        )?;

        // Batch 1: a (object), b (subject), c (predicate)
        let batch1_path = temp_path.join("batch1.vocab.zst");
        create_test_partial_vocab(
            &batch1_path,
            vec![
                ("a", Roles::OBJECT, Some(0), None),
                ("b", Roles::SUBJECT, Some(1), None),
                ("c", Roles::PREDICATE, None, Some(0)),
            ],
        )?;

        // Batch 2: b (object), d (subject)
        let batch2_path = temp_path.join("batch2.vocab.zst");
        create_test_partial_vocab(
            &batch2_path,
            vec![
                ("b", Roles::OBJECT, Some(0), None),
                ("d", Roles::SUBJECT, Some(1), None),
            ],
        )?;

        let batch_infos = vec![
            (0, batch0_path),
            (1, batch1_path),
            (2, batch2_path),
        ];
        let result = merge_vocabularies(batch_infos, temp_path, TEST_MEMORY_BUDGET)?;

        // "a" shared (subject + object), "b" shared (subject + object) + predicate, "c" predicate, "d" subject
        assert_eq!(result.counts.shared, 2);     // a, b
        assert_eq!(result.counts.subjects, 1);   // d
        assert_eq!(result.counts.predicates, 2); // b, c

        let mapping_0 = read_mapping_from_temp(temp_path, 0)?;
        let mapping_1 = read_mapping_from_temp(temp_path, 1)?;
        let mapping_2 = read_mapping_from_temp(temp_path, 2)?;

        // Verify ID mappings are consistent across batches
        let a_global_b0 = mapping_0.so_map[0]; // a from batch 0 (subject)
        let a_global_b1 = mapping_1.so_map[0]; // a from batch 1 (object)
        assert_eq!(a_global_b0, a_global_b1, "a should map to same global ID whether it's subject or object");

        // Verify that b appears as shared (not just in one role)
        let b_so_id = mapping_1.so_map[1]; // b as subject/object from batch 1
        assert!(b_so_id <= result.counts.shared as u64, "b's SO ID should be in shared section");

        // Verify d is subject-only (not shared)
        let d_id = mapping_2.so_map[1];
        assert!(d_id > result.counts.shared as u64, "d should have subject-only ID");

        Ok(())
    }

    /// Test merging with empty batch (edge case).
    #[test]
    fn test_merge_with_empty_batch() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Batch 0: term
        let batch0_path = temp_path.join("batch0.vocab.zst");
        create_test_partial_vocab(
            &batch0_path,
            vec![("term", Roles::SUBJECT, Some(0), None)],
        )?;

        // Batch 1: empty
        let batch1_path = temp_path.join("batch1.vocab.zst");
        create_test_partial_vocab(&batch1_path, vec![])?;

        let batch_infos = vec![(0, batch0_path), (1, batch1_path)];
        let result = merge_vocabularies(batch_infos, temp_path, TEST_MEMORY_BUDGET)?;

        // Should only count terms from batch 0
        assert_eq!(result.counts.shared, 0);
        assert_eq!(result.counts.subjects, 1);

        let mapping_0 = read_mapping_from_temp(temp_path, 0)?;
        let mapping_1 = read_mapping_from_temp(temp_path, 1)?;

        // Batch 0 should have a mapping
        assert_eq!(mapping_0.so_map.len(), 1);

        // Batch 1 might have pre-allocated but empty mapping (header said max_so_id = 0, max_p_id = 0)
        // Verify mapping files exist and are readable
        assert_eq!(mapping_1.batch_id, 1);

        Ok(())
    }

    #[test]
    fn test_parallel_pass2_stream_tiny_capacity_ordering() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        let batch0_path = temp_path.join("batch0.vocab.zst");
        create_test_partial_vocab(
            &batch0_path,
            vec![
                ("a", Roles::SUBJECT, Some(0), None),
                ("c", Roles::SUBJECT, Some(1), None),
            ],
        )?;

        let batch1_path = temp_path.join("batch1.vocab.zst");
        create_test_partial_vocab(
            &batch1_path,
            vec![
                ("b", Roles::SUBJECT, Some(0), None),
                ("d", Roles::SUBJECT, Some(1), None),
            ],
        )?;

        let batch_infos = vec![(0usize, batch0_path), (1usize, batch1_path)];
        let handle = build_vocab_merge_tree(&batch_infos, 1, temp_path)?;

        let mut observed_terms: Vec<String> = Vec::new();
        let mut observed_batches: Vec<usize> = Vec::new();
        while let Ok(item) = handle.rx.recv() {
            let entry = item?;
            observed_terms.push(String::from_utf8(entry.term).expect("test terms should be valid UTF-8"));
            observed_batches.push(entry.source_batch);
        }

        handle.join()?;

        assert_eq!(observed_terms, vec!["a", "b", "c", "d"]);
        assert_eq!(observed_batches, vec![0, 1, 0, 1]);

        Ok(())
    }

    #[test]
    fn test_parallel_pass2_stream_missing_file_propagates_error() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let missing_path = temp_dir.path().join("does_not_exist.vocab.zst");
        let batch_infos = vec![(42usize, missing_path)];

        let handle = build_vocab_merge_tree(&batch_infos, 2, temp_dir.path())?;

        let first = handle.rx.recv().expect("expected an error item from missing-file source");
        let err = first.expect_err("expected error result for missing partial vocab file");
        assert!(
            err.to_string().contains("Failed to open partial vocab for batch 42"),
            "unexpected error: {err}"
        );

        handle.join()?;
        Ok(())
    }
}
