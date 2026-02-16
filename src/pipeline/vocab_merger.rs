//! K-way merge of partial vocabularies into global dictionary with ID mappings.

use crate::dictionary::pfc::PfcEncoder;
use crate::dictionary::DictCounts;
use crate::pipeline::PartialVocabReader;
use anyhow::{Context, Result};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

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

/// Heap entry for k-way merge.
struct HeapEntry {
    term: Vec<u8>,
    roles: Roles,
    so_local_id: Option<u32>,
    p_local_id: Option<u32>,
    source_batch: usize,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.term == other.term
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap
        other.term.cmp(&self.term)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Result of vocabulary merge.
pub struct VocabMergeResult {
    pub dict_sections: Vec<Vec<u8>>, // PFC-encoded: [shared, subjects, predicates, objects]
    pub counts: DictCounts,
    #[allow(dead_code)]
    pub predicate_ids: HashMap<String, u64>,
    #[allow(dead_code)]
    pub id_mappings: Vec<IdMapping>,
}

/// Merge partial vocabularies into global dictionary.
pub fn merge_vocabularies(
    batch_infos: Vec<(usize, PathBuf)>, // (batch_id, vocab_path)
    temp_dir: &Path,
) -> Result<VocabMergeResult> {
    tracing::info!("Merging {} partial vocabularies", batch_infos.len());

    // Open readers for all batches
    let mut readers: Vec<Option<PartialVocabReader>> = Vec::new();
    let mut merge_heap = BinaryHeap::new();
    let mut id_mappings: Vec<IdMapping> = Vec::new();

    for (batch_id, vocab_path) in &batch_infos {
        tracing::debug!("Opening partial vocab for batch {}: {:?}", batch_id, vocab_path);
        let mut reader = PartialVocabReader::open(vocab_path)
            .with_context(|| format!("Failed to open partial vocab for batch {}", batch_id))?;

        // Pre-allocate ID mappings based on max IDs from this batch
        let max_so_id = reader.max_so_id();
        let max_p_id = reader.max_p_id();
        let mut mapping = IdMapping::new(*batch_id);
        mapping.so_map = vec![0u64; (max_so_id + 1) as usize];
        mapping.p_map = vec![0u64; (max_p_id + 1) as usize];
        id_mappings.push(mapping);

        // Read first entry and push to heap
        match reader.read_entry() {
            Ok(Some(entry)) => {
                tracing::debug!(
                    "Batch {}: first entry: {:?}, roles: {:02x}",
                    batch_id,
                    String::from_utf8_lossy(&entry.term),
                    entry.roles
                );
                merge_heap.push(HeapEntry {
                    term: entry.term.clone(),
                    roles: entry.roles,
                    so_local_id: entry.so_local_id,
                    p_local_id: entry.p_local_id,
                    source_batch: *batch_id,
                });
            }
            Ok(None) => {
                tracing::warn!("Batch {}: no entries in partial vocab", batch_id);
            }
            Err(e) => {
                tracing::error!("Batch {}: error reading first entry: {}", batch_id, e);
                return Err(e);
            }
        }

        readers.push(Some(reader));
    }

    // Initialize PFC encoders for each section
    let mut shared_enc = PfcEncoder::new();
    let mut subjects_enc = PfcEncoder::new();
    let mut predicates_enc = PfcEncoder::new();
    let mut objects_enc = PfcEncoder::new();

    let mut counts = DictCounts::default();
    let mut predicate_ids = HashMap::new();

    // Two-pass streaming merge for constant memory usage:
    // Pass 1: Count terms in each section to determine offsets
    // Pass 2: Re-stream and assign global IDs with correct offsets

    // === PASS 1: Count terms per section ===
    tracing::debug!("Pass 1: Counting terms per section");
    let mut current_term: Option<Vec<u8>> = None;
    let mut merged_roles = Roles::empty();

    while let Some(heap_entry) = merge_heap.pop() {
        let term = heap_entry.term;
        let source_batch = heap_entry.source_batch;
        let roles_in_batch = heap_entry.roles;

        // Check if this is the same term as previous or a new term
        let is_same_term = current_term
            .as_ref()
            .map_or(false, |prev| prev == &term);

        if !is_same_term && current_term.is_some() {
            // Count completed term
            count_term_section(&merged_roles, &mut counts);
            merged_roles = Roles::empty();
        }

        // Accumulate roles for current term
        merged_roles |= roles_in_batch;
        current_term = Some(term);

        // Fetch next entry from same source
        if let Some(Some(reader)) = readers.get_mut(source_batch) {
            if let Some(next_entry) = reader.read_entry()? {
                merge_heap.push(HeapEntry {
                    term: next_entry.term.clone(),
                    roles: next_entry.roles,
                    so_local_id: next_entry.so_local_id,
                    p_local_id: next_entry.p_local_id,
                    source_batch,
                });
            }
        }
    }

    // Count final term
    if current_term.is_some() {
        count_term_section(&merged_roles, &mut counts);
    }

    tracing::debug!("Counts: {} shared, {} subjects, {} predicates, {} objects",
                    counts.shared, counts.subjects, counts.predicates, counts.objects);

    // === PASS 2: Re-open readers and assign global IDs ===
    tracing::debug!("Pass 2: Assigning global IDs with correct offsets");

    // Re-open all partial vocab files
    readers.clear();
    merge_heap.clear();

    for (batch_id, vocab_path) in &batch_infos {
        let mut reader = PartialVocabReader::open(vocab_path)
            .with_context(|| format!("Failed to re-open partial vocab for batch {}", batch_id))?;

        // Read first entry and push to heap
        if let Some(entry) = reader.read_entry()? {
            merge_heap.push(HeapEntry {
                term: entry.term.clone(),
                roles: entry.roles,
                so_local_id: entry.so_local_id,
                p_local_id: entry.p_local_id,
                source_batch: *batch_id,
            });
        }
        readers.push(Some(reader));
    }

    // Second k-way merge: assign global IDs
    let mut section_counts = DictCounts::default();  // Track current position in each section
    current_term = None;
    merged_roles = Roles::empty();
    let mut batches_with_term: Vec<TermBatchInfo> = Vec::new();

    while let Some(heap_entry) = merge_heap.pop() {
        let term = heap_entry.term;
        let source_batch = heap_entry.source_batch;
        let roles_in_batch = heap_entry.roles;
        let so_local_id = heap_entry.so_local_id;
        let p_local_id = heap_entry.p_local_id;

        // Check if this is the same term as previous or a new term
        let is_same_term = current_term
            .as_ref()
            .map_or(false, |prev| prev == &term);

        if !is_same_term && current_term.is_some() {
            // Process completed term
            assign_global_ids_and_record_mappings(
                current_term.as_ref().unwrap(),
                merged_roles,
                &batches_with_term,
                &counts,  // Use final counts for offset calculation
                &mut section_counts,  // Track current position
                &mut shared_enc,
                &mut subjects_enc,
                &mut predicates_enc,
                &mut objects_enc,
                &mut predicate_ids,
                &mut id_mappings,
            )?;

            batches_with_term.clear();
            merged_roles = Roles::empty();
        }

        // Accumulate roles for current term
        merged_roles |= roles_in_batch;
        batches_with_term.push(TermBatchInfo {
            batch_id: source_batch,
            roles: roles_in_batch,
            so_local_id,
            p_local_id,
        });
        current_term = Some(term);

        // Fetch next entry from same source
        if let Some(Some(reader)) = readers.get_mut(source_batch) {
            if let Some(next_entry) = reader.read_entry()? {
                merge_heap.push(HeapEntry {
                    term: next_entry.term.clone(),
                    roles: next_entry.roles,
                    so_local_id: next_entry.so_local_id,
                    p_local_id: next_entry.p_local_id,
                    source_batch,
                });
            }
        }
    }

    // Process final term
    if let Some(term) = current_term {
        assign_global_ids_and_record_mappings(
            &term,
            merged_roles,
            &batches_with_term,
            &counts,
            &mut section_counts,
            &mut shared_enc,
            &mut subjects_enc,
            &mut predicates_enc,
            &mut objects_enc,
            &mut predicate_ids,
            &mut id_mappings,
        )?;
    }

    tracing::info!(
        "Merged vocabulary: {} shared, {} subjects, {} predicates, {} objects",
        counts.shared,
        counts.subjects,
        counts.predicates,
        counts.objects
    );

    // Encode dictionary sections
    let mut dict_sections = Vec::new();

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

    // Write ID mappings to files
    for mapping in &id_mappings {
        let mapping_path = temp_dir.join(format!("id_mapping_{:06}.map.zst", mapping.batch_id));
        mapping.write_to_file(&mapping_path)
            .with_context(|| format!("Failed to write ID mapping for batch {}", mapping.batch_id))?;
        tracing::debug!(
            "Wrote ID mapping for batch {}: {} SO entries, {} P entries",
            mapping.batch_id,
            mapping.so_map.len(),
            mapping.p_map.len()
        );
    }

    Ok(VocabMergeResult {
        dict_sections,
        counts,
        predicate_ids,
        id_mappings,
    })
}

/// Count a completed term into the appropriate dictionary section.
fn count_term_section(roles: &Roles, counts: &mut DictCounts) {
    if roles.contains(Roles::PREDICATE) {
        counts.predicates += 1;
    }
    if roles.contains(Roles::SUBJECT) && roles.contains(Roles::OBJECT) {
        counts.shared += 1;
    } else if roles.contains(Roles::SUBJECT) {
        counts.subjects += 1;
    } else if roles.contains(Roles::OBJECT) {
        counts.objects += 1;
    }
}

/// Assign global IDs and record mappings for a single term.
fn assign_global_ids_and_record_mappings(
    term: &[u8],
    roles: Roles,
    batches: &[TermBatchInfo],
    final_counts: &DictCounts,  // Final section counts for offset calculation
    section_counts: &mut DictCounts,  // Current position in each section
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
        section_counts.predicates += 1;
        let global_pred_id = section_counts.predicates;
        predicates_enc.push(term_str);
        predicate_ids.insert(term_str.to_string(), global_pred_id);

        // Record mapping for each batch that had this predicate
        for info in batches {
            if info.roles.contains(Roles::PREDICATE) {
                if let Some(local_p_id) = info.p_local_id {
                    id_mappings[info.batch_id].p_map[local_p_id as usize] = global_pred_id;
                }
            }
        }
    }

    // Handle subjects/objects (shared ID space)
    if roles.intersects(Roles::SUBJECT | Roles::OBJECT) {
        let global_so_id = if roles.contains(Roles::SUBJECT | Roles::OBJECT) {
            // Shared: appears as both subject and object
            section_counts.shared += 1;
            shared_enc.push(term_str);
            section_counts.shared
        } else if roles.contains(Roles::SUBJECT) {
            // Subject-only: offset by total shared count
            section_counts.subjects += 1;
            subjects_enc.push(term_str);
            final_counts.shared + section_counts.subjects
        } else {
            // Object-only: offset by total shared count
            section_counts.objects += 1;
            objects_enc.push(term_str);
            final_counts.shared + section_counts.objects
        };

        // Record mapping for each batch that had this subject/object
        for info in batches {
            if info.roles.intersects(Roles::SUBJECT | Roles::OBJECT) {
                if let Some(local_so_id) = info.so_local_id {
                    id_mappings[info.batch_id].so_map[local_so_id as usize] = global_so_id;
                }
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
        let result = merge_vocabularies(batch_infos, temp_path)?;

        // Verify counts
        assert_eq!(result.counts.shared, 0);
        assert_eq!(result.counts.subjects, 2);
        assert_eq!(result.counts.objects, 2);
        assert_eq!(result.counts.predicates, 2);

        // Verify ID mappings are correct
        assert_eq!(result.id_mappings.len(), 2);

        // Batch 0 mappings: a→1 (first subject), b→1 (first object, same ID offset), c→1 (first predicate)
        assert_eq!(result.id_mappings[0].so_map[0], 1); // a (first subject-only)
        assert_eq!(result.id_mappings[0].so_map[1], 1); // b (first object-only)
        assert_eq!(result.id_mappings[0].p_map[0], 1);  // c (first predicate)

        // Batch 1 mappings: d→2 (second subject), e→2 (second object, same ID offset), f→2 (second predicate)
        assert_eq!(result.id_mappings[1].so_map[0], 2); // d (second subject-only)
        assert_eq!(result.id_mappings[1].so_map[1], 2); // e (second object-only)
        assert_eq!(result.id_mappings[1].p_map[0], 2);  // f (second predicate)

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
        let result = merge_vocabularies(batch_infos, temp_path)?;

        // "x" should be shared (appears as both subject and object)
        // "p1" should be a predicate
        assert_eq!(result.counts.shared, 1); // x
        assert_eq!(result.counts.subjects, 0);
        assert_eq!(result.counts.objects, 0);
        assert_eq!(result.counts.predicates, 1); // p1

        // Both batches should map "x" to the same global ID (1, the shared ID)
        assert_eq!(result.id_mappings[0].so_map[0], 1); // x from batch 0
        assert_eq!(result.id_mappings[1].so_map[0], 1); // x from batch 1

        // Both batches should map "p1" to the same global predicate ID (1)
        assert_eq!(result.id_mappings[0].p_map[0], 1); // p1 from batch 0
        assert_eq!(result.id_mappings[1].p_map[0], 1); // p1 from batch 1

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
        let result = merge_vocabularies(batch_infos, temp_path)?;

        // "multi" should be shared (appears as both subject and object)
        // and also as predicate
        assert_eq!(result.counts.shared, 1);
        assert_eq!(result.counts.predicates, 1);

        // Verify mappings for each batch
        // Batch 0: "multi" as subject → global ID 1 (shared section starts at 1)
        assert_eq!(result.id_mappings[0].so_map[0], 1);
        // Batch 1: "multi" as predicate → global ID 1
        assert_eq!(result.id_mappings[1].p_map[0], 1);
        // Batch 2: "multi" as object → global ID 1 (shared)
        assert_eq!(result.id_mappings[2].so_map[0], 1);

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
        let result = merge_vocabularies(batch_infos, temp_path)?;

        // "a" shared (subject + object), "b" shared (subject + object) + predicate, "c" predicate, "d" subject
        assert_eq!(result.counts.shared, 2);     // a, b
        assert_eq!(result.counts.subjects, 1);   // d
        assert_eq!(result.counts.predicates, 2); // b, c

        // Verify ID mappings are consistent across batches
        let a_global_b0 = result.id_mappings[0].so_map[0]; // a from batch 0 (subject)
        let a_global_b1 = result.id_mappings[1].so_map[0]; // a from batch 1 (object)
        assert_eq!(a_global_b0, a_global_b1, "a should map to same global ID whether it's subject or object");

        // Verify that b appears as shared (not just in one role)
        let b_so_id = result.id_mappings[1].so_map[1]; // b as subject/object from batch 1
        assert!(b_so_id <= result.counts.shared as u64, "b's SO ID should be in shared section");

        // Verify d is subject-only (not shared)
        let d_id = result.id_mappings[2].so_map[1];
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
        let result = merge_vocabularies(batch_infos, temp_path)?;

        // Should only count terms from batch 0
        assert_eq!(result.counts.shared, 0);
        assert_eq!(result.counts.subjects, 1);

        // Batch 0 should have a mapping
        assert_eq!(result.id_mappings[0].so_map.len(), 1);

        // Batch 1 might have pre-allocated but empty mapping (header said max_so_id = 0, max_p_id = 0)
        // Just verify it exists and isn't panicking
        assert_eq!(result.id_mappings.len(), 2);

        Ok(())
    }
}
