//! K-way merge of partial vocabularies into global dictionary with ID mappings.

use crate::dictionary::pfc::PfcEncoder;
use crate::dictionary::DictCounts;
use crate::pipeline::{partial_vocab_path, PartialVocabEntry, PartialVocabReader};
use anyhow::{Context, Result};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use super::batch_vocab::{ROLE_GRAPH, ROLE_OBJECT, ROLE_PREDICATE, ROLE_SUBJECT};

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
    roles: u8,
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
    let mut merged_roles: u8 = 0;

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
            let is_subject = merged_roles & ROLE_SUBJECT != 0;
            let is_predicate = merged_roles & ROLE_PREDICATE != 0;
            let is_object = merged_roles & ROLE_OBJECT != 0;

            if is_predicate {
                counts.predicates += 1;
            }
            if is_subject && is_object {
                counts.shared += 1;
            } else if is_subject {
                counts.subjects += 1;
            } else if is_object {
                counts.objects += 1;
            }

            merged_roles = 0;
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
        let is_subject = merged_roles & ROLE_SUBJECT != 0;
        let is_predicate = merged_roles & ROLE_PREDICATE != 0;
        let is_object = merged_roles & ROLE_OBJECT != 0;

        if is_predicate {
            counts.predicates += 1;
        }
        if is_subject && is_object {
            counts.shared += 1;
        } else if is_subject {
            counts.subjects += 1;
        } else if is_object {
            counts.objects += 1;
        }
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
    merged_roles = 0;
    let mut batches_with_term: Vec<(usize, u8, Option<u32>, Option<u32>)> = Vec::new();

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
            merged_roles = 0;
        }

        // Accumulate roles for current term
        merged_roles |= roles_in_batch;
        batches_with_term.push((source_batch, roles_in_batch, so_local_id, p_local_id));
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

/// Assign global IDs and record mappings for a single term.
fn assign_global_ids_and_record_mappings(
    term: &[u8],
    roles: u8,
    batches: &[(usize, u8, Option<u32>, Option<u32>)], // (batch_id, roles, so_local_id, p_local_id)
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

    let is_subject = roles & ROLE_SUBJECT != 0;
    let is_predicate = roles & ROLE_PREDICATE != 0;
    let is_object = roles & ROLE_OBJECT != 0;

    // Handle predicates (separate ID space)
    if is_predicate {
        section_counts.predicates += 1;
        let global_pred_id = section_counts.predicates;
        predicates_enc.push(term_str);
        predicate_ids.insert(term_str.to_string(), global_pred_id);

        // Record mapping for each batch that had this predicate
        for &(batch_id, batch_roles, _so_local_id, p_local_id) in batches {
            if batch_roles & ROLE_PREDICATE != 0 {
                if let Some(local_p_id) = p_local_id {
                    id_mappings[batch_id].p_map[local_p_id as usize] = global_pred_id;
                }
            }
        }
    }

    // Handle subjects/objects (shared ID space)
    if is_subject || is_object {
        let global_so_id = if is_subject && is_object {
            // Shared: appears as both subject and object
            section_counts.shared += 1;
            shared_enc.push(term_str);
            section_counts.shared
        } else if is_subject {
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
        for &(batch_id, batch_roles, so_local_id, _p_local_id) in batches {
            if (batch_roles & ROLE_SUBJECT != 0) || (batch_roles & ROLE_OBJECT != 0) {
                if let Some(local_so_id) = so_local_id {
                    id_mappings[batch_id].so_map[local_so_id as usize] = global_so_id;
                }
            }
        }
    }

    Ok(())
}
