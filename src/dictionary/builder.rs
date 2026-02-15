//! Dictionary builder: extracts terms from RDF, sorts, partitions, and encodes to PFC.
//!
//! This implements the multi-pass dictionary construction:
//! Pass 1: Extract terms with roles, external sort, deduplicate, partition, assign IDs
//! Output: PFC-encoded dictionary sections + SST for term-to-ID lookup

use crate::dictionary::pfc::PfcEncoder;
use crate::dictionary::sst::{DictSection, SstReader, SstWriter, TermId};
use crate::rdf::{stream_quads, RdfInput};
use crate::sort::{ExternalSorter, Sortable};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;

/// Role flags for terms (which positions they appear in).
const ROLE_SUBJECT: u8 = 0x01;
const ROLE_PREDICATE: u8 = 0x02;
const ROLE_OBJECT: u8 = 0x04;
const ROLE_GRAPH: u8 = 0x08;

/// A term with its accumulated role flags, used during external sort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermRecord {
    pub term: String,
    pub roles: u8,
}

impl PartialOrd for TermRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TermRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.term.cmp(&other.term)
    }
}

impl Sortable for TermRecord {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        let term_bytes = self.term.as_bytes();
        let len = term_bytes.len() as u32;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(term_bytes)?;
        writer.write_all(&[self.roles])?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut term_buf = vec![0u8; len];
        reader.read_exact(&mut term_buf)?;

        let mut role_buf = [0u8; 1];
        reader.read_exact(&mut role_buf)?;

        let term = String::from_utf8(term_buf)
            .context("Invalid UTF-8 in term record")?;

        Ok(Some(TermRecord {
            term,
            roles: role_buf[0],
        }))
    }

    fn mem_size(&self) -> usize {
        self.term.len() + std::mem::size_of::<Self>()
    }
}

/// Counts for each dictionary section.
#[derive(Debug, Default, Clone)]
pub struct DictCounts {
    pub shared: u64,
    pub subjects: u64,
    pub predicates: u64,
    pub objects: u64,
    #[allow(dead_code)]
    pub graphs: u64,
}

/// Result of dictionary construction.
pub struct DictBuildResult {
    /// PFC-encoded sections, in order: shared, subjects, predicates, objects
    pub sections: Vec<Vec<u8>>,
    /// Section counts
    pub counts: DictCounts,
    /// SST reader for subject/object term-to-ID lookups in Pass 2
    pub sst: SstReader,
    /// Predicate term-to-ID map (predicates have their own ID space, kept in memory)
    pub predicate_ids: HashMap<String, u64>,
}

/// Build the dictionary from RDF inputs.
///
/// This runs Pass 1:
/// 1. Stream all inputs, extract terms with role flags
/// 2. External sort and deduplicate terms
/// 3. Partition into shared/subjects/predicates/objects
/// 4. PFC-encode each section
/// 5. Build SST for Pass 2 lookups
pub fn build_dictionary(
    inputs: &[RdfInput],
    temp_dir: &Path,
    memory_budget: usize,
    include_graphs: bool,
    base_uri: Option<&str>,
    sst_block_size: Option<usize>,
) -> Result<DictBuildResult> {
    tracing::info!("Pass 1: Extracting and sorting terms...");

    // Step 1: Extract terms and external sort
    let mut sorter = ExternalSorter::new(temp_dir, memory_budget);
    let mut buffer = Vec::new();
    let mut mem_used = 0;
    let mut total_quads = 0u64;
    let mut total_errors = 0u64;

    for (file_index, input) in inputs.iter().enumerate() {
        tracing::info!("  Reading {}", input.path.display());

        let stats = stream_quads(input, file_index, base_uri, |quad| {
            // Subject
            sorter.push(
                TermRecord { term: quad.subject.clone(), roles: ROLE_SUBJECT },
                &mut buffer,
                &mut mem_used,
            )?;

            // Predicate
            sorter.push(
                TermRecord { term: quad.predicate.clone(), roles: ROLE_PREDICATE },
                &mut buffer,
                &mut mem_used,
            )?;

            // Object
            sorter.push(
                TermRecord { term: quad.object.clone(), roles: ROLE_OBJECT },
                &mut buffer,
                &mut mem_used,
            )?;

            // Graph (if quads mode)
            if include_graphs {
                if let Some(ref graph) = quad.graph {
                    sorter.push(
                        TermRecord { term: graph.clone(), roles: ROLE_GRAPH },
                        &mut buffer,
                        &mut mem_used,
                    )?;
                }
            }

            Ok(())
        })?;

        total_quads += stats.quads;
        total_errors += stats.errors;
    }

    tracing::info!(
        "Pass 1a complete: {} quads extracted, {} errors skipped",
        total_quads,
        total_errors
    );

    // Step 2: Merge sorted chunks, deduplicating and merging role flags
    let merged = sorter.finish(&mut buffer)?;

    tracing::info!("Pass 1b: Merging and deduplicating terms...");

    // Step 3: Deduplicate and partition
    let mut shared_enc = PfcEncoder::new();
    let mut subjects_enc = PfcEncoder::new();
    let mut predicates_enc = PfcEncoder::new();
    let mut objects_enc = PfcEncoder::new();

    let mut counts = DictCounts::default();
    let mut predicate_ids: HashMap<String, u64> = HashMap::new();

    let mut prev_term: Option<String> = None;
    let mut prev_roles: u8 = 0;

    // Buffer terms to write to SST after we know total count
    let mut sst_entries: Vec<(Vec<u8>, DictSection, u64)> = Vec::new();

    // Closure to process a completed (deduplicated) term.
    // Predicates go into the in-memory HashMap (separate ID space).
    // Subjects/objects are buffered for SST (one entry per term, no duplicates).
    let process_term = |term: &str, roles: u8, counts: &mut DictCounts,
                            sst_entries: &mut Vec<(Vec<u8>, DictSection, u64)>,
                            predicate_ids: &mut HashMap<String, u64>,
                            shared_enc: &mut PfcEncoder, subjects_enc: &mut PfcEncoder,
                            predicates_enc: &mut PfcEncoder, objects_enc: &mut PfcEncoder| -> Result<()> {
        let is_subject = roles & ROLE_SUBJECT != 0;
        let is_predicate = roles & ROLE_PREDICATE != 0;
        let is_object = roles & ROLE_OBJECT != 0;

        // Predicates: separate ID space, stored in memory
        if is_predicate {
            counts.predicates += 1;
            predicates_enc.push(term);
            predicate_ids.insert(term.to_string(), counts.predicates);
        }

        // Subjects/objects: Buffer for SST (each term appears at most once)
        if is_subject && is_object {
            counts.shared += 1;
            shared_enc.push(term);
            sst_entries.push((term.as_bytes().to_vec(), DictSection::Shared, counts.shared));
        } else if is_subject {
            counts.subjects += 1;
            subjects_enc.push(term);
            sst_entries.push((term.as_bytes().to_vec(), DictSection::Subjects, counts.subjects));
        } else if is_object {
            counts.objects += 1;
            objects_enc.push(term);
            sst_entries.push((term.as_bytes().to_vec(), DictSection::Objects, counts.objects));
        }

        Ok(())
    };

    for result in merged {
        let record = result?;

        match &prev_term {
            Some(prev) if prev == &record.term => {
                // Same term: merge role flags
                prev_roles |= record.roles;
            }
            _ => {
                // New term: process the previous one
                if let Some(ref prev) = prev_term {
                    process_term(
                        prev, prev_roles, &mut counts, &mut sst_entries, &mut predicate_ids,
                        &mut shared_enc, &mut subjects_enc, &mut predicates_enc, &mut objects_enc,
                    )?;
                }
                prev_term = Some(record.term);
                prev_roles = record.roles;
            }
        }
    }

    // Process the last term
    if let Some(ref prev) = prev_term {
        process_term(
            prev, prev_roles, &mut counts, &mut sst_entries, &mut predicate_ids,
            &mut shared_enc, &mut subjects_enc, &mut predicates_enc, &mut objects_enc,
        )?;
    }

    tracing::info!(
        "Dictionary: {} shared, {} subjects, {} predicates, {} objects",
        counts.shared,
        counts.subjects,
        counts.predicates,
        counts.objects
    );

    // Step 3b: Write SST with optimal block size
    let total_sst_terms = sst_entries.len() as u64;
    let block_size = sst_block_size.unwrap_or_else(|| {
        let size = crate::dictionary::sst::optimal_block_size(total_sst_terms);
        let est_mem_mb = (total_sst_terms / size as u64) * 50 / 1_000_000;
        tracing::info!(
            "Auto-selected SST block size: {} (~{}MB index for {} terms)",
            size, est_mem_mb, total_sst_terms
        );
        size
    });

    let sst_path = temp_dir.join("term_lookup.sst");
    let mut sst_writer = SstWriter::new(&sst_path, block_size)?;

    for (key, section, id) in sst_entries {
        sst_writer.write_entry(&key, section, id)?;
    }

    // Step 4: Encode PFC sections
    let mut sections = Vec::with_capacity(4);
    for (name, enc) in [
        ("shared", &shared_enc),
        ("subjects", &subjects_enc),
        ("predicates", &predicates_enc),
        ("objects", &objects_enc),
    ] {
        let mut section_buf = Vec::new();
        enc.write_to(&mut section_buf)?;
        tracing::debug!("  {} section: {} strings, {} bytes", name, enc.len(), section_buf.len());
        sections.push(section_buf);
    }

    // Step 5: Finalize SST
    let (sst_path, sst_index) = sst_writer.finish()?;
    let sst = SstReader::open(sst_path, sst_index)?;

    Ok(DictBuildResult {
        sections,
        counts,
        sst,
        predicate_ids,
    })
}

/// Resolve a term's global ID given its SST lookup result and dictionary counts.
///
/// For subjects: shared terms use their shared ID directly (1..m),
///               subject-only terms use shared_count + local_id.
/// For objects:  shared terms use their shared ID directly (1..m),
///               object-only terms use shared_count + local_id.
/// Predicates have their own ID space (1..p).
pub fn resolve_global_id(term_id: &TermId, counts: &DictCounts, _is_subject: bool) -> u64 {
    match term_id.section {
        DictSection::Shared => term_id.local_id,
        DictSection::Subjects => counts.shared + term_id.local_id,
        DictSection::Objects => counts.shared + term_id.local_id,
        DictSection::Predicates => term_id.local_id,
        DictSection::Graphs => term_id.local_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_record_sortable() {
        let records = vec![
            TermRecord { term: "c".into(), roles: ROLE_SUBJECT },
            TermRecord { term: "a".into(), roles: ROLE_OBJECT },
            TermRecord { term: "b".into(), roles: ROLE_PREDICATE },
        ];

        let mut sorted = records.clone();
        sorted.sort();

        assert_eq!(sorted[0].term, "a");
        assert_eq!(sorted[1].term, "b");
        assert_eq!(sorted[2].term, "c");
    }

    #[test]
    fn test_term_record_roundtrip() {
        let record = TermRecord {
            term: "http://example.org/test".into(),
            roles: ROLE_SUBJECT | ROLE_OBJECT,
        };

        let mut buf = Vec::new();
        record.write_to(&mut buf).unwrap();

        let mut cursor = std::io::Cursor::new(&buf);
        let decoded = TermRecord::read_from(&mut cursor).unwrap().unwrap();

        assert_eq!(decoded.term, record.term);
        assert_eq!(decoded.roles, record.roles);
    }

    #[test]
    fn test_resolve_global_id() {
        let counts = DictCounts {
            shared: 100,
            subjects: 50,
            predicates: 20,
            objects: 30,
            graphs: 0,
        };

        // Shared term with local_id=5 -> global 5
        let tid = TermId { section: DictSection::Shared, local_id: 5 };
        assert_eq!(resolve_global_id(&tid, &counts, true), 5);

        // Subject-only with local_id=3 -> global 100+3=103
        let tid = TermId { section: DictSection::Subjects, local_id: 3 };
        assert_eq!(resolve_global_id(&tid, &counts, true), 103);

        // Object-only with local_id=7 -> global 100+7=107
        let tid = TermId { section: DictSection::Objects, local_id: 7 };
        assert_eq!(resolve_global_id(&tid, &counts, false), 107);

        // Predicate with local_id=2 -> 2 (own space)
        let tid = TermId { section: DictSection::Predicates, local_id: 2 };
        assert_eq!(resolve_global_id(&tid, &counts, true), 2);
    }
}
