//! Parallel ID remapping from local to global IDs.

use crate::hdt::input_adapter::HdtTripleReader;
use crate::pipeline::batch_vocab::LocalIdTriple;
use crate::pipeline::vocab_merger::IdMapping;
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

/// Source of local-ID triples for remapping.
pub enum TripleSource {
    /// Standard local-ID triples file (zstd-compressed `.ltr.zst`).
    LtrFile(PathBuf),
    /// HDT BitmapTriples — read streaming from the HDT file.
    HdtFile {
        path: PathBuf,
        triples_data_offset: u64,
        num_triples: u64,
        num_sp_pairs: u64,
        shared_count: u64,
        subjects_count: u64,
    },
}

/// Information about a batch for remapping.
pub struct BatchRemapInfo {
    pub batch_id: usize,
    pub triple_source: TripleSource,
    pub mapping_path: PathBuf,
}

/// Remap a single batch: local IDs → global IDs.
fn remap_batch(
    batch_info: &BatchRemapInfo,
    global_triple_tx: &Sender<Vec<IdTriple>>,
    chunk_size: usize,
) -> Result<usize> {
    // Load ID mapping
    let mapping = IdMapping::read_from_file(&batch_info.mapping_path)
        .with_context(|| format!("Failed to load ID mapping for batch {}", batch_info.batch_id))?;

    let mut count = 0;
    let mut chunk: Vec<IdTriple> = Vec::with_capacity(chunk_size);

    let mut remap_and_send = |s: u32, p: u32, o: u32| -> Result<()> {
        let global_subject = *mapping.so_map.get(s as usize).ok_or_else(|| {
            anyhow::anyhow!(
                "SO mapping missing for local subject ID {} in batch {} (map size: {})",
                s,
                batch_info.batch_id,
                mapping.so_map.len()
            )
        })?;
        let global_predicate = *mapping.p_map.get(p as usize).ok_or_else(|| {
            anyhow::anyhow!(
                "P mapping missing for local predicate ID {} in batch {} (map size: {})",
                p,
                batch_info.batch_id,
                mapping.p_map.len()
            )
        })?;
        let global_object = *mapping.so_map.get(o as usize).ok_or_else(|| {
            anyhow::anyhow!(
                "SO mapping missing for local object ID {} in batch {} (map size: {})",
                o,
                batch_info.batch_id,
                mapping.so_map.len()
            )
        })?;

        chunk.push(IdTriple {
            subject: global_subject,
            predicate: global_predicate,
            object: global_object,
        });
        if chunk.len() >= chunk_size {
            let chunk_to_send = std::mem::take(&mut chunk);
            if global_triple_tx.send(chunk_to_send).is_err() {
                anyhow::bail!("Global triple receiver disconnected");
            }
            chunk = Vec::with_capacity(chunk_size);
        }
        count += 1;
        Ok(())
    };

    match &batch_info.triple_source {
        TripleSource::LtrFile(triples_path) => {
            let file = File::open(triples_path).with_context(|| {
                format!(
                    "Failed to open triples file for batch {}",
                    batch_info.batch_id
                )
            })?;
            let buf_reader = BufReader::new(file);
            let mut decoder = zstd::Decoder::with_buffer(buf_reader)?;

            loop {
                match LocalIdTriple::read_from(&mut decoder) {
                    Ok(Some(t)) => remap_and_send(t.subject, t.predicate, t.object)?,
                    Ok(None) => break,
                    Err(e) => {
                        return Err(e).with_context(|| {
                            format!(
                                "Failed to read local triple from batch {}",
                                batch_info.batch_id
                            )
                        });
                    }
                }
            }
        }
        TripleSource::HdtFile {
            path,
            triples_data_offset,
            num_triples,
            num_sp_pairs,
            shared_count,
            subjects_count,
        } => {
            let mut reader = HdtTripleReader::open(
                path,
                *triples_data_offset,
                *num_triples,
                *num_sp_pairs,
                *shared_count,
                *subjects_count,
            )
            .with_context(|| {
                format!(
                    "Failed to open HDT triples for batch {}",
                    batch_info.batch_id
                )
            })?;

            while let Some((s, p, o)) = reader.next_triple()? {
                remap_and_send(s, p, o)?;
            }
            reader.finalize().with_context(|| {
                format!(
                    "HDT triples CRC verification failed for batch {}",
                    batch_info.batch_id
                )
            })?;
        }
    }

    if !chunk.is_empty() && global_triple_tx.send(chunk).is_err() {
        anyhow::bail!("Global triple receiver disconnected");
    }

    Ok(count)
}

/// Stage 5: Parallel ID remapper.
///
/// Processes batches in parallel, remapping local IDs to global IDs.
pub fn id_remapper_stage(
    batch_remap_rx: Receiver<BatchRemapInfo>,
    global_triple_tx: Sender<Vec<IdTriple>>,
    num_threads: usize,
    chunk_size: usize,
) -> Result<u64> {
    anyhow::ensure!(chunk_size > 0, "chunk_size must be greater than 0");

    // Use rayon to process batches in parallel
    let batches: Vec<_> = batch_remap_rx.iter().collect();

    let results: Vec<Result<usize>> = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()?
        .install(|| {
            use rayon::prelude::*;
            batches
                .par_iter()
                .map(|batch_info| {
                    match remap_batch(batch_info, &global_triple_tx, chunk_size) {
                        Ok(count) => {
                            // Files fully consumed — delete temp files to free disk space.
                            // HDT source files are not owned by us, so skip deletion.
                            if let TripleSource::LtrFile(ref triples_path) =
                                batch_info.triple_source
                                && let Err(e) = std::fs::remove_file(triples_path)
                            {
                                tracing::warn!(
                                    "Failed to delete {}: {}",
                                    triples_path.display(),
                                    e
                                );
                            }
                            if let Err(e) = std::fs::remove_file(&batch_info.mapping_path) {
                                tracing::warn!(
                                    "Failed to delete {}: {}",
                                    batch_info.mapping_path.display(), e
                                );
                            }
                            tracing::debug!(
                                "Batch {}: remapped {} triples",
                                batch_info.batch_id,
                                count
                            );
                            Ok(count)
                        }
                        Err(e) => {
                            tracing::error!("Failed to remap batch {}: {}", batch_info.batch_id, e);
                            Err(e)
                        }
                    }
                })
                .collect()
        });

    // Check for errors and sum up counts
    let mut total = 0u64;
    for result in results {
        total += result? as u64;
    }

    tracing::debug!("ID remapping complete: {} total triples remapped", total);
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::Path;

    /// Create a test ID mapping file.
    fn create_test_mapping(path: &Path, so_map: Vec<u64>, p_map: Vec<u64>) -> Result<()> {
        let file = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(file);
        let mut encoder = zstd::Encoder::new(&mut writer, 3)?;

        // Write batch ID (0)
        encoder.write_all(&0u32.to_le_bytes())?;

        // Write SO map
        encoder.write_all(&(so_map.len() as u32).to_le_bytes())?;
        for &id in &so_map {
            encoder.write_all(&id.to_le_bytes())?;
        }

        // Write P map
        encoder.write_all(&(p_map.len() as u32).to_le_bytes())?;
        for &id in &p_map {
            encoder.write_all(&id.to_le_bytes())?;
        }

        encoder.finish()?;
        Ok(())
    }

    /// Create a test triples file with local IDs.
    fn create_test_triples(path: &Path, triples: Vec<LocalIdTriple>) -> Result<()> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        let mut encoder = zstd::Encoder::new(writer, 3)?;

        for triple in triples {
            triple.write_to(&mut encoder)?;
        }

        encoder.finish()?;
        Ok(())
    }

    /// Test remapping a single triple with known mapping.
    #[test]
    fn test_remap_single_triple() -> Result<()> {
        use tempfile::TempDir;

        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Create a mapping: local IDs 0,1,2 → global IDs 10,20,30
        let mapping_path = temp_path.join("mapping.map.zst");
        create_test_mapping(
            &mapping_path,
            vec![10u64, 11u64, 12u64],       // SO map
            vec![20u64, 21u64],              // P map
        )?;

        // Create a triple: local (0, 0, 1)
        let triples_path = temp_path.join("triples.bin.zst");
        create_test_triples(
            &triples_path,
            vec![LocalIdTriple {
                subject: 0,
                predicate: 0,
                object: 1,
            }],
        )?;

        // Create a channel to receive remapped triples
        let (tx, rx) = crossbeam_channel::bounded(10);

        let batch_info = BatchRemapInfo {
            batch_id: 0,
            triple_source: TripleSource::LtrFile(triples_path),
            mapping_path,
        };

        // Remap the batch
        let count = remap_batch(&batch_info, &tx, 4)?;
        drop(tx);

        // Verify result
        assert_eq!(count, 1);

        let remapped_chunk = rx.recv().expect("Should receive remapped triple chunk");
        assert_eq!(remapped_chunk.len(), 1);
        let remapped = remapped_chunk[0];
        assert_eq!(remapped.subject, 10);
        assert_eq!(remapped.predicate, 20);
        assert_eq!(remapped.object, 11);

        Ok(())
    }

    /// Test remapping multiple triples.
    #[test]
    fn test_remap_multiple_triples() -> Result<()> {
        use tempfile::TempDir;

        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Create a mapping
        let mapping_path = temp_path.join("mapping.map.zst");
        create_test_mapping(
            &mapping_path,
            vec![100u64, 101u64, 102u64, 103u64],
            vec![200u64, 201u64],
        )?;

        // Create three triples
        let triples_path = temp_path.join("triples.bin.zst");
        create_test_triples(
            &triples_path,
            vec![
                LocalIdTriple { subject: 0, predicate: 0, object: 1 },
                LocalIdTriple { subject: 1, predicate: 1, object: 2 },
                LocalIdTriple { subject: 2, predicate: 0, object: 3 },
            ],
        )?;

        let (tx, rx) = crossbeam_channel::bounded(10);

        let batch_info = BatchRemapInfo {
            batch_id: 0,
            triple_source: TripleSource::LtrFile(triples_path),
            mapping_path,
        };

        let count = remap_batch(&batch_info, &tx, 4)?;
        drop(tx);

        assert_eq!(count, 3);

        let remapped_triples: Vec<_> = rx
            .iter()
            .flat_map(|chunk| chunk.into_iter())
            .collect();
        assert_eq!(remapped_triples.len(), 3);

        // Verify each triple
        assert_eq!(remapped_triples[0], IdTriple { subject: 100, predicate: 200, object: 101 });
        assert_eq!(remapped_triples[1], IdTriple { subject: 101, predicate: 201, object: 102 });
        assert_eq!(remapped_triples[2], IdTriple { subject: 102, predicate: 200, object: 103 });

        Ok(())
    }

    /// Test error handling for out-of-bounds local ID.
    #[test]
    fn test_remap_out_of_bounds_so_id() -> Result<()> {
        use tempfile::TempDir;

        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Create a small mapping: only 2 SO IDs
        let mapping_path = temp_path.join("mapping.map.zst");
        create_test_mapping(
            &mapping_path,
            vec![100u64, 101u64],
            vec![200u64],
        )?;

        // Create a triple referencing local SO ID 5 (out of bounds)
        let triples_path = temp_path.join("triples.bin.zst");
        create_test_triples(
            &triples_path,
            vec![LocalIdTriple { subject: 5, predicate: 0, object: 1 }],
        )?;

        let (tx, _rx) = crossbeam_channel::bounded(10);

        let batch_info = BatchRemapInfo {
            batch_id: 0,
            triple_source: TripleSource::LtrFile(triples_path),
            mapping_path,
        };

        let result = remap_batch(&batch_info, &tx, 4);
        assert!(result.is_err(), "Should error on out-of-bounds ID");
        assert!(result.unwrap_err().to_string().contains("SO mapping missing"));

        Ok(())
    }

    /// Test error handling for out-of-bounds predicate ID.
    #[test]
    fn test_remap_out_of_bounds_p_id() -> Result<()> {
        use tempfile::TempDir;

        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Create a mapping with only 1 predicate ID
        let mapping_path = temp_path.join("mapping.map.zst");
        create_test_mapping(
            &mapping_path,
            vec![100u64, 101u64],
            vec![200u64],
        )?;

        // Create a triple referencing predicate ID 5 (out of bounds)
        let triples_path = temp_path.join("triples.bin.zst");
        create_test_triples(
            &triples_path,
            vec![LocalIdTriple { subject: 0, predicate: 5, object: 1 }],
        )?;

        let (tx, _rx) = crossbeam_channel::bounded(10);

        let batch_info = BatchRemapInfo {
            batch_id: 0,
            triple_source: TripleSource::LtrFile(triples_path),
            mapping_path,
        };

        let result = remap_batch(&batch_info, &tx, 4);
        assert!(result.is_err(), "Should error on out-of-bounds predicate ID");
        assert!(result.unwrap_err().to_string().contains("P mapping missing"));

        Ok(())
    }
}
