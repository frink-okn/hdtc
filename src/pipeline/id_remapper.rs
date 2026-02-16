//! Parallel ID remapping from local to global IDs.

use crate::pipeline::batch_vocab::LocalIdTriple;
use crate::pipeline::vocab_merger::IdMapping;
use crate::triples::id_triple::IdTriple;
use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

/// Information about a batch for remapping.
pub struct BatchRemapInfo {
    pub batch_id: usize,
    pub triples_path: PathBuf,
    pub mapping_path: PathBuf,
}

/// Remap a single batch: local IDs → global IDs.
fn remap_batch(
    batch_info: &BatchRemapInfo,
    global_triple_tx: &Sender<IdTriple>,
) -> Result<usize> {
    // Load ID mapping
    let mapping = IdMapping::read_from_file(&batch_info.mapping_path)
        .with_context(|| format!("Failed to load ID mapping for batch {}", batch_info.batch_id))?;

    // Open local-ID triples file
    let file = File::open(&batch_info.triples_path)
        .with_context(|| format!("Failed to open triples file for batch {}", batch_info.batch_id))?;
    let buf_reader = BufReader::new(file);
    let mut decoder = zstd::Decoder::with_buffer(buf_reader)?;

    let mut count = 0;

    // Read and remap each triple
    loop {
        match LocalIdTriple::read_from(&mut decoder) {
            Ok(Some(local_triple)) => {
                // Look up global IDs from mappings
                let global_subject = *mapping.so_map.get(local_triple.subject as usize)
                    .ok_or_else(|| anyhow::anyhow!(
                        "SO mapping missing for local subject ID {} in batch {} (map size: {})",
                        local_triple.subject, batch_info.batch_id, mapping.so_map.len()
                    ))?;
                let global_predicate = *mapping.p_map.get(local_triple.predicate as usize)
                    .ok_or_else(|| anyhow::anyhow!(
                        "P mapping missing for local predicate ID {} in batch {} (map size: {})",
                        local_triple.predicate, batch_info.batch_id, mapping.p_map.len()
                    ))?;
                let global_object = *mapping.so_map.get(local_triple.object as usize)
                    .ok_or_else(|| anyhow::anyhow!(
                        "SO mapping missing for local object ID {} in batch {} (map size: {})",
                        local_triple.object, batch_info.batch_id, mapping.so_map.len()
                    ))?;

                let global_triple = IdTriple {
                    subject: global_subject,
                    predicate: global_predicate,
                    object: global_object,
                };

                // Send to next stage
                if global_triple_tx.send(global_triple).is_err() {
                    anyhow::bail!("Global triple receiver disconnected");
                }

                count += 1;
            }
            Ok(None) => break, // End of file
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("Failed to read local triple from batch {}", batch_info.batch_id)
                });
            }
        }
    }

    Ok(count)
}

/// Stage 5: Parallel ID remapper.
///
/// Processes batches in parallel, remapping local IDs to global IDs.
pub fn id_remapper_stage(
    batch_remap_rx: Receiver<BatchRemapInfo>,
    global_triple_tx: Sender<IdTriple>,
    num_threads: usize,
) -> Result<u64> {
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
                    match remap_batch(batch_info, &global_triple_tx) {
                        Ok(count) => {
                            tracing::info!(
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

    tracing::info!("ID remapping complete: {} total triples remapped", total);
    Ok(total)
}
