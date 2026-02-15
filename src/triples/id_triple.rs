//! ID triple generation (Pass 2): re-stream RDF and look up term IDs.

use crate::dictionary::{resolve_global_id, DictCounts, SstReader};
use crate::rdf::{stream_quads, RdfInput};
use crate::sort::{ExternalSorter, Sortable};
use anyhow::Result;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;

/// A triple encoded as integer IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdTriple {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64,
}

impl PartialOrd for IdTriple {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IdTriple {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.subject
            .cmp(&other.subject)
            .then(self.predicate.cmp(&other.predicate))
            .then(self.object.cmp(&other.object))
    }
}

impl Sortable for IdTriple {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.subject.to_le_bytes())?;
        writer.write_all(&self.predicate.to_le_bytes())?;
        writer.write_all(&self.object.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; 24];
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        Ok(Some(IdTriple {
            subject: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            predicate: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            object: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        }))
    }

    fn mem_size(&self) -> usize {
        24
    }
}

/// Generate sorted, deduplicated ID triples from RDF inputs.
///
/// This is Pass 2: re-streams all input files, looks up subjects/objects in the SST
/// and predicates in the in-memory HashMap, then externally sorts in SPO order.
pub fn generate_id_triples(
    inputs: &[RdfInput],
    sst: &SstReader,
    predicate_ids: &HashMap<String, u64>,
    counts: &DictCounts,
    temp_dir: &Path,
    memory_budget: usize,
    base_uri: Option<&str>,
) -> Result<SortedTriples> {
    tracing::info!("Pass 2: Generating ID triples...");

    let mut sorter = ExternalSorter::new(temp_dir, memory_budget);
    let mut buffer = Vec::new();
    let mut mem_used = 0;
    let mut triple_count = 0u64;
    let mut lookup_failures = 0u64;

    for (file_index, input) in inputs.iter().enumerate() {
        tracing::info!("  Re-reading {}", input.path.display());

        stream_quads(input, file_index, base_uri, |quad| {
            // Look up subject (in SST)
            let s_id = match sst.get(quad.subject.as_bytes()) {
                Some(tid) => resolve_global_id(&tid, counts, true),
                None => {
                    lookup_failures += 1;
                    if lookup_failures <= 5 {
                        tracing::warn!("Subject not found in dictionary: {}", quad.subject);
                    }
                    return Ok(());
                }
            };

            // Look up predicate (in memory HashMap)
            let p_id = match predicate_ids.get(&quad.predicate) {
                Some(&id) => id,
                None => {
                    lookup_failures += 1;
                    if lookup_failures <= 5 {
                        tracing::warn!("Predicate not found in dictionary: {}", quad.predicate);
                    }
                    return Ok(());
                }
            };

            // Look up object (in SST)
            let o_id = match sst.get(quad.object.as_bytes()) {
                Some(tid) => resolve_global_id(&tid, counts, false),
                None => {
                    lookup_failures += 1;
                    return Ok(());
                }
            };

            let triple = IdTriple {
                subject: s_id,
                predicate: p_id,
                object: o_id,
            };

            sorter.push(triple, &mut buffer, &mut mem_used)?;
            triple_count += 1;
            Ok(())
        })?;
    }

    if lookup_failures > 0 {
        tracing::warn!("{lookup_failures} terms not found in dictionary during Pass 2");
    }

    tracing::info!("Pass 2: {triple_count} ID triples generated, sorting...");

    let merged = sorter.finish(&mut buffer)?;

    Ok(SortedTriples {
        inner: merged,
        prev: None,
    })
}

/// Iterator over sorted, deduplicated ID triples.
pub struct SortedTriples {
    inner: crate::sort::external::MergeIterator<IdTriple>,
    prev: Option<IdTriple>,
}

impl Iterator for SortedTriples {
    type Item = Result<IdTriple>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let result = self.inner.next()?;
            match result {
                Ok(triple) => {
                    // Deduplicate
                    if Some(triple) == self.prev {
                        continue;
                    }
                    self.prev = Some(triple);
                    return Some(Ok(triple));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_triple_ordering() {
        let t1 = IdTriple { subject: 1, predicate: 1, object: 1 };
        let t2 = IdTriple { subject: 1, predicate: 1, object: 2 };
        let t3 = IdTriple { subject: 1, predicate: 2, object: 1 };
        let t4 = IdTriple { subject: 2, predicate: 1, object: 1 };

        assert!(t1 < t2);
        assert!(t2 < t3);
        assert!(t3 < t4);
    }

    #[test]
    fn test_id_triple_roundtrip() {
        let triple = IdTriple { subject: 42, predicate: 7, object: 100 };
        let mut buf = Vec::new();
        triple.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), 24);

        let mut cursor = std::io::Cursor::new(&buf);
        let decoded = IdTriple::read_from(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded, triple);
    }
}
