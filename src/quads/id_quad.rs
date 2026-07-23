use crate::sort::Sortable;
use crate::triples::id_triple::IdTriple;
use anyhow::Result;
use std::io::{Read, Write};

/// A globally remapped RDF quad membership.
///
/// Graph ID zero denotes the RDF default graph. Named graph IDs start at one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdQuad {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64,
    pub graph: u64,
}

impl PartialOrd for IdQuad {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IdQuad {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.subject
            .cmp(&other.subject)
            .then(self.predicate.cmp(&other.predicate))
            .then(self.object.cmp(&other.object))
            .then(self.graph.cmp(&other.graph))
    }
}

impl Sortable for IdQuad {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.subject.to_le_bytes())?;
        writer.write_all(&self.predicate.to_le_bytes())?;
        writer.write_all(&self.object.to_le_bytes())?;
        writer.write_all(&self.graph.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; 32];
        match reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(Self {
                subject: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
                predicate: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
                object: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
                graph: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn mem_size(&self) -> usize {
        32
    }
}

/// One output graph membership, sorted graph-major for sidecar finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GraphMembership {
    pub graph: u64,
    pub position: u64,
}

/// A source-sidecar membership transposed into HDT position order for the
/// Stage 5 merge join. `graph` is the source sidecar graph ID (zero is the
/// default graph).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PositionGraphMembership {
    pub position: u64,
    pub graph: u64,
}

/// Streaming SPO-deduplicating adapter over `(S,P,O,gid)` sort order.
///
/// It emits every distinct membership through `emit_membership` and yields one
/// union triple per SPO group. Only one quad of lookahead is retained.
pub struct QuadUnionIterator<I, F> {
    inner: I,
    emit_membership: F,
    pending: Option<IdQuad>,
    position: u64,
}

impl<I, F> QuadUnionIterator<I, F>
where
    I: Iterator<Item = Result<IdQuad>>,
    F: FnMut(GraphMembership) -> Result<()>,
{
    pub fn new(inner: I, emit_membership: F) -> Self {
        Self {
            inner,
            emit_membership,
            pending: None,
            position: 0,
        }
    }
}

impl<I, F> Iterator for QuadUnionIterator<I, F>
where
    I: Iterator<Item = Result<IdQuad>>,
    F: FnMut(GraphMembership) -> Result<()>,
{
    type Item = Result<IdTriple>;

    fn next(&mut self) -> Option<Self::Item> {
        let first = match self.pending.take() {
            Some(quad) => quad,
            None => match self.inner.next()? {
                Ok(quad) => quad,
                Err(error) => return Some(Err(error)),
            },
        };

        let triple = IdTriple {
            subject: first.subject,
            predicate: first.predicate,
            object: first.object,
        };
        let mut last_graph = None;
        let mut current = first;

        loop {
            if last_graph != Some(current.graph) {
                if let Err(error) = (self.emit_membership)(GraphMembership {
                    graph: current.graph,
                    position: self.position,
                }) {
                    return Some(Err(error));
                }
                last_graph = Some(current.graph);
            }

            match self.inner.next() {
                Some(Ok(next))
                    if next.subject == triple.subject
                        && next.predicate == triple.predicate
                        && next.object == triple.object =>
                {
                    current = next;
                }
                Some(Ok(next)) => {
                    self.pending = Some(next);
                    break;
                }
                Some(Err(error)) => return Some(Err(error)),
                None => break,
            }
        }

        self.position += 1;
        Some(Ok(triple))
    }
}

impl Sortable for GraphMembership {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.graph.to_le_bytes())?;
        writer.write_all(&self.position.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; 16];
        match reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(Self {
                graph: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
                position: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn mem_size(&self) -> usize {
        16
    }
}

impl Sortable for PositionGraphMembership {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.position.to_le_bytes())?;
        writer.write_all(&self.graph.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; 16];
        match reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(Self {
                position: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
                graph: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn mem_size(&self) -> usize {
        16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quad_order_uses_graph_as_final_key() {
        let base = IdQuad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: 0,
        };
        assert!(base < IdQuad { graph: 1, ..base });
    }

    #[test]
    fn union_iterator_deduplicates_spo_and_memberships() -> Result<()> {
        let quads = vec![
            Ok(IdQuad {
                subject: 1,
                predicate: 1,
                object: 1,
                graph: 0,
            }),
            Ok(IdQuad {
                subject: 1,
                predicate: 1,
                object: 1,
                graph: 0,
            }),
            Ok(IdQuad {
                subject: 1,
                predicate: 1,
                object: 1,
                graph: 2,
            }),
            Ok(IdQuad {
                subject: 1,
                predicate: 1,
                object: 2,
                graph: 1,
            }),
        ];
        let mut memberships = Vec::new();
        let triples = QuadUnionIterator::new(quads.into_iter(), |m| {
            memberships.push(m);
            Ok(())
        })
        .collect::<Result<Vec<_>>>()?;

        assert_eq!(triples.len(), 2);
        assert_eq!(
            memberships,
            vec![
                GraphMembership {
                    graph: 0,
                    position: 0
                },
                GraphMembership {
                    graph: 2,
                    position: 0
                },
                GraphMembership {
                    graph: 1,
                    position: 1
                },
            ]
        );
        Ok(())
    }
}
