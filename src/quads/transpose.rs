//! Graph memberships transposed into position-major order.
//!
//! A layer set stores memberships graph-major: one strictly increasing position
//! list per graph. Consumers that walk a triple order — the graphs-index builder
//! over SPO, `hdtc void` over SPO and OPS — need the transpose, `(position,
//! graph)` in position order, joined against their scan.

use crate::quads::PositionGraphMembership;
use crate::quads::{EmbeddedLayerSetReader, GraphSidecarReader, LayerMemberIter};
use crate::sort::{ExternalSorter, MergeIterator};
use anyhow::{Context, Result};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;

const POSITION_CHUNK_SHIFT: u32 = 16;
const LAYER_MERGE_LIMIT: u64 = 128;
// A chunked layer decodes one 2^POSITION_CHUNK_SHIFT container at a time, so a
// fully dense container is the resident cost of holding one layer iterator.
const LAYER_MERGE_ESTIMATED_BYTES: usize = (1 << POSITION_CHUNK_SHIFT) * 8;

/// A set of graph layers over one position space.
pub(crate) trait LayerSource {
    fn layer_iter(&mut self, graph: u64) -> Result<LayerMemberIter>;
}

impl LayerSource for GraphSidecarReader {
    fn layer_iter(&mut self, graph: u64) -> Result<LayerMemberIter> {
        GraphSidecarReader::layer_iter(self, graph)
    }
}

impl LayerSource for EmbeddedLayerSetReader {
    fn layer_iter(&mut self, graph: u64) -> Result<LayerMemberIter> {
        EmbeddedLayerSetReader::layer_iter(self, graph)
    }
}

/// Position-major memberships produced by merging a layer set.
///
/// Every layer is already strictly increasing in position, so transposing the
/// layers is a k-way merge rather than a sort of all memberships: nothing
/// spills, and each layer contributes one buffered iterator.
pub(crate) struct LayerMergeIter {
    layers: Vec<LayerMemberIter>,
    heads: BinaryHeap<Reverse<(u64, usize)>>,
}

impl LayerMergeIter {
    fn new(source: &mut impl LayerSource, named_graphs: u64) -> Result<Self> {
        let count = usize::try_from(
            named_graphs
                .checked_add(1)
                .context("layer-merge layer count overflow")?,
        )
        .context("layer-merge layer count overflow")?;
        let mut layers = Vec::with_capacity(count);
        let mut heads = BinaryHeap::with_capacity(count);
        for graph in 0..=named_graphs {
            let mut layer = source.layer_iter(graph)?;
            if let Some(position) = layer.next().transpose()? {
                heads.push(Reverse((position, layers.len())));
            }
            layers.push(layer);
        }
        Ok(Self { layers, heads })
    }
}

impl Iterator for LayerMergeIter {
    type Item = Result<PositionGraphMembership>;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse((position, index)) = self.heads.pop()?;
        match self.layers[index].next().transpose() {
            Ok(Some(next)) => self.heads.push(Reverse((next, index))),
            Ok(None) => {}
            Err(error) => return Some(Err(error)),
        }
        Some(Ok(PositionGraphMembership {
            position,
            graph: index as u64,
        }))
    }
}

/// A layer set's memberships in `(position, graph)` order.
///
/// A bounded graph dictionary merges its layers directly. Past that the layers
/// no longer fit as concurrent iterators, so the memberships go through the
/// bounded external sort instead.
pub(crate) enum PositionMajorMemberships {
    Merged(LayerMergeIter),
    Sorted {
        // Owns the chunk files its iterator reads, so it outlives the merge.
        _sorter: ExternalSorter,
        sorted: MergeIterator<PositionGraphMembership>,
    },
}

/// Bytes the k-way layer merge holds resident, or `None` if it does not apply.
///
/// The merge keeps one buffered iterator per layer, so its cost is known up
/// front rather than budgeted as a fraction. Reporting it lets the caller hand
/// the rest of the budget to whatever else it runs.
pub(crate) fn layer_merge_reserve(layer_count: u64, memory_budget: usize) -> Option<usize> {
    if layer_count > LAYER_MERGE_LIMIT {
        return None;
    }
    let reserve = layer_count
        .checked_mul(LAYER_MERGE_ESTIMATED_BYTES as u64)
        .and_then(|bytes| usize::try_from(bytes).ok())?;
    (reserve <= memory_budget / 2).then_some(reserve)
}

impl PositionMajorMemberships {
    pub(crate) fn open(
        source: &mut impl LayerSource,
        named_graphs: u64,
        temp_dir: &Path,
        memory_budget: usize,
        merge: bool,
    ) -> Result<Self> {
        let layer_count = named_graphs
            .checked_add(1)
            .context("layer-merge layer count overflow")?;
        if merge {
            tracing::info!(layer_count, "Transposing graph layers by k-way merge");
            return Ok(Self::Merged(LayerMergeIter::new(source, named_graphs)?));
        }
        tracing::info!(
            layer_count,
            "Graph count exceeds layer-merge resource limit; transposing by external sort"
        );
        let mut sorter = ExternalSorter::new(temp_dir, memory_budget.max(1));
        let mut buffer = Vec::new();
        let mut memory = 0usize;
        for graph in 0..=named_graphs {
            for position in source.layer_iter(graph)? {
                sorter.push(
                    PositionGraphMembership {
                        position: position?,
                        graph,
                    },
                    &mut buffer,
                    &mut memory,
                )?;
            }
        }
        let sorted = sorter.finish(&mut buffer)?;
        Ok(Self::Sorted {
            _sorter: sorter,
            sorted,
        })
    }
}

impl Iterator for PositionMajorMemberships {
    type Item = Result<PositionGraphMembership>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Merged(inner) => inner.next(),
            Self::Sorted { sorted, .. } => sorted.next(),
        }
    }
}
