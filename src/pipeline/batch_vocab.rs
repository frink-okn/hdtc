//! Batch vocabulary builder with hash map and arena allocation.

use bumpalo::Bump;
use hashbrown::HashMap;

/// Role flags for terms (can be OR-ed together).
pub const ROLE_SUBJECT: u8 = 0x01;
pub const ROLE_PREDICATE: u8 = 0x02;
pub const ROLE_OBJECT: u8 = 0x04;
pub const ROLE_GRAPH: u8 = 0x08;

/// Local ID assigned within a batch (32-bit sufficient for 10M triples/batch).
pub type LocalId = u32;

/// Triple with local IDs (compact: 12 bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalIdTriple {
    pub subject: LocalId,
    pub predicate: LocalId,
    pub object: LocalId,
}

impl LocalIdTriple {
    /// Write this triple to a writer in binary format.
    pub fn write_to<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.subject.to_le_bytes())?;
        writer.write_all(&self.predicate.to_le_bytes())?;
        writer.write_all(&self.object.to_le_bytes())?;
        Ok(())
    }

    /// Read a triple from a reader.
    pub fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Option<Self>> {
        let mut subject_bytes = [0u8; 4];
        if reader.read_exact(&mut subject_bytes).is_err() {
            return Ok(None); // End of file
        }
        let subject = u32::from_le_bytes(subject_bytes);

        let mut predicate_bytes = [0u8; 4];
        reader.read_exact(&mut predicate_bytes)?;
        let predicate = u32::from_le_bytes(predicate_bytes);

        let mut object_bytes = [0u8; 4];
        reader.read_exact(&mut object_bytes)?;
        let object = u32::from_le_bytes(object_bytes);

        Ok(Some(LocalIdTriple {
            subject,
            predicate,
            object,
        }))
    }
}

/// Batch vocabulary builder using hash map with arena allocation.
///
/// Processes a batch of triples, assigning local IDs to terms and accumulating
/// ID triples. Terms are stored in an arena for efficient memory management.
pub struct BatchVocabBuilder<'bump> {
    /// HashMap for subject/object terms: term bytes → (local_id, roles)
    so_term_map: HashMap<&'bump [u8], (LocalId, u8)>,
    /// HashMap for predicate terms: term bytes → local_id
    p_term_map: HashMap<&'bump [u8], LocalId>,
    /// Arena allocator for term storage
    arena: &'bump Bump,
    /// Next ID to assign (subject/object ID space)
    next_so_id: LocalId,
    /// Next ID to assign (predicate ID space, separate)
    next_p_id: LocalId,
    /// Accumulated triples with local IDs
    pub id_triples: Vec<LocalIdTriple>,
}

impl<'bump> BatchVocabBuilder<'bump> {
    /// Create a new batch vocabulary builder.
    ///
    /// # Arguments
    /// * `arena` - Arena allocator for term storage
    /// * `expected_terms` - Expected number of unique terms (for pre-allocation)
    pub fn new(arena: &'bump Bump, expected_terms: usize) -> Self {
        Self {
            so_term_map: HashMap::with_capacity(expected_terms),
            p_term_map: HashMap::with_capacity(expected_terms / 10), // Fewer predicates typically
            arena,
            next_so_id: 0,
            next_p_id: 0,
            id_triples: Vec::new(),
        }
    }

    /// Get or assign a local ID for a term.
    ///
    /// Returns the local ID for this term in the appropriate ID space.
    pub fn get_or_assign_id(&mut self, term: &[u8], role: u8) -> LocalId {
        let is_predicate = role & ROLE_PREDICATE != 0;

        if is_predicate {
            // Predicate ID space
            if let Some(&id) = self.p_term_map.get(term) {
                return id;
            }

            // Allocate term in arena
            let arena_term = self.arena.alloc_slice_copy(term);
            let id = self.next_p_id;
            self.next_p_id += 1;
            self.p_term_map.insert(arena_term, id);
            id
        } else {
            // Subject/Object ID space
            if let Some(&(id, existing_roles)) = self.so_term_map.get(term) {
                // Merge roles if term already exists
                self.so_term_map.get_mut(term).unwrap().1 |= role;
                return id;
            }

            // Allocate term in arena
            let arena_term = self.arena.alloc_slice_copy(term);
            let id = self.next_so_id;
            self.next_so_id += 1;
            self.so_term_map.insert(arena_term, (id, role));
            id
        }
    }

    /// Add a triple to this batch.
    pub fn add_triple(&mut self, subject: &[u8], predicate: &[u8], object: &[u8]) {
        let s_id = self.get_or_assign_id(subject, ROLE_SUBJECT);
        let p_id = self.get_or_assign_id(predicate, ROLE_PREDICATE);
        let o_id = self.get_or_assign_id(object, ROLE_OBJECT);

        self.id_triples.push(LocalIdTriple {
            subject: s_id,
            predicate: p_id,
            object: o_id,
        });
    }

    /// Finish building and return sorted vocabulary and ID triples.
    ///
    /// Consumes the builder and returns:
    /// - Sorted vocabulary entries (term, roles, so_local_id, p_local_id)
    /// - Local-ID triples
    pub fn finish(self) -> (Vec<(Vec<u8>, u8, Option<LocalId>, Option<LocalId>)>, Vec<LocalIdTriple>) {
        use std::collections::{HashMap as StdHashMap};

        // Collect all unique terms with their roles and IDs
        let mut term_info: StdHashMap<Vec<u8>, (u8, Option<LocalId>, Option<LocalId>)> = StdHashMap::new();

        // Add SO terms with their tracked roles
        for (term, &(so_id, roles)) in &self.so_term_map {
            term_info.insert(term.to_vec(), (roles, Some(so_id), None));
        }

        // Add/merge P terms
        for (term, &p_id) in &self.p_term_map {
            term_info.entry(term.to_vec())
                .and_modify(|(roles, _so_id, p_id_opt)| {
                    *roles |= ROLE_PREDICATE;
                    *p_id_opt = Some(p_id);
                })
                .or_insert((ROLE_PREDICATE, None, Some(p_id)));
        }

        // Convert to sorted vec
        let mut entries: Vec<_> = term_info
            .into_iter()
            .map(|(term, (roles, so_id, p_id))| (term, roles, so_id, p_id))
            .collect();

        // Sort by term bytes
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        (entries, self.id_triples)
    }

    /// Get statistics about this batch.
    pub fn stats(&self) -> BatchStats {
        use std::collections::HashSet;

        // Count unique terms across both hash maps
        let mut unique_terms = HashSet::new();
        for term in self.so_term_map.keys() {
            unique_terms.insert(*term);
        }
        for term in self.p_term_map.keys() {
            unique_terms.insert(*term);
        }

        BatchStats {
            num_terms: unique_terms.len(),
            num_triples: self.id_triples.len(),
        }
    }
}

/// Statistics about a batch.
#[derive(Debug, Clone, Copy)]
pub struct BatchStats {
    pub num_terms: usize,
    pub num_triples: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_vocab_builder() {
        let arena = Bump::new();
        let mut builder = BatchVocabBuilder::new(&arena, 100);

        // Add some triples
        builder.add_triple(b"s1", b"p1", b"o1");
        builder.add_triple(b"s1", b"p1", b"o2");
        builder.add_triple(b"s2", b"p1", b"o1");

        let stats = builder.stats();
        assert_eq!(stats.num_triples, 3);
        assert_eq!(stats.num_terms, 5); // s1, s2, p1, o1, o2

        let (vocab, triples) = builder.finish();
        assert_eq!(vocab.len(), 5);
        assert_eq!(triples.len(), 3);

        // Vocabulary should be sorted
        for i in 1..vocab.len() {
            assert!(vocab[i - 1].0 < vocab[i].0);
        }
    }

    #[test]
    fn test_role_merging() {
        let arena = Bump::new();
        let mut builder = BatchVocabBuilder::new(&arena, 100);

        // Same term appears as both subject and object
        let id1 = builder.get_or_assign_id(b"term1", ROLE_SUBJECT);
        let id2 = builder.get_or_assign_id(b"term1", ROLE_OBJECT);

        assert_eq!(id1, id2); // Should get same ID

        let (vocab, _) = builder.finish();
        assert_eq!(vocab.len(), 1);
        assert_eq!(vocab[0].1, ROLE_SUBJECT | ROLE_OBJECT);
    }
}
