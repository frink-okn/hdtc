//! Batch vocabulary builder with hash map and arena allocation.

use bitflags::bitflags;
use bumpalo::Bump;
use hashbrown::HashMap;

bitflags! {
    /// Role flags for terms (can be OR-ed together).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Roles: u8 {
        const SUBJECT   = 0x01;
        const PREDICATE = 0x02;
        const OBJECT    = 0x04;
        const GRAPH     = 0x08;
    }
}

/// Local ID assigned within a batch (32-bit sufficient for 10M triples/batch).
pub type LocalId = u32;

/// A vocabulary entry produced by batch processing: a term with its roles and local IDs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VocabEntry {
    pub term: Vec<u8>,
    pub roles: Roles,
    pub so_local_id: Option<LocalId>,
    pub p_local_id: Option<LocalId>,
}

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
    so_term_map: HashMap<&'bump [u8], (LocalId, Roles)>,
    /// HashMap for predicate terms: term bytes → local_id
    p_term_map: HashMap<&'bump [u8], LocalId>,
    /// Arena allocator for term storage
    arena: &'bump Bump,
    /// Next ID to assign (subject/object ID space)
    next_so_id: LocalId,
    /// Next ID to assign (predicate ID space, separate)
    next_p_id: LocalId,
    /// Count of unique terms across both maps
    unique_term_count: usize,
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
            unique_term_count: 0,
            id_triples: Vec::new(),
        }
    }

    /// Get or assign a local ID for a term.
    ///
    /// Returns the local ID for this term in the appropriate ID space.
    pub fn get_or_assign_id(&mut self, term: &[u8], role: Roles) -> LocalId {
        if role.contains(Roles::PREDICATE) {
            // Predicate ID space
            if let Some(&id) = self.p_term_map.get(term) {
                return id;
            }

            // New predicate term — only a new unique term if not already in SO map
            if !self.so_term_map.contains_key(term) {
                self.unique_term_count += 1;
            }

            // Allocate term in arena
            let arena_term = self.arena.alloc_slice_copy(term);
            let id = self.next_p_id;
            self.next_p_id += 1;
            self.p_term_map.insert(arena_term, id);
            id
        } else {
            // Subject/Object ID space
            if let Some((id, existing_roles)) = self.so_term_map.get_mut(term) {
                *existing_roles |= role;
                return *id;
            }

            // New SO term — only a new unique term if not already in P map
            if !self.p_term_map.contains_key(term) {
                self.unique_term_count += 1;
            }

            // Allocate term in arena
            let arena_term = self.arena.alloc_slice_copy(term);
            let id = self.next_so_id;
            self.next_so_id += 1;
            self.so_term_map.insert(arena_term, (id, role));
            id
        }
    }

    /// Add a triple to this batch (test helper).
    #[cfg(test)]
    pub fn add_triple(&mut self, subject: &[u8], predicate: &[u8], object: &[u8]) {
        let s_id = self.get_or_assign_id(subject, Roles::SUBJECT);
        let p_id = self.get_or_assign_id(predicate, Roles::PREDICATE);
        let o_id = self.get_or_assign_id(object, Roles::OBJECT);

        self.id_triples.push(LocalIdTriple {
            subject: s_id,
            predicate: p_id,
            object: o_id,
        });
    }

    /// Finish building and return sorted vocabulary and ID triples.
    ///
    /// Consumes the builder and returns:
    /// - Sorted vocabulary entries
    /// - Local-ID triples
    pub fn finish(self) -> (Vec<VocabEntry>, Vec<LocalIdTriple>) {
        // Collect entries directly into a Vec — no intermediate HashMap needed.
        let mut entries: Vec<VocabEntry> =
            Vec::with_capacity(self.so_term_map.len() + self.p_term_map.len());

        for (term, (so_id, roles)) in self.so_term_map {
            entries.push(VocabEntry {
                term: term.to_vec(),
                roles,
                so_local_id: Some(so_id),
                p_local_id: None,
            });
        }

        for (term, p_id) in self.p_term_map {
            entries.push(VocabEntry {
                term: term.to_vec(),
                roles: Roles::PREDICATE,
                so_local_id: None,
                p_local_id: Some(p_id),
            });
        }

        // Sort by term bytes — duplicates (terms in both maps) become adjacent
        entries.sort_unstable_by(|a, b| a.term.cmp(&b.term));

        // Merge adjacent duplicates (terms appearing in both SO and P maps)
        entries.dedup_by(|b, a| {
            if a.term == b.term {
                a.roles |= b.roles;
                if a.so_local_id.is_none() { a.so_local_id = b.so_local_id; }
                if a.p_local_id.is_none() { a.p_local_id = b.p_local_id; }
                true
            } else {
                false
            }
        });

        (entries, self.id_triples)
    }

    /// Get statistics about this batch.
    pub fn stats(&self) -> BatchStats {
        BatchStats {
            num_terms: self.unique_term_count,
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
            assert!(vocab[i - 1].term < vocab[i].term);
        }
    }

    #[test]
    fn test_role_merging() {
        let arena = Bump::new();
        let mut builder = BatchVocabBuilder::new(&arena, 100);

        // Same term appears as both subject and object
        let id1 = builder.get_or_assign_id(b"term1", Roles::SUBJECT);
        let id2 = builder.get_or_assign_id(b"term1", Roles::OBJECT);

        assert_eq!(id1, id2); // Should get same ID

        let (vocab, _) = builder.finish();
        assert_eq!(vocab.len(), 1);
        assert_eq!(vocab[0].roles, Roles::SUBJECT | Roles::OBJECT);
    }

    /// Test a term appearing in all three roles: subject, predicate, and object.
    #[test]
    fn test_term_in_all_roles() {
        let arena = Bump::new();
        let mut builder = BatchVocabBuilder::new(&arena, 100);

        // Same term used as subject, predicate, and object
        let subject_id = builder.get_or_assign_id(b"universal", Roles::SUBJECT);
        let _predicate_id = builder.get_or_assign_id(b"universal", Roles::PREDICATE);
        let object_id = builder.get_or_assign_id(b"universal", Roles::OBJECT);

        // Subject/object share ID space; predicate has separate ID
        assert_eq!(subject_id, object_id);
        // Both SO and P spaces start at 0, so first term in each gets 0. They're tracked separately.
        // What matters is that both so_local_id and p_local_id are present.

        let (vocab, _) = builder.finish();
        assert_eq!(vocab.len(), 1);

        let entry = &vocab[0];
        assert_eq!(entry.term, b"universal");
        assert!(entry.roles.contains(Roles::SUBJECT));
        assert!(entry.roles.contains(Roles::PREDICATE));
        assert!(entry.roles.contains(Roles::OBJECT));
        assert!(entry.so_local_id.is_some());
        assert!(entry.p_local_id.is_some());
    }

    /// Test term appearing as both subject and predicate.
    #[test]
    fn test_term_as_subject_and_predicate() {
        let arena = Bump::new();
        let mut builder = BatchVocabBuilder::new(&arena, 100);

        // Add a predicate first to ensure its ID space is non-empty
        builder.get_or_assign_id(b"other_pred", Roles::PREDICATE);

        let subject_id = builder.get_or_assign_id(b"term_sp", Roles::SUBJECT);
        let predicate_id = builder.get_or_assign_id(b"term_sp", Roles::PREDICATE);

        // With other_pred at predicate ID 0, term_sp gets predicate ID 1
        // So they can have different values in their respective ID spaces
        assert_eq!(subject_id, 0); // First SO term
        assert_eq!(predicate_id, 1); // Second P term

        let (vocab, _) = builder.finish();
        let has_term_sp = vocab.iter().any(|e| e.term == b"term_sp");
        assert!(has_term_sp);

        let entry = vocab.iter().find(|e| e.term == b"term_sp").unwrap();
        assert_eq!(entry.roles, Roles::SUBJECT | Roles::PREDICATE);
        assert!(entry.so_local_id.is_some());
        assert!(entry.p_local_id.is_some());
    }

    /// Test term appearing as both predicate and object.
    #[test]
    fn test_term_as_predicate_and_object() {
        let arena = Bump::new();
        let mut builder = BatchVocabBuilder::new(&arena, 100);

        // Add another SO term first
        builder.get_or_assign_id(b"other_so", Roles::SUBJECT);

        let predicate_id = builder.get_or_assign_id(b"term_po", Roles::PREDICATE);
        let object_id = builder.get_or_assign_id(b"term_po", Roles::OBJECT);

        // With other_so at SO ID 0, term_po gets SO ID 1
        assert_eq!(object_id, 1); // Second SO term
        assert_eq!(predicate_id, 0); // First P term

        let (vocab, _) = builder.finish();
        let entry = vocab.iter().find(|e| e.term == b"term_po").unwrap();
        assert_eq!(entry.roles, Roles::PREDICATE | Roles::OBJECT);
        assert!(entry.so_local_id.is_some());
        assert!(entry.p_local_id.is_some());
    }

    /// Test unique term counting with multi-role terms.
    #[test]
    fn test_unique_term_count_with_multi_role() {
        let arena = Bump::new();
        let mut builder = BatchVocabBuilder::new(&arena, 100);

        // Add terms in various combinations
        builder.add_triple(b"shared", b"p", b"shared");  // "shared" as subject+object, "p" as predicate
        builder.add_triple(b"multi", b"multi", b"other"); // "multi" as subject+predicate

        let stats = builder.stats();
        // unique terms: "shared" (counted once), "p", "multi" (counted once), "other"
        assert_eq!(stats.num_terms, 4);

        let (vocab, _) = builder.finish();
        assert_eq!(vocab.len(), 4);
    }

    /// Test ID assignment order for subject/object vs predicate spaces.
    #[test]
    fn test_id_space_separation() {
        let arena = Bump::new();
        let mut builder = BatchVocabBuilder::new(&arena, 100);

        // Add subjects and objects in order
        let s1_id = builder.get_or_assign_id(b"s1", Roles::SUBJECT);
        let s2_id = builder.get_or_assign_id(b"s2", Roles::SUBJECT);
        let o1_id = builder.get_or_assign_id(b"o1", Roles::OBJECT);

        // Predicates should get separate ID space
        let p1_id = builder.get_or_assign_id(b"p1", Roles::PREDICATE);
        let p2_id = builder.get_or_assign_id(b"p2", Roles::PREDICATE);

        // SO IDs should be sequential starting from 0
        assert_eq!(s1_id, 0);
        assert_eq!(s2_id, 1);
        assert_eq!(o1_id, 2);

        // P IDs should be sequential starting from 0 in separate space
        assert_eq!(p1_id, 0);
        assert_eq!(p2_id, 1);
    }

    /// Test that ID remapping with role info is preserved through finish().
    #[test]
    fn test_finish_preserves_role_info() {
        let arena = Bump::new();
        let mut builder = BatchVocabBuilder::new(&arena, 100);

        builder.add_triple(b"s", b"p", b"s");  // subject/object reuse
        builder.add_triple(b"p", b"q", b"o"); // "p" also appears as subject

        let (vocab, _) = builder.finish();

        // Find "p" entry
        let p_entry = vocab.iter().find(|e| e.term == b"p").unwrap();
        // "p" should have PREDICATE role from first triple, SUBJECT role from second
        assert!(p_entry.roles.contains(Roles::PREDICATE));
        assert!(p_entry.roles.contains(Roles::SUBJECT));
    }
}
