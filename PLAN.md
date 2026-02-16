# hdtc Performance & Quality Improvement Plan

Based on review of QLever architecture recommendations (`qlever-notes.md`) against the current
pipelined codebase. The old two-pass architecture has been replaced with a single-pass pipeline
using batched hash maps, partial vocabulary merging, and parallel ID remapping. This plan covers
the remaining improvements.

## Status Key
- [ ] Not started
- [x] Complete

---

## Phase 1: High-Impact Performance Fixes

### 1.1 Stream BitmapTriples Construction
**Impact:** Eliminates ~5.8GB peak memory for 550M triples
**Files:** `src/triples/builder.rs`, `src/io/bitmap.rs`, `src/io/log_array.rs`
**QLever ref:** Recommendation 3.3 (HIGH IMPACT)

Currently `build_bitmap_triples` accumulates four large vectors in memory:
- `array_z_entries: Vec<u64>` — 550M x 8 bytes = 4.4GB
- `array_y_entries: Vec<u64>` — ~100M x 8 bytes = 800MB
- `bitmap_z_bits: Vec<bool>` — 550M x 1 byte = 550MB (wastes 7/8 of space)
- `bitmap_y_bits: Vec<bool>` — ~100M x 1 byte = 100MB

Then it makes a second pass to feed them into LogArrayWriter/BitmapWriter.

**Steps:**
- [x] Write directly to `BitmapWriter` as triples arrive (eliminate intermediate `Vec<bool>`)
- [x] Track `max_subject`, `max_predicate`, `max_object` during the triple collection phase and pass them into the builder, so LogArrayWriter can be initialized with correct bit width upfront
- [x] Write directly to `LogArrayWriter` as triples arrive (eliminate intermediate `Vec<u64>`)
- [x] The BitmapTriples encoding is inherently streaming since triples arrive in SPO order: on subject change emit 1-bit to BitmapY; on predicate change within same subject emit 1-bit to BitmapY; for each triple push object to ArrayZ and 0/1 to BitmapZ
- [x] Update tests to verify identical output (all 101 tests pass)

### 1.2 Single-Pass Vocab Merge
**Impact:** Eliminates ~50% of merge I/O (currently reads all partial vocab files twice)
**Files:** `src/pipeline/vocab_merger.rs`

The current merger reads partial vocabularies twice:
- Pass 1 (lines 211-280): Count terms per section (shared/subject-only/object-only)
- Pass 2 (lines 286-381): Re-open files, re-merge, assign global IDs

This exists because subject-only IDs are offset by the shared count, which isn't known until
all terms are counted.

**Steps:**
- [ ] Choose approach (recommended: Option A — buffer merged entries in memory during single merge pass, since deduplicated unique terms are modest in size, ~26M entries for 550M triples)
- [ ] Option A: During the single merge, buffer merged `(term, roles, batch_sources)` entries. After merge completes, iterate buffer to assign global IDs.
- [ ] Option B (alternative): Pre-count with a lightweight first pass reading only roles bytes (skip term data) then do a full second pass. Cheaper than current but still two passes.
- [ ] Option C (alternative): Assign provisional IDs during single merge, then fix up subject-only and object-only offsets in the mapping arrays (no re-read of vocab files).
- [ ] Remove the file re-open and second merge loop
- [ ] Update tests to verify identical dictionary output

---

## Phase 2: Medium-Impact Performance & Safety Fixes

### 2.1 Fix Double Hash Lookup in `get_or_assign_id`
**Impact:** CPU savings on the hottest code path (called 3x per triple, billions of times)
**Files:** `src/pipeline/batch_vocab.rs`

Lines 112-115 hash the key twice — once for `get()`, once for `get_mut().unwrap()`:
```rust
if let Some(&(id, _existing_roles)) = self.so_term_map.get(term) {
    self.so_term_map.get_mut(term).unwrap().1 |= role;
    return id;
}
```

**Steps:**
- [x] Use `get_mut` directly instead of `get` + `get_mut().unwrap()` for a single lookup
- [x] Verify no regression in tests

### 2.2 Eliminate Redundant HashMap in `finish()`
**Impact:** Saves ~250MB per batch of redundant allocation
**Files:** `src/pipeline/batch_vocab.rs`

`finish()` (lines 147-176) creates a `std::collections::HashMap`, copies all terms from both
hashbrown maps into it, then converts to Vec and sorts.

**Steps:**
- [x] Collect entries directly from `so_term_map` and `p_term_map` into a single `Vec`
- [x] For terms appearing in both maps (rare: term used as both predicate and subject/object), merge during the Vec construction
- [x] Sort the Vec directly — no intermediate HashMap needed

### 2.3 Scale Arena Allocation to Batch Size
**Impact:** Better memory efficiency (currently wastes up to 490MB on small batches)
**Files:** `src/pipeline/mod.rs`

Line 139 hardcodes `bumpalo::Bump::with_capacity(500_000_000)`.

**Steps:**
- [x] Calculate arena capacity based on batch size: `batch_len * 100` bytes with a floor of 10MB
- [ ] ~Or simply use `Bump::new()` and let bumpalo grow dynamically~ (not needed, used dynamic sizing instead)

### 2.4 Add Bounds Checking in ID Remapper
**Impact:** Safety — prevents silent corruption, gives actionable error messages on malformed data
**Files:** `src/pipeline/id_remapper.rs`

Lines 41-43 do direct array indexing without bounds checks:
```rust
let global_subject = mapping.so_map[local_triple.subject as usize];
```

**Steps:**
- [x] Replace direct indexing with `.get()` and descriptive error messages (includes map size in error for debugging)

### 2.5 Remove Unused Error Channel
**Impact:** Code clarity
**Files:** `src/pipeline/mod.rs`

The `error_tx`/`error_rx` channels (line 285-286) are created but all senders are unused
(prefixed `_error_tx`). Errors propagate via thread join handles already.

**Steps:**
- [x] Remove `error_tx`, `error_rx`, and all `_error_tx` parameters from stage functions
- [x] Verify pipeline error propagation still works via thread joins

---

## Phase 3: Code Quality & Type Safety

### 3.1 Remove Dead Code from Old Architecture
**Impact:** Maintainability — removes ~500 lines of unused code and stale dependencies
**Files:** `src/dictionary/builder.rs`, `src/dictionary/sst.rs`, `src/triples/id_triple.rs`, `src/dictionary/mod.rs`, `src/triples/mod.rs`, `Cargo.toml`

**Steps:**
- [x] Remove old `build_dictionary` function from `dictionary/builder.rs` (kept `DictCounts`, removed `resolve_global_id` since it depended on removed SST types)
- [x] Remove `dictionary/sst.rs` entirely (SST not needed in single-pass pipeline)
- [x] Remove `generate_id_triples` and `SortedTriples` from `triples/id_triple.rs` (kept `IdTriple` struct and its `Sortable` impl)
- [x] Clean up `#[allow(dead_code)]` and `#[allow(unused_imports)]` annotations in mod files
- [x] Remove `memmap2` from `Cargo.toml` (no longer used by any live code)
- [x] Remove dead `add_triple` method from `BatchVocabBuilder` (changed to `#[cfg(test)]` since used by tests)
- [x] Verify all 101 tests still pass after removal (68 unit + 9 compat + 24 integration)

### 3.2 Introduce Named Structs for Tuple Types
**Impact:** Readability and safety
**Files:** `src/pipeline/mod.rs`, `src/pipeline/batch_vocab.rs`, `src/pipeline/vocab_merger.rs`

`ProcessedBatch.vocab` is `Vec<(Vec<u8>, u8, Option<u32>, Option<u32>)>` — opaque tuple.

**Steps:**
- [ ] Create a `VocabEntry` struct:
  ```rust
  struct VocabEntry {
      term: Vec<u8>,
      roles: u8,
      so_local_id: Option<LocalId>,
      p_local_id: Option<LocalId>,
  }
  ```
- [ ] Replace all tuple uses with the named struct in `ProcessedBatch`, `PartialVocabWriter`, and `vocab_merger`
- [ ] Similarly replace `batches_with_term: Vec<(usize, u8, Option<u32>, Option<u32>)>` in the merger with a named struct

### 3.3 Use Bitflags for Role Constants
**Impact:** Type safety — prevents invalid role values at the type level
**Files:** `src/pipeline/batch_vocab.rs`, `src/pipeline/vocab_merger.rs`, `src/pipeline/partial_vocab.rs`

**Steps:**
- [ ] Add `bitflags` crate to dependencies (or use a simple newtype wrapper with const methods)
- [ ] Replace `ROLE_SUBJECT: u8 = 0x01` etc. with a bitflags type:
  ```rust
  bitflags! {
      struct Roles: u8 {
          const SUBJECT   = 0x01;
          const PREDICATE = 0x02;
          const OBJECT    = 0x04;
          const GRAPH     = 0x08;
      }
  }
  ```
- [ ] Update all role flag usage throughout the pipeline modules

### 3.4 Use Newtypes for ID Spaces
**Impact:** Prevents accidentally mixing subject/object IDs with predicate IDs
**Files:** `src/pipeline/batch_vocab.rs`, `src/pipeline/id_remapper.rs`, `src/pipeline/vocab_merger.rs`

**Steps:**
- [ ] Evaluate whether converting `LocalId` from type alias to newtype (`struct LocalId(u32)`) is worth the ergonomic cost
- [ ] If adopted, consider separate newtypes for SO local IDs vs P local IDs
- [ ] If the ergonomic cost (`.0` everywhere, no arithmetic) is too high, document the convention instead

### 3.5 Fix Expensive `stats()` Method
**Impact:** Minor CPU savings
**Files:** `src/pipeline/batch_vocab.rs`

`stats()` creates a HashSet to count unique terms across both maps.

**Steps:**
- [x] Track count incrementally in `get_or_assign_id` with a `unique_term_count` field — on new insertion into either map, only increment if the term is absent from the other map
- [x] Replace `stats()` HashSet construction with O(1) counter return

---

## Phase 4: Future Performance Improvements

These are from the QLever analysis but are more complex and should only be pursued after
benchmarking shows they're needed.

### 4.1 Parallel N-Triples/N-Quads Parsing
**Impact:** ~3-4x parsing speedup for large N-Triples files
**QLever ref:** Recommendation 3.4 (MEDIUM IMPACT)
**Complexity:** High

**Steps:**
- [ ] For N-Triples: split decompressed input at newline boundaries into ~10MB blocks
- [ ] Parse blocks in parallel (one oxrdfio parser per block, or a simpler custom N-Triples parser)
- [ ] Collect results into the batched hash map via a bounded channel
- [ ] For Turtle/TriG: parse prefixes sequentially first, then parallel-parse the body with shared prefix map

### 4.2 Parallel K-Way Merge
**Impact:** Modest — merge is no longer the bottleneck after batched hash maps
**QLever ref:** Recommendation 3.7 (LOW IMPACT)

**Steps:**
- [ ] Implement recursive binary merge tree with parallel internal nodes (QLever's approach)

### 4.3 Double-Buffered Async I/O
**Impact:** Modest — overlaps computation with disk writes
**QLever ref:** Recommendation 3.8 (LOW IMPACT)

**Steps:**
- [ ] Implement compute-batch-N-while-writing-batch-N-1 pattern for vocab writer stage
- [ ] Apply to other stages where I/O and computation can overlap

---

## Phase 5: Test Coverage Gaps

### 5.1 Pipeline Component Unit Tests
**Impact:** Catches regressions in individual pipeline stages
**Files:** New test modules in `src/pipeline/` submodules

- [ ] Test `BatchVocabBuilder` with terms appearing in multiple roles (subject+predicate, predicate+object, all three)
- [ ] Test vocab merger with multiple batches containing overlapping terms, verify correct global IDs
- [ ] Test ID remapper with known mappings and verify output triples are correct
- [ ] Test pipeline with batches of exactly 1 triple (edge case)
- [ ] Test pipeline with more batches than channel capacity (exercises backpressure)

### 5.2 Stress / Property Tests
- [ ] Test with large synthetic datasets (100K+ triples) to verify memory stays bounded
- [ ] Test with many batches (very small batch size) to exercise merge with many partial vocabularies
- [ ] Test with a dataset where every term appears in every role (subject, predicate, and object)

### 5.3 Benchmarks
- [ ] Add criterion benchmarks for the hot paths: hash map lookup, vocab merge, BitmapTriples construction
- [ ] Establish baseline performance numbers for regression detection

---

## Recommended Execution Order

Prioritized to maximize value with minimal risk at each step:

| Step | Item | Rationale |
|------|------|-----------|
| ~~1~~ | ~~Phase 3.1 — Remove dead code~~ | ~~Done~~ |
| ~~2~~ | ~~Phase 2.5 — Remove error channel~~ | ~~Done~~ |
| ~~3~~ | ~~Phase 3.5 — Fix `stats()`~~ | ~~Done~~ |
| ~~4~~ | ~~Phase 2.1 — Fix double hash lookup~~ | ~~Done~~ |
| ~~5~~ | ~~Phase 2.2 — Eliminate redundant HashMap in `finish()`~~ | ~~Done~~ |
| ~~6~~ | ~~Phase 2.3 — Scale arena allocation~~ | ~~Done~~ |
| ~~7~~ | ~~Phase 2.4 — Bounds checking in remapper~~ | ~~Done~~ |
| ~~8~~ | ~~Phase 1.1 — Stream BitmapTriples~~ | ~~Done~~ |
| 9 | Phase 1.2 — Single-pass vocab merge | Second biggest improvement — halves merge I/O |
| 10 | Phase 3.2 — Named structs | Can be done alongside other work |
| 11 | Phase 3.3 — Bitflags for roles | Can be done alongside other work |
| 12 | Phase 5.1 — Pipeline unit tests | Should accompany phases 8-9 |
| 13 | Phase 3.4 — Newtype IDs | Evaluate after other changes settle |
| 14 | Phase 5.2-5.3 — Stress tests & benchmarks | After core improvements are in |
| 15 | Phase 4.1-4.3 — Parallel parsing etc. | Only if benchmarks show it's needed |
