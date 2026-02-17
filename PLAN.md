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

- [x] Create a `VocabEntry` struct with `term`, `roles`, `so_local_id`, `p_local_id` fields in `batch_vocab.rs`
- [x] Replace all tuple uses with `VocabEntry` in `ProcessedBatch`, `BatchVocabBuilder::finish()`, `PartialVocabWriter` (via `PartialVocabEntry::from_vocab_entry`)
- [x] Replace `batches_with_term: Vec<(usize, u8, Option<u32>, Option<u32>)>` with `TermBatchInfo` struct in the merger

### 3.3 Use Bitflags for Role Constants

**Impact:** Type safety — prevents invalid role values at the type level
**Files:** `src/pipeline/batch_vocab.rs`, `src/pipeline/vocab_merger.rs`, `src/pipeline/partial_vocab.rs`

**Steps:**

- [x] Add `bitflags` crate to dependencies
- [x] Replace `ROLE_SUBJECT: u8 = 0x01` etc. with `Roles` bitflags type in `batch_vocab.rs`
- [x] Update all role flag usage throughout pipeline modules (`batch_vocab.rs`, `mod.rs`, `vocab_merger.rs`, `partial_vocab.rs`), using `contains()` and `intersects()` for flag checks and `.bits()` / `from_bits_truncate()` for serialization

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

### 4.4 Stage 4 Acceleration Plan (Low-Memory First)

**Impact:** Expected 5-12% end-to-end on large datasets where Stage 4 is 25-35% of runtime
**Goal:** Speed up Stage 4 without materially increasing peak RAM
**Why now:** Recent profiling shows Stage 4 can take ~50-55s on ~73.5M triples

This sub-plan intentionally prioritizes reducing duplicate merge work before introducing a
parallel merge tree. It is designed to keep memory bounded and avoid "buffer-every-term"
strategies.

#### Phase A — Instrument Stage 4 precisely (required gate)

**Files:** `src/pipeline/vocab_merger.rs`, `src/pipeline/mod.rs`

**Steps:**

- [x] Add separate timers/counters for: pass 1 read, pass 2 read, ID assignment, PFC writes, mapping writes
- [x] Log bytes read from partial vocab files in each pass
- [x] Capture baseline over 3 runs on the same dataset (median + p95)

**Exit criterion:** We can quantify exactly how much time is spent re-reading and re-merging.

#### Phase B — Reduce second-pass cost without extra RAM

**Files:** `src/pipeline/vocab_merger.rs`, `src/pipeline/partial_vocab.rs`

**Approach:** Keep two logical passes, but make pass 1 lightweight by reading only the fields
needed for section counts (roles) and skipping term materialization/clone work.

**Steps:**

- [ ] Add a lightweight partial-vocab scan path for count-only pass (roles + presence of IDs, no term clone)
- [ ] Keep existing pass-2 correctness semantics and output format unchanged
- [ ] Verify dictionary byte-for-byte compatibility on representative fixtures

**Memory guardrail:** Do not retain merged term buffers across the full vocabulary.

**Expected gain:** Typically 10-25% Stage 4 reduction when pass-1 term decoding is expensive.

#### Phase C — Optional parallel merge tree (only if still justified)

**Files:** `src/pipeline/vocab_merger.rs` (and helper module if needed)

**Approach:** Recursive binary merge tree with bounded channels at internal nodes; preserve the
same deterministic ordering and ID assignment as current single-threaded merge.

**Steps:**

- [x] Prototype 2-way internal merge node with bounded output buffer
- [x] Compose nodes into fixed fan-in tree over partial vocab inputs
- [x] Keep final ID assignment single-writer deterministic path
- [x] Add backpressure + cancellation propagation to avoid deadlocks on error

**Memory guardrail:** Max in-flight entries bounded by `node_count * channel_capacity`; cap via config.

**Expected gain:** Additional 10-30% Stage 4 reduction depending on CPU cores and storage.

#### Validation & rollback criteria

**Steps:**

- [x] Benchmark before/after with same input and `--memory-limit` (3-5 runs, compare medians)
- [x] Fail plan if peak RSS increases > 10% without proportional speedup
- [x] Fail plan if end-to-end gain < 3% after Phase B + C (then keep simpler implementation)
- [x] Keep each phase as a separate commit to allow clean rollback

#### Function-level implementation checklist (do-this-next)

**Phase A touchpoints (instrumentation):**

- [x] In `merge_vocabularies` (`src/pipeline/vocab_merger.rs`), add stage-local timers around:
  - [x] reader initialization (`PartialVocabReader::open` + first `read_entry`)
  - [x] pass 1 merge/count loop
  - [x] pass 2 merge/ID-assignment loop
  - [x] dictionary section serialization (`shared_enc.write_to`, `subjects_enc.write_to`, `predicates_enc.write_to`, `objects_enc.write_to`)
  - [x] mapping writes (`IdMapping::write_to_file` loop)
- [x] In `merge_vocabularies`, add compressed-byte counters consumed in pass 1 and pass 2
- [x] Emit one `tracing::info!` summary with: pass1/pass2 timings, bytes read, mapping-write time

**Phase B touchpoints (lightweight pass 1):**

- [ ] In `src/pipeline/partial_vocab.rs`, add a count-only scan API (e.g., `scan_roles_only`) that:
  - [ ] reads `term_len`, skips term bytes without allocating term vectors
  - [ ] reads roles byte and skips optional SO/P local IDs based on roles
  - [ ] returns role aggregates needed for `count_term_section`
- [ ] In `merge_vocabularies`, switch pass 1 to the roles-only scan path
- [ ] Keep pass 2 on full `read_entry` and preserve current `assign_global_ids_and_record_mappings` logic
- [ ] Add equivalence checks in tests for unchanged `DictCounts`, dictionary bytes, and mapping outputs

**Phase C touchpoints (optional parallel merge tree):**

- [ ] Extract current heap-based merge into a reusable 2-way merge primitive (new helper module under `src/pipeline/`)
- [x] Build recursive merge tree with bounded channels between internal merge nodes
- [x] Keep final global ID assignment in a single deterministic consumer stage
- [x] Add cancellation path: any worker error closes channels and propagates one root error
- [x] Add deadlock-safety tests (small channel capacities + injected early errors)

**Definition of done for 4.4:**

- [x] Stage 4 median runtime reduced by >= 15% on the 73.5M-triple benchmark (or documented reason if not)
- [x] Peak RSS increase <= 10%
- [x] Existing `pipeline::vocab_merger` tests remain green
- [x] End-to-end `cargo test` remains green

---

## Phase 5: Test Coverage Gaps

### 5.1 Pipeline Component Unit Tests

**Impact:** Catches regressions in individual pipeline stages
**Files:** New test modules in `src/pipeline/` submodules

- [x] Test `BatchVocabBuilder` with terms appearing in multiple roles (subject+predicate, predicate+object, all three)
- [x] Test vocab merger with multiple batches containing overlapping terms, verify correct global IDs
- [x] Test ID remapper with known mappings and verify output triples are correct
- [x] Test pipeline with batches of exactly 1 triple (edge case)
- [x] Test pipeline with more batches than channel capacity (exercises backpressure)

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

| Step   | Item                                                      | Rationale                                     |
| ------ | --------------------------------------------------------- | --------------------------------------------- |
| ~~1~~  | ~~Phase 3.1 — Remove dead code~~                          | ~~Done~~                                      |
| ~~2~~  | ~~Phase 2.5 — Remove error channel~~                      | ~~Done~~                                      |
| ~~3~~  | ~~Phase 3.5 — Fix `stats()`~~                             | ~~Done~~                                      |
| ~~4~~  | ~~Phase 2.1 — Fix double hash lookup~~                    | ~~Done~~                                      |
| ~~5~~  | ~~Phase 2.2 — Eliminate redundant HashMap in `finish()`~~ | ~~Done~~                                      |
| ~~6~~  | ~~Phase 2.3 — Scale arena allocation~~                    | ~~Done~~                                      |
| ~~7~~  | ~~Phase 2.4 — Bounds checking in remapper~~               | ~~Done~~                                      |
| ~~8~~  | ~~Phase 1.1 — Stream BitmapTriples~~                      | ~~Done~~                                      |
| 9      | Phase 1.2 — Single-pass vocab merge                       | Second biggest improvement — halves merge I/O |
| ~~10~~ | ~~Phase 3.2 — Named structs~~                             | ~~Done~~                                      |
| ~~11~~ | ~~Phase 3.3 — Bitflags for roles~~                        | ~~Done~~                                      |
| ~~12~~ | ~~Phase 5.1 — Pipeline unit tests~~                       | ~~Done~~                                      |
| ~~13~~ | ~~Phase 6.1 — Index creation (post-HDT)~~                 | ~~Done~~                                      |
| 14     | Phase 3.4 — Newtype IDs                                   | Evaluate after other changes settle           |
| 15     | Phase 5.2-5.3 — Stress tests & benchmarks                 | After core improvements are in                |
| ~~16~~ | ~~Phase 6.3 — Standalone index CLI command~~              | ~~Done~~                                      |
| 17     | Phase 6.2 — In-pipeline index creation                    | Future optimization if post-HDT is slow       |
| 18     | Phase 4.1-4.3 — Parallel parsing etc.                     | Only if benchmarks show it's needed           |

---

## Phase 6: HDT Index Creation

### Background

The `.hdt.index.v1-1` file is a **complex sidecar index**, not just reordered triples. Based on hdt-java source analysis, it contains five sections:

1. **Control Info** (type=INDEX, format=indexFoQ)
2. **BitmapIndexZ** (bitmap for object tracking in permutation)
3. **IndexZ** (sequence of object IDs in permuted order)
4. **PredicateIndex** = Bitmap (predicate boundaries) + Sequence (position mappings)
5. **PredicateCount** (predicate occurrence statistics)

The index enables efficient **predicate-based queries** through an **inverted index** that maps predicate values to their positions. The index covers **a single permutation** (typically OPS for Object-Predicate-Subject queries).

**Complexity:** Creating the index is nearly as expensive as creating the main HDT:

- Must decode SPO triples, column-swap, and re-sort to chosen permutation
- Must build full BitmapTriples encoding in new order
- Must build PredicateIndex inverted structures from seqY values

The index can be created:

1. **Post-HDT** (primary): Read SPO HDT → decode → permute → re-sort → build full BitmapTriples + indices
2. **In-pipeline** (future): Create during pipeline alongside SPO (overlaps I/O, can save 20-40% time)
3. **Standalone** (future): Separate CLI command to create index for existing HDT files

### 6.1 Post-HDT Index Creation (Primary Implementation)

**Impact:** Enables index generation with `--index` flag; moderately slower than in-pipeline but simpler
**Files:** `src/index/mod.rs`, `src/index/builder.rs`, `src/hdt/mod.rs` (for index write support)
**Dependencies:** Read HDT file's triples section, apply column permutation, re-sort in OPS order

**Approach:**

1. After main HDT file is written, if `--index` flag is set:
   - Open the HDT file and read the SPO BitmapTriples section
   - Decode SPO triples back to (subject, predicate, object) ID tuples
   - Permute columns and sort in OPS order: `(object, predicate, subject)`
   - Build OPS BitmapTriples structure (seqY, seqZ, bitmapY, bitmapZ)
   - Build PredicateIndex inverted structures from seqY values
   - Write index file with five sections

**Steps:**

- [x] Create `src/index/decoder.rs` to decode existing HDT BitmapTriples:
  - [x] Read and decompress seqY, seqZ, bitmapY, bitmapZ from main HDT file
  - [x] Implement `BitmapTriplesDecoder` iterator to yield (S, P, O) tuples
  - [x] Handle AdjacencyList traversal pattern for multi-level bitmap decoding
- [x] Create `src/index/ops_triple.rs` with OPS triple wrapper:
  - [x] Implement `OpsTriple` struct with (Object, Predicate, Subject) ordering
  - [x] Implement `Sortable` trait for external sorter compatibility
- [x] Create `src/index/predicate_index.rs` with PredicateIndex building logic:
  - [x] `build_predicate_index()` builds inverted index structures
  - [x] `build_predicate_count()` builds per-predicate occurrence counts
  - [x] Implements hdt-java PredicateIndexArray algorithm
- [x] Create `src/index/writer.rs` to write index file:
  - [x] `write_index()` writes magic bytes and control info
  - [x] Write five sections: bitmapIndexZ, indexZ, predicateIndex (bitmap + sequence), predicateCount
  - [x] Proper control info with format=indexFoQ, order=6 (OPS)
- [x] Integrate into main pipeline:
  - [x] In `main.rs`, after `hdt::write_hdt()`, check `cli.index` flag
  - [x] If set, call `index::create_index(&hdt_path, memory_budget, &temp_dir)`
  - [x] ExternalSorter auto-cleans temp files
- [x] Test index creation with sample RDF data and verify file format

**Index File Format (confirmed by hdt-java source):**

- Magic bytes: `$HDT` (0x24 0x48 0x44 0x54)
- ControlInfo (type=INDEX, format=indexFoQ, numTriples, order=OPS enum ordinal=6)
- Five binary sections in sequence (no intermediate HDT headers):
  1. bitmapIndexZ (bitmap from OPS BitmapTriples Z component)
  2. indexZ (sequence from OPS BitmapTriples Z component)
  3. predicateIndex.bitmap (inverted index boundaries for predicates)
  4. predicateIndex.sequence (position mappings for predicate occurrences)
  5. predicateCount (occurrence counts per predicate)
- CRC32 after all data (standard HDT format)

### 6.2 In-Pipeline Index Creation (Future Optimization)

**Impact:** 20-40% faster index creation vs post-HDT; overlaps I/O with computation
**QLever ref:** Recommendation 2.5 (Cascading twin-permutation creation)
**Complexity:** High — requires architectural changes to pipeline and triple sorting

**Approach:**
Modify the external sort stage (Stage 6) to produce two sorted outputs simultaneously:

1. **Main path:** Sort global-ID triples in SPO order → build SPO BitmapTriples for main HDT
2. **Index path:** In parallel, sort _copies_ in OPS order → build OPS BitmapTriples + PredicateIndex for index file

This overlaps the sorts and I/O, avoiding a second full re-sort of triples.

**Trade-offs:**

- **Pros:** Avoids re-reading HDT file (expensive I/O), overlaps sorting, saves 20-40% time
- **Cons:** Requires dual-sort tracking, more memory during pipeline, more complex architecture
- **Only pursue if:** Post-HDT profiling shows index creation is a significant bottleneck (>10% of total time)

**Steps (if pursued):**

- [ ] Profile Phase 6.1 implementation to measure actual overhead
- [ ] Measure I/O time for re-reading HDT file vs dual-sort memory cost
- [ ] If index creation overhead > 10% of total: refactor external sorter to support dual-output
- [ ] Modify BitmapTriples builder to accept permutation order parameter
- [ ] Run two BitmapTriples builders in parallel (one SPO, one OPS) from sorted triple sources
- [ ] Write both HDT and index files from builder outputs

### 6.3 Standalone Index CLI Command

**Impact:** User convenience — can create index for existing HDT files without reprocessing
**Files:** `src/main.rs`, `src/cli.rs`, `src/index/mod.rs`

**Approach:**
Add a new CLI subcommand: `hdtc index <HDT_FILE>` that:

1. Reads an existing HDT file
2. Builds OPS index using logic from Phase 6.1
3. Writes `.hdt.index.v1-1` file

**Steps:**

- [x] Refactor CLI from flat structure to subcommand-based (both `create` and `index`)
- [x] Update `src/cli.rs` to use clap's subcommand structure with `Commands` enum
- [x] Create `CreateArgs` struct for RDF-to-HDT conversion
- [x] Create `IndexArgs` struct for index creation from existing HDT
- [x] Split `main()` into `create_hdt()` and `create_index_from_hdt()` handlers
- [x] Update test harnesses (`tests/common/mod.rs`, `tests/integration_test.rs`) to use new CLI
- [x] Verify all 129 tests pass with new subcommand structure
- [x] Update README.md with new CLI usage documentation

**CLI Usage:**

```sh
# Create HDT with index in one step
hdtc create data.nt -o data.hdt --index

# Create HDT, then add index separately
hdtc create data.nt -o data.hdt
hdtc index data.hdt  # Creates data.hdt.index.v1-1
```

**Verified:**

- Both subcommands work correctly with all relevant options (--memory-limit, --temp-dir)
- Help text displays correctly for both `hdtc create --help` and `hdtc index --help`
- All integration and compatibility tests pass
- [ ] Test with various HDT files (including sample vdjserver.hdt)

---
