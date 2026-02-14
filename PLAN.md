# Implementation Plan: hdtc (HDT Creator)

## Overview

Build `hdtc`, a Rust CLI tool that converts RDF files to HDT format, optimized for massive datasets (100B+ triples) under constrained memory (~20GB). The output HDT files must be fully compatible with existing HDT tools (hdt-java, hdt-cpp).

## Resolved Decisions

- **Compatibility**: Fully compatible with hdt-java/hdt-cpp (output loadable by those tools)
- **Quads**: HDTQ approach (annotated triples + graph dictionary + compressed graph membership bitmaps)
- **Error handling**: Skip malformed RDF with warning, report total skipped count at end
- **Compressed input**: Transparently handle .gz, .bz2, .xz
- **RDF formats**: All standard formats via oxrdfio (N-Triples, N-Quads, Turtle, TriG, RDF/XML, JSON-LD, N3)
- **Term-to-ID lookup**: Custom sorted string table (SST) with sparse in-memory block index, memory-mapped via `memmap2`
- **Index priority**: After core HDT generation is working and tested

---

## Phase 1: Project Foundation

### 1.1 Project Setup
- Initialize Cargo project
- Add dependencies: `clap`, `oxrdfio`, `anyhow`, `thiserror`, `tracing`, `tracing-subscriber`, `crc`, `rayon`, `tempfile`, `memmap2`, `indicatif`, `flate2`, `bzip2`, `xz2`
- Set up basic project structure with module stubs

### 1.2 CLI Definition
- Define CLI with `clap` derive API
- Arguments:
  - Input files/folders (positional, multiple)
  - `--output` / `-o`: output HDT file path (required)
  - `--mode`: `triples` or `quads` (default: triples)
  - `--temp-dir`: location for temporary working files (default: system temp)
  - `--index`: generate `.hdt.index.v1-1` file (flag)
  - `--graph-map`: mapping of input files/folders to named graphs (for quads mode)
  - `--default-graph`: URI for the default graph (for quads mode)
  - `--threads`: number of threads (default: available cores)
  - `--memory-limit`: soft memory limit for internal buffers
  - `--base-uri`: base URI for the dataset
  - Verbosity flags (`-v`, `-q`)

### 1.3 Input Discovery
- Recursively walk input directories
- Detect RDF format from file extensions (`.nt`, `.nq`, `.ttl`, `.trig`, `.rdf`, `.xml`, `.jsonld`, `.n3`)
- Strip compression extensions to detect base format (e.g., `.nt.gz` -> N-Triples)
- Open compressed files through appropriate decompression reader
- Validate that input files exist and are readable

---

## Phase 2: Low-Level HDT Building Blocks

### 2.1 Binary Encoding Utilities (`src/io/`)
- **VByte**: Variable-byte integer encoding/decoding (MSB continuation bit, 7 data bits, LE order)
- **LogArray** (Log64): Bit-packed integer arrays with configurable bits-per-entry, metadata preamble (format byte + VByte count + CRC8), data payload + CRC32C
- **Bitmap**: Bit sequences, metadata preamble (format byte + VByte count + CRC8), data payload + CRC32C
- **CRC**: CRC8-CCITT (poly 0x07), CRC16-ANSI (poly 0x8005), CRC32C (poly 0x1EDC6F41) — use `crc` crate
- **Control Information**: `$HDT` magic, type byte, null-terminated format URI, semicolon-separated properties, null terminator, CRC16
- Full unit tests for each primitive, including round-trip and known-value tests

### 2.2 Front-Coded Dictionary Section (`src/dictionary/`)
- PFC (Plain Front Coding) encoder:
  - Block-based encoding with configurable block size (default 16)
  - First string in each block: stored verbatim + null terminator
  - Subsequent strings: VByte(shared_prefix_length) + suffix + null terminator
  - Block offset index as LogArray
  - Section layout: VByte(string_count) + VByte(buffer_length) + CRC8 + LogArray(block_offsets) + Buffer(encoded_strings) + CRC32C
- PFC decoder (for testing/verification and potential read-back)
- Unit tests with known byte sequences and round-trip verification

---

## Phase 3: Dictionary Construction (Disk-Backed)

### 3.1 Term Extraction (Pass 1a)
- Stream all input RDF files using `oxrdfio` (streaming quad parser)
- For each quad, extract subject, predicate, object (and graph in quads mode)
- Serialize each term to its canonical N-Triples/N-Quads string form
- Disambiguate blank nodes: prefix with per-file identifier (e.g., `_:f{file_index}_original_id`)
- Accumulate `(term_string, role_flags)` records in memory buffer
- Role flags: bitmask of Subject=0x01, Predicate=0x02, Object=0x04, Graph=0x08
- When buffer reaches memory budget threshold, sort by term string, flush to temp file
- Use length-prefixed binary format for temp records

### 3.2 External Merge Sort of Terms (Pass 1b)
- K-way merge of sorted chunk files using a min-heap
- During merge: deduplicate consecutive identical terms, OR their role flags together
- Output: single sorted file of unique `(term_string, role_flags)` records
- Multi-threaded: merge can use parallel readers with prefetching

### 3.3 Dictionary Partitioning and ID Assignment (Pass 1c)
- Single scan through sorted unique terms
- Partition by role flags:
  - **Shared** (S∩O): terms with both Subject and Object flags → IDs [1..m]
  - **Subjects-only**: Subject flag only → IDs [m+1..]
  - **Predicates**: Predicate flag → separate ID space [1..p]
  - **Objects-only**: Object flag only → IDs [m+1..] (mapping 1)
  - **Graphs** (quads mode): Graph flag → separate ID space [1..g]
- Write each partition to PFC-encoded dictionary section
- Simultaneously write entries to sorted SST file for Pass 2 lookups

### 3.4 Sorted String Table (SST) for Term-to-ID Lookup
- Custom write-once, read-many data structure optimized for our exact access pattern
- **Build** (during Phase 3.3, entries arrive already sorted from merge sort):
  - Write `(term_string, section_id, term_id)` records sequentially to a flat file
  - Records use length-prefixed format: `u32 key_len | key_bytes | u8 section | u64 id`
  - Every Nth record (e.g., N=1024), emit a sparse index entry: `(key_hash, file_offset)`
  - After all records written, write the sparse index as a separate file (or appended with offset marker)
- **Sparse block index** (held in memory during Pass 2):
  - Array of `(key_prefix_or_hash, file_offset)` entries
  - For 10B unique terms with N=1024: ~10M index entries, ~150MB RAM
  - Loaded into memory at start of Pass 2
- **Lookup** (during Phase 4.1):
  - Binary search the in-memory sparse index to find the enclosing block
  - Seek to block offset in memory-mapped SST file
  - Linear scan within the block (up to N records) comparing keys
  - 1 page fault per lookup on average (OS page cache handles hot blocks)
- **Memory mapping**: SST file opened via `memmap2` for zero-copy reads; OS manages page cache
- **Advantages over RocksDB**:
  - Zero write amplification (single sequential write pass)
  - Single disk read per lookup (vs 2-5 for LSM tree)
  - Predictable ~150MB memory footprint for index (vs multi-GB for RocksDB caches/bloom filters)
  - No C++ dependency, no compaction overhead
  - Trivial cleanup: delete the temp file
- **Temporary**: SST file deleted after HDT generation completes

---

## Phase 4: Triple Encoding

### 4.1 ID Triple Generation (Pass 2)
- Stream all input RDF files again using `oxrdfio`
- Re-apply same blank node disambiguation (same per-file prefixing)
- Look up each term in the SST to get `(section, id)`
- Compute the global ID for subjects and objects:
  - Shared terms: use shared ID directly
  - Subject-only: shared_count + subject_only_index
  - Object-only: shared_count + object_only_index (mapping 1)
- Write `(subject_id: u64, predicate_id: u64, object_id: u64)` tuples to temp files (24 bytes each, fixed-width)
- In quads mode: also write `(triple_index, graph_id)` pairs to a separate temp file

### 4.2 External Sort of ID Triples
- External merge sort of the fixed-width `(s, p, o)` records in SPO order
- Compare by S first, then P, then O
- Deduplicate identical triples during merge (important: duplicates can arise from multiple input files)
- Output: sorted, deduplicated ID triples in temp file

### 4.3 BitmapTriples Construction
- Stream sorted SPO triples sequentially
- Track previous subject and predicate to detect boundaries
- Build four structures incrementally:
  - **ArrayY (Sp)**: append predicate ID whenever (subject, predicate) changes
  - **BitmapY (Bp)**: set bit=1 when subject changes, bit=0 otherwise
  - **ArrayZ (So)**: append object ID for every triple
  - **BitmapZ (Bo)**: set bit=1 when (subject, predicate) changes, bit=0 otherwise
- Write to output file using LogArray and Bitmap binary formats
- Control Information: format=`http://purl.org/HDT/hdt#triplesBitmap`, order=SPO (1), numTriples=count

---

## Phase 5: HDT File Assembly

### 5.1 Header Section
- Generate RDF metadata as N-Triples string using VoID vocabulary:
  - `_:header rdf:type void:Dataset`
  - `void:triples`, `void:distinctSubjects`, `void:properties`, `void:distinctObjects`
  - `void:entities` (shared count)
  - `dcterms:source` (base URI)
  - `dcterms:issued` (generation timestamp)
  - HDT properties: `hdtDictionary`, `hdtTriples` format URIs
- Control Information: type=Header (1), format=`ntriples`, property `length`=byte_count

### 5.2 File Assembly Order
1. **Global Control Information**: type=Global (0), format=`http://purl.org/HDT/hdt#HDTv1`, properties: BaseURI, Software
2. **Header**: Control Info + N-Triples metadata bytes
3. **Dictionary**: Control Info (format=`http://purl.org/HDT/hdt#dictionaryFour`, mapping=1, elements=total) + Shared PFC + Subjects PFC + Predicates PFC + Objects PFC
4. **Triples**: Control Info + BitmapY + ArrayY + BitmapZ + ArrayZ

### 5.3 Verification
- Read back the generated HDT file section by section
- Verify all CRC checksums pass
- Verify triple count matches expected
- Verify dictionary section counts are consistent

---

## Phase 6: HDT Index Generation

*Implemented after core HDT generation is working and tested.*

### 6.1 OPS Index Construction
- Re-sort the ID triples by (O, P, S) order using external merge sort (can reuse the ID triples from Phase 4)
- Build index structures:
  - **bitmapIndex** (BitSequence375): marks boundaries between object groups
  - **arrayIndex** (LogSequence2): Y-axis position references for each entry
  - **predicateIndex**: maps predicates to their positions
  - **predicateCount**: occurrence count per predicate
- Write `.hdt.index.v1-1` file with Control Information header

### 6.2 Format Compatibility
- Reverse-engineer the exact binary format from hdt-cpp source code
- Verify generated index files are loadable by hdt-cpp/hdt-java

---

## Phase 7: HDTQ Quads Support

*Implemented after triples HDT is fully working.*

### 7.1 Graph Dictionary
- Fifth dictionary section for graph terms
- PFC encoded, separate ID space [1..g]

### 7.2 Graph Membership Tracking
- During Pass 2 (ID generation), emit `(triple_index, graph_id)` pairs
- Sort by triple_index
- For each unique triple, build a set of graph IDs it belongs to

### 7.3 Quad Information Component
- HDTQ Annotated Triples (HDT-AT) approach:
  - For each triple, store a compressed bitset of graph memberships
  - Use Roaring Bitmaps for efficient compression of sparse bitsets
- Append after the standard Triples section in the HDT file
- Add appropriate Control Information for the quad section

### 7.4 Graph Mapping CLI
- `--graph-map file1.nt=http://example.org/graph1` syntax
- `--graph-map dir/=http://example.org/graphs/` for directories (appends filename)
- `--default-graph http://example.org/default` for triples without explicit graph

---

## Phase 8: Performance and Polish

### 8.1 Multi-Threading
- **Parallel parsing**: Multiple input files parsed concurrently (bounded channel to back-pressure)
- **Parallel chunk sorting**: Use `rayon` for in-memory sort of term chunks
- **Parallel external merge**: K-way merge with prefetch threads
- **Pipeline parallelism**: Overlap parsing with term extraction and chunk writing

### 8.2 Progress Reporting
- Phase-level progress bars (using `indicatif`)
- Per-phase metrics: items processed, throughput (items/sec), elapsed time
- Overall pipeline progress indicator
- Memory usage reporting (via `jemalloc` stats or `/proc/self/status`)

### 8.3 Memory Management
- Configurable memory budget via `--memory-limit` (default: auto-detect ~50% of available RAM)
- Budget split across sort buffers, SST block index, I/O buffers
- Chunk flush threshold based on budget
- Monitor and log actual memory usage periodically

---

## Phase 9: Testing Strategy

### 9.1 Unit Tests (per module)
- VByte encoding/decoding round-trips and known values
- LogArray: various bit widths, boundary values, CRC verification
- Bitmap: set/get bits, CRC verification
- Control Information: write/read round-trip
- PFC dictionary: encode/decode round-trips, known byte sequences from hdt-cpp
- BitmapTriples: construction from small known triple sets, verify structure
- CRC: verify against known checksums

### 9.2 Integration Tests
- Small hand-crafted RDF files (5-50 triples) -> HDT, verify structure
- Multiple RDF formats as input (N-Triples, Turtle, RDF/XML, etc.)
- Multiple input files merged correctly
- Blank node disambiguation across files
- Compressed input files (.gz, .bz2, .xz)
- Duplicate triple elimination
- Malformed input skipping with warning count

### 9.3 Compatibility Tests
- Generate reference HDT files using hdt-java or hdt-cpp from same inputs
- Verify our output is loadable by hdt-java/hdt-cpp
- Structural comparison: same dictionary entries, same triple count, same ID assignments
- If possible: load our HDT with the existing `hdt` Rust crate for querying verification

### 9.4 Scale Tests
- Synthetic datasets: 1K, 100K, 10M, 100M triples
- Verify memory usage stays within configured limits
- Benchmark throughput and identify bottlenecks
- Test with multiple concurrent input files

---

## Implementation Order

The recommended order prioritizes getting a working end-to-end pipeline early, then layering on scale, quality, and features:

1. **Phase 1** (Foundation) — project setup, CLI, input discovery
2. **Phase 2** (Building blocks) — VByte, LogArray, Bitmap, CRC, Control Info, PFC
3. **Phase 3** (Dictionary) — term extraction, external sort, partitioning, SST lookup
4. **Phase 4** (Triples) — ID generation, external sort, BitmapTriples
5. **Phase 5** (Assembly) — header, file assembly, verification
6. **Phase 9.1-9.3** (Testing) — unit tests, integration tests, compatibility tests
7. **Phase 8** (Performance) — multi-threading, progress, memory management
8. **Phase 6** (Index) — OPS index generation
9. **Phase 7** (Quads) — HDTQ support
10. **Phase 9.4** (Scale tests) — large-scale benchmarking

---

## Dependency Summary

| Crate | Purpose |
|-------|---------|
| `clap` | CLI argument parsing (derive API) |
| `oxrdfio` | RDF parsing (all standard formats, streaming) |
| `anyhow` | Application error handling |
| `thiserror` | Structured error types |
| `tracing` / `tracing-subscriber` | Structured logging |
| `crc` | CRC checksum computation (CRC8, CRC16, CRC32C) |
| `rayon` | Data parallelism for sorting |
| `tempfile` | Temporary file/directory management |
| `memmap2` | Memory-mapped file I/O (SST lookup, potentially LogArray/Bitmap reads) |
| `indicatif` | Progress bars and status |
| `flate2` | Gzip decompression |
| `bzip2` | Bzip2 decompression |
| `xz2` / `liblzma` | XZ/LZMA decompression |
| `roaring` | Roaring Bitmaps (for HDTQ graph membership) |
| `walkdir` | Recursive directory traversal |
