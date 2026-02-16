# hdtc — HDT Creator

A Rust CLI tool for converting RDF files to HDT (Header, Dictionary, Triples) format.

## Project Overview

Converts input RDF files (any standard format) to HDT binary format, optimized for very large datasets (up to 100 billion triples) with bounded memory (default 4GB, configurable).

## Key Technical Decisions

- **RDF Parsing**: `oxrdfio` crate - supports all standard formats (N-Triples, N-Quads, Turtle, TriG, RDF/XML, JSON-LD, N3), streaming, actively maintained
- **CLI**: `clap` with derive API
- **HDT Format**: Fully compatible with hdt-java / hdt-cpp (standard binary format)
- **Dictionary**: Four-section dictionary with Plain Front Coding (PFC)
- **Triples**: BitmapTriples encoding in SPO order
- **Index**: Standard `.hdt.index.v1-1` (OPS order), implemented after core HDT generation is solid
- **Quads**: HDTQ approach (ESWC 2018) - standard BitmapTriples + graph dictionary + compressed graph membership bitmaps
- **Single-Pass Pipeline**: QLever-inspired architecture — per-batch hash maps assign local IDs during a single parse, partial vocabularies are k-way merged to build the global dictionary, local IDs are remapped to global IDs in parallel. No second parse needed.
- **Compressed Input**: Transparently handle .gz, .bz2, .xz based on file extension
- **Error Handling**: Skip malformed RDF with warning, report total skipped count at end
- **All RDF formats**: N-Triples, N-Quads, Turtle, TriG, RDF/XML, JSON-LD, N3

## Architecture

Single-pass, 6-stage streaming pipeline with bounded channels and backpressure:

1. **Parse** — Stream RDF input, batch quads (per-file blank node disambiguation)
2. **Batch Vocab** — Per-batch hash map assigns local IDs, arena-allocated terms (`bumpalo`)
3. **Vocab Writer** — Write zstd-compressed partial vocabularies and local-ID triples to disk
4. **Vocab Merger** — K-way merge partial vocabularies → assign global IDs, write PFC dictionary
5. **ID Remapper** — Convert local IDs to global IDs (parallel across batches)
6. **BitmapTriples** — External sort in SPO order, streaming BitmapTriples construction

Then assemble HDT file (header + dictionary + triples). Optionally build OPS index (.hdt.index.v1-1).

Key design points:
- Input is parsed **once** (no second pass). Local-ID triples are written during parsing; only a lightweight ID remap is needed after the vocabulary merge.
- Per-batch hash maps deduplicate terms early, reducing the data volume for sorting by ~60x compared to sorting all term occurrences.
- Adaptive batch sizing based on `--memory-limit` (1M–20M triples per batch).
- All intermediate files are zstd-compressed and automatically cleaned up.

### Resource Requirements

**Memory:** Default 4GB, configurable via `--memory-limit`. For 100B triples, recommend 16-32GB for optimal performance.

**Temporary Disk Space:** Temp files consist of partial vocabularies (deduplicated unique terms), local-ID triples, ID mappings, and SPO sort chunks — all zstd-compressed.
- **Rule of thumb:** ~6–10 bytes/triple after compression
- Examples: 500M triples ≈ 4GB, 10B triples ≈ 80GB, 100B triples ≈ 800GB
- Varies by term uniqueness and compressibility
- Specify temp directory with `--temp-dir` (uses system temp by default)
- Temp files automatically cleaned up after completion

**Output Size:** HDT file is typically 10-20% of uncompressed N-Triples size due to dictionary compression and BitmapTriples encoding.

## HDT Binary Format Reference

- **Byte order**: Little-endian throughout
- **Magic bytes**: `$HDT` (0x24 0x48 0x44 0x54)
- **CRCs**: CRC8-CCITT (0x07) after preambles, CRC16-ANSI (0x8005) after control info, CRC32C (0x1EDC6F41) after data payloads
- **VByte**: MSB=1 means this is the LAST byte (termination bit), 7 data bits per byte, little-endian order
- **Dictionary ID scheme**: Shared IDs [1..m], Subject-only IDs [m+1..], Object-only IDs [m+1..] (mapping 1), Predicates have separate ID space
- **BitmapTriples**: Three-level tree (S->P->O). ArrayY/BitmapY for predicates, ArrayZ/BitmapZ for objects. Bit=1 marks last child of parent (hdt-java convention).

## Conventions

- Rust 2024 edition (requires Rust 1.85+)
- Error handling: `anyhow` for application errors, `thiserror` for library-style error types
- Logging: `tracing` crate
- Testing: standard `#[cfg(test)]` modules + integration tests in `tests/`
- No `unsafe` code unless absolutely necessary and well-justified
- Blank node disambiguation: prefix blank node IDs with per-file identifier

## File Structure

```
src/
  main.rs          - CLI entry point, pipeline orchestration
  cli.rs           - CLI argument definitions (clap)
  rdf/             - RDF parsing, input discovery, streaming, compressed input
  dictionary/      - Dictionary construction, PFC encoding
  triples/         - BitmapTriples encoding (streaming)
  hdt/             - HDT file serialization (header, dictionary, triples sections)
  index/           - HDT index file generation (.hdt.index.v1-1)
  io/              - VByte, LogArray, Bitmap, CRC utilities, Control Information
  pipeline/        - 6-stage pipeline (batch vocab, partial vocab, merger, ID remapper)
  quads/           - HDTQ quad support (graph dictionary, membership bitmaps)
  sort/            - External merge sort
tests/
  integration_test.rs - End-to-end pipeline tests
  compat_test.rs      - Compatibility tests against the hdt crate
  data/               - Sample RDF fixtures
```

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| `clap` | CLI argument parsing (derive API) |
| `oxrdfio` | RDF parsing (all standard formats, streaming) |
| `anyhow` | Application error handling |
| `thiserror` | Structured error types |
| `tracing` / `tracing-subscriber` | Structured logging |
| `crc` | CRC checksum computation (CRC8, CRC16, CRC32C) |
| `rayon` | Data parallelism for sorting and processing |
| `tempfile` | Temporary file management |
| `crossbeam-channel` | Bounded MPMC channels (pipeline backpressure) |
| `hashbrown` | High-performance hash maps (batch vocabulary) |
| `bumpalo` | Arena allocator (per-batch term storage) |
| `bitflags` | Type-safe role flags |
| `indicatif` | Progress bars and status reporting |
| `flate2` | Gzip decompression |
| `bzip2` | Bzip2 decompression |
| `xz2` / `liblzma` | XZ/LZMA decompression |
| `zstd` | Zstandard compression for temp files |
