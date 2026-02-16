# hdtc — HDT Creator

A Rust CLI tool for converting RDF files to HDT (Header, Dictionary, Triples) format.

## Project Overview

Converts input RDF files (any standard format) to HDT binary format, optimized for very large datasets (up to 100 billion triples) with constrained memory (~20GB).

## Key Technical Decisions

- **RDF Parsing**: `oxrdfio` crate - supports all standard formats (N-Triples, N-Quads, Turtle, TriG, RDF/XML, JSON-LD, N3), streaming, actively maintained
- **CLI**: `clap` with derive API
- **HDT Format**: Fully compatible with hdt-java / hdt-cpp (standard binary format)
- **Dictionary**: Four-section dictionary with Plain Front Coding (PFC)
- **Triples**: BitmapTriples encoding in SPO order
- **Index**: Standard `.hdt.index.v1-1` (OPS order), implemented after core HDT generation is solid
- **Quads**: HDTQ approach (ESWC 2018) - standard BitmapTriples + graph dictionary + compressed graph membership bitmaps
- **Term-to-ID Lookup**: Custom sorted string table (SST) with sparse in-memory block index, memory-mapped via `memmap2`. Write-once during dictionary construction (entries arrive sorted), read-many during Pass 2. Adaptive block size balances speed vs memory: 64-128 for <1B terms (~100MB index), 256-512 for 1-50B terms (~1.5GB index), 1024 for 50B+ terms (~12GB index for 100B terms). Override with `--sst-block-size`. 1 disk read per lookup vs 2-5 for LSM-based stores. No C++ dependency.
- **Compressed Input**: Transparently handle .gz, .bz2, .xz based on file extension
- **Error Handling**: Skip malformed RDF with warning, report total skipped count at end
- **All RDF formats**: N-Triples, N-Quads, Turtle, TriG, RDF/XML, JSON-LD, N3

## Architecture

Multi-pass, disk-backed approach for scalability:
1. Pass 1: Stream input, extract terms, write to zstd-compressed temp files, external merge sort to build dictionary
2. Write dictionary sections with PFC encoding, build sorted string table (SST) for term-to-ID lookup
3. Pass 2: Stream input again, encode triples as integer IDs using SST lookups
4. External sort ID-triples in SPO order (zstd-compressed chunks), build BitmapTriples
5. Assemble HDT file (header + dictionary + triples)
6. Optionally build OPS index (.hdt.index.v1-1)

### Resource Requirements

**Memory:** Default 4GB, configurable via `--memory-limit`. For 100B triples, recommend 16-32GB for optimal performance.

**Temporary Disk Space:** Peak usage during Phase 1 (term extraction and sorting):
- **Rule of thumb:** `Triples × 40 bytes` for typical RDF datasets (with zstd compression)
- Examples: 500M triples ≈ 20GB, 10B triples ≈ 400GB, 100B triples ≈ 4TB
- Varies by term uniqueness and compressibility: 25-60 bytes/triple (lower = more repeated terms)
- External sort chunks are compressed with zstd level 1 (~67% space reduction vs uncompressed)
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
  main.rs          - CLI entry point
  cli.rs           - CLI argument definitions (clap)
  rdf/             - RDF parsing, input discovery, streaming, compressed input
  dictionary/      - Dictionary construction, PFC encoding, SST term-to-ID lookup
  triples/         - BitmapTriples encoding, external sort
  hdt/             - HDT file serialization (header, dictionary, triples sections)
  index/           - HDT index file generation (.hdt.index.v1-1)
  io/              - VByte, LogArray, Bitmap, CRC utilities, Control Information
  quads/           - HDTQ quad support (graph dictionary, membership bitmaps)
tests/
  integration/     - Integration tests with sample RDF files
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
| `memmap2` | Memory-mapped file I/O (SST lookup, large file access) |
| `indicatif` | Progress bars and status reporting |
| `flate2` | Gzip decompression |
| `bzip2` | Bzip2 decompression |
| `xz2` / `liblzma` | XZ/LZMA decompression |
| `zstd` | Zstandard compression for temp chunk files |
