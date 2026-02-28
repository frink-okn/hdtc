# hdtc

A Rust command-line tool for converting RDF files to [HDT](https://www.rdfhdt.org/) (Header, Dictionary, Triples) binary format. Designed for very large datasets (up to 100 billion triples) with bounded memory usage.

HDT files produced by hdtc are fully compatible with [hdt-java](https://github.com/rdfhdt/hdt-java) and [hdt-cpp](https://github.com/rdfhdt/hdt-cpp).

## Features

- **All standard RDF formats** — N-Triples, N-Quads, Turtle, TriG, RDF/XML, JSON-LD, N3
- **HDT inputs** — existing HDT files can be used as inputs, enabling merging of HDT files with each other or with RDF files
- **Transparent decompression** — `.gz`, `.bz2`, `.xz` inputs handled automatically
- **Scalable** — streaming, disk-backed pipeline with configurable memory limit (default 4 GB)
- **Multiple inputs** — accepts any mix of RDF files, HDT files, and directories; recursively discovers RDF files
- **Parallel NT/NQ parsing** — newline-safe chunk parsing for N-Triples/N-Quads (including `.gz`, `.bz2`, `.xz`) with bounded in-flight memory
- **Quads support** — HDTQ format (ESWC 2018) with graph dictionary and membership bitmaps
- **Index generation** — optional `.hdt.index.v1-1` for OPS-order queries
- **Resilient parsing** — skips malformed triples with warnings, reports total skipped at the end

## Installation

Requires [Rust](https://www.rust-lang.org/tools/install) 1.85 or later.

```sh
cargo install --path .
```

Or build from source:

```sh
cargo build --release
# Binary is at target/release/hdtc
```

## Usage

hdtc supports three main commands:

### `hdtc create` — Convert RDF to HDT

```
hdtc create [OPTIONS] --output <OUTPUT> <INPUTS>...
```

### `hdtc index` — Create index for existing HDT

```
hdtc index [OPTIONS] <HDT_FILE>
```

### `hdtc dump` — Convert HDT to N-Triples

```
hdtc dump <HDT_FILE> --output <OUTPUT>
```

### Create: Basic examples

Convert a single N-Triples file:

```sh
hdtc create data.nt -o data.hdt
```

Convert multiple files at once:

```sh
hdtc create file1.ttl file2.nt.gz -o combined.hdt
```

Convert an entire directory of RDF files:

```sh
hdtc create ./rdf-data/ -o output.hdt
```

Generate an HDT index alongside the output:

```sh
hdtc create data.nt -o data.hdt --index
```

### Create: Merging HDT files

Merge two existing HDT files into one:

```sh
hdtc create part1.hdt part2.hdt -o merged.hdt
```

Combine an existing HDT file with new RDF data:

```sh
hdtc create existing.hdt updates.nt.gz -o combined.hdt
```

### Create: Quads mode

Produce an HDTQ file from N-Quads input:

```sh
hdtc create data.nq -o data.hdt -m quads
```

Map input files to named graphs:

```sh
hdtc create people.nt places.nt -o data.hdt -m quads \
  --graph-map people.nt=http://example.org/people \
  --graph-map places.nt=http://example.org/places
```

### Create: Tuning for large datasets

Set a higher memory limit for better throughput:

```sh
hdtc create huge.nt.gz -o huge.hdt --memory-limit 16G
```

Direct temporary files to a fast disk with sufficient space:

```sh
hdtc create huge.nt -o huge.hdt --temp-dir /mnt/fast-ssd/tmp
```

### Index: Creating indexes

Create an index file for an existing HDT file:

```sh
hdtc index existing.hdt
# Creates: existing.hdt.index.v1-1
```

With custom memory and temp settings:

```sh
hdtc index existing.hdt --memory-limit 8G --temp-dir /mnt/fast-ssd/tmp
```

### Dump: Exporting to N-Triples

Export an HDT file to N-Triples:

```sh
hdtc dump existing.hdt -o existing.nt
```

If the output file already exists, it is overwritten.

### Create: All options

| Option                             | Default                      | Description                                                 |
| ---------------------------------- | ---------------------------- | ----------------------------------------------------------- |
| `<INPUTS>...`                      | _(required)_                 | Input RDF files or directories                              |
| `-o, --output`                     | _(required)_                 | Output HDT file path                                        |
| `-m, --mode`                       | `triples`                    | Output mode: `triples` or `quads`                           |
| `--temp-dir`                       | system temp                  | Directory for temporary working files                       |
| `--index`                          | off                          | Generate `.hdt.index.v1-1` index file                       |
| `--base-uri`                       | `http://example.org/dataset` | Base URI for the HDT header                                 |
| `--graph-map PATH=URI`             | —                            | Map input paths to named graph URIs (quads mode)            |
| `--default-graph URI`              | —                            | Default graph for triples without an explicit graph         |
| `--memory-limit SIZE`              | `4G`                         | Soft memory limit for internal buffers (e.g. `4G`, `2000M`) |
| `--parse-file-workers N`           | auto                         | Number of files parsed concurrently                         |
| `--parse-chunk-workers N`          | auto (capped)                | Parser workers per active NT/NQ file                        |
| `--parse-chunk-bytes BYTES`        | auto                         | Target NT/NQ chunk size in bytes                            |
| `--parse-max-inflight-bytes BYTES` | auto                         | Max in-flight parser chunk bytes per file                   |
| `--benchmark`                      | off                          | Emit stage timing and RSS high-water summary                |
| `-v, --verbose`                    | —                            | Increase log verbosity (`-v` debug, `-vv` trace)            |
| `-q, --quiet`                      | —                            | Suppress all output except errors                           |

Auto parser tuning is derived from `--memory-limit` (accepts `G`/`M` suffixes, e.g. `16G` or `2000M`): by default hdtc allocates a bounded parser budget, caps chunk-worker fanout, and computes chunk size / in-flight chunk bytes from that budget.

### Index: All options

| Option                | Default      | Description                                                   |
| --------------------- | ------------ | ------------------------------------------------------------- |
| `<HDT_FILE>`          | _(required)_ | Path to existing HDT file                                     |
| `--temp-dir`          | system temp  | Directory for temporary working files                         |
| `--memory-limit SIZE` | `4G`         | Soft memory limit for sorting operations (e.g. `4G`, `2000M`) |
| `--benchmark`         | off          | Emit stage timing and RSS high-water summary                  |
| `-v, --verbose`       | —            | Increase log verbosity (`-v` debug, `-vv` trace)              |
| `-q, --quiet`         | —            | Suppress all output except errors                             |

### Dump: All options

| Option                | Default      | Description                                                 |
| --------------------- | ------------ | ----------------------------------------------------------- |
| `<HDT_FILE>`          | _(required)_ | Path to existing HDT file                                   |
| `-o, --output`        | _(required)_ | Output N-Triples file path                                  |
| `--memory-limit SIZE` | `4G`         | Soft memory limit for dictionary cache (e.g. `4G`, `2000M`) |
| `--benchmark`         | off          | Emit stage timing and RSS high-water summary                |
| `-v, --verbose`       | —            | Increase log verbosity (`-v` debug, `-vv` trace)            |
| `-q, --quiet`         | —            | Suppress all output except errors                           |

## Resource requirements

### Memory

Default is 4 GB, configurable with `--memory-limit` (e.g. `--memory-limit 16G`). For datasets over 10 billion triples, 16–32 GB is recommended.

### Temporary disk space

The single-pass pipeline deduplicates terms early via per-batch hash maps, so temporary files hold only deduplicated partial vocabularies, compact local-ID triples, and SPO sort chunks — all zstd-compressed. Approximate peak usage:

| Triples | Approx. temp space |
| ------: | -----------------: |
|   500 M |               4 GB |
|    10 B |              80 GB |
|   100 B |             800 GB |

Actual usage varies with term uniqueness and compressibility (~6–10 bytes/triple after compression). Temporary files are automatically cleaned up after completion. Use `--temp-dir` to direct them to a disk with sufficient space.

### Output size

HDT files are typically 10–20% of the equivalent uncompressed N-Triples.

## Architecture

hdtc uses a multi-stage, streaming pipeline (inspired by [Qlever](https://github.com/ad-freiburg/qlever)) with bounded channels and backpressure:

```
Stage 1  Parse RDF input (parallel files + parallel NT/NQ chunks, bounded backpressure)
   ↓
Stage 2  Build per-batch vocabularies (hash map, arena-allocated terms)
   ↓
Stage 3  Write partial vocabularies to disk (zstd-compressed)
   ↓
Stage 4  K-way merge partial vocabularies → assign global IDs, write dictionary
   ↓
Stage 5  Remap local IDs to global IDs (parallel)
   ↓
Stage 6  Build BitmapTriples (streaming, SPO order)
   ↓
         Assemble HDT file (header + dictionary + triples)
```

All intermediate data is spilled to disk in zstd-compressed temporary files, keeping memory usage bounded regardless of input size.

## Project structure

```
src/
  main.rs            CLI entry point and pipeline orchestration
  cli.rs             Argument definitions (clap derive)
  rdf/               RDF parsing, format/compression detection, input discovery
  dictionary/        Dictionary construction, Plain Front Coding (PFC)
  triples/           BitmapTriples encoding
  hdt/               HDT file serialization
  index/             HDT index generation (.hdt.index.v1-1)
  io/                VByte, LogArray, Bitmap, CRC, Control Information
  pipeline/          6-stage pipelined architecture
  quads/             HDTQ quad support
  sort/              External merge sort
tests/
  integration_test.rs   End-to-end pipeline tests
  compat_test.rs        Compatibility tests against the hdt crate
  data/                 Sample RDF fixtures
```

## Development

### Building

```sh
cargo build
```

### Running tests

```sh
cargo test
```

Tests include unit tests across all modules, integration tests that run the full pipeline, and compatibility tests that verify output using the [`hdt`](https://crates.io/crates/hdt) Rust crate.

### Release build

The release profile enables LTO and single codegen unit for best performance:

```sh
cargo build --release
```

## License

MIT — see [LICENSE](LICENSE) for details.
