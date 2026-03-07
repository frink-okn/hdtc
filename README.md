# hdtc

A Rust command-line tool for converting RDF files to [HDT](https://www.rdfhdt.org/) (Header, Dictionary, Triples) binary format. Designed for very large datasets (up to 100 billion triples) with bounded memory usage.

HDT files produced by hdtc are fully compatible with [hdt-java](https://github.com/rdfhdt/hdt-java) and [hdt-cpp](https://github.com/rdfhdt/hdt-cpp).

Development of hdtc is done primarily through Claude Code.

## Features

- **All standard RDF formats** — N-Triples, N-Quads, Turtle, TriG, RDF/XML, JSON-LD, N3
- **HDT inputs** — existing HDT files can be used as inputs, enabling merging of HDT files with each other or with RDF files
- **Transparent decompression** — `.gz`, `.bz2`, `.xz` inputs handled automatically
- **Scalable** — streaming, disk-backed pipeline with configurable memory limit (default 4 GB)
- **Multiple inputs** — accepts any mix of RDF files, HDT files, and directories; recursively discovers RDF files
- **Parallel NT/NQ parsing** — newline-safe chunk parsing for N-Triples/N-Quads (including `.gz`, `.bz2`, `.xz`) with bounded in-flight memory
- **Quad inputs** — N-Quads and TriG inputs are accepted; the graph component is dropped and triples are indexed normally
- **Index generation** — optional `.hdt.index.v1-1` enables efficient `? P ?`, `? ? O`, and `? P O` queries
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

hdtc supports four main commands:

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
hdtc dump [OPTIONS] <HDT_FILE>
```

### `hdtc search` — Query an HDT file with a triple pattern

```
hdtc search [OPTIONS] --query <PATTERN> <HDT_FILE>
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

Export an HDT file to N-Triples (writes to stdout if `--output` is omitted):

```sh
hdtc dump existing.hdt -o existing.nt
```

Stream directly to another tool:

```sh
hdtc dump existing.hdt | gzip > existing.nt.gz
```

If the output file already exists, it is overwritten.

### Search: Querying with triple patterns

Search using a triple pattern — three N-Triples terms separated by whitespace, with `?` as a wildcard for any position. Outputs tab-delimited N-Triples (`S\tP\tO\t.`) to stdout or a file.

Output all triples (equivalent to `hdtc dump`):

```sh
hdtc search existing.hdt --query "? ? ?"
```

Find all triples about a specific subject:

```sh
hdtc search existing.hdt --query "<http://example.org/alice> ? ?"
```

Find triples with a specific subject and predicate:

```sh
hdtc search existing.hdt --query "<http://example.org/alice> <http://xmlns.com/foaf/0.1/name> ?"
```

Look up an exact triple:

```sh
hdtc search existing.hdt --query "<http://example.org/alice> <http://xmlns.com/foaf/0.1/name> \"Alice\"@en"
```

Count matching triples without outputting them:

```sh
hdtc search existing.hdt --query "<http://example.org/alice> ? ?" --count
```

Limit output to the first 10 results:

```sh
hdtc search existing.hdt --query "<http://example.org/alice> ? ?" --limit 10
```

Skip the first 20 matches, then return up to 10 results:

```sh
hdtc search existing.hdt --query "? ? ?" --offset 20 --limit 10
```

Write results to a file:

```sh
hdtc search existing.hdt --query "<http://example.org/alice> ? ?" -o alice.nt
```

Find all triples with a given predicate (requires index):

```sh
hdtc search data.hdt --query "? <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?"
```

Same query using a sequential scan fallback (no index needed):

```sh
hdtc search data.hdt --query "? <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?" --no-index
```

Use an index file at a non-default path:

```sh
hdtc search data.hdt --query "? <http://xmlns.com/foaf/0.1/knows> ?" --index /path/to/data.hdt.index.v1-1
```

Find all triples with a given object (requires index):

```sh
hdtc search data.hdt --query "? ? <http://example.org/Person>"
```

Find triples with a specific predicate and object:

```sh
hdtc search data.hdt --query "? <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person>"
```

**Supported patterns:**

| Pattern | Index required?       | Description                                        |
| ------- | --------------------- | -------------------------------------------------- |
| `? ? ?` | No                    | All triples                                        |
| `S ? ?` | No                    | All triples for a subject                          |
| `S P ?` | No                    | All objects for a subject–predicate pair           |
| `S ? O` | No                    | All predicates linking a subject to an object      |
| `S P O` | No                    | Exact triple lookup                                |
| `? P ?` | Yes (or `--no-index`) | All triples with a given predicate                 |
| `? ? O` | Yes (or `--no-index`) | All triples with a given object                    |
| `? P O` | Yes (or `--no-index`) | All triples with a given predicate and object      |

For `? P ?`, `? ? O`, and `? P O`, hdtc uses the `.hdt.index.v1-1` sidecar file (auto-detected next to the HDT file, or specified with `--index`). Pass `--no-index` to fall back to a sequential full scan instead. For `? P O`, hdtc automatically chooses the most efficient query path based on predicate selectivity.

### Create: All options

| Option                             | Default                      | Description                                                 |
| ---------------------------------- | ---------------------------- | ----------------------------------------------------------- |
| `<INPUTS>...`                      | _(required)_                 | Input RDF files or directories                              |
| `-o, --output`                     | _(required)_                 | Output HDT file path                                        |
| `--temp-dir`                       | system temp                  | Directory for temporary working files                       |
| `--index`                          | off                          | Generate `.hdt.index.v1-1` index file                       |
| `--base-uri`                       | `http://example.org/dataset` | Base URI for the HDT header                                 |
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
| `-o, --output PATH`   | stdout       | Write N-Triples to file instead of stdout                   |
| `--memory-limit SIZE` | `4G`         | Soft memory limit for dictionary cache (e.g. `4G`, `2000M`) |
| `--benchmark`         | off          | Emit stage timing and RSS high-water summary                |
| `-v, --verbose`       | —            | Increase log verbosity (`-v` debug, `-vv` trace)            |
| `-q, --quiet`         | —            | Suppress all output except errors                           |

### Search: All options

| Option                | Default                     | Description                                                              |
| --------------------- | --------------------------- | ------------------------------------------------------------------------ |
| `<HDT_FILE>`          | _(required)_                | Path to existing HDT file                                                |
| `--query PATTERN`     | _(required)_                | Triple pattern (three N-Triples terms, `?` or `*` as wildcard)           |
| `-o, --output PATH`   | stdout                      | Write results to file instead of stdout                                  |
| `--count`             | off                         | Print only the count of matching triples                                 |
| `--limit N`           | unlimited                   | Stop after N results (ignored when combined with `--count`)              |
| `--offset N`          | 0                           | Skip the first N matching results (ignored when combined with `--count`) |
| `--index PATH`        | `<HDT_FILE>.hdt.index.v1-1` | Index file path (used for `? P ?`, `? ? O`, and `? P O` queries)        |
| `--no-index`          | off                         | Disable index use; fall back to sequential scan for all patterns         |
| `--memory-limit SIZE` | `4G`                        | Soft memory limit for dictionary caches (e.g. `4G`, `2000M`)             |
| `-v, --verbose`       | —                           | Increase log verbosity (`-v` debug, `-vv` trace)                         |
| `-q, --quiet`         | —                           | Suppress all output except errors                                        |

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

## Funding

Development supported by [NSF award 2535091](https://www.nsf.gov/awardsearch/show-award?AWD_ID=2535091).
