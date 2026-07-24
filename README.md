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
- **Named graphs** — optional packed `<data.hdt>.graphs` sidecars preserve N-Quads/TriG datasets while the standard HDT remains the deduplicated triples union
- **Index generation** — optional `.hdt.index.v1-1` enables efficient `? P ?`, `? ? O`, and `? P O` queries
- **VoID statistics** — compute dataset-level, property, and class partition statistics as N-Triples
- **Membership and overlap sketches** — build source-bound binary fuse filters and bottom-k MinHash files directly from an HDT dictionary
- **Resilient parsing** — skips malformed triples with warnings, reports total skipped at the end

## Named graphs

hdtc preserves RDF datasets without changing the standard HDT triples format. In
quads mode, the output is a pair of files:

- `data.hdt` is an ordinary HDT containing the deduplicated union of all triples
  in the dataset. It remains usable by existing HDT software that knows nothing
  about named graphs.
- `data.hdt.graphs` records which default or named graphs contain each union
  triple. It has a sorted graph dictionary and one compressed membership layer
  per graph. Graph ID 0 denotes the RDF default graph; named graphs receive
  consecutive IDs starting at 1.

The union and the default graph are distinct views. A triple may occur in the
default graph, in one or more named graphs, or in both. Repeated copies of the
same quad create one membership, while the same triple in different graphs
creates one HDT triple with several memberships.

Memberships refer to final SPO positions in the associated HDT. The sidecar is
bound to the HDT's exact dictionary-and-triples bytes with a SHA-256 digest, so
it cannot accidentally be used with another HDT. Each graph layer independently
uses a dense chunk table, sparse chunk table, or Elias–Fano encoding according to
its density. Readers can answer access, rank, select, iteration, and graph lookup
directly from the file without loading a whole layer.

Creation remains streaming and bounded by `--memory-limit`. hdtc deduplicates and
sorts memberships on disk, then encodes one graph layer at a time using reusable
scratch space. Merging HDTs reconstructs memberships by graph name and triple,
instead of copying position-dependent bitmaps.

See the normative [HDT graphs sidecar format, version 1](docs/graphs-sidecar-format.md)
for the binary layout, checksums, identity rules, encodings, and validation
requirements.

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

hdtc supports these main commands:

### `hdtc create` — Convert RDF to HDT

```
hdtc create [OPTIONS] --output <OUTPUT> <INPUTS>...
```

### `hdtc index` — Create index for existing HDT

```
hdtc index [OPTIONS] <HDT_FILE>
```

### `hdtc dump` — Export the triples union or RDF dataset

```
hdtc dump [OPTIONS] <HDT_FILE>
```

### `hdtc search` — Query triples or graph memberships

```
hdtc search [OPTIONS] --query <PATTERN> <HDT_FILE>
```

### `hdtc void` — Compute VoID statistics

```
hdtc void [OPTIONS] <HDT_FILE>
```

### `hdtc sketch` — Build membership filters and overlap sketches

```
hdtc sketch [OPTIONS] <HDT_FILE>
```

### `hdtc header` — Dump or modify the embedded header triples

```
hdtc header <HDT_FILE> [--replace FILE | --add FILE] [--dataset-uri IRI] [--output PATH]
```

With no flags, dumps the header N-Triples to stdout. With `--replace`/`--add`/
`--dataset-uri`, writes a modified copy to `--output` (the original is never
changed; the dictionary and triples are copied verbatim, so any
`.hdt.index.v1-1` stays valid):

- `--replace FILE` — swap the descriptive metadata for the triples in `FILE`,
  keeping the data-derived statistics hdtc generated.
- `--add FILE` — append the triples in `FILE` to the header.
- `--dataset-uri IRI` — rename the dataset: rewrite every occurrence of the
  current dataset IRI in the header (subject or object) to `IRI` (the post-hoc
  counterpart to `create --dataset-uri`).

`--replace`/`--add` reject any input triple that asserts an hdtc-managed
predicate (the `void:` statistics and the `hdt:` namespace).

### Create: Named graph options

Use `-m quads` (or `--mode quads`) to write a standard triples HDT plus its
packed graph sidecar. The default mode is `triples`, which drops graph
information. In quads mode, the HDT contains the `N` unique union triples and
the sidecar contains the `M` distinct default/named-graph memberships described
in the [format specification](docs/graphs-sidecar-format.md).

```sh
hdtc create dataset.nq -o dataset.hdt -m quads
# Creates dataset.hdt and dataset.hdt.graphs
```

`--graph-map PATH=URI` adds a source-level named graph, and `--default-graph URI`
is the fallback for otherwise unassigned statements.

When an input is itself an HDT file, hdtc looks beside it for
`<input.hdt>.graphs`. `--input-sidecars` controls those input artifacts only; it
does not affect graph names parsed directly from RDF inputs:

- `preserve` — preserve and validate a sidecar when present, but allow an HDT
  input without one. This is the default in quads mode.
- `require` — require every HDT input to have a valid sidecar. This fail-closed
  setting is useful for automated merges where a missing file must not silently
  discard the input dataset's original graph memberships.
- `drop` — ignore input sidecars even when present and use only each HDT's triples
  union.

For an HDT whose sidecar is absent or dropped, memberships come from a matching
`--graph-map`, then `--default-graph`, or finally the RDF default graph. Graph
construction and source-position remapping use bounded external sorts governed
by `--memory-limit`. Graph maps, default graph assignment, and preserving or
requiring input sidecars require `--mode quads`; triples mode always drops input
sidecars.

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

### Sketch: Building membership and overlap artifacts

Build the default subject and object artifacts from an existing HDT:

```sh
hdtc sketch data.hdt
```

By default this creates a `filters/` directory beside the HDT containing:

```text
filters/
  subjects.filter
  objects.filter
  subjects.minhash
  objects.minhash
```

Subject artifacts contain distinct IRIs from the HDT shared and subject-only
dictionary sections. Object artifacts contain distinct IRIs from the shared and
object-only sections. Literals and blank nodes are excluded. Shared IRIs belong
to both roles. Predicates are not currently a supported sketch role.

The `.filter` files use deterministic BinaryFuse8 by default; use
`--filter-bits 16` for a lower false-positive rate at approximately twice the
filter size. The `.minhash` files contain the smallest distinct XXH64 hashes,
with a default capacity of 65,536 (about 512 KiB per saturated role). Hashing
uses seed 0 over the exact IRI UTF-8 bytes without normalization.

Choose a larger MinHash or emit only one role:

```sh
hdtc sketch data.hdt --k 131072 --filter-bits 16
hdtc sketch data.hdt --roles subjects --output-dir subject-filters
```

Each file is self-describing, protected by CRC32C, and bound to the source HDT
with a SHA-256 digest. As with `.graphs` sidecars, that digest covers the
dictionary and triples but not the header, so artifacts stay valid across
`hdtc header` rewrites of the same data. Existing target artifacts are not
overwritten.

These artifacts are meant to be read by other tools, in other languages, built
by other parties. See the normative
[HDT sketch artifact formats, version 1](docs/sketch-format.md) for the byte
layouts, the term-to-key rule, the complete membership probe algorithm, the
validation a reader must perform on untrusted files, and the frozen conformance
vectors. Nothing in the format requires a particular library: an implementation
can be written from that document alone.

IRI hashes are streamed to temporary disk files, and each role's filter is built
separately from its own file. `--memory-limit` bounds both phases: before
scanning, hdtc checks the combined MinHash estimate for all selected roles;
during the scan, each role stops as soon as it has more IRIs than the binary
fuse scratch arrays, fingerprints, keys, and retained MinHash values can fit —
so an oversized input is refused while it is being read rather than after. If a
role does not fit, reduce `--k` or increase the limit. Use `--temp-dir` to place
the uncompressed hashed-key files on a disk with sufficient space.

### Dump: Exporting the union or dataset

Export an HDT file to N-Triples (writes to stdout if `--output` is omitted):

```sh
hdtc dump existing.hdt -o existing.nt
```

The default is explicitly the triples **union view**. For a lossless N-Quads
export from an HDT/sidecar pair:

```sh
hdtc dump dataset.hdt --graph-view dataset -o dataset.nq
```

Stream directly to another tool:

```sh
hdtc dump existing.hdt | gzip > existing.nt.gz
```

If the output file already exists, it is overwritten.

### Search: Querying triples and graph memberships

Query arity selects the view: three positions search the HDT triples union and
emit N-Triples; four positions search graph memberships and emit N-Quads. Use
`?` or `*` as a wildcard. In graph position, `default` binds the RDF default
graph. Default-graph results have no fourth RDF term, as required by N-Quads.

Output all triples (equivalent to `hdtc dump`):

```sh
hdtc search existing.hdt --query "? ? ?"
```

Output every default/named-graph membership from a graph sidecar:

```sh
hdtc search dataset.hdt --query "? ? ? ?"
```

Search one named graph, or the RDF default graph:

```sh
hdtc search dataset.hdt --query "? ? ? <http://example.org/graph>"
hdtc search dataset.hdt --query "? ? ? default"
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

| Pattern | Index required?       | Description                                   |
| ------- | --------------------- | --------------------------------------------- |
| `? ? ?` | No                    | All triples                                   |
| `S ? ?` | No                    | All triples for a subject                     |
| `S P ?` | No                    | All objects for a subject–predicate pair      |
| `S ? O` | No                    | All predicates linking a subject to an object |
| `S P O` | No                    | Exact triple lookup                           |
| `? P ?` | Yes (or `--no-index`) | All triples with a given predicate            |
| `? ? O` | Yes (or `--no-index`) | All triples with a given object               |
| `? P O` | Yes (or `--no-index`) | All triples with a given predicate and object |

For `? P ?`, `? ? O`, and `? P O`, hdtc uses the `.hdt.index.v1-1` sidecar file (auto-detected next to the HDT file, or specified with `--index`). Pass `--no-index` to fall back to a sequential full scan instead. For `? P O`, hdtc automatically chooses the most efficient query path based on predicate selectivity.

Adding a fourth graph position applies any of the same S/P/O constraints to
dataset memberships. A bound graph streams its layer directly. A wildcard graph
uses a disk-backed position transpose governed by `--memory-limit` and
`--temp-dir`, so memory remains bounded independently of dataset size. A
four-position query requires `<HDT_FILE>.graphs`.

### Void: Computing VoID statistics

Compute [VoID](https://www.w3.org/TR/void/) (Vocabulary of Interlinked Datasets) statistics for an HDT file and output the results as N-Triples. This is useful for generating dataset metadata describing the structure and content of an RDF dataset.

The output includes:

- **Dataset-level statistics** — total triples, distinct subjects, distinct objects, number of properties
- **Property partitions** — triple count per predicate
- **Class partitions** — entity count and triple count per `rdf:type` class, with nested property partitions
- **Object class partitions** — per-property target class breakdown (using the [void-ext](http://ldf.fi/void-ext) `objectClassPartition` extension), within class-level property partitions
- **Datatype partitions** — per-property breakdown of literal objects by RDF datatype (e.g., `xsd:integer`, `xsd:string`) using the void-ext `datatypePartition` extension, within class-level property partitions
- **Language partitions** — for `rdf:langString` literals, further breakdown by language tag (e.g., `en`, `de`), nested inside the corresponding datatype partition using the void-ext `languagePartition` extension

Generate VoID statistics to stdout:

```sh
hdtc void data.hdt --dataset-uri http://example.org/mydataset
```

Write VoID output to a file:

```sh
hdtc void data.hdt --dataset-uri http://example.org/mydataset -o void.nt
```

Use blank nodes instead of URI references for partition identifiers:

```sh
hdtc void data.hdt --dataset-uri http://example.org/mydataset --use-blank-nodes
```

The algorithm uses two sequential passes over the HDT triples plus a dictionary scan (no index required):

1. **Pass 1** scans all triples to identify `rdf:type` relationships, building a subject-to-class index.
2. **Datatype index** — a sequential scan of the object-only dictionary section extracts each literal's datatype or language tag, building a compact 2-byte-per-entry index. Shared-section terms are skipped (literals can never be subjects, so shared terms are always URIs or blank nodes).
3. **Pass 2** scans all triples again to accumulate per-property and per-class statistics, including datatype and language counts, using the indices from the previous steps.

Partition URIs are generated using MD5 hashes of the corresponding class, property, datatype, or language tag. Blank-node classes (common in OWL ontologies) are automatically filtered out and do not produce class partitions.

### Create: All options

| Option                             | Default                      | Description                                                 |
| ---------------------------------- | ---------------------------- | ----------------------------------------------------------- |
| `<INPUTS>...`                      | _(required)_                 | Input RDF files or directories                              |
| `-o, --output`                     | _(required)_                 | Output HDT file path                                        |
| `-m, --mode triples\|quads`        | `triples`                    | Drop graphs or write a packed graph sidecar                  |
| `--graph-map PATH=URI`             | —                            | Add a named graph to statements from a path                  |
| `--default-graph URI`              | —                            | Fallback graph for otherwise-unassigned statements           |
| `--input-sidecars POLICY`          | preserve in quads mode       | Preserve, require, or ignore sidecars beside HDT inputs      |
| `--temp-dir`                       | system temp                  | Directory for temporary working files                       |
| `--index`                          | off                          | Generate `.hdt.index.v1-1` index file                       |
| `--base-uri`                       | first input's `file://` URI  | Base URI used to resolve relative IRIs while parsing input  |
| `--dataset-uri`                    | `--base-uri` value           | Dataset IRI recorded as the subject of the header metadata  |
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

### Sketch: All options

| Option                    | Default                  | Description                                                        |
| ------------------------- | ------------------------ | ------------------------------------------------------------------ |
| `<HDT_FILE>`              | _(required)_             | Source HDT file                                                     |
| `-o, --output-dir DIR`    | `filters/` beside HDT    | Directory for generated artifacts                                  |
| `--k N`                   | `65536`                  | Bottom-k MinHash capacity (minimum 2)                               |
| `--filter-bits 8\|16`     | `8`                      | Binary fuse fingerprint width                                      |
| `--roles ROLE,...`        | `subjects,objects`       | Roles to emit (`subjects` and/or `objects`)                         |
| `--temp-dir DIR`          | system temp              | Directory for uncompressed temporary hashed-key files              |
| `-m, --memory-limit SIZE` | `4G`                     | Soft budget for combined MinHash and per-role filter construction  |
| `--benchmark`             | off                      | Emit total sketch timing                                           |
| `-v, --verbose`           | —                        | Increase log verbosity (`-v` debug, `-vv` trace)                    |
| `-q, --quiet`             | —                        | Suppress all output except errors                                  |

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
| `--query PATTERN`     | _(required)_                | Three-position triple or four-position quad pattern                      |
| `-o, --output PATH`   | stdout                      | Write results to file instead of stdout                                  |
| `--count`             | off                         | Print only the count of matching triples                                 |
| `--limit N`           | unlimited                   | Stop after N results (ignored when combined with `--count`)              |
| `--offset N`          | 0                           | Skip the first N matching results (ignored when combined with `--count`) |
| `--index PATH`        | `<HDT_FILE>.hdt.index.v1-1` | Index file path (used for `? P ?`, `? ? O`, and `? P O` queries)         |
| `--no-index`          | off                         | Disable index use; fall back to sequential scan for all patterns         |
| `--temp-dir DIR`      | system temp                 | Directory for wildcard-graph membership sorting                          |
| `--memory-limit SIZE` | `4G`                        | Memory limit for dictionary caches and membership sorting                |
| `-v, --verbose`       | —                           | Increase log verbosity (`-v` debug, `-vv` trace)                         |
| `-q, --quiet`         | —                           | Suppress all output except errors                                        |

### Void: All options

| Option                    | Default                      | Description                                                         |
| ------------------------- | ---------------------------- | ------------------------------------------------------------------- |
| `<HDT_FILE>`              | _(required)_                 | Path to existing HDT file                                           |
| `--dataset-uri URI`       | `http://example.org/dataset` | URI identifying the dataset being described                         |
| `-o, --output PATH`       | stdout                       | Write VoID N-Triples to file instead of stdout                      |
| `--use-blank-nodes`       | off                          | Use blank nodes for partition identifiers instead of URI references |
| `-m, --memory-limit SIZE` | `4G`                         | Soft memory limit for dictionary caches (e.g. `4G`, `2000M`)        |
| `-v, --verbose`           | —                            | Increase log verbosity (`-v` debug, `-vv` trace)                    |
| `-q, --quiet`             | —                            | Suppress all output except errors                                   |

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

`hdtc sketch` additionally writes an uncompressed 8-byte hash for each
qualifying IRI in every selected role while building the filters. An IRI in the
shared dictionary is therefore present in both temporary role files when both
roles are selected. These files are also cleaned up automatically.

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
  hdt/               HDT serialization, reading, querying, statistics, and sketches
  index/             HDT index generation (.hdt.index.v1-1)
  io/                VByte, LogArray, Bitmap, CRC, Control Information
  pipeline/          6-stage pipelined architecture
  sort/              External merge sort
tests/
  integration_test.rs   End-to-end pipeline tests
  compat_test.rs        Compatibility tests against the hdt crate
  sketch_test.rs        Sketch envelope, role, filter, and edge-case tests
  data/                 Sample RDF fixtures
docs/
  graphs-sidecar-format.md  Normative .graphs sidecar format
  sketch-format.md          Normative .filter / .minhash formats
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
