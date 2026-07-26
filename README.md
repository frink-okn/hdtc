# hdtc

A Rust command-line tool for converting RDF files to [HDT](https://www.rdfhdt.org/) (Header, Dictionary, Triples) binary format. Designed for very large datasets (up to 100 billion triples) with bounded memory usage.

HDT files produced by hdtc are fully compatible with [hdt-java](https://github.com/rdfhdt/hdt-java) and [hdt-cpp](https://github.com/rdfhdt/hdt-cpp).

Development of hdtc is done primarily through Claude Code.

## Features

- **All standard RDF formats** — N-Triples, N-Quads, Turtle, TriG, RDF/XML, JSON-LD, N3
- **HDT inputs** — existing HDT files can be used as inputs, enabling merging of HDT files with each other or with RDF files
- **Transparent decompression** — `.gz`, `.bz2`, `.xz`, `.zst` inputs handled automatically
- **Scalable** — streaming, disk-backed pipeline with configurable memory limit (default 4 GB)
- **Multiple inputs** — accepts any mix of RDF files, HDT files, and directories; recursively discovers RDF files
- **Parallel NT/NQ parsing** — newline-safe chunk parsing for N-Triples/N-Quads (including `.gz`, `.bz2`, `.xz`, `.zst`) with bounded in-flight memory
- **Index generation** — optional `.hdt.index.v1-1` enables efficient `? P ?`, `? ? O`, and `? P O` queries
- **VoID statistics** — compute dataset-level, property, and class partition statistics as N-Triples
- **Structural validation** — walk an HDT's triple structures and checksums, and any graph sidecar, end to end
- **Resilient parsing** — skips malformed triples with warnings, reports total skipped at the end
- **[Named graphs](#named-graphs)** — optional packed `<data.hdt>.graphs` sidecars preserve N-Quads/TriG datasets while the standard HDT remains the deduplicated triples union
- **[Membership and overlap sketches](#membership-and-overlap-sketches)** — build source-bound binary fuse filters and bottom-k MinHash files directly from an HDT dictionary
- **[Exact key sets](#exact-key-sets)** — publish the complete distinct key set for a role, Elias-Fano encoded, for membership and overlap without approximation

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

hdtc supports these main commands. The `sketch` and `keyset` commands, and the
named-graph options of `create`, are covered in detail under
[Beyond standard HDT](#beyond-standard-hdt).

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

### `hdtc validate` — Check HDT structures and any graph sidecar

```
hdtc validate [OPTIONS] <HDT_FILE>
```

### `hdtc void` — Compute VoID statistics

```
hdtc void [OPTIONS] <HDT_FILE>
```

### `hdtc sketch` — Build membership filters and overlap sketches

```
hdtc sketch [OPTIONS] <HDT_FILE>
```

### `hdtc keyset` — Build exact key sets

```
hdtc keyset [OPTIONS] <HDT_FILE>
```

### `hdtc text` — Build a full-text index over the literals

```
hdtc text [OPTIONS] <HDT_FILE>
```

### `hdtc header` — Dump or modify the embedded header triples

```
hdtc header <HDT_FILE> [--replace FILE | --add FILE] [--dataset-uri IRI] [--output PATH]
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

### Validate: Checking structures and checksums

Verify that an HDT decodes correctly and that its checksums match:

```sh
hdtc validate data.hdt
```

The check reads the control information, the header's triple count, and the four
dictionary sections, then walks BitmapY, BitmapZ, ArrayY, and ArrayZ from end to
end — rejecting term ID 0, a BitmapZ boundary count that disagrees with the
`(S,P)` pair count, and truncated sections — and verifies each streamed section's
CRC. It reports the triple count, the `(S,P)` pair count, and the ArrayZ object
ID range. Memory is constant regardless of file size.

If `<data.hdt>.graphs` is present it is validated too, strictly: the identity
digest against the HDT, every CRC, graph dictionary ordering and IRI syntax,
layer region ordering, and each layer's positions being strictly increasing and
in range. That pass uses a bounded external sort, so `--temp-dir` and
`--memory-limit` apply to it. A missing sidecar is not an error; hdtc checks one
only if it finds one beside the HDT.

Progress and results are logged at `info` level, and the command exits non-zero
on the first failure.

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

### Header: Dumping or modifying header triples

With no flags, `hdtc header` dumps the header N-Triples to stdout. With
`--replace`/`--add`/`--dataset-uri`, it writes a modified copy to `--output` (the
original is never changed; the dictionary and triples are copied verbatim, so any
`.hdt.index.v1-1` stays valid):

- `--replace FILE` — swap the descriptive metadata for the triples in `FILE`,
  keeping the data-derived statistics hdtc generated.
- `--add FILE` — append the triples in `FILE` to the header.
- `--dataset-uri IRI` — rename the dataset: rewrite every occurrence of the
  current dataset IRI in the header (subject or object) to `IRI` (the post-hoc
  counterpart to `create --dataset-uri`).

`--replace`/`--add` reject any input triple that asserts an hdtc-managed
predicate (the `void:` statistics and the `hdt:` namespace).

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

Named-graph options (`-m quads`, `--graph-map`, `--default-graph`,
`--input-sidecars`) are described under [Named graphs](#named-graphs).

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

| Option                          | Default      | Description                                                     |
| ------------------------------- | ------------ | --------------------------------------------------------------- |
| `<HDT_FILE>`                    | _(required)_ | Path to existing HDT file                                       |
| `-o, --output PATH`             | stdout       | Write results to file instead of stdout                         |
| `--graph-view union\|dataset`   | `union`      | Export the triples union, or dataset memberships as N-Quads     |
| `--temp-dir DIR`                | system temp  | Directory for dataset-export membership sorting                 |
| `--memory-limit SIZE`           | `4G`         | Soft memory limit for dictionary cache (e.g. `4G`, `2000M`)     |
| `--benchmark`                   | off          | Emit stage timing and RSS high-water summary                    |
| `-v, --verbose`                 | —            | Increase log verbosity (`-v` debug, `-vv` trace)                |
| `-q, --quiet`                   | —            | Suppress all output except errors                               |

### Search: All options

| Option                | Default                     | Description                                                              |
| --------------------- | --------------------------- | ------------------------------------------------------------------------ |
| `<HDT_FILE>`          | _(required)_                | Path to existing HDT file                                                |
| `--query PATTERN`     | _(one of)_                  | Three-position triple or four-position quad pattern                      |
| `--text TEXT`         | _(one of)_                  | Ranked full-text search over the index built by `hdtc text`              |
| `-o, --output PATH`   | stdout                      | Write results to file instead of stdout                                  |
| `--count`             | off                         | Print only the count of matching triples                                 |
| `--limit N`           | unlimited                   | Stop after N results (ignored when combined with `--count`)              |
| `--offset N`          | 0                           | Skip the first N matching results (ignored when combined with `--count`) |
| `--index PATH`        | `<HDT_FILE>.hdt.index.v1-1` | Index file path (used for `? P ?`, `? ? O`, and `? P O` queries)         |
| `--no-index`          | off                         | Disable index use; fall back to sequential scan for all patterns         |
| `--temp-dir DIR`      | system temp                 | Directory for wildcard-graph membership sorting                          |
| `--text-match MODE`   | `all`                       | `--text` only: how query tokens combine (`all`, `any`, `phrase`)         |
| `--fuzzy N`           | `0`                         | `--text` only: maximum edit distance per token (0-2)                     |
| `--prefix`            | off                         | `--text` only: match the final query token as a prefix                   |
| `--lang LANG,...`     | all                         | `--text` only: BCP 47 ranges; untagged literals stay eligible            |
| `--predicate IRI`     | all                         | `--text` only: keep only matches occurring on this predicate             |
| `--no-dedupe`         | off                         | `--text` only: emit every occurrence instead of one row per subject      |
| `--scores`            | off                         | `--text` only: append the relevance score as an N-Triples comment        |
| `--text-index DIR`    | `<HDT_FILE>.text`           | `--text` only: text index directory                                      |
| `-m, --memory-limit SIZE` | `4G`                    | Memory limit for dictionary caches and membership sorting                |
| `-v, --verbose`       | —                           | Increase log verbosity (`-v` debug, `-vv` trace)                         |
| `-q, --quiet`         | —                           | Suppress all output except errors                                        |

### Text: All options

| Option                    | Default           | Description                                                        |
| ------------------------- | ----------------- | ------------------------------------------------------------------ |
| `<HDT_FILE>`              | _(required)_      | Path to existing HDT file                                          |
| `-o, --output DIR`        | `<HDT_FILE>.text` | Output index directory                                             |
| `--max-literal-bytes N`   | `4096`            | Skip literals whose lexical form is longer than this               |
| `--exclude-datatype IRI`  | —                 | Skip this datatype in addition to the defaults (repeatable)        |
| `--index-all-datatypes`   | off               | Index every datatype, dropping the default value-space exclusions  |
| `--untagged-language LANG`| `en`              | Language to stem untagged literals as, or `none` to leave them unstemmed |
| `--threads N`             | auto              | Number of indexing threads                                         |
| `-m, --memory-limit SIZE` | `4G`              | Soft memory limit for the indexing arena (e.g. `4G`, `2000M`)      |
| `--benchmark`             | off               | Emit total indexing timing                                         |
| `-v, --verbose`           | —                 | Increase log verbosity (`-v` debug, `-vv` trace)                   |
| `-q, --quiet`             | —                 | Suppress all output except errors                                  |

### Validate: All options

| Option                | Default      | Description                                                       |
| --------------------- | ------------ | ----------------------------------------------------------------- |
| `<HDT_FILE>`          | _(required)_ | Path to existing HDT file                                         |
| `--temp-dir`          | system temp  | Directory for graph-sidecar validation sort files                 |
| `--memory-limit SIZE` | `4G`         | Soft memory limit for graph-sidecar validation (e.g. `4G`, `2000M`) |
| `--benchmark`         | off          | Emit total validation timing                                      |
| `-v, --verbose`       | —            | Increase log verbosity (`-v` debug, `-vv` trace)                  |
| `-q, --quiet`         | —            | Suppress all output except errors                                 |

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

### Header: All options

| Option              | Default      | Description                                                          |
| ------------------- | ------------ | -------------------------------------------------------------------- |
| `<HDT_FILE>`        | _(required)_ | Path to existing HDT file                                            |
| `--replace FILE`    | —            | Replace the descriptive metadata with the triples in `FILE`           |
| `--add FILE`        | —            | Append the triples in `FILE` to the header                            |
| `--dataset-uri IRI` | —            | Rewrite the current dataset IRI throughout the header                 |
| `-o, --output PATH` | —            | Output path; required for any modification, rejected for a plain dump |
| `--benchmark`       | off          | Emit total header timing                                              |
| `-v, --verbose`     | —            | Increase log verbosity (`-v` debug, `-vv` trace)                      |
| `-q, --quiet`       | —            | Suppress all output except errors                                    |

`--replace` and `--add` are mutually exclusive; either may be combined with
`--dataset-uri`.

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

`hdtc keyset` sorts each role's keys on disk, so its temporary space is what
lets it build a key set larger than memory. Peak usage is roughly **16 bytes per
distinct key** per role — the compressed sort chunks plus the merged run the
encoder streams from — released as soon as the artifact is published. Point
`--temp-dir` at a disk with room for that.

### Output size

HDT files are typically 10–20% of the equivalent uncompressed N-Triples.

## Beyond standard HDT

Everything above produces or reads plain HDT files. hdtc can also write optional
sidecar artifacts that sit beside a `.hdt` and are bound to it by a SHA-256
digest of its dictionary and triples: `.graphs` preserves the named graphs of an
RDF dataset, `.filter` and `.minhash` answer membership and overlap
approximately, and `.keys` answers both exactly. In every case the `.hdt` remains
an ordinary HDT that software knowing nothing about these files can still read.

Each of these formats is specified normatively in [`docs/`](docs/), because they
are meant to be read by other tools, in other languages, built by other parties.

### Named graphs

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

Memberships refer to final SPO positions in the associated HDT, so a sidecar is
bound to that HDT's exact bytes and cannot accidentally be used with another one.
Each graph layer is encoded independently — a dense chunk table, a sparse chunk
table, or Elias–Fano, according to its density — and readers can answer access,
rank, select, iteration, and graph lookup directly from the file without loading
a whole layer.

Creation is streaming and bounded by `--memory-limit`: hdtc deduplicates and
sorts memberships on disk, then encodes one layer at a time. Merging HDTs
reconstructs memberships by graph name and triple, rather than copying
position-dependent bitmaps.

See the normative [HDT graphs sidecar format, version 1](docs/graphs-sidecar-format.md)
for the binary layout, checksums, identity rules, encodings, and validation
requirements.

#### Create: Named graph options

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

Reading a dataset back is covered under
[Dump](#dump-exporting-the-union-or-dataset) (`--graph-view dataset`) and
[Search](#search-querying-triples-and-graph-memberships) (four-position
patterns).

### Membership and overlap sketches

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

The `.filter` files use deterministic BinaryFuse16 by default; use
`--filter-bits 8` to roughly halve the filter size at the cost of a higher
false-positive rate. The `.minhash` files contain the smallest distinct XXH64
hashes, with a default capacity of 65,536 (about 512 KiB per saturated role).
Hashing uses seed 0 over the exact IRI UTF-8 bytes without normalization.

Choose a larger MinHash or emit only one role:

```sh
hdtc sketch data.hdt --k 131072 --filter-bits 8
hdtc sketch data.hdt --roles subjects --output-dir subject-filters
```

Each file is self-describing, protected by CRC32C, and bound to the source HDT
with a SHA-256 digest. As with `.graphs` sidecars, that digest covers the
dictionary and triples but not the header, so artifacts stay valid across
`hdtc header` rewrites of the same data. Existing target artifacts are not
overwritten.

See the normative
[HDT sketch artifact formats, version 1](docs/sketch-format.md) for the byte
layouts, the term-to-key rule, the complete membership probe algorithm, the
validation a reader must perform on untrusted files, and the frozen conformance
vectors. Nothing in the format requires a particular library: an implementation
can be written from that document alone.

IRI hashes are streamed to temporary disk files, and each role's filter is built
separately from its own file. `--memory-limit` bounds both phases: hdtc checks
the combined MinHash budget for all selected roles before scanning, and during
the scan each role stops as soon as its IRIs exceed what the binary fuse scratch
arrays, fingerprints, keys, and retained MinHash values can hold — so an
oversized input is refused while it is being read rather than after. If a role
does not fit, reduce `--k` or raise the limit. Use `--temp-dir` to place the
uncompressed hashed-key files on a disk with sufficient space.

#### Sketch: All options

| Option                    | Default                  | Description                                                        |
| ------------------------- | ------------------------ | ------------------------------------------------------------------ |
| `<HDT_FILE>`              | _(required)_             | Source HDT file                                                     |
| `-o, --output-dir DIR`    | `filters/` beside HDT    | Directory for generated artifacts                                  |
| `--k N`                   | `65536`                  | Bottom-k MinHash capacity (minimum 2)                               |
| `--filter-bits 8\|16`     | `16`                     | Binary fuse fingerprint width                                      |
| `--roles ROLE,...`        | `subjects,objects`       | Roles to emit (`subjects` and/or `objects`)                         |
| `--temp-dir DIR`          | system temp              | Directory for uncompressed temporary hashed-key files              |
| `-m, --memory-limit SIZE` | `4G`                     | Soft budget for combined MinHash and per-role filter construction  |
| `--benchmark`             | off                      | Emit total sketch timing                                           |
| `-v, --verbose`           | —                        | Increase log verbosity (`-v` debug, `-vv` trace)                    |
| `-q, --quiet`             | —                        | Suppress all output except errors                                  |

### Exact key sets

Where `hdtc sketch` answers approximately, `hdtc keyset` answers exactly. It
writes the complete, sorted set of distinct 64-bit term keys for a role, under
the same term-to-key rule the sketches use:

```sh
hdtc keyset data.hdt
```

By default this creates a `keysets/` directory beside the HDT containing:

```text
keysets/
  subjects-only.keys
  objects-only.keys
  shared.keys
```

These are the HDT dictionary's disjoint subject/object sections. The familiar
role views are reconstructed without loss by merging sorted files:

```text
subjects = shared ∪ subjects-only
objects  = shared ∪ objects-only
```

This avoids writing every shared IRI twice while preserving directional overlap
questions such as `objects_A × subjects_B`. The optional `subjects` and
`objects` roles emit those overlapping views directly and describe the same sets
as sketch artifacts of the same role.

A key set needs no probe list: two `.keys` files intersect in a linear merge of
sorted integers, giving `|A ∩ B|` and the shared key set itself, with neither
party's IRIs in hand. A filter can only be probed one key at a time, and a
MinHash estimates the overlap without identifying it. A `.minhash` is derivable
from a key set (take the bottom `k`) and a `.filter` is built from one, so the
key set is the exact parent of both for the composite subject/object roles.

Keys are XXH64 hashes and are not reversible. A shared key becomes an IRI on
whichever side holds the terms: scan that HDT's dictionary once, hash each term,
and keep those whose key is in the shared set. So a key set answers *how big is
the join, and which of my terms are in it* — exactly, modulo the `n/2⁶⁴` hash
collision the sketches already live with — without either party publishing its
IRIs.

The cost is size. Elias-Fano lands at 5.5 bytes/key on a 4-million-key role —
about 2.4× a Fuse16 filter (~2.26 bytes/key) or 4.9× a Fuse8 one (~1.13),
depending on the `--filter-bits` you would otherwise ship. The rate *falls* as a
role grows, to about 4.4 bytes/key at 2.3 billion keys, so key sets suit small
and mid-size datasets while the sketches keep their edge at the top end.

Two encodings are available:

```sh
hdtc keyset data.hdt --encoding elias-fano   # default, ~4.4-5.8 bytes/key
hdtc keyset data.hdt --encoding raw          # sorted u64 array, 8 bytes/key
```

Elias-Fano is near the information floor for uniformly random 64-bit keys; raw is
the simplest possible decode — `mmap` the payload as a `u64` slice and binary
search it. The encoding is a per-file choice recorded in the header and does not
affect comparability: two files with different encodings hold the same kind of
set and intersect normally.

All six roles can be selected explicitly. For example, emit the overlapping
sketch-compatible views and the predicate dictionary section:

```sh
hdtc keyset data.hdt --roles subjects,objects,predicates
```

The supported roles are `subjects`, `objects`, `predicates`, `shared`,
`subjects-only`, and `objects-only`. `predicates` contains qualifying IRIs from
the predicate dictionary section only and has no sketch counterpart.

Files are comparable whenever their convention and hash agree — the role tells
you how to read the answer, not whether you may ask. Comparing *different* roles
is in fact one of the main things a key set is for: intersecting one dataset's
`objects` with another's `subjects` gives the exact set of shared join keys —
which of the things you mention that dataset actually describes.

**Any role builds at any size.** Keys are externally sorted and both encoders
stream, so `--memory-limit` bounds the sort buffers rather than the key count —
it is a throughput knob, not a ceiling on what can be published. A 12.4-million-
key build at a 1 MiB limit spills 95 chunks and emits bytes identical to the same
build at 4 GiB. The [sketches](#membership-and-overlap-sketches) are the
exception: binary fuse construction peels a hypergraph over the whole key set and
has no streaming form, so it keeps its keys resident and enforces a ceiling.

As with the sketches, each file is self-describing, protected by CRC32C, bound
to the source HDT by the same SHA-256 digest, and never overwritten if the
target already exists. See the normative
[HDT key-set artifact format, version 1](docs/keyset-format.md) for the byte
layout, the Elias-Fano sizing rule, the validation a reader must perform on
untrusted files, and the frozen conformance vectors.

#### Keyset: All options

| Option                    | Default                               | Description                                                                                          |
| ------------------------- | ------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `<HDT_FILE>`              | _(required)_                          | Source HDT file                                                                                      |
| `-o, --output-dir DIR`    | `keysets/` beside HDT                 | Directory for generated artifacts                                                                    |
| `--encoding ENCODING`     | `elias-fano`                          | Payload encoding (`elias-fano` or `raw`)                                                             |
| `--roles ROLE,...`        | `subjects-only,objects-only,shared`   | Roles to emit (`subjects`, `objects`, `predicates`, `shared`, `subjects-only`, and `objects-only`)   |
| `--temp-dir DIR`          | system temp                           | Directory for temporary sort chunks and merged key runs                                              |
| `-m, --memory-limit SIZE` | `4G`                                  | Soft budget for key sort buffers; bounds memory, not the key count                                   |
| `--benchmark`             | off                                   | Emit total keyset timing                                                                             |
| `-v, --verbose`           | —                                     | Increase log verbosity (`-v` debug, `-vv` trace)                                                     |
| `-q, --quiet`             | —                                     | Suppress all output except errors                                                                    |

### Full-text search over literals

`hdtc text` builds a full-text index over an HDT's literals, and
`hdtc search --text` queries it. It answers the question a triple pattern cannot:
*which resources are named or described by a string like this?*

```sh
hdtc text data.hdt
hdtc search data.hdt --text "atrazine degradation" --limit 20
```

The index lands in `data.hdt.text/` beside the HDT.

**Every literal is indexed.** There is no list of "label predicates" to
configure, and deliberately so: a predicate nobody thought to configure produces
resources that a search cannot find, and nothing in the result says so. Across
many independently published datasets, such a configuration is a guess that is
wrong somewhere and undiagnosable everywhere.

What that configuration was approximating comes free from ranking. BM25
normalizes by document length, so a one-word `rdfs:label` outranks a
two-hundred-word `rdfs:comment` for the same query term, with nothing declared
anywhere:

```console
$ hdtc search data.hdt --text atrazine --scores
<…/chebi/38769>  <…rdf-schema#label>  "atrazine"@en                      . # score=1.1420
<…/gene/2>       <…rdf-schema#label>  "Atrazine chlorohydrolase"@en      . # score=0.9889
<…/gene/1>       <…rdf-schema#label>  "atrazine degradation pathway"@en . # score=0.8719
```

Results are entity-level: one row per subject, represented by its highest-ranked
matching literal. `--no-dedupe` restores the occurrence view.

**The unit of indexing is the distinct literal**, identified by its object
dictionary ID, so the index scales with distinct strings rather than with
triples — in annotation-heavy graphs, repeated type labels and boilerplate
definitions make that difference large. It also means the index stores **no
subject and no predicate at all**: a hit is an HDT ID, and the `? ? O` path
through the `.hdt.index.v1-1` index turns it into every `(subject, predicate)`
that uses it. `hdtc text` therefore reads no triples — one pass over the object
dictionary is the whole build.

**A literal that *is* your query wins.** Searching `body` returns the resources
*named* "body" before those that merely contain the word — including before a
short literal that repeats it, which plain BM25 would otherwise rank first
(`"Body structure (body structure)"` scores above `"body"` on term frequency
alone). Results come in three classes, in order: whole-literal, then exact, then
stemmed. Literals up to 256 bytes get a whole-literal key; longer ones stay
findable but cannot be matched as a whole, and the manifest publishes how many
are covered.

**Stemming.** Every literal is indexed twice — as written, and stemmed for its
language — so `run` finds `running`, and `process` finds `processes`. Exact
matches always rank above stemmed ones, as a class, so widening recall never
displaces a literal hit. Untagged literals are stemmed as English by default
(`--untagged-language`), because tagging is inconsistent across source
ontologies within a single merged graph: in Ubergraph, `rdfs:label` is untagged
on UBERON terms and `@en` on GO terms, and leaving untagged text unstemmed would
make search quality depend on which ontology a term came from. Stemming covers
the 18 languages Snowball implements; others stay exactly searchable.

Query options beyond plain token matching:

```sh
hdtc search data.hdt --text "atrazine" --predicate http://www.w3.org/2000/01/rdf-schema#label
hdtc search data.hdt --text "atrazine" --lang en          # untagged literals stay eligible
hdtc search data.hdt --text "atrasine" --fuzzy 1          # typo tolerance
hdtc search data.hdt --text "atraz" --prefix              # typeahead
hdtc search data.hdt --text "atrazine degradation" --text-match phrase
```

`--lang` filters by BCP 47 basic filtering (`en` selects `en-GB`), and untagged
literals are always eligible: an untagged string asserts no language, and in
practice is often language-neutral by nature — a chemical name, a gene symbol,
an accession — which is exactly what a cross-language client is looking for.

**What is left out, and how you know.** Three kinds of literal are skipped:
values above `--max-literal-bytes` (default 4 KiB), values whose datatype has an
ordered value space (`xsd:integer`, `xsd:date` and friends — text search over
them is noise), and values with no alphanumeric character. Each is *counted*, and
the counts plus the exact datatype set are published in the index's manifest:

```console
$ cat data.hdt.text/hdtc-text.meta
hdtc-text	1
analyzer	3
tantivy	0.26.1
untagged_language	en
literals_scanned	12
indexed_docs	9
whole_literal_keys	9
excluded_oversize	0
excluded_datatype	2
excluded_no_tokens	1
language	de	2
language	en	5
language	und	2
```

An index that silently omits some fraction of a dataset's literals makes every
search over it quietly wrong about coverage. Publishing the counts turns an
invisible gap into a readable one.

**One caveat worth stating plainly.** Unlike the sketches and key sets, the
published bytes here are [Tantivy](https://github.com/quickwit-oss/tantivy)'s,
not hdtc's. hdtc pins an exact Tantivy release and specifies the *convention*
around the index — the schema, the analyzer, the exclusion rules, the manifest —
rather than the byte layout, so in practice a text index is readable only by a
program linking the same release. The alternative was a hand-written term
dictionary and scorer that still could not do bounded-cost fuzzy matching. See
[HDT text index format, version 1](docs/text-index-format.md) §1.1 for the full
trade and the migration path.


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
  hdt/               HDT serialization, reading, querying, statistics, sketches, and key sets
  index/             HDT index generation (.hdt.index.v1-1)
  io/                VByte, LogArray, Bitmap, CRC, Control Information
  pipeline/          6-stage pipelined architecture
  sort/              External merge sort
tests/
  integration_test.rs   End-to-end pipeline tests
  compat_test.rs        Compatibility tests against the hdt crate
  sketch_test.rs        Sketch envelope, role, filter, and edge-case tests
  keyset_test.rs        Key-set envelope, role, encoding, and edge-case tests
  data/                 Sample RDF fixtures
docs/
  graphs-sidecar-format.md  Normative .graphs sidecar format
  sketch-format.md          Normative .filter / .minhash formats
  keyset-format.md          Normative .keys format
```

`hdtc sketch` and `hdtc keyset` derive their artifacts from one pass over an HDT
dictionary and share a single term-to-key convention (`src/hdt/artifacts.rs`),
which both formats assert by declaring `convention_id = 1`. For the `subjects`
and `objects` roles, filters, sketches, and key sets describe the same population
because that function has one definition.

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
