# HDT permutation index format, version 1

Status: normative format specification for hdtc.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY**
are to be interpreted as described by RFC 2119.

## 0. Purpose and relationship to the FoQ index

A standard HDT stores its triples once, in SPO order. Patterns rooted at a
predicate or an object are therefore not directly answerable from the exchange
format, and every HDT implementation adds something. The established answer is
the FoQ-style index (`.hdt.index.v1-1`), which stores, for each object and each
predicate, the list of `pos_y` positions pointing back into the SPO structures.

That design is complete but indirect. Every binary-search probe and every
enumerated row costs two scattered reads into the SPO structures — `ArrayY[pos_y]`
for the predicate, a rank over `BitmapY` for the subject — and the predicate index
has a different shape from the object index, so the two access paths share no
code. The format also predates the assumption that the whole file is memory-mapped
and read in place: it is a serialization of structures intended to be loaded into
memory at open.

`hdtc perm` writes an alternative for readers that memory-map their data and serve
scattered random access: **two additional complete triple permutations, POS and
OPS**, each encoded exactly like the HDT `BitmapTriples` a reader already knows,
plus **persisted rank directories for all six triples bitmaps** — the four in this
file and the two inside the associated HDT.

What that buys, relative to FoQ:

- **Contiguity without indirection.** In POS, a predicate's objects are one run
  and each `(predicate, object)` pair's subjects are another; in OPS the same
  holds for `(object, predicate)`. Enumerating a pattern is a sequential read of
  `ArrayZ`, not a gather through a second structure.
- **One access path.** POS, OPS, and the HDT's own SPO have identical shape, so a
  reader implements group descent, counting, and cursor resume once.
- **Arithmetic cardinality.** Every pattern except `S ? O` resolves to a
  half-open range, so its cardinality is a subtraction (§10.3).
- **Open without a warm-up pass.** Because the rank directories are persisted
  rather than computed, opening a dataset touches headers only. A reader that
  builds directories at open must instead read every bitmap byte in the HDT and
  in this file; a reader that maps this file reads `O(log)` directory pages on
  the first query and nothing before it. This is the property that makes it
  practical to hold many datasets open at once.

The cost is size: FoQ stores one triple-sized array, this format stores two
(§11). It is an alternative, not a successor — the two files are independent, a
dataset may carry either, both, or neither, and `hdtc` continues to write and read
FoQ indexes.

This file is derived data. It contains no information not recoverable from the
associated HDT, and it MUST NOT be treated as part of the dataset's content
identity.

## 1. Artifact and data model

For an HDT named `data.hdt`, automatic discovery uses one packed sidecar named:

```text
data.hdt.perm
```

The `.perm` suffix is appended to the complete HDT path. It does not replace the
`.hdt` suffix. This matches the convention used by `data.hdt.graphs`
([graphs-sidecar-format.md](graphs-sidecar-format.md) §1).

The associated HDT is a standard triples HDT with a four-section dictionary and
`BitmapTriples` in SPO order, containing `N` unique triples.

### 1.1 Identifier spaces

The dictionary numbers terms from 1. Let:

- `S` be the number of distinct subjects — the shared section followed by the
  subjects-only section, so subject identifiers occupy `[1, S]` with no gaps;
- `P` be the number of distinct predicates, occupying `[1, P]` with no gaps;
- `O` be the number of distinct objects — the shared section followed by the
  objects-only section, so object identifiers occupy `[1, O]` with no gaps.

Every identifier in each range occurs in at least one triple in that role: a
shared-section term appears by definition as both a subject and an object, and
the remaining sections are populated by construction. **This density is what
allows level 1 of every permutation to be implicit**, and a writer MUST verify it
rather than assume it (§12).

**These three counts are normative, and a writer MUST derive them from the
`string_count` preambles of the four PFC dictionary sections:**

```text
S = shared + subjects
P = predicates
O = shared + objects
```

A writer MUST NOT take them from the HDT's RDF Header. Header statements are
mutable metadata, and §9 deliberately excludes the Header from the identity
digest so that it can be rewritten without invalidating this sidecar — which is
exactly what makes it unfit to determine anything structural. A Header written or
edited by another implementation may disagree with the dictionary it describes.

The §4 header records these values, and a reader MUST verify them against the
dictionary (validation check 1). This check is load-bearing rather than
belt-and-braces: `S`, `P`, and `O` determine every packed width in §6.1. In
particular, an *inflated* `S` yields a wider `ArrayZ` that still encodes exactly
the right identifiers. Such a file satisfies every other requirement in this
specification — its triples decode correctly, its bitmaps and directories are
consistent, its checksums match — so without this check it would be a second
valid encoding of the same HDT, and the uniqueness claim of §12.1 would be false.

### 1.2 Permutations

Version 1 stores two permutations. Each is a three-level structure in which
level 1 is implicit, matching HDT `BitmapTriples`:

| | level 1 (implicit) | level 2 (`ArrayY`, `BitmapY`) | level 3 (`ArrayZ`, `BitmapZ`) |
|---|---|---|---|
| SPO (in the HDT, not here) | subject `[1, S]` | predicates per subject | objects per `(S,P)` |
| POS | predicate `[1, P]` | objects per predicate | subjects per `(P,O)` |
| OPS | object `[1, O]` | predicates per object | subjects per `(O,P)` |

Both permutations stored here have subject identifiers at level 3.

Let `M_POS` be the number of distinct `(predicate, object)` pairs and `M_OPS` the
number of distinct `(object, predicate)` pairs.

### 1.3 Logical operations

A reader can expose at least these operations without decompressing a whole
structure, where `perm` is `POS` or `OPS`:

| Operation | Result |
|---|---|
| `group_y(perm, id)` | Half-open `ArrayY` range of level-1 identifier `id` |
| `group_z(perm, y)` | Half-open `ArrayZ` range of the level-2 entry at position `y` |
| `y_of(perm, z)` | Level-2 position containing `ArrayZ` position `z` |
| `id_of(perm, y)` | Level-1 identifier containing `ArrayY` position `y` |
| `rank1(bitmap, p)` | Set bits in `[0, p)`, for `p` in `[0, L]` |
| `select1(bitmap, i)` | Position of the zero-based `i`-th set bit, for `i` in `[0, popcount)` |

A position may equal `L` only as the argument to `rank1`; `select1(popcount)` is
out of range. The same six operations over the HDT's SPO bitmaps are served by
the SPO directories in §7.3.

## 2. Primitive conventions

- All fixed-width integers are unsigned and little-endian.
- Offsets are absolute byte offsets from the start of the sidecar.
- **Interval notation is explicit.** `[a, b]` includes both endpoints; `[a, b)`
  excludes the upper one. The ambiguous `a..b` form is not used for value
  domains. Ranges of *positions* into an array or bitmap are half-open —
  `[0, N)` — while ranges of *dictionary identifiers* are inclusive and
  one-based, `[1, S]`; that difference is a recurring source of off-by-one
  errors and is called out again where each occurs.
- Packed bits are least-significant-bit first within each byte or word.
- The file header, the section directory, every section payload, and the footer
  start at 64-byte-aligned offsets.
- **Regions are tightly packed:** each region begins at the *smallest* 64-byte
  aligned offset at or after the end of the preceding region, and the footer
  begins at the smallest such offset after the last payload. A writer MUST NOT
  insert discretionary padding. Combined with the fixed region order of §3 and
  the ascending type order of §5.1, this leaves every offset — and hence the file
  size, the directory bytes, and both checksums — determined by the file's
  contents alone, which is what makes the format byte-reproducible (§12). A later
  version that wants larger alignment for some region must declare it in a field,
  not choose it silently.
- Padding bytes are zero and are excluded from section lengths and checksums.
- An absent zero-length section has offset zero.
- Writers MUST reject arithmetic overflow in sizes, counts, offsets, and
  alignment calculations.

`CRC32C` means Castagnoli CRC-32C, polynomial `0x1EDC6F41` in normal notation
(equivalent to `CRC_32_ISCSI`), stored as a little-endian `u32`.

### 2.1 Packed regions and mapped access

Unlike the HDT `LogArray` and `Bitmap` sections, the payloads in this file carry
**no preamble and no embedded checksum**. Element width, element count, and
CRC32C live in the section directory (§6), so every payload is a bare packed
region beginning at a 64-byte-aligned offset. This is deliberate: a mapped reader
computes a bit offset and loads directly, with no per-section parse and no
unaligned base. The same choice is made by the Elias–Fano regions of the graphs
sidecar (graphs-sidecar-format.md §8.1).

**Mapped-load guarantee.** A reader MAY load a full 64-bit word at any byte
offset within a section's declared `[offset, offset + length)` without reading
past end of file. This follows from 64-byte section alignment and the mandatory
64-byte footer, and writers MUST preserve it.

**Packed integer arrays.** A packed array of `n` entries at `w` bits per entry
occupies `ceil(n * w / 8)` bytes. Entry `i` begins at bit `i * w`, LSB-first.
`w` is in `[0, 64]` — 64 inclusive, since the `SUPERRANK` sections use it —
and `w = 0` means every entry is zero. Unused high bits in the final byte MUST
be zero.

**Bitmaps.** A bitmap of `L` bits occupies `ceil(L / 8)` bytes, LSB-first within
each byte. Unused tail bits MUST be zero.

## 3. File layout

Sections occur in this order:

```text
[256-byte header]
[zero padding]
[section directory]
[directory CRC32C]
[zero padding]
[section payload 0]
[zero padding]
[section payload 1]
...
[zero padding]
[64-byte footer]
```

Payloads occur in ascending section-directory order and MUST NOT overlap.
Readers MUST use recorded offsets rather than assume that regions are adjacent.

## 4. File header

The header is exactly 256 bytes.

| Offset | Size | Field | Version 1 value or meaning |
|---:|---:|---|---|
| 0 | 8 | Magic | ASCII `$HDTPERM` |
| 8 | 2 | Major version | `1` |
| 10 | 2 | Minor version | `0` |
| 12 | 4 | Header size | `256` |
| 16 | 8 | Flags | See below |
| 24 | 8 | Triple count | `N` |
| 32 | 8 | Subject count | `S` (§1.1; verified against the dictionary) |
| 40 | 8 | Predicate count | `P` (§1.1; verified against the dictionary) |
| 48 | 8 | Object count | `O` (§1.1; verified against the dictionary) |
| 56 | 8 | POS pair count | `M_POS` |
| 64 | 8 | OPS pair count | `M_OPS` |
| 72 | 8 | Source data length | Associated HDT dictionary-and-triples suffix length |
| 80 | 8 | Sidecar size | Exact file size, including footer |
| 88 | 8 | Directory offset | Absolute, 64-byte aligned |
| 96 | 8 | Directory length | Exactly `section_count * 64 + 4` |
| 104 | 4 | Section count | Number of directory entries; at least 20 (§5) |
| 108 | 4 | Identity algorithm | `1`, meaning SHA-256 |
| 112 | 8 | Footer offset | Exactly `sidecar_size - 64` |
| 120 | 4 | Superblock bits | `4096` |
| 124 | 4 | Subblock bits | `512` |
| 128 | 32 | Source digest | SHA-256 described in section 9 |
| 160 | 92 | Reserved | All zero |
| 252 | 4 | Header CRC32C | CRC32C of bytes `[0, 252)` |

Flags are assertions:

| Bit | Name | Meaning |
|---:|---|---|
| 0 | `HAS_POS` | The POS permutation is present and complete |
| 1 | `HAS_OPS` | The OPS permutation is present and complete |
| 2 | `HAS_SPO_DIRECTORIES` | Rank directories for the HDT's SPO bitmaps are present |
| 3–63 (inclusive) | Reserved | Zero |

Version 1 writers MUST set bits 0, 1, and 2. The bits exist so that a later minor
version can describe a partial file without changing the directory's meaning; a
version 1 reader MUST reject a file in which any of the three is clear.

Version 1 writers MUST write `4096` and `512` for the superblock and subblock
widths. Readers MUST reject other values in version 1 but MUST read the fields
rather than assume the constants, so a later version can change the sampling
density without changing the layout.

## 5. Section types

A section type is a `u32` composed as `(component << 8) | kind`.

| Component | Value |
|---|---:|
| POS | `0x01` |
| OPS | `0x02` |
| SPO (directories only) | `0x03` |
| Reserved for a further permutation | `0x04`–`0x0F` (inclusive) |

| Kind | Value | Payload |
|---|---:|---|
| `ARRAY_Y` | `0x01` | Packed array, level-2 values |
| `BITMAP_Y` | `0x02` | Bitmap over `ArrayY` positions |
| `ARRAY_Z` | `0x03` | Packed array, level-3 values |
| `BITMAP_Z` | `0x04` | Bitmap over `ArrayZ` positions |
| `BITMAP_Y_SUPERRANK` | `0x05` | Packed `u64` array (§7.2) |
| `BITMAP_Y_SUBRANK` | `0x06` | Packed `u16` array (§7.2) |
| `BITMAP_Z_SUPERRANK` | `0x07` | Packed `u64` array (§7.2) |
| `BITMAP_Z_SUBRANK` | `0x08` | Packed `u16` array (§7.2) |
| `SPO_POSITION_MAP` | `0x09` | Defined optional section; see §13.1 |
| Reserved | `0x0A`–`0xFF` (inclusive) | — |

### 5.1 Core sections

A version 1 file contains these twenty **core** sections, each exactly once:

- POS: `0x0101` … `0x0108`
- OPS: `0x0201` … `0x0208`
- SPO: `0x0305`, `0x0306`, `0x0307`, `0x0308`

Component `0x03` has no `ARRAY_*` or `BITMAP_*` sections: the SPO structures live
in the associated HDT and are not copied here. Only their directories are stored.

A file MAY additionally contain any number of **optional** sections, subject to
§6's forward-compatibility rule; §13 lists those defined and reserved so far.
Entries MUST
appear in ascending `type` order and each type MUST appear at most once, so the
directory holds at least twenty entries and `section_count` is at least 20.

## 6. Section directory

The directory contains `section_count` consecutive 64-byte entries, followed
immediately by a little-endian CRC32C over all entry bytes.

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | Section type | §5 |
| 4 | 4 | Section flags | See below |
| 8 | 8 | Payload offset | Absolute, 64-byte aligned; zero if empty |
| 16 | 8 | Payload length | Bytes excluding padding; zero if empty |
| 24 | 8 | Entry count | Packed elements, or bits for a bitmap |
| 32 | 1 | Bits per entry | Packed width `w`; `1` for a bitmap |
| 33 | 3 | Reserved | Zero |
| 36 | 4 | Payload CRC32C | CRC of payload bytes; zero if empty |
| 40 | 8 | Parameter | Type-specific; zero unless stated |
| 48 | 8 | Indexed bit length | Rank-directory sections only; zero otherwise |
| 56 | 8 | Reserved | Zero |

`Entry count` always counts the elements of *this* section's payload. For a
`SUPERRANK` or `SUBRANK` section that is the number of samples (§7.2), **not** the
length of the bitmap being indexed; the latter is carried separately in
`Indexed bit length` so that a reader can validate a directory against its bitmap
exactly rather than inferring a range from the sample count.

Section flags:

| Bit | Name | Meaning |
|---:|---|---|
| 0 | `REQUIRED` | A reader that does not understand this type MUST reject the file |
| 1–31 (inclusive) | Reserved | Zero |

All twenty core sections MUST set `REQUIRED`. **Forward compatibility rule:** a
reader MUST reject a file containing an unknown section type with `REQUIRED` set,
and MUST ignore an unknown section type with `REQUIRED` clear. This is the
mechanism by which optional sections (§13) are added without a version change, and
it is why an unrecognized type is not by itself an error.

For a `SUPERRANK` section, `Parameter` is the superblock width in bits; for a
`SUBRANK` section, the subblock width in bits. Both MUST equal the corresponding
header field. `Parameter` is zero for all other version 1 types.

### 6.1 Required sizes

For component `X` in {POS, OPS}, with `M_X` pairs:

| Section | Entry count | Bits per entry | Payload length |
|---|---|---|---|
| `ARRAY_Y` | `M_X` | `ceil(log2(V + 1))` where `V` is the maximum level-2 value | `ceil(M_X * w / 8)` |
| `BITMAP_Y` | `M_X` | `1` | `ceil(M_X / 8)` |
| `ARRAY_Z` | `N` | `ceil(log2(S + 1))` | `ceil(N * w / 8)` |
| `BITMAP_Z` | `N` | `1` | `ceil(N / 8)` |

The maximum level-2 value is `O` for POS and `P` for OPS. Level-2 and level-3
values are dictionary identifiers, which are one-based, so the maximum value is
the count itself and the width covers `V`, not `V - 1`. A writer MUST use the
minimum sufficient width; a reader MUST NOT assume a particular width.

**Rank-directory widths are fixed by section kind, not by content:** every
`SUPERRANK` section has `bits_per_entry = 64` and every `SUBRANK` section has
`bits_per_entry = 16`, in every file including an empty one. A reader MUST reject
any other value. Their entry counts and payload lengths are given in §7.2, and
their `Indexed bit length` values are:

| Directory sections of | `Indexed bit length` |
|---|---|
| POS `BITMAP_Y` / `BITMAP_Z` | `M_POS` / `N` |
| OPS `BITMAP_Y` / `BITMAP_Z` | `M_OPS` / `N` |
| SPO `BITMAP_Y` / `BITMAP_Z` | `n_sp` / `N` |

`n_sp` is the associated HDT's distinct `(subject, predicate)` pair count. A
reader MUST validate the two SPO `Indexed bit length` values against the bit
lengths it parses from the HDT's `BitmapTriples` sections and MUST reject a
mismatch: a rank directory is meaningless against a different bitmap, and this is
the check that catches a sidecar paired with the wrong HDT before any query
silently returns wrong answers.

### 6.2 Empty datasets

If `N = 0` then `S`, `P`, `O`, `M_POS`, and `M_OPS` are all zero, and all twenty
core sections are present with zero entry count, zero length, zero offset, zero
CRC, and zero `Indexed bit length`. `Bits per entry` is:

| Section kind | `Bits per entry` when `N = 0` |
|---|---:|
| `ARRAY_Y`, `ARRAY_Z` | `0` (the width formulas of §6.1 yield zero for an empty value space) |
| `BITMAP_Y`, `BITMAP_Z` | `1` |
| `SUPERRANK` | `64` (fixed by kind, §6.1) |
| `SUBRANK` | `16` (fixed by kind, §6.1) |

This is the canonical encoding of the **twenty core sections** when `N = 0`.
§7.2's sample-count formulas do not apply to a zero-length bitmap: an empty
directory stores no sentinel.

It constrains nothing else. An empty file MAY carry optional sections, and the
only rule this section imposes on them is on the position maps of §13.1, whose
contents are a function of `N` and which are therefore empty when `N = 0` by
their own width and count rules. **A reader MUST NOT infer that an unknown
optional section is empty because `N = 0`,** and MUST NOT reject one that is not:
a future optional section may carry metadata independent of the triple count, and
§6's forward-compatibility rule already obliges a reader to ignore an unknown
type with `REQUIRED` clear rather than reason about its contents.

## 7. Permutation encoding

### 7.1 Structure

`ArrayY` holds level-2 values, grouped by level-1 identifier in ascending
identifier order. `BitmapY` has one bit per `ArrayY` entry; **a set bit marks the
last entry of a level-1 group.** `ArrayZ` holds level-3 values, grouped by
`ArrayY` position; `BitmapZ` has one bit per `ArrayZ` entry and **a set bit marks
the last entry of a level-2 group.** This is the HDT `BitmapTriples` convention
exactly.

Normative content requirements:

1. Level-2 values are **strictly increasing** within each level-1 group.
2. Level-3 values are **strictly increasing** within each level-2 group.
3. No group is empty: every level-1 identifier in `[1, V1]` has at least one
   `ArrayY` entry, and every `ArrayY` entry has at least one `ArrayZ` entry.
4. **The final bit of a non-empty `BITMAP_Y` or `BITMAP_Z` MUST be set.** A group
   is delimited by the set bit that ends it, so a clear final bit would leave a
   suffix of the array in no group at all and unreachable — a three-bit `BitmapY`
   of `110` has the required two set bits yet strands its third `ArrayY` entry.
   Population count alone does not exclude this.
5. `popcount(BITMAP_Y) = V1`, where `V1` is `P` for POS and `O` for OPS.
6. `popcount(BITMAP_Z) = M_X`.
7. The multiset of triples recovered from the permutation equals the associated
   HDT's triple set exactly.

Requirement 3 is what makes level 1 implicit, requirement 1 is what makes level-2
lookup a binary search, and requirement 4 is what makes the group formulas below
total. A reader is entitled to rely on all three without checking, so a writer
MUST enforce them.

Group boundaries are derived as follows, with `select1` zero-based:

```text
group_y(id)  = [ id == 1 ? 0 : select1(BITMAP_Y, id - 2) + 1,
                 select1(BITMAP_Y, id - 1) + 1 )

group_z(y)   = [ y == 0  ? 0 : select1(BITMAP_Z, y - 1) + 1,
                 select1(BITMAP_Z, y) + 1 )

id_of(y)     = rank1(BITMAP_Y, y) + 1
y_of(z)      = rank1(BITMAP_Z, z)
```

### 7.2 Rank directories

Each bitmap of `L > 0` bits has a two-level cumulative-popcount directory. With
superblock width `B = 4096` and subblock width `b = 512`:

- `superrank[k]`, a `u64`: set bits before bit `min(k * B, L)`. There are
  `ceil(L / B) + 1` entries; the final entry is the total population count.
- `subrank[j]`, a `u16`: set bits from the start of the containing superblock to
  bit `min(j * b, L)`. There are `ceil(L / b)` entries. Every eighth entry — the
  first of each superblock — is zero.

If `L = 0`, both directory sections for that bitmap are empty in the §6.2 sense:
no sentinel is stored, `rank1(0) = 0`, and `select1` has empty domain.

`subrank` values are bounded by `B - b = 3584`, since the last sampled point in a
superblock is its eighth subblock start, so `u16` suffices with room to spare; a
writer MUST reject any computed value that does not fit.

Both arrays are packed at their natural widths (`w = 64` and `w = 16`), so
`superrank` occupies `(ceil(L / B) + 1) * 8` bytes and `subrank` occupies
`ceil(L / b) * 2` bytes. Together they are approximately `3L / 512` bytes, or
**about 4.7% of the bitmap they index**. Because a permutation's bitmaps are one
bit per element against arrays of `ceil(log2(S + 1))` bits, all four directories
of one permutation come to roughly **0.2% of that permutation's total bytes**.

Derived operations, for `L > 0`:

```text
rank1(L)   = superrank[ceil(L / B)]              // the sentinel; total popcount

rank1(p)   = superrank[p / B] + subrank[p / b]   // for p < L
             + popcount of bits in [ (p / b) * b, p )

select1(i) = largest k with superrank[k] <= i, then
             largest j in that superblock with superrank[k] + subrank[j] <= i,
             then scan at most b bits
```

The `p == L` case is separated deliberately: neither sample array carries a
boundary entry beyond its last block, so when `L` is a multiple of `b` the general
formula would index `subrank[L / b]`, one past the end. Callers legitimately ask
for `rank1(L)` — it is how a half-open range ending at the bitmap's end is
counted — so the domain includes it and the sentinel answers it in one load.

`rank1` is constant time — one `u64` load, one `u16` load, and at most eight
`u64` popcounts. `select1` is `O(log(L / B))` for the binary search plus the same
bounded scan. Version 1 stores no select samples; §13.2 records the extension point
if profiling ever justifies them.

### 7.3 SPO directories

Component `0x03` carries the four directory sections for the associated HDT's own
`BitmapY` and `BitmapZ`. Their content and derivation are exactly as in §7.2; the
bitmaps they describe are read from the HDT, not from this file, and their lengths
are recorded in `Indexed bit length` (§6.1).

They exist here because the HDT exchange format has no room for them: adding a
section to `data.hdt` would make it non-standard. Persisting them beside the
permutations means a reader can open a dataset and answer subject-rooted patterns
without a warm-up pass over the HDT's bitmaps, which is otherwise the single
largest cost of opening a large dataset.

## 8. Footer

The footer is exactly 64 bytes and starts at `footer_offset`.

| Offset | Size | Field | Version 1 value or meaning |
|---:|---:|---|---|
| 0 | 8 | Magic | ASCII `$HDTPEND` |
| 8 | 2 | Major version | `1` |
| 10 | 2 | Minor version | `0` |
| 12 | 4 | Footer size | `64` |
| 16 | 8 | File size | Exact sidecar size |
| 24 | 8 | Header offset | `0` |
| 32 | 8 | Directory offset | Copy of header field |
| 40 | 8 | Directory length | Copy of header field |
| 48 | 4 | Section count | Copy of header field |
| 52 | 4 | Reserved | Zero |
| 56 | 4 | Header CRC32C copy | Copy of header checksum |
| 60 | 4 | Footer CRC32C | CRC32C of bytes `[0, 60)` |

The footer exists so that a truncated or concatenated file is detected without
trusting the header alone, and so that a reader recovering from a damaged header
can still locate the directory. Its copied fields MUST equal the header's; a
reader MUST reject any disagreement rather than prefer one copy.

## 9. Binding to the associated HDT

The identity input begins at the first byte of the HDT Dictionary Control
Information and extends through end of file. It therefore contains the complete
Dictionary and Triples sections, including their control information and
checksums.

The header stores both the byte length of this suffix and its SHA-256 digest, and
also the HDT's decoded triple count `N`. A strict reader MUST verify all three
values before trusting the permutations.

The HDT Global and Header sections are deliberately excluded, so `hdtc header`
rewrites do not invalidate the sidecar. Any change to dictionary identifiers or
`BitmapTriples` data does invalidate it.

**This is byte-for-byte the same binding the graphs sidecar uses**
(graphs-sidecar-format.md §10), so one verification routine serves both artifacts.

A normal low-latency open MAY compare only the suffix length and triple count
after validating fixed metadata. Operations that cross a trust boundary MUST also
recompute SHA-256.

## 10. Pattern resolution (non-normative)

Recorded here because the permutation choice is only justified by the patterns it
serves. `?` denotes an unbound position.

### 10.1 Direct resolution

| Pattern | Structure | Resolution |
|---|---|---|
| `? ? ?` | SPO | Full `ArrayZ` scan |
| `S ? ?` | SPO | `group_y(S)`, then the spanning `ArrayZ` range |
| `S P ?` | SPO | Binary search `P` in `group_y(S)`, then `group_z` |
| `S P O` | SPO | As above, then binary search `O` |
| `? P ?` | POS | `group_y(P)`, then the spanning `ArrayZ` range |
| `? P O` | POS | Binary search `O` in `group_y(P)`, then `group_z` |
| `? ? O` | OPS | `group_y(O)`, then the spanning `ArrayZ` range |

### 10.2 `S ? O`

This pattern is contiguous in no permutation stored here. It is resolved by
probing one side:

- via SPO, binary search `O` in each level-2 group of `S`;
- via OPS, binary search `S` in each level-2 group of `O`.

Because triples are unique, each predicate contributes at most one result, so the
answer is bounded by `P` regardless of which side is probed. A reader SHOULD
choose the side with the smaller level-1 group, making the worst case
`min(deg(S), deg(O))`-shaped. Both sides yield results in ascending predicate
order, so the choice does not affect enumeration order and MAY differ between
resumptions of the same enumeration.

An OSP permutation would resolve this pattern in a single descent. It is not
stored, because OPS additionally serves predicate-filtered object resolution —
"which subjects link to this object under these predicates" — which OSP degrades
to a scan of every subject of the object, and which is the dominant object-rooted
operation for text-index and label consumers.

### 10.3 Cardinality

Every pattern in §10.1 resolves to a half-open `ArrayZ` range, so its cardinality
is the range width: exact, and computed without enumeration. `S ? O` has no such
shortcut and costs what enumeration costs, which is bounded by `P`.

## 11. Size (non-normative)

For one permutation, ignoring directories:

```text
bits = N * ceil(log2(S + 1))       ArrayZ
     + N                            BitmapZ
     + M * ceil(log2(V + 1))        ArrayY
     + M                            BitmapY
```

Directories add approximately 0.2% (§7.2). Both permutations share the same
`ArrayZ` width, so the file is dominated by `2 * N * ceil(log2(S + 1))` bits.

Compared with a FoQ index over the same HDT, which stores one triple-sized array
of `pos_y` values plus a predicate index of `n_sp` entries, this format's dominant
term is roughly doubled. That is the trade being made: two sequential-access
permutations in place of one indirection index.

Widths are fixed rather than delta-compressed. This costs bytes against a
block-compressed alternative and is deliberate: fixed widths are directly
addressable under `mmap`, so random access costs a page fault at most and never a
block decode, and the operating system's page cache serves as the buffer pool.
For the scattered access this format exists to serve, that trade favours fixed
width; for pure sequential scans it does not.

## 12. Conformance and validation

A version 1 reader MUST accept version exactly 1.0 and MUST reject unknown flags,
nonzero reserved fields, invalid ranges, overlaps, duplicate or out-of-order
section types, missing core sections, unknown `REQUIRED` section types, and
arithmetic overflow.

Strict validation MUST additionally verify:

1. The source-HDT binding of §9 — suffix length, SHA-256 digest, and triple
   count `N` — **and the header's `S`, `P`, and `O` against the four PFC
   dictionary section counts** per §1.1 (`S = shared + subjects`,
   `P = predicates`, `O = shared + objects`). The second half is what pins every
   packed width: in particular, an inflated `S` widens `ArrayZ` without changing
   the identifiers it encodes, so checks 2 through 9 all pass on such a file and
   §12.1's uniqueness fails. Nothing else in this list can detect it.
2. Header CRC32C, footer CRC32C, the footer's copied fields against the header,
   the directory CRC32C, and every payload CRC32C.
3. Section alignment, non-overlap, ascending order, and the §6.1 sizes against
   the header counts.
4. For each permutation: bitmap population counts (§7.1 requirements 5 and 6),
   the terminal boundary bits (requirement 4), strictly increasing values within
   every group (requirements 1 and 2), and no empty groups (requirement 3).
5. Every rank directory of a bitmap with `L > 0` against a recomputed cumulative
   popcount of that bitmap, including the `superrank` sentinel and the
   zero-valued `subrank` entries at superblock starts. A directory of a
   zero-length bitmap is instead checked to be empty per §6.2, sentinel included:
   it has none.
6. Every `Indexed bit length`, including the two SPO values against the bit
   lengths parsed from the associated HDT (§6.1).
7. The canonical byte representation: tight region packing per §2 — every region
   at the smallest aligned offset following its predecessor, so no discretionary
   gaps — zero-valued padding bytes, zero unused high bits in the final byte of
   every packed array, and zero unused tail bits in every bitmap. A file that
   decodes correctly but is non-canonical is invalid.
8. That each permutation's triples, decoded in order, are a permutation of the
   HDT's SPO triples — an external sort of one side, or a streaming comparison
   after re-sorting, since the orders differ.
9. Every optional section (§13) the validator understands, by that section's own
   rules. A validator that does not understand a present optional section MUST
   report it as unvalidated rather than as valid. For an `SPO_POSITION_MAP` the
   rules are: entry count equals `N`; `bits_per_entry` is the minimum width of
   §13.1; every entry lies in `[0, N)`; the entries are pairwise distinct; and the
   triple at each mapped SPO position equals the triple at the corresponding
   position of the map's own permutation. Only the last of these has any
   semantic content — a map with correct metadata, a correct CRC, and wrong
   contents passes checks 1 through 8 unchanged, and consumers use it to scope
   queries against position-keyed sidecars, so a silent error there produces
   wrong answers rather than a detected fault.

Validation MUST be implementable with bounded memory. Checks 8 and 9 MAY use an
external sort rather than an allocation proportional to `N`.

### 12.1 Byte reproducibility

Within a fixed **section set**, the file is a function of the associated HDT: the
region order of §3, the ascending type order of §5.1, the tight packing of §2,
and the canonical representation of check 7 together leave no writer discretion,
so two independent implementations emitting the same sections MUST produce
byte-identical output.

Reproducibility is scoped that way because §13 leaves the optional sections to
the writer — a file with an OPS position map and one without are both conforming
for the same HDT. The **core profile** — exactly the twenty core sections of
§5.1 and no optional section — is therefore the canonical form for conformance
testing: for a given HDT there is exactly one valid core-profile file, so a
checksum comparison against a published vector is a complete test of a writer.
Comparisons involving optional sections MUST state the section set.

Because this file is derived data, a reader that fails validation MAY discard it
and fall back to any other available access path, including regenerating it. A
reader MUST NOT silently answer queries from a file that failed validation.

## 13. Optional and reserved sections

Two different states are described here and MUST NOT be confused. A **defined
optional section** (§13.1) is fully specified and legal to emit in a version 1
file today; it is absent from typical files by a writer's choice, not because
the format withholds it. A **reserved** type (§13.2) has its type space claimed
but no specified meaning, and a version 1 writer MUST NOT emit one.

Both categories exist under §6's forward-compatibility rule, but only the first
is written today. A **defined optional section is written with `REQUIRED` clear**,
so a file carrying one stays readable by a version 1 reader that ignores what it
does not implement; it is subject to validation check 9, and it sits outside the
core profile, so a file containing it is compared only against a stated section
set (§12.1). Nothing in §13.2 is emitted by a version 1 writer at all. When one of
those types is later defined — without a `format_version` bump, which is the point
of reserving it — it MUST likewise be written with `REQUIRED` clear, since that is
the only thing that keeps already-deployed readers working.

### 13.1 `SPO_POSITION_MAP` — defined, optional

**Type `0x0109` maps POS positions; type `0x0209` maps OPS positions.** Both are
valid version 1 sections. The two are independent: a writer MAY emit either,
both, or neither, since a consumer that scopes only object-rooted patterns needs
only the OPS map.

A packed array of `N` entries mapping each permutation's `ArrayZ` position to the
corresponding SPO `ArrayZ` position. Entries are **zero-based positions in
`[0, N)`**, unlike the one-based dictionary identifiers of §6.1, so the minimum
width is `ceil(log2(N))` for `N > 0` and `0` for `N = 0` — not
`ceil(log2(N + 1))`, which would waste a bit per triple at every power of two.
Validation rules are validation check 9.

This lets a consumer test a triple found through POS or OPS against any
SPO-position-keyed sidecar. Without it, that test costs an SPO descent per
candidate; with it, one array read.

**It is no longer the recommended route for graph scoping**, which was its
original motivation. [graphs-index-format.md](graphs-index-format.md) stores
graph membership layers keyed directly to POS and OPS positions, so a
graph-scoped index-side pattern is answered in its native space by a rank
difference. That is strictly better than what a position map can offer: mapping
positions lowers the per-candidate constant but leaves scoped cardinality linear
in the pattern range, whereas a natively keyed layer makes it exact and constant.
The maps remain defined here because the mapping is a plausible primitive for
other position-keyed sidecars, and because fixing their type ids and widths now
costs nothing.

**Writers SHOULD omit both maps by default.** Each adds roughly `log2(N)` bits per
triple, a large fraction of the file, for a benefit only specialized consumers of
other SPO-position-keyed sidecars collect. A graphs-index builder can also use a
map to avoid reconstructing the same correspondence, but graph-index queries do
not require one. The reason to specify the maps now rather than later is that the
type ids, widths, and validation rules are the part that must not change once
files exist; having them fixed lets a deployment enable them without a
`format_version` bump or a reader update.

### 13.2 Reserved for future definition

A version 1 writer MUST NOT emit either of these; their semantics are not
specified, so a reader encountering one cannot validate it.

- **Select samples.** A sampled `select1` position array per bitmap, reducing
  `select1` from a binary search to a bounded scan. Version 1 omits them because
  the binary search is over `ceil(L / 4096) + 1` entries, which is small at every
  scale hdtc targets. No kind is assigned; assign from the reserved range when
  profiling justifies it.
- **A third permutation.** Component values `0x04`–`0x0F` (inclusive) are
  reserved. OSP is the candidate (§10.2) should `S ? O` prove hot enough to
  warrant its own structure.

## 14. Construction guidance (non-normative)

The format is designed for streaming construction with bounded memory, matching
the rest of hdtc's write path.

Each permutation is one external sort of the HDT's triples into the target order,
followed by a single streaming pass that emits `ArrayY`, `BitmapY`, `ArrayZ`, and
`BitmapZ` through the existing `StreamingLogArrayEncoder` and
`StreamingBitmapEncoder` — the same encoders the HDT builder uses, since the
structures are the same. The FoQ index builder already performs the OPS sort, so
that machinery is reused rather than written.

The element widths must be known before packing begins. They follow from `S`,
`P`, and `O`, which §1.1 normatively requires be read from the four PFC
dictionary section preambles rather than from the RDF Header. Reading four
preambles is cheap and needs no triples pass, so nothing is lost by taking the
structural source — and a width derived from a stale Header is a defect that only
validation check 1 detects.

Rank directories are computed during the same pass that writes each bitmap: the
encoder already holds each word as it is emitted, so the cumulative popcounts
cost one running counter and no additional read. The SPO directories require one
sequential read of the HDT's two bitmaps, which is the only part of construction
that touches the associated HDT's triples region beyond the sort input.

Payloads may be spooled to scratch files in section order and concatenated with
alignment padding in a final pass, so that the section directory — which needs
every payload's final offset, length, and CRC — can be written after the payloads
are sized but before they are copied into place.
