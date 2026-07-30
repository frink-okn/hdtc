# HDT graphs index format, version 1

Status: normative format specification for hdtc.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY**
are to be interpreted as described by RFC 2119.

## 0. Purpose and relationship to the graphs sidecar

The graphs sidecar ([graphs-sidecar-format.md](graphs-sidecar-format.md)) stores
graph membership as a set of SPO positions per graph. That is the right shape for
its own question — *given a graph, which triples?* — and it answers it with
`rank`, `select`, and `count` at effectively constant cost.

It also answers more than it appears to. Because each layer's contribution to a
contiguous SPO range is a rank difference, the **total membership count** of a
range follows from one pass over the layers:

```text
memberships(a, b) = sum over g in 0..G of ( rank(g, b) - rank(g, a) )
```

so quad-view cardinality over an SPO-contiguous pattern already costs `O(G)` rank
operations on the sidecar alone. Any claim that it requires a scan is wrong.

What the sidecar genuinely cannot do efficiently is:

1. **Scope an index-side pattern.** A pattern rooted at a predicate or an object
   is contiguous in the POS or OPS permutation
   ([permutation-index-format.md](permutation-index-format.md)), not in SPO. Its
   matching triples are scattered through SPO position space, so no rank
   difference applies in either the scoped or the quad-view direction. Two
   fallbacks exist — walk the pattern range and probe each candidate's
   membership, or walk the layer and test each member's predicate or object — so
   the cost is `O(min(range, count(g)))`, not `O(range)`. But both terms are
   properties of the data, neither is bounded by the answer size or by any
   published cap, and for a large graph over a common predicate both are large.

   **Enumeration is the sharper problem, not counting.** Writing `A` for the
   pattern's triples and `B` for the graph's, a reader takes whichever strategy
   is better, so scoping by probe produces results at a fill rate of
   `|A ∩ B| / min(|A|, |B|)`. That ratio has no lower bound, and where it is
   small a page of results costs an unbounded number of candidate probes and
   returns truncated having produced almost nothing. With a natively keyed layer
   set, `next_member` walks the intersection directly and every step yields a
   result.

   Note what the `min` implies about *when* this bites: it requires `A` and `B`
   both large and their intersection small. A pattern or a graph that is small in
   absolute terms bounds the scan by its own size regardless of selectivity, so
   the structure that makes this pay is many mid-sized graphs rather than a few
   coarse ones.
2. **Project the graphs of a triple.** *Given a position, which graphs?* is the
   transpose of what the sidecar stores, so it costs `G + 1` probes per row.

This file addresses both. The first is the load-bearing one: it is a
correctness-of-cost problem with no workaround. The second is an optimisation of
a loop whose cost is already bounded by the graph count, and it should be
adopted on evidence rather than by default (§9).

This file contains **no information that is not already in the associated HDT and
graphs sidecar**. It is a derived index: optional, separately droppable, and
regenerable from its two parents. Dropping it degrades the operations above to
the costs just described; it never changes an answer.

That distinction determines where these structures live. The graphs sidecar is
**data** — it carries membership, which the HDT does not. This file and the
permutation index are **indexes** over that data. An index binds to every parent
it was derived from and may be discarded at any time; data binds to one parent
and may not. It is also why the permutation index carries no graph structures: it
depends on the HDT alone, and folding graph layers into it would make a bundle's
core query index depend on a graphs sidecar it has no other need for.

## 1. Artifact and data model

For an HDT named `data.hdt` with a graphs sidecar `data.hdt.graphs`, automatic
discovery uses one packed file named:

```text
data.hdt.graphs.idx
```

The suffix is appended to the complete graphs-sidecar path.

A reader MUST NOT open this file unless both parents are present. If
`data.hdt.graphs` is absent, this file MUST be absent; a reader encountering it
without its sidecar MUST reject it.

Let `N` be the HDT's triple count, `G` the sidecar's named graph count, and `M`
its membership count, all as defined by graphs-sidecar-format.md §1. Graph IDs
are `0..G`, with 0 denoting the RDF default graph, exactly as in the sidecar.
This file introduces no graph identifiers and carries **no graph dictionary**:
term resolution stays in the sidecar, which owns it.

### 1.1 Contents

Version 1 defines four independently optional structures.

| Structure | Answers | Cost on the sidecar alone |
|---|---|---|
| POS-keyed layer set | scoped cardinality and enumeration for `? P ?`, `? P O` | `O(min(range, count(g)))` per §0 |
| | quad-view cardinality for the same | `O(sum over g of min(range, count(g)))` |
| OPS-keyed layer set | the same for `? ? O` | the same |
| Transpose `BitmapG` | quad-view cardinality, SPO-contiguous | `O(G)` rank operations |
| Transpose `ArrayG` | `graphs_of(position)` | `G + 1` probes per row |

Note that the quad-view fallback is not one probe per candidate: with the graph
unbound, every candidate's *whole* membership set is needed, so the work is
per-graph, not per-triple.

The two groups differ in kind, and §9's build policy turns on the difference:

- **Layer sets remove an unbounded cost.** Without them, no index-side path —
  scoped or quad-view, counting or enumerating — is bounded by anything but the
  data, and enumeration in particular has an unbounded candidates-per-result
  ratio (§0).
- **Both transpose halves remove a bounded cost.** They replace `O(G)` rank
  operations with `O(log(M / 4096))` select operations, so this is a trade
  between two small quantities rather than a change of complexity class. They
  earn their bytes only where `G` is large enough to dominate that logarithm.

`BitmapG` and `ArrayG` are separately optional because their sizes scale
differently: `BitmapG` is one bit per membership regardless of `G`, while
`ArrayG` grows with `log G` and dominates the pair at any realistic graph count
(§9). `ArrayG` requires `BitmapG`; the converse does not hold.

### 1.2 Position spaces

A **position space** is an ordering of the HDT's `N` triples. Version 1 names
three:

| Value | Space | Position meaning |
|---:|---|---|
| `1` | SPO | BitmapTriples SeqZ position in `data.hdt` — the sidecar's own space |
| `2` | POS | `ArrayZ` position in the POS permutation |
| `3` | OPS | `ArrayZ` position in the OPS permutation |

Spaces 2 and 3 are defined by permutation-index-format.md §7.1. They are a
function of the HDT alone — the permutation orderings are total and normative —
so this file does **not** require a `.perm` file to exist, only the ordering it
also implements. A reader that has both MUST verify their triple counts agree.

Every layer set stores the same memberships as the sidecar, renumbered into its
space. Consequently, for every graph `g` and every space, `count(g)` is
identical and the layer counts sum to `M`. **These equalities are necessary but
far from sufficient** — see §7, which is explicit about what they can and cannot
detect.

## 2. Primitive conventions

All conventions of permutation-index-format.md §2 apply, with one localised
exception noted below: little-endian fixed-width integers, absolute offsets,
half-open ranges, LSB-first bit packing, 64-byte alignment for the header,
section directory, section payloads, and footer, zero padding excluded from
lengths and checksums, and `CRC32C` meaning Castagnoli CRC-32C stored as a
little-endian `u32`.

**Exception.** The layer payload *region* sections (`0x0102`, `0x0202`) are span
descriptors rather than owned payloads: their lengths include interior alignment
padding and their CRC fields are zero even when non-empty. §5.1 states the rule
and the reason. No other section type departs from the conventions above.

The mapped-load guarantee of permutation-index-format.md §2.1 applies: a reader
MAY load a full 64-bit word at any byte offset within a section's declared extent
without reading past end of file.

A packed array of `n` entries at `w` bits per entry occupies `ceil(n * w / 8)`
bytes, entry `i` beginning at bit `i * w`, LSB-first, with unused high bits in
the final byte zero. A bitmap of `L` bits occupies `ceil(L / 8)` bytes with
unused tail bits zero.

## 3. File layout

```text
[256-byte header]
[zero padding]
[section directory]
[directory CRC32C]
[zero padding]
[section payloads, in ascending section-directory order]
[zero padding]
[64-byte footer]
```

## 4. File header

The header is exactly 256 bytes.

| Offset | Size | Field | Version 1 value or meaning |
|---:|---:|---|---|
| 0 | 8 | Magic | ASCII `$HDTGIDX` |
| 8 | 2 | Major version | `1` |
| 10 | 2 | Minor version | `0` |
| 12 | 4 | Header size | `256` |
| 16 | 8 | Flags | See below |
| 24 | 8 | Triple count | `N` |
| 32 | 8 | Named graph count | `G` |
| 40 | 8 | Membership count | `M` |
| 48 | 8 | HDT source data length | Dictionary-and-triples suffix length |
| 56 | 8 | File size | Exact size, including footer |
| 64 | 8 | Directory offset | Absolute, 64-byte aligned |
| 72 | 8 | Directory length | Exactly `section_count * 64 + 4` |
| 80 | 4 | Section count | Number of directory entries |
| 84 | 4 | Identity algorithm | `1`, meaning SHA-256 |
| 88 | 8 | Footer offset | Exactly `file_size - 64` |
| 96 | 4 | Superblock bits | `4096` |
| 100 | 4 | Subblock bits | `512` |
| 104 | 4 | Position chunk shift | `16`, matching the sidecar |
| 108 | 4 | Reserved | Zero |
| 112 | 32 | HDT source digest | SHA-256, §6 |
| 144 | 32 | Graphs sidecar digest | SHA-256, §6 |
| 176 | 76 | Reserved | All zero |
| 252 | 4 | Header CRC32C | CRC32C of bytes `[0, 252)` |

Flags are assertions:

| Bit | Name | Meaning |
|---:|---|---|
| 0 | `HAS_POS_LAYERS` | A complete POS-keyed layer set is present |
| 1 | `HAS_OPS_LAYERS` | A complete OPS-keyed layer set is present |
| 2 | `HAS_MEMBERSHIP_RANKS` | `BitmapG` and its rank directory are present |
| 3 | `HAS_MEMBERSHIP_IDS` | `ArrayG` is present |
| 4..63 | Reserved | Zero |

At least one flag MUST be set; a file asserting nothing is invalid. A reader MUST
reject a file whose sections disagree with its flags.

Bit 3 MUST NOT be set unless bit 2 is: graph identifiers are uninterpretable
without the position boundaries that delimit each position's run. Bit 2 without
bit 3 is a supported configuration.

If `M = 0` — which, since the sidecar is exhaustive, holds exactly when `N = 0` —
bits 2 and 3 MUST be clear and all transpose sections MUST be absent. This
removes the degenerate rank-directory case entirely rather than defining a
zero-length encoding for it.

## 5. Sections

The section directory uses the 64-byte entry layout of
permutation-index-format.md §6 without modification, including the `REQUIRED`
flag and its forward-compatibility rule: a reader MUST reject an unknown section
type with `REQUIRED` set and MUST ignore one with `REQUIRED` clear.

Section type is `(structure << 8) | kind`.

| Structure | Value |
|---|---:|
| POS-keyed layer set | `0x01` |
| OPS-keyed layer set | `0x02` |
| SPO transpose | `0x03` |
| Reserved | `0x04..0xFF` |

Every field of every version 1 section is fixed by the following table. `w`
denotes the `ArrayG` width of §5.2. `Indexed bit length` is the
permutation-index-format.md §6 field at entry offset 48, carried by rank-directory
sections so that a reader can validate a directory against the bitmap it indexes.

| Type | Payload | Length (bytes) | Entry count | Bits/entry | Parameter | Indexed bit length | CRC field |
|---:|---|---|---|---:|---:|---:|---|
| `0x0101` | POS layer directory | `(G + 1) * 96 + 4` | `G + 1` | `0` | `0` | `0` | CRC of payload |
| `0x0102` | POS layer payload region | span, §5.1 | non-empty layer count | `0` | `0` | `0` | `0` |
| `0x0201` | OPS layer directory | `(G + 1) * 96 + 4` | `G + 1` | `0` | `0` | `0` | CRC of payload |
| `0x0202` | OPS layer payload region | span, §5.1 | non-empty layer count | `0` | `0` | `0` | `0` |
| `0x0301` | Transpose `ArrayG` | `ceil(M * w / 8)` | `M` | `w` | `0` | `0` | CRC of payload |
| `0x0302` | Transpose `BitmapG` | `ceil(M / 8)` | `M` | `1` | `0` | `0` | CRC of payload |
| `0x0303` | `BitmapG` superrank | `(ceil(M / 4096) + 1) * 8` | `ceil(M / 4096) + 1` | `64` | `4096` | `M` | CRC of payload |
| `0x0304` | `BitmapG` subrank | `ceil(M / 512) * 2` | `ceil(M / 512)` | `16` | `512` | `M` | CRC of payload |

Entries MUST appear in ascending type order, each type at most once. All version
1 sections set `REQUIRED`. A reader MUST reject any section whose declared
length, entry count, bits per entry, parameter, or indexed bit length departs
from this table. Every reserved field of every entry is zero.

### 5.1 Layer sets

A layer set is **exactly** the structure specified by graphs-sidecar-format.md §6
(layer directory), §7 (dense and sparse chunked encodings, chunk entries,
containers, sparse access hash), and §8 (Elias–Fano encoding), reproduced here by
reference and not restated. Encoding choice is per layer and independent between
sets.

**Framing.** The layer *directory* section payload has the **same framing** as the
sidecar's directory block — `G + 1` consecutive 96-byte entries in the
graphs-sidecar-format.md §6 layout, followed by a little-endian CRC32C over those
entry bytes — so the sidecar's directory reader applies unchanged. The section
entry's own CRC32C covers all `(G + 1) * 96 + 4` payload bytes, embedded CRC
included; the two checksums are redundant by design and a reader MUST verify
both.

**The entries themselves are rebuilt, never copied.** A layer set is a different
encoding of the same memberships in a different position space and a different
file, so a writer MUST recompute every entry field:

- **Primary/secondary offsets** are absolute offsets *into this file*, not the
  sidecar.
- **Minimum position** and **maximum position, exclusive** are the layer's
  extrema **in this set's position space**, which differ from the sidecar's.
- **Encoding** is chosen independently per layer from this set's density, so a
  layer stored as sparse chunks in the sidecar may be dense or Elias–Fano here.
- **Item counts A and B, Parameter, primary CRC32C, and secondary CRC32C** all
  describe this file's payloads.

Only **member count** is necessarily equal to the sidecar's, since it counts
triples and is position-space invariant — which is also why it is the one field
that cannot validate a mapping (§7). Copying the sidecar's directory block would
produce a file whose offsets point into the wrong file and whose extrema describe
the wrong ordering, and whose payload CRCs would fail on first read.

The `Parameter` field of each 96-byte *layer directory entry* keeps its sidecar
meaning: an encoding-specific value, per graphs-sidecar-format.md §6. That entry
has no `Indexed bit length` field; the field of that name belongs to this file's
64-byte *section* directory entries (§5).

**The payload region is a span section, and takes two explicit exceptions to §2.**
Directory entries carry absolute offsets, so layer payloads are located by the
layer directory and never by the region section. Types `0x0102` and `0x0202`
therefore do not *own* bytes the way every other section does; they describe an
extent that contains independently framed structures, and exist so that
validation can bound that extent and confirm it overlaps no other section. Two
inherited rules are suspended for them, and for no other section type:

- **Padding is included in the length.** §2 excludes padding from section
  lengths; a region's length runs from its offset through the last byte of the
  last layer payload, *including* the alignment gaps between payloads. A length
  that excluded interior gaps would bound nothing, which is the section's only
  purpose.
- **The CRC field is zero even when the extent is non-empty.** §2 zeroes a
  payload CRC only for an empty payload. Here every **non-padding** byte inside
  the extent is covered by an **encoding-specific child CRC32C**, and the
  remaining bytes — the interior alignment gaps — are constrained to zero and
  MUST be checked as such (below). Between the two, a span CRC would add no
  coverage while forcing recomputation on any re-layout. Note that the zero check
  is what makes that true: without it the gaps would be unconstrained bytes inside
  a declared extent covered by no checksum at all, and a span CRC *would* buy
  something.

  "Encoding-specific" is load-bearing, and the layer directory's own two CRC
  fields are **not** the whole story — an implementer who verifies only those
  leaves the bulk of the data unchecked. Coverage is nested, and a validator MUST
  descend to it:

  | Bytes | Covered by |
  |---|---|
  | Chunk directory (chunked layers) | Layer directory entry's primary CRC32C |
  | Sparse access hash | Layer directory entry's secondary CRC32C |
  | Chunk container payloads | Payload CRC32C **in each chunk entry** |
  | Elias–Fano 160-byte header | Layer directory entry's primary CRC32C |
  | Elias–Fano lower, superrank, subrank, upper regions | The four CRC32C fields **in the EF header** |

  So for a chunked layer the container payloads — most of its bytes — are covered
  one level below the layer directory, and for an Elias–Fano layer the four
  packed regions are covered by the EF header rather than by the directory entry
  that points at it. All of this is graphs-sidecar-format.md §6, §7.2, §7.3 and
  §8.1 unchanged; it is spelled out here only because the phrase "the layer's
  CRCs" invites checking two fields and stopping.

**Alignment gaps belong to the region, not to any layer.** The bytes between one
layer payload and the next are region bytes for the purpose of the extent above,
and are excluded from every child payload's declared length, which follows
graphs-sidecar-format.md's own framing unchanged. A validator must therefore
check child payloads for mutual non-overlap and for containment within the
region, but MUST NOT require them to tile it.

**Those gap bytes MUST be zero, and strict validation MUST verify it.** §2's
padding rule already requires zero padding, but a validator that enforces it by
sweeping the bytes *between sections* will miss these: they sit **inside** a
declared section extent, not between two. They are the only bytes in the file
that belong to a declared extent and are covered by no checksum, so leaving them
unchecked would be a real gap — a corrupted alignment byte would pass every CRC
in the file. Verifying them is what allows the region's own CRC field to stay
zero without losing coverage.

**The scan is proportional to the padded region size, not to the layer count.**
An earlier draft of this section claimed it costs "a few bytes per layer"; that
was an assumption about writers, not a property of the format. Child payloads are
located by recorded offsets and graphs-sidecar-format.md §3 explicitly forbids
readers from assuming regions are adjacent, so a conforming writer MAY leave
arbitrarily large interior gaps and a validator MUST NOT assume otherwise. Strict
validation therefore scans every byte of every gap, and its cost is bounded by
the region extent rather than by `G`.

That is acceptable because **strict validation is already linear in file size**:
checks 5 and 6 recompute SHA-256 over the entire HDT dictionary-and-triples
suffix and the entire sidecar file. A zero-comparison sweep over this file's own
regions is strictly cheaper than hashing it, so the gap scan changes nothing
about the cost class of the operation it belongs to. It is only the *open* path
that must stay `O(1)`, and the gap scan is not on it.

**Writers SHOULD pack child payloads tightly**, placing each at the smallest
offset at or after its predecessor's end that satisfies its alignment requirement
— at most 63 bytes of gap before a layer's primary structure and at most 7 before
a secondary index or chunk payload. This keeps real files compact and their
validation cost close to `O(G)`. It is a SHOULD rather than a MUST so that a
layer set remains exactly the structure graphs-sidecar-format.md specifies (§5.1)
rather than a narrowed dialect of it; a validator gains nothing from the
guarantee, since it must handle the general case regardless.

**Alignment inside a region is the sidecar's rule, unchanged.** Restating it
exactly, because paraphrase has already gone wrong here: **every** non-empty
layer's primary structure — not merely the first — starts at a 64-byte-aligned
offset, while secondary indexes and chunk payloads start at offsets aligned to at
least 8 bytes (graphs-sidecar-format.md §2). A writer MUST NOT relax the 64-byte
requirement for later layers; doing so produces layer sets that are not the
structure §5.1 claims they are, and breaks any reader that maps them with the
sidecar's own code.

Region offsets need no separate rule as a result. §2 requires a section payload
to begin on a 64-byte boundary, and a region begins at the first non-empty
layer's primary structure, which is 64-byte aligned by the sentence above. The
two requirements agree without a writer having to do anything special.

When a set has no non-empty layer, the region section is present with zero
offset, zero length, and zero entry count.

The only difference from the sidecar is the meaning of a position: layer set
`0x01` stores positions in space 2 (POS) and `0x02` in space 3 (OPS), per §1.2.
The universe remains `[0, N)` in every space. Because the encodings are
identical, one reader implementation serves the sidecar and both layer sets,
parameterised only by which permutation supplies a candidate's position.

### 5.2 The SPO transpose

The transpose stores, for each SPO position in ascending order, the graph IDs
containing it in ascending order. It uses the adjacency encoding of HDT
`BitmapTriples`: `ArrayG` holds the concatenated graph IDs, and `BitmapG` has one
bit per `ArrayG` entry where **a set bit marks the last graph of a position**.

Normative requirements:

1. If present, `ArrayG` has exactly `M` entries; `BitmapG` has exactly `M` bits.
2. Graph IDs are strictly increasing within each position's run.
3. `popcount(BitmapG) = N` — the sidecar is exhaustive, so every position has at
   least one graph and contributes exactly one set bit.
4. The multiset of `(position, graph)` pairs recovered equals the sidecar's
   memberships exactly.

**Width.** Graph IDs take the values `0..G`, so the maximum value is `G` and the
packed width is

```text
w = ceil(log2(G + 1))
```

which is `0` when `G = 0`. Using `ceil(log2(G + 2))` would waste one bit per
membership at every `G` of the form `2^k - 1`.

**Derived operations.** With `select1`/`rank1` over `BitmapG` as specified in
permutation-index-format.md §7.2, define `offset(p)` as the **`ArrayG` index at
which position `p`'s run begins** — equivalently, the number of memberships held
by all positions before `p`:

```text
offset(p)         = p == 0 ? 0 : select1(p - 1) + 1
graphs_of(p)      = ArrayG[ offset(p) .. select1(p) + 1 )
memberships(a, b) = offset(b) - offset(a)
```

The name matters: `offset(p)` is a *start*, not an end. Position `p`'s run is
`[offset(p), select1(p) + 1)`, whose exclusive end is `select1(p) + 1` and is not
`offset(p)`. Describing it as an end inverts the meaning of both formulas.

`memberships(a, b)` is the number of memberships held by positions in `[a, b)`,
which is the quad-view cardinality of an SPO-contiguous pattern. It is a
difference of **adjacency offsets**, not of ranks: `rank1(offset(p))` returns `p`,
the number of *positions*, which is not the quantity wanted. `offset(0) = 0` by
definition, so `a = 0` needs no special case and `select1` is never called with a
negative argument.

**Cost.** `memberships` is two `select1` calls, each `O(log(M / 4096))` — a binary
search over the superrank samples plus a bounded scan, per
permutation-index-format.md §7.2. `graphs_of` is two `select1` calls plus its own
output size. Neither is `O(1)`. Against the sidecar's `O(G)` rank operations (§0)
this is a trade between two small quantities, not a change of complexity class,
and §9 sizes it accordingly.

`BitmapG`'s rank directory is the two-level superrank/subrank structure of
permutation-index-format.md §7.2, with identical widths and semantics.

### 5.3 Why the transpose is SPO-keyed only

A reader enumerating in POS or OPS order holds a position in that space, so this
structure does not serve the graph column for index-side patterns. Those fall
back to `G + 1` probes of the corresponding layer set — the pre-existing cost,
acceptable at moderate graph counts.

Transposes for spaces 2 and 3 are a natural extension — structure values `0x04`
and `0x05` are reserved — but version 1 omits them because the dominant
quad-view consumers enumerate in SPO order and each additional transpose costs as
much again. Add them when measurement, not symmetry, justifies it.

## 6. Binding to the parents

This file has two parents and binds to both.

**The HDT.** The identity input begins at the first byte of the HDT Dictionary
Control Information and extends through end of file, exactly as in
graphs-sidecar-format.md §10. The header stores its byte length and SHA-256, plus
the decoded triple count `N`.

**The graphs sidecar.** The identity input is the **entire sidecar file**, byte 0
through end of file, and the header stores its SHA-256. The whole file is used
because, unlike an HDT, a sidecar has no metadata region that may be rewritten
while the data stays fixed; any change to it invalidates this index.

**The two parents must also be bound to each other.** Verifying each digest
independently proves only that *these* two files are the ones this index was
built from — not that the sidecar's memberships describe *this* HDT. A sidecar
built against a different HDT with a coincidentally equal triple count would
satisfy both digests while every position it stores referred to another
ordering, and a build script that picked up the wrong sidecar for a version
would produce a silently wrong index rather than a rejected one. Versioned
bundles make that coincidence entirely reachable: two releases of one dataset
can easily share a triple count.

The sidecar carries its own HDT binding (graphs-sidecar-format.md §10), recorded
over the identical byte range this file uses. The chain is therefore established
by comparing stored header fields, and the expensive part of identity validation
is not the comparing but the **recomputation** of a digest from the bytes it
covers. The two are separated accordingly.

**Required on every open**, including the lowest-latency path. All of these are
header-field comparisons or values already parsed, so together they are `O(1)`
and there is no configuration in which skipping them is a defensible trade:

1. This file's recorded HDT digest and suffix length are **equal as stored
   fields** to the sidecar's recorded HDT digest and suffix length. This is the
   cross-parent check: it is what proves the sidecar describes the same HDT this
   index does, and it costs two field comparisons.
2. This file's recorded HDT suffix length and `N` match the HDT actually opened.
3. `N`, `G`, and `M` in this header match the sidecar header.
4. The sidecar's recorded triple count matches the HDT actually opened.

**Required only for strict validation**, since each rereads and rehashes a file:

5. Recomputed SHA-256 over the HDT's dictionary-and-triples suffix equals the
   digest recorded here and in the sidecar.
6. Recomputed SHA-256 over the entire sidecar file equals the digest recorded
   here.

This is the same division the sidecar draws in its own §10 — a low-latency open
compares lengths and counts, and only a trust-boundary crossing recomputes
SHA-256 — extended with the cross-parent field comparison, which the sidecar has
no occasion to specify because it has only one parent.

Check 1 is the one this format exists to get right, and the one a reader is most
likely to omit, because the sidecar looks already validated when it arrives from
a loader that checked it against *some* HDT.

**What a sidecar loader can and cannot be delegated.** A conforming
graphs-sidecar loader performs that format's §11 validation: its HDT binding and
its internal CRCs. A reader obtaining the sidecar through one MAY therefore rely
on it for check 4 and for the sidecar's own identity validation.

It MUST NOT rely on such a loader for check 6. **The whole-sidecar SHA-256 is a
digest this format defines for its own binding; the sidecar format never computes
it**, having no use for a digest of itself, so a generic loader does not produce
the value the comparison needs. Delegating check 6 silently skips it. A reader
MAY treat check 6 as satisfied by a loader only if that loader explicitly hashed
the entire sidecar file and either performed the comparison against this index's
recorded digest or returned the digest for the reader to compare — a capability a
loader has to have been built for, not one implied by conformance.

Nor may check 1 be delegated, for the reason above: only the reader holds both
recorded digests, and only check 1 establishes that the loader's HDT and this
index's HDT are the same file.

Note that check 6 binds *bytes*, not logical content. The sidecar format states
that encoding choice is not part of logical file identity, so re-encoding a
sidecar without changing a single membership produces a different digest and
invalidates this index. That is deliberate: the index is derived and cheap to
rebuild, so failing closed on a benign re-encoding costs less than defining a
canonical logical digest to avoid it.

## 7. Conformance and validation

A version 1 reader MUST accept version exactly 1.0 and MUST reject unknown flags,
nonzero reserved fields, invalid ranges, overlaps, duplicate or out-of-order
section types, unknown `REQUIRED` section types, sections departing from the §5
table, a file whose parents are absent, and arithmetic overflow.

Strict validation MUST additionally verify:

1. The complete identity chain of §6 — checks 1–4, which a conforming reader has
   already performed on open, plus the recomputations of checks 5 and 6, plus
   graphs-sidecar-format.md §11 identity validation of the sidecar against the
   same HDT. Note that §6 check 1, the stored-digest comparison between the two
   parents, is not optional at any validation level: without it an index pairing
   unrelated parents whose counts happen to agree is accepted.
2. Header CRC32C, footer CRC32C, the footer's copied fields against the header,
   the section directory CRC32C, every section payload CRC32C, every layer
   directory's embedded CRC32C, and **every encoding-specific child CRC32C
   nested below a layer directory entry** — chunk-entry payload CRCs and
   Elias–Fano header region CRCs included, per the coverage table in §5.1.
   Verifying only the layer directory entries' primary and secondary CRCs leaves
   most of a layer's bytes unchecked.
3. Section alignment, non-overlap, ascending order, the §5 sizes, and that every
   padding byte is zero — **including the alignment gaps interior to a layer
   payload region**, which lie inside a declared extent rather than between
   sections and are the only bytes in the file no CRC covers (§5.1). Interior
   gaps may be arbitrarily large, so this sweep is proportional to the region
   extent; it is nonetheless cheaper than checks 5 and 6, which already hash both
   parents in full. A layer
   payload region overlaps no other *section*, but properly contains its own
   layer payloads, which are not sections; those are checked for mutual
   non-overlap and containment, not for tiling the region (§5.1).
4. For each present layer set, everything graphs-sidecar-format.md §11 requires
   of a layer set: order, alignment, non-overlap, encoding-specific sizes, rank
   recurrences, cardinalities, strictly increasing positions, and zero tail bits.
5. **Count agreement:** for every graph `g`, `count(g)` is equal in the sidecar
   and in every present layer set, and the layer counts sum to `M` in each.
6. **Membership agreement:** each present layer set decodes to the same
   `(graph, triple)` relation as the sidecar, after mapping positions through the
   respective permutation. This MAY use an external sort.
7. For the transpose: requirements 1–4 of §5.2 as far as the present sections
   allow, and the rank directory against a recomputed cumulative popcount. With
   `BitmapG` alone, requirement 4 is checked as per-position membership *counts*
   against the sidecar — the set-bit pattern must reproduce every position's
   membership count — which is the strongest statement derivable without graph
   identifiers.

**What check 5 cannot do.** `count(g)` is the number of *triples* in graph `g`,
which is identical in every position space. Count agreement therefore holds even
if a layer set was built with the wrong permutation's positions, including the
case where POS and OPS sets are interchanged. It detects a wrong or truncated
sidecar, a corrupted directory, and a mismatched graph count — nothing about the
positional mapping. **Only check 6 validates the mapping**, and neither a builder
nor a reader may treat check 5 as standing in for it.

There is no cheap complete substitute, because any check on positions must
compute those positions. A builder wanting sub-linear assurance SHOULD verify a
random sample of `k` memberships through the mapping, which costs `k` permutation
descents and catches a systematic error with probability approaching 1 in `k`.

Because this file is derived, a reader that fails validation MAY discard it and
fall back to the sidecar alone. A reader MUST NOT answer from a file that failed
validation.

## 8. Footer

The footer is exactly 64 bytes and starts at `footer_offset`.

| Offset | Size | Field | Version 1 value or meaning |
|---:|---:|---|---|
| 0 | 8 | Magic | ASCII `$HDTGXND` |
| 8 | 2 | Major version | `1` |
| 10 | 2 | Minor version | `0` |
| 12 | 4 | Footer size | `64` |
| 16 | 8 | File size | Exact file size |
| 24 | 8 | Header offset | `0` |
| 32 | 8 | Directory offset | Copy of header field |
| 40 | 8 | Directory length | Copy of header field |
| 48 | 8 | Reserved | Zero |
| 56 | 4 | Header CRC32C copy | Copy of header checksum |
| 60 | 4 | Footer CRC32C | CRC32C of bytes `[0, 60)` |

This mirrors graphs-sidecar-format.md §9 field for field apart from the magic, so
one footer reader serves both artifacts.

## 9. Size and build policy (non-normative)

The transpose halves size very differently. `BitmapG` plus its rank directory is
`M` bits plus about 4.7%, **independent of `G`**. `ArrayG` is `M * w` bits,
growing with the graph count. For `N = 10^8` and `M = 1.2 * 10^8`:

| `G` | `w` | `BitmapG` + directory | `ArrayG` | both |
|---:|---:|---:|---:|---:|
| 5 | 3 | 16 MB | 45 MB | 61 MB |
| 50 | 6 | 16 MB | 90 MB | 106 MB |
| 1000 | 10 | 16 MB | 150 MB | 166 MB |

`ArrayG` is 74–90% of the pair across that range, which is why the halves are
separately optional.

Each layer set costs approximately what the sidecar's own layers cost, since it
holds the same memberships under a different ordering; density differences
between spaces may shift the chunked-versus-Elias–Fano choice per layer, so the
totals are similar but not identical.

Build policy follows from §1.1's distinction between removing an unbounded cost
and removing a bounded one:

- **Layer sets SHOULD be built whenever a reader serves scoped index-side
  patterns.** They are the only structures here that change a complexity class.
  Their decisive property is enumeration rather than counting: probe-based
  scoping fills a page at `|A ∩ B| / min(|A|, |B|)`, which has no lower bound, so
  where that ratio is small a page costs an unbounded number of candidate probes,
  while a natively keyed layer yields a result per step (§0). The `min` is what
  bounds the fallback in the easy cases and what makes the hard case specific:
  many mid-sized graphs intersecting mid-sized predicates thinly. A deployment
  whose graphs are few and coarse — one dominant graph plus small ones — will
  find the fallback adequate, since a large graph intersects most patterns
  heavily and a small one is cheap to scan outright.
- The two sets are independent, and a reader that never scopes object-rooted
  patterns MAY ship `0x01` alone. Building both is nonetheless the sane default:
  the code is identical, the costs are symmetric, and the case OPS covers — a
  scoped pattern on a hub object such as a common class IRI — is exactly where the
  probe fallback is worst.
- **`BitmapG` SHOULD be built only when `G` is large.** It replaces `O(G)` rank
  operations on the sidecar with two `select1` calls. At a few dozen graphs the
  sidecar's own summation costs on the order of a hundred rank operations —
  microseconds — and 16 MB is a poor trade for it. At `G` in the tens of
  thousands the summation becomes the dominant cost of a count and the trade
  reverses.
- **`ArrayG` SHOULD be built only when `G` is large or the graph column is known
  to be hot.** The probe loop it replaces is amortised by locality during
  sequential enumeration: a page walks positions in order, so the same `G + 1`
  chunk payloads stay resident and the per-row cost falls to a few bit tests. It
  earns its bytes when the layer working set stops fitting in cache, or when
  access is scattered and locality no longer amortises anything.

So the common configuration for a bundle whose graphs are components or other
coarse partitions is **both layer sets and no transpose at all**. The transpose is
a large-graph-count structure; a bundle with dozens of graphs should not carry it.

## 10. Construction guidance (non-normative)

Every structure here derives from the sidecar's memberships, and hdtc already
produces them in position order: `GraphsSidecarReader::validate_strict` externally
sorts all memberships into `(position, graph)` order and can emit that stream for
a caller that needs the transposition.

**The transpose** is that stream written directly: `ArrayG` receives each graph ID
in order, `BitmapG` receives a set bit at each position boundary, and the rank
directory accumulates during the write. No sort beyond the existing sweep.

**Each layer set** needs memberships keyed by permuted position, and this is not a
single merge. Decoding a permutation yields triples in *permutation* order — that
is, ascending permuted position — whereas the membership stream is in ascending
*SPO* position, so the two are not co-sorted and cannot be merge-joined directly.
A correct build is:

1. Decode the target permutation in order, emitting `(s, p, o, permuted_position)`.
2. Externally sort that stream into SPO order, yielding
   `(spo_position, permuted_position)` pairs in ascending `spo_position` — the
   position mapping. Sorting by `(s, p, o)` is sorting by SPO position, so the
   SPO permutation need not be decoded to number them.
3. Merge that mapping against the position-ordered membership stream, both now
   ascending in `spo_position`, producing `(graph, permuted_position)`.
4. Externally sort by `(graph, permuted_position)` to get the graph-major stream
   the layer finalizer consumes.

Two external sorts of `N` and `M` records respectively, plus two linear passes.

The corresponding `SPO_POSITION_MAP` section (permutation-index-format.md §13.1)
helps less than it appears to. It maps `permuted_position -> spo_position`, which
is the direction step 1 produces, whereas the merge in step 3 needs a lookup
**by** `spo_position` — the inverse. Reading the map in order therefore yields
exactly the pairs of step 1, still in permuted order, and step 2's sort remains.

A builder holding the map can skip step 2 only by materialising the inverse
explicitly: allocate an `N`-entry array of `ceil(log2(N))`-bit slots and, for each
`permuted_position`, write it at index `spo_position`. That is `N` random writes,
which beats an external sort when the array fits in memory or on a
memory-mapped scratch file — roughly `N * ceil(log2(N)) / 8` bytes, about 340 MB
at `N = 10^8` — and loses badly when it does not. The choice is the builder's, and
neither route is required.

Step 1 also becomes unnecessary when the map is present, since the map *is* the
decoded permutation's position correspondence; only the inversion question above
remains.

**The finalizer** for each layer set is the sidecar's own: spool one layer's
sorted positions, gather density statistics, choose chunked or Elias–Fano,
encode, reuse the scratch file. No new encoder is required.

**Decorating the permutation sort** is a faster construction path. Wherever a
producer holds each unique SPO triple adjacent to its graph IDs, it can carry
those IDs through each POS/OPS permutation sort, group the sorted output by
triple, and append the current permuted position directly to one spool per graph
when the graph dictionary fits the implementation's bounded direct-spool limit.
Positions in every spool are then intrinsically sorted. For larger graph
dictionaries, the producer may feed the grouped memberships to a bounded
graph-major external sort instead. Both forms remove the inverse-position sort
described above. If a permutation index is also being emitted, its encoder
consumes the same grouped streams; no additional permutation sort is necessary.

Integrated HDT creation has that adjacency for free. A producer working from a
finished HDT can reconstruct it: the memberships of a §4 layer set are already
sorted by position within each layer, so transposing the sidecar is a k-way
merge over its layers rather than a sort of every membership, and an SPO scan of
the HDT is already in position order, so the two join sequentially.

Decorating repeats a triple's key once per membership, so its advantage narrows
as memberships per triple rise; past roughly sixteen, sorting each triple once
and mapping permuted positions onto the memberships moves fewer bytes. Producers
are free to choose either, or to route between them — the encoded bytes do not
depend on which is used.

A builder MUST NOT treat §7 check 5 as verification of a completed layer set
(§7). Verifying the mapping means check 6 or the sampling procedure described
there; counts alone would pass a build whose POS and OPS sets were swapped.
