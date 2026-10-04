# RDF 1.2 triple terms beside a standard HDT, version 1

Status: normative format specification for hdtc.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY**
are to be interpreted as described by RFC 2119.

## 0. Purpose

RDF 1.2 adds a fourth kind of term, the *triple term* `<<( s p o )>>`, which may
appear in object position — most often as the object of `rdf:reifies`, where a
reifier names an occurrence of a statement and carries metadata about it. A
standard HDT dictionary holds IRIs, blank nodes and literals and nothing else,
and a reader that met anything else would reject the file.

This specification keeps `data.hdt` a standard HDT by naming every distinct
triple term with an IRI in a reserved namespace (§2) and storing that IRI where
the term occurs. What each IRI denotes — its *proposition*, the subject,
predicate and object of the triple term — is recorded beside the HDT:

- an **asserted** triple term, whose triple is itself in `data.hdt`, is recorded
  only by reference, because its proposition is already stored;
- an **unasserted** one is stored as a triple of a second, standard HDT, the
  *propositions HDT* (§3), which holds nothing else.

The *triple-term index* (§4–§8) ties the two together. For each triple-term IRI
it gives the proposition; for each triple of either HDT, in each of SPO, POS and
OPS order, it says whether that triple is a proposition and which IRI names it.
The second direction is what lets a reader match patterns *inside* triple terms
(`?e rdf:reifies <<( :s ?p ?o )>>`) with the same range arithmetic it uses for
ordinary patterns (§10).

The split by assertedness is deliberate. A dataset that annotates every triple —
provenance on every edge is common — would otherwise store every triple twice,
with a second dictionary; here it stores one layer bit and one map entry per
triple instead. A dataset of unasserted claims stores those claims once, in the
propositions HDT.

## 1. Artifacts

For an HDT named `data.hdt`:

| File | Content | Kind |
|---|---|---|
| `data.hdt` | Standard HDT; triple terms appear as IRIs (§2) | — |
| `data.hdt.perm` | Permutation index (permutation-index-format.md) | index |
| `data.hdt.props.hdt` | Propositions HDT (§3), present iff any triple term is unasserted | data |
| `data.hdt.props.hdt.perm` | Its permutation index | index |
| `data.hdt.tt` | Triple-term index (§4–§8) | index |

An HDT whose dictionary contains an IRI in the triple-term namespace MUST be
accompanied by `data.hdt.tt` and `data.hdt.perm`, and by the propositions HDT and
its permutation index when the index's `HAS_PROPOSITIONS` flag is set. A reader
that finds triple-term IRIs without an index MUST refuse to render them rather
than present them as ordinary IRIs.

The propositions HDT is **data**: an unasserted triple term's proposition is
recorded nowhere else, and the IRI that names it is a hash, which cannot be
inverted. The triple-term index is an **index**: every byte of it can be
recomputed from `data.hdt` and the propositions HDT, by minting the IRI of each
of their triples and looking it up.

### 1.1 Definitions

- A triple term's **proposition** is its subject, predicate and object. A
  nested triple term in object position is a component like any other.
- A triple term is **asserted** when its proposition is a triple of `data.hdt` —
  in any graph, since `data.hdt` is the union of a dataset's graphs.
- A **run** is the half-open range of object identifiers that the triple-term
  IRIs occupy in one HDT's dictionary. Triple terms occur only in object
  position, so the IRIs are objects-only terms; they share the namespace prefix,
  so the dictionary's byte order makes them contiguous.

## 2. Triple-term IRIs (convention 1)

### 2.1 Canonical form

The **canonical form** of a triple term is its RDF 1.2 canonical N-Triples
serialization: `<<( ` subject ` ` predicate ` ` object ` )>>`, components
separated by exactly one space, nested triple terms written inline, and

- IRIs as `<iri>`, a character an N-Triples IRIREF excludes (U+0000–U+0020
  and `` < > " { } | ^ ` \ ``) written as `\uXXXX` with uppercase hex — lenient
  ingest can store such an IRI, and written raw it could end early and make two
  terms alike — and a literal's datatype IRI the same way;
- literals as `"value"` with the value escaped as canonical N-Triples requires —
  `\b \t \n \f \r \" \\` as ECHAR; U+0000–U+0007, U+000B, U+000E–U+001F, U+007F,
  U+FFFE and U+FFFF as `\uXXXX` with uppercase hex; everything else unescaped —
  followed by `@` and the lowercase language tag (and `--ltr` or `--rtl` for a
  directional language-tagged string), or `^^<datatype>`, with `xsd:string`
  omitted;
- blank nodes as placeholders, described next.

The form is computed from dictionary strings, and is a function of them that no
two strings share. The predicate is always written as an IRI. The subject's and
object's kind is read from the string the way HDT distinguishes terms: `_:`
opens a blank node, `"` a literal, anything else is an IRI. An IRI that lenient
ingest stored spelled like a blank node (`<_:x>`) or a literal (`<"a">`) is
held in the dictionary as one, so it is named as one.

A string is written as a literal only when it is in the form hdtc's literal
encoding writes and can be split back into its parts — `"value"`,
`"value"@tag` with the tag lowercased, or `"value"^^<iri>` with `xsd:string`
dropped — and otherwise as an IRI, escaped as above: `<\u0022a\u0022@EN>`.
Written as a literal, such a string could end early, lose a capital, or split
in two ways, and spell like another term. It arises from an IRI spelled like a
literal (`<"x>`, `<"a"@EN>`), or from a language tag lenient ingest kept that
is not a well-formed BCP 47 tag (`xml:lang="en_US"`, stored as `"v"@en_us`).
Every literal with a well-formed tag or datatype is in the encoded form, so this
changes no name minted from valid input.

### 2.2 The two forms

A **ground** triple term — no blank node at any depth — is named

```text
urn:x-kgf:tt:g:{h}
```

where `{h}` is the first 16 bytes of the SHA-256 of the UTF-8 canonical form, as
22 characters of unpadded base64url (RFC 4648 §5: `A–Z a–z 0–9 - _`). The
encoding is chosen for size: the hash is stored once per distinct triple term,
and base64url carries its 128 bits in 22 characters where hex takes 32. Equal
terms receive equal IRIs in every build; that is what lets separately built
HDTs merge their triple terms by string equality, and what makes a ground
triple term's IRI a federation-wide identifier.

A triple term **with blank nodes** is named

```text
urn:x-kgf:tt:b:{k}:{label₁}:{label₂}…
```

The canonical form is first rewritten into a *skeleton* by replacing each blank
node with `_:b1`, `_:b2`, … in order of first appearance, reading depth-first
through nested terms; a node appearing twice receives one placeholder. `{k}` is
hashed from the skeleton as `{h}` is from a ground term. The labels follow, one
per distinct blank node in placeholder order, each being the node's dictionary
label without `_:`, percent-encoded: bytes outside `A–Z a–z 0–9 - . _ ~` become
`%XX` with uppercase hex.

A content hash cannot name a term whose components include blank nodes: when
separately built HDTs are merged, their blank nodes are renamed apart, and a
string made by hashing cannot be renamed. The labels are therefore kept where
the rename can reach them. A merge that renames blank node `_:x` to `_:{prefix}x`
MUST rename every label of every `…:tt:b:` IRI the same way, inserting the
percent-encoded prefix at the start of each label. Renaming a term's blank nodes
and then minting yields the same string as minting and then renaming the
IRI's labels, so the two stay consistent without re-hashing; and because the
same prefix is inserted before every label, and `:` never appears inside an
encoded label, the rename preserves byte order among such IRIs, which a k-way
dictionary merge requires of each input.

Such an IRI identifies its term only as far as its blank nodes are identified:
uniquely within one HDT, with no claim across builds.

### 2.3 The reserved namespace

The namespace `urn:x-kgf:tt:` is reserved whole. A writer MUST refuse source RDF
that uses an IRI in it in any position — subject, predicate, object, graph name,
datatype, or inside a triple term at any depth — rather than admit it as data or
escape it: an admitted name could forge a triple term, or collide with one a
later version mints. An HDT input to a merge is not source RDF; its triple-term
IRIs are admitted together with the index that defines them.

### 2.4 Nesting

A writer MUST NOT produce a triple term nested more than 64 levels deep,
counting the outermost as the first, and a reader MAY refuse one. hdtc skips a
source statement that nests deeper, as it skips a statement it cannot parse:
nested terms are built recursively, and an unbounded depth — a few hundred
kilobytes of input — would exhaust the stack of any reader that follows it.

### 2.5 Directional language-tagged strings

RDF 1.2 also adds a base direction to language-tagged strings. The dictionary
stores one as N-Triples 1.2 writes it, the direction following the tag:
`"…"@ar--rtl`. This is a literal like any other and needs nothing in this
specification beyond §2.1's canonical form.

## 3. The propositions HDT

`data.hdt.props.hdt` is a standard HDT whose triples are exactly the
propositions of the unasserted triple terms, each once. It MUST NOT contain any
other triple, and MUST be absent when no triple term is unasserted.

- A component that is itself a triple term appears as its IRI, so the
  propositions HDT has a run of its own when unasserted terms nest.
- A blank node keeps the label it has in `data.hdt`: the same node in both
  files is the same node, and a reader identifies it across them by label.

## 4. Index file layout

The index follows the permutation index's container exactly
(permutation-index-format.md §2, §3 and §8): little-endian fixed-width fields;
a 256-byte header; a directory of 64-byte entries followed by its CRC32C; bare
packed payloads at 64-byte aligned offsets, tightly packed in ascending section
type order, zero padding excluded from lengths and checksums; and a 64-byte
footer. The footer is that format's, with magic `$HDTTEND`. A writer MUST
produce the canonical tight packing, so the bytes are a function of the
contents alone.

## 5. Header

| Offset | Size | Field | Version 1 value or meaning |
|---:|---:|---|---|
| 0 | 8 | Magic | ASCII `$HDTTERM` |
| 8 | 2 | Major version | `1` |
| 10 | 2 | Minor version | `0` |
| 12 | 4 | Header size | `256` |
| 16 | 8 | Flags | Bit 0 `HAS_PROPOSITIONS`; others zero |
| 24 | 4 | IRI convention | `1` (§2) |
| 28 | 4 | Identity algorithm | `1`, meaning SHA-256 |
| 32 | 4 | Superblock bits | `4096` |
| 36 | 4 | Subblock bits | `512` |
| 40 | 4 | Section count | Directory entries |
| 44 | 4 | Reserved | Zero |
| 48 | 8 | Sidecar size | Exact file size, including footer |
| 56 | 8 | Directory offset | Absolute, 64-byte aligned |
| 64 | 8 | Directory length | `section_count * 64 + 4` |
| 72 | 8 | Footer offset | `sidecar_size - 64` |
| 80 | 8 | Data triples | `data.hdt` triple count |
| 88 | 8 | Data objects | `data.hdt` object count (shared + objects-only) |
| 96 | 8 | Data source length | Length of `data.hdt`'s identity suffix (§9) |
| 104 | 8 | Data run start | First object identifier of the run; `0` if empty |
| 112 | 8 | Data run length | Number of triple-term IRIs in `data.hdt` |
| 120 | 8 | Props triples | Propositions HDT triple count, or `0` |
| 128 | 8 | Props objects | Its object count, or `0` |
| 136 | 8 | Props source length | Its identity suffix length, or `0` |
| 144 | 8 | Props run start | As for data; `0` if empty |
| 152 | 8 | Props run length | As for data |
| 160 | 32 | Data digest | SHA-256 (§9) |
| 192 | 32 | Props digest | SHA-256 (§9), or zero |
| 224 | 28 | Reserved | Zero |
| 252 | 4 | Header CRC32C | CRC32C of bytes `[0, 252)` |

## 6. Sections

A section type is `(component << 8) | kind`. Every section's `REQUIRED` flag is
set. A reader MUST reject an unknown section type.

| Component | Value | Meaning |
|---|---:|---|
| `DATA_RUN` | `0x01` | The run of triple-term IRIs in `data.hdt` |
| `PROPS_RUN` | `0x02` | The run in the propositions HDT |
| `DATA_SPO`, `DATA_POS`, `DATA_OPS` | `0x11`–`0x13` | Layers over `data.hdt` |
| `PROPS_SPO`, `PROPS_POS`, `PROPS_OPS` | `0x21`–`0x23` | Layers over the propositions HDT |

| Kind | Value | Payload | In |
|---|---:|---|---|
| `TARGET_SUBJECT` | `0x01` | Packed: `(subject << 1) \| unasserted` | runs |
| `TARGET_PREDICATE` | `0x02` | Packed: predicate identifier | runs |
| `TARGET_OBJECT` | `0x03` | Packed: object identifier | runs |
| `LAYER` | `0x04` | Bitmap | dense layers (§8.1) |
| `LAYER_SUPERRANK` | `0x05` | Packed `u64` (permutation-index-format.md §7.2) | layers |
| `LAYER_SUBRANK` | `0x06` | Packed `u16` (ibid.) | layers |
| `REVERSE` | `0x07` | Packed: `data.hdt` object identifier | layers |
| `LAYER_HIGH` | `0x08` | Bitmap: Elias–Fano high bits | sparse layers (§8.1) |
| `LAYER_LOW` | `0x09` | Packed: Elias–Fano low bits | sparse layers (§8.1) |

Both run components are always present, with zero-length payloads when their
run is empty. The three `DATA_*` layer components are always present. The
three `PROPS_*` components are present iff `HAS_PROPOSITIONS` is set.

Packed widths are fixed by the header's identifier spaces, not by the values
present (§7, §8), so an empty run still declares them; the rank directories'
widths are fixed at 64 and 16, with `Parameter` and `Indexed bit length` as in
the permutation index.

Each layer component holds `LAYER_SUPERRANK`, `LAYER_SUBRANK` and `REVERSE`,
and either `LAYER` (dense) or both `LAYER_HIGH` and `LAYER_LOW` (sparse) — the
encoding §8.1 chooses, and no other. The rank directory indexes whichever
bitmap is present.

## 7. Runs

A run's three arrays have one entry per triple-term IRI in that HDT's run, in
run order. Entry `i` describes the IRI with object identifier `run_start + i`:
its proposition's subject, predicate and object identifiers, in **the HDT that
holds the proposition** — `data.hdt` if the triple term is asserted, the
propositions HDT if not — with the low bit of `TARGET_SUBJECT` set for the
latter.

The subject array's width is one more than the width of the larger of the two
HDTs' subject counts; the predicate and object arrays' widths are those of the
larger predicate and object counts. Both runs use the same widths.

Storing identifiers rather than a triple position is a choice for decoding: an
IRI's proposition is three array reads and three dictionary lookups, with no
rank or select over the HDT's bitmaps, and no permutation index involved.

Requirements:

1. Each run is exactly the set of the dictionary's IRIs under `urn:x-kgf:tt:`:
   `run_start` is the first such object identifier and `run_length` their count.
   No such IRI is a subject.
2. Every IRI is the name (§2) of the proposition its entry gives, nested terms
   decoded recursively through the runs.
3. A target flagged asserted is a triple of `data.hdt`; one flagged unasserted is
   a triple of the propositions HDT, and its triple is not in `data.hdt`.

## 8. Layers

A layer covers one HDT's triples in one order: SPO as stored in the HDT, POS and
OPS as stored in its permutation index. It has, logically, one bit per triple
position — §8.1 says how it is stored — and
**bit `z` is set iff the triple at position `z` is a proposition whose
triple-term IRI is an object in `data.hdt`'s dictionary.**

The `REVERSE` array has one entry per set bit, in position order: entry `r` is
the `data.hdt` object identifier of the IRI naming the triple at the `r`-th set
bit. Its width is that of `data.hdt`'s object count.

The condition is on `data.hdt`'s dictionary — not merely on being a
proposition — because a layer exists to serve patterns over `data.hdt`
(`? rdf:reifies <<( … )>>`), and a proposition whose IRI occurs only nested
inside another proposition is the object of no `data.hdt` triple. Marking it
would make a range contain positions that resolve to nothing, and enumeration
would lose its bound.

### 8.1 Dense and sparse layers

A layer over `N` positions with `M` of them set is stored in one of two
encodings, chosen by `N` and `M` alone:

- **Dense:** `LAYER` is the bitmap of `N` bits itself.
- **Sparse:** the Elias–Fano encoding of the set positions `x_0 < … < x_{M-1}`,
  with `l = floor(log2(floor(N / max(M, 1))))` low bits. `LAYER_LOW` holds
  `x_i mod 2^l` for each `i`, packed `l` bits wide, in rank order. `LAYER_HIGH`
  is a bitmap of `M + ((N - 1) >> l) + 1` bits in which bit `(x_i >> l) + i` is
  set for each `i` and every other bit is clear: positions fall into buckets of
  `2^l`, and each bucket's set bits are followed by one clear bit closing it.

The **sparse** encoding is used exactly when `l ≥ 1` and its payload is
smaller in bytes than the dense one's, where a bitmap of `b` bits with its rank
directory takes `ceil(b/8) + 8 * super_count(b) + 2 * sub_count(b)` bytes
(permutation-index-format.md §7.2) and the sparse layer adds `ceil(M * l / 8)`
for its low bits. Ties are dense. The choice is a function of the counts, so a
layer's bytes remain a function of its contents, and a reader or validator can
recompute it from `N` and `REVERSE`'s entry count.

Sparse is the smaller below about a quarter of the positions set — at 10%, about
0.5 bits per position against 1.05 — and much the smaller when few triples are
reified: at 0.1%, about twelve bits per mark rather than a thousand. Both
encodings answer the same queries, and the rest of the format does not depend
on which is used:

- `rank1(z)`, the set positions before `z`: dense, as for any ranked bitmap;
  sparse, the marks before `z`'s bucket `h = z >> l` are
  `select0(h - 1) - (h - 1)` in the high bits (`0` when `h = 0`), the marks in
  it end at `select0(h) - h`, and among those the low bits, ascending, are
  searched for `z mod 2^l`.
- `select1(r)`, the `r`-th set position: sparse,
  `((select1_high(r) - r) << l) | LOW[r]`.
- Enumeration from a rank reads the high bits forward from `select1_high(r)`,
  skipping one clear bit per bucket passed, and `LOW` and `REVERSE` from `r`;
  where a range's buckets far outnumber its marks, each mark is selected
  instead, as for a dense layer's sparse ranges.

## 9. Binding

Each digest is the SHA-256 of the bound HDT's identity suffix, from the first
byte of its Dictionary Control Information to end of file, exactly as the
permutation index binds (permutation-index-format.md §9). A low-latency open MAY
compare only triple counts, object counts and suffix lengths; strict validation
MUST recompute both digests.

## 10. Reading (non-normative)

**Decoding** an IRI found in `data.hdt` at object identifier `o`: if `o` is in
the data run, read entry `o - run_start` of the data run; if its unasserted bit
is clear, the three identifiers are `data.hdt`'s, otherwise the propositions
HDT's. An object that is itself in that HDT's run is a nested triple term,
decoded the same way through that HDT's run.

**Matching inside a triple term.** For an inner pattern `<<( s p o )>>` with
any positions bound, each HDT is consulted separately and the results are
concatenated, since a triple term is asserted or not, never both:

| Inner pattern | Order | Range |
|---|---|---|
| `s ? ?`, `s p ?`, `s p o` | SPO | the subject group, narrowed as for ordinary patterns |
| `? p ?`, `? p o` | POS | the predicate group, narrowed |
| `? ? o` | OPS | the object group |
| `? ? ?` | any | the whole layer |
| `s ? o` | SPO | the subject group, probing for `o` per predicate |

Within a range `[a, b)` the matching triple terms are the layer's set bits:
`rank1(b) - rank1(a)` counts them exactly, and `REVERSE[rank1(a) …]` lists the
IRIs naming them in range order, so enumeration costs `O(1)` per triple term
and resumes from a rank. Each IRI then resolves through `data.hdt`'s OPS group
to the triples that use it — for `rdf:reifies`, its reifiers.

Counting the *triples* that use the matched terms is not a range operation: it
is the sum of each term's group size. Readers that need it exactly pay per
matched term.

When every inner position is a wildcard, no layer is needed: the triples whose
object is any triple term are exactly those whose object is in the data run, and
because the run is a contiguous range of object identifiers, they are one
contiguous range of OPS.

A pattern whose *outer* subject is bound is better answered from the other
side: the subject's SPO group is small, and each triple-term object in it is
decoded and tested.

**Text search.** A text index over `data.hdt` also covers the literals of the
propositions HDT (text-index-format.md §2.4), so a literal that occurs only
inside unasserted claims is still found; it resolves through the propositions
HDT's OPS group, its SPO layer and reverse entry, to the `data.hdt` triples that
name the triple term.

## 11. Construction (non-normative)

hdtc builds these artifacts after `data.hdt` and its permutation index:

1. The parser records every triple term it meets, nested ones included.
2. The records are sorted by IRI and deduplicated; equal IRIs are equal terms.
3. Each proposition's terms are located in `data.hdt`'s dictionary and the
   located triples merge-joined with its SPO stream. Those found are asserted.
4. The rest become the propositions HDT, built by the ordinary pipeline.
5. One more pass over the sorted records, beside both dictionaries' runs, writes
   the runs and the layer entries; POS and OPS positions come from merge-joins
   with each permutation index's own stream.

A merge renames blank-node labels in `…:tt:b:` IRIs as §2.2 requires, takes the
union of its inputs' propositions — asserted ones read back from each input's
SPO layer, unasserted ones from its propositions HDT — and repeats steps 2–5.
A term unasserted in one input and asserted in another is asserted in the
result; the propositions HDT is therefore not merged as a file.

## 12. Validation

A version 1 reader MUST reject an unknown version, unknown flags or sections,
sections that break §6's presence rules or shapes, nonzero reserved fields, a
footer that disagrees with the header, checksum failures, and a header whose
counts disagree with the bound HDTs. Strict validation MUST additionally verify:

1. both digests (§9), every payload checksum, and every rank directory against
   its bitmap — `LAYER`, or a sparse layer's `LAYER_HIGH`;
2. each run against the dictionary's triple-term IRIs (§7 requirement 1);
3. every run entry, by decoding its proposition and minting it again (§7
   requirement 2);
4. each layer: that every set position's triple is the target of the IRI its
   `REVERSE` entry names — which, with item 3, is the IRI that triple's
   proposition mints — that each layer has exactly one set position per IRI
   of the data run whose proposition it covers (§8), and, sparse, that the
   positions decoded are strictly increasing and below `N` (§8.1);
5. that every target is a triple of the HDT its flag names, that the
   propositions HDT holds exactly the unasserted targets, and that none of
   them is a triple of `data.hdt` (§7 requirement 3, §3);
6. the propositions HDT's permutation index, as for any permutation index; and
   that no propositions HDT exists without an index.

## 13. Size (non-normative)

With `T` triple terms, `A` of them asserted, and `N` triples in `data.hdt`:

```text
runs     ≈ T * (ceil(log2 S) + 1 + ceil(log2 P) + ceil(log2 O)) bits
layers   ≈ 3 * min(1.05 * N, 1.05 * (A + N / 2^l) + A * l) bits,  l = floor(log2(N / A))
reverse  ≈ 3 * A * ceil(log2 O) bits
```

plus the same for the propositions HDT's `T - A` triples (§8.1 has the exact
rule). For ROBOKOP annotated (`N` = 1.40 G, `A` = 133 M, every triple term
asserted) the layers are sparse with `l = 3`, 90 MB each against 183 MB dense,
and the index 2.75 GB rather than 3.03 GB. With a tenth as many triple terms
they would take 15 MB each, and with a hundredth, 2 MB.
