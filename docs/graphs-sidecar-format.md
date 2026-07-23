# HDT graphs sidecar format, version 1

Status: normative format specification for hdtc.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY**
are to be interpreted as described by RFC 2119.

## 1. Artifact and data model

For an HDT named `data.hdt`, automatic discovery uses one packed sidecar named:

```text
data.hdt.graphs
```

The `.graphs` suffix is appended to the complete HDT path. It does not replace
the `.hdt` suffix.

The associated HDT is a standard triples HDT containing `N` unique triples: the
deduplicated union of the RDF dataset. These triples are numbered by their
zero-based BitmapTriples SeqZ/SPO position `p` in `[0, N)`. The sidecar stores a
set of pairs:

```text
(graph_id, SPO_position)
```

Graph ID 0 denotes the RDF default graph and has no dictionary entry. The graph
dictionary assigns named graphs the IDs `1..G`, so the file always has `G + 1`
membership layers indexed by `0..G`.

Membership has set semantics. Duplicate copies of the same quad produce one
membership. A triple may belong to the default graph, one or more named graphs,
or both. The triples union is not the RDF default graph.

Every version 1 sidecar is exhaustive: every position in `[0, N)` occurs in at
least one layer. Let `count(g)` be the number of members of layer `g`; the total
membership count is:

```text
M = sum(count(g)) for g in 0..G
```

Therefore `M >= N`.

Sidecars are immutable and position-dependent. They MUST NOT be copied, shifted,
concatenated, or ORed when HDTs are merged. A merge reconstructs memberships by
graph term and triple, then assigns new positions in the output HDT.

### 1.1 Logical operations

A reader can expose at least these operations without loading a complete layer:

| Operation | Result |
|---|---|
| `access(g, p)` | Whether layer `g` contains position `p` |
| `rank(g, p)` | Number of members in `[0, p)` |
| `select(g, i)` | Position of zero-based member `i` |
| `next_member(g, p)` | First member position greater than or equal to `p` |
| `count(g)` | Number of members in layer `g` |
| `graphs_of(p)` | Increasing graph IDs containing `p` |
| `graph(g)` | Graph term for ID `g`, or the default-graph marker for 0 |
| `graph_id(term)` | ID of a graph term, if present |

Valid graph IDs are `0..G`. A position may equal `N` only as the second argument
to `rank`. `select(g, count(g))` is out of range.

## 2. Primitive conventions

- All fixed-width integers are unsigned and little-endian.
- Offsets are absolute byte offsets from the start of the sidecar.
- Ranges are half-open unless stated otherwise.
- Packed bits are least-significant-bit first within each byte or word.
- The file header, graph dictionary, layer directory, every non-empty layer's
  primary structure, and the footer start at 64-byte-aligned offsets.
- Secondary indexes and payloads start at offsets aligned to at least 8 bytes.
- Padding bytes are zero and are excluded from section lengths and checksums.
- An absent zero-length section has offset zero unless this specification says
  otherwise.
- Writers MUST reject arithmetic overflow in sizes, counts, offsets, and
  alignment calculations.

`CRC32C` means Castagnoli CRC-32C, polynomial `0x1EDC6F41` in normal notation
(equivalent to `CRC_32_ISCSI`), stored as a little-endian `u32`.

`CRC8` means CRC-8-CCITT with polynomial `0x07`, initial value 0, no reflection,
and final XOR 0.

HDT VByte values store seven data bits per byte, least-significant group first;
the high bit is 1 on the final byte and 0 on preceding bytes.

## 3. File layout

Sections occur in this order:

```text
[256-byte header]
[graph dictionary: one HDT PFC section]
[zero padding]
[G+1 layer-directory entries]
[directory CRC32C]
[zero padding]
[layer 0 data]
[layer 1 data]
...
[layer G data]
[zero padding]
[64-byte footer]
```

Layers are stored in graph-ID order. Readers MUST use recorded offsets rather
than assume that regions are adjacent.

## 4. File header

The header is exactly 256 bytes.

| Offset | Size | Field | Version 1 value or meaning |
|---:|---:|---|---|
| 0 | 8 | Magic | ASCII `$HDTGRPH` |
| 8 | 2 | Major version | `1` |
| 10 | 2 | Minor version | `0` |
| 12 | 4 | Header size | `256` |
| 16 | 8 | Flags | See below |
| 24 | 8 | Triple count | `N` |
| 32 | 8 | Named graph count | `G` |
| 40 | 8 | Membership count | `M` |
| 48 | 8 | Source data length | Associated HDT dictionary-and-triples suffix length |
| 56 | 8 | Sidecar size | Exact file size, including footer |
| 64 | 8 | Dictionary offset | Absolute, 64-byte aligned |
| 72 | 8 | Dictionary length | PFC bytes, excluding padding |
| 80 | 8 | Directory offset | Absolute, 64-byte aligned |
| 88 | 8 | Directory length | Exactly `(G + 1) * 96 + 4` |
| 96 | 8 | Layers offset | First layer region, or footer offset if no layer data exists |
| 104 | 8 | Layers length | Span through the last layer payload, including internal padding |
| 112 | 8 | Footer offset | Exactly `sidecar_size - 64` |
| 120 | 4 | Identity algorithm | `1`, meaning SHA-256 |
| 124 | 4 | Position chunk shift | `16` |
| 128 | 32 | Source digest | SHA-256 described in section 10 |
| 160 | 92 | Reserved | All zero |
| 252 | 4 | Header CRC32C | CRC32C of bytes `[0, 252)` |

If there is no layer data, `layers_length` is zero and `layers_offset` equals
`footer_offset`. Otherwise `layers_offset` and `layers_offset + layers_length`
bound all layer regions.

Flags are assertions:

| Bit | Name | Meaning |
|---:|---|---|
| 0 | `EXHAUSTIVE` | Every SPO position belongs to at least one graph |
| 1 | `DISJOINT` | Every SPO position belongs to at most one graph |
| 2 | `HAS_BLANK_GRAPH_NAMES` | At least one dictionary term begins with `_:` |
| 3..63 | Reserved | Zero |

Version 1 writers MUST set `EXHAUSTIVE`. If `DISJOINT` is set, exhaustiveness
means every position occurs in exactly one layer.

## 5. Graph dictionary

The dictionary is one standard HDT Plain Front Coding (PFC) section:

```text
u8    section_type = 0x02
VByte string_count = G
VByte buffer_length
VByte block_size = 16
u8    preamble_crc8
LogArray block_offsets
u8    front_coded_buffer[buffer_length]
u32   buffer_crc32c
```

`preamble_crc8` covers the section type and the three encoded VByte fields.
`buffer_crc32c` covers only the front-coded buffer.

Terms are unique and strictly sorted by unsigned UTF-8 byte order. Their
one-based ordinal is their graph ID. An IRI is stored as its UTF-8 IRI string
without angle brackets. A blank-node graph name is stored as `_:label` after
input blank-node scoping. Literals and a default-graph marker are forbidden.

The buffer contains blocks of at most 16 terms. The first term in a block is its
UTF-8 bytes followed by NUL. Each later term is:

```text
VByte(common_prefix_bytes_with_previous_term)
UTF-8 suffix bytes
NUL
```

Prefix lengths are byte counts and MUST end on a UTF-8 boundary.

The block-offset LogArray is:

```text
u8    type = 1
u8    bits_per_entry
VByte entry_count
u8    preamble_crc8
u8    packed_entries[ceil(entry_count * bits_per_entry / 8)]
u32   entries_crc32c
```

Its preamble CRC covers `type`, `bits_per_entry`, and the encoded entry count.
Entries are packed LSB-first with a fixed width of `bits_per_entry`, which is in
`0..64`. There is one offset per PFC block followed by a sentinel equal to
`buffer_length`, so:

```text
entry_count = ceil(G / 16) + 1
```

Offsets are monotone, block offsets point into the buffer, and the final sentinel
equals the buffer length. For an empty dictionary, the sole entry is zero and
may use zero bits. Unused packed tail bits MUST be zero.

## 6. Layer directory

The directory contains `G + 1` consecutive 96-byte entries indexed by graph ID,
followed immediately by a little-endian CRC32C over all entry bytes.

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 8 | Primary offset | Absolute encoding index/header offset |
| 8 | 8 | Primary length | Encoding-specific bytes |
| 16 | 8 | Secondary offset | Absolute secondary-index offset |
| 24 | 8 | Secondary length | Encoding-specific bytes |
| 32 | 8 | Item count A | Encoding-specific count |
| 40 | 8 | Item count B | Encoding-specific count |
| 48 | 8 | Member count | `count(g)` |
| 56 | 8 | Minimum position | First member, or `N` if empty |
| 64 | 8 | Maximum position, exclusive | Last member plus one, or 0 if empty |
| 72 | 4 | Encoding | `1` dense chunks, `2` sparse chunks, `3` Elias–Fano |
| 76 | 4 | Layer flags | Zero |
| 80 | 4 | Primary CRC32C | CRC of primary bytes |
| 84 | 4 | Secondary CRC32C | CRC of secondary bytes |
| 88 | 8 | Parameter | Encoding-specific value |

An empty layer MUST use encoding 2 with all offsets, lengths, item counts, CRCs,
and the parameter set to zero, `member_count = 0`, `minimum_position = N`, and
`maximum_position_exclusive = 0`.

For a non-empty chunked layer, primary is the chunk directory. Item count A is
the number of stored chunk entries and item count B is the number of non-empty
chunks. For a sparse layer, secondary is its access hash and parameter is the
hash capacity. Dense layers have no secondary section and parameter zero.

For Elias–Fano, primary is its 160-byte header. Both item counts, the secondary
fields, and parameter are zero.

## 7. Chunked layer encodings

Positions are divided into chunks of `2^16 = 65,536`:

```text
chunk_key    = p >> 16
chunk_offset = p & 0xffff
```

The universe contains zero chunks when `N = 0`; otherwise it contains
`1 + ((N - 1) >> 16)` chunks.

### 7.1 Dense and sparse directories

Encoding 1 is dense. It stores one entry for every universe chunk, including
empty chunks. Item count A equals the universe chunk count, each `chunk_key`
equals its entry index, and there is no secondary index.

Encoding 2 is sparse. A non-empty sparse layer stores only non-empty chunks in
strictly increasing key order, so item counts A and B are equal. Its secondary
index is the hash table in section 7.4.

Readers MUST support both encodings. Encoding choice is not part of logical file
identity.

### 7.2 Chunk entry

Each stored chunk entry is 48 bytes.

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 8 | Chunk key | Position chunk number |
| 8 | 8 | Rank before | Layer members in all smaller-key chunks |
| 16 | 8 | Payload offset | Absolute and 8-byte aligned; zero if empty |
| 24 | 4 | Payload length | Bytes excluding padding; zero if empty |
| 28 | 4 | Cardinality | Members in this chunk, `0..65,536` |
| 32 | 1 | Container encoding | `0` empty, `1` array, `2` bitmap |
| 33 | 1 | Flags | Zero |
| 34 | 2 | Reserved | Zero |
| 36 | 4 | Payload CRC32C | CRC of payload; zero if empty |
| 40 | 8 | Reserved | Zero |

`rank_before` starts at zero and each following entry's value is the preceding
entry's `rank_before + cardinality`. The final sum equals the layer's member
count.

An empty dense chunk uses container 0 and has no payload. Sparse entries are
never empty. Payloads occur in increasing chunk-key order and MUST NOT overlap.

### 7.3 Containers

Cardinalities 1 through 4096 use an array container. Its payload is exactly
`cardinality` strictly increasing little-endian `u16` offsets, for a length of
`2 * cardinality` bytes.

Cardinalities 4097 through 65,536 use a bitmap container. Its payload is exactly
8,448 bytes:

```text
u16 subrank[128]   // 256 bytes
u8  bitmap[8192]   // 65,536 bits
```

`subrank[j]` counts set bits at offsets less than `j * 512`. The bitmap is
LSB-first. Its population count equals the chunk cardinality. In the final
universe chunk, bits representing positions at or above `N` MUST be zero.

Every non-empty payload has its own CRC32C recorded in the chunk entry.

### 7.4 Sparse access hash

The sparse secondary index is an immutable open-addressed hash table from chunk
key to chunk-directory entry index:

- Capacity is the smallest power of two at least twice the number of entries,
  with a minimum of 2.
- Each slot is a little-endian `u64`.
- Zero denotes an empty slot; any other value is `entry_index + 1`.
- Entries are inserted in increasing chunk-key order.
- Collisions use linear probing with wraparound. There are no tombstones.

The initial slot is `mix64(chunk_key) & (capacity - 1)`, using wrapping `u64`
arithmetic:

```text
z = chunk_key + 0x9e3779b97f4a7c15
z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
z = (z ^ (z >> 27)) * 0x94d049bb133111eb
mix64 = z ^ (z >> 31)
```

The directory's secondary CRC covers the complete hash table. Readers MUST
bounds-check a decoded entry index before dereferencing it.

## 8. Elias–Fano layer encoding

Encoding 3 stores a non-empty, strictly increasing sequence of `m` positions in
the universe `[0, N)`. Define, using integer division:

```text
l = 0 if N / m <= 1, otherwise floor(log2(N / m))
H = 1 + ((N - 1) >> l)
L = H + m
```

The low `l` bits of each position are packed consecutively in sequence order.
The upper bitmap has exactly `L` bits and sets bit `(x_i >> l) + i` for every
zero-based sequence index `i`.

### 8.1 Elias–Fano header

The directory's primary offset points to this 160-byte, 64-byte-aligned header.
The directory's primary CRC32C covers all 160 bytes.

| Offset | Size | Field | Value or meaning |
|---:|---:|---|---|
| 0 | 8 | Magic | ASCII `$HDTEF01` |
| 8 | 4 | Header size | `160` |
| 12 | 4 | Low-bit width | `l` |
| 16 | 8 | Universe | `N` |
| 24 | 8 | Member count | `m` |
| 32 | 8 | Upper bit length | `L` |
| 40 | 8 | High bucket count | `H` |
| 48 | 8 | Lower offset | Absolute and 8-byte aligned, or zero when `l = 0` |
| 56 | 8 | Lower length | `ceil(m * l / 8)` |
| 64 | 8 | Superrank offset | Absolute and 8-byte aligned |
| 72 | 8 | Superrank length | `superrank_count * 8` |
| 80 | 8 | Subrank offset | Absolute and 8-byte aligned |
| 88 | 8 | Subrank length | `subrank_count * 2` |
| 96 | 8 | Upper offset | Absolute and 8-byte aligned |
| 104 | 8 | Upper length | `ceil(L / 8)` |
| 112 | 8 | Superrank count | `ceil(L / 4096) + 1` |
| 120 | 8 | Subrank count | `ceil(L / 512)` |
| 128 | 4 | Lower CRC32C | CRC of lower bytes, or zero if absent |
| 132 | 4 | Superrank CRC32C | CRC of superrank bytes |
| 136 | 4 | Subrank CRC32C | CRC of subrank bytes |
| 140 | 4 | Upper CRC32C | CRC of upper bytes |
| 144 | 12 | Reserved | Zero |
| 156 | 4 | Header CRC32C | CRC32C of bytes `[0, 156)` |

Regions occur in header, lower (if present), superrank, subrank, upper order and
do not overlap. All unused high bits in final packed bytes are zero.

### 8.2 Packed data and rank indexes

Lower entry `i` begins at bit `i * l` and contains the low `l` bits of `x_i`,
LSB-first. When `l = 0`, the lower length, offset, and CRC are all zero.

The upper bitmap is LSB-first and contains exactly `m` set bits. Its indexes are:

- `superrank[k]`: one bits before bit `min(k * 4096, L)`. The final sentinel is
  `m`.
- `subrank[j]`: one bits from the start of the containing 4096-bit superblock to
  bit `min(j * 512, L)`. Every eighth subrank is zero.

Sequence position `i` is recovered as:

```text
high  = select1(upper, i) - i
value = (high << l) | lower(i)
```

The four packed regions have the independent CRC32C values in the header.

## 9. Footer

The footer is exactly 64 bytes and starts at `footer_offset`.

| Offset | Size | Field | Version 1 value or meaning |
|---:|---:|---|---|
| 0 | 8 | Magic | ASCII `$HDTGEND` |
| 8 | 2 | Major version | `1` |
| 10 | 2 | Minor version | `0` |
| 12 | 4 | Footer size | `64` |
| 16 | 8 | File size | Exact sidecar size |
| 24 | 8 | Header offset | `0` |
| 32 | 8 | Directory offset | Copy of header field |
| 40 | 8 | Directory length | Copy of header field |
| 48 | 8 | Reserved | Zero |
| 56 | 4 | Header CRC32C copy | Copy of header checksum |
| 60 | 4 | Footer CRC32C | CRC32C of bytes `[0, 60)` |

## 10. Binding to the associated HDT

The sidecar's positions are valid only for one exact serialization of the HDT's
position-defining data. The identity input begins at the first byte of the HDT
Dictionary Control Information and extends through end of file. It therefore
contains the complete Dictionary and Triples sections, including their control
information and checksums.

The header stores both the byte length of this suffix and its SHA-256 digest.
It also stores the HDT's decoded triple count `N`. A strict reader MUST verify all
three values before trusting memberships.

The HDT Global and Header sections are deliberately excluded. Metadata can be
rewritten while copying Dictionary and Triples bytes verbatim without
invalidating the sidecar. Any change to dictionary IDs, BitmapTriples data, or
SPO positions invalidates it.

A normal low-latency open MAY compare only the suffix length and triple count
after validating fixed metadata. Operations that cross a trust boundary, such as
strict validation or a graph-preserving merge, MUST also recompute SHA-256.

## 11. Conformance and validation

A version 1 reader MUST accept version exactly 1.0 and MUST reject unknown flags,
encodings, nonzero reserved fields, invalid ranges, overlaps, and arithmetic
overflow.

Strict validation MUST additionally verify:

1. The source-HDT suffix length, SHA-256 digest, and triple count.
2. Header, footer, dictionary, directory, primary, secondary, and payload CRCs.
3. PFC term validity, strict sort order, block offsets, sentinel, and the
   `HAS_BLANK_GRAPH_NAMES` assertion.
4. Layer order, alignment, non-overlap, encoding-specific sizes, rank
   recurrences, cardinalities, strictly increasing member positions, and zero
   tail bits.
5. The sum of layer counts equals `M`.
6. An external position-order sweep covers every position `0..N-1`; if
   `DISJOINT` is set, each position occurs exactly once.

Validation MUST be implementable with bounded memory. The exhaustive global
check may use an external sort rather than an allocation proportional to `N` or
`M`.

## 12. Construction guidance (non-normative)

The packed format is designed for streaming construction. hdtc carries graph IDs
through its global SPO sort, deduplicates `(S, P, O, graph_id)` records, assigns a
position when a new union triple is emitted, and produces distinct memberships.

For a moderate graph count, memberships can be routed into bounded per-layer
compressed spools. For a very large graph count or a small memory budget, they
can be externally sorted by `(graph_id, position)`. Both routes yield the same
graph-major stream.

The finalizer consumes that stream once. It spools only the current layer's
sorted `u64` positions to one reusable scratch file, gathers density and chunk
statistics, selects a chunked or Elias–Fano representation, rewinds and encodes
the layer, then reuses the scratch file. It need not retain a large graph in
memory or create one temporary file per graph.

Readers likewise keep the dictionary, indexes, and payloads on disk. Point
operations do not require whole-layer decompression or an allocation
proportional to `N`.
