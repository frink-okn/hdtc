# HDT key-set artifact format, version 1

Status: normative format specification for hdtc.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY**
are to be interpreted as described by RFC 2119.

## 0. Purpose and relationship to the sketches

`hdtc keyset` writes the **complete, sorted set of distinct 64-bit term keys**
for a dictionary role, under the *same* term-to-key convention the sketch
artifacts use ([sketch-format.md](sketch-format.md) §3). Where a `.filter`
answers membership with a false-positive rate and a `.minhash` answers overlap
with an estimator, a `.keys` file answers both **exactly**:

- **membership** is a lookup — no false positives;
- **overlap** `|A ∩ B|` is an integer merge — no estimator, no skew failure;
- **the shared keys can be enumerated**, which is what an actual join needs and
  which neither sketch can do at any accuracy.

The key set is the exact parent of both sketches: a `.minhash` is its bottom `k`
values, and a `.filter` is built from it. It costs roughly **four times** a
Fuse8 filter's bytes (§5), which is the whole trade-off — a role below ~10⁸ keys
is cheap enough to publish exactly, and above that the sketches earn their
approximation.

"Exact" here means *exact modulo the same 64-bit collision semantics the filters
already live with* (§1.3), not a new source of error.

**These files are designed to be produced by one party and consumed by
another, with no shared code and no shared runtime.** Everything a reader needs
is in the file or in this document; both payload encodings are specified as
integer arithmetic and defer to no library.

## 1. Artifacts and data model

One file per role, conventionally in a `keysets/` directory beside the HDT:

```text
keysets/
  subjects.keys     complete distinct-key set over subject IRIs
  objects.keys      complete distinct-key set over object IRIs
```

Each file is self-describing and independently readable, for the same reasons
the sketch files are ([sketch-format.md](sketch-format.md) §1).

Two things that are *not* in that directory are worth distinguishing, because
they sound alike and are not:

- The **union of the published pair**, `subjects ∪ objects`, is not shipped. It
  is their deduplicated merge, which any consumer reconstructs from the two
  files in one linear pass, so shipping it would be redundant.
- The **`terms` role** (§1.1) is not that union. It also carries predicate IRIs,
  which appear in neither published role, so it **cannot** be reconstructed from
  the pair. That is precisely why it is an emitted role rather than a derived
  view.

### 1.1 Roles

| id | name | membership |
|---:|---|---|
| `0` | subjects | qualifying terms in `Shared ∪ Subjects` |
| `1` | objects | qualifying terms in `Shared ∪ Objects` |
| `2` | terms | **every** qualifying term in the dictionary: `Shared ∪ Subjects ∪ Objects ∪ Predicates` |

Roles `0` and `1` are identical to the sketch roles of the same id, so a
`.keys`, a `.filter`, and a `.minhash` for the same role describe the same set.
A term in the Shared section is both a subject and an object and is therefore
present in **both** roles; that is intended, since roles are compared
role-to-role.

**Role `2` (`terms`) is an hdtc extension, not part of the published role
pair.** It exists to measure what a whole-vocabulary key set costs and what it
answers that the role split does not, and it MAY be withdrawn in a future
version. Two consequences:

- Role `2` covers a larger population than either published role, so an
  intersection involving it answers a **different question**, not an invalid
  one: `terms` against `subjects` asks "which IRIs does this dataset use at all
  that that dataset describes?", which is not the symmetric role-to-role
  overlap the published pair reports. Containment and Jaccard computed across
  the two are therefore not comparable with figures from the pair. This is a
  caution about interpretation; it is not a restriction on what may be
  intersected (§4.1).
- Role `2` is the only role that includes **predicate** IRIs. The sketch
  convention gives predicates no role at all
  ([sketch-format.md](sketch-format.md) §1.1) because a dataset's predicate set
  is small and VoID property partitions describe it exactly. Nothing here
  changes that judgement for the published roles.

Because `role` is an explicit field that a reader MUST reject when unknown
(§4.4), a reader written against roles `0` and `1` alone rejects a `terms` file
safely rather than misreading it.

### 1.2 Qualifying terms

Identical to [sketch-format.md](sketch-format.md) §1.2, and **this is a
requirement, not a coincidence**: it is half of what `convention_id = 1`
asserts. A dictionary term qualifies iff it is an **absolute IRI**. Excluded:

- **Literals** — not identity-bearing across datasets. Recognised in HDT's
  dictionary by a leading `"` (U+0022).
- **Blank nodes** — dataset-local by construction. Recognised by a leading `_:`.
- **Skolem IRIs** — dataset-local for the same reason, and MUST be excluded when
  the producer applies a documented skolem prefix.

This applies to role `2` exactly as it does to roles `0` and `1`: `terms` is
every qualifying **IRI** in the dictionary, not every dictionary entry. A
dictionary's literals and blank nodes never appear in any role.

A role whose qualifying set is empty is **not** an error. It produces a
well-formed file with `key_count = 0` (§4.4).

### 1.3 Distinctness and collisions

The payload is a **set**: strictly ascending, therefore distinct. Producers MUST
deduplicate before encoding.

Distinct *terms* are guaranteed by the source — Shared, Subjects, and Objects
are distinct and mutually disjoint — but distinct *keys* are not, and role `2`
adds one further source of duplication: the Predicates section is a separate ID
space whose IRIs may also appear in Shared, Subjects, or Objects. Deduplication
handles both cases identically.

**These two causes must not be conflated when reporting.** For roles `0` and `1`
a duplicate key can only be a hash collision, and is rare enough to be worth a
warning. For role `2` duplicates are ordinary and common: an IRI used as a
predicate is very often also a subject or object of some triple — in ubergraph,
1129 of 1251 predicate IRIs are — so counting them as collisions would overstate
the collision rate by many orders of magnitude. A producer that reports
collisions MUST distinguish the two, or report neither.

Consequently `key_count` **is the number of distinct keys**, and is
authoritative for the payload — unlike the sketch `key_count`, which counts
distinct qualifying IRIs and may exceed the distinct key count
([sketch-format.md](sketch-format.md) §3.3). A producer SHOULD log the number of
collisions observed. By the birthday bound, at one billion distinct IRIs in a
role the probability of at least one collision is about 2.7%; membership false
positives from a query-key collision are `≈ n / 2⁶⁴`, about 10⁻¹⁰ even at
Wikidata scale.

**64 bits is the right width.** Spurious intersection between two independent
roles is `≈ n_A · n_B / 2⁶⁴`; the largest realistic pair (2.3×10⁹ against
itself) expects ~0.29 spurious keys. At `2⁵⁶` it would be ~18 800.

## 2. Primitive conventions

- All integers are **unsigned** and **little-endian**.
- `u8`, `u16`, `u32`, `u64` denote 1, 2, 4, and 8 byte unsigned integers.
- `>>` is a logical (zero-filling) shift. `|` is bitwise OR, `&` bitwise AND.
- **Arithmetic over sizes, counts, offsets, and every validation rule is exact**:
  it is evaluated over mathematical integers, or in a width that cannot overflow,
  or with checked operations that fail the file. It MUST NOT wrap. Nothing in
  this format wraps — there is no counterpart to the sketch probe's deliberate
  modular arithmetic.

  This matters most at §4.4 rule 9: `2^(64 - low_width)` reaches the top of the
  `u64` range for the largest key counts, so the payload-length check MUST be
  evaluated in a wider type (or with checked operations), never in `u64`. A
  reader that lets it wrap can be made to accept a file whose declared payload
  is far shorter than the structure it describes.
- Fields named `reserved` MUST be written as zero, and a reader MUST reject a
  file in which any reserved field is nonzero.
- `CRC32C` means Castagnoli CRC-32C, polynomial `0x1EDC6F41` in normal notation
  (reversed form `0x82F63B78`, equivalent to `CRC_32_ISCSI`), initial value
  `0xFFFFFFFF`, reflected input and output, final XOR `0xFFFFFFFF`, stored as a
  little-endian `u32`.

Every file has the shape:

```text
[96-byte header] [payload] [4-byte CRC32C trailer]
```

The CRC32C is computed over **every byte from offset 0 up to but not including
the CRC field itself**, and there MUST be no bytes after it.

## 3. Key derivation (term → `u64`)

**Identical to [sketch-format.md](sketch-format.md) §3, normatively.** That
section is the definition; it is not restated here, so that the two cannot
drift. In summary, and only as a summary:

- the bytes hashed are the **expanded absolute IRI** as its exact Unicode
  codepoints in UTF-8, with **no normalization of any kind** (§3.1 there);
- `hash_id = 1` denotes **XXH64 with seed 0** over those bytes, the full 64-bit
  result used directly as the key (§3.2 there);
- `XXH64("", seed 0) = 0xEF46DB3751D8E999` is the anchor an implementation must
  reproduce before emitting `hash_id = 1`.

This is the one place where a disagreement between implementations is both fatal
and silent: artifacts built with a different key derivation are not comparable,
but nothing detects that — intersections simply come out empty. The conformance
vectors in §8 share their key values with
[sketch-format.md](sketch-format.md) §9 precisely so that an implementation can
confirm the two agree.

## 4. File format

### 4.1 Header

| Offset | Size | Type | Field | Value or meaning |
|---:|---:|---|---|---|
| 0 | 8 | `char[8]` | `magic` | `"KGFKEYS\0"` (`4B 47 46 4B 45 59 53 00`) |
| 8 | 2 | `u16` | `format_version` | container format version; `1` |
| 10 | 2 | `u16` | `convention_id` | semantic convention; `1` = §1.1–§1.3 and §3 |
| 12 | 1 | `u8` | `hash_id` | `1` = XXH64 seed 0 |
| 13 | 1 | `u8` | `role` | `0` = subjects, `1` = objects, `2` = terms |
| 14 | 1 | `u8` | `encoding` | `0` = raw sorted `u64`, `1` = Elias-Fano |
| 15 | 1 | `u8` | `low_width` | Elias-Fano low-part width; `0` when `encoding = 0` |
| 16 | 8 | `u64` | `key_count` | number of distinct keys in the payload — authoritative |
| 24 | 8 | `u64` | `min_key` | smallest key; `0` when `key_count = 0` |
| 32 | 8 | `u64` | `max_key` | largest key; `0` when `key_count = 0` |
| 40 | 8 | `u64` | `payload_len` | payload length in **bytes** |
| 48 | 32 | `u8[32]` | `source_digest` | SHA-256 binding to the source HDT (§6) |
| 80 | 16 | `u8[16]` | `reserved` | MUST be zero |

The payload begins at offset **96**, a multiple of 32, so both encodings are
naturally `u64`-aligned and a consumer MAY memory-map the file and use the
payload in place with no copy and no unaligned access.

Total file size is `96 + payload_len + 4`.

`convention_id` identifies the **semantics** behind the keys — which terms
qualify, how a role is defined, and how a term becomes a key. It is separate
from `format_version` because two files can share a byte layout and still be
incomparable, which is the more dangerous mismatch (§9).

Two files are **comparable** iff they have the same `convention_id` and the same
`hash_id` — identical to [sketch-format.md](sketch-format.md) §4, because it is
the same convention. Nothing else affects comparability: not `format_version`,
not `encoding`, not `role`, and in particular not `source_digest`. A `.keys`
file is comparable in this sense with a `.filter` or `.minhash`, which is what
makes the three artifact families interoperate.

**`role` describes which population a file's keys were drawn from; it does not
restrict what may be intersected.** Cross-role comparison is a normal and
important operation, not a defect: intersecting one dataset's `objects` with
another's `subjects` answers *which of the things I mention does that dataset
describe?*, which is the question a `void:Linkset` or an actual join is asking,
and a key set answers it exactly. Requiring matching roles would rule out the
capability §0 exists to provide.

What a consumer must do is read the result in light of the roles involved. A
same-role intersection is a symmetric overlap between comparable populations; a
cross-role one is directional and its two operands have different sizes and
meanings, so a Jaccard computed across them is not a dataset-similarity measure
and should not be reported as one. Consumers SHOULD state the roles alongside
any published overlap figure.

`min_key` and `max_key` exist for the disjoint-range prefilter of §5. They carry
no information when `key_count = 0`, and are specified as zero there so the
field is never mistaken for a real bound; a reader MUST treat `key_count = 0` as
the empty set and skip the prefilter rather than comparing the zero range.

### 4.2 Payload, `encoding = 0` (raw sorted `u64`)

`key_count` little-endian `u64` values in strictly ascending order.
`payload_len = key_count × 8`, and `low_width = 0`.

This is the honest baseline: 8 bytes per key, `mmap` + binary search for
membership, and the fastest possible merge for intersection. It is larger than
Elias-Fano and is offered so a bundle can trade bytes for decode simplicity, and
so the two can be measured against each other.

### 4.3 Payload, `encoding = 1` (Elias-Fano)

Elias-Fano is near-optimal for this data and is the recommended encoding. For
`n` keys drawn uniformly from `u = 2^64` it costs about `2 + ⌈log₂(u/n)⌉` bits
per key, within ~0.06 bytes of the information floor. **The rate falls as the
role grows** (§5).

Let `n = key_count`, and `l = low_width`.

**Sizing rule (normative).** For `n ≥ 1`:

```text
l = 63 - floor(log2(n))
```

which is the standard `l = floor(log2(u/n))` specialised to `u = 2^64`. For
`n = 0`, `l = 0` and the payload is empty. This rule is normative — not merely
how hdtc happens to size the array — because `payload_len` and the high-bits
vector length are both derived from it, and a reader must be able to check them
(§4.4).

Each key `v_i` splits into `high_i = v_i >> l` and
`low_i = v_i & (2^l - 1)`.

**Low-bits array**, first. `n` fields of `l` bits each, field `i` occupying bits
`[i·l, (i+1)·l)` of a little-endian bit stream: bit `j` of the stream is bit
`j mod 64` of `u64` word `⌊j / 64⌋`, and each field is stored
least-significant-bit first. The array is zero-padded to a whole number of
`u64` words, so it occupies `⌈n·l / 64⌉ × 8` bytes. When `l = 0` it is empty.

**High-bits vector**, immediately after. A bit vector of

```text
H = n + 2^(64 - l)      bits
```

in which **bit `high_i + i` is set for each `i`**, and every other bit is zero.
Same bit order as the low array; zero-padded to a whole number of `u64` words,
occupying `⌈H / 64⌉ × 8` bytes.

Because `l = 63 - floor(log2(n))`, the term `2^(64-l)` is in `[n, 2n)`, so `H`
is always in `[2n, 3n)` bits — the vector cannot blow up for any key count. The
positions `high_i + i` are strictly increasing, so a producer writes the vector
forward in a single pass and a reader decodes it in one.

Therefore:

```text
payload_len = (ceil(n·l / 64) + ceil(H / 64)) × 8          # n >= 1
payload_len = 0                                            # n == 0
```

**Decoding.** Walk the high-bits vector; when the `i`-th set bit (counting from
0) is found at position `p`, the key is:

```text
v_i = ((p - i) << l) | low_i
```

A reader that wants `O(1)` `select` builds its own sampled index after loading.
The format deliberately stores no such index: it is derivable, its parameters
are a consumer's performance choice rather than a property of the data, and
storing pointers a reader must then validate would add the one class of
structural attack this format otherwise does not have.

### 4.4 Reader validation rules

These files cross trust boundaries — the whole point is that any party can
consume any other party's artifacts. A reader MUST verify **all** of the
following before interpreting the payload, and MUST reject the file if any
fails. Per §2, every rule is evaluated in exact arithmetic.

1. `magic` is exactly `"KGFKEYS\0"`.
2. `format_version` is one the reader implements.
3. `convention_id` is one the reader implements.
4. `hash_id` is one the reader implements.
5. `role` is one the reader implements. Roles `0` and `1` are the published
   pair; a reader that does not implement role `2` MUST reject it rather than
   treat it as a subject or object set.
6. `encoding` is `0` or `1`.
7. `reserved` is all zero.
8. The file length is exactly `96 + payload_len + 4`, and the CRC32C trailer
   matches the preceding bytes. **Verify the CRC before interpreting any other
   field.**
9. `payload_len` and `low_width` agree with the encoding:
   - `encoding = 0`: `low_width == 0` and `payload_len == key_count × 8`.
   - `encoding = 1` with `key_count == 0`: `low_width == 0` and
     `payload_len == 0`.
   - `encoding = 1` with `key_count ≥ 1`: `low_width == 63 - floor(log2(key_count))`
     and `payload_len == (ceil(key_count·low_width / 64) + ceil(H / 64)) × 8`
     for `H = key_count + 2^(64 - low_width)`.
10. `key_count == 0` implies `min_key == 0` and `max_key == 0`; otherwise
    `min_key ≤ max_key`.

Rule 9 is what makes decoding memory-safe: it fixes the exact extent of both
arrays inside a payload whose length rule 8 has already pinned to the file
length, so a decoder that has validated the header cannot be driven to read
outside the mapping. It is the counterpart of
[sketch-format.md](sketch-format.md) §5.3 invariant 5, and it fails in the same
way if evaluated in a width that wraps.

A reader SHOULD additionally verify, when it can afford the pass, that the
decoded keys are **strictly ascending** and that the first and last equal
`min_key` and `max_key`. This is a well-formedness check, not a safety check —
no decode step depends on it — but binary search, galloping, and the merge of §5
all silently return wrong answers on an unsorted payload. A reader that skips it
for size reasons MUST NOT then report its results as exact. Verifying it is
`O(n)` and is folded into any full scan for free.

## 5. Operations and cost

**Membership** (`is key x in role R?`): `encoding = 0` → binary search,
`O(log n)`. `encoding = 1` → `rank`/`select` over the high-bits vector,
effectively `O(1)` with a sampled select structure the reader builds. Exact
modulo the `n/2⁶⁴` collision of §1.3.

**Intersection**, by regime — the `min_key`/`max_key` fields gate the cheap
cases first:

- **Disjoint ranges** (`max_A < min_B`, or the reverse) → empty, with zero
  payload reads.
- **Skewed pair** (a 136-key role against a 2.3×10⁹-key role) → gallop the small
  side into the large: `≈ n_small · log₂(n_big/n_small)` probes, ~3 300 for that
  example. Sub-millisecond, and exact — which is the case where a bottom-`k`
  sketch pair returns zero shared keys and tells you nothing.
- **Balanced pair** → linear merge; when only the cardinality is wanted, count
  without materializing.

Containment `C(A→B) = |A ∩ B| / |A|` is then computed, not debiased.

**Size.** The measured rate for the Elias-Fano encoding, against a Fuse8
filter over the same role:

| role size `n` | `low_width` | EF bytes/key | raw bytes/key | Fuse8 bytes/key |
|---:|---:|---:|---:|---:|
| 10⁶ | 44 | 5.76 | 8 | ~1.13 |
| 10⁷ | 40 | 5.33 | 8 | ~1.13 |
| 10⁸ | 37 | 4.92 | 8 | ~1.13 |
| 2.3×10⁹ | 32 | 4.36 | 8 | ~1.13 |

So a key set costs roughly **4.3× a Fuse8 filter**, and Elias-Fano saves roughly
**35–45%** against the raw array at these scales. Below about 10³ keys the
per-key figures are dominated by the 100-byte fixed overhead and by word
rounding, and Elias-Fano can exceed 8 bytes per key; the encoding is chosen for
the large roles where it matters, and the absolute sizes are trivial either way.

**Do not compress a key set,** at rest or in transport. The keys are `XXH64`
outputs — built to be uniform and incompressible — and Elias-Fano has already
captured the only structure present, the gap distribution. What remains is
high-entropy, so a general-purpose compressor finds ~0–2% and costs the file its
`mmap`-in-place random access. Compressing the *raw* encoding instead does
better, because high-order bytes drift slowly across sorted neighbours, but
reaches only ~6–7 bytes/key — still worse than Elias-Fano, which encodes that
same gap structure exactly rather than guessing at it.

## 6. Binding to the source HDT

`source_digest` is the SHA-256 of the source HDT's **Dictionary-and-Triples
suffix**: the byte range beginning at the Dictionary Control Information and
continuing to end of file. The HDT Global and Header sections are excluded.

This is the same identity range the sketches use
([sketch-format.md](sketch-format.md) §7) and the graphs sidecar uses
([graphs-sidecar-format.md](graphs-sidecar-format.md) §10), for the same reason:
`hdtc header` rewrites an HDT's metadata while copying the Dictionary and
Triples bytes verbatim, so a header-only edit leaves these artifacts valid.
A `.keys` file and a `.filter` built from the same HDT carry the identical
digest, which is how a consumer confirms they describe the same build.

`source_digest` is **advisory**. It detects staleness and records provenance. It
MUST NOT gate comparability (§4.1), and a reader comparing two datasets MUST NOT
require their digests to relate in any way. Any change to Dictionary or Triples
bytes changes the digest even when the resulting key sets are identical, so a
mismatch means "rebuilt", not necessarily "different".

## 7. Conformance

A conforming **producer** MUST:

1. Emit each file so that it conforms on its own.
2. Derive keys exactly per §3, including the no-normalization rule.
3. Emit the payload strictly ascending and deduplicated (§1.3).
4. Size the payload by the §4.3 rule, and write `low_width`, `payload_len`,
   `min_key`, and `max_key` consistently with it.
5. Write zero to every reserved field.
6. Emit a well-formed file for a role it covers whose qualifying set is empty,
   rather than omitting the file (§1.2).
7. Evaluate all size arithmetic exactly (§2).

**Emitting a subset of the roles is conforming.** A publisher distributing a
dataset for general consumption SHOULD emit both published roles, because a
consumer that finds one role has no way to tell "this role was not built" from
"this dataset has no such IRIs" — the empty file in rule 6 exists to make that
distinction.

A conforming **reader** MUST:

1. Verify the CRC32C before interpreting any field.
2. Apply every §4.4 rule, in exact arithmetic, before decoding the payload.
3. Treat `source_digest` as advisory only.
4. Never compare artifacts across differing `convention_id` or `hash_id`.
   Differing `role` is comparable and often the point (§4.1); the reader is
   responsible for interpreting a cross-role result as directional rather than
   as a symmetric overlap.
5. Handle a missing role file as absent information, never as an empty role. An
   empty role is stated by a file with `key_count = 0`, not by silence.

## 8. Conformance vectors (frozen)

Unlike the filter's, a key set's bytes are **fully determined** by its key set
and encoding — there is no construction freedom (contrast
[sketch-format.md](sketch-format.md) §5.4). An implementation that does not
reproduce these bytes exactly is non-conforming.

`source_digest` is omitted from the vectors because it depends on the source HDT
build, not on the key set; substitute the digest of whatever HDT is at hand.

### 8.1 The key set

The 5 IRIs `https://example.org/resource/NNN` for `NNN` = `000` through `004`.
Their keys, ascending, are **the same five values** frozen as the "small set"
`.minhash` vector in [sketch-format.md](sketch-format.md) §9.2 — reproducing
both confirms the two artifact families share one convention:

```text
0x00CC3131E8F7A0C5  0x0DA98875B72FDF91  0x35C5F517A376FED8
0x45C64AD78FDE51E4  0xAF5A5827FAE076D7
```

### 8.2 `encoding = 0` (raw)

```text
key_count   = 5
encoding    = 0
low_width   = 0
min_key     = 0x00CC3131E8F7A0C5
max_key     = 0xAF5A5827FAE076D7
payload_len = 40
file size   = 140
```

Payload (40 bytes, from offset 96):

```text
  0: C5 A0 F7 E8 31 31 CC 00  91 DF 2F B7 75 88 A9 0D
 16: D8 FE 76 A3 17 F5 C5 35  E4 51 DE 8F D7 4A C6 45
 32: D7 76 E0 FA 27 58 5A AF
```

SHA-256 of the payload:
`05f9f50f4f6d5ac7b569175cfab754e1d2414f786fe67ec5d9a97962f66f2182`

### 8.3 `encoding = 1` (Elias-Fano)

`floor(log2(5)) = 2`, so `l = 61`; `H = 5 + 2^3 = 13` bits.
Low array `⌈5·61/64⌉ = 5` words = 40 bytes; high vector `⌈13/64⌉ = 1` word =
8 bytes.

```text
key_count   = 5
encoding    = 1
low_width   = 61
min_key     = 0x00CC3131E8F7A0C5
max_key     = 0xAF5A5827FAE076D7
payload_len = 48
file size   = 148
```

Payload (48 bytes, from offset 96) — the first 40 are the low array, the last 8
are the high-bits vector:

```text
  0: C5 A0 F7 E8 31 31 CC 20  F2 FB E5 B6 0E 31 B5 61
 16: FB DB 8D 5E D4 17 57 F2  28 EF C7 6B 25 E3 72 6D
 32: 07 AE 7F 82 A5 F5 00 00  2B 02 00 00 00 00 00 00
```

SHA-256 of the payload:
`1254a33e82fb1082a0c302380d0f5425fc8b3937f392ab5de90773c5a44246dc`

### 8.4 Worked high-bits trace

For the §8.1 keys at `l = 61`, so an implementer can localise a discrepancy:

| `i` | key | `high_i = key >> 61` | set bit at `high_i + i` |
|---:|---|---:|---:|
| 0 | `0x00CC3131E8F7A0C5` | 0 | 0 |
| 1 | `0x0DA98875B72FDF91` | 0 | 1 |
| 2 | `0x35C5F517A376FED8` | 1 | 3 |
| 3 | `0x45C64AD78FDE51E4` | 2 | 5 |
| 4 | `0xAF5A5827FAE076D7` | 5 | 9 |

Bits 0, 1, 3, 5, and 9 set gives the single high word
`2^0 + 2^1 + 2^3 + 2^5 + 2^9 = 555 = 0x22B`, stored little-endian as
`2B 02 00 00 00 00 00 00` — the last eight bytes of §8.3.

Decoding runs the table backwards: the `i`-th set bit at position `p` yields
`((p - i) << 61) | low_i`.

### 8.5 A larger set

The 100 IRIs `https://example.org/resource/NNN` for `NNN` = `000` through `099`
— the same toy set as [sketch-format.md](sketch-format.md) §9.2, whose 16
smallest keys are frozen there:

```text
key_count   = 100
min_key     = 0x00952D9604E1CF2C
max_key     = 0xFE83C15D566B9855

encoding = 1:  low_width = 57, payload_len = 752, file size 852
               SHA-256(payload) =
               a601e77c1f48929b7c9ecc6cc8dd47849123d0d07b42efed73b1666d502056b3
encoding = 0:  low_width =  0, payload_len = 800, file size 900
               SHA-256(payload) =
               e07ccb2322f490bd3cae2eca78c8a5d01a0d80bb715714f9475617cb66c665d0
```

### 8.6 Empty role

A role with no qualifying IRIs, at either encoding:

```text
key_count = 0, low_width = 0, min_key = 0, max_key = 0,
payload_len = 0, file size 100 (the 96-byte header and the CRC trailer)
```

The file is emitted, not omitted (§1.2, §7 producer rule 6).

## 9. Versioning

- `format_version` (offset 8) governs the **container**: field layout and
  offsets. A change that adds or moves a field increments it.
- `convention_id` (offset 10) governs the **semantics**: the term scope, the
  serialization rule, the hash, and the role definitions. Changing any of these
  produces convention `2`; it is never a revision of convention `1`, because
  artifacts either side of the change are not comparable and no reader should
  try to compare them. It is deliberately the same numbering as
  [sketch-format.md](sketch-format.md), because it identifies the same
  convention.
- `encoding` (offset 14) is **not** a version. It is a per-file choice with no
  effect on comparability: two files with different encodings hold the same kind
  of set and intersect normally.

A reader MUST reject values of `format_version`, `convention_id`, `hash_id`,
`role`, or `encoding` that it does not implement (§4.4). Reserved fields exist
so that a compatible extension can add information an old reader will refuse
rather than misread; this is why a nonzero reserved field is a rejection and not
a warning.

Adding a new `role`, a new `encoding`, or a new `hash_id` are all changes an old
reader cannot interpret — which is exactly why rules 4–6 of §4.4 require
rejection rather than tolerance, and why role `2` can exist in version 1 without
endangering a reader that only implements the published pair.

## 10. Implementation notes for hdtc (non-normative)

`hdtc keyset <hdt>` scans the source HDT's dictionary once, filtering to
qualifying IRIs and hashing each per §3. It shares that scan and that
term-to-key function with `hdtc sketch` (`src/hdt/artifacts.rs`), so there is no
new hashing and no new pass over the triples — a key set is very nearly free
given a filter build, which is the practical argument for producing both.

The shared section is read once and fanned out to every selected role; the
subject-only, object-only, and predicate sections then go to the roles that draw
from them.

**No stage holds the key set in memory, so any role builds at any size.** Each
role's keys go to an external merge sort: buffered to its share of
`--memory-limit`, sorted, and spilled to compressed chunk files, which are then
k-way merged — deduplicating as they go — into one ascending run on disk. The
encoders stream that run: raw copies it once, Elias-Fano reads it twice, once
for the low-bits array and once for the high-bits vector. Memory is therefore
`--memory-limit` regardless of the key count, and the limit is a throughput
knob, not a ceiling on what can be built. A 12.4-million-key build at a 1 MiB
limit spills 95 chunks and emits bytes identical to the same build at 4 GiB.

Because the sizing rule (§4.3) makes `payload_len` a function of `key_count`,
and `key_count` is only known once the merge has deduplicated, the merge
materializes its run before encoding rather than streaming straight into the
file. Peak temporary space is therefore about 16 bytes per distinct key — the
compressed sort chunks plus the uncompressed run — released as soon as the file
is published.

`hdtc sketch` keeps its keys resident by contrast, because binary fuse
construction peels a hypergraph over the whole key set and has no streaming
form. That is a property of filter construction, not of the key convention the
two commands share.

Artifacts are written to temporary files in the output directory and published
by atomic rename only after all of them succeed, so a failed run leaves no
partial `keysets/` directory and never overwrites an existing artifact.

hdtc opens the source HDT by path several times during a build — to scan the
dictionary layout, to digest it, and once per dictionary section. It records the
file's identity alongside the digest and rechecks it immediately before
publishing, so an HDT replaced mid-build fails the run instead of yielding an
artifact whose keys and `source_digest` describe different bytes. The digest
being advisory (§6) means it may be *stale*; it does not license it being wrong
about which bytes it covers.

`--encoding raw` exists to measure Elias-Fano against the baseline of §5 on real
data; `--roles terms` exists to measure the whole-vocabulary variant of §1.1.
Both are experiments, and the `terms` role in particular may be withdrawn.
