# HDT sketch artifact formats, version 1

Status: normative format specification for hdtc.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY**
are to be interpreted as described by RFC 2119.

## 0. Purpose and the portability guarantee

`hdtc sketch` summarises an HDT's dictionary into two kinds of small, static
file. Both answer questions about a dataset's IRIs without opening the dataset.

A **membership filter** (`.filter`) answers *might this dataset mention this
IRI?* At the 8-bit width it approaches 1.13 bytes per distinct IRI as the set
grows — 11.3 MB for 10 million IRIs, 113 MB for 100 million, with a higher ratio
on small sets where segment rounding dominates. It is probed in constant time
with three array reads, and never returns a false negative, so a negative answer
is certain. Given a
collection of datasets, one filter each is enough to narrow "who might have this
IRI?" from the whole collection to the few worth actually querying, using memory
proportional to the number of datasets rather than to their size.

An **overlap sketch** (`.minhash`) answers *how many IRIs do these two datasets
share?* Its size depends only on the configured `k`, not on the dataset — 512 KiB
at the standard `k = 65536` — and a pair of them estimates shared-IRI count and
Jaccard similarity directly, without comparing the datasets themselves. That
makes an all-pairs overlap matrix over a large collection cheap, which is
otherwise the expensive part of deciding which datasets are worth joining,
merging, or presenting together.

Both are computed from one pass over an existing HDT's dictionary, and both are
per-dataset and independent: nothing about producing them requires knowing what
other datasets exist. Comparability comes from every producer following §3, not
from coordination between producers.

**These files are designed to be produced by one party and consumed by another,
with no shared code and no shared runtime.** Everything a reader needs is in the
file or in this document. In particular the membership probe (§5.2) is specified
completely, as integer arithmetic; it does not defer to any library's source. A
conforming implementation can be written from this document alone, in any
language with 64-bit unsigned integers. hdtc's own use of the Rust `xorf` crate
is an implementation detail of *construction*, which §5.4 explains is
deliberately not part of the format.

## 1. Artifacts and data model

A complete sketch of one HDT is **four independent files**, conventionally in a
`filters/` directory beside it:

```text
filters/
  subjects.filter     membership filter over distinct subject IRIs
  objects.filter      membership filter over distinct object IRIs
  subjects.minhash    bottom-k overlap sketch over distinct subject IRIs
  objects.minhash     bottom-k overlap sketch over distinct object IRIs
```

Each file is self-describing and independently readable. They are deliberately
**not** packed into one container, because consumers want different subsets and
keep them for different lengths of time: a membership index memory-maps the two
`.filter` files and probes them for the life of the process, while an overlap
matrix reads the two `.minhash` files once and then keeps only the derived
numbers. Packing would force every consumer to fetch bytes it does not use.
Per-file integrity comes from the CRC trailer, so nothing is lost by keeping
them apart.

This is the opposite trade-off from the HDT graphs sidecar
([graphs-sidecar-format.md](graphs-sidecar-format.md)), which packs its parts
into one file because they are meaningless apart and always used together.

### 1.1 Roles

The two roles are drawn from the source HDT's four-section dictionary:

- **subjects** (role id `0`) = qualifying terms in `Shared ∪ Subjects`
- **objects** (role id `1`) = qualifying terms in `Shared ∪ Objects`

A term in the Shared section is both a subject and an object, and is therefore
present in **both** roles. This is intended: the roles are compared
role-to-role, and role-specific overlap is the question being asked.

Predicates have no role. A dataset's predicate set is small and enumerable, and
VoID property partitions describe it exactly; an approximate structure over it
would be strictly worse than the exact one.

### 1.2 Qualifying terms

A dictionary term qualifies iff it is an **absolute IRI**. Excluded:

- **Literals** — not identity-bearing across datasets. In HDT's dictionary a
  literal is recognised by a leading `"` (U+0022).
- **Blank nodes** — dataset-local by construction, so they can never produce a
  true cross-dataset match. In HDT's dictionary a blank node is recognised by a
  leading `_:`.
- **Skolem IRIs** — blank nodes that were replaced by IRIs at build time. These
  are dataset-local for the same reason and MUST be excluded when the producer
  applies a documented skolem prefix. A producer that does not skolemize has
  nothing to exclude on this rule.

A role whose qualifying set is empty is **not** an error. It produces a
well-formed file with `key_count = 0` (§5.3, §6.2).

### 1.3 Distinctness

The key set of a role is a set of distinct `u64` keys. Because HDT dictionary
sections are already distinct and mutually disjoint, distinct *terms* are
guaranteed by the source. Distinct *keys* are not: a 64-bit hash over a very
large dictionary can collide (§3.3). Producers MUST deduplicate keys before
construction; see §5.4.

## 2. Primitive conventions

- All integers are **unsigned** and **little-endian**.
- Offsets are absolute byte offsets from the start of the file.
- `u8`, `u16`, `u32`, `u64` denote 1, 2, 4, and 8 byte unsigned integers.
- All arithmetic in this document is on unsigned integers of the stated width
  and **wraps modulo 2^width** unless stated otherwise. `>>` is a logical
  (zero-filling) shift. `^` is bitwise XOR, `&` bitwise AND.
- Fields named `reserved` MUST be written as zero, and a reader MUST reject a
  file in which any reserved field is nonzero. This is what keeps a future
  version's added fields from being silently ignored by an old reader.
- `CRC32C` means Castagnoli CRC-32C, polynomial `0x1EDC6F41` in normal notation
  (reversed form `0x82F63B78`, equivalent to `CRC_32_ISCSI`), initial value
  `0xFFFFFFFF`, reflected input and output, final XOR `0xFFFFFFFF`. It is stored
  as a little-endian `u32`.

Every file in this specification has the shape:

```text
[56-byte common header] [type-specific body] [4-byte CRC32C trailer]
```

The CRC32C is computed over **every byte from offset 0 up to but not including
the CRC field itself**, and there MUST be no bytes after it. A reader therefore
knows the file is complete and intact before interpreting any of it.

## 3. Key derivation (term → `u64`)

Everything else in this document is a container around the key derived here, and
this is the one section where a disagreement between implementations is both
fatal and silent: artifacts built with a different key derivation are not
comparable, but nothing detects that. Filters simply report members as absent
and overlaps come out near zero. Implement this section exactly.

### 3.1 Term bytes

The bytes fed to the hash are the **expanded absolute IRI**, as its exact
Unicode codepoints encoded in UTF-8, with **no normalization of any kind**:

- no prefix or CURIE abbreviation (prefix maps are per-dataset),
- no case folding of scheme, host, or path,
- no percent-encoding or percent-decoding normalization,
- no Unicode normalization (NFC, NFD, NFKC, NFKD),
- no trailing-slash, dot-segment, or default-port normalization,
- no IDN/punycode conversion.

This is RDF IRI equality, which is codepoint-exact. Whatever form the source
data uses is what is hashed.

This rule is easy to violate by accident, because several languages and XML/RDF
toolchains normalize strings on the way in or out. Two illustrations, both
frozen as conformance vectors in §9.1:

| Term | UTF-8 bytes | Key |
|---|---:|---|
| `https://example.org/a%20b` | 25 | `0x839FF1BDBEC20449` |
| `https://example.org/a b` | 23 | `0x3003D5437A432CC8` |

and, for the same visible string `https://example.org/é` in the two Unicode
normal forms:

| Form | UTF-8 bytes | Key |
|---|---:|---|
| NFC (`é` = U+00E9) | 22 | `0x059CB2E7093FE708` |
| NFD (`e` + U+0301) | 23 | `0xF7505861366CA3A5` |

These are different terms and MUST produce different keys. An implementation
that silently normalizes will appear to work in isolation and fail only when
compared against another party's artifacts.

### 3.2 Hash function

`hash_id = 1` denotes **XXH64** (the 64-bit xxHash algorithm) with **seed 0**,
over the UTF-8 bytes of §3.1, with the full 64-bit result used directly as the
key.

XXH64 is specified rather than a faster modern variant because byte-identical,
independently tested implementations exist in Rust, C, C++, Python, Java, Go,
JavaScript, C#, and others — a hard requirement for a portable standard. The
algorithm is defined by its reference implementation and the xxHash
specification; this document does not restate it, but §9.1 provides vectors
sufficient to confirm a given implementation is the right one, starting with:

```text
XXH64("", seed 0) = 0xEF46DB3751D8E999
```

An implementation that does not reproduce this anchor is not XXH64 and MUST NOT
emit `hash_id = 1`.

### 3.3 Key collisions

Distinct IRIs can hash to the same `u64`. At one billion distinct IRIs in a role
the probability of at least one collision is roughly 2.7%; at the structural
ceiling of §5.3 it is roughly 43%. A collision is not a correctness failure of
the format — it merely means two IRIs are indistinguishable to these artifacts,
which slightly overstates overlap and understates cardinality. Both effects are
far below the sampling error of the estimators in §6.3.

Producers MUST deduplicate the key set before filter construction (§5.4), and
MUST NOT emit duplicate values in `minima` (§6.2). Producers SHOULD report the
number of collisions observed. `key_count` is the number of **distinct
qualifying IRIs**, which may therefore exceed the number of distinct keys; see
§5.3 for why this does not affect validation.

## 4. Common file envelope

All four files begin with this 56-byte header.

| Offset | Size | Type | Field | Value or meaning |
|---:|---:|---|---|---|
| 0 | 4 | `char[4]` | `magic` | `"KGFF"` (`0x4B 0x47 0x46 0x46`) for a filter, `"KGFM"` (`0x4B 0x47 0x46 0x4D`) for a sketch |
| 4 | 2 | `u16` | `format_version` | container format version; `1` |
| 6 | 2 | `u16` | `convention_id` | semantic convention; `1` = the convention of §1.1–§1.3 and §3 |
| 8 | 1 | `u8` | `hash_id` | `1` = XXH64 seed 0 (§3.2) |
| 9 | 1 | `u8` | `role` | `0` = subjects, `1` = objects |
| 10 | 6 | `u8[6]` | `reserved` | MUST be zero |
| 16 | 8 | `u64` | `key_count` | number of distinct qualifying IRIs in this role |
| 24 | 32 | `u8[32]` | `source_digest` | SHA-256 binding to the source HDT (§7) |

Then the type-specific body (§5.1 or §6.2), then the CRC32C trailer.

The `magic` values are opaque four-byte constants; only their exact bytes
matter.

`convention_id` identifies the **semantics** behind the keys — which terms
qualify (§1.2), how a role is defined (§1.1), and how a term becomes a key (§3).
Convention `1` is the one this document specifies. It is separate from
`format_version` because two files can share a byte layout and still be
incomparable, which is a far more dangerous kind of mismatch (§10).

Two files are **comparable** iff they have the same `convention_id` and the same
`hash_id`. Nothing else affects comparability — not `format_version`, not
`variant`, not `k`, and in particular not `source_digest`.

A reader MUST reject a file whose `magic` is not one of the two values, whose
`format_version` it does not implement, whose `convention_id` it does not
implement, whose `hash_id` it does not implement, or whose `role` is not `0` or
`1`. Rejecting is required rather than ignoring: silently mixing conventions
produces plausible-looking but meaningless cross-dataset numbers, which is worse
than an error.

## 5. Membership filter file (`.filter`)

`magic = "KGFF"`. The file answers one question: *might this dataset mention
this IRI?* It has **no false negatives** — a `false` result means the IRI is
definitely absent from the role. A `true` result means probably present, with
false-positive probability approximately `2^-variant`.

### 5.1 Body layout

| Offset | Size | Type | Field | Notes |
|---:|---:|---|---|---|
| 56 | 1 | `u8` | `variant` | fingerprint width in bits: `8` or `16` |
| 57 | 7 | `u8[7]` | `reserved` | MUST be zero |
| 64 | 8 | `u64` | `seed` | filter construction seed (§5.2 note) |
| 72 | 4 | `u32` | `segment_length` | power of two in `[4, 262144]` |
| 76 | 4 | `u32` | `segment_length_mask` | `segment_length - 1` |
| 80 | 4 | `u32` | `segment_count_length` | `segment_count × segment_length` |
| 84 | 4 | `u32` | `reserved` | MUST be zero |
| 88 | 8 | `u64` | `fingerprint_len` | number of fingerprint **entries**, not bytes |
| 96 | `fingerprint_len × variant/8` | `u8[]` or `u16[]` LE | `fingerprints` | fingerprint array |

The fingerprint array begins at offset 96, which is a multiple of 32. Both
`variant` widths are therefore naturally aligned, and a consumer MAY memory-map
the file and reference the array in place with no copy and no unaligned access.

`fingerprint_len` counts entries. The array occupies `fingerprint_len` bytes when
`variant = 8` and `2 × fingerprint_len` bytes when `variant = 16`. Total file
size is `96 + fingerprint_len × variant/8 + 4`.

Filters are probed independently per dataset and are never merged or composed,
so `variant` MAY differ freely between datasets and between roles. Only the §3
key derivation must agree.

### 5.2 Probe algorithm (normative)

This is the binary fuse filter of Graf & Lemire (2022). It is specified here
completely so that no implementer needs to read a particular library's source.

All values are unsigned; `u64` arithmetic wraps modulo 2^64 and `u32` arithmetic
wraps modulo 2^32.

**Step 0 — the finalizer.** `mix64` is the MurmurHash3 64-bit finalizer:

```text
function mix64(k: u64) -> u64:
    k = k ^ (k >> 33)
    k = k * 0xFF51AFD7ED558CCD        # mod 2^64
    k = k ^ (k >> 33)
    k = k * 0xC4CEB9FE1A85EC53        # mod 2^64
    k = k ^ (k >> 33)
    return k
```

**Steps 1–5 — the probe.**

```text
function contains(key: u64) -> bool:
    # 1. avalanche the key with the filter's construction seed
    h = mix64(key + seed)                                  # + is mod 2^64

    # 2. the expected fingerprint, truncated to the variant width
    f = (h ^ (h >> 32)) mod 2^variant

    # 3. three fingerprint indices
    hi = (h * segment_count_length) >> 64                  # high 64 bits of the
                                                           # 128-bit product
    h0 = hi mod 2^32
    h1 = (h0 + segment_length) mod 2^32
    h2 = (h1 + segment_length) mod 2^32
    h1 = h1 ^ (((h >> 18) mod 2^32) & segment_length_mask)
    h2 = h2 ^ ((h mod 2^32) & segment_length_mask)

    # 4. fold the three stored fingerprints in
    f = f ^ fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]

    # 5. a member folds to zero
    return f == 0
```

Notes for implementers:

- Step 3's `hi` is the high half of a 128-bit product ("Lemire fast-range"), a
  multiply-and-shift alternative to a modulo. In a language without a 128-bit
  type, compute it from 32-bit halves, or use a widening-multiply intrinsic. It
  is **not** `(h * segment_count_length) >> 64` evaluated in 64-bit arithmetic,
  which would always be zero.
- Because `segment_count_length < 2^32`, `hi` is always less than `2^32`, so the
  `mod 2^32` on `h0` never discards information. It is written for clarity.
- Step 2 truncates to the low `variant` bits: `& 0xFF` for `variant = 8`,
  `& 0xFFFF` for `variant = 16`.
- **Two different seeds exist and MUST NOT be conflated.** The term-hash seed is
  `0` and is used in §3.2 to turn an IRI into a key. The `seed` field at offset
  64 is the filter's internal construction seed, an output of the build, and is
  used only in step 1 of this probe.
- The probe reads exactly three entries and performs no allocation. It is
  branch-free apart from the final comparison, which is why probing is
  microseconds even for a memory-mapped multi-gigabyte filter.

§5.3 guarantees `h0`, `h1`, and `h2` are all less than `fingerprint_len`, so a
reader that has validated the header need not bounds-check inside the probe.

An empty filter (`key_count = 0`) is a normal filter with an all-zero
fingerprint array. It does **not** short-circuit to `false`: a key whose step-2
fingerprint is zero folds to zero and returns `true`. Its false-positive rate is
therefore the nominal `2^-variant`, not zero. Readers MUST NOT special-case it,
and producers MUST NOT omit it.

### 5.3 Structural invariants and limits

Let `segment_count = segment_count_length / segment_length`. A reader MUST
verify **all** of the following before probing, and MUST reject the file if any
fails:

1. `variant` is `8` or `16`.
2. `segment_length` is a power of two, and `4 ≤ segment_length ≤ 262144`.
3. `segment_length_mask == segment_length - 1`.
4. `segment_count_length mod segment_length == 0` and `segment_count ≥ 1`.
5. `fingerprint_len == segment_count_length + 2 × segment_length`.
6. `fingerprint_len ≤ 4294967295` (`2^32 - 1`).
7. The file length is exactly `96 + fingerprint_len × variant/8 + 4`.
8. The CRC32C trailer matches the preceding bytes.

Invariant 5 is what makes the probe memory-safe, and it is the reason a reader
can skip bounds checks in the hot path. Proof: `h0 < segment_count_length` by
step 3; `h1` before its XOR is `h0 + segment_length`, and the XOR alters only
the low `log2(segment_length)` bits, which cannot move it out of its
`segment_length`-aligned block, so `h1 < segment_count_length + segment_length`;
by the same argument `h2 < segment_count_length + 2 × segment_length`, which by
invariant 5 is `fingerprint_len`.

**This validation is mandatory, not advisory.** These files cross trust
boundaries — the whole point is that any party can consume any other party's
artifacts. A file with an inflated `segment_count_length` and a short
fingerprint array is a straightforward out-of-bounds read in an implementation
that trusts the header, and in a memory-mapped reader that is an out-of-bounds
read against mapped memory.

**Key ceiling.** Invariant 6 bounds how many keys a filter can hold. Under the
reference sizing of §5.4, `segment_count_length + 2 × segment_length` first
exceeds `2^32 - 1` at **3,817,515,692** keys, so a producer using that sizing
MUST refuse a role with more than **3,817,515,691** distinct keys. A producer
that reaches this limit should split the dataset rather than widen the field:
the limit is inherent to 32-bit indexing in the published algorithm, not to this
container.

`key_count` counts distinct qualifying IRIs, whereas the array is sized from the
distinct *key* count, which may be smaller after collision deduplication (§3.3).
Readers therefore MUST NOT attempt to re-derive `fingerprint_len` from
`key_count`; invariant 5 is the only relationship that holds.

### 5.4 Construction (non-normative, with one normative consequence)

**Construction is deliberately not specified.** A `.filter` file is conforming
iff it satisfies §5.3 and the §5.2 probe returns `true` for every key in the
role's key set. Any procedure that produces such an array conforms — a different
peeling implementation, a different retry schedule, a different random seed
sequence, or a future faster algorithm are all free to differ, and their outputs
remain readable by every conforming reader.

That is the property that makes this format safe for a federated system: two
parties must agree on how to *read* a filter, and need not agree on how to
*write* one.

For reference, the standard construction (Graf & Lemire 2022) sizes the array as
follows. With `arity = 3` and `n` distinct keys:

```text
segment_length      = min(2^floor(log(n) / log(3.33) + 2.25), 262144)   # 4 if n = 0
size_factor         = max(1.125, 0.875 + 0.25 × log(1000000) / log(n))
capacity            = round(n × size_factor)                            # 0 if n ≤ 1
init_segment_count  = ceil(capacity / segment_length)
segment_count       = max(1, init_segment_count - 2)                    # see note
segment_count_length = segment_count × segment_length
fingerprint_len     = segment_count_length + 2 × segment_length
```

(The `segment_count` step is `proposed - (arity - 1)` where
`proposed = init_segment_count`, floored at 1.) The build then assigns each key
to a 3-hyperedge via §5.2 step 3, peels the hypergraph to find an ordering, and
back-fills fingerprints in reverse peel order so that each key's three entries
XOR to its fingerprint. Construction can fail and is retried with a fresh seed;
the seed that succeeded is what is written to offset 64.

The peel requires the key set resident, roughly 8 bytes per key plus scratch
arrays; producers should budget for that rather than assume streaming.

Duplicate keys break the peel — the algorithm is defined only for distinct keys,
and a duplicate can send it into a long retry loop before failing. Producers
MUST deduplicate (sorting the key array and removing equal neighbours is
sufficient and costs one pass).

### 5.5 What hdtc pins, and why

hdtc builds filters with the Rust `xorf` crate at an exact version. That pin is
**not** part of this format and readers MUST NOT infer anything from it. It
exists for one reason: the §9 conformance vectors record the exact bytes hdtc
emits for a fixed input, and a construction change would alter those bytes while
leaving every file perfectly valid. Re-pinning the crate therefore requires
regenerating the §9.3 filter vector — and nothing else.

## 6. Overlap sketch file (`.minhash`)

`magic = "KGFM"`. The file answers: *how many IRIs do two datasets share?*

### 6.1 Definition

A bottom-k (k-minimum-values) MinHash: hash every qualifying IRI in the role and
retain the **k smallest distinct key values**. If the role has fewer than `k`
distinct keys, all of them are retained and the sketch is **exact** — every
estimator in §6.3 then returns a true value, not an estimate.

### 6.2 Body layout

| Offset | Size | Type | Field | Notes |
|---:|---:|---|---|---|
| 56 | 4 | `u32` | `k` | configured capacity; `65536` standard, `≥ 2` |
| 60 | 4 | `u32` | `stored_count` | number of minima that follow |
| 64 | 1 | `u8` | `saturated` | `1` if `key_count ≥ k`, else `0` |
| 65 | 7 | `u8[7]` | `reserved` | MUST be zero |
| 72 | `stored_count × 8` | `u64[]` LE | `minima` | key values, ascending, distinct |

Total file size is `72 + stored_count × 8 + 4`.

A reader MUST verify:

1. `k ≥ 2`.
2. `stored_count ≤ k`.
3. `stored_count == min(k, key_count)`, unless key collisions were reported by
   the producer, in which case `stored_count ≤ min(k, key_count)`.
4. `saturated == 1` iff `key_count ≥ k`.
5. `minima` is strictly ascending — which enforces both sorted order and
   distinctness in one pass.
6. The file length is exactly `72 + stored_count × 8 + 4`.
7. The CRC32C trailer matches the preceding bytes.

Strict ascent is required rather than merely recommended: it makes the k-th
smallest value `minima[stored_count - 1]` without a scan, makes union merges
linear, and makes the §6.3 estimators well defined.

`k` MAY differ between datasets. A conforming reader supports any `k ≥ 2`;
`65536` is the standard emission (512 KiB per saturated role, independent of
dataset size).

### 6.3 Estimators

A sketch is only meaningful together with the formulas used to read it, so they
are specified here. These are the standard bottom-k (k-minimum-values)
estimators; they are stated in full so that two consumers of the same pair of
files report the same numbers.

Let `φ(v) = v / 2^64` map a key into `[0, 1)`.

**Cardinality of one role.**

```text
if saturated == 0:  n̂ = stored_count                      # exact
else:               n̂ = (k - 1) / φ(minima[k - 1])         # KMV estimator
```

**Union, Jaccard, and intersection of two sketches A and B.** Comparison
truncates both to the smaller capacity, which is what allows datasets published
with different `k` to be compared:

```text
k*  = min(k_A, k_B)
U   = dedup(minima_A ∪ minima_B)             # ascending
S∪  = the min(k*, |U|) smallest values of U   # bottom-k* sketch of A ∪ B
D   = |S∪|

n̂∪  = D  if D < k*  else  (k* - 1) / φ(S∪[k* - 1])

y   = |{ v ∈ S∪ : v ∈ minima_A and v ∈ minima_B }|
Ĵ   = y / D

|A ∩ B| ≈ Ĵ × n̂∪
```

Both reduce to exact values when the inputs are unsaturated, so small datasets
get exact answers automatically.

Consumers SHOULD report the absolute estimated intersection alongside `Ĵ`: a
small Jaccard over a large union can still be a large and useful shared count.

Consumers MUST NOT estimate intersection by inclusion–exclusion
(`|A| + |B| - |A ∪ B|`) over cardinality estimates. For small overlaps that
subtracts two large noisy quantities to recover a small one, and the result is
dominated by error. Bottom-k estimates the overlap ratio directly, which is why
it is the primitive here.

## 7. Binding to the source HDT

`source_digest` is the SHA-256 of the source HDT's **Dictionary-and-Triples
suffix**: the byte range beginning at the Dictionary Control Information and
continuing to end of file. The HDT Global and Header sections are excluded.

This is the same identity range used by the graphs sidecar
([graphs-sidecar-format.md](graphs-sidecar-format.md) §10), and the reason is
the same: `hdtc header` rewrites an HDT's metadata while copying the Dictionary
and Triples bytes verbatim, so a header-only edit leaves these artifacts
perfectly valid. Digesting the whole file would invalidate them for no reason.

To compute it, a reader parses the HDT's Global control information and Header
control information, skips the header payload of the declared length, and hashes
from that offset to end of file.

`source_digest` is **advisory**. It detects staleness and records provenance. It
MUST NOT gate comparability (§4), and a reader comparing two datasets MUST NOT
require their digests to relate in any way. Any change to Dictionary or Triples
bytes changes the digest even when the resulting key sets are identical, so a
mismatch means "rebuilt", not necessarily "different".

A packaging layer that distributes these files alongside the HDT — a manifest, a
checksum list, a signature — is the appropriate place for integrity that must
cover the complete HDT file including its metadata. `source_digest` deliberately
does less than that, so that it survives header edits.

## 8. Conformance and validation

A conforming **producer** MUST:

1. Emit all four files for a dataset, or none of them. A consumer that finds one
   role may reasonably expect the other.
2. Derive keys exactly per §3, including the no-normalization rule.
3. Deduplicate keys before construction (§3.3).
4. Write zero to every reserved field.
5. Emit `minima` strictly ascending.
6. Emit a well-formed file for an empty role rather than omitting it.
7. Refuse a role exceeding the §5.3 key ceiling rather than emitting a filter
   with wrapped sizing.

A conforming **reader** MUST:

1. Verify the CRC32C before interpreting any field.
2. Reject unknown `magic`, `format_version`, `convention_id`, `hash_id`, or
   `role`, and reject any nonzero reserved field.
3. Verify all §5.3 invariants before probing a filter, and all §6.2 invariants
   before using a sketch.
4. Treat `source_digest` as advisory only.
5. Never compare artifacts across differing `convention_id` or `hash_id`.

A reader MAY memory-map a `.filter` and probe it in place; §5.1 guarantees
alignment and §5.3 guarantees index safety, provided validation ran first.

## 9. Conformance vectors (frozen)

A portable format needs canonical values that any implementation can check
itself against. These are frozen: an implementation that does not reproduce
them, subject to §9.3's note on construction, is non-conforming. They were
generated by hdtc and independently reproduced by a from-scratch implementation
written against this document alone.

### 9.1 Term → key

XXH64, seed 0, over the UTF-8 bytes of the term exactly as written.

| Term | UTF-8 bytes | Key |
|---|---:|---|
| *(empty string)* | 0 | `0xEF46DB3751D8E999` |
| `http://example.org/Alice` | 24 | `0x9A609FB40498CF38` |
| `http://example.org/` | 19 | `0x5CA3AF779DB3B833` |
| `https://example.org/a%20b` | 25 | `0x839FF1BDBEC20449` |
| `https://example.org/a b` | 23 | `0x3003D5437A432CC8` |
| `https://example.org/path?q=1&r=2#frag` | 37 | `0x847426C74DD51315` |
| `https://例え.テスト/é/雪` | 31 | `0x7725C712FF131331` |
| `https://example.org/Ünicode/Ärger` | 35 | `0xC6389897BA4327DA` |
| `urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66` | 45 | `0xD2958FDBF4678E1B` |
| `urn:example:long:` + `a`×48 | 65 | `0xC5236D32CB6297AA` |

The two non-ASCII terms are given as UTF-8 byte counts as well as text so that an
implementation can confirm it is hashing the bytes this document intends. The
`é` in `https://例え.テスト/é/雪` is NFC (U+00E9). See also the NFC/NFD pair in
§3.1.

### 9.2 MinHash

**Toy set (saturated).** The 100 IRIs `https://example.org/resource/NNN` for
`NNN` = `000` through `099`, zero-padded to three digits, at `k = 16`:

```text
key_count    = 100
k            = 16
stored_count = 16
saturated    = 1
minima       = 0x00952D9604E1CF2C  0x00CC3131E8F7A0C5  0x01F352D0F9D5CF80
               0x06BF0ECF32E0B062  0x07D9A683F85C7CB8  0x07FA5DA3A0952933
               0x0A724817BE71EE72  0x0DA98875B72FDF91  0x0F98C196C842182A
               0x0FD0E9CD6B8AA629  0x12C19725EA52303F  0x146E55F2B679E888
               0x15275B3706D1E3AF  0x15FF25A10102039D  0x1940B18C45C1E8A5
               0x19676957CB8C7933
```

Total file size 204 bytes.

**Small set (exact).** The first 5 IRIs of the same family (`000`–`004`), at
`k = 16`:

```text
key_count    = 5
k            = 16
stored_count = 5
saturated    = 0
minima       = 0x00CC3131E8F7A0C5  0x0DA98875B72FDF91  0x35C5F517A376FED8
               0x45C64AD78FDE51E4  0xAF5A5827FAE076D7
```

Total file size 116 bytes.

### 9.3 Filter

The toy set of §9.2 (100 IRIs), `variant = 8`, as built by hdtc:

```text
key_count            = 100
variant              = 8
seed                 = 0x910A2DEC89025CC1
segment_length       = 64
segment_length_mask  = 63
segment_count_length = 64
fingerprint_len      = 192
SHA-256(fingerprints)= 4684db4089f6c89f7609e1cd00d8246e02835f64a1377ca14cc63ee491df9960
```

Total file size 292 bytes. Note `192 = 64 + 2 × 64`, satisfying §5.3 invariant 5.

Because construction is not normative (§5.4), an independent implementation is
**not** required to reproduce `seed` or the fingerprint digest. It is required
to reproduce the *behaviour*: for a filter it builds itself, the §5.2 probe must
return `true` for all 100 member keys; and given the reference bytes above, its
probe must agree with the trace in §9.4 and return `true` for all 100 members.

### 9.4 Worked probe trace

Against the §9.3 filter, for one member and one non-member. Every intermediate
value is given so an implementer can localise a discrepancy.

Member — `https://example.org/resource/000`:

```text
key (XXH64)          = 0x45C64AD78FDE51E4
h = mix64(key+seed)  = 0xDD072E4B6B05A8A7
f = (h ^ (h>>32))&FF = 0xEC
h0, h1, h2           = 55, 118, 144
fingerprints[h0..]   = 0x39, 0x28, 0xFD
f after fold         = 0x00   -> contains = true
```

Non-member — `https://example.org/absent`:

```text
key (XXH64)          = 0xD5ECB43F977B8759
h = mix64(key+seed)  = 0x4439E9B4D1716E65
f = (h ^ (h>>32))&FF = 0xD1
h0, h1, h2           = 17, 77, 180
fingerprints[h0..]   = 0x00, 0x46, 0xDB
f after fold         = 0x4C   -> contains = false
```

### 9.5 Empty role

A role with no qualifying IRIs, at `k = 16`, `variant = 8`:

```text
.minhash: key_count = 0, stored_count = 0, saturated = 0, file size 76 bytes
.filter:  key_count = 0, segment_length = 4, segment_length_mask = 3,
          segment_count_length = 4, fingerprint_len = 12, all fingerprints zero,
          file size 112 bytes
```

Again `12 = 4 + 2 × 4`. Per §5.2 this filter returns `true` for approximately
1 in 256 probed keys; that is correct behaviour, not a defect.

## 10. Versioning

- `format_version` (offset 4) governs the **container**: field layout and
  offsets. A change that adds or moves a field increments it.
- `convention_id` (offset 6) governs the **semantics**: the term scope, the
  serialization rule, the hash, and the role definitions. Changing any of these
  produces convention `2`; it is never a revision of convention `1`, because
  artifacts either side of the change are not comparable and no reader should
  try to compare them.

A reader MUST reject values of either field it does not implement (§4). Reserved
fields exist so that a compatible extension can add information that an old
reader will refuse rather than misread; this is why nonzero reserved fields are
a rejection and not a warning.

Adding a new `variant`, a new `role`, or a new `hash_id` are all
`convention_id`-visible changes in effect, since an old reader cannot interpret
them — such additions MUST bump the field that makes an old reader reject.

## 11. Implementation notes for hdtc (non-normative)

`hdtc sketch <hdt>` scans the source HDT's dictionary once, filtering to
qualifying IRIs (§1.2) and hashing each per §3. The shared section is read once
and fanned out to both roles.

Each role's keys are spooled to an uncompressed temporary file (8 bytes per
qualifying IRI) so the two roles need not be resident together; the bottom-k
sketch rides the same scan in a bounded ordered set. Filter construction then
reads one role's keys back, sorts and deduplicates them, and peels.

`--memory-limit` bounds both phases: the combined bottom-k state is checked
before the scan, and each role's key count is checked against the largest set
whose peel fits the remaining budget as the scan proceeds — so an oversized
input is refused while being read, rather than after a full scan.

Artifacts are written to temporary files in the output directory and published
by atomic rename only after all of them succeed, so a failed run leaves no
partial `filters/` directory and never overwrites an existing artifact.
