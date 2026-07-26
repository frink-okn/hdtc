# HDT text index format, version 1

`hdtc text` publishes a full-text index over the literals of an HDT dictionary,
and `hdtc search --text` queries it. This document is normative for what that
artifact contains and what a consumer may rely on. Where this document and the
hdtc implementation disagree, this document is the specification and the
implementation is in error — with the one large exception §1.1 sets out.

The design comes from the KGF design docs, doc 19 ("Literals: text search and
label resolution"). Where this document restates a rationale from there it is
for the reader's benefit; doc 19 governs the *design*, this document governs the
*artifact*.

## 0. Purpose

Given an HDT file, answer "which resources are named, described or otherwise
labelled by a string like this?" in bounded time, without the caller enumerating
literals.

That is the operation an agent reaches for first when it holds a name and needs
an IRI, and it is the capability with the largest gap between an HDT server and
plain triple-pattern access: pattern enumeration alone offers no way to find
"atrazine" except paging every literal in the dataset.

## 1. Relationship to the other hdtc artifacts

`hdtc` publishes several artifacts derived from an HDT dictionary — membership
filters and overlap sketches (`docs/sketch-format.md`), exact key sets
(`docs/keyset-format.md`), the graph sidecar (`docs/graphs-sidecar-format.md`).
The text index joins them in purpose and in publication discipline: built from
one pass over the dictionary, written to a staging location and renamed into
place, bound to its source by digest, refusing to overwrite an existing
artifact.

It differs from them in one important way.

### 1.1 The bytes are Tantivy's, not hdtc's — and what that costs

The other artifacts specify their bytes exhaustively, so that a reader in any
language can be written from the document alone. This one does not. The segment
files are written and read by [Tantivy](https://github.com/quickwit-oss/tantivy)
at the exact version §4 pins, and this document specifies the *convention around
them*: what a document is, what fields it carries, how a literal becomes text,
which literals are omitted, and what the manifest records.

This is a deliberate trade, and the cost is real: **a text index published by
hdtc is, in practice, readable only by a program that links the same Tantivy
release.** A conforming reader cannot be written from this document the way one
can for `.filter` or `.keys`.

It was accepted because the alternative is worse in the specific case. A binary
fuse probe is thirty lines of normative text; a Lucene-class inverted index is
not, and specifying one exhaustively would buy a cross-language guarantee at the
price of a hand-written term dictionary, posting lists and scorer — and would
still not deliver bounded-cost fuzzy matching, which needs an automaton
intersected with an FST term dictionary. `GET /search` (KGF doc 03 §3.4.5)
requires `match_kind ∈ exact | normalized | prefix | fuzzy`, so that capability
is in the contract, not optional.

**Migration path.** If a non-Rust consumer appears, the exit is not a rewrite of
this format but an export: every document is `(object_id, text, lang)`, all
three recoverable from the index, so a dump-and-rebuild into another engine is
mechanical. Nothing in §2, §3 or §4 depends on Tantivy; only §5 and §6 do.

## 2. Data model

### 2.1 The document is a distinct literal

A document is **one distinct literal from the HDT dictionary's object section**,
and its identity is that literal's **object dictionary ID**. One document per
distinct string, not one per triple.

The consequence that makes this the right unit: **the index stores no subject
and no predicate at all.** A hit is an object ID; `? ? <that object>` through the
HDT-FoQ index (`.hdt.index.v1-1`) turns it into every `(subject, predicate)` that
uses it. Therefore:

- Index size scales with *distinct literals*, not with triples. In
  annotation-heavy datasets, shared type labels, boilerplate definitions and
  repeated units mean occurrences exceed distinct values by a large factor.
  Indexing every literal is affordable precisely because the unit changed.
- Ranking is blind to how often a literal is *used*. A string on one triple
  scores like one on ten thousand. Popularity is a post-hoc signal, available
  from HDT at resolution time, rather than something baked into the index.
- Intersecting a text result with a pattern enumeration is an operation on HDT
  IDs, with no string materialization in between.

§8 states what this costs.

### 2.2 Object IDs

HDT's object ID space is: IDs `1..=shared_count` address the shared section, and
IDs `shared_count + n` address the *n*-th entry of the object-only section. A
text index's documents carry IDs from that space, and a consumer resolves one by
the ordinary HDT dictionary lookup.

Only the object-only section is scanned. A literal can never be an RDF subject,
so it is never in the shared section, and a conforming builder need not read it.

### 2.3 Language

Every document carries a language value: the literal's language tag, normalized
to lower case, or `und` (BCP 47's registered "undetermined") when the literal
carries no tag.

Untagged is a *value*, not an absence, because it has to be selectable. Doc 19
§19.4.2 ranks an untagged literal above a wrong-language one: `@de` positively
asserts a language the client did not ask for, whereas an untagged literal
asserts nothing and is frequently language-neutral by nature — a chemical name,
a gene symbol, a species binomial, an accession, a product code. Those are
exactly the strings a cross-language client most wants, and a language filter
that dropped them would hide them.

A literal whose actual language tag is `und` is indistinguishable from an
untagged one. This is accepted: the two make the same claim.

## 3. The analyzer (`analyzer_id = 1`)

Every index records an `analyzer_id`. It asserts every rule in this section. A
reader whose analyzer ID differs from an index's **must refuse the index**
rather than query it: a query tokenized by one convention and terms indexed by
another fail to match silently, which is the worst available outcome.

Changing any rule below is a new `analyzer_id`, never a patch to this one.

### 3.1 Splitting a literal

A dictionary term is a literal if and only if its first byte is `"`. Such a term
is split into:

- **value** — the raw lexical form between the opening quote and the closing
  quote, as HDT stores it (unescaped, not N-Triples-escaped);
- **language** — present when the closing quote is followed by `@`, and is the
  rest of the term;
- **datatype** — present when the closing quote is followed by `^^<` and the
  term ends with `>`, and is the IRI between them.

A literal has at most one of language and datatype. Quotes inside the value do
not terminate it; the closing quote is found from the end of the term.

### 3.2 Tokenization

Every indexed literal is analyzed into **three** forms: the plain tokens, the
stemmed tokens (§3.6), and — when short enough — a single whole-literal key
(§3.7). Each goes to its own field (§5.1). This section describes the plain
chain, which is also what a query string is tokenized by, and which the other
two are derived from.

The value is tokenized by, in order:

1. **Split** into maximal runs of characters for which Unicode
   `Alphabetic ∪ Nd/Nl/No` holds (Rust's `char::is_alphanumeric`, Tantivy's
   `SimpleTokenizer`). Everything else — whitespace, punctuation, symbols,
   combining marks — separates tokens and is discarded.
2. **Drop** any token longer than **128 bytes**. Dropped, not truncated:
   truncating would make a long sequence match the first 128 characters of an
   unrelated one. A token at exactly 128 bytes is kept.
3. **Lowercase** by Unicode simple lowercasing (`char::to_lowercase`). The
   length cap in step 2 applies to the pre-folding form.

Nothing else in the plain chain. **No stopwords, no ASCII folding, no Unicode
normalization** — each discards information a client cannot recover, and unlike
stemming none of them can be separated into their own field and ranked below the
literal truth.

Query strings are tokenized by exactly the same chain. That symmetry is not an
optimization — it is what makes a query token comparable to an indexed one.

Two consequences worth stating plainly:

- `1,3-dichlorobenzene` indexes as `1`, `3`, `dichlorobenzene`.
- A language whose script does not delimit words (Chinese, Japanese, Thai)
  produces one token per unbroken run. Such text is findable by exact phrase but
  not by word. A future `analyzer_id` may address this; version 1 does not.

### 3.3 Which literals are indexed

**Every literal is indexed**, subject only to the three mechanical exclusions
below. There is no configured predicate list, and there is no way to ask for one.

This is the design's central bet, and it is worth stating why the alternative was
rejected. A build that indexes a configured set of "label-ish" predicates fails
silently: a predicate nobody thought to configure produces resources that
`--text` cannot find, and nothing in the result says so. Across many
independently published datasets, such a configuration is a guess that is wrong
somewhere and undiagnosable everywhere.

What predicate configuration was approximating comes free from ranking: BM25
normalizes by document length, so for the same query term a one-word `rdfs:label`
outranks a two-hundred-word `rdfs:comment` without anyone declaring which is
which. Field filtering is a *query-time* concern (§7.3), where a wrong answer is
a corrected flag rather than a rebuild.

### 3.4 The exclusions

A literal is not indexed when, tested in this order:

1. **Datatype** — its datatype IRI is in the build's exclusion set (§3.5).
2. **Oversize** — its value is longer than the build's byte cap, default
   **4096** bytes. Sequences, embedded JSON or XML, full document text.
3. **No tokens** — its value contains no alphanumeric character, so the
   tokenizer would produce nothing and the document could never be retrieved.

The order is normative because the manifest counts each literal under exactly one
reason, and a count is only meaningful if the attribution is fixed.

Test 3 is deliberately the cheap characterization rather than a trial
tokenization: a literal *every* one of whose tokens exceeds the 128-byte cap is
therefore indexed, as an empty document that matches nothing. It counts toward
`indexed_docs`. This is a knowing imprecision, taken because the alternative is
tokenizing the whole corpus twice.

### 3.5 The datatype exclusion set

The default set is the XSD datatypes with an ordered value space:

```
xsd:base64Binary  xsd:boolean   xsd:byte       xsd:date      xsd:dateTime
xsd:dateTimeStamp xsd:dayTimeDuration          xsd:decimal   xsd:double
xsd:duration      xsd:float     xsd:gDay       xsd:gMonth    xsd:gMonthDay
xsd:gYear         xsd:gYearMonth xsd:hexBinary xsd:int       xsd:integer
xsd:long          xsd:negativeInteger          xsd:nonNegativeInteger
xsd:nonPositiveInteger          xsd:positiveInteger          xsd:short
xsd:time          xsd:unsignedByte             xsd:unsignedInt
xsd:unsignedLong  xsd:unsignedShort            xsd:yearMonthDuration
```

(`xsd:` is `http://www.w3.org/2001/XMLSchema#`; the manifest records full IRIs.)

Indexing these as text produces tokens nobody searches for. Everything else is
indexed, including `xsd:string`, `rdf:langString`, `xsd:anyURI`, `rdf:XMLLiteral`,
the string-derived types, and any datatype the builder has never heard of —
which is the exhaustive-by-default stance of §3.3 applied to datatypes.

A build may add IRIs to this set or replace it with the empty set. Whatever set
was used is recorded in full in the manifest (§4), so a consumer never has to
assume it.

### 3.6 Stemming

Each indexed literal is also tokenized through the plain chain **plus the
Snowball stemmer for its language**, and those tokens are indexed in a second
field. Stemming folds `running` and `runs` onto `run`, so a query for any of
them finds the others.

**Which stemmer.** The literal's own language tag decides, matched on the
primary subtag so `en-GB` stems as English. The available algorithms are the
ones Snowball publishes: Arabic, Danish, Dutch, English, Finnish, French,
German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian,
Spanish, Swedish, Tamil, Turkish. A literal in any other language is simply
absent from the stemmed field; it remains exactly searchable.

Two languages are absent for different reasons, and the distinction matters to
anyone deciding whether this format serves their data. Polish, Czech, Hindi and
Hebrew have no Snowball algorithm here but nothing conceptual stands in the way.
Chinese, Japanese, Korean and Thai are a different problem: their scripts do not
delimit words, so what they need is segmentation, not suffix stripping, and
§3.2's consequence about unbroken runs applies to them with or without a
stemmer.

**Untagged literals are stemmed as a declared default language**, recorded in
the manifest as `untagged_language`, and `en` unless a build says otherwise. A
build may set it to any supported language, or to `none` to leave untagged
literals unstemmed.

This is worth justifying, because the cautious-looking alternative — never stem
text whose language is not declared — is the wrong rule. Tagging is inconsistent
*within* a single merged graph: in Ubergraph, `rdfs:label` is untagged on UBERON
terms and `@en` on GO terms, and definitions are untagged on CHEBI and `@en` on
AISM. Declining to stem untagged text therefore makes search quality depend on
which source ontology a term happened to come from, which no consumer can
predict or work around. Assuming a language is a real assumption, so it is
declared in the manifest rather than left implicit — and it is a *default*, not
a fact about the data.

The risk this assumption carries is small and bounded. Stemming is applied
symmetrically to queries, so a mis-stemmed token still matches itself:
`"oxidane"` and a query for `oxidane` both reduce to `oxidan`. What an
inappropriate stemmer can produce is a *coincidence* — two unrelated words
sharing a stem — and every such hit lands in the stemmed class, below every
exact match (§6).

**One term space.** All stemmed tokens share one field regardless of language,
so stems from different languages can coincide: German `Atrazin` and English
`atrazine` both reduce to `atrazin`. This is a real effect, not a defect to be
designed away — and it already exists without stemming, since lowercasing alone
collides German `Gift` with English `gift`. A language filter (§7.2) excludes
what a caller does not want; ranking keeps the rest harmless.

### 3.7 The whole-literal key

A literal short enough to be a name also gets a **whole-literal key**: its plain
tokens joined by single spaces. `"Body"`, `"body"` and `"BODY"` share the key
`body`; `"Body structure (body structure)"` has the key
`body structure body structure`.

The key exists to answer a different question from the one the other fields
answer. `text` and `text_stemmed` find literals that *contain* the query; the
key finds literals that *are* the query — "which resource is named this", which
is the entity-resolution operation §0 opens with.

**Why this is not just ranking.** BM25 cannot express it. Scoring `body` against
Ubergraph puts `"Body structure (body structure)"` *above* the three resources
literally named `"body"`, because the term occurs twice in four tokens and term
frequency outweighs the length penalty. That is textbook BM25 and it is the
wrong answer: a literal that repeats itself beat the literals that are the
query. No boost factor fixes it in general, because the two are not on a common
scale — it has to be a separate class (§6).

The key is built from tokens, not from the raw lexical form, so case and
punctuation do not decide identity — the same normalization that makes `body`
match `"Body"` inside a sentence makes it match `"Body."` as a whole.

**Length cap.** A literal whose key would exceed **256 bytes** does not get one.
Nobody types a definition out in full, and a term dictionary holding every one
of them grows by the size of the corpus text. Such literals remain fully
searchable through the other two fields; they just cannot be matched as a whole.
The cap is generous enough for the long strings that *are* pasted whole —
systematic chemical names, full taxonomic labels. On Ubergraph it covers 98.1%
of indexed literals (10 268 968 of 10 470 056); tightening it to 96 bytes would
save 60 MiB of a 677 MiB index and give up exactly the long names.
`whole_literal_keys` in the manifest publishes the coverage per index.

## 4. The manifest (`hdtc-text.meta`)

Every index directory contains a file named `hdtc-text.meta`: UTF-8, one record
per line, fields separated by U+0009, lines by U+000A. It is what identifies the
directory as an hdtc text index, and it carries everything Tantivy does not
model.

Keys, each at most once except where noted:

| key | fields | meaning |
|---|---|---|
| `hdtc-text` | version | Manifest schema version. `1` for this document. **Required, must be first-known.** |
| `analyzer` | id | §3's convention ID. Required. |
| `tantivy` | version | Exact Tantivy release that wrote the segments (§5), as that release reports itself. Required. |
| `source_digest` | 64 hex chars | SHA-256 over the source HDT's dictionary-and-triples suffix. |
| `max_literal_bytes` | count | The §3.4 byte cap this build used. |
| `untagged_language` | tag or `none` | Language untagged literals were stemmed as (§3.6). |
| `literals_scanned` | count | Literals seen in the object section, indexed or not. |
| `indexed_docs` | count | Documents in the index. |
| `whole_literal_keys` | count | Documents carrying a whole-literal key (§3.7). |
| `excluded_oversize` | count | Literals excluded by §3.4 rule 2. |
| `excluded_datatype` | count | Literals excluded by §3.4 rule 1. |
| `excluded_no_tokens` | count | Literals excluded by §3.4 rule 3. |
| `excluded_datatype_iri` | IRI | One member of the §3.5 set. Repeated, ascending. |
| `language` | tag, count | One language value and its document count. Repeated, ascending by tag. |

`literals_scanned = indexed_docs + excluded_oversize + excluded_datatype +
excluded_no_tokens`, and the `language` counts sum to `indexed_docs`.

Reader rules:

- A file without an `hdtc-text` line is not a text index.
- A reader **must refuse** a manifest whose `hdtc-text` version, `analyzer` ID,
  or `tantivy` version it does not implement, and should name the mismatch.
- A reader **must ignore** keys it does not recognize, so that a later version
  may add lines without invalidating this one.

### 4.1 Why the exclusions are counted

An index that silently omits some fraction of a dataset's literals makes every
search over it quietly wrong about coverage — a caller cannot distinguish "no
such resource" from "that literal was too long". Publishing the counts, and the
exact datatype set, turns an invisible gap into a readable one. It is the
statistics-honesty rule the other hdtc artifacts follow, applied to text.

## 5. The Tantivy index

The rest of the directory is a Tantivy index, written by the release named in the
manifest's `tantivy` line: **0.26.1**.

A builder must take that string from the linked Tantivy rather than from a
constant of its own, so that a dependency bump cannot label an index with a
version that did not write it. The release version is stricter than the index
format version Tantivy also reports — two releases may share a format — and
refusing to read across releases anyway costs a rebuild rather than a wrong
answer.

### 5.1 Schema

Three fields, in this order:

| field | type | options |
|---|---|---|
| `text` | text | indexed, tokenizer `hdtc`, positions and frequencies, **not** stored |
| `text_stemmed` | text | indexed, tokenizer `raw`, positions and frequencies, **not** stored |
| `text_exact` | text | indexed, tokenizer `raw`, no frequencies or positions, **not** stored |
| `object` | u64 | fast, **not** indexed, **not** stored |
| `lang` | text | indexed raw (one token, no tokenization), **not** fast, **not** stored |

`text` holds the literal's value (§3.1), `text_stemmed` its stemmed form (§3.6),
`text_exact` its whole-literal key (§3.7), `object` its object dictionary ID
(§2.2), `lang` its language value (§2.3).

`text_exact` holds exactly one term per document, so it records neither
frequency nor position: a query either is the whole literal or is not. It is
absent for documents over the §3.7 cap.

`text_stemmed` is written as an **already-tokenized value**, and its declared
tokenizer is `raw` because no chain ever runs over it. This is forced by the
design: the stemmer depends on the *document's* language, and Tantivy binds one
tokenizer per field, so per-language stemming cannot be expressed as a field
tokenizer without a field per language — which would make the schema depend on
the dataset. The builder stems each literal itself and supplies the finished
tokens, preserving each token's position so that a phrase query works against
this field too. A reader builds terms for it directly and must not analyze them.

A document whose language has no stemmer (§3.6) simply carries no value for this
field.

Nothing is stored. The literal itself is already in the HDT dictionary,
addressed by `object`; storing it again would duplicate the dataset's largest
component to save one dictionary read. A consumer reads `object` from the fast
field and resolves the text through HDT.

Positions are recorded because phrase queries need them, and a phrase is how a
client asks for the `exact` match kind.

### 5.2 The `hdtc` tokenizer

The schema names its tokenizer `hdtc`; Tantivy stores the *name*, not the chain.
Both a builder and a reader **must** register, under that name, the chain of
§3.2: `SimpleTokenizer` → `RemoveLongFilter(129)` → `LowerCaser`. A reader that
omits this silently falls back to Tantivy's built-in `default` chain, which caps
tokens at 40 bytes and would fail to match any longer term.

### 5.3 Segments

A published index is merged to a single segment where it has any documents. This
is a property of what hdtc publishes, not a requirement on readers.

## 6. Ranking

Scoring is Tantivy's BM25 over the `text` field, with that release's default
parameters, and collection statistics computed over *distinct literals* (§2.1).

Two properties a consumer may rely on:

- Rarer query terms weigh more, and among documents containing a term equally
  often the shorter one scores higher.
- Scores are **not comparable across indexes**. Two indexes built from different
  datasets have different collection statistics, and under distinct-literal
  indexing they also have different duplication profiles. Merging results across
  datasets belongs to the client, by rank rather than by raw score.

**Three classes, in order.** A query is run as three phases, and every hit from
an earlier phase ranks above every hit from a later one, with duplicates
attributed to the earliest:

1. **whole-literal** — the literal *is* the query (`text_exact`, §3.7);
2. **exact** — the query's tokens appear as written (`text`);
3. **stemmed** — they appear only after stemming (`text_stemmed`, §3.6).

This is a guarantee, not a tendency: BM25 scores from different fields are not
comparable, so no set of boost factors could promise it. It is doc 03 §3.4.5's
`match_kind` treated as a class rather than a score component, which is how that
document says results should be merged.

Scores are therefore comparable *within* a class and not across two of them.

Approximate matching — fuzzy and prefix — is automaton-driven and *constant
score* in Tantivy: it ranks nothing on its own. hdtc therefore unions an
approximate query with the exact term query for the same token, so an exact match
outranks an approximate one and exact matches keep their usual ordering among
themselves. Documents matched only approximately are mutually unranked, and are
ordered by ascending object ID for determinism. Fuzzy and prefix widening apply
to the `text` field only.

## 7. Query semantics

These are the semantics `hdtc search --text` implements; they are part of the
artifact's contract in that a different reader implementing them will produce the
same answers.

### 7.1 Matching

The query string is tokenized by §3.2. The tokens combine as:

- **all** (default) — every token must be present. The entity-resolution mode.
- **any** — any token may match; more matching tokens rank higher.
- **phrase** — the tokens must be adjacent and in order.

The whole-literal phase (§6) ignores the mode: a query either is the literal or
is not, so there is nothing for `all`, `any` or `phrase` to vary, and fuzzy and
prefix widening do not apply to it either. The remaining phases evaluate the
mode against the plain field and then the stemmed one. In the stemmed phase a query token is reduced by *every* stemmer the index
contains — the query carries no language of its own — and the resulting stems
are unioned. Most stemmers leave a short word untouched, so this is typically one
or two distinct terms. Under `all`, a token that no stemmer changes still has to
match, so a query token absent from the stemmed field drops that phase rather
than weakening the requirement.

Optionally, each token may be widened to a maximum edit distance (with a
transposition counting as one edit), and the final token may be treated as a
prefix. Both apply to the plain field only.

### 7.2 Language

A language filter is a set of BCP 47 ranges, applied by RFC 4647 §3.3.1 basic
filtering: `en` selects `en` and `en-gb`, not `english`. **`und` documents are
always eligible** (§2.3), unconditionally.

"Unconditionally" is load-bearing. It would be tempting to suppress untagged
documents when no requested range names a tag the index actually holds, on the
grounds that answering `--lang fr` with untagged strings overstates the match.
That rule is incoherent: it makes an untagged literal's visibility depend on
whether some *unrelated* language happens to be present, so `--lang de` and
`--lang fr` would answer differently about a chemical formula that is neither.
A filter that names no tagged document simply returns the untagged ones.

### 7.3 Predicate filtering, and over-fetch

The index holds no predicate (§2.1), so a predicate restriction is applied
*after* ranking, when each hit is resolved to its occurrences.

This is the cost of the distinct-literal design and it should be named clearly:
filling a page of results can require walking past arbitrarily many ranked
literals whose occurrences all fail the filter. Cost is bounded per literal —
one OPS descent — but the number of literals examined is not bounded by the page
size.

A conforming implementation should report how far it walked. Doc 19 §19.2.3
proposes a predicate-ID → object-ID sidecar that would make this filtering
pre-rank; §19.7 makes building it conditional on measuring the over-fetch factor
first. Version 1 of this format has no such sidecar. Adding one is additive — a
new file beside this index — and is not a change to this format.

### 7.4 Results

Results are RDF triples. Every ranked literal expands to every
`(subject, predicate)` occurrence that uses it, subject to the predicate filter,
and no occurrences are collapsed by subject. A subject therefore appears once
for each matching triple, preserving the same statement-oriented semantics as
an ordinary pattern search.

Ordering follows the whole-literal, exact, and stemmed class order of §6. Within
a class it is by descending score, with ties broken by ascending object
dictionary ID, then by the OPS order of a literal's occurrences. Every
occurrence of one literal carries the same score. The tie-break is normative:
determinism here is worth more than any cleverness about which of two equally
scored literals is nicer, because a page that varies between identical calls
poisons caching and response diffs.

When scores are requested in line-oriented RDF output, each score is appended
after the terminating dot as an N-Triples/N-Quads comment, for example
`<s> <p> "value" . # score=1.1420`. Scores never add a field to the RDF statement,
so scored output remains valid RDF and can be consumed directly by a conforming
parser.

## 8. Cost

| operation | work |
|---|---|
| build | one pass over the object dictionary section; no triple pass |
| build memory | Tantivy's indexing arena, bounded by the configured limit |
| index size | scales with distinct indexed literals and their token counts |
| query, ranking | posting-list scan and top-k heap, over distinct literals |
| hit → `(subject, predicate)` | one OPS descent per ranked literal |
| page fill | ranking cost plus one descent per literal examined (§7.3) |

The build reads no triples at all, which is what distinguishes this artifact's
build cost from an occurrence-level index.

## 9. Binding to the source HDT

`source_digest` is the SHA-256 of the source HDT's dictionary-and-triples suffix,
the same digest the sketch and key-set artifacts record, and it is **advisory**:
a consumer may use it to detect an index built from different bytes, but nothing
in the format requires checking it, and an index whose digest does not match its
neighbouring HDT is stale rather than invalid.

A builder must nevertheless guarantee that the digest describes the bytes it
actually read: hdtc records the source's identity before and after the
dictionary scan and refuses to publish if the file changed underneath it. A
digest that is *stale* is a documented condition; one that is *wrong about which
bytes it covers* would be actively misleading.

Object IDs are meaningful only against the HDT the index was built from. An
index paired with a different HDT resolves to unrelated terms, which is the
failure `source_digest` exists to make detectable.

## 10. Versioning

- Adding a manifest key is a compatible change; readers ignore unknown keys.
- Changing any §3 rule is a new `analyzer_id`.
- Changing the §5.1 schema, or moving to a Tantivy release whose segments the
  pinned one cannot read, is a new `hdtc-text` manifest version.
- Adding a sidecar beside the index (§7.3) changes neither.

An index is identified by the triple (`hdtc-text` version, `analyzer_id`,
`tantivy` version). A reader must check all three, because each fails
differently: the first structurally, the second silently, the third at the byte
level.
