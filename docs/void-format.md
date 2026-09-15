# hdtc VoID description, version 1

Status: normative description of the RDF that `hdtc void` emits.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY**
are to be interpreted as described by RFC 2119.

## 0. Purpose

`hdtc void` describes an HDT with [VoID](https://www.w3.org/TR/void/)
statistics: dataset counts, property and class partitions, and a nested
⟨class, property, object class⟩ tree with datatype and language breakdowns.
Consumers plan queries from the counts and learn a graph's schema from the
tree. KGF stores the output as a queryable `stats/void.hdt`.

Unlike the other documents in `docs/`, this one specifies RDF, not bytes. The
output is a **set of triples**: statement order and N-Triples formatting are
not significant, and two runs are equivalent when their triple sets are equal.
What this document fixes is the graph's shape (§2), how nodes are named (§3),
and how an RDF dataset's graphs are described (§4).

There are two **graph views**:

| View | Describes | Requires |
|---|---|---|
| `union` (default) | The HDT's triples union | the HDT |
| `dataset` | The union, plus one `void:subset` per graph of the RDF dataset | the HDT, `.graphs`, `.perm`, and `.graphs.idx` with OPS layers (§6) |

## 1. Vocabulary

| Prefix | Namespace |
|---|---|
| `rdf:` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#` |
| `xsd:` | `http://www.w3.org/2001/XMLSchema#` |
| `void:` | `http://rdfs.org/ns/void#` |
| `void-ext:` | `http://ldf.fi/void-ext#` |
| `sd:` | `http://www.w3.org/ns/sparql-service-description#` |

Every count is an `xsd:integer` literal.

## 2. A dataset description

A **dataset description** is rooted at one `void:Dataset` node `D` with IRI
`{D}`. The union is described this way at the IRI given by `--dataset-uri`,
and every graph subset is described the same way at its own IRI (§4).

### 2.1 Dataset counts

| Statement | Value |
|---|---|
| `D rdf:type void:Dataset` | |
| `D void:triples n` | Distinct triples |
| `D void:distinctSubjects n` | Distinct subject terms |
| `D void:properties n` | Distinct predicate terms |
| `D void:distinctObjects n` | Distinct object terms |

For the union these are the HDT's triple count and dictionary section sizes.

### 2.2 The partition tree

Every partition node is typed `rdf:type void:Dataset` and carries
`void:triples`, the number of the parent's triples it holds.

| Partition | Linked from its parent by | Identifying statements | Triples it holds |
|---|---|---|---|
| Property | `D void:propertyPartition P` | `P void:property <p>` | the triples with predicate `p` |
| Class | `D void:classPartition C` | `C void:class <c>`, `C void:entities n` | the triples whose subject has class `c` |
| Class property | `C void:propertyPartition CP` | `CP void:property <p>` | class `c`'s triples with predicate `p` |
| Object class | `CP void-ext:objectClassPartition T` | `T void:class <t>`, or nothing if untyped | `CP`'s triples whose object has class `t`, or no class |
| Datatype | `CP void-ext:datatypePartition DT` | `DT void-ext:datatype <dt>` | `CP`'s triples whose object is a literal of datatype `dt` |
| Language | `DT void-ext:languagePartition L` | `L void-ext:language "tag"` | `DT`'s triples whose literal has language tag `tag` |

Datatype and language partitions exist only beneath class property partitions.
Language partitions exist only beneath the `rdf:langString` datatype partition.
A class partition's `void:entities` is the number of distinct subjects with
that class.

### 2.3 Classes and datatypes

- **A class** is an IRI object of an `rdf:type` triple in the described
  dataset. Blank-node and literal objects of `rdf:type` are not classes. They
  create no class partition, and they do not make a subject or object typed.
- **An object with several classes** counts toward the object-class partition
  of each one. An object with none counts toward the single untyped partition,
  which has no `void:class`. That includes literals and untyped IRIs and blank
  nodes.
- **A subject with several classes** counts toward each class partition. Class
  partition triple counts therefore need not sum to `D void:triples`.
- **Datatypes follow RDF 1.1.** A plain literal is `xsd:string`, and a
  language-tagged literal is `rdf:langString`.

### 2.4 Distinct counts

`--partition-distinct-counts` adds `void:distinctSubjects` and
`void:distinctObjects` to partitions:

| Scope | Partitions that receive them |
|---|---|
| (absent) | none |
| `dataset-properties` | property partitions directly below `D` |
| `all` | every partition in §2.2 |

A class partition's `void:distinctSubjects` equals its `void:entities`.

## 3. Node names

Every node below the root is named by extending its parent's IRI:

```text
{parent}/{kind}/{md5}
```

`{md5}` is the lowercase hexadecimal MD5 digest of the UTF-8 **key** named
below. An IRI key is the IRI without angle brackets.

| Node | IRI | Key |
|---|---|---|
| Property partition | `{D}/property/{md5}` | predicate IRI |
| Class partition | `{D}/class/{md5}` | class IRI |
| Class property partition | `{C}/property/{md5}` | predicate IRI |
| Object-class partition | `{CP}/target/{md5}` | class IRI, or `__untyped__` |
| Datatype partition | `{CP}/datatype/{md5}` | datatype IRI |
| Language partition | `{DT}/language/{md5}` | language tag |
| Graph subset (§4) | `{D}/graph/{md5}` | graph name |
| Named graph (§4) | `{D}/named-graph/{md5}` | graph name |
| Default graph subset (§4) | `{D}/default-graph` | — |

Consequences a consumer MAY rely on:

- **Names are deterministic.** They depend only on the dataset IRI and the
  terms, so re-running over the same HDT reproduces them.
- **Descriptions nest.** Every node of the description rooted at `X` has IRI
  `X` or an IRI beginning `X/`, so one description can be extracted from the
  output by IRI prefix.

Consumers SHOULD NOT parse digests back into terms. The identifying statements
of §2.2 and §4.2 carry the terms.

With `--use-blank-nodes`, every partition, subset, and named-graph node is a
blank node instead. The root dataset node stays an IRI. The structure is
unchanged, but §3's consequences no longer hold.

## 4. Graph subsets

The `dataset` view describes an RDF dataset stored as an HDT plus its `.graphs`
sidecar ([graphs-sidecar-format.md](graphs-sidecar-format.md)). The HDT holds
the `N` distinct triples of the union. The sidecar records which of the graphs
contain each triple: graph 0 is the default graph, and graphs `1..G` are
named. A triple may belong to several graphs, so the membership count `M` is at
least `N`.

### 4.1 What a subset describes

The subset for graph `g` is rooted at `{D}/graph/{md5(name)}`, or at
`{D}/default-graph` for graph 0. **Its description MUST be exactly the triple
set that a `union`-view run would emit for an HDT containing only `g`'s
triples, given the subset IRI as the dataset IRI and the same distinct-count
scope,** plus one statement typing the root `sd:Graph`.

Everything a subset reports follows from that rule:

- **Typing is local to the graph.** Only `rdf:type` triples in `g` make a
  subject or object typed within `g`. A subject typed `Person` in one graph is
  untyped in another. Its triples there fall into no class partition, and as
  an object it lands in the untyped object-class partition.
- **Counts are the graph's own.** `void:triples` is `g`'s membership count.
  The distinct counts are over `g`'s terms.
- **Subsets are not additive.** Subset `void:triples` sum to `M`, not to the
  union's `N`. Distinct counts and class entities are set sizes, so terms shared
  between graphs count once in the union.

The union's own description is unchanged by the `dataset` view, apart from the
statements in §4.2.

### 4.2 Linking subsets to the dataset

```turtle
<{D}> a void:Dataset, sd:Dataset ;
    void:subset <{D}/graph/9f2c…>, <{D}/default-graph> ;
    sd:namedGraph <{D}/named-graph/9f2c…> .

<{D}/named-graph/9f2c…> a sd:NamedGraph ;
    sd:name <http://purl.obolibrary.org/obo/uberon.owl> ;
    sd:graph <{D}/graph/9f2c…> .

<{D}/graph/9f2c…> a void:Dataset, sd:Graph ;
    void:triples 1200000 ;
    void:classPartition <{D}/graph/9f2c…/class/51ab…> .

<{D}/default-graph> a void:Dataset, sd:Graph ;
    void:triples 5000 .
```

In the `dataset` view, the root `D` additionally has:

| Statement | When |
|---|---|
| `D rdf:type sd:Dataset` | always |
| `D void:subset S` | for every described graph |
| `D sd:namedGraph NG` | for every graph named by an IRI |

Each subset root `S` is a dataset description (§2) typed both `void:Dataset`
and `sd:Graph`. Each `sd:NamedGraph` node `NG` has exactly:

| Statement |
|---|
| `NG rdf:type sd:NamedGraph` |
| `NG sd:name <graph IRI>` |
| `NG sd:graph S` |

This is the SPARQL 1.1 Service Description shape. `sd:name` and `sd:graph`
both have domain `sd:NamedGraph`, and the specification's own examples put
`void:triples` on the `sd:Graph` reached through `sd:graph`. Facts about a
graph as published, such as `sd:entailmentRegime` (also domain
`sd:NamedGraph`) or a license, belong on `NG`. Statistics belong on `S`.

### 4.3 Which graphs are described

- **Every named graph** in the sidecar's graph dictionary gets a subset.
- **The default graph** gets a subset only when it contains at least one
  triple. It is linked by `void:subset` alone.
- **Graphs named by blank nodes** get a subset keyed by the stored `_:label`,
  but no `sd:NamedGraph`: `sd:name` requires an IRI, and the label is not
  meaningful outside the HDT.

hdtc **MUST NOT** emit `sd:defaultGraph`. In Service Description, the default
graph is whatever a query without `GRAPH` sees. Many engines, QLever and KGF
among them, answer that from the union. Pointing `sd:defaultGraph` at graph 0
would advertise a nearly empty default graph for a dataset that is entirely
named graphs. Whoever describes the service knows its semantics and can add
the statement.

### 4.4 Why subsets are not named by the graph IRI (non-normative)

A graph name is global and usually already denotes something. In Ubergraph,
`http://purl.obolibrary.org/obo/uberon.owl` is the UBERON ontology.
`void:triples` on that IRI would claim a size for the ontology itself.
Descriptions of two datasets, or two releases, that load the same graph would
then give one subject conflicting counts once merged. Minting subsets and
named-graph nodes under `{D}` scopes every statement to the dataset that
makes it.

## 5. Combining descriptions (non-normative)

Only linking combines subset descriptions. Arithmetic does not. The union's
distinct counts, property count, and class entities are set sizes, and its
triple count is a sum only when no triple is in two graphs. **The union's
description cannot be derived from its subsets.** It needs a run over the union
HDT.

The simplest route is therefore always one `dataset`-view run over the quads
HDT. Component HDTs built separately become one first:

```bash
hdtc create --mode quads --graph-map asserted.hdt=https://example.org/kg#asserted \
  --graph-map entailed.hdt=https://example.org/kg#entailed \
  asserted.hdt entailed.hdt --perm --graphs-index -o kg.hdt
```

A description already computed for a component remains valid when it was run
with `--dataset-uri {D}/graph/{md5(graph IRI)}` and the same distinct-count
scope, since by §4.1 it is that graph's subset. To join it, add the `sd:Graph`
type and the §4.2 links to the union's own description.

## 6. Required artifacts and cost (non-normative)

| Run | `.perm` | `.graphs` | `.graphs.idx` (OPS layers) |
|---|:---:|:---:|:---:|
| `union` | | | |
| `union` with `--partition-distinct-counts` | ✓ | | |
| `dataset` | ✓ | ✓ | ✓ |

The union's distinct objects come from the dictionary. A graph's do not, so
the `dataset` view always runs the object-ordered pass. That pass joins the
permutation's OPS scan with the OPS layer set of `.graphs.idx`. The two SPO
passes join the HDT's triple scan with the sidecar's layers.

Each join transposes a layer set into position order. When the graphs' layers
fit the memory budget as concurrent iterators (at most 128 layers), the
transpose is a k-way merge. Otherwise it is an external sort in `--temp-dir`.
The `dataset` view splits `--memory-limit` between the dictionary cache and
these transposes.

Analysis memory is not bounded by `--memory-limit`. The subject-to-class index
stays at 4 bytes per subject, with combinations keyed by (graph, class). The
partition statistics, however, are repeated for every graph, so memory grows
with graphs × partitions. Tens of coarse graphs cost little. Thousands of
schema-rich graphs may not fit.
