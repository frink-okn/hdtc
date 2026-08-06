//! Querying a published text index.
//!
//! A search returns ranked **object dictionary IDs** and nothing else — the
//! index holds no subject and no predicate. Turning those IDs
//! into (subject, predicate, literal) rows is the caller's job, done through
//! HDT itself; see [`crate::hdt::search`].

use super::analyzer::{
    TOKENIZER_NAME, UNDETERMINED_LANGUAGE, language_matches, stemmer_language, stemming_tokenizer,
};
use super::manifest::TextManifest;
use super::schema::{FIELD_LANG, FIELD_OBJECT, FIELD_TEXT, FIELD_TEXT_STEMMED, register_tokenizer};
use anyhow::{Context, Result, ensure};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::path::Path;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{
    BooleanQuery, EnableScoring, FuzzyTermQuery, Occur, PhraseQuery, Query, QueryClone, TermQuery,
};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::tokenizer::{Language, TextAnalyzer, TokenStream};
use tantivy::{DocSet, Index, IndexReader, ReloadPolicy, TERMINATED, Term};

/// How the tokens of a query must combine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MatchMode {
    /// Every token must be present — the focused multi-token default.
    #[default]
    All,
    /// Any token may match; more matching tokens rank higher.
    Any,
    /// The tokens must appear adjacently and in order.
    Phrase,
}

/// One query against a text index.
#[derive(Debug, Clone, Default)]
pub struct TextQuery {
    pub text: String,
    pub mode: MatchMode,
    /// Maximum Levenshtein distance per token; 0 disables fuzzy matching.
    pub fuzzy: u8,
    /// Treat the final token as a prefix, for typeahead-style lookup.
    pub prefix: bool,
    /// BCP 47 language ranges to restrict to; empty means no restriction.
    pub languages: Vec<String>,
}

/// How a hit matched, used to keep broader analysis below literal matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MatchKind {
    /// The query's tokens appear in the literal as written, among others.
    Exact,
    /// They appear only after stemming — `run` finding `running`.
    Stemmed,
}

/// One ranked literal.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TextHit {
    /// HDT object dictionary ID — the document's identity.
    pub object_id: u64,
    /// BM25-derived score. Comparable within one index and **not** across
    /// indexes, which have different collection statistics.
    /// Comparable *within* a [`MatchKind`], not across two of them.
    pub score: f32,
    pub kind: MatchKind,
}

/// A bounded ranked search and what it established about the whole query.
#[derive(Debug, Clone, PartialEq)]
pub struct TextSearch {
    /// The best retained hits, in normative class/score/object order.
    pub hits: Vec<TextHit>,
    /// Matching literal documents scored before the search stopped.
    pub examined: u64,
    /// `true` when every matching literal document was scored.
    pub complete: bool,
}

/// An opaque resume point in an immutable text index.
///
/// Positions are meaningful only against the index that issued them. They are
/// deliberately compact so a stateless caller can carry one in its own cursor,
/// while the segment/document split remains hdtc's concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TextScanPosition(u64);

impl TextScanPosition {
    /// Restore a position previously returned by [`Self::encode`].
    pub const fn decode(encoded: u64) -> Self {
        Self(encoded)
    }

    /// Compact representation suitable for an enclosing cursor token.
    pub const fn encode(self) -> u64 {
        self.0
    }

    const fn new(segment_ord: u32, doc_id: u32) -> Self {
        Self((segment_ord as u64) << 32 | doc_id as u64)
    }

    const fn segment_ord(self) -> u32 {
        (self.0 >> 32) as u32
    }

    const fn doc_id(self) -> u32 {
        self.0 as u32
    }
}

/// One bounded page of unranked matching object dictionary IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextMatchPage {
    /// Matching HDT object dictionary IDs in immutable scan order.
    pub object_ids: Vec<u64>,
    /// Where the next page begins, when this one spent its limit.
    pub next: Option<TextScanPosition>,
    /// `true` when the query was exhausted in this page.
    pub complete: bool,
}

/// Heap entry whose greatest member is the worst retained hit.
#[derive(Debug, Clone, Copy)]
struct WorstFirst(TextHit);

impl PartialEq for WorstFirst {
    fn eq(&self, other: &Self) -> bool {
        self.0.score.to_bits() == other.0.score.to_bits() && self.0.object_id == other.0.object_id
    }
}

impl Eq for WorstFirst {}

impl PartialOrd for WorstFirst {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorstFirst {
    fn cmp(&self, other: &Self) -> Ordering {
        // Lower scores and, at a tie, higher object IDs are worse.
        other
            .0
            .score
            .total_cmp(&self.0.score)
            .then(self.0.object_id.cmp(&other.0.object_id))
    }
}

struct PhaseSearch {
    hits: Vec<TextHit>,
    complete: bool,
}

fn retain(heap: &mut BinaryHeap<WorstFirst>, hit: TextHit, limit: usize) {
    if limit == 0 {
        return;
    }
    if heap.len() < limit {
        heap.push(WorstFirst(hit));
        return;
    }

    let worst = heap.peek().expect("a full non-empty heap has a member").0;
    let score_order = hit.score.total_cmp(&worst.score);
    let better = score_order == Ordering::Greater
        || (score_order == Ordering::Equal && hit.object_id < worst.object_id);
    if better {
        heap.pop();
        heap.push(WorstFirst(hit));
    }
}

fn ordered(heap: BinaryHeap<WorstFirst>) -> Vec<TextHit> {
    let mut hits: Vec<TextHit> = heap.into_iter().map(|entry| entry.0).collect();
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then(a.object_id.cmp(&b.object_id))
    });
    hits
}

/// Which field a query phase targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// The literal as written — exact matching, and where fuzzy and prefix
    /// widening apply.
    Plain,
    /// The stemmed form, for the extra recall stemming buys.
    Stemmed,
}

/// An opened text index, ready to answer queries.
pub struct TextSearcher {
    reader: IndexReader,
    analyzer: TextAnalyzer,
    manifest: TextManifest,
    text_field: Field,
    stemmed_field: Field,
    lang_field: Field,
    /// One analyzer per stemming algorithm the index actually used, so a query
    /// token is stemmed the same way the documents were. Empty when the index
    /// stems nothing, which skips the stemmed phase entirely.
    stemmers: Vec<TextAnalyzer>,
}

impl TextSearcher {
    /// Open the index published at `dir`.
    ///
    /// The manifest is read first: it says this index is hdtc's and identifies
    /// its schema and analyzer conventions. The writer metadata is diagnostic;
    /// Tantivy itself decides whether it can read the segment bytes.
    pub fn open(dir: &Path) -> Result<Self> {
        ensure!(
            dir.is_dir(),
            "Text index not found: {} (run `hdtc text` to build one)",
            dir.display()
        );
        let manifest = TextManifest::read(dir)?;
        let format = manifest
            .tantivy_index_format
            .map_or_else(|| "unknown".to_string(), |version| version.to_string());
        let index = Index::open_in_dir(dir).with_context(|| {
            format!(
                "Failed to open text index {} (written by Tantivy {}, index format {})",
                dir.display(),
                manifest.tantivy_writer,
                format
            )
        })?;
        register_tokenizer(&index);

        let schema = index.schema();
        let text_field = schema.get_field(FIELD_TEXT)?;
        let stemmed_field = schema.get_field(FIELD_TEXT_STEMMED)?;
        let lang_field = schema.get_field(FIELD_LANG)?;
        let stemmers = build_stemmers(&manifest);
        // Read through the fast-field column by name, so the schema is only
        // consulted for the two fields queries are built against.
        schema.get_field(FIELD_OBJECT)?;
        let analyzer = index
            .tokenizers()
            .get(TOKENIZER_NAME)
            .context("hdtc tokenizer was not registered")?;
        // Published hdtc indexes are immutable. Tantivy's default reader
        // watches meta.json for later commits and logs its initial checksum as
        // a modification; manual reload avoids that misleading watcher and an
        // unnecessary reload.
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .with_context(|| format!("Failed to open text index reader {}", dir.display()))?;

        Ok(Self {
            reader,
            analyzer,
            manifest,
            text_field,
            stemmed_field,
            lang_field,
            stemmers,
        })
    }

    /// What the build recorded about this index.
    ///
    /// A consumer that publishes its own description of the index — which
    /// literals were left out and why, which languages are present — should
    /// report what the build actually did rather than restate the configuration
    /// it was asked for.
    pub fn manifest(&self) -> &TextManifest {
        &self.manifest
    }

    /// Analyze a query string into tokens, using the index's own chain.
    ///
    /// Build and query go through the same [`TextAnalyzer`], which is what
    /// keeps a query token comparable with an indexed one.
    pub fn analyze(&self, text: &str) -> Vec<String> {
        let mut analyzer = self.analyzer.clone();
        let mut stream = analyzer.token_stream(text);
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().text.clone());
        }
        tokens
    }

    /// Number of documents matching `query` in either form, without ranking.
    pub fn count(&self, query: &TextQuery) -> Result<usize> {
        let Some(built) = self.union_query(query)? else {
            return Ok(0);
        };
        let searcher = self.reader.searcher();
        // A document matching both forms is one document; the union query
        // counts it once, which is what a `--count` has to report.
        Ok(searcher.search(&built, &Count)?)
    }

    /// Count matching documents, stopping once `limit` of them are known.
    ///
    /// Returns the count and whether it stopped early — `(n, false)` is exact,
    /// `(limit, true)` means "at least this many".
    ///
    /// [`count`](TextSearcher::count) walks every posting the query matches,
    /// which is the right answer for a CLI reporting a total and the wrong one
    /// for a service that has published a bound on the work a request may do:
    /// a single common token can match millions of literals, and no argument
    /// to `count` says "stop". This walks the same documents and stops, so a
    /// caller with a budget can spend it rather than discovering afterwards
    /// what the query cost.
    ///
    /// Iterating the scorers directly rather than through a collector, because
    /// a Tantivy collector has no way to say "enough" — it sees each document
    /// and returns nothing until the search is over.
    pub fn count_up_to(&self, query: &TextQuery, limit: u64) -> Result<(u64, bool)> {
        let Some(built) = self.union_query(query)? else {
            return Ok((0, false));
        };
        if limit == 0 {
            return Ok((0, true));
        }

        let searcher = self.reader.searcher();
        let weight = built.weight(EnableScoring::disabled_from_searcher(&searcher))?;
        let mut counted = 0u64;
        for reader in searcher.segment_readers() {
            let mut scorer = weight.scorer(reader, 1.0)?;
            // The union already collapses a document matching both the plain
            // and the stemmed field, so a document is seen once per segment
            // and segments do not overlap.
            let alive = reader.alive_bitset();
            while scorer.doc() != TERMINATED {
                if alive.is_none_or(|alive| alive.is_alive(scorer.doc())) {
                    counted += 1;
                    if counted >= limit {
                        return Ok((counted, true));
                    }
                }
                scorer.advance();
            }
        }
        Ok((counted, false))
    }

    /// The `top_k` highest-scoring literals, best first.
    ///
    /// Exact matches come first as a class, then stemmed-only ones. Running the
    /// two as separate phases makes that a guarantee rather than something a
    /// boost factor usually achieves: BM25 scores from two fields are not
    /// comparable, so no single weighting could promise an exact hit outranks a
    /// stemmed one.
    ///
    /// Within a class, ties are broken by ascending object ID, so two runs of
    /// the same query over the same index return the same page in the same
    /// order.
    pub fn search(&self, query: &TextQuery, top_k: usize) -> Result<Vec<TextHit>> {
        if top_k == 0 {
            return Ok(Vec::new());
        }
        let mut hits = Vec::new();
        for (built, kind) in self.phase_queries(query)? {
            if hits.len() >= top_k {
                break;
            }
            hits.extend(self.run(&*built, top_k - hits.len(), kind)?);
        }
        Ok(hits)
    }

    /// Rank at most `candidate_limit` matching literal documents.
    ///
    /// Unlike a collector limit, `candidate_limit` bounds documents whose
    /// scores are calculated, not merely the heap and returned vector. The
    /// retained heap is independently bounded by `top_k`. When `complete` is
    /// false, the hits are the deterministic top-k among the examined prefix,
    /// not a claim about unexamined documents.
    pub fn search_up_to(
        &self,
        query: &TextQuery,
        top_k: usize,
        candidate_limit: u64,
    ) -> Result<TextSearch> {
        let searcher = self.reader.searcher();
        let mut examined = 0u64;
        let mut hits = Vec::new();

        for (built, kind) in self.phase_queries(query)? {
            // An exact hit always outranks a stemmed-only one. Once the exact
            // phase has filled the result, the stemmed phase still has to be
            // walked to establish `complete` and `examined`, but it retains no
            // hits that cannot enter the answer.
            let retain = top_k.saturating_sub(hits.len());
            let phase = self.run_up_to(
                &searcher,
                &*built,
                kind,
                retain,
                candidate_limit,
                &mut examined,
            )?;
            hits.extend(phase.hits);
            if !phase.complete {
                return Ok(TextSearch {
                    hits,
                    examined,
                    complete: false,
                });
            }
        }

        Ok(TextSearch {
            hits,
            examined,
            complete: true,
        })
    }

    /// Return one resumable page of unranked matching object IDs.
    ///
    /// The union query yields each document once even when its plain and
    /// stemmed fields both match. `from` is a position previously returned by
    /// this index. A page that returns exactly `limit` items conservatively
    /// carries a continuation; the following page may prove that the last item
    /// was also the end without examining a candidate past this page's bound.
    pub fn scan_matching_objects(
        &self,
        query: &TextQuery,
        from: Option<TextScanPosition>,
        limit: usize,
    ) -> Result<TextMatchPage> {
        ensure!(limit > 0, "text match scan limit must be positive");
        let Some(built) = self.union_query(query)? else {
            ensure!(
                from.is_none(),
                "a query with no indexable tokens has no text scan continuation"
            );
            return Ok(TextMatchPage {
                object_ids: Vec::new(),
                next: None,
                complete: true,
            });
        };

        let searcher = self.reader.searcher();
        let segments = searcher.segment_readers();
        let start = from.unwrap_or_default();
        let start_segment = usize::try_from(start.segment_ord())
            .context("text scan segment does not fit this platform")?;
        ensure!(
            start_segment < segments.len() || (from.is_none() && segments.is_empty()),
            "text scan position names segment {}, but the index has {} segments",
            start_segment,
            segments.len()
        );
        if segments.is_empty() {
            ensure!(
                start.doc_id() == 0,
                "text scan position lies past the end of the index"
            );
            return Ok(TextMatchPage {
                object_ids: Vec::new(),
                next: None,
                complete: true,
            });
        }

        let weight = built.weight(EnableScoring::disabled_from_searcher(&searcher))?;
        if from.is_some() {
            let previous = start
                .doc_id()
                .checked_sub(1)
                .context("text scan position is the beginning, not a continuation")?;
            let segment = &segments[start_segment];
            let mut verifier = weight.scorer(segment, 1.0)?;
            ensure!(
                verifier.seek(previous) == previous
                    && segment
                        .alive_bitset()
                        .is_none_or(|alive| alive.is_alive(previous)),
                "text scan position does not follow a matching document"
            );
        }

        // Grow only as matches arrive. Callers may publish a very large limit,
        // and reserving it before the query proves that many matches exist can
        // turn a harmless sparse query into an allocation failure.
        let mut object_ids = Vec::new();
        for (segment_ord, segment) in segments.iter().enumerate().skip(start_segment) {
            let column = segment
                .fast_fields()
                .u64(FIELD_OBJECT)
                .context("Text index has no object ID column")?;
            let mut scorer = weight.scorer(segment, 1.0)?;
            if segment_ord == start_segment && start.doc_id() != 0 {
                scorer.seek(start.doc_id());
            }
            while scorer.doc() != TERMINATED {
                let doc = scorer.doc();
                if segment
                    .alive_bitset()
                    .is_none_or(|alive| alive.is_alive(doc))
                {
                    let object_id = column
                        .first(doc)
                        .context("Text index document carries no object ID")?;
                    object_ids.push(object_id);
                    if object_ids.len() == limit {
                        let segment_ord = u32::try_from(segment_ord)
                            .context("text index has too many segments to resume")?;
                        let next = TextScanPosition::new(segment_ord, doc + 1);
                        return Ok(TextMatchPage {
                            object_ids,
                            next: Some(next),
                            complete: false,
                        });
                    }
                }
                scorer.advance();
            }
        }

        Ok(TextMatchPage {
            object_ids,
            next: None,
            complete: true,
        })
    }

    /// Visit every matching object ID without calculating or retaining scores.
    ///
    /// The union query yields a document once even when both text fields match.
    /// This is the bounded-memory path used by `search --text --count`.
    pub(crate) fn for_each_matching_object(
        &self,
        query: &TextQuery,
        mut visit: impl FnMut(u64) -> Result<()>,
    ) -> Result<usize> {
        let Some(built) = self.union_query(query)? else {
            return Ok(0);
        };
        let searcher = self.reader.searcher();
        let weight = built.weight(EnableScoring::disabled_from_searcher(&searcher))?;
        let mut count = 0usize;

        for segment in searcher.segment_readers() {
            let object_ids = segment
                .fast_fields()
                .u64(FIELD_OBJECT)
                .context("Text index has no object ID column")?;
            let mut scorer = weight.scorer(segment, 1.0)?;
            while scorer.doc() != TERMINATED {
                let doc = scorer.doc();
                if segment
                    .alive_bitset()
                    .is_none_or(|alive| alive.is_alive(doc))
                {
                    let object_id = object_ids
                        .first(doc)
                        .context("Text index document carries no object ID")?;
                    visit(object_id)?;
                    count += 1;
                }
                scorer.advance();
            }
        }
        Ok(count)
    }

    /// Visit every ranked hit without retaining the complete ranking.
    ///
    /// Hits arrive in segment/doc order. The caller can feed them to an
    /// external sorter to obtain the normative class/score/object ordering
    /// without a `TopDocs` heap proportional to the result count.
    pub(crate) fn for_each_ranked_hit(
        &self,
        query: &TextQuery,
        mut visit: impl FnMut(TextHit) -> Result<()>,
    ) -> Result<usize> {
        let searcher = self.reader.searcher();
        let mut count = 0usize;

        for (built, kind) in self.phase_queries(query)? {
            let weight = built.weight(EnableScoring::enabled_from_searcher(&searcher))?;
            for segment in searcher.segment_readers() {
                let object_ids = segment
                    .fast_fields()
                    .u64(FIELD_OBJECT)
                    .context("Text index has no object ID column")?;
                let mut scorer = weight.scorer(segment, 1.0)?;
                while scorer.doc() != TERMINATED {
                    let doc = scorer.doc();
                    if segment
                        .alive_bitset()
                        .is_none_or(|alive| alive.is_alive(doc))
                    {
                        let object_id = object_ids
                            .first(doc)
                            .context("Text index document carries no object ID")?;
                        visit(TextHit {
                            object_id,
                            score: scorer.score(),
                            kind,
                        })?;
                        count += 1;
                    }
                    scorer.advance();
                }
            }
        }
        Ok(count)
    }

    /// Union of the plain and stemmed forms, for unranked set operations.
    fn union_query(&self, query: &TextQuery) -> Result<Option<Box<dyn Query>>> {
        let mut clauses = Vec::new();
        for target in [Target::Plain, Target::Stemmed] {
            if let Some(built) = self.build_query(query, target)? {
                clauses.push((Occur::Should, built));
            }
        }
        Ok((!clauses.is_empty()).then(|| Box::new(BooleanQuery::new(clauses)) as Box<dyn Query>))
    }

    /// Ranked phases, with documents matched by the plain phase excluded from
    /// the stemmed phase at query time. Besides avoiding an unbounded `seen`
    /// set, this ensures a top-k stemmed request cannot be consumed by plain
    /// duplicates while lower-ranked stemmed-only documents are missed.
    fn phase_queries(&self, query: &TextQuery) -> Result<Vec<(Box<dyn Query>, MatchKind)>> {
        let plain = self.build_query(query, Target::Plain)?;
        let stemmed = self.build_query(query, Target::Stemmed)?;
        let plain_exclusion = plain.as_ref().map(|built| built.box_clone());
        let mut phases = Vec::with_capacity(2);

        if let Some(built) = plain {
            phases.push((built, MatchKind::Exact));
        }
        if let Some(built) = stemmed {
            let stemmed_only: Box<dyn Query> = match plain_exclusion {
                Some(exact) => Box::new(BooleanQuery::new(vec![
                    (Occur::Must, built),
                    (Occur::MustNot, exact),
                ])),
                None => built,
            };
            phases.push((stemmed_only, MatchKind::Stemmed));
        }
        Ok(phases)
    }

    /// Collect one unbounded phase with Tantivy's optimized top-k path.
    fn run(&self, query: &dyn Query, top_k: usize, kind: MatchKind) -> Result<Vec<TextHit>> {
        let searcher = self.reader.searcher();
        // Object ID is part of the collector key, rather than a sort applied
        // after collection. Tantivy otherwise breaks a score tie by DocAddress,
        // so a top-k boundary could discard the lower object ID before we ever
        // had a chance to impose the published ordering.
        let collector = TopDocs::with_limit(top_k).tweak_score(|segment| {
            let object_ids = segment.fast_fields().u64(FIELD_OBJECT).ok();
            move |doc, score| {
                let object_id = object_ids
                    .as_ref()
                    .and_then(|column| column.first(doc))
                    .unwrap_or(0);
                (score, Reverse(object_id))
            }
        });
        let collected = searcher.search(query, &collector)?;

        let mut hits = Vec::with_capacity(collected.len());
        for ((score, _), address) in collected {
            let segment = searcher.segment_reader(address.segment_ord);
            let object_id = segment
                .fast_fields()
                .u64(FIELD_OBJECT)
                .context("Text index has no object ID column")?
                .first(address.doc_id)
                .context("Text index document carries no object ID")?;
            hits.push(TextHit {
                object_id,
                score,
                kind,
            });
        }
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then(a.object_id.cmp(&b.object_id))
        });
        Ok(hits)
    }

    /// Scan one ranked phase while retaining only its best `top_k` hits.
    fn run_up_to(
        &self,
        searcher: &tantivy::Searcher,
        query: &dyn Query,
        kind: MatchKind,
        top_k: usize,
        candidate_limit: u64,
        examined: &mut u64,
    ) -> Result<PhaseSearch> {
        let weight = query.weight(EnableScoring::enabled_from_searcher(searcher))?;
        // Grow only as matches arrive. A configured limit may be much larger
        // than the query's result, and reserving it eagerly would spend memory
        // for candidates that do not exist.
        let mut heap = BinaryHeap::new();

        for segment in searcher.segment_readers() {
            let object_ids = segment
                .fast_fields()
                .u64(FIELD_OBJECT)
                .context("Text index has no object ID column")?;
            let mut scorer = weight.scorer(segment, 1.0)?;
            while scorer.doc() != TERMINATED {
                let doc = scorer.doc();
                if segment
                    .alive_bitset()
                    .is_none_or(|alive| alive.is_alive(doc))
                {
                    if *examined >= candidate_limit {
                        return Ok(PhaseSearch {
                            hits: ordered(heap),
                            complete: false,
                        });
                    }
                    let object_id = object_ids
                        .first(doc)
                        .context("Text index document carries no object ID")?;
                    let hit = TextHit {
                        object_id,
                        score: scorer.score(),
                        kind,
                    };
                    *examined += 1;
                    retain(&mut heap, hit, top_k);
                }
                scorer.advance();
            }
        }

        Ok(PhaseSearch {
            hits: ordered(heap),
            complete: true,
        })
    }

    /// Assemble one phase's query, or `None` when that phase cannot match —
    /// a query with no tokens, or a stemmed phase over an index that stems
    /// nothing.
    fn build_query(&self, query: &TextQuery, target: Target) -> Result<Option<Box<dyn Query>>> {
        ensure!(
            query.mode != MatchMode::Phrase || (query.fuzzy == 0 && !query.prefix),
            "--fuzzy and --prefix cannot be combined with --text-match phrase"
        );
        let tokens = self.analyze(&query.text);
        if tokens.is_empty() {
            return Ok(None);
        }
        if target == Target::Stemmed && self.stemmers.is_empty() {
            return Ok(None);
        }

        let text_query: Box<dyn Query> = if query.mode == MatchMode::Phrase {
            match self.phrase_query(&tokens, target) {
                Some(built) => built,
                None => return Ok(None),
            }
        } else {
            let occur = match query.mode {
                MatchMode::Any => Occur::Should,
                _ => Occur::Must,
            };
            let last = tokens.len() - 1;
            let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(tokens.len());
            for (position, token) in tokens.iter().enumerate() {
                match target {
                    Target::Plain => {
                        clauses.push((occur, self.token_query(token, query, position == last)))
                    }
                    // A token has one stem per language the index holds, and
                    // any of them may be the one that matches.
                    Target::Stemmed => match self.stem_query(token) {
                        Some(built) => clauses.push((occur, built)),
                        // Under `all`, a token with no stem cannot be required
                        // of the stemmed field, so the phase is dropped rather
                        // than silently weakened to the remaining tokens.
                        None if query.mode != MatchMode::Any => return Ok(None),
                        None => {}
                    },
                }
            }
            if clauses.is_empty() {
                return Ok(None);
            }
            Box::new(BooleanQuery::new(clauses))
        };

        let Some(language_query) = self.language_query(&query.languages) else {
            return Ok(Some(text_query));
        };
        Ok(Some(Box::new(BooleanQuery::new(vec![
            (Occur::Must, text_query),
            (Occur::Must, language_query),
        ]))))
    }

    /// A phrase query needs at least two tokens; one token is just a term.
    ///
    /// The stemmed field keeps each token's original position, so a phrase
    /// works there too — but one stemmer at a time, since a phrase needs a
    /// single term per position. The per-language phrases are unioned.
    fn phrase_query(&self, tokens: &[String], target: Target) -> Option<Box<dyn Query>> {
        let phrase_of = |terms: Vec<Term>| -> Box<dyn Query> {
            if terms.len() < 2 {
                return Box::new(TermQuery::new(
                    terms.into_iter().next().expect("tokens is non-empty"),
                    IndexRecordOption::WithFreqs,
                ));
            }
            Box::new(PhraseQuery::new(terms))
        };

        match target {
            Target::Plain => Some(phrase_of(
                tokens
                    .iter()
                    .map(|token| Term::from_field_text(self.text_field, token))
                    .collect(),
            )),
            Target::Stemmed => {
                let clauses: Vec<(Occur, Box<dyn Query>)> = self
                    .stemmers
                    .iter()
                    .map(|analyzer| {
                        let terms = tokens
                            .iter()
                            .map(|token| {
                                Term::from_field_text(
                                    self.stemmed_field,
                                    &stem_with(&mut analyzer.clone(), token),
                                )
                            })
                            .collect();
                        (Occur::Should, phrase_of(terms))
                    })
                    .collect();
                (!clauses.is_empty())
                    .then(|| Box::new(BooleanQuery::new(clauses)) as Box<dyn Query>)
            }
        }
    }

    /// One token against the stemmed field: a union over the stem each of the
    /// index's languages produces for it.
    ///
    /// Most stemmers leave a short word alone, so this is usually one or two
    /// distinct terms. It is also where a cross-language coincidence can enter
    /// — German *Gift* and English *gift* share a stem — which is contained by
    /// the stemmed phase ranking below every exact hit.
    fn stem_query(&self, token: &str) -> Option<Box<dyn Query>> {
        let mut stems: Vec<String> = self
            .stemmers
            .iter()
            .map(|analyzer| stem_with(&mut analyzer.clone(), token))
            .collect();
        stems.sort();
        stems.dedup();
        if stems.is_empty() {
            return None;
        }
        let clauses: Vec<(Occur, Box<dyn Query>)> = stems
            .into_iter()
            .map(|stem| {
                let term = Term::from_field_text(self.stemmed_field, &stem);
                let query: Box<dyn Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Should, query)
            })
            .collect();
        Some(Box::new(BooleanQuery::new(clauses)))
    }

    fn token_query(&self, token: &str, query: &TextQuery, is_last: bool) -> Box<dyn Query> {
        let term = Term::from_field_text(self.text_field, token);
        let widened = query.fuzzy > 0 || (query.prefix && is_last);
        if !widened {
            return Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
        }

        // Automaton queries — fuzzy and prefix alike — score every matching
        // document the same, so on their own they rank by nothing. Union the
        // widened query with the exact term query, which does carry BM25: an
        // exact match then outranks an approximate one, and exact matches keep
        // their usual short-literal-first ordering among themselves.
        let widened: Box<dyn Query> = if query.prefix && is_last {
            // Transpositions count as one edit: the common typo is two adjacent
            // letters swapped, and charging it two edits puts it out of reach
            // at distance 1.
            Box::new(FuzzyTermQuery::new_prefix(term.clone(), query.fuzzy, true))
        } else {
            Box::new(FuzzyTermQuery::new(term.clone(), query.fuzzy, true))
        };
        Box::new(BooleanQuery::new(vec![
            (
                Occur::Should,
                Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs)) as Box<dyn Query>,
            ),
            (Occur::Should, widened),
        ]))
    }

    /// A disjunction over the indexed language tags the requested ranges match.
    ///
    /// Untagged literals are always included. A tag such as `@de` positively
    /// asserts a language the client did not ask for, while an untagged literal
    /// asserts nothing and is often language-neutral by nature — a chemical
    /// name, a gene symbol, an accession. Excluding those from a
    /// language-filtered search would hide exactly the strings a cross-language
    /// client most wants.
    ///
    /// That inclusion is unconditional, and deliberately so. Suppressing
    /// untagged documents when no requested range names an indexed tag would
    /// make an untagged literal's visibility depend on whether some *unrelated*
    /// language happens to be in the index — `--lang de` and `--lang fr` would
    /// answer differently about a chemical formula that is neither.
    fn language_query(&self, ranges: &[String]) -> Option<Box<dyn Query>> {
        if ranges.is_empty() {
            return None;
        }
        let mut tags: Vec<&str> = self
            .manifest
            .languages
            .iter()
            .map(|language| language.tag.as_str())
            .filter(|tag| {
                *tag == UNDETERMINED_LANGUAGE
                    || ranges.iter().any(|range| language_matches(range, tag))
            })
            .collect();
        tags.sort_unstable();

        let clauses: Vec<(Occur, Box<dyn Query>)> = tags
            .into_iter()
            .map(|tag| {
                let term = Term::from_field_text(self.lang_field, tag);
                let query: Box<dyn Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic));
                (Occur::Should, query)
            })
            .collect();
        Some(Box::new(BooleanQuery::new(clauses)))
    }
}

/// The stemming analyzers an index's documents were built with, derived from
/// the languages its manifest records.
///
/// A query carries no language of its own, so it is stemmed by every algorithm
/// present in the index and the results unioned. The alternative — asking the
/// user which language they are typing — would be a worse trade: the set is
/// small, and most stemmers leave a short word untouched.
fn build_stemmers(manifest: &TextManifest) -> Vec<TextAnalyzer> {
    let mut languages: Vec<Language> = manifest
        .languages
        .iter()
        .filter_map(|language| {
            if language.tag == UNDETERMINED_LANGUAGE {
                // Untagged documents were stemmed as whatever the build
                // assumed, which the manifest records precisely so that a
                // query can reproduce it rather than guess.
                manifest
                    .untagged_language
                    .as_deref()
                    .and_then(stemmer_language)
            } else {
                stemmer_language(&language.tag)
            }
        })
        .collect();
    languages.sort_by_key(|language| format!("{language:?}"));
    languages.dedup();
    languages.into_iter().map(stemming_tokenizer).collect()
}

/// Reduce one already-analyzed token to its stem.
///
/// Run through the full chain rather than the bare stemmer, so a query token
/// takes exactly the path an indexed token took.
fn stem_with(analyzer: &mut TextAnalyzer, token: &str) -> String {
    let mut stream = analyzer.token_stream(token);
    if stream.advance() {
        stream.token().text.clone()
    } else {
        token.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::analyzer::{ANALYZER_ID, DatatypeExclusions};
    use crate::text::schema::{SCHEMA_ID, text_schema};
    use tantivy::{IndexWriter, TantivyDocument};

    fn fixture(object_ids: &[u64]) -> Result<(Index, TextSearcher)> {
        let index = Index::create_in_ram(text_schema());
        register_tokenizer(&index);
        let schema = index.schema();
        let text_field = schema.get_field(FIELD_TEXT)?;
        let stemmed_field = schema.get_field(FIELD_TEXT_STEMMED)?;
        let object_field = schema.get_field(FIELD_OBJECT)?;
        let lang_field = schema.get_field(FIELD_LANG)?;

        let mut writer: IndexWriter<TantivyDocument> =
            index.writer_with_num_threads(1, 20 * 1024 * 1024)?;
        for object_id in object_ids {
            let mut document = TantivyDocument::new();
            document.add_text(text_field, "shared");
            document.add_u64(object_field, *object_id);
            document.add_text(lang_field, UNDETERMINED_LANGUAGE);
            writer.add_document(document)?;
        }
        writer.commit()?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let analyzer = index
            .tokenizers()
            .get(TOKENIZER_NAME)
            .context("hdtc tokenizer was not registered")?;
        Ok((
            index,
            TextSearcher {
                reader,
                analyzer,
                manifest: TextManifest {
                    analyzer_id: ANALYZER_ID,
                    schema_id: SCHEMA_ID,
                    tantivy_writer: "test".to_string(),
                    tantivy_index_format: None,
                    source_digest: [0; 32],
                    max_literal_bytes: 4096,
                    untagged_language: None,
                    literals_scanned: object_ids.len() as u64,
                    indexed_docs: object_ids.len() as u64,
                    excluded_oversize: 0,
                    excluded_datatype: 0,
                    excluded_no_tokens: 0,
                    exclusions: DatatypeExclusions::default(),
                    languages: Vec::new(),
                },
                text_field,
                stemmed_field,
                lang_field,
                stemmers: Vec::new(),
            },
        ))
    }

    fn shared() -> TextQuery {
        TextQuery {
            text: "shared".to_string(),
            ..TextQuery::default()
        }
    }

    #[test]
    fn top_k_applies_the_object_id_tiebreak_before_truncation() -> Result<()> {
        // Deliberately make DocAddress order the reverse of object-ID order.
        // All documents have the same term frequency and length, so their BM25
        // scores tie exactly at the top-k boundary.
        let (_index, searcher) = fixture(&[30, 20, 10])?;

        let hits = searcher.search(&shared(), 2)?;
        assert_eq!(
            hits.iter().map(|hit| hit.object_id).collect::<Vec<_>>(),
            [10, 20]
        );
        Ok(())
    }

    #[test]
    fn ranked_search_stops_scoring_at_the_candidate_limit() -> Result<()> {
        let (_index, searcher) = fixture(&[30, 20, 10])?;

        let bounded = searcher.search_up_to(&shared(), 2, 2)?;
        assert_eq!(bounded.examined, 2);
        assert!(!bounded.complete);
        assert_eq!(
            bounded
                .hits
                .iter()
                .map(|hit| hit.object_id)
                .collect::<Vec<_>>(),
            [20, 30],
            "an unexamined lower object ID must not enter the heap"
        );

        let whole = searcher.search_up_to(&shared(), 2, 3)?;
        assert_eq!(whole.examined, 3);
        assert!(whole.complete);
        assert_eq!(
            whole
                .hits
                .iter()
                .map(|hit| hit.object_id)
                .collect::<Vec<_>>(),
            [10, 20]
        );
        Ok(())
    }

    #[test]
    fn matching_object_scan_resumes_without_loss_or_duplication() -> Result<()> {
        let (_index, searcher) = fixture(&[30, 20, 10])?;

        assert!(
            searcher
                .scan_matching_objects(&shared(), Some(TextScanPosition::decode(0)), 2)
                .is_err(),
            "the beginning is not a continuation the index issued"
        );
        assert!(
            searcher
                .scan_matching_objects(
                    &TextQuery {
                        text: "!!!".to_owned(),
                        ..TextQuery::default()
                    },
                    Some(TextScanPosition::decode(1)),
                    2,
                )
                .is_err(),
            "a tokenless query cannot have issued a continuation"
        );

        let first = searcher.scan_matching_objects(&shared(), None, 2)?;
        assert_eq!(first.object_ids, [30, 20]);
        assert!(!first.complete);
        let next = first.next.expect("a full page carries a position");
        assert_eq!(TextScanPosition::decode(next.encode()), next);

        let second = searcher.scan_matching_objects(&shared(), Some(next), 2)?;
        assert_eq!(second.object_ids, [10]);
        assert!(second.complete);
        assert_eq!(second.next, None);
        Ok(())
    }
}
