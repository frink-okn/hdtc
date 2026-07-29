//! `hdtc search --text` — ranked text search, resolved back to triples.
//!
//! The text index answers with object dictionary IDs and a score, and nothing
//! else: it holds no subject and no predicate. This module is
//! the other half of that bargain — it turns each ranked literal into the
//! `(subject, predicate)` pairs that use it, by running the same `? ? <object>`
//! resolution the `??O` pattern search uses.
//!
//! The cost this design trades for its small index is **over-fetch**: a filter
//! the index cannot apply — `--predicate` — is applied after ranking, so
//! filling a page can require walking past many ranked literals. Every run
//! reports how far it walked so the cost can be measured before deciding
//! whether a predicate sidecar is worth building.

use crate::hdt::index_reader::open_index;
use crate::hdt::reader::{
    BitmapTriplesScanner, DictionaryResolver, HdtSectionOffsets, make_writer, open_hdt,
    write_nt_object, write_nt_subject, write_triple_tab,
};
use crate::hdt::search::{Visit, resolve_index_path, resolve_object_page, scan_object_occurrences};
use crate::permutation::PermutationIndex;
use crate::sort::{ExternalSorter, Sortable};
use crate::text::{
    MatchKind, MatchMode, TextHit, TextQuery, TextSearcher, default_text_index_path,
};
use anyhow::{Context, Result, bail};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// How many ranked literals to ask for before the first resolution attempt,
/// relative to the page being filled. Four tolerates literals whose occurrences
/// do not survive predicate filtering; anything more is paid for by growth.
const INITIAL_OVERFETCH: u64 = 4;
/// Floor on the first request, so a two-row page still starts with a useful
/// slice of the ranking.
const MIN_HITS_REQUESTED: u64 = 64;
/// Resolution batches are deliberately much smaller than a full ranking. The
/// occurrence resolver separately caps the aggregate occurrence entries it
/// keeps resident for one batch.
const MIN_RESOLUTION_BATCH: usize = 64;
const MAX_RESOLUTION_BATCH: usize = 262_144;

/// One text search.
#[derive(Debug, Clone)]
pub struct TextSearchOptions<'a> {
    pub hdt_path: &'a Path,
    /// Text index directory; `None` derives `<hdt>.text`.
    pub text_index: Option<&'a Path>,
    pub query: &'a str,
    pub mode: MatchMode,
    pub fuzzy: u8,
    pub prefix: bool,
    /// BCP 47 language ranges; empty means every language.
    pub languages: &'a [String],
    /// Restrict matches to occurrences on this predicate IRI.
    pub predicate: Option<&'a str>,
    /// Append each row's score as a trailing N-Triples comment.
    pub scores: bool,
    pub output: Option<&'a Path>,
    pub count_only: bool,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub memory_limit: usize,
    /// Directory for temporary ranking chunks; `None` uses the system temp dir.
    pub temp_dir: Option<&'a Path>,
    /// HDT-FoQ index path; `None` derives `<hdt>.index.v1-1`.
    pub index_path: Option<&'a Path>,
    /// Resolve each bounded occurrence batch by a sequential triples pass.
    pub no_index: bool,
}

/// Run a text search, writing rows and returning how many were emitted.
pub fn search_text_streaming(options: &TextSearchOptions<'_>) -> Result<u64> {
    let text_dir = options
        .text_index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_text_index_path(options.hdt_path));
    let searcher = TextSearcher::open(&text_dir)?;

    if searcher.analyze(options.query).is_empty() {
        bail!(
            "Query {:?} has no indexable tokens (the analyzer keeps runs of letters and digits; \
             see docs/text-index-format.md §3)",
            options.query
        );
    }
    let query = TextQuery {
        text: options.query.to_string(),
        mode: options.mode,
        fuzzy: options.fuzzy,
        prefix: options.prefix,
        languages: options.languages.to_vec(),
    };

    let (offsets, mut dictionary) = open_hdt(options.hdt_path, options.memory_limit)
        .with_context(|| format!("Failed to open HDT file {}", options.hdt_path.display()))?;

    // A predicate the dataset does not use cannot match anything, which is an
    // empty result rather than an error.
    let predicate_filter = match options.predicate {
        None => None,
        Some(iri) => match dictionary.locate_predicate(iri.as_bytes())? {
            Some(id) => Some(id),
            None => {
                tracing::info!("Predicate {iri} is not in the dictionary; no rows can match");
                return finish_empty(options);
            }
        },
    };

    if !options.count_only && options.limit == Some(0) {
        return finish_empty(options);
    }

    let skip = options.offset.unwrap_or(0);
    let mut resolver = OccurrenceResolver::open(options, &offsets)?;

    // Counting needs neither scores nor ranking. Stream the matching document
    // IDs and resolve them in bounded batches instead of constructing a
    // TopDocs heap and occurrence map proportional to the entire result set.
    if options.count_only {
        let (outcome, total_hits) = count_rows(
            &searcher,
            &query,
            &mut resolver,
            predicate_filter,
            options.memory_limit,
        )?;
        let mut writer = make_writer(options.output)?;
        writeln!(writer, "{}", outcome.rows)?;
        writer.flush()?;
        report_overfetch(&outcome, total_hits);
        return Ok(outcome.rows);
    }

    // An unlimited result still has a defined ranking. Spill scored hits to
    // bounded external-sort chunks, then resolve and emit the merged stream in
    // batches. This keeps both ranking and occurrence memory independent of
    // the number of matching literals.
    let Some(take) = options.limit else {
        return emit_unlimited_rows(
            options,
            &searcher,
            &query,
            &mut resolver,
            &mut dictionary,
            predicate_filter,
            skip,
        );
    };

    let total_hits = searcher.count(&query)?;
    if total_hits == 0 {
        return finish_empty(options);
    };
    tracing::debug!("{total_hits} literal(s) match the query");

    // A bounded page: ask for a slice of the ranking, and widen it only if the
    // page could not be filled. Rows are buffered rather than written during
    // these attempts, because a wider slice re-ranks from the top and would
    // otherwise emit the same rows twice.
    let target = skip.saturating_add(take);
    let mut requested = target
        .saturating_mul(INITIAL_OVERFETCH)
        .max(MIN_HITS_REQUESTED)
        .min(total_hits as u64);
    let (buffer, outcome) = loop {
        let hits = searcher.search(&query, requested as usize)?;
        let exhausted = hits.len() as u64 >= total_hits as u64;
        let mut buffer = Vec::new();
        let mut state = EmitState::new(skip);
        emit_rows(
            &hits,
            &mut resolver,
            &mut dictionary,
            &EmitOptions {
                predicate_filter,
                scores: options.scores,
                take: Some(take),
            },
            &mut state,
            &mut buffer,
        )?;
        let outcome = state.outcome;
        if outcome.rows >= take || exhausted {
            break (buffer, outcome);
        }
        requested = requested.saturating_mul(4).min(total_hits as u64);
        tracing::debug!(
            "Page not filled from {} literal(s); widening to {requested}",
            outcome.literals_resolved
        );
    };

    let mut writer = make_writer(options.output)?;
    writer.write_all(&buffer)?;
    writer.flush()?;
    report_overfetch(&outcome, total_hits);
    Ok(outcome.rows)
}

/// Count matching triple occurrences without ranking the matching literals.
fn count_rows(
    searcher: &TextSearcher,
    query: &TextQuery,
    resolver: &mut OccurrenceResolver,
    predicate_filter: Option<u64>,
    memory_limit: usize,
) -> Result<(EmitOutcome, usize)> {
    let capacity = resolution_batch_capacity(memory_limit);
    let mut object_ids = Vec::with_capacity(capacity);
    let mut outcome = EmitOutcome::default();
    let total_hits = searcher.for_each_matching_object(query, |object_id| {
        object_ids.push(object_id);
        if object_ids.len() >= capacity {
            count_object_batch(&object_ids, resolver, predicate_filter, &mut outcome)?;
            object_ids.clear();
        }
        Ok(())
    })?;
    if !object_ids.is_empty() {
        count_object_batch(&object_ids, resolver, predicate_filter, &mut outcome)?;
    }
    Ok((outcome, total_hits))
}

fn count_object_batch(
    object_ids: &[u64],
    resolver: &mut OccurrenceResolver,
    predicate_filter: Option<u64>,
    outcome: &mut EmitOutcome,
) -> Result<()> {
    resolver.prepare_ids(object_ids)?;
    for &object_id in object_ids {
        let occurrences = resolver.occurrences(object_id, predicate_filter)?;
        outcome.literals_resolved += 1;
        outcome.occurrences_examined += occurrences.examined;
        outcome.rows += occurrences.pairs.len() as u64;
    }
    Ok(())
}

/// Externally sort every hit, then resolve the ranking in bounded batches.
fn emit_unlimited_rows(
    options: &TextSearchOptions<'_>,
    searcher: &TextSearcher,
    query: &TextQuery,
    resolver: &mut OccurrenceResolver,
    dictionary: &mut DictionaryResolver,
    predicate_filter: Option<u64>,
    skip: u64,
) -> Result<u64> {
    let work_dir = match options.temp_dir {
        Some(parent) => tempfile::Builder::new()
            .prefix("hdtc_text_search_")
            .tempdir_in(parent)
            .with_context(|| {
                format!(
                    "Failed to create text-search work directory in {}",
                    parent.display()
                )
            })?,
        None => tempfile::Builder::new()
            .prefix("hdtc_text_search_")
            .tempdir()
            .context("Failed to create text-search work directory")?,
    };
    let sort_budget = (options.memory_limit / 2).max(64 * 1024);
    let mut sorter = ExternalSorter::new(work_dir.path(), sort_budget);
    let mut sort_buffer = Vec::new();
    let mut sort_memory = 0usize;
    let total_hits = searcher.for_each_ranked_hit(query, |hit| {
        sorter.push(RankedHit(hit), &mut sort_buffer, &mut sort_memory)
    })?;
    if total_hits == 0 {
        return finish_empty(options);
    }
    tracing::debug!("{total_hits} literal(s) match the query");

    // Small result sets stay in memory and avoid a needless temp-file
    // round-trip. Once any chunk has spilled, `finish` performs the bounded
    // merge over it and the final buffer.
    let outcome = if sorter.chunk_file_count() == 0 {
        sort_buffer.sort_unstable();
        emit_ranked_stream(
            sort_buffer.into_iter().map(Ok),
            options,
            resolver,
            dictionary,
            predicate_filter,
            skip,
        )?
    } else {
        let ranking = sorter.finish(&mut sort_buffer)?;
        emit_ranked_stream(
            ranking,
            options,
            resolver,
            dictionary,
            predicate_filter,
            skip,
        )?
    };
    report_overfetch(&outcome, total_hits);
    Ok(outcome.rows)
}

fn emit_ranked_stream(
    ranking: impl Iterator<Item = Result<RankedHit>>,
    options: &TextSearchOptions<'_>,
    resolver: &mut OccurrenceResolver,
    dictionary: &mut DictionaryResolver,
    predicate_filter: Option<u64>,
    skip: u64,
) -> Result<EmitOutcome> {
    let capacity = resolution_batch_capacity(options.memory_limit);
    let mut hits = Vec::with_capacity(capacity);
    let mut state = EmitState::new(skip);
    let emit_options = EmitOptions {
        predicate_filter,
        scores: options.scores,
        take: None,
    };
    let mut writer = make_writer(options.output)?;

    for ranked in ranking {
        hits.push(ranked?.0);
        if hits.len() >= capacity {
            emit_rows(
                &hits,
                resolver,
                dictionary,
                &emit_options,
                &mut state,
                &mut writer,
            )?;
            hits.clear();
        }
    }
    if !hits.is_empty() {
        emit_rows(
            &hits,
            resolver,
            dictionary,
            &emit_options,
            &mut state,
            &mut writer,
        )?;
    }
    writer.flush()?;
    Ok(state.outcome)
}

fn resolution_batch_capacity(memory_limit: usize) -> usize {
    (memory_limit / (std::mem::size_of::<TextHit>() * 16))
        .clamp(MIN_RESOLUTION_BATCH, MAX_RESOLUTION_BATCH)
}

/// Emit the zero-result form: a `0` under `--count`, nothing otherwise.
fn finish_empty(options: &TextSearchOptions<'_>) -> Result<u64> {
    if options.count_only {
        let mut writer = make_writer(options.output)?;
        writeln!(writer, "0")?;
        writer.flush()?;
    } else if options.output.is_some() {
        // An empty output file still has to exist; a caller reading it back
        // should find it empty rather than missing.
        make_writer(options.output)?.flush()?;
    }
    Ok(0)
}

fn report_overfetch(outcome: &EmitOutcome, total_hits: usize) {
    tracing::info!(
        "Text search: {} row(s) from {} of {total_hits} matching literal(s), {} occurrence(s) \
         examined",
        outcome.rows,
        outcome.literals_resolved,
        outcome.occurrences_examined
    );
}

/// Disk representation of a ranked text hit. Its `Ord` is the normative
/// result order: match class, descending score, then ascending object ID.
#[derive(Debug, Clone, Copy)]
struct RankedHit(TextHit);

impl PartialEq for RankedHit {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RankedHit {}

impl PartialOrd for RankedHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .kind
            .cmp(&other.0.kind)
            .then_with(|| other.0.score.total_cmp(&self.0.score))
            .then_with(|| self.0.object_id.cmp(&other.0.object_id))
    }
}

impl Sortable for RankedHit {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        let kind = match self.0.kind {
            MatchKind::Exact => 0,
            MatchKind::Stemmed => 1,
        };
        writer.write_all(&[kind])?;
        writer.write_all(&self.0.score.to_le_bytes())?;
        writer.write_all(&self.0.object_id.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut kind = [0u8; 1];
        match reader.read(&mut kind)? {
            0 => return Ok(None),
            1 => {}
            _ => unreachable!("one-byte read buffer"),
        }
        let kind = match kind[0] {
            0 => MatchKind::Exact,
            1 => MatchKind::Stemmed,
            value => bail!("Invalid text ranking match class {value}"),
        };
        let mut score = [0u8; 4];
        let mut object_id = [0u8; 8];
        reader.read_exact(&mut score)?;
        reader.read_exact(&mut object_id)?;
        Ok(Some(Self(TextHit {
            object_id: u64::from_le_bytes(object_id),
            score: f32::from_le_bytes(score),
            kind,
        })))
    }

    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

// ---------------------------------------------------------------------------
// Row emission
// ---------------------------------------------------------------------------

struct EmitOptions {
    predicate_filter: Option<u64>,
    scores: bool,
    take: Option<u64>,
}

#[derive(Debug, Default, Clone, Copy)]
struct EmitOutcome {
    /// Rows emitted after `skip`.
    rows: u64,
    /// Ranked literals whose occurrences were resolved.
    literals_resolved: usize,
    /// `(subject, predicate)` pairs examined, before filtering.
    occurrences_examined: u64,
}

struct EmitState {
    outcome: EmitOutcome,
    remaining_skip: u64,
}

impl EmitState {
    fn new(skip: u64) -> Self {
        Self {
            outcome: EmitOutcome::default(),
            remaining_skip: skip,
        }
    }
}

/// Walk the ranking, resolve each literal's occurrences, and write the rows
/// that survive filtering.
fn emit_rows(
    hits: &[TextHit],
    resolver: &mut OccurrenceResolver,
    dictionary: &mut DictionaryResolver,
    options: &EmitOptions,
    state: &mut EmitState,
    writer: &mut impl Write,
) -> Result<bool> {
    if options.take == Some(0) {
        return Ok(true);
    }
    let object_ids: Vec<u64> = hits.iter().map(|hit| hit.object_id).collect();
    resolver.prepare_ids(&object_ids)?;
    let mut object_buf = Vec::new();
    let mut subject_buf = Vec::new();
    let mut predicate_buf = Vec::new();

    for hit in hits {
        // Collected rather than visited in place, because writing needs the
        // dictionary and the resolver may already be borrowing the HDT.
        let occurrences = resolver.occurrences(hit.object_id, options.predicate_filter)?;
        state.outcome.literals_resolved += 1;
        state.outcome.occurrences_examined += occurrences.examined;
        if occurrences.pairs.is_empty() {
            continue;
        }

        let mut object_resolved = false;
        for (subject, predicate) in occurrences.pairs {
            if state.remaining_skip > 0 {
                state.remaining_skip -= 1;
                continue;
            }

            state.outcome.rows += 1;
            if !object_resolved {
                dictionary
                    .object_term(hit.object_id, &mut object_buf)
                    .with_context(|| format!("Failed to resolve object ID {}", hit.object_id))?;
                object_resolved = true;
            }
            dictionary
                .subject_term(subject, &mut subject_buf)
                .with_context(|| format!("Failed to resolve subject ID {subject}"))?;
            dictionary
                .predicate_term(predicate, &mut predicate_buf)
                .with_context(|| format!("Failed to resolve predicate ID {predicate}"))?;
            if options.scores {
                write_scored_triple_tab(
                    writer,
                    &subject_buf,
                    &predicate_buf,
                    &object_buf,
                    hit.score,
                )?;
            } else {
                write_triple_tab(writer, &subject_buf, &predicate_buf, &object_buf)?;
            }

            if options.take.is_some_and(|take| state.outcome.rows >= take) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Write a scored result as valid N-Triples, with the diagnostic score in a
/// trailing comment rather than an extra data column.
fn write_scored_triple_tab(
    writer: &mut impl Write,
    subject: &[u8],
    predicate: &[u8],
    object: &[u8],
    score: f32,
) -> std::io::Result<()> {
    write_nt_subject(writer, subject)?;
    writer.write_all(b"\t<")?;
    writer.write_all(predicate)?;
    writer.write_all(b">\t")?;
    write_nt_object(writer, object)?;
    writeln!(writer, "\t. # score={score:.4}")
}

// ---------------------------------------------------------------------------
// Turning an object ID into its occurrences
// ---------------------------------------------------------------------------

/// The two ways to find every `(subject, predicate)` that uses an object.
enum OccurrenceResolver {
    /// Through the OPS permutation, reading each object's contiguous run.
    Permutation(PermutationResolver),
    /// Through the HDT-FoQ index, resolving a whole page in one pass.
    Indexed(IndexedResolver),
    /// One sequential pass over every triple, shared by every literal.
    ///
    /// The fallback for `--no-index`. Each bounded literal batch requires one
    /// full triples pass; this is intentionally a small-file escape hatch, not
    /// the scalable resolution path.
    Scanned(ScannedResolver),
}

struct PermutationResolver {
    index: PermutationIndex,
    memory_limit: usize,
    occurrences: HashMap<u64, Vec<(u64, u64)>>,
}

struct IndexedResolver {
    hdt_path: PathBuf,
    index_path: PathBuf,
    index: crate::hdt::index_reader::IndexSectionOffsets,
    offsets: HdtSectionOffsets,
    memory_limit: usize,
    /// Occurrences for the literals of the current attempt, resolved together.
    /// A literal whose group was too large to batch is absent and is resolved
    /// on demand instead.
    occurrences: HashMap<u64, Vec<(u64, u64)>>,
}

struct ScannedResolver {
    hdt_path: PathBuf,
    offsets: HdtSectionOffsets,
    /// Occurrences by object ID for the literals of the current attempt.
    occurrences: HashMap<u64, Vec<(u64, u64)>>,
}

impl OccurrenceResolver {
    fn open(options: &TextSearchOptions<'_>, offsets: &HdtSectionOffsets) -> Result<Self> {
        if options.no_index {
            return Ok(Self::Scanned(ScannedResolver {
                hdt_path: options.hdt_path.to_path_buf(),
                offsets: *offsets,
                occurrences: HashMap::new(),
            }));
        }

        if options.index_path.is_none() {
            let perm_path = crate::permutation::canonical_path(options.hdt_path);
            if perm_path.exists() {
                let index = PermutationIndex::open(&perm_path, options.hdt_path)?;
                return Ok(Self::Permutation(PermutationResolver {
                    index,
                    memory_limit: options.memory_limit,
                    occurrences: HashMap::new(),
                }));
            }
        }

        let index_path = resolve_index_path(options.hdt_path, options.index_path);
        if !index_path.exists() {
            bail!(
                "Text search resolves each matching literal through the HDT index.\n\
                 Expected: {}\n\
                 Run `hdtc index {}` to create one, or pass `--no-index` to resolve them with a \
                 sequential scan instead.",
                index_path.display(),
                options.hdt_path.display()
            );
        }
        let index = open_index(&index_path)
            .with_context(|| format!("Failed to read index file {}", index_path.display()))?;
        Ok(Self::Indexed(IndexedResolver {
            hdt_path: options.hdt_path.to_path_buf(),
            index_path,
            index,
            offsets: *offsets,
            memory_limit: options.memory_limit,
            occurrences: HashMap::new(),
        }))
    }

    /// Do the work that a whole attempt's worth of literals can share.
    ///
    /// Both resolvers scan a structure sized by the *dataset*, so doing it once
    /// per page rather than once per literal is what keeps a page's cost from
    /// scaling with its size: the sequential resolver reads every triple once,
    /// and the indexed one makes a single pass over bitmapIndexZ and BitmapY
    /// for the whole page.
    fn prepare_ids(&mut self, object_ids: &[u64]) -> Result<()> {
        let resolver = match self {
            Self::Permutation(resolver) => {
                resolver.occurrences.clear();
                let pair_budget =
                    (resolver.memory_limit / std::mem::size_of::<(u64, u64)>()).max(1);
                let mut stored = 0usize;
                for &object_id in object_ids {
                    let mut pairs = Vec::new();
                    for triple in resolver.index.triples(None, Some(object_id))? {
                        let (subject, predicate, _) = triple?;
                        if stored.saturating_add(pairs.len()) >= pair_budget {
                            pairs.clear();
                            break;
                        }
                        pairs.push((subject, predicate));
                    }
                    if !pairs.is_empty() {
                        stored = stored.saturating_add(pairs.len());
                        resolver.occurrences.insert(object_id, pairs);
                    }
                }
                return Ok(());
            }
            Self::Indexed(resolver) => {
                tracing::debug!(
                    "Resolving {} ranked literal(s) in one pass",
                    object_ids.len()
                );
                resolver.occurrences = resolve_object_page(
                    &resolver.hdt_path,
                    &resolver.index_path,
                    &resolver.index,
                    object_ids,
                    &resolver.offsets,
                    resolver.memory_limit,
                )?;
                return Ok(());
            }
            Self::Scanned(resolver) => resolver,
        };
        let wanted: HashSet<u64> = object_ids.iter().copied().collect();
        tracing::debug!(
            "Scanning all triples to resolve {} ranked literal(s) (--no-index)",
            wanted.len()
        );
        resolver.occurrences.clear();
        let mut scanner = BitmapTriplesScanner::new(&resolver.offsets, &resolver.hdt_path)?;
        while let Some((subject, predicate, object)) = scanner.next_triple()? {
            if wanted.contains(&object) {
                resolver
                    .occurrences
                    .entry(object)
                    .or_default()
                    .push((subject, predicate));
            }
        }
        scanner.finish()?;
        // BitmapTriples is scanned in SPO order; text results promise the same
        // OPS occurrence order as the indexed resolver.
        for pairs in resolver.occurrences.values_mut() {
            pairs.sort_unstable_by_key(|(subject, predicate)| (*predicate, *subject));
        }
        Ok(())
    }

    /// Every `(subject, predicate)` using `object_id`, in OPS order.
    fn occurrences(
        &mut self,
        object_id: u64,
        predicate_filter: Option<u64>,
    ) -> Result<ResolvedOccurrences> {
        match self {
            Self::Permutation(resolver) => {
                if let Some(pairs) = resolver.occurrences.get(&object_id) {
                    return Ok(ResolvedOccurrences {
                        examined: pairs.len() as u64,
                        pairs: pairs
                            .iter()
                            .copied()
                            .filter(|(_, predicate)| {
                                predicate_filter.is_none_or(|wanted| *predicate == wanted)
                            })
                            .collect(),
                    });
                }
                let mut pairs = Vec::new();
                let mut examined = 0u64;
                for triple in resolver.index.triples(None, Some(object_id))? {
                    let (subject, predicate, _) = triple?;
                    examined += 1;
                    if predicate_filter.is_none_or(|wanted| predicate == wanted) {
                        pairs.push((subject, predicate));
                    }
                }
                Ok(ResolvedOccurrences { pairs, examined })
            }
            Self::Indexed(resolver) => {
                // Resolved with the rest of the page unless its group was too
                // large to hold; only then does it cost a pass of its own.
                if let Some(pairs) = resolver.occurrences.get(&object_id) {
                    return Ok(ResolvedOccurrences {
                        examined: pairs.len() as u64,
                        pairs: pairs
                            .iter()
                            .copied()
                            .filter(|(_, predicate)| {
                                predicate_filter.is_none_or(|wanted| *predicate == wanted)
                            })
                            .collect(),
                    });
                }
                let mut pairs = Vec::new();
                let mut examined = 0u64;
                scan_object_occurrences(
                    &resolver.hdt_path,
                    &resolver.index_path,
                    &resolver.index,
                    object_id,
                    None,
                    None,
                    &resolver.offsets,
                    resolver.memory_limit,
                    None,
                    &mut |subject, predicate| {
                        examined += 1;
                        if predicate_filter.is_none_or(|wanted| predicate == wanted) {
                            pairs.push((subject, predicate));
                        }
                        Ok(Visit::Continue)
                    },
                )?;
                Ok(ResolvedOccurrences { pairs, examined })
            }
            Self::Scanned(resolver) => {
                let Some(pairs) = resolver.occurrences.get(&object_id) else {
                    return Ok(ResolvedOccurrences::default());
                };
                Ok(ResolvedOccurrences {
                    examined: pairs.len() as u64,
                    pairs: pairs
                        .iter()
                        .copied()
                        .filter(|(_, predicate)| {
                            predicate_filter.is_none_or(|wanted| *predicate == wanted)
                        })
                        .collect(),
                })
            }
        }
    }
}

#[derive(Debug, Default)]
struct ResolvedOccurrences {
    pairs: Vec<(u64, u64)>,
    /// Pair count before applying `--predicate`.
    examined: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spilled_ranked_hits_round_trip_in_normative_order() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut sorter = ExternalSorter::new(temp.path(), 1);
        let mut buffer = Vec::new();
        let mut memory = 0;
        for hit in [
            TextHit {
                object_id: 9,
                score: 50.0,
                kind: MatchKind::Stemmed,
            },
            TextHit {
                object_id: 7,
                score: 2.0,
                kind: MatchKind::Exact,
            },
            TextHit {
                object_id: 3,
                score: 2.0,
                kind: MatchKind::Exact,
            },
            TextHit {
                object_id: 1,
                score: 4.0,
                kind: MatchKind::Exact,
            },
        ] {
            sorter.push(RankedHit(hit), &mut buffer, &mut memory)?;
        }
        let hits: Vec<TextHit> = sorter
            .finish(&mut buffer)?
            .map(|hit| hit.map(|ranked| ranked.0))
            .collect::<Result<_>>()?;

        assert_eq!(
            hits.iter().map(|hit| hit.object_id).collect::<Vec<_>>(),
            vec![1, 3, 7, 9]
        );
        Ok(())
    }
}
