//! `hdtc search --text` — ranked text search, resolved back to triples.
//!
//! The text index answers with object dictionary IDs and a score, and nothing
//! else: it holds no subject and no predicate (doc 19 §19.2.2). This module is
//! the other half of that bargain — it turns each ranked literal into the
//! `(subject, predicate)` pairs that use it, by running the same `? ? <object>`
//! resolution the `??O` pattern search uses.
//!
//! Results are **entity-level and deduplicated by subject** by default (doc 19
//! §19.3): a subject appears once, represented by its highest-ranked matching
//! literal. Without that, one popular string fills a page on its own.
//!
//! The cost this design trades for its small index is **over-fetch**: a filter
//! the index cannot apply — `--predicate`, or subject deduplication itself —
//! is applied after ranking, so filling a page can require walking past many
//! ranked literals. Every run reports how far it walked, which is the
//! measurement doc 19 §19.7 asks for before deciding whether a predicate
//! sidecar is worth building.

use crate::hdt::index_reader::open_index;
use crate::hdt::reader::{
    BitmapTriplesScanner, DictionaryResolver, HdtSectionOffsets, make_writer, open_hdt,
    write_triple_tab,
};
use crate::hdt::search::{Visit, resolve_index_path, resolve_object_page, scan_object_occurrences};
use crate::text::{MatchMode, TextHit, TextQuery, TextSearcher, default_text_index_path};
use anyhow::{Context, Result, bail};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};

/// How many ranked literals to ask for before the first resolution attempt,
/// relative to the page being filled. Four covers the common case where most
/// literals contribute a fresh subject; anything more is paid for by growth.
const INITIAL_OVERFETCH: u64 = 4;
/// Floor on the first request, so a two-row page still starts with a useful
/// slice of the ranking.
const MIN_HITS_REQUESTED: u64 = 64;

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
    /// Collapse to one row per subject.
    pub dedupe: bool,
    /// Prefix each row with its score.
    pub scores: bool,
    pub output: Option<&'a Path>,
    pub count_only: bool,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub memory_limit: usize,
    /// HDT-FoQ index path; `None` derives `<hdt>.index.v1-1`.
    pub index_path: Option<&'a Path>,
    /// Resolve occurrences by one sequential triples pass instead of the index.
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

    let total_hits = searcher.count(&query)?;
    if total_hits == 0 {
        return finish_empty(options);
    }
    tracing::debug!("{total_hits} literal(s) match the query");

    let skip = options.offset.unwrap_or(0);
    let take = if options.count_only {
        None
    } else {
        options.limit
    };
    let mut resolver = OccurrenceResolver::open(options, &offsets)?;

    // Without a row limit every ranked literal has to be resolved anyway, so
    // the page is produced in one pass, streaming straight to the output.
    let Some(take) = take else {
        let hits = searcher.search(&query, total_hits)?;
        let mut writer = make_writer(options.output)?;
        let outcome = emit_rows(
            &hits,
            &mut resolver,
            &mut dictionary,
            &EmitOptions {
                predicate_filter,
                dedupe: options.dedupe,
                scores: options.scores,
                skip,
                take: None,
                count_only: options.count_only,
            },
            &mut writer,
        )?;
        if options.count_only {
            writeln!(writer, "{}", outcome.rows)?;
        }
        writer.flush()?;
        report_overfetch(&outcome, total_hits);
        return Ok(outcome.rows);
    };

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
        let outcome = emit_rows(
            &hits,
            &mut resolver,
            &mut dictionary,
            &EmitOptions {
                predicate_filter,
                dedupe: options.dedupe,
                scores: options.scores,
                skip,
                take: Some(take),
                count_only: options.count_only,
            },
            &mut buffer,
        )?;
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
        "Text search: {} row(s) from {} of {total_hits} ranked literal(s), {} occurrence(s) \
         examined",
        outcome.rows,
        outcome.literals_resolved,
        outcome.occurrences_examined
    );
}

// ---------------------------------------------------------------------------
// Row emission
// ---------------------------------------------------------------------------

struct EmitOptions {
    predicate_filter: Option<u64>,
    dedupe: bool,
    scores: bool,
    skip: u64,
    take: Option<u64>,
    count_only: bool,
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

/// Walk the ranking, resolve each literal's occurrences, and write the rows
/// that survive filtering.
fn emit_rows(
    hits: &[TextHit],
    resolver: &mut OccurrenceResolver,
    dictionary: &mut DictionaryResolver,
    options: &EmitOptions,
    writer: &mut impl Write,
) -> Result<EmitOutcome> {
    resolver.prepare(hits)?;
    let mut outcome = EmitOutcome::default();
    let mut skipped = 0u64;
    let mut seen_subjects: HashSet<u64> = HashSet::new();
    let mut object_buf = Vec::new();
    let mut subject_buf = Vec::new();
    let mut predicate_buf = Vec::new();

    for hit in hits {
        // Collected rather than visited in place, because writing needs the
        // dictionary and the resolver may already be borrowing the HDT.
        let occurrences = resolver.occurrences(hit.object_id, options.predicate_filter)?;
        outcome.literals_resolved += 1;
        outcome.occurrences_examined += occurrences.len() as u64;
        if occurrences.is_empty() {
            continue;
        }

        let mut object_resolved = false;
        for (subject, predicate) in occurrences {
            // Deduplication is by *subject*, not by literal: a subject appears
            // once, represented by its highest-ranked matching literal (doc 19
            // §19.3). A literal used by a thousand different subjects is a
            // thousand entities and yields a row for each — collapsing it to
            // one would hide every entity but the first that happens to share
            // a common name.
            if options.dedupe && !seen_subjects.insert(subject) {
                continue;
            }
            if skipped < options.skip {
                skipped += 1;
                continue;
            }

            outcome.rows += 1;
            if !options.count_only {
                if !object_resolved {
                    dictionary
                        .object_term(hit.object_id, &mut object_buf)
                        .with_context(|| {
                            format!("Failed to resolve object ID {}", hit.object_id)
                        })?;
                    object_resolved = true;
                }
                dictionary
                    .subject_term(subject, &mut subject_buf)
                    .with_context(|| format!("Failed to resolve subject ID {subject}"))?;
                dictionary
                    .predicate_term(predicate, &mut predicate_buf)
                    .with_context(|| format!("Failed to resolve predicate ID {predicate}"))?;
                if options.scores {
                    write!(writer, "{:.4}\t", hit.score)?;
                }
                write_triple_tab(writer, &subject_buf, &predicate_buf, &object_buf)?;
            }

            if options.take.is_some_and(|take| outcome.rows >= take) {
                return Ok(outcome);
            }
        }
    }
    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Turning an object ID into its occurrences
// ---------------------------------------------------------------------------

/// The two ways to find every `(subject, predicate)` that uses an object.
enum OccurrenceResolver {
    /// Through the HDT-FoQ index, resolving a whole page in one pass.
    Indexed(IndexedResolver),
    /// One sequential pass over every triple, shared by every literal.
    ///
    /// The fallback for `--no-index`. It cannot be lazy — the pass has to
    /// finish before the first row is known — so it holds every occurrence of
    /// every ranked literal in memory. Fine for a small file, which is when a
    /// user has no index; the indexed path is what scales.
    Scanned(ScannedResolver),
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
    fn prepare(&mut self, hits: &[TextHit]) -> Result<()> {
        let resolver = match self {
            Self::Indexed(resolver) => {
                let object_ids: Vec<u64> = hits.iter().map(|hit| hit.object_id).collect();
                tracing::debug!(
                    "Resolving {} ranked literal(s) in one pass",
                    object_ids.len()
                );
                resolver.occurrences = resolve_object_page(
                    &resolver.hdt_path,
                    &resolver.index_path,
                    &resolver.index,
                    &object_ids,
                    &resolver.offsets,
                    resolver.memory_limit,
                )?;
                return Ok(());
            }
            Self::Scanned(resolver) => resolver,
        };
        let wanted: HashSet<u64> = hits.iter().map(|hit| hit.object_id).collect();
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
        Ok(())
    }

    /// Every `(subject, predicate)` using `object_id`, in OPS order.
    fn occurrences(
        &mut self,
        object_id: u64,
        predicate_filter: Option<u64>,
    ) -> Result<Vec<(u64, u64)>> {
        match self {
            Self::Indexed(resolver) => {
                // Resolved with the rest of the page unless its group was too
                // large to hold; only then does it cost a pass of its own.
                if let Some(pairs) = resolver.occurrences.get(&object_id) {
                    return Ok(pairs
                        .iter()
                        .copied()
                        .filter(|(_, predicate)| {
                            predicate_filter.is_none_or(|wanted| *predicate == wanted)
                        })
                        .collect());
                }
                let mut pairs = Vec::new();
                scan_object_occurrences(
                    &resolver.hdt_path,
                    &resolver.index_path,
                    &resolver.index,
                    object_id,
                    None,
                    predicate_filter,
                    &resolver.offsets,
                    resolver.memory_limit,
                    None,
                    &mut |subject, predicate| {
                        pairs.push((subject, predicate));
                        Ok(Visit::Continue)
                    },
                )?;
                Ok(pairs)
            }
            Self::Scanned(resolver) => Ok(resolver
                .occurrences
                .get(&object_id)
                .map(|pairs| {
                    pairs
                        .iter()
                        .copied()
                        .filter(|(_, predicate)| {
                            predicate_filter.is_none_or(|wanted| *predicate == wanted)
                        })
                        .collect()
                })
                .unwrap_or_default()),
        }
    }
}
