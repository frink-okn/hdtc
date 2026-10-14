//! Parallel in-place sort whose stack depth does not depend on the data.
//!
//! The external sorter's chunks are the whole sort budget, billions of items,
//! and rayon's `par_sort_unstable` cannot be trusted with them. Its quicksort
//! hands both sides of every partition to `join` while either side is longer
//! than 2,000 items, and each `join` restarts the pattern-defeating bookkeeping
//! that would otherwise answer a run of lopsided partitions with heapsort. So
//! every lopsided partition costs a level of worker stack, and the data decides
//! how many there are: a 40,000-item adversarial input takes one worker past
//! 5 MiB, against 4 KiB for the standard library's sequential sort. hdtc's SPO
//! sort of a 7.7-billion-triple Wikidata chunk aborted with a worker stack
//! overflow. No fixed stack size is safe against a depth that grows with the
//! input, and rayon 1.12 sorts the same way.
//!
//! This sort splits the slice around sampled-median pivots for a fixed number
//! of levels, partitioning each level in parallel, and finishes the pieces with
//! the standard library's `sort_unstable`, whose recursion is bounded. The
//! split depth is set by the thread count alone, and a bad pivot costs balance,
//! never depth.

use rayon::prelude::*;
use std::ops::Range;

/// Size thresholds. They are a value rather than constants so that tests can
/// shrink them, and drive deep splits and many pieces through small inputs.
#[derive(Clone, Copy, Debug)]
struct Tuning {
    /// Slices up to this length are sorted sequentially: below it, splitting
    /// costs more than it parallelizes.
    sequential_len: usize,
    /// The partition pass's pieces are at least this long, bar the last. At
    /// least one. The swap pass's pairs are at most one piece long, and can
    /// be shorter: a stray range cut by a piece boundary may be a single item.
    min_piece_len: usize,
}

const TUNING: Tuning = Tuning {
    sequential_len: 1 << 16,
    min_piece_len: 1 << 14,
};

impl Tuning {
    fn piece_len(self, len: usize) -> usize {
        len.div_ceil(rayon::current_num_threads() * PIECES_PER_THREAD)
            .max(self.min_piece_len)
    }
}

/// Items sampled to choose a pivot. Odd, so the median is one item.
const SAMPLE_LEN: usize = 255;

/// Sorted pieces per thread. More than one lets threads that finish early take
/// up the slack when pieces come out uneven.
const PIECES_PER_THREAD: usize = 4;

/// Sort `v` unstably, in place, on the current rayon pool, and report whether
/// it came out sorted.
///
/// It always does unless this module has a bug, and a caller about to write
/// the result where nothing else checks its order should treat `false` as an
/// error. The check is a parallel read-only pass, skipped when `v` was already
/// sorted on entry, since the entry check covered it.
pub(crate) fn sort_unstable<T: Ord + Send + Clone>(v: &mut [T]) -> bool {
    match sort_tuned(v, TUNING) {
        None => true,
        Some(_depth) => is_sorted_in_pieces(v, TUNING.piece_len(v.len())),
    }
}

/// Sort `v` under the given thresholds. Returns `None` if `v` was already
/// sorted, and otherwise how many levels deep `split` recursed.
fn sort_tuned<T: Ord + Send + Clone>(v: &mut [T], tuning: Tuning) -> Option<u32> {
    // Splitting would partition an already-sorted slice once per level, where
    // a read-only pass can tell. Unsorted input fails the check at once.
    if is_sorted_in_pieces(v, tuning.piece_len(v.len())) {
        return None;
    }
    Some(split(v, split_levels(), tuning))
}

/// How many levels `split` may recurse: enough for `PIECES_PER_THREAD` pieces
/// per thread. The thread count alone sets it.
fn split_levels() -> u32 {
    (rayon::current_num_threads() * PIECES_PER_THREAD)
        .next_power_of_two()
        .trailing_zeros()
}

/// Returns how many levels deep it split, which is at most `levels`.
fn split<T: Ord + Send + Clone>(v: &mut [T], levels: u32, tuning: Tuning) -> u32 {
    if levels == 0 || v.len() <= tuning.sequential_len {
        v.sort_unstable();
        return 0;
    }
    let (pivot, common) = sample_pivot(v);
    let piece_len = tuning.piece_len(v.len());
    let less = partition(v, &pivot, piece_len, |item, pivot| item < pivot);
    let (left, rest) = v.split_at_mut(less);
    // A pivot this common would leave all its copies on the right, which then
    // splits no better. Set them apart instead: they are already in place.
    let equal = if common {
        partition(rest, &pivot, piece_len, |item, pivot| item <= pivot)
    } else {
        0
    };
    let right = &mut rest[equal..];
    let (left_depth, right_depth) = rayon::join(
        || split(left, levels - 1, tuning),
        || split(right, levels - 1, tuning),
    );
    1 + left_depth.max(right_depth)
}

/// The median of a pseudo-random sample of `v`, and whether it fills an eighth
/// or more of that sample.
///
/// The positions are pseudo-random rather than evenly spaced because the SPO
/// sort's buffer interleaves fixed-size runs from many remapper threads, and an
/// even stride could alias with them. The seed is the length, so a sort is
/// reproducible.
fn sample_pivot<T: Ord + Clone>(v: &[T]) -> (T, bool) {
    let mut state = (v.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut sample: Vec<&T> = (0..SAMPLE_LEN)
        .map(|_| {
            // xorshift64
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            &v[(state % v.len() as u64) as usize]
        })
        .collect();
    sample.sort_unstable();
    let pivot = sample[SAMPLE_LEN / 2];
    let copies = sample.iter().filter(|&&item| item == pivot).count();
    (pivot.clone(), copies * 8 >= SAMPLE_LEN)
}

/// Reorder `v` in parallel so that every item for which `goes_left(item,
/// pivot)` holds precedes every item for which it does not, and return how
/// many went left.
///
/// Each piece of `piece_len` items is partitioned on its own first. The
/// right-going items then left before the boundary are exactly as many as the
/// left-going items after it, and swapping the two sets finishes the job.
fn partition<T, F>(v: &mut [T], pivot: &T, piece_len: usize, goes_left: F) -> usize
where
    T: Send + Clone,
    F: Fn(&T, &T) -> bool + Sync,
{
    let len = v.len();
    let lefts: Vec<usize> = v
        .par_chunks_mut(piece_len)
        .map_with(pivot.clone(), |pivot, piece| {
            partition_piece(piece, |item| goes_left(item, pivot))
        })
        .collect();
    let boundary: usize = lefts.iter().sum();

    // Stray right-going ranges, as offsets into `v[..boundary]`, and stray
    // left-going ones, as offsets into `v[boundary..]`.
    let mut stray_right = Vec::new();
    let mut stray_left = Vec::new();
    for (index, &left) in lefts.iter().enumerate() {
        let start = index * piece_len;
        let end = (start + piece_len).min(len);
        let split = start + left;
        if split < end.min(boundary) {
            stray_right.push(split..end.min(boundary));
        }
        if start.max(boundary) < split {
            stray_left.push(start.max(boundary) - boundary..split - boundary);
        }
    }
    let (front, back) = v.split_at_mut(boundary);
    pair_up(
        carve(front, &stray_right),
        carve(back, &stray_left),
        piece_len,
    )
    .into_par_iter()
    .for_each(|(right_going, left_going)| right_going.swap_with_slice(left_going));
    boundary
}

/// Lomuto partition of one piece. The swap is unconditional so the loop has no
/// data-dependent branch: when an item stays right, it swaps two right-going
/// items, or an item with itself.
fn partition_piece<T>(v: &mut [T], goes_left: impl Fn(&T) -> bool) -> usize {
    let mut left = 0;
    for index in 0..v.len() {
        let moves = goes_left(&v[index]);
        v.swap(left, index);
        left += usize::from(moves);
    }
    left
}

/// Borrow the given ascending, disjoint ranges of `v` as separate slices.
fn carve<'a, T>(mut rest: &'a mut [T], ranges: &[Range<usize>]) -> Vec<&'a mut [T]> {
    let mut slices = Vec::with_capacity(ranges.len());
    let mut offset = 0;
    for range in ranges {
        let (_, tail) = std::mem::take(&mut rest).split_at_mut(range.start - offset);
        let (slice, tail) = tail.split_at_mut(range.len());
        slices.push(slice);
        rest = tail;
        offset = range.end;
    }
    slices
}

/// Cut two lists of slices with equal total length into pairs of equal-length
/// slices, none longer than `max_len`.
fn pair_up<'a, T>(
    firsts: Vec<&'a mut [T]>,
    seconds: Vec<&'a mut [T]>,
    max_len: usize,
) -> Vec<(&'a mut [T], &'a mut [T])> {
    let mut pairs = Vec::new();
    let (mut firsts, mut seconds) = (firsts.into_iter(), seconds.into_iter());
    let (mut first, mut second): (&mut [T], &mut [T]) = (Default::default(), Default::default());
    loop {
        if first.is_empty() {
            match firsts.next() {
                Some(slice) => first = slice,
                None => break,
            }
        }
        if second.is_empty() {
            match seconds.next() {
                Some(slice) => second = slice,
                None => break,
            }
        }
        let len = first.len().min(second.len()).min(max_len);
        let (first_head, first_tail) = std::mem::take(&mut first).split_at_mut(len);
        let (second_head, second_tail) = std::mem::take(&mut second).split_at_mut(len);
        pairs.push((first_head, second_head));
        first = first_tail;
        second = second_tail;
    }
    pairs
}

fn is_sorted_in_pieces<T: Ord + Send>(v: &mut [T], piece_len: usize) -> bool {
    v.par_chunks_mut(piece_len).all(|piece| piece.is_sorted())
        && v.chunks(piece_len)
            .zip(v.chunks(piece_len).skip(1))
            .all(|(before, after)| before[before.len() - 1] <= after[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(threads: usize, stack_size: usize) -> rayon::ThreadPool {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("sort-test-{index}"))
            .stack_size(stack_size)
            .build()
            .unwrap()
    }

    fn xorshift(seed: u64) -> impl FnMut() -> u64 {
        let mut state = seed | 1;
        move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        }
    }

    fn patterns(len: usize) -> Vec<(&'static str, Vec<u64>)> {
        let n = len as u64;
        let mut next = xorshift(len as u64);
        let mut next_half = xorshift(len as u64 + 1);
        vec![
            ("random", (0..len).map(|_| next()).collect()),
            ("sorted", (0..n).collect()),
            ("reversed", (0..n).rev().collect()),
            ("all equal", vec![7; len]),
            ("three values", (0..n).map(|i| i % 3).collect()),
            (
                "half one value",
                (0..n)
                    .map(|i| if i % 2 == 0 { 42 } else { next_half() })
                    .collect(),
            ),
            ("sawtooth", (0..n).map(|i| i % 1000).collect()),
            ("organ pipe", (0..n).map(|i| i.min(n - i)).collect()),
            (
                "sorted but last",
                (0..n).map(|i| if i + 1 == n { 0 } else { i + 1 }).collect(),
            ),
        ]
    }

    #[test]
    fn sorts_like_the_standard_library() {
        for threads in [1, 3, 8] {
            let pool = pool(threads, 2 << 20);
            for len in [
                0,
                1,
                2,
                1000,
                TUNING.sequential_len,
                TUNING.sequential_len + 1,
                5 * TUNING.sequential_len + 7,
            ] {
                for (name, input) in patterns(len) {
                    let mut expected = input.clone();
                    expected.sort_unstable();
                    let mut actual = input;
                    assert!(pool.install(|| sort_unstable(&mut actual)));
                    assert!(actual == expected, "{name}, {len} items, {threads} threads");
                }
            }
        }
    }

    #[test]
    fn sorts_items_with_heap_data() {
        let mut next = xorshift(3);
        let input: Vec<(u64, String)> = (0..3 * TUNING.sequential_len)
            .map(|_| {
                let key = next() % 5000;
                (key, format!("term {}", key % 17))
            })
            .collect();
        let mut expected = input.clone();
        expected.sort_unstable();
        let mut actual = input;
        assert!(pool(4, 2 << 20).install(|| sort_unstable(&mut actual)));
        assert_eq!(actual, expected);
    }

    /// Pieces of production size, at a length that splits into several.
    #[test]
    fn partition_separates_and_keeps_every_item() {
        let pool = pool(4, 2 << 20);
        let mut next = xorshift(11);
        let input: Vec<u64> = (0..200_003).map(|_| next() % 1000).collect();
        let mut expected = input.clone();
        expected.sort_unstable();
        for pivot in [0, 1, 500, 999, 1000, u64::MAX] {
            for inclusive in [false, true] {
                let goes_left = move |item: &u64, pivot: &u64| {
                    if inclusive {
                        item <= pivot
                    } else {
                        item < pivot
                    }
                };
                let mut v = input.clone();
                let left = pool.install(|| {
                    let piece_len = TUNING.piece_len(v.len());
                    partition(&mut v, &pivot, piece_len, goes_left)
                });
                assert!(
                    v[..left].iter().all(|item| goes_left(item, &pivot)),
                    "pivot {pivot}"
                );
                assert!(
                    !v[left..].iter().any(|item| goes_left(item, &pivot)),
                    "pivot {pivot}"
                );
                v.sort_unstable();
                assert_eq!(v, expected, "pivot {pivot}");
            }
        }
    }

    /// Every small layout of pieces against the boundary: up to 64 items in
    /// pieces of 1 to 9, pivots below, among, and above the values, and both
    /// predicates. The bookkeeping that pairs stray items across the boundary
    /// is where an off-by-one would hide.
    #[test]
    fn partition_handles_every_small_layout() {
        let pool = pool(3, 2 << 20);
        let mut next = xorshift(5);
        for len in 0..=64 {
            let input: Vec<u64> = (0..len).map(|_| next() % 8).collect();
            let mut expected = input.clone();
            expected.sort_unstable();
            for piece_len in 1..=9 {
                for pivot in 0..=8 {
                    for inclusive in [false, true] {
                        let goes_left = move |item: &u64, pivot: &u64| {
                            if inclusive {
                                item <= pivot
                            } else {
                                item < pivot
                            }
                        };
                        let mut v = input.clone();
                        let left = pool.install(|| partition(&mut v, &pivot, piece_len, goes_left));
                        let case = format!(
                            "{len} items, pieces of {piece_len}, pivot {pivot}, inclusive {inclusive}"
                        );
                        assert!(
                            v[..left].iter().all(|item| goes_left(item, &pivot)),
                            "{case}"
                        );
                        assert!(
                            !v[left..].iter().any(|item| goes_left(item, &pivot)),
                            "{case}"
                        );
                        v.sort_unstable();
                        assert_eq!(v, expected, "{case}");
                    }
                }
            }
        }
    }

    /// A false "sorted" would return a chunk unsorted, so every single
    /// adjacent inversion must be caught, at every position against every
    /// piece boundary.
    #[test]
    fn is_sorted_catches_every_inversion() {
        let pool = pool(3, 2 << 20);
        for len in 0..=40u64 {
            for piece_len in 1..=6 {
                let mut sorted: Vec<u64> = (0..len).collect();
                assert!(pool.install(|| is_sorted_in_pieces(&mut sorted, piece_len)));
                let mut equal = vec![3u64; len as usize];
                assert!(pool.install(|| is_sorted_in_pieces(&mut equal, piece_len)));
                for index in 1..len as usize {
                    let mut v = sorted.clone();
                    v.swap(index - 1, index);
                    assert!(
                        !pool.install(|| is_sorted_in_pieces(&mut v, piece_len)),
                        "{len} items, pieces of {piece_len}, inversion at {index}"
                    );
                }
            }
        }
    }

    /// The bound this module exists for: `split` recurses no deeper than the
    /// thread count allows, whatever the data. With thresholds this small,
    /// splitting until the pieces were short would go many levels deeper.
    #[test]
    fn split_depth_is_set_by_the_thread_count() {
        let tuning = Tuning {
            sequential_len: 4,
            min_piece_len: 1,
        };
        for threads in [1, 2, 5, 8] {
            let pool = pool(threads, 2 << 20);
            let levels = pool.install(split_levels);
            for (name, input) in patterns(20_000) {
                let mut v = input;
                let depth = pool.install(|| sort_tuned(&mut v, tuning));
                let case = format!("{name}, {threads} threads");
                assert!(v.is_sorted(), "{case}");
                assert!(depth.unwrap_or(0) <= levels, "{case}: {depth:?} > {levels}");
                // Random input splits evenly to the bound, so the bound is
                // what stopped it.
                if name == "random" {
                    assert_eq!(depth, Some(levels), "{case}");
                }
            }
        }
    }

    /// Thresholds shrunk so that inputs of a few thousand items split to full
    /// depth and partition in many pieces, over random lengths, thresholds,
    /// thread counts, and value ranges from two values to all distinct.
    #[test]
    fn sorts_random_inputs_with_small_thresholds() {
        let mut next = xorshift(17);
        for threads in [1, 2, 3, 5, 8] {
            let pool = pool(threads, 2 << 20);
            let mut cases: Vec<(String, Tuning, Vec<u64>)> = Vec::new();
            for _ in 0..200 {
                let len = (next() % 3000) as usize;
                let range = [2, 10, 1000, u64::MAX][(next() % 4) as usize];
                let tuning = Tuning {
                    sequential_len: 1 + (next() % 32) as usize,
                    min_piece_len: 1 + (next() % 8) as usize,
                };
                let input = (0..len).map(|_| next() % range).collect();
                cases.push((format!("random below {range}"), tuning, input));
            }
            for len in [0, 1, 2, 3, 17, 100, 1000, 4099] {
                for (name, input) in patterns(len) {
                    let tuning = Tuning {
                        sequential_len: 4,
                        min_piece_len: 1,
                    };
                    cases.push((name.to_string(), tuning, input));
                }
            }
            for (name, tuning, input) in cases {
                let mut expected = input.clone();
                expected.sort_unstable();
                let len = input.len();
                let mut actual = input;
                pool.install(|| sort_tuned(&mut actual, tuning));
                assert!(
                    actual == expected,
                    "{name}, {len} items, {threads} threads, {tuning:?}"
                );
            }
        }
    }

    /// A "killer adversary" for quicksort (McIlroy, 1999), run against rayon's
    /// sort: the comparator leaves item values unfixed until the sort compares
    /// two unfixed items, then gives the first the smallest value not yet
    /// used. Fixed this way, the values make rayon's partitions lopsided, and
    /// sorting them again drives its recursion depth up linearly with the
    /// input length.
    fn adversary_for_rayon(len: usize) -> Vec<u64> {
        use std::cell::RefCell;
        const GAS: u64 = u64::MAX;
        thread_local! {
            /// Item values (`GAS` while unfixed) and the next value to fix.
            static STATE: RefCell<(Vec<u64>, u64)> = const { RefCell::new((Vec::new(), 0)) };
        }
        // One thread, so the comparator's state needs no locking, and a stack
        // deep enough for what the adversary does to rayon.
        pool(1, 64 << 20).install(|| {
            STATE.with(|state| *state.borrow_mut() = (vec![GAS; len], 0));
            let mut items: Vec<u32> = (0..len as u32).collect();
            items.par_sort_unstable_by(|&first, &second| {
                STATE.with(|state| {
                    let (values, next) = &mut *state.borrow_mut();
                    let (first, second) = (first as usize, second as usize);
                    if values[first] == GAS && values[second] == GAS {
                        values[first] = *next;
                        *next += 1;
                    }
                    values[first].cmp(&values[second])
                })
            });
            STATE.with(|state| {
                let (values, next) = &mut *state.borrow_mut();
                for value in values.iter_mut().filter(|value| **value == GAS) {
                    *value = *next;
                    *next += 1;
                }
                std::mem::take(values)
            })
        })
    }

    /// The input that takes rayon's `par_sort_unstable` megabytes deep, and
    /// inputs long enough to split, all sort on 128 KiB worker stacks. The
    /// first is shorter than `sequential_len`, so it goes straight to the leaf
    /// sort: should this module ever hand its slices back to rayon's sort,
    /// there or in place of `split`, it overflows the stack and aborts the
    /// test binary. `split_depth_is_set_by_the_thread_count` covers `split`.
    #[test]
    fn sorts_on_small_stacks() {
        let pool = pool(4, 128 << 10);
        let adversarial = adversary_for_rayon(20_000);
        let mut inputs = vec![("adversarial", adversarial)];
        inputs.extend(patterns(4 * TUNING.sequential_len + 3));
        for (name, input) in inputs {
            let mut expected = input.clone();
            expected.sort_unstable();
            let mut actual = input;
            assert!(pool.install(|| sort_unstable(&mut actual)));
            assert!(actual == expected, "{name}");
        }
    }
}
