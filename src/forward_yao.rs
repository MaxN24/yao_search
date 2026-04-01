use std::{
    io::{self, IsTerminal},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use hashbrown::HashMap;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::{
    cache::Cache, constants::UPPER_BOUNDS, free_poset::FreePoset, poset::Poset,
    pseudo_canonified_poset::PseudoCanonifiedPoset, search_forward::Cost, utils::format_duration,
};

pub struct Search<'a> {
    m: u8,
    n: u8,
    i: u8,
    current_max: u8,
    cache: &'a mut Cache,
    analytics: Analytics,
    comparisons: &'a mut HashMap<PseudoCanonifiedPoset, (u8, u8)>,
    use_prune: bool,
}

pub struct Analytics {
    total_posets: u64,
    cache_hits: u64,
    cache_misses: u64,
    cache_replaced: u64,
    max_progress_depth: u8,
    multiprogress: MultiProgress,
    progress_bars: Vec<(ProgressBar, AtomicU64)>,
    plain_progress: bool,
    next_plain_report: AtomicU64,
}

impl<'a> Search<'a> {
    pub fn new(
        m: u8,
        n: u8,
        i: u8,
        cache: &'a mut Cache,
        comparisons: &'a mut HashMap<PseudoCanonifiedPoset, (u8, u8)>,
        use_prune: bool,
    ) -> Self {
        assert!(n > 0, "subset size n must be > 0");
        assert!(n <= m, "subset size n must satisfy n <= m");
        assert!(i < n, "rank i must satisfy i < n (0-based)");

        Search {
            m,
            n,
            i,
            current_max: 0,
            cache,
            analytics: Analytics::new(m.max(4) - 3),
            comparisons,
            use_prune,
        }
    }

    fn search_cache(&mut self, poset: &PseudoCanonifiedPoset) -> Option<Cost> {
        let result = self.cache.get_mut(poset);
        if result.is_some() {
            self.analytics.record_hit();
        } else {
            self.analytics.record_miss();
        }
        result
    }

    fn insert_cache(&mut self, poset: PseudoCanonifiedPoset, new_cost: Cost) {
        if let Some(cost) = self.cache.get(&poset) {
            let res = match (cost, new_cost) {
                (Cost::Minimum(old_min), Cost::Minimum(new_min)) => {
                    Cost::Minimum(new_min.max(old_min))
                }
                (Cost::Solved(old_solved), Cost::Solved(new_solved)) => {
                    Cost::Solved(new_solved.min(old_solved))
                }
                (Cost::Solved(_), Cost::Minimum(_)) => cost,
                (Cost::Minimum(_), Cost::Solved(_)) => new_cost,
            };

            let replaced = self.cache.insert(poset, res);
            if replaced {
                self.analytics.record_replace();
            }
        } else {
            let replaced = self.cache.insert(poset, new_cost);
            if replaced {
                self.analytics.record_replace();
            }
        }
    }

    pub fn search(&mut self) -> u8 {
        const PAIR_WISE_OPTIMIZATION: bool = false;

        let start = Instant::now();

        let min = 0;
        let max = UPPER_BOUNDS[self.m as usize][self.i as usize];

        let mut result = max as u8;

        for current in min.. {
            let mut poset = FreePoset::new(self.m, self.i);
            let mut comparisons_done = 0u8;
            if PAIR_WISE_OPTIMIZATION {
                println!("Attention: searching with pairwise-optimisation");
                for k in (0..self.m - 1).step_by(2) {
                    comparisons_done += 1;
                    poset.add_and_close(k, k + 1);
                }
            }

            let current = current as u8 - comparisons_done;
            self.current_max = current;
            self.analytics.set_max_depth(current / 2);

            let search_result = self.search_rec(poset.canonified_without_reduction(), current, 0);

            result = match search_result {
                Cost::Solved(solved) => solved + comparisons_done,
                Cost::Minimum(min) => {
                    self.analytics.multiprogress.clear().unwrap();
                    println!(
                        "m: {}, n: {}, i: {} needs at least {} comparisons",
                        self.m,
                        self.n,
                        self.i,
                        min + comparisons_done
                    );
                    println!("{}", format_duration(start));

                    continue;
                }
            };
            break;
        }

        self.analytics.complete_all();

        println!();
        println!(
            "Congratulations. A solution was found!\n\nm: {}, n: {}, i: {}",
            self.m, self.n, self.i
        );
        println!("Comparisons: {result}");
        println!();

        self.print_cache();
        println!("{}", format_duration(start));
        println!();

        result
    }

    pub fn is_solvable_within(&mut self, max_comparisons: u8) -> bool {
        const PAIR_WISE_OPTIMIZATION: bool = false;

        let mut poset = FreePoset::new(self.m, self.i);
        let mut comparisons_done = 0u8;
        if PAIR_WISE_OPTIMIZATION {
            for k in (0..self.m - 1).step_by(2) {
                comparisons_done += 1;
                poset.add_and_close(k, k + 1);
            }
        }

        if max_comparisons < comparisons_done {
            return false;
        }

        let budget = max_comparisons - comparisons_done;
        self.current_max = budget;
        self.analytics.set_max_depth(budget / 2);

        let result = self.search_rec(poset.canonified_without_reduction(), budget, 0);
        self.analytics.complete_all();

        matches!(result, Cost::Solved(solved) if solved + comparisons_done <= max_comparisons)
    }

    #[allow(clippy::too_many_lines)]
    fn search_rec(&mut self, poset: PseudoCanonifiedPoset, max_comparisons: u8, depth: u8) -> Cost {
        debug_assert_eq!(
            poset.n(),
            self.m,
            "Yao search must keep all m elements; use no-reduction transitions"
        );

        if self.solved(poset) {
            return Cost::Solved(0);
        }

        if max_comparisons == 0 {
            return Cost::Minimum(1);
        }

        if let Some(cost) = self.search_cache(&poset) {
            match cost {
                Cost::Solved(_) => {
                    return cost;
                }
                Cost::Minimum(min) => {
                    if min > max_comparisons {
                        return cost;
                    }
                }
            }
        }

        if self.use_prune && self.yao_prune(&poset, max_comparisons) {
            let result = Cost::Minimum(max_comparisons + 1);
            self.insert_cache(poset, result);
            return result;
        }

        let pairs = poset.get_comparison_pairs_without_reduction(self.n);
        let n_pairs = pairs.len() as u64;

        self.analytics.inc_length(depth, n_pairs);

        let mut best_comparison = (0, 0);
        let mut current_best = max_comparisons + 1;
        for (first, second, i, j) in pairs {
            if current_best <= 1 {
                break;
            }

            self.analytics.update_stats(
                depth,
                self.current_max,
                self.cache.len(),
                self.cache.max_entries(),
            );

            let child_budget = current_best - 2;

            let first_result = self.search_rec(first, child_budget, depth + 1);

            if !first_result.is_solved() || first_result.value() > child_budget {
                self.analytics.inc(depth, 1);
                continue;
            }

            let second_result = self.search_rec(second, child_budget, depth + 1);

            if !second_result.is_solved() || second_result.value() > child_budget {
                self.analytics.inc(depth, 1);
                continue;
            }

            best_comparison = (i, j);

            current_best = first_result.value().max(second_result.value()) + 1;

            self.analytics.inc(depth, 1);
        }

        let result = if current_best <= max_comparisons {
            self.comparisons.insert(poset, best_comparison);
            Cost::Solved(current_best)
        } else {
            Cost::Minimum(max_comparisons + 1)
        };

        self.analytics.inc_complete(depth, n_pairs);

        self.analytics.record_poset();

        self.insert_cache(poset, result);

        result
    }

    fn solved(&self, poset: PseudoCanonifiedPoset) -> bool {
        let (less, greater) = poset.calculate_relations();
        let need_below = self.i as usize;
        let need_above = (self.n - self.i - 1) as usize;

        for s in 0..poset.n() as usize {
            if less[s] as usize >= need_below && greater[s] as usize >= need_above {
                return true;
            }
        }

        false
    }

    #[inline]
    fn min_budget(best: &[usize], required: usize) -> Option<usize> {
        if required == 0 {
            return Some(0);
        }

        best.iter().position(|&covered| covered >= required)
    }

    #[cfg(test)]
    #[inline]
    fn brute_union_coverage(gain_masks: &[u16], max_budget: usize) -> Vec<usize> {
        let mut best = vec![0usize; max_budget + 1];
        let len = gain_masks.len();
        if len == 0 {
            return best;
        }

        let subset_count = 1usize << len;
        let mut unions = vec![0u16; subset_count];

        for subset in 1..subset_count {
            let lsb = subset & subset.wrapping_neg();
            let idx = lsb.trailing_zeros() as usize;
            let prev = subset ^ lsb;

            unions[subset] = unions[prev] | gain_masks[idx];

            let picks = subset.count_ones() as usize;
            if picks <= max_budget {
                let covered = unions[subset].count_ones() as usize;
                if covered > best[picks] {
                    best[picks] = covered;
                }
            }
        }

        for budget in 1..=max_budget {
            if best[budget - 1] > best[budget] {
                best[budget] = best[budget - 1];
            }
        }

        best
    }

    #[inline]
    fn remove_dominated_masks(masks: &mut Vec<u16>) {
        masks.retain(|&mask| mask != 0);
        masks.sort_unstable_by(|a, b| b.count_ones().cmp(&a.count_ones()).then(b.cmp(a)));

        let mut filtered: Vec<u16> = Vec::with_capacity(masks.len());
        'outer: for &mask in masks.iter() {
            for &kept in &filtered {
                if (mask & !kept) == 0 {
                    continue 'outer;
                }
            }
            filtered.push(mask);
        }

        *masks = filtered;
    }

    fn exact_union_coverage(gain_masks: &[u16], max_budget: usize) -> Vec<usize> {
        let mut masks = gain_masks.to_vec();
        Self::remove_dominated_masks(&mut masks);

        let mut best = vec![0usize; max_budget + 1];
        if masks.is_empty() || max_budget == 0 {
            return best;
        }

        masks.sort_unstable_by(|a, b| b.count_ones().cmp(&a.count_ones()).then(b.cmp(a)));

        let len = masks.len();
        let mut suffix_union = vec![0u16; len + 1];
        for idx in (0..len).rev() {
            suffix_union[idx] = suffix_union[idx + 1] | masks[idx];
        }

        fn dfs(
            masks: &[u16],
            suffix_union: &[u16],
            idx: usize,
            used: usize,
            union_mask: u16,
            max_budget: usize,
            best: &mut [usize],
        ) {
            let covered = union_mask.count_ones() as usize;
            if covered > best[used] {
                best[used] = covered;
            }

            if idx == masks.len() || used == max_budget {
                return;
            }

            let remaining = masks.len() - idx;
            let max_extra = (max_budget - used).min(remaining);
            if max_extra == 0 {
                return;
            }

            let union_bound = (union_mask | suffix_union[idx]).count_ones() as usize;
            let mut marginal_additions = [0usize; 16];
            let mut marginal_count = 0usize;
            for &mask in &masks[idx..] {
                marginal_additions[marginal_count] = (mask & !union_mask).count_ones() as usize;
                marginal_count += 1;
            }
            marginal_additions[..marginal_count].sort_unstable_by(|a, b| b.cmp(a));

            let mut dominated_for_all_budgets = true;
            let mut marginal_sum = 0usize;
            for extra in 1..=max_extra {
                marginal_sum += marginal_additions[extra - 1];
                let optimistic_cover = union_bound.min(covered + marginal_sum);
                if optimistic_cover > best[used + extra] {
                    dominated_for_all_budgets = false;
                    break;
                }
            }
            if dominated_for_all_budgets {
                return;
            }

            dfs(
                masks,
                suffix_union,
                idx + 1,
                used + 1,
                union_mask | masks[idx],
                max_budget,
                best,
            );
            dfs(
                masks,
                suffix_union,
                idx + 1,
                used,
                union_mask,
                max_budget,
                best,
            );
        }

        dfs(&masks, &suffix_union, 0, 0, 0, max_budget, &mut best);

        for budget in 1..=max_budget {
            if best[budget - 1] > best[budget] {
                best[budget] = best[budget - 1];
            }
        }

        best
    }

    fn yao_prune(&self, poset: &PseudoCanonifiedPoset, remaining: u8) -> bool {
        let m = poset.n() as usize;
        let r = remaining as usize;

        let min_down = self.i as usize + 1;
        let min_up = self.n as usize - self.i as usize;

        let mut down = vec![0u16; m];
        let mut up = vec![0u16; m];
        for x in 0..m {
            down[x] = poset.get_all_less_than(x as u8).bits() | (1u16 << x);
            up[x] = poset.get_all_greater_than(x as u8).bits() | (1u16 << x);
        }

        let mut down_sets = Vec::with_capacity(m.saturating_sub(1));
        let mut up_sets = Vec::with_capacity(m.saturating_sub(1));

        for s in 0..m {
            let down_s = down[s];
            let up_s = up[s];
            let down_len = down_s.count_ones() as usize;
            let up_len = up_s.count_ones() as usize;

            if down_len >= min_down && up_len >= min_up {
                return false;
            }

            down_sets.clear();
            up_sets.clear();

            let comparable_to_s = down_s | up_s;
            for x in 0..m {
                if x == s || ((comparable_to_s >> x) & 1) != 0 {
                    continue;
                }

                let down_x = down[x] & !down_s;
                let up_x = up[x] & !up_s;

                if down_x != 0 {
                    down_sets.push(down_x);
                }
                if up_x != 0 {
                    up_sets.push(up_x);
                }
            }

            let best_down = Self::exact_union_coverage(&down_sets, r);
            let best_up = Self::exact_union_coverage(&up_sets, r);

            let need_down = min_down.saturating_sub(down_len);
            let need_up = min_up.saturating_sub(up_len);

            let Some(k_min) = Self::min_budget(&best_down, need_down) else {
                continue;
            };
            let Some(j_min) = Self::min_budget(&best_up, need_up) else {
                continue;
            };

            if k_min.saturating_add(j_min) <= r {
                return false;
            }
        }

        true
    }

    #[allow(dead_code)]
    pub fn witness(&self, poset: PseudoCanonifiedPoset) -> Option<(Vec<u8>, u8)> {
        let down_needed = self.i as usize;
        let up_needed = self.n as usize - self.i as usize - 1;

        for s in 0..poset.n() {
            let down = poset.get_all_less_than(s);
            let up = poset.get_all_greater_than(s);

            if down.len() < down_needed || up.len() < up_needed {
                continue;
            }

            let mut subset = Vec::with_capacity(self.n as usize);
            subset.extend(down.into_iter().take(down_needed).map(|idx| idx as u8));
            subset.push(s);
            subset.extend(up.into_iter().take(up_needed).map(|idx| idx as u8));

            debug_assert_eq!(subset.len(), self.n as usize);
            return Some((subset, s));
        }

        None
    }

    pub fn print_cache(&self) {
        println!("Cache entries: {}", self.cache.len());
        println!("Cache size: {:.3} Gigabyte", self.cache.size_as_gigabyte());
        println!("Cache hits: {}", self.analytics.cache_hits());
        println!("Cache misses: {}", self.analytics.cache_misses());
        println!("Cache replaced: {}", self.analytics.cache_replaced());
        println!();
        println!("Posets searched: {}", self.analytics.total_posets());
    }
}

impl Analytics {
    fn new(max_progress_depth: u8) -> Analytics {
        let multiprogress = MultiProgress::new();

        let mut progress_bars = Vec::with_capacity(max_progress_depth as usize);
        for _ in 0..max_progress_depth {
            let pb = ProgressBar::new(0)
                .with_style(ProgressStyle::with_template("[{pos:2}/{len:2}] {msg}").unwrap());
            let pb = multiprogress.add(pb);
            progress_bars.push((pb, AtomicU64::new(0)));
        }
        Analytics {
            total_posets: 0,
            cache_hits: 0,
            cache_misses: 0,
            cache_replaced: 0,
            max_progress_depth,
            multiprogress,
            progress_bars,
            plain_progress: !io::stderr().is_terminal(),
            next_plain_report: AtomicU64::new(1_000),
        }
    }

    fn set_max_depth(&mut self, new_depth: u8) {
        if new_depth > self.max_progress_depth {
            for _ in self.max_progress_depth..new_depth {
                let pb = ProgressBar::new(0)
                    .with_style(ProgressStyle::with_template("[{pos:2}/{len:2}] {msg}").unwrap());
                let pb = self.multiprogress.add(pb);
                self.progress_bars.push((pb, AtomicU64::new(0)));
            }
        } else {
            for _ in new_depth..self.max_progress_depth {
                let (pb, _) = self.progress_bars.pop().unwrap();
                pb.finish_and_clear();
                self.multiprogress.remove(&pb);
            }
        }
        self.max_progress_depth = new_depth;
    }

    #[inline]
    fn inc_length(&self, depth: u8, count: u64) {
        if depth >= self.max_progress_depth {
            return;
        }
        self.progress_bars[depth as usize].0.inc_length(count);
        self.progress_bars[depth as usize]
            .1
            .fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    fn inc(&self, depth: u8, amount: u64) {
        if depth >= self.max_progress_depth {
            return;
        }
        self.progress_bars[depth as usize].0.inc(amount);
    }

    #[inline]
    fn inc_complete(&self, depth: u8, count: u64) {
        if depth >= self.max_progress_depth {
            return;
        }
        let (pb, len) = &self.progress_bars[depth as usize];

        pb.inc(count.wrapping_neg());
        pb.set_length(len.fetch_sub(count, Ordering::Release) - count);
    }

    #[inline]
    fn update_stats(&self, depth: u8, current_max: u8, cache_entries: usize, max_entries: usize) {
        if depth >= self.max_progress_depth {
            return;
        }

        let cache_percentage = cache_entries as f64 / max_entries as f64 * 100.0;
        self.progress_bars[0].0.set_message(format!(
            "limit: {:3} total: {:10}, cache: {:10} ({:2.2} %)",
            current_max, self.total_posets, cache_entries, cache_percentage
        ));

        if self.plain_progress {
            let threshold = self.next_plain_report.load(Ordering::Relaxed);
            if self.total_posets >= threshold
                && self
                    .next_plain_report
                    .compare_exchange(
                        threshold,
                        threshold.saturating_add(1_000),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                eprintln!(
                    "[live] limit={} total={} cache={}/{} ({:.2}%) hits={} misses={} replaced={}",
                    current_max,
                    self.total_posets,
                    cache_entries,
                    max_entries,
                    cache_percentage,
                    self.cache_hits,
                    self.cache_misses,
                    self.cache_replaced
                );
            }
        }
    }

    fn complete_all(&self) {
        for i in 0..self.max_progress_depth as usize {
            let (pb, _) = &self.progress_bars[i];
            pb.finish_and_clear();
            self.multiprogress.remove(pb);
        }
    }

    #[inline]
    fn record_hit(&mut self) {
        self.cache_hits += 1;
    }

    #[inline]
    fn record_miss(&mut self) {
        self.cache_misses += 1;
    }

    #[inline]
    fn record_replace(&mut self) {
        self.cache_replaced += 1;
    }

    #[inline]
    fn record_poset(&mut self) {
        self.total_posets += 1;
    }

    fn cache_hits(&self) -> u64 {
        self.cache_hits
    }

    fn cache_misses(&self) -> u64 {
        self.cache_misses
    }

    fn cache_replaced(&self) -> u64 {
        self.cache_replaced
    }

    fn total_posets(&self) -> u64 {
        self.total_posets
    }
}

impl Drop for Analytics {
    fn drop(&mut self) {
        self.complete_all();
    }
}

#[cfg(test)]
mod tests {
    use super::Search;

    #[test]
    fn exact_union_matches_subset_enumeration() {
        let sets = vec![0b0000_0111, 0b0001_1100, 0b0011_0001, 0b1100_0000];
        let exact = Search::exact_union_coverage(&sets, 4);
        let brute = Search::brute_union_coverage(&sets, 4);
        assert_eq!(exact, brute);
    }

    #[test]
    fn exact_union_ignores_dominated_and_duplicate_sets() {
        let with_dominated = vec![0b0000_0111, 0b0000_0011, 0b0000_0111, 0b0011_0000];
        let reduced = vec![0b0000_0111, 0b0011_0000];
        let exact_with_dominated = Search::exact_union_coverage(&with_dominated, 3);
        let exact_reduced = Search::exact_union_coverage(&reduced, 3);
        assert_eq!(exact_with_dominated, exact_reduced);
    }

    #[test]
    fn exact_union_stays_below_sum_bound() {
        let sets = vec![0b0000_0111, 0b0001_1100, 0b0011_0001];
        let exact = Search::exact_union_coverage(&sets, 3);

        let mut sizes = sets
            .iter()
            .map(|mask| mask.count_ones() as usize)
            .collect::<Vec<_>>();
        sizes.sort_unstable_by(|a, b| b.cmp(a));

        let mut sum_bound = vec![0usize; 4];
        for budget in 1..=3 {
            sum_bound[budget] = sum_bound[budget - 1] + sizes[budget - 1];
        }

        for budget in 0..=3 {
            assert!(exact[budget] <= sum_bound[budget]);
        }
    }
}
