use backward_cache::BackwardCache;
use backwards_poset::BackwardsPoset;
use clap::{
    error::{Error, ErrorKind},
    ArgAction, Parser,
};
use hashbrown::HashMap;
use pseudo_canonified_poset::PseudoCanonifiedPoset;
use search_backward::iterative_deepening_backward;
use search_forward::Cost;
use std::{
    fs::{DirBuilder, OpenOptions},
    io::{BufWriter, Write},
    str::FromStr,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    cache::Cache,
    constants::{KNOWN_VALUES, MAX_N},
    forward_yao::Search as YaoSearch,
    free_poset::FreePoset,
    poset::Poset,
    search_forward::Search,
    utils::format_duration,
};

mod algorithm_test;
mod backward_cache;
mod backwards_poset;
mod bitset;
mod cache;
mod constants;
mod forward_yao;
mod free_poset;
mod poset;
mod pseudo_canonified_poset;
mod search_backward;
mod search_forward;
mod utils;

#[derive(Debug, Clone)]
pub enum SearchMode {
    Forward,
    ForwardYao,
    Backward,
    Bidirectional,
}

impl FromStr for SearchMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "forward" => Ok(SearchMode::Forward),
            "forward_yao" | "forward-yao" | "yao" => Ok(SearchMode::ForwardYao),
            "backward" => Ok(SearchMode::Backward),
            "bidirectional" => Ok(SearchMode::Bidirectional),
            _ => Err(Error::new(ErrorKind::InvalidValue)),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum WeightFunction {
    CompatibleSolutions,
    Weight0,
    Weight,
    None,
}

impl FromStr for WeightFunction {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cs" => Ok(WeightFunction::CompatibleSolutions),
            "w0" => Ok(WeightFunction::Weight0),
            "w" => Ok(WeightFunction::Weight),
            "none" => Ok(WeightFunction::None),
            _ => Err(Error::new(ErrorKind::InvalidValue)),
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version)]
struct Args {
    #[arg(long)]
    search_mode: SearchMode,
    /// The n to start at, or m for `search_mode=forward_yao`
    #[arg(short)]
    n: Option<u8>,
    /// Subset size n for Yao mode
    #[arg(long)]
    subset_n: Option<u8>,
    /// The i to start at. Requires n.
    #[arg(short, requires("n"))]
    i: Option<u8>,
    /// Do only a single calculation
    #[arg(short, long, default_value_t = false, requires("i"))]
    single: bool,
    #[arg(long)]
    max_core: Option<usize>,
    /// Explore the cache interactively
    #[arg(short, long, default_value_t = false)]
    explore: bool,
    /// The max amount of bytes of the cache
    #[arg(long, default_value_t = 1 << 33)]
    max_cache_size: usize,
    /// Increase verbosity level
    #[clap(short, long, action = ArgAction::Count)]
    verbose: u8,
    /// Print the algorithm which solves the problem
    #[arg(short, long, default_value_t = false)]
    print_algorithm: bool,
    /// Weight function to use for forward search
    #[arg(long, default_value = "w")]
    weight_function: WeightFunction,
    /// In Yao mode, only test budgets classic(n,i)-1 and classic(n,i)
    #[arg(long, default_value_t = false)]
    yao_classic_check: bool,
    /// In Yao mode, disable optimistic pruning (for A/B runtime comparisons)
    #[arg(long, default_value_t = false)]
    yao_disable_optimistic_prune: bool,
}

fn main() {
    let args = Args::parse();

    if let Some(max_cores) = args.max_core {
        rayon::ThreadPoolBuilder::new()
            .num_threads(max_cores)
            .build_global()
            .unwrap();
    }

    // Additional meta information.
    if args.verbose != 0 {
        utils::print_git_info();
        utils::print_lscpu();
        utils::print_current_time();
    }

    if matches!(args.search_mode, SearchMode::Bidirectional) {
        assert!(
            args.max_core.is_some(),
            "You should specify the maximum number of cores via e.g. '--max-cores 4'. \
             Otherwise the backward-search will consume all resources and the \
             forward-search gets really slow. Hint: leave at least one core for \
             the forward-search"
        );
    }

    match args.search_mode {
        SearchMode::Forward | SearchMode::Bidirectional => run_forward(&args),
        SearchMode::ForwardYao => run_forward_yao(&args),
        SearchMode::Backward => run_backward(&args),
    }
}

fn run_forward(args: &Args) {
    let use_bidirectional_search = matches!(args.search_mode, SearchMode::Bidirectional);
    let start_n = args.n.unwrap_or(1);

    let mut cache = Cache::new(args.max_cache_size);
    let mut algorithm = HashMap::new();

    println!("Cache entries: {}", cache.len());
    println!("Maximum cache entries: {}", cache.max_entries());

    for n in start_n..=MAX_N as u8 {
        let start_i = if n == start_n { args.i.unwrap_or(0) } else { 0 };

        for i in start_i..(n + 1) / 2 {
            let result = Search::new(
                n,
                i,
                &mut cache,
                &mut algorithm,
                use_bidirectional_search,
                args.weight_function,
            )
            .search();

            if (n as usize) < KNOWN_VALUES.len() && (i as usize) < KNOWN_VALUES[n as usize].len() {
                assert_eq!(result, KNOWN_VALUES[n as usize][i as usize] as u8);
            }

            if args.print_algorithm {
                DirBuilder::new()
                    .recursive(true)
                    .create("algorithms")
                    .unwrap();

                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(format!("algorithms/{n}_{i}.rs"))
                    .unwrap();

                let mut writer = BufWriter::new(file);
                writeln!(writer, "#[allow(unused)]\npub const N: usize = {n};\n#[allow(unused)]\npub const I: usize = {i};\n\n#[allow(unused)]\npub fn run(input: [usize; {n}]) -> usize {{\n  select_0(input)\n}}\n\n").unwrap();

                print_algorithm(
                    PseudoCanonifiedPoset::new(n, i),
                    &mut writer,
                    &algorithm,
                    &mut HashMap::new(),
                );
            }

            if args.explore {
                let mapping = {
                    let mut mapping = [0; MAX_N];
                    mapping
                        .iter_mut()
                        .enumerate()
                        .for_each(|(i, elem)| *elem = i as u8);
                    mapping
                };

                explore(FreePoset::new(n, i), mapping, &cache);
                return;
            }

            if args.single {
                return;
            }
        }
    }
}

fn run_forward_yao(args: &Args) {
    assert!(
        !args.explore,
        "--explore is currently not supported for search_mode=forward_yao"
    );

    if args.print_algorithm {
        println!("Warning: --print-algorithm is ignored for search_mode=forward_yao");
    }

    let subset_n = args
        .subset_n
        .expect("Please specify --subset-n for search_mode=forward_yao");
    let subset_n_usize = subset_n as usize;

    assert!(
        subset_n_usize <= MAX_N,
        "subset size n exceeds MAX_N ({MAX_N})"
    );

    if let Some(i) = args.i {
        assert!(
            i < subset_n,
            "rank i must satisfy i < subset_n in Yao mode (0-based)"
        );
    }

    let start_m = args.n.unwrap_or(subset_n).max(subset_n);

    let mut cache = Cache::new(args.max_cache_size);
    let mut algorithm = HashMap::new();
    let use_prune = !args.yao_disable_optimistic_prune;

    println!("Cache entries: {}", cache.len());
    println!("Maximum cache entries: {}", cache.max_entries());
    println!("Yao prune enabled: {}", use_prune);

    for m in start_m..=MAX_N as u8 {
        let start_i = if m == start_m { args.i.unwrap_or(0) } else { 0 };

        for i in start_i..subset_n {
            let mirrored_i = i.min(subset_n - i - 1);
            let mirrored_i_usize = mirrored_i as usize;

            if !(subset_n_usize < KNOWN_VALUES.len()
                && mirrored_i_usize < KNOWN_VALUES[subset_n_usize].len())
            {
                println!(
                    "No classic KNOWN_VALUES entry for n={}, i={} (mirrored index {})",
                    subset_n, i, mirrored_i
                );

                let yao_result =
                    YaoSearch::new(m, subset_n, i, &mut cache, &mut algorithm, use_prune).search();
                println!("Yao(m={}, n={}, i={}) = {}", m, subset_n, i, yao_result);
                if args.single {
                    return;
                }
                continue;
            }

            let classic_known = KNOWN_VALUES[subset_n_usize][mirrored_i_usize] as u8;
            println!(
                "Classic known value for n={}, i={} (mirrored index {}): {}",
                subset_n, i, mirrored_i, classic_known
            );

            if args.yao_classic_check {
                let classic_minus_one = classic_known.saturating_sub(1);
                println!(
                    "Decision-only check: budgets {} and {}",
                    classic_minus_one, classic_known
                );

                let solved_minus_one =
                    YaoSearch::new(m, subset_n, i, &mut cache, &mut algorithm, use_prune)
                        .is_solvable_within(classic_minus_one);
                let solved_classic =
                    YaoSearch::new(m, subset_n, i, &mut cache, &mut algorithm, use_prune)
                        .is_solvable_within(classic_known);

                if solved_minus_one && solved_classic {
                    println!(
                        "Counterexample found: Yao(m={}, n={}, i={}) <= {} < classic {}",
                        m, subset_n, i, classic_minus_one, classic_known
                    );
                } else if !solved_minus_one && solved_classic {
                    println!(
                        "No benefit here: Yao(m={}, n={}, i={}) = classic = {}",
                        m, subset_n, i, classic_known
                    );
                } else if solved_minus_one && !solved_classic {
                    println!(
                        "Yao result is above classic here: monotonicity violation for m={}, n={}, i={}",
                        m,
                        subset_n,
                        i
                    );
                } else {
                    println!(
                        "Yao result is above classic here: unsolved within classic budget {} for m={}, n={}, i={}",
                        classic_known, m, subset_n, i
                    );
                }
            } else {
                let yao_result =
                    YaoSearch::new(m, subset_n, i, &mut cache, &mut algorithm, use_prune).search();

                if yao_result < classic_known {
                    println!(
                        "Counterexample found: Yao(m={}, n={}, i={}) = {} < classic {}",
                        m, subset_n, i, yao_result, classic_known
                    );
                } else if yao_result == classic_known {
                    println!(
                        "No benefit here: Yao(m={}, n={}, i={}) = classic = {}",
                        m, subset_n, i, yao_result
                    );
                } else {
                    println!(
                        "Yao result is above classic here: Yao(m={}, n={}, i={}) = {}, classic = {}",
                        m, subset_n, i, yao_result, classic_known
                    );
                }
            }

            if args.single {
                return;
            }
        }
    }
}

fn run_backward(args: &Args) {
    let start_n = args.n.unwrap_or(1);

    let interrupt = Arc::new(AtomicBool::new(false));

    for n in start_n..=MAX_N as u8 {
        let start_i = if n == start_n { args.i.unwrap_or(0) } else { 0 };

        for i in start_i..(n + 1) / 2 {
            let (comparisons, cache) = iterative_deepening_backward(&interrupt, n, i);

            if (n as usize) < KNOWN_VALUES.len() && (i as usize) < KNOWN_VALUES[n as usize].len() {
                assert_eq!(comparisons, KNOWN_VALUES[n as usize][i as usize] as u8);
            }

            if args.print_algorithm {
                let start = std::time::Instant::now();
                DirBuilder::new()
                    .recursive(true)
                    .create("algorithms")
                    .unwrap();

                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(format!("algorithms/{n}_{i}.rs"))
                    .unwrap();

                let mut writer = BufWriter::new(file);
                writeln!(writer, "#[allow(unused)]\npub const N: usize = {n};\n#[allow(unused)]\npub const I: usize = {i};\n\n#[allow(unused)]\npub fn run(input: [usize; {n}]) -> usize {{\n  select_0(input, false)\n}}\n\n").unwrap();

                print_algorithm_backward(
                    BackwardsPoset::new(n, i),
                    &mut writer,
                    &cache,
                    &mut HashMap::new(),
                );

                writer.flush().expect("Failed to flush BufWriter");

                println!();
                println!("generating algorithm {}", format_duration(start));
            }

            if args.single {
                return;
            }

            println!("==============================================================");
        }
    }
}

#[allow(clippy::too_many_lines)]
fn print_algorithm<W>(
    poset: PseudoCanonifiedPoset,
    writer: &mut BufWriter<W>,
    comparisons: &HashMap<PseudoCanonifiedPoset, (u8, u8)>,
    done: &mut HashMap<PseudoCanonifiedPoset, usize>,
) -> usize
where
    W: Write,
{
    const VARIABLES: [&str; MAX_N] = [
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
    ];

    if let Some(index) = done.get(&poset) {
        return *index;
    }

    let index = done.len();

    done.insert(poset, index);
    if poset.n() == 1 {
        writeln!(writer, "/// n = 1").unwrap();
        writeln!(writer, "fn select_{index}([a]: [usize; 1]) -> usize {{").unwrap();
        writeln!(writer, "    a").unwrap();
        writeln!(writer, "}}").unwrap();

        return index;
    }

    let comparison = comparisons.get(&poset);
    let (i, j) = if let Some((i, j)) = comparison {
        (*i, *j)
    } else {
        dbg!(poset);
        unreachable!()
    };

    let (less, less_mapping) = poset.to_free().with_less_mapping(i, j);
    let (greater, greater_mapping) = poset.to_free().with_less_mapping(j, i);

    let less_index = print_algorithm(less.canonified(), writer, comparisons, done);
    let greater_index = print_algorithm(greater.canonified(), writer, comparisons, done);

    let vars = (0..poset.n() as usize)
        .map(|i| VARIABLES[i].to_string())
        .reduce(|a, b| format!("{a}, {b}"))
        .unwrap();

    let less_vars = less_mapping
        .iter()
        .take(less.n() as usize)
        .map(|i| VARIABLES[*i].to_string())
        .reduce(|a, b| format!("{a}, {b}"))
        .unwrap();

    let greater_vars = greater_mapping
        .iter()
        .take(greater.n() as usize)
        .map(|i| VARIABLES[*i].to_string())
        .reduce(|a, b| format!("{a}, {b}"))
        .unwrap();

    // calculate comparisons
    let mut comparisons = vec![];
    for i in 0..poset.n() {
        'j_loop: for j in i + 1..poset.n() {
            if !poset.is_less(i, j) {
                continue;
            }

            for k in (i + 1)..=j {
                if poset.is_less(i, k) && poset.is_less(k, j) {
                    continue 'j_loop;
                }
            }

            comparisons.push((i, j));
        }
    }

    let comparisons = comparisons
        .into_iter()
        .map(|(i, j)| format!("{} < {}", VARIABLES[i as usize], VARIABLES[j as usize]))
        .reduce(|a, b| format!("{a}, {b}"))
        .map_or(String::new(), |s| ", ".to_string() + s.as_str());

    if less_index == greater_index {
        debug_assert_eq!(less.n(), greater.n());
        let mut different = [false; MAX_N];
        let n = less.n() as usize;

        for i in 0..n {
            if less_mapping[i] != greater_mapping[i] {
                different[i] = true;
            }
        }

        let less_diff = less_mapping
            .iter()
            .take(less.n() as usize)
            .map(|i| VARIABLES[*i].to_string())
            .enumerate()
            .filter(|(i, _)| different[*i])
            .map(|(_, v)| v)
            .reduce(|a, b| format!("{a}, {b}"))
            .unwrap();

        let greater_diff = greater_mapping
            .iter()
            .take(greater.n() as usize)
            .map(|i| VARIABLES[*i].to_string())
            .enumerate()
            .filter(|(i, _)| different[*i])
            .map(|(_, v)| v)
            .reduce(|a, b| format!("{a}, {b}"))
            .unwrap();

        writeln!(
            writer,
            "/// n = {}, i = {}{comparisons}",
            poset.n(),
            poset.i()
        )
        .unwrap();
        writeln!(
            writer,
            "fn select_{index}([{vars}]: [usize; {}]) -> usize {{",
            poset.n()
        )
        .unwrap();
        if different.iter().filter(|a| **a).count() == 1 {
            writeln!(
                writer,
                "    let {less_diff} = if {} < {} {{",
                VARIABLES[i as usize], VARIABLES[j as usize]
            )
            .unwrap();
            writeln!(writer, "        {less_diff}").unwrap();
            writeln!(writer, "    }} else {{").unwrap();
            writeln!(writer, "        {greater_diff}").unwrap();
            writeln!(writer, "    }};").unwrap();
        } else {
            writeln!(
                writer,
                "    let ({less_diff}) = if {} < {} {{",
                VARIABLES[i as usize], VARIABLES[j as usize]
            )
            .unwrap();
            writeln!(writer, "        ({less_diff})").unwrap();
            writeln!(writer, "    }} else {{").unwrap();
            writeln!(writer, "        ({greater_diff})").unwrap();
            writeln!(writer, "    }};").unwrap();
        }
        writeln!(writer, "    select_{less_index}([{less_vars}])").unwrap();
        writeln!(writer, "}}").unwrap();
    } else {
        writeln!(
            writer,
            "/// n = {}, i = {}{comparisons}",
            poset.n(),
            poset.i()
        )
        .unwrap();
        writeln!(
            writer,
            "fn select_{index}([{vars}]: [usize; {}]) -> usize {{",
            poset.n()
        )
        .unwrap();
        writeln!(
            writer,
            "    if {} < {} {{",
            VARIABLES[i as usize], VARIABLES[j as usize]
        )
        .unwrap();
        writeln!(writer, "        select_{less_index}([{less_vars}])").unwrap();
        writeln!(writer, "    }} else {{").unwrap();
        writeln!(writer, "        select_{greater_index}([{greater_vars}])").unwrap();
        writeln!(writer, "    }}").unwrap();
        writeln!(writer, "}}").unwrap();
    }

    index
}

#[allow(clippy::too_many_lines)]
fn print_algorithm_backward<W>(
    poset: BackwardsPoset,
    writer: &mut BufWriter<W>,
    comparisons: &BackwardCache,
    done: &mut HashMap<BackwardsPoset, usize>,
) -> usize
where
    W: Write,
{
    const VARIABLES: [&str; MAX_N] = [
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
    ];

    if let Some(index) = done.get(&poset) {
        return *index;
    }

    let index = done.len();

    done.insert(poset, index);
    if poset.n() == 1 {
        writeln!(writer, "/// n = 1, i = 0").unwrap();
        writeln!(
            writer,
            "fn select_{index}([a]: [usize; 1], _: bool) -> usize {{"
        )
        .unwrap();
        writeln!(writer, "    a").unwrap();
        writeln!(writer, "}}").unwrap();

        return index;
    }

    let (i, j) = comparisons.get(&poset);

    let (less, (less_mapping, less_is_dual)) = poset.with_less_mapping(i, j);
    let (greater, (greater_mapping, greater_is_dual)) = poset.with_less_mapping(j, i);

    let less_index = print_algorithm_backward(less, writer, comparisons, done);
    let greater_index = print_algorithm_backward(greater, writer, comparisons, done);

    let vars = (0..poset.n() as usize)
        .map(|i| VARIABLES[i].to_string())
        .reduce(|a, b| format!("{a}, {b}"))
        .unwrap();

    let less_vars = less_mapping
        .iter()
        .take(less.n() as usize)
        .map(|i| VARIABLES[*i as usize].to_string())
        .reduce(|a, b| format!("{a}, {b}"))
        .unwrap();

    let greater_vars = greater_mapping
        .iter()
        .take(greater.n() as usize)
        .map(|i| VARIABLES[*i as usize].to_string())
        .reduce(|a, b| format!("{a}, {b}"))
        .unwrap();

    // =====================

    // calculate comparisons
    let mut comparisons = vec![];
    for i in 0..poset.n() {
        'j_loop: for j in i + 1..poset.n() {
            if !poset.is_less(i, j) {
                continue;
            }

            for k in (i + 1)..=j {
                if poset.is_less(i, k) && poset.is_less(k, j) {
                    continue 'j_loop;
                }
            }

            comparisons.push((i, j));
        }
    }

    let comparisons = comparisons
        .into_iter()
        .map(|(i, j)| format!("{} < {}", VARIABLES[i as usize], VARIABLES[j as usize]))
        .reduce(|a, b| format!("{a}, {b}"))
        .map_or(String::new(), |s| ", ".to_string() + s.as_str());

    writeln!(
        writer,
        "/// n = {}, i = {}{comparisons}",
        poset.n(),
        poset.i()
    )
    .unwrap();
    writeln!(
        writer,
        "fn select_{index}([{vars}]: [usize; {}], is_dual: bool) -> usize {{",
        poset.n()
    )
    .unwrap();
    writeln!(
        writer,
        "    if (!is_dual && {} < {}) || (is_dual && {} > {}) {{",
        VARIABLES[i as usize], VARIABLES[j as usize], VARIABLES[i as usize], VARIABLES[j as usize]
    )
    .unwrap();
    writeln!(
        writer,
        "        select_{less_index}([{less_vars}], {}is_dual)",
        if less_is_dual { "!" } else { "" }
    )
    .unwrap();
    writeln!(writer, "    }} else {{").unwrap();
    writeln!(
        writer,
        "        select_{greater_index}([{greater_vars}], {}is_dual)",
        if greater_is_dual { "!" } else { "" }
    )
    .unwrap();
    writeln!(writer, "    }}").unwrap();
    writeln!(writer, "}}").unwrap();

    index
}

#[allow(clippy::too_many_lines)]
fn explore(poset: FreePoset, mapping: [u8; MAX_N], cache: &Cache) {
    loop {
        let old_mapping = {
            let mut old = [0; MAX_N];
            for i in 0..poset.n() {
                old[mapping[i as usize] as usize] = i;
            }
            old
        };

        let mut best_comp = (0, 0);
        let mut best_cost = u8::MAX;

        // print comparisons
        let mut first = true;
        for i in 0..poset.n() {
            let normal_i = i;
            let i = old_mapping[i as usize];

            'j_loop: for j in normal_i + 1..poset.n() {
                let normal_j = j;
                let j = old_mapping[j as usize];

                if !poset.has_order(i, j) {
                    continue;
                }

                let is_less = poset.is_less(i, j);

                for k in 0..poset.n() {
                    if k != i
                        && k != j
                        && poset.is_less(i, k) == is_less
                        && poset.is_less(k, j) == is_less
                    {
                        continue 'j_loop;
                    }
                }

                if first {
                    first = false;
                } else {
                    print!(", ");
                }
                if is_less {
                    print!("{normal_i} < {normal_j}");
                } else {
                    print!("{normal_j} < {normal_i}");
                }
            }
        }
        println!();

        // print matrix
        print!("     |");
        for i in 0..poset.n() {
            print!(" {i:2}|");
        }
        println!();

        println!("-----+{}", "---+".repeat(poset.n().into()));
        for i in 0..poset.n() {
            let mapped_i = old_mapping[i as usize];

            print!("{i:2} < |");

            for j in 0..poset.n() {
                let mapped_j = old_mapping[j as usize];

                if mapped_i == mapped_j || poset.has_order(mapped_i, mapped_j) {
                    print!("   |");
                    continue;
                }

                let less = poset.with_less(mapped_i, mapped_j);
                let greater = poset.with_less(mapped_j, mapped_i);
                match cache.get(&less) {
                    Some(Cost::Solved(cost)) => {
                        print!(" {cost:<2}|");

                        if let Some(Cost::Solved(other_cost)) = cache.get(&greater) {
                            let max_cost = cost.max(other_cost);
                            if max_cost < best_cost {
                                best_cost = max_cost;
                                best_comp = (i, j);
                            }
                        }
                    }
                    Some(Cost::Minimum(cost)) => print!(">{:2}|", cost - 1),
                    None => print!(" ? |"),
                }
            }

            println!();
            println!("-----+{}", "---+".repeat(poset.n().into()));
        }

        let mut input = String::new();

        println!(
            "best: {}, {}, cost: {}",
            best_comp.0, best_comp.1, best_cost
        );

        print!("> ");
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_line(&mut input).unwrap();

        match input.trim() {
            "" | "q" => break,
            _ => {
                if let Some((left, right)) = input.trim().split_once('<') {
                    if let (Ok(i), Ok(j)) = (left.trim().parse::<u8>(), right.trim().parse::<u8>())
                    {
                        let mapped_i = old_mapping[i as usize];
                        let mapped_j = old_mapping[j as usize];

                        let (next, new_mapping) = poset.with_less_mapping(mapped_i, mapped_j);

                        let next_mapping = {
                            let mut new = [0; MAX_N];
                            for i in 0..poset.n() {
                                new[i as usize] = mapping[new_mapping[i as usize]];
                            }
                            new
                        };

                        explore(next, next_mapping, cache);
                    }
                }
            }
        }
    }
}
