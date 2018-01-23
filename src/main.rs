extern crate colored;
extern crate clap;
extern crate rand;
extern crate tiny_keccak;

#[macro_use]
mod log;

mod chain;
mod message;
mod network;
mod node;
mod params;
mod parse;
mod prefix;
mod random;
mod section;
mod stats;

use clap::{App, Arg, ArgMatches};
use colored::Colorize;
use network::Network;
use params::{Params, RelocationStrategy};
use random::Seed;
use std::collections;
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;
use std::panic;
use std::str::FromStr;

type Age = u64;

fn main() {
    let params = get_params();

    let seed = params.seed;
    random::reseed(seed);

    // Print seed on panic.
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        println!("{:?}", seed);
    }));

    log::set_verbosity(params.verbosity);

    let mut network = Network::new(params.clone());

    for i in 0..params.num_iterations {
        info!(
            "{}",
            format!("Iteration: {}", format!("{}", i).bold()).green()
        );

        let run = network.tick(i);

        if params.stats_frequency > 0 && i % params.stats_frequency == 0 {
            print_tick_stats(&network);
        }

        if !run {
            break;
        }
    }

    println!("\n===== Summary =====");
    println!("\n{:?}\n", params);
    println!("{}", network.stats().summary());
    println!("Age distribution:");
    println!("{}", network.age_dist());
    println!("Section size distribution:");
    println!("{}", network.section_size_dist());
    println!("Prefix length distribution:");
    println!("{}", network.prefix_len_dist());

    if let Some(path) = params.file {
        network.stats().write_to_file(path);
    }
}

fn get_params() -> Params {
    let matches = App::new("SAFE network simulation")
        .about("Simulates evolution of SAFE network")
        .arg(
            Arg::with_name("SEED")
                .short("S")
                .long("seed")
                .help("Random seed")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ITERATIONS")
                .short("n")
                .long("iterations")
                .help("Number of simulation iterations")
                .takes_value(true)
                .default_value("100000"),
        )
        .arg(
            Arg::with_name("GROUP_SIZE")
                .short("g")
                .long("group-size")
                .help("Group size")
                .takes_value(true)
                .default_value("8"),
        )
        .arg(
            Arg::with_name("INIT_AGE")
                .short("i")
                .long("init-age")
                .help("Initial age of newly joining nodes")
                .takes_value(true)
                .default_value("4"),
        )
        .arg(
            Arg::with_name("ADULT_AGE")
                .short("a")
                .long("adult-age")
                .help("Age at which a node becomes adult")
                .takes_value(true)
                .default_value("5"),
        )
        .arg(
            Arg::with_name("MAX_SECTION_SIZE")
                .short("s")
                .long("max-section-size")
                .help(
                    "Maximum section size (number of nodes) before the simulation fails",
                )
                .takes_value(true)
                .default_value("60"),
        )
        .arg(
            Arg::with_name("MAX_RELOCATION_ATTEMPTS")
                .short("r")
                .long("max-relocation-attempts")
                .help("Maximum number of relocation attempts after a Live event")
                .takes_value(true)
                .default_value("5"),
        )
        .arg(
            Arg::with_name("MAX_INFANTS_PER_SECTION")
                .short("I")
                .long("max-infants-per-section")
                .help("Maximum number of infants per section")
                .takes_value(true)
                .default_value("1"),
        )
        .arg(
            Arg::with_name("RELOCATION_STRATEGY")
                .short("R")
                .long("relocation-strategy")
                .help("Relocation strategy (o = oldest first, y = youngest first)")
                .takes_value(true)
                .possible_values(&["o", "y"])
                .hide_possible_values(true)
                .default_value("o"),
        )
        .arg(
            Arg::with_name("STATS_FREQUENCY")
                .short("F")
                .long("stats-frequency")
                .help(
                    "how often (every which iteration) to output network statistics",
                )
                .takes_value(true)
                .default_value("10"),
        )
        .arg(
            Arg::with_name("FILE")
                .long("file")
                .short("f")
                .help("Output file for network structure data")
                .takes_value(true),
        )
        .arg(Arg::with_name("VERBOSITY").short("v").multiple(true).help(
            "Log verbosity",
        ))
        .get_matches();

    let seed = match matches.value_of("SEED") {
        Some(seed) => seed.parse().expect("SEED must be in form `[1, 2, 3, 4]`"),
        None => Seed::random(),
    };

    let relocation_strategy =
        RelocationStrategy::from_str(matches.value_of("RELOCATION_STRATEGY").unwrap()).unwrap();

    Params {
        seed,
        num_iterations: get_number(&matches, "ITERATIONS"),
        group_size: get_number(&matches, "GROUP_SIZE"),
        init_age: get_number(&matches, "INIT_AGE"),
        adult_age: get_number(&matches, "ADULT_AGE"),
        max_section_size: get_number(&matches, "MAX_SECTION_SIZE"),
        max_relocation_attempts: get_number(&matches, "MAX_RELOCATION_ATTEMPTS"),
        max_infants_per_section: get_number(&matches, "MAX_INFANTS_PER_SECTION"),
        relocation_strategy,
        stats_frequency: get_number(&matches, "STATS_FREQUENCY"),
        file: matches.value_of("FILE").map(String::from),
        verbosity: matches.occurrences_of("VERBOSITY") as usize + 1,
    }
}

fn print_tick_stats(network: &Network) {
    println!(
        "Header {:?}, AgeDist {:?}, SectionSizeDist {:?}, PrefixLenDist {:?}",
        network.stats().summary(),
        network.age_dist(),
        network.section_size_dist(),
        network.prefix_len_dist(),
    )
}

fn get_number<T: Number>(matches: &ArgMatches, name: &str) -> T {
    match matches.value_of(name).unwrap().parse() {
        Ok(value) => value,
        Err(_) => panic!("{} must be a number.", name),
    }
}

trait Number: FromStr {}
impl Number for usize {}
impl Number for u64 {}

// Use these type aliases instead of the default collections to make sure
// we use consistent hashing across runs, to enable deterministic results.
type HashMap<K, V> = collections::HashMap<K, V, BuildHasherDefault<DefaultHasher>>;
type HashSet<T> = collections::HashSet<T, BuildHasherDefault<DefaultHasher>>;
