extern crate colored;
extern crate clap;
extern crate rand;
extern crate tiny_keccak;

mod chain;
mod log;
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
use params::Params;
use random::Seed;
use std::collections;
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;
use std::str::FromStr;

type Age = u64;

fn main() {
    let params = get_params();
    let mut ticks = 0;

    random::reseed(params.seed);
    let mut network = Network::new(params.clone());

    for i in 0..params.num_iterations {
        ticks = i + 1;

        println!(
            "{}",
            format!("Iteration: {}", format!("{}", i).bold()).green()
        );

        if !network.tick() {
            break;
        }
    }

    println!("");
    println!("{:?}", params);

    println!("");
    println!("Total iterations: {}", ticks);
    println!("Age distribution:");
    for (age, count) in network.age_dist() {
        println!("{:4}: {}", age, count);
    }

    if let Some(path) = params.file {
        network.stats().write_samples_to_file(path);
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
                .default_value("1"),
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
            Arg::with_name("FILE")
                .long("file")
                .short("f")
                .help("Output file for network structure data")
                .takes_value(true),
        )
        .get_matches();

    let seed = match matches.value_of("SEED") {
        Some(seed) => seed.parse().expect("SEED must be in form `[1, 2, 3, 4]`"),
        None => Seed::random(),
    };

    Params {
        seed,
        num_iterations: get_number(&matches, "ITERATIONS"),
        group_size: get_number(&matches, "GROUP_SIZE"),
        init_age: get_number(&matches, "INIT_AGE"),
        adult_age: get_number(&matches, "ADULT_AGE"),
        max_section_size: get_number(&matches, "MAX_SECTION_SIZE"),
        max_relocation_attempts: get_number(&matches, "MAX_RELOCATION_ATTEMPTS"),
        file: matches.value_of("FILE").map(String::from),
    }
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
