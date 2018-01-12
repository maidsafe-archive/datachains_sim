extern crate clap;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tiny_keccak;

mod network;
mod random;
mod params;

use random::random_range;
use network::Network;
use params::Params;
use std::collections::BTreeMap;
use clap::{App, Arg};

/// The probabilities for nodes joining and leaving the network, as percentages.
/// If they don't add up to 100, the remainder is the probability of rejoining
/// by a node that was a part of the network, but left.
const P_ADD: u8 = 90;
const P_DROP: u8 = 7;

/// Generates a random churn event in the network. There are three possible kinds:
/// node joining, node leaving and node rejoining.
fn random_event(network: &mut Network) {
    let x = random_range(0, 100);
    if x < P_ADD {
        network.add_random_node();
    } else if x >= P_ADD && x < P_ADD + P_DROP {
        network.drop_random_node();
    } else {
        network.rejoin_random_node();
    }
}

fn print_dist(mut dist: BTreeMap<u8, usize>) {
    let mut age = 1;
    while !dist.is_empty() {
        let num = dist.remove(&age).unwrap_or(0);
        println!("{}\t{}", age, num);
        age += 1;
    }
}

fn get_params() -> Params {
    let matches = App::new("Ageing Simulation")
        .about("Simulates ageing in SAFE network")
        .arg(
            Arg::with_name("initage")
                .short("i")
                .long("initage")
                .value_name("AGE")
                .help("Sets the initial age of newly joining peers")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("split")
                .short("s")
                .long("split")
                .value_name("STRATEGY")
                .help("Selects the strategy for splitting (always/complete)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("norejectyoung")
                .short("y")
                .long("norejectyoung")
                .help("Don't reject young peers when one already present in the section"),
        )
        .get_matches();
    let init_age = matches
        .value_of("initage")
        .unwrap_or("1")
        .parse()
        .expect("Initial age must be a number!");
    let split = matches
        .value_of("split")
        .unwrap_or("complete")
        .parse()
        .ok()
        .expect("Split strategy must be \"always\" or \"complete\".");
    let norejectyoung = matches.is_present("norejectyoung");
    Params {
        init_age,
        split_strategy: split,
        norejectyoung,
    }
}

fn main() {
    let params = get_params();
    let mut network = Network::new(params);

    for i in 0..100000 {
        println!("Iteration {}...", i);
        // Generate a random event...
        random_event(&mut network);
        // ... and process the churn cascade that may happen
        // (every churn event may trigger other churn events, that
        // may trigger others etc.)
        network.process_events();
    }
    println!("Network state:\n{:?}", network);
    println!("");

    println!(
        "Number of sections: {} (complete: {})",
        network.num_sections(),
        network.complete_sections()
    );

    let age_dist = network.age_distribution();
    println!("Age distribution:");
    print_dist(age_dist);

    let drop_dist = network.drops_distribution();
    println!("\nDrops distribution by age:");
    print_dist(drop_dist);
}
