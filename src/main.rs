extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tiny_keccak;

mod network;
mod random;

use random::random_range;
use network::Network;
use std::collections::BTreeMap;

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

fn main() {
    let mut network = Network::new();
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

    println!("Number of sections: {}", network.num_sections());

    let age_dist = network.age_distribution();
    println!("Age distribution:");
    print_dist(age_dist);

    let drop_dist = network.drops_distribution();
    println!("\nDrops distribution by age:");
    print_dist(drop_dist);
}
