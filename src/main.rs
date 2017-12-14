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

    let mut age_dist = network.age_distribution();
    let mut age = 1;
    println!("Age distribution:");
    while !age_dist.is_empty() {
        let num = age_dist.remove(&age).unwrap_or(0);
        println!("{}\t{}", age, num);
        age += 1;
    }
}
