extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tiny_keccak;

mod prefix;
mod network;

use rand::{thread_rng, Rng};
use network::Network;

fn random_event<R: Rng>(network: &mut Network, rng: &mut R) {
    let x = rng.gen_range(0, 100);
    if x >= 0 && x < 75 {
        network.add_random_node(rng);
    } else if x >= 75 && x < 95 {
        network.drop_random_node(rng);
    } else {
        network.rejoin_random_node(rng);
    }
}

fn main() {
    let mut rng = thread_rng();
    let mut network = Network::new();
    for i in 0..1000 {
        println!("Iteration {}...", i);
        random_event(&mut network, &mut rng);
    }
    println!("Network state:\n{:?}", network);
}
