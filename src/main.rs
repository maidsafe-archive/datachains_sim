extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tiny_keccak;

mod prefix;
mod network;

use network::Network;

fn main() {
    let mut network = Network::new();
}
