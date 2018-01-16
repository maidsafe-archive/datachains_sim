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
use network::{Network, NetworkStructure};
use params::Params;
use std::collections::BTreeMap;
use clap::{App, Arg};

/// Generates a random churn event in the network. There are three possible kinds:
/// node joining, node leaving and node rejoining.
fn random_event(network: &mut Network, probs: (u8, u8)) {
    let x = random_range(0, 100);
    if x < probs.0 {
        network.add_random_node();
    } else if x >= probs.0 && x < probs.0 + probs.1 {
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
        .arg(
            Arg::with_name("p_add1")
                .long("padd1")
                .value_name("P")
                .help("Probability that a peer will join during a step (0-100)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("p_drop1")
                .long("pdrop1")
                .value_name("P")
                .help("Probability that a peer will be dropped during a step (0-100)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("struct_file")
                .long("network-struct-out")
                .value_name("FILE")
                .help("Output file for network structure data")
                .takes_value(true),
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
    let p_add1 = matches
        .value_of("p_add1")
        .unwrap_or("90")
        .parse()
        .expect("Add probability must be a number!");
    assert!(p_add1 < 100, "Probability must be between 0 and 100!");
    let p_drop1 = matches
        .value_of("p_drop1")
        .unwrap_or("7")
        .parse()
        .expect("Drop probability must be a number!");
    assert!(p_drop1 < 100, "Probability must be between 0 and 100!");
    assert!(
        p_add1 + p_drop1 <= 100,
        "Add and drop probabilites must add up to at most 100!"
    );
    let structure_output_file = matches.value_of("struct_file").map(|s| s.to_owned());
    Params {
        init_age,
        split_strategy: split,
        norejectyoung,
        growth: (p_add1, p_drop1),
        structure_output_file,
    }
}

fn output_structure_file(file: &str, data: &[NetworkStructure]) {
    use std::fs::File;
    use std::io::Write;
    let mut file = File::create(file)
        .ok()
        .expect(&format!("Couldn't create file {}!", file));
    for (i, data) in data.into_iter().enumerate() {
        let _ = write!(
            file,
            "{} {} {} {}\n",
            i, data.size, data.sections, data.complete
        );
    }
}

fn main() {
    let params = get_params();
    let mut network = Network::new(params.clone());

    for i in 0..100000 {
        println!("Iteration {}...", i);
        // Generate a random event...
        random_event(&mut network, params.growth);
        // ... and process the churn cascade that may happen
        // (every churn event may trigger other churn events, that
        // may trigger others etc.)
        network.process_events();
    }

    println!("Network state:\n{:?}", network);
    println!("");

    println!("{:?}\n", params.clone());
    println!(
        "Number of sections: {} (complete: {})",
        network.num_sections(),
        network.complete_sections()
    );

    let age_dist = network.age_distribution();
    println!("\nAge distribution:");
    print_dist(age_dist);

    let drop_dist = &network.output().drops_dist;
    println!("\nDrops distribution by age:");
    print_dist(drop_dist.clone());

    if let Some(ref file) = params.structure_output_file {
        output_structure_file(file, &network.output().network_structure);
    }
}
