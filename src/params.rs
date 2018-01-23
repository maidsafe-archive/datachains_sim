//! Simulation parameters.

use parse::ParseError;
use random::Seed;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct Params {
    /// Seed for the random number generator.
    pub seed: Seed,
    /// Number of simulation iterations.
    pub num_iterations: u64,
    /// Number of nodes to form a complete group.
    pub group_size: usize,
    /// Age of newly joined node.
    pub init_age: u64,
    /// Age at which a node becomes adult.
    pub adult_age: u64,
    /// Maximum number of nodes a section can have before the simulation fails.
    pub max_section_size: usize,
    /// Maximum number of reocation attempts after a `Live` event.
    pub max_relocation_attempts: usize,
    /// Maximum number of infants allowed in one section.
    pub max_infants_per_section: usize,
    /// Relocation strategy
    pub relocation_strategy: RelocationStrategy,
    /// Print statistics every Nth iteration (supress if 0)
    pub stats_frequency: u64,
    /// File to store  network structure data.
    pub file: Option<String>,
    /// Log veribosity
    pub verbosity: usize,
}

impl Params {
    /// Quorum size - a simple majority of the group.
    pub fn quorum(&self) -> usize {
        self.group_size / 2 + 1
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelocationStrategy {
    YoungestFirst,
    OldestFirst,
}

impl FromStr for RelocationStrategy {
    type Err = ParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.chars().next() {
            Some('y') | Some('Y') => Ok(RelocationStrategy::YoungestFirst),
            Some('o') | Some('O') => Ok(RelocationStrategy::OldestFirst),
            _ => Err(ParseError),
        }
    }
}
