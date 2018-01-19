//! Simulation parameters.

use random::Seed;

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
    /// File to store  network structure data.
    pub file: Option<String>,
}

impl Params {
    /// Quorum size - a simple majority of the group.
    pub fn quorum(&self) -> usize {
        self.group_size / 2 + 1
    }
}
