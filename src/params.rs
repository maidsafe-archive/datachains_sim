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
    pub init_age: u8,
    /// Age at which a node becomes adult.
    pub adult_age: u8,
    /// Maximum number of nodes a section can have before the simulation fails.
    pub max_section_size: usize,
    /// Maximum number of reocation attempts after a `Live` event.
    pub max_relocation_attempts: usize,
    /// Maximum number of infants allowed in one section.
    pub max_infants_per_section: usize,
    /// Print statistics every Nth iteration (supress if 0)
    pub stats_frequency: u64,
    /// File to store  network structure data.
    pub file: Option<String>,
    /// Log veribosity
    pub verbosity: usize,
    /// Disable colored output
    pub disable_colors: bool,
}

impl Params {
    /// Quorum size - a simple majority of the group.
    pub fn quorum(&self) -> usize {
        self.group_size / 2 + 1
    }
}
