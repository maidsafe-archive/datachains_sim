pub mod churn;
pub mod prefix;
pub mod node;
pub mod network;
pub mod section;

/// Determines the numbers of the elders in every section
pub const GROUP_SIZE: usize = 8;
/// A number of spare nodes when splitting - we don't want to
/// merge again right after we split if a node leaves, so we
/// only split if the child sections will have at least
/// GROUP_SIZE + BUFFER nodes
pub const BUFFER: usize = 3;

pub use self::network::{Network, NetworkStructure};
