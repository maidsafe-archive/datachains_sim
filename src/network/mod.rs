pub mod churn;
pub mod prefix;
pub mod node;
pub mod network;
pub mod section;

pub const GROUP_SIZE: usize = 8;
pub const BUFFER: usize = 3;

pub use self::network::Network;
