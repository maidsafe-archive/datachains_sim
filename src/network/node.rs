use std::fmt;
use serde_json;
use random::random;
use tiny_keccak::sha3_256;
use network::prefix::{Name, Prefix};

pub type Digest = [u8; 32];

/// A node has a name and an age
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    name: Name,
    age: u8,
}

impl fmt::Debug for Node {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Node({:?}; age={})", self.name, self.age)
    }
}

impl Node {
    /// Creates a new node
    pub fn new(name: u64) -> Node {
        Node {
            name: Name(name),
            age: 1,
        }
    }

    /// Generates a relocated name and increases the age by 1
    pub fn relocate(&mut self, prefix: &Prefix) {
        self.name = prefix.substituted_in(Name(random()));
        self.age += 1;
    }

    /// Halves the age, because the node is rejoining
    pub fn rejoined(&mut self) {
        if self.age > 1 {
            self.age /= 2;
        }
    }

    /// Returns the name
    pub fn name(&self) -> Name {
        self.name
    }

    /// Returns the age
    pub fn age(&self) -> u8 {
        self.age
    }

    /// Returns whether the node is an Adult
    pub fn is_adult(&self) -> bool {
        self.age > 4
    }

    /// Returns the weight used in randomly choosing a node to be dropped
    pub fn drop_probability(&self) -> f64 {
        10.0 / self.age as f64
    }

    /// Returns the hash of the node struct
    #[allow(unused)]
    pub fn hash(&self) -> Digest {
        sha3_256(serde_json::to_string(self).unwrap().as_bytes())
    }
}
