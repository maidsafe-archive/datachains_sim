use std::fmt;
use serde_json;
use rand::Rng;
use tiny_keccak::sha3_256;
use network::prefix::{Name, Prefix};

pub type Digest = [u8; 32];

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
    pub fn new(name: u64) -> Node {
        Node {
            name: Name(name),
            age: 1,
        }
    }

    pub fn relocate<R: Rng>(&mut self, rng: &mut R, prefix: &Prefix) {
        self.name = prefix.substituted_in(Name(rng.gen()));
        self.age += 1;
    }

    pub fn rejoined(&mut self) {
        if self.age > 1 {
            self.age /= 2;
        }
    }

    pub fn name(&self) -> Name {
        self.name
    }

    pub fn age(&self) -> u8 {
        self.age
    }

    pub fn is_adult(&self) -> bool {
        self.age > 4
    }

    pub fn drop_probability(&self) -> f64 {
        10.0 / self.age as f64
    }

    pub fn hash(&self) -> Digest {
        sha3_256(serde_json::to_string(self).unwrap().as_bytes())
    }
}
