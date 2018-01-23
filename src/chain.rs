use Age;
use prefix::Name;
use rand::{Rand, Rng};
use random;
use std::fmt;
use std::ops::Deref;
use tiny_keccak::sha3_256;

#[derive(Clone)]
pub struct Chain {
    // blocks: Vec<Block>,
}

impl Chain {
    pub fn new() -> Self {
        // Chain { blocks: Vec::new() }
        Chain {}
    }

    pub fn insert(&mut self, _event: Event, _name: Name, _age: Age) {
        // self.blocks.push(Block { event, name, age });
        // self.verify()
    }

    pub fn extend(&mut self, _other: Chain) {
        // self.blocks.extend(other.blocks);
        // self.verify()
    }

    pub fn relocation_hash(&self, _name: Option<Name>) -> Option<Hash> {
        Some(random::gen())
        // name.and_then(|name| self.last_live_of(name))
        //     .or_else(|| self.last_live())
        //     .map(|block| block.hash())
    }

    // fn last_live_of(&self, name: Name) -> Option<&Block> {
    //     self.blocks.iter().rev().find(|block| {
    //         block.event == Event::Live && block.name == name
    //     })
    // }

    // fn last_live(&self) -> Option<&Block> {
    //     self.blocks.iter().rev().find(
    //         |block| block.event == Event::Live,
    //     )
    // }
}

impl fmt::Debug for Chain {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Chain")
    }
}

/*
#[derive(Clone)]
pub struct Block {
    event: Event,
    name: Name,
    age: Age,
}

impl Block {
    pub fn hash(&self) -> Hash {
        let slice = unsafe {
            let ptr = self as *const _ as *const u8;
            slice::from_raw_parts(ptr, mem::size_of::<Self>())
        };

        Hash(sha3_256(slice))
    }
}
*/

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Event {
    Live,
    Dead,
    Gone,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Hash([u8; 32]);

impl Hash {
    pub fn hash(&self) -> Self {
        Hash(sha3_256(&self.0))
    }

    #[allow(unused)]
    pub fn trailing_zeros(&self) -> u64 {
        let mut result = 0;
        for digit in self.0.iter().rev() {
            let zeros = digit.trailing_zeros();
            result += zeros;

            if zeros < 8 {
                break;
            }
        }

        result as u64
    }
}

impl Deref for Hash {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Rand for Hash {
    fn rand<R: Rng>(rng: &mut R) -> Self {
        Hash(rng.gen())
    }
}
