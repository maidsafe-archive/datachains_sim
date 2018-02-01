use Age;
use byteorder::{ByteOrder, LittleEndian};
use prefix::Name;
use rand::{Rand, Rng};
use std::fmt;
use std::ops::Deref;
use tiny_keccak::sha3_256;

#[derive(Clone)]
pub struct Chain {
    last_live: Option<Block>,
}

impl Chain {
    pub fn new() -> Self {
        Chain { last_live: None }
    }

    pub fn insert(&mut self, block: Block) {
        if let Event::Live = block.event {
            self.last_live = Some(block)
        }
    }

    pub fn extend(&mut self, other: Chain) {
        if let Some(block) = other.last_live {
            self.last_live = Some(block)
        }
    }

    pub fn last_live(&self) -> Option<Block> {
        self.last_live.clone()
    }
}

impl fmt::Debug for Chain {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Chain")
    }
}

#[derive(Clone)]
pub struct Block {
    event: Event,
    name: Name,
    age: Age,
}

impl Block {
    pub fn new(event: Event, name: Name, age: Age) -> Self {
        Block { event, name, age }
    }

    pub fn hash(&self) -> Hash {
        let mut bytes = [0; 17];
        bytes[0] = match self.event {
            Event::Live => 0,
            Event::Dead => 1,
            Event::Gone => 2,
        };

        LittleEndian::write_u64(&mut bytes[1..], self.name.0);
        LittleEndian::write_u64(&mut bytes[9..], self.age);

        Hash(sha3_256(&bytes))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Event {
    Live,
    Dead,
    Gone,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Hash([u8; 32]);

impl Hash {
    pub fn rehash(&self) -> Self {
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

impl From<Name> for Hash {
    fn from(src: Name) -> Self {
        let mut value_in = src.0;
        let mut result: [u8; 32] = [0; 32];

        for i in 0..8 {
            result[7 - i] = (value_in % 256) as u8;
            value_in = value_in / 256;
            if value_in == 0 {
                break;
            }
        }

        Hash(result)
    }
}

impl Into<Name> for Hash {
    fn into(self) -> Name {
        let mut result: u64 = 0;

        for i in 0..8 {
            let value = self.0[i];
            result = result * 256 + value as u64;
        }

        Name(result)
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
