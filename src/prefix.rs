use parse::ParseError;
use rand::{Rand, Rng};
use std::fmt;
use std::str::FromStr;

/// A network name to identify nodes.
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Name(pub u64);

impl Rand for Name {
    fn rand<R: Rng>(rng: &mut R) -> Self {
        Name(rng.gen())
    }
}

impl fmt::Debug for Name {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let (b0, b1, b2) = (
            (self.0 >> 56) as u8,
            (self.0 >> 48) as u8,
            (self.0 >> 40) as u8,
        );
        write!(fmt, "{:02x}{:02x}{:02x}...", b0, b1, b2)
    }
}

/// A structure representing a network prefix - a simplified version of the Prefix struct from
/// `routing`
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Prefix {
    len: u8,
    bits: u64,
}

impl Prefix {
    pub const EMPTY: Self = Prefix { bits: 0, len: 0 };

    pub fn len(&self) -> u8 {
        self.len
    }

    pub fn extend(self, bit: u8) -> Prefix {
        if self.len > 63 {
            return self;
        }
        let bit = (bit as u64 & 1) << (63 - self.len);
        Prefix {
            bits: self.bits | bit,
            len: self.len + 1,
        }
    }

    pub fn shorten(self) -> Self {
        if self.len < 1 {
            return self;
        }
        let mask = self.len_mask() << 1;
        Prefix {
            bits: self.bits & mask,
            len: self.len - 1,
        }
    }

    pub fn split(self) -> [Prefix; 2] {
        [self.extend(0), self.extend(1)]
    }

    pub fn sibling(self) -> Self {
        if self.len > 0 {
            self.with_flipped_bit(self.len - 1)
        } else {
            self
        }
    }

    pub fn with_flipped_bit(self, bit: u8) -> Prefix {
        let mask = 1 << (63 - bit);
        Prefix {
            bits: self.bits ^ mask,
            len: self.len,
        }
    }

    pub fn matches(&self, name: Name) -> bool {
        (name.0 & self.len_mask()) ^ self.bits == 0
    }

    pub fn is_ancestor(&self, other: &Prefix) -> bool {
        self.len <= other.len && self.matches(Name(other.bits))
    }

    #[allow(unused)]
    pub fn is_descendant(&self, other: &Prefix) -> bool {
        other.is_ancestor(self)
    }

    #[allow(unused)]
    pub fn is_compatible_with(&self, other: &Prefix) -> bool {
        self.is_ancestor(other) || self.is_descendant(other)
    }

    #[allow(unused)]
    pub fn is_sibling(&self, other: &Prefix) -> bool {
        if self.len > 0 {
            (*self).with_flipped_bit(self.len - 1) == *other
        } else {
            false
        }
    }

    #[allow(unused)]
    pub fn is_neighbour(&self, other: &Prefix) -> bool {
        let diff = self.bits ^ other.bits;
        let bit = diff.leading_zeros() as u8;
        if bit < self.len && bit < other.len {
            let diff = self.with_flipped_bit(bit).bits ^ other.bits;
            let bit = diff.leading_zeros() as u8;
            bit >= self.len || bit >= other.len
        } else {
            false
        }
    }

    pub fn substituted_in(&self, mut name: Name) -> Name {
        let mask = self.len_mask();
        name.0 &= !mask;
        name.0 |= self.bits;
        name
    }

    fn len_mask(&self) -> u64 {
        if self.len == 0 {
            0
        } else {
            (-1i64 as u64) << (64 - self.len)
        }
    }
}

impl FromStr for Prefix {
    type Err = ParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut prefix = Self::EMPTY;
        for c in input.chars() {
            match c {
                '0' => {
                    prefix = prefix.extend(0);
                }
                '1' => {
                    prefix = prefix.extend(1);
                }
                _ => {
                    return Err(ParseError);
                }
            }
        }
        Ok(prefix)
    }
}

impl fmt::Display for Prefix {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        for i in 0..self.len {
            let mask = 1 << (63 - i);
            if self.bits & mask == 0 {
                write!(fmt, "0")?;
            } else {
                write!(fmt, "1")?;
            }
        }

        Ok(())
    }
}

impl fmt::Debug for Prefix {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Prefix({})", self)
    }
}
