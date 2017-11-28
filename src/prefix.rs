use std::fmt;

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prefix {
    len: u8,
    bits: u64,
}

impl Prefix {
    pub fn empty() -> Prefix {
        Prefix { bits: 0, len: 0 }
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

    pub fn len(&self) -> u8 {
        self.len
    }

    fn len_mask(&self) -> u64 {
        if self.len == 0 {
            0
        } else {
            (-1i64 as u64) << (64 - self.len)
        }
    }

    pub fn shorten(self) -> Prefix {
        if self.len < 1 {
            return self;
        }
        let mask = self.len_mask() << 1;
        Prefix {
            bits: self.bits & mask,
            len: self.len - 1,
        }
    }

    pub fn with_flipped_bit(self, bit: u8) -> Prefix {
        let mask = 1 << (63 - bit);
        Prefix {
            bits: self.bits ^ mask,
            len: self.len,
        }
    }

    pub fn matches(&self, name: u64) -> bool {
        (name & self.len_mask()) ^ self.bits == 0
    }

    pub fn is_ancestor(&self, other: &Prefix) -> bool {
        self.len <= other.len && self.matches(other.bits)
    }

    pub fn is_child(&self, other: &Prefix) -> bool {
        other.is_ancestor(self)
    }

    pub fn is_compatible_with(&self, other: &Prefix) -> bool {
        self.is_ancestor(other) || self.is_child(other)
    }

    pub fn is_sibling(&self, other: &Prefix) -> bool {
        if self.len > 0 {
            (*self).with_flipped_bit(self.len - 1) == *other
        } else {
            false
        }
    }

    pub fn is_neighbour(&self, other: &Prefix) -> bool {
        let diff = self.bits ^ other.bits;
        let bit = diff.leading_zeros() as u8;
        if bit < self.len {
            let diff = self.with_flipped_bit(bit).bits ^ other.bits;
            diff.leading_zeros() as u8 >= self.len
        } else {
            false
        }
    }

    pub fn substituted_in(&self, mut name: u64) -> u64 {
        let mask = self.len_mask();
        name &= !mask;
        name |= self.bits;
        name
    }

    pub fn from_str(s: &str) -> Option<Prefix> {
        let mut prefix = Self::empty();
        for c in s.chars() {
            match c {
                '0' => {
                    prefix = prefix.extend(0);
                }
                '1' => {
                    prefix = prefix.extend(1);
                }
                _ => {
                    return None;
                }
            }
        }
        Some(prefix)
    }

    pub fn to_string(&self) -> String {
        let mut result = String::new();
        for i in 0..self.len {
            let mask = 1 << (63 - i);
            if self.bits & mask == 0 {
                result.push('0');
            } else {
                result.push('1');
            }
        }
        result
    }
}

impl fmt::Debug for Prefix {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Prefix({})", self.to_string())
    }
}
