use std::fmt;

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct Prefix {
    bits: u64,
    len: u8,
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

    fn len_mask(&self) -> u64 {
        (-1i64 as u64) << (64 - self.len)
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
