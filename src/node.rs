use Age;
use params::Params;
use prefix::Name;
use std::fmt;

#[derive(Eq, PartialEq, Hash)]
pub struct Node {
    name: Name,
    age: Age,
    elder: bool,
}

impl Node {
    pub fn new(name: Name, age: Age) -> Self {
        Node {
            name,
            age,
            elder: false,
        }
    }

    pub fn name(&self) -> Name {
        self.name
    }

    pub fn set_name(&mut self, name: Name) {
        self.name = name
    }

    pub fn age(&self) -> Age {
        self.age
    }

    pub fn is_infant(&self, params: &Params) -> bool {
        self.age < params.adult_age
    }

    pub fn is_adult(&self, params: &Params) -> bool {
        self.age >= params.adult_age
    }

    pub fn is_elder(&self) -> bool {
        self.elder
    }

    pub fn promote(&mut self) {
        self.elder = true
    }

    pub fn demote(&mut self) {
        self.elder = false
    }

    pub fn increment_age(&mut self) {
        self.age = self.age.saturating_add(1);
    }

    /// Returns the probability this node will be dropped.
    pub fn drop_probability(&self) -> f64 {
        2f64.powf(-(self.age as f64))
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Node({:?}; age={})", self.name, self.age)
    }
}

/// Returns how many of the nodes are adults.
pub fn count_adults<'a, I: IntoIterator<Item = &'a Node>>(params: &Params, nodes: I) -> usize {
    nodes
        .into_iter()
        .filter(|node| node.is_adult(params))
        .count()
}

pub fn count_infants<'a, I: IntoIterator<Item = &'a Node>>(params: &Params, nodes: I) -> usize {
    nodes
        .into_iter()
        .filter(|node| node.is_infant(params))
        .count()
}

/// Returns the nodes sorted by age (from youngest to oldest).
pub fn by_age<'a, I: IntoIterator<Item = &'a Node>>(nodes: I) -> Vec<&'a Node> {
    let mut nodes: Vec<_> = nodes.into_iter().collect();
    nodes.sort_by_key(|node| (node.age(), node.name()));
    nodes
}
