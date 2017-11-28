use std::collections::{BTreeSet, BTreeMap};
use std::fmt;
use std::iter::Iterator;
use prefix::Prefix;
use rand::Rng;

pub const GROUP_SIZE: usize = 8;

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct Node {
    name: u64,
    age: u8,
}

impl fmt::Debug for Node {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let (b0, b1, b2) = (
            (self.name >> 56) as u8,
            (self.name >> 48) as u8,
            (self.name >> 40) as u8,
        );
        write!(
            fmt,
            "Node({:02x}{:02x}{:02x}...; age={})",
            b0,
            b1,
            b2,
            self.age
        )
    }
}

impl Node {
    pub fn new(name: u64) -> Node {
        Node { name, age: 1 }
    }

    pub fn get_older(&mut self) {
        self.age += 1;
    }

    pub fn rejoined(&mut self) {
        if self.age > 1 {
            self.age /= 2;
        }
    }

    pub fn name(&self) -> u64 {
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
}

#[derive(Clone, Debug)]
pub struct Section {
    prefix: Prefix,
    nodes: BTreeMap<u64, Node>,
    elders: BTreeSet<u64>,
    adults: BTreeSet<u64>,
    infants: BTreeSet<u64>,
}

impl Section {
    pub fn new(prefix: Prefix) -> Section {
        Section {
            prefix,
            nodes: BTreeMap::new(),
            elders: BTreeSet::new(),
            adults: BTreeSet::new(),
            infants: BTreeSet::new(),
        }
    }

    pub fn update_elders(&mut self) {
        let mut by_age: Vec<_> = self.nodes.iter().filter(|&(_, n)| n.is_adult()).collect();
        by_age.sort_by_key(|&(_, x)| -(x.age as i8));
        self.elders = by_age
            .into_iter()
            .take(GROUP_SIZE)
            .map(|(name, _)| *name)
            .collect();
    }

    pub fn add(&mut self, node: Node) {
        assert!(self.prefix.matches(node.name()));
        if node.is_adult() {
            self.adults.insert(node.name());
        } else {
            self.infants.insert(node.name());
        }
        self.nodes.insert(node.name(), node);
        self.update_elders();
    }

    pub fn remove(&mut self, name: u64) -> Option<Node> {
        let node = self.nodes.remove(&name);
        let _ = self.adults.remove(&name);
        let _ = self.infants.remove(&name);
        self.update_elders();
        node
    }

    pub fn prefix(&self) -> Prefix {
        self.prefix
    }

    pub fn split(self) -> (Section, Section) {
        let (prefix0, prefix1) = (self.prefix.extend(0), self.prefix.extend(1));
        let (mut section0, mut section1) = (Section::new(prefix0), Section::new(prefix1));
        for (name, node) in self.nodes {
            if prefix0.matches(name) {
                section0.add(node);
            } else if prefix1.matches(name) {
                section1.add(node);
            } else {
                panic!("Node {:?} found in section {:?}", node, self.prefix);
            }
        }
        (section0, section1)
    }

    pub fn merge(self, other: Section) -> Section {
        let merged_prefix = self.prefix.shorten();
        let mut result = Section::new(merged_prefix);
        for (_, node) in self.nodes.into_iter().chain(other.nodes.into_iter()) {
            result.add(node);
        }
        result
    }

    pub fn should_split(&self) -> bool {
        false
    }

    pub fn should_merge(&self) -> bool {
        false
    }

    pub fn nodes(&self) -> BTreeSet<Node> {
        self.nodes.iter().map(|(_, n)| *n).collect()
    }
}

#[derive(Clone, Debug)]
pub struct Network {
    nodes: BTreeMap<Prefix, Section>,
    left_nodes: Vec<Node>,
}

impl Network {
    pub fn new() -> Network {
        let mut nodes = BTreeMap::new();
        nodes.insert(Prefix::empty(), Section::new(Prefix::empty()));
        Network {
            nodes,
            left_nodes: Vec::new(),
        }
    }

    fn add_node(&mut self, node: Node) {
        let mut should_split = None;
        for (p, s) in &mut self.nodes {
            if p.matches(node.name()) {
                s.add(node);
                if s.should_split() {
                    should_split = Some(*p);
                }
                break;
            }
        }
        if let Some(prefix) = should_split {
            let section = self.nodes.remove(&prefix).unwrap();
            let (section0, section1) = section.split();
            self.nodes.insert(section0.prefix(), section0);
            self.nodes.insert(section1.prefix(), section1);
        }
    }

    pub fn add_random_node<R: Rng>(&mut self, rng: &mut R) {
        let node = Node::new(rng.gen());
        self.add_node(node);
    }

    fn total_drop_weight(&self) -> f64 {
        self.nodes
            .iter()
            .flat_map(|(_, s)| s.nodes().into_iter())
            .map(|n| n.drop_probability())
            .sum()
    }

    pub fn drop_random_node<R: Rng>(&mut self, rng: &mut R) {
        let total_weight = self.total_drop_weight();
        let mut drop = rng.gen::<f64>() * total_weight;
        let node_and_prefix = {
            let mut res = None;
            let nodes_iter = self.nodes.iter().flat_map(|(p, s)| {
                s.nodes().into_iter().map(move |n| (*p, n))
            });
            for (p, n) in nodes_iter {
                if n.drop_probability() < drop {
                    res = Some((p, n.name()));
                    break;
                }
                drop -= n.drop_probability();
            }
            res
        };
        let node = node_and_prefix.and_then(|(prefix, name)| {
            self.nodes.get_mut(&prefix).and_then(|section| {
                section.remove(name).and_then(|node| {
                    Some((
                        node,
                        if section.should_merge() {
                            Some(prefix)
                        } else {
                            None
                        },
                    ))
                })
            })
        });
        if let Some((node, should_merge)) = node {
            self.left_nodes.push(node);
            if let Some(prefix) = should_merge {
                let merged_pfx = prefix.shorten();
                let sections: Vec<_> = self.nodes
                    .keys()
                    .filter(|&pfx| merged_pfx.is_ancestor(pfx))
                    .cloned()
                    .collect();
                let mut sections: Vec<_> = sections
                    .into_iter()
                    .filter_map(|pfx| self.nodes.remove(&pfx))
                    .collect();
                while sections.len() > 1 {
                    sections.sort_by_key(|s| s.prefix());
                    let section1 = sections.pop().unwrap();
                    let section2 = sections.pop().unwrap();
                    sections.push(section1.merge(section2));
                }
                let section = sections.pop().unwrap();
                self.nodes.insert(section.prefix(), section);
            }
        }
    }

    pub fn rejoin_random_node<R: Rng>(&mut self, rng: &mut R) {
        let node_index = rng.gen_range(0, self.left_nodes.len());
        let mut node = self.left_nodes.remove(node_index);
        node.rejoined();
        self.add_node(node);
    }
}
