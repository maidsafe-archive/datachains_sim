use std::collections::{BTreeSet, BTreeMap};
use std::fmt;
use std::iter::Iterator;
use prefix::Prefix;
use rand::Rng;
use serde_json;
use tiny_keccak::sha3_256;

pub const GROUP_SIZE: usize = 8;
pub const BUFFER: usize = 3;
type Digest = [u8; 32];

fn trailing_zeros(hash: Digest) -> u8 {
    let mut result = 0;
    let mut byte_index = 31;
    loop {
        let zeros = hash[byte_index].trailing_zeros();
        result += zeros;
        if zeros < 8 || byte_index == 0 {
            break;
        }
        byte_index -= 1;
    }
    result as u8
}

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn relocate<R: Rng>(&mut self, rng: &mut R, prefix: &Prefix) {
        self.name = prefix.substituted_in(rng.gen());
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

#[derive(Serialize, Deserialize)]
enum ChurnEvent {
    PeerAdded(Node),
    PeerRemoved(Node),
    Merge(Prefix),
    Split(Prefix),
}

impl ChurnEvent {
    fn hash(&self) -> Digest {
        let serialized = serde_json::to_vec(self).unwrap();
        sha3_256(&serialized)
    }

    fn get_node(&self) -> Option<Node> {
        match *self {
            ChurnEvent::PeerAdded(n) |
            ChurnEvent::PeerRemoved(n) => Some(n),
            _ => None,
        }
    }
}

enum ChurnResult {
    Dropped(Node),
    Relocate(Node),
}

#[derive(Clone)]
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

    fn nodes_by_age(&self) -> Vec<Node> {
        let mut by_age: Vec<_> = self.nodes.iter().map(|(_, n)| *n).collect();
        by_age.sort_by_key(|x| -(x.age as i8));
        by_age
    }

    fn update_elders(&mut self) {
        let by_age = self.nodes_by_age();
        self.elders = by_age
            .into_iter()
            .take(GROUP_SIZE)
            .filter(|n| n.is_adult())
            .map(|n| n.name())
            .collect();
    }

    fn add(&mut self, node: Node) -> Vec<ChurnResult> {
        assert!(self.prefix.matches(node.name()));
        if node.is_adult() {
            self.adults.insert(node.name());
        } else {
            self.infants.insert(node.name());
        }
        self.nodes.insert(node.name(), node);
        self.update_elders();
        self.churn(ChurnEvent::PeerAdded(node))
    }

    fn remove_or_relocate<F: FnOnce(Node) -> ChurnResult>(
        &mut self,
        name: u64,
        f: F,
    ) -> Vec<ChurnResult> {
        let node = self.nodes.remove(&name);
        let _ = self.adults.remove(&name);
        let _ = self.infants.remove(&name);
        self.update_elders();
        if let Some(node) = node {
            let mut result = self.churn(ChurnEvent::PeerRemoved(node));
            result.push(f(node));
            result
        } else {
            vec![]
        }
    }

    fn remove(&mut self, name: u64) -> Vec<ChurnResult> {
        self.remove_or_relocate(name, ChurnResult::Dropped)
    }

    fn relocate(&mut self, name: u64) -> Vec<ChurnResult> {
        self.remove_or_relocate(name, ChurnResult::Relocate)
    }

    pub fn prefix(&self) -> Prefix {
        self.prefix
    }

    fn split(self) -> (Section, Section, Vec<ChurnResult>) {
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
        let mut churn = section0.churn(ChurnEvent::Split(self.prefix));
        churn.extend(section1.churn(ChurnEvent::Split(self.prefix)));
        (section0, section1, churn)
    }

    fn merge(self, other: Section) -> (Section, Vec<ChurnResult>) {
        let merged_prefix = self.prefix.shorten();
        let mut result = Section::new(merged_prefix);
        for (_, node) in self.nodes.into_iter().chain(other.nodes.into_iter()) {
            result.add(node);
        }
        let prefix = result.prefix();
        let churn_result = result.churn(ChurnEvent::Merge(prefix));
        (result, churn_result)
    }

    pub fn should_split(&self) -> bool {
        self.adults.len() >= 2 * (GROUP_SIZE + BUFFER)
    }

    pub fn should_merge(&self) -> bool {
        self.adults.len() <= GROUP_SIZE
    }

    pub fn nodes(&self) -> BTreeSet<Node> {
        self.nodes.iter().map(|(_, n)| *n).collect()
    }

    fn churn(&mut self, event: ChurnEvent) -> Vec<ChurnResult> {
        if let Some(node) = event.get_node() {
            if !node.is_adult() && self.prefix.len() > 4 {
                return vec![];
            }
        }
        let event_hash = event.hash();
        let trailing_zeros = trailing_zeros(event_hash);
        let by_age = self.nodes_by_age();
        let node_to_age = by_age.into_iter().find(|n| n.age <= trailing_zeros);
        if let Some(node) = node_to_age {
            self.relocate(node.name())
        } else {
            vec![]
        }
    }
}

impl fmt::Debug for Section {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "Section {{\n\tprefix: {:?}\n\telders: {}\n\tadults: {}\n\tinfants: {}\n\tall nodes: {:?}\n}}",
            self.prefix,
            self.elders.len(),
            self.adults.len() - self.elders.len(),
            self.infants.len(),
            self.nodes.values().collect::<Vec<_>>()
        )
    }
}

#[derive(Clone)]
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

    fn add_node<R: Rng>(&mut self, rng: &mut R, node: Node) {
        let mut should_split = None;
        let mut churn = vec![];
        for (p, s) in &mut self.nodes {
            if p.matches(node.name()) {
                println!("Adding node {:?} to section {:?}", node, p);
                churn.extend(s.add(node));
                if s.should_split() {
                    should_split = Some(*p);
                }
                break;
            }
        }
        if let Some(prefix) = should_split {
            let section = self.nodes.remove(&prefix).unwrap();
            let (section0, section1, split_churn) = section.split();
            self.nodes.insert(section0.prefix(), section0);
            self.nodes.insert(section1.prefix(), section1);
            churn.extend(split_churn);
        }
        self.handle_churn(rng, churn);
    }

    pub fn add_random_node<R: Rng>(&mut self, rng: &mut R) {
        let node = Node::new(rng.gen());
        self.add_node(rng, node);
    }

    fn total_drop_weight(&self) -> f64 {
        self.nodes
            .iter()
            .flat_map(|(_, s)| s.nodes().into_iter())
            .map(|n| n.drop_probability())
            .sum()
    }

    fn merge_if_necessary(&mut self, node: Node) -> Vec<ChurnResult> {
        let section_to_merge = self.nodes
            .iter_mut()
            .find(|&(ref pfx, _)| pfx.matches(node.name()))
            .and_then(|(_, section)| if section.should_merge() {
                Some(section.prefix())
            } else {
                None
            });
        if let Some(prefix) = section_to_merge {
            self.merge(prefix)
        } else {
            vec![]
        }
    }

    fn merge(&mut self, prefix: Prefix) -> Vec<ChurnResult> {
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
        let mut churn_results = vec![];
        while sections.len() > 1 {
            sections.sort_by_key(|s| s.prefix());
            let section1 = sections.pop().unwrap();
            let section2 = sections.pop().unwrap();
            let (section, churn_result) = section1.merge(section2);
            sections.push(section);
            churn_results.extend(churn_result);
        }
        let section = sections.pop().unwrap();
        self.nodes.insert(section.prefix(), section);
        churn_results
    }

    fn relocate<R: Rng>(&mut self, rng: &mut R, mut node: Node) {
        {
            let src_section = self.nodes
                .keys()
                .find(|&pfx| pfx.matches(node.name()))
                .unwrap();
            let neighbours: Vec<_> = self.nodes
                .keys()
                .filter(|&pfx| pfx.is_neighbour(src_section))
                .collect();
            let neighbour = if let Some(n) = rng.choose(&neighbours) {
                n
            } else {
                src_section
            };
            node.relocate(rng, neighbour);
        }
        self.add_node(rng, node);
    }

    fn handle_churn<R: Rng>(&mut self, rng: &mut R, churn: Vec<ChurnResult>) {
        let mut churn_result = churn;
        loop {
            let mut new_churn = vec![];
            for result in churn_result {
                match result {
                    ChurnResult::Dropped(node) => {
                        self.left_nodes.push(node);
                        new_churn.extend(self.merge_if_necessary(node));
                    }
                    ChurnResult::Relocate(node) => {
                        self.relocate(rng, node);
                        new_churn.extend(self.merge_if_necessary(node));
                    }
                }
            }
            churn_result = new_churn;
            if churn_result.is_empty() {
                break;
            }
        }
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
        node_and_prefix.map(|(prefix, name)| if let Some(results) =
            self.nodes.get_mut(&prefix).map(
                |section| section.remove(name),
            )
        {
            self.handle_churn(rng, results);
        });
    }

    pub fn rejoin_random_node<R: Rng>(&mut self, rng: &mut R) {
        rng.shuffle(&mut self.left_nodes);
        if let Some(mut node) = self.left_nodes.pop() {
            node.rejoined();
            self.add_node(rng, node);
        }
    }
}

impl fmt::Debug for Network {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "Network {{\n\n{:?}\nleft_nodes: {:?}\n\n}}",
            self.nodes.values(),
            self.left_nodes
        )
    }
}
