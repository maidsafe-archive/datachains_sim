use std::collections::{BTreeSet, BTreeMap};
use std::fmt;
use std::iter::{Iterator, Sum};
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

    pub fn len(&self) -> usize {
        self.nodes.len()
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

    fn add(&mut self, node: Node, ignore: bool) -> Vec<ChurnResult> {
        if node.age() == 1 && self.nodes.values().any(|n| n.age() == 1) && self.elders.len() == 8 &&
            self.elders.iter().filter_map(|x| self.nodes.get(x)).all(
                |n| {
                    n.is_adult()
                },
            )
        {
            // disallow more than one node aged 1 per section if the section is complete
            // (all elders are adults)
            return vec![];
        }
        assert!(self.prefix.matches(node.name()));
        if node.is_adult() {
            self.adults.insert(node.name());
        } else {
            self.infants.insert(node.name());
        }
        self.nodes.insert(node.name(), node);
        self.update_elders();
        if ignore {
            vec![]
        } else {
            self.churn(ChurnEvent::PeerAdded(node))
        }
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
        let mut churn = vec![];
        let (prefix0, prefix1) = (self.prefix.extend(0), self.prefix.extend(1));
        println!(
            "Splitting {:?} into {:?} and {:?}",
            self.prefix,
            prefix0,
            prefix1
        );
        let (mut section0, mut section1) = (Section::new(prefix0), Section::new(prefix1));
        for (name, node) in self.nodes {
            if prefix0.matches(name) {
                churn.extend(section0.add(node, true));
            } else if prefix1.matches(name) {
                churn.extend(section1.add(node, true));
            } else {
                panic!("Node {:?} found in section {:?}", node, self.prefix);
            }
        }
        churn.extend(section0.churn(ChurnEvent::Split(self.prefix)));
        churn.extend(section1.churn(ChurnEvent::Split(self.prefix)));
        (section0, section1, churn)
    }

    fn merge(self, other: Section) -> (Section, Vec<ChurnResult>) {
        assert!(
            self.prefix.is_sibling(&other.prefix),
            "Attempt to merge {:?} with {:?}",
            self.prefix,
            other.prefix
        );
        let merged_prefix = self.prefix.shorten();
        let mut result = Section::new(merged_prefix);
        let mut churn = vec![];
        for (_, node) in self.nodes.into_iter().chain(other.nodes.into_iter()) {
            churn.extend(result.add(node, true));
        }
        let prefix = result.prefix();
        churn.extend(result.churn(ChurnEvent::Merge(prefix)));
        (result, churn)
    }

    pub fn should_split(&self) -> bool {
        let prefix0 = self.prefix.extend(0);
        let prefix1 = self.prefix.extend(1);
        let adults0 = self.adults.iter().filter(|&n| prefix0.matches(*n)).count();
        let adults1 = self.adults.iter().filter(|&n| prefix1.matches(*n)).count();
        adults0 >= GROUP_SIZE + BUFFER && adults1 >= GROUP_SIZE + BUFFER
    }

    pub fn should_merge(&self) -> bool {
        self.prefix.len() > 0 && self.adults.len() <= GROUP_SIZE
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
            self.nodes_by_age(),
        )
    }
}

#[derive(Clone)]
pub struct Network {
    adds: u64,
    drops: u64,
    rejoins: u64,
    nodes: BTreeMap<Prefix, Section>,
    left_nodes: Vec<Node>,
}

impl Network {
    pub fn new() -> Network {
        let mut nodes = BTreeMap::new();
        nodes.insert(Prefix::empty(), Section::new(Prefix::empty()));
        Network {
            adds: 0,
            drops: 0,
            rejoins: 0,
            nodes,
            left_nodes: Vec::new(),
        }
    }

    fn add_node(&mut self, node: Node, relocation: bool) -> Vec<ChurnResult> {
        let mut should_split = None;
        let mut churn = vec![];
        for (p, s) in &mut self.nodes {
            if p.matches(node.name()) {
                churn.extend(s.add(node, relocation));
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
        churn
    }

    pub fn add_random_node<R: Rng>(&mut self, rng: &mut R) {
        self.adds += 1;
        let node = Node::new(rng.gen());
        println!("Adding node {:?}", node);
        let churn = self.add_node(node, false);
        self.handle_churn(rng, churn);
    }

    fn total_drop_weight(&self) -> f64 {
        self.nodes
            .iter()
            .flat_map(|(_, s)| s.nodes().into_iter())
            .map(|n| n.drop_probability())
            .sum()
    }

    fn prefix_for_node(&self, node: Node) -> Option<Prefix> {
        self.nodes
            .keys()
            .find(|pfx| pfx.matches(node.name()))
            .cloned()
    }

    fn merge_if_necessary(&mut self, pfx: Prefix) -> Vec<ChurnResult> {
        if self.nodes.get(&pfx).map(|s| s.should_merge()).unwrap_or(
            false,
        )
        {
            self.merge(pfx)
        } else {
            vec![]
        }
    }

    fn merge(&mut self, prefix: Prefix) -> Vec<ChurnResult> {
        let merged_pfx = prefix.shorten();
        println!("Merging into {:?}", merged_pfx);
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

    fn relocate<R: Rng>(&mut self, rng: &mut R, mut node: Node) -> Vec<ChurnResult> {
        {
            let src_section = self.nodes
                .keys()
                .find(|&pfx| pfx.matches(node.name()))
                .unwrap();
            let mut neighbours: Vec<_> = self.nodes
                .keys()
                .filter(|&pfx| pfx.is_neighbour(src_section))
                .collect();
            neighbours.sort_by_key(|pfx| (pfx.len(), self.nodes.get(pfx).unwrap().len()));
            let neighbour = if let Some(n) = neighbours.first() {
                n
            } else {
                src_section
            };
            let old_node = node.clone();
            node.relocate(rng, neighbour);
            println!(
                "Relocating {:?} from {:?} to {:?} as {:?}",
                old_node,
                src_section,
                neighbour,
                node
            );
        }
        self.add_node(node, true)
    }

    fn handle_churn<R: Rng>(&mut self, rng: &mut R, churn: Vec<ChurnResult>) {
        let mut churn_result = churn;
        loop {
            let mut new_churn = vec![];
            for result in churn_result {
                match result {
                    ChurnResult::Dropped(node) => {
                        self.left_nodes.push(node);
                        let churn = self.prefix_for_node(node)
                            .map(|pfx| self.merge_if_necessary(pfx))
                            .unwrap_or(vec![]);
                        new_churn.extend(churn);
                    }
                    ChurnResult::Relocate(node) => {
                        new_churn.extend(self.relocate(rng, node));
                        let churn = self.prefix_for_node(node)
                            .map(|pfx| self.merge_if_necessary(pfx))
                            .unwrap_or(vec![]);
                        new_churn.extend(churn);
                    }
                }
            }
            churn_result = new_churn;
            if churn_result.is_empty() {
                // final check for merges
                let prefixes: Vec<_> = self.nodes.keys().cloned().collect();
                for pfx in prefixes {
                    churn_result.extend(self.merge_if_necessary(pfx));
                }
                if churn_result.is_empty() {
                    break;
                }
            }
        }
    }

    pub fn drop_random_node<R: Rng>(&mut self, rng: &mut R) {
        self.drops += 1;
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
            println!("Dropping node {:?} from section {:?}", name, prefix);
            self.handle_churn(rng, results);
        });
    }

    pub fn rejoin_random_node<R: Rng>(&mut self, rng: &mut R) {
        self.rejoins += 1;
        rng.shuffle(&mut self.left_nodes);
        if let Some(mut node) = self.left_nodes.pop() {
            println!("Rejoining node {:?}", node);
            node.rejoined();
            let churn = self.add_node(node, false);
            self.handle_churn(rng, churn);
        }
    }
}

impl fmt::Debug for Network {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "Network {{\n\tadds: {}\n\tdrops: {}\n\trejoins: {}\n\ttotal nodes: {}\n\n{:?}\nleft_nodes: {:?}\n\n}}",
            self.adds,
            self.drops,
            self.rejoins,
            usize::sum(self.nodes.values().map(|s| s.len())),
            self.nodes.values(),
            self.left_nodes
        )
    }
}
