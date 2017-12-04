use std::collections::{BTreeSet, BTreeMap};
use std::fmt;
use network::{BUFFER, GROUP_SIZE};
use network::prefix::{Prefix, Name};
use network::node::{Node, Digest};
use network::churn::{ChurnEvent, ChurnResult};

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

#[derive(Clone)]
pub struct Section {
    prefix: Prefix,
    nodes: BTreeMap<Name, Node>,
    elders: BTreeSet<Name>,
    adults: BTreeSet<Name>,
    infants: BTreeSet<Name>,
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
        by_age.sort_by_key(|x| (-(x.age() as i8), x.hash()[0]));
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

    pub fn add(&mut self, node: Node, ignore: bool) -> Vec<ChurnResult> {
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

    fn remove_or_relocate(&mut self, name: Name, relocate: bool) -> Vec<ChurnResult> {
        let node = self.nodes.remove(&name);
        let _ = self.adults.remove(&name);
        let _ = self.infants.remove(&name);
        self.update_elders();
        if let Some(node) = node {
            if relocate {
                let mut result = self.churn(ChurnEvent::PeerRelocated(node));
                result.push(ChurnResult::Relocate(node));
                result
            } else {
                let mut result = self.churn(ChurnEvent::PeerRemoved(node));
                result.push(ChurnResult::Dropped(node));
                result
            }
        } else {
            vec![]
        }
    }

    pub fn remove(&mut self, name: Name) -> Vec<ChurnResult> {
        self.remove_or_relocate(name, false)
    }

    pub fn relocate(&mut self, name: Name) -> Vec<ChurnResult> {
        self.remove_or_relocate(name, true)
    }

    pub fn prefix(&self) -> Prefix {
        self.prefix
    }

    pub fn split(self) -> (Section, Section, Vec<ChurnResult>) {
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
                churn.extend(section0.add(node, false));
            } else if prefix1.matches(name) {
                churn.extend(section1.add(node, false));
            } else {
                panic!("Node {:?} found in section {:?}", node, self.prefix);
            }
        }
        churn.extend(section0.churn(ChurnEvent::Split(self.prefix)));
        churn.extend(section1.churn(ChurnEvent::Split(self.prefix)));
        (section0, section1, churn)
    }

    pub fn merge(self, other: Section) -> (Section, Vec<ChurnResult>) {
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
            churn.extend(result.add(node, false));
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
        if !event.should_count() {
            return vec![];
        }
        let event_hash = event.hash();
        let trailing_zeros = trailing_zeros(event_hash);
        let by_age = self.nodes_by_age();
        let node_to_age = by_age.into_iter().find(|n| n.age() <= trailing_zeros);
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
