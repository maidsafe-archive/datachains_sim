use std::collections::{BTreeSet, BTreeMap};
use std::fmt;
use network::{BUFFER, GROUP_SIZE};
use network::prefix::{Prefix, Name};
use network::node::{Node, Digest};
use network::churn::NetworkEvent;

#[derive(Clone, Copy, PartialEq, Eq)]
enum EventResult {
    Handled,
    Ignored,
}

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

pub type SplitData = (Section, Vec<NetworkEvent>);

#[derive(Clone)]
pub struct Section {
    prefix: Prefix,
    nodes: BTreeMap<Name, Node>,
    elders: BTreeSet<Name>,
    adults: BTreeSet<Name>,
    infants: BTreeSet<Name>,
    merging: bool,
    splitting: bool,
}

impl Section {
    pub fn new(prefix: Prefix) -> Section {
        Section {
            prefix,
            nodes: BTreeMap::new(),
            elders: BTreeSet::new(),
            adults: BTreeSet::new(),
            infants: BTreeSet::new(),
            merging: false,
            splitting: false,
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

    fn is_complete(&self) -> bool {
        self.elders.len() == 8 &&
            self.elders.iter().filter_map(|x| self.nodes.get(x)).all(
                |n| {
                    n.is_adult()
                },
            )
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

    pub fn handle_event(&mut self, event: NetworkEvent) -> Vec<NetworkEvent> {
        let mut events = vec![];
        if self.should_merge() {
            events.push(NetworkEvent::PrefixChange(self.prefix.shorten()));
        }
        if self.should_split() {
            events.push(NetworkEvent::PrefixChange(self.prefix));
        }
        let result = match event {
            NetworkEvent::Live(node) => self.add(node),
            NetworkEvent::Relocated(node) |
            NetworkEvent::Gone(node) => self.remove(node.name()),
            NetworkEvent::Lost(name) => self.remove(name),
            NetworkEvent::PrefixChange(prefix) => {
                self.splitting = false;
                self.merging = false;
                self.prefix = prefix;
                EventResult::Handled
            }
        };
        if result == EventResult::Ignored {
            vec![]
        } else {
            self.check_ageing(event)
        }
    }

    fn check_ageing(&mut self, event: NetworkEvent) -> Vec<NetworkEvent> {
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
            vec![NetworkEvent::Relocated(node)]
        } else {
            vec![]
        }
    }

    fn add(&mut self, node: Node) -> EventResult {
        if node.age() == 1 && self.nodes.values().any(|n| n.age() == 1) && self.is_complete() {
            // disallow more than one node aged 1 per section if the section is complete
            // (all elders are adults)
            return EventResult::Ignored;
        }
        let verifying_prefix = if self.merging {
            self.prefix.shorten()
        } else {
            self.prefix
        };
        assert!(verifying_prefix.matches(node.name()));
        if node.is_adult() {
            self.adults.insert(node.name());
        } else {
            self.infants.insert(node.name());
        }
        self.nodes.insert(node.name(), node);
        self.update_elders();
        EventResult::Handled
    }

    fn remove(&mut self, name: Name) -> EventResult {
        let _ = self.nodes.remove(&name);
        let _ = self.adults.remove(&name);
        let _ = self.infants.remove(&name);
        self.update_elders();
        EventResult::Handled
    }

    pub fn prefix(&self) -> Prefix {
        self.prefix
    }


    pub fn split(self) -> (SplitData, SplitData) {
        let mut churn0 = vec![];
        let mut churn1 = vec![];
        let (prefix0, prefix1) = (self.prefix.extend(0), self.prefix.extend(1));
        println!(
            "Splitting {:?} into {:?} and {:?}",
            self.prefix,
            prefix0,
            prefix1
        );
        let (mut section0, mut section1) = (self.clone(), self);
        section0.prefix = prefix0;
        section1.prefix = prefix1;
        for (name, node) in &section0.nodes {
            if prefix0.matches(*name) {
                churn1.push(NetworkEvent::Gone(*node));
            } else if prefix1.matches(*name) {
                churn0.push(NetworkEvent::Gone(*node));
            } else {
                panic!(
                    "Node {:?} found in section {:?}",
                    node,
                    section0.prefix.shorten()
                );
            }
        }
        ((section0, churn0), (section1, churn1))
    }

    pub fn merge(self, other: Section) -> Section {
        assert!(
            self.prefix.is_sibling(&other.prefix),
            "Attempt to merge {:?} with {:?}",
            self.prefix,
            other.prefix
        );
        let merged_prefix = self.prefix.shorten();
        let mut result = Section::new(merged_prefix);
        for (_, node) in self.nodes.into_iter().chain(other.nodes.into_iter()) {
            result.add(node);
        }
        result
    }

    pub fn should_split(&self) -> bool {
        let prefix0 = self.prefix.extend(0);
        let prefix1 = self.prefix.extend(1);
        let adults0 = self.adults.iter().filter(|&n| prefix0.matches(*n)).count();
        let adults1 = self.adults.iter().filter(|&n| prefix1.matches(*n)).count();
        !self.splitting && adults0 >= GROUP_SIZE + BUFFER && adults1 >= GROUP_SIZE + BUFFER
    }

    pub fn should_merge(&self) -> bool {
        !self.merging && self.prefix.len() > 0 && self.adults.len() <= GROUP_SIZE
    }

    pub fn nodes(&self) -> BTreeSet<Node> {
        self.nodes.iter().map(|(_, n)| *n).collect()
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
