use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use network::{BUFFER, GROUP_SIZE};
use network::prefix::{Name, Prefix};
use network::node::{Digest, Node};
use network::churn::{NetworkEvent, SectionEvent};
use params::Params;

/// An enum for return values of some methods.
/// The methods can say that the event was ignored, in which case its processing ends as if nothing
/// ever happened. If the event was handled, it could generate some additional response to the
/// network.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EventResult {
    Handled,
    HandledWithEvent(SectionEvent),
    Ignored,
}

/// Returns the number of trailing zeros in a hash
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

/// A section after a split together with events it needs to process afterwards.
pub type SplitData = (Section, Vec<NetworkEvent>);

/// The structure representing a section.
/// It has a prefix and some nodes. The nodes are sorted into categories: Elders, Adults and
/// Infants, according to their age an function in the section.
#[derive(Clone)]
pub struct Section {
    /// the section's prefix
    prefix: Prefix,
    /// the prefix used to verify whether a node belongs to the section; should only differ from
    /// `prefix` during merges
    verifying_prefix: Prefix,
    /// the nodes belonging to the section
    nodes: BTreeMap<Name, Node>,
    /// the names of the Elders
    elders: BTreeSet<Name>,
    /// the names of the Adults (including the Elders)
    adults: BTreeSet<Name>,
    /// the names of the Infants (including the Elders, if some of them are Infants during the
    /// network startup phase)
    infants: BTreeSet<Name>,
    /// are we currently merging?
    merging: bool,
    /// are we currently splitting?
    splitting: bool,
}

impl Section {
    /// Creates a new, empty section
    pub fn new(prefix: Prefix) -> Section {
        Section {
            prefix,
            verifying_prefix: prefix,
            nodes: BTreeMap::new(),
            elders: BTreeSet::new(),
            adults: BTreeSet::new(),
            infants: BTreeSet::new(),
            merging: false,
            splitting: false,
        }
    }

    /// Returns the number of nodes in the section
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the list of nodes in the section sorted by age.
    fn nodes_by_age(&self) -> Vec<Node> {
        let mut by_age: Vec<_> = self.nodes.iter().map(|(_, n)| *n).collect();
        by_age.sort_by_key(|x| -(x.age() as i8));
        by_age
    }

    /// Returns whether the section has a complete group.
    /// A complete group is GROUP_SIZE nodes that are Adults (have age > 4)
    pub fn is_complete(&self) -> bool {
        self.elders.len() == 8
            && self.elders
                .iter()
                .filter_map(|x| self.nodes.get(x))
                .all(|n| n.is_adult())
    }

    /// Updates the names of the Elders in the section
    fn update_elders(&mut self) {
        let by_age = self.nodes_by_age();
        self.elders = by_age
            .into_iter()
            .take(GROUP_SIZE)
            .filter(|n| n.is_adult())
            .map(|n| n.name())
            .collect();
    }

    /// Processes a network event passed to the section and responds with appropriate section
    /// events
    pub fn handle_event(&mut self, event: NetworkEvent, params: &Params) -> Vec<SectionEvent> {
        let mut events = vec![];
        let other_event = match event {
            NetworkEvent::Live(node) => self.add(node, params),
            NetworkEvent::Relocated(node) | NetworkEvent::Gone(node) => self.relocate(node.name()),
            NetworkEvent::Lost(name) => self.remove(name),
            NetworkEvent::PrefixChange(p) => {
                println!("{:?} PrefixChange to {:?}", self.prefix, p);
                EventResult::Handled
            }
            NetworkEvent::StartMerge(prefix) => {
                if prefix.is_ancestor(&self.verifying_prefix) {
                    // in order to accept new nodes, we must know that we are merging
                    self.verifying_prefix = prefix;
                    self.merging = true;
                    println!(
                        "MERGE: {:?} (verifying: {:?}) StartMerge({:?})",
                        self.prefix, self.verifying_prefix, prefix
                    );
                    EventResult::Handled
                } else {
                    EventResult::Ignored
                }
            }
        };
        if self.should_merge(params) {
            self.merging = true;
            events.push(SectionEvent::RequestMerge);
        }
        if self.should_split(params) {
            self.splitting = true;
            println!("{:?} Requesting a split", self.prefix);
            events.push(SectionEvent::RequestSplit);
        }
        match other_event {
            EventResult::Handled => {
                events.extend(self.check_ageing(event));
            }
            EventResult::HandledWithEvent(ev) => {
                events.extend(self.check_ageing(event));
                events.push(ev);
            }
            EventResult::Ignored => (),
        }
        events
    }

    /// Return the node that should be relocated, with age no greater than `age`
    fn choose_for_relocation(&self, age: u8) -> Option<Node> {
        let by_age: Vec<_> = self.nodes_by_age()
            .into_iter()
            .filter(|n| n.age() <= age)
            .collect();
        let candidates = by_age.first().cloned().map(|n| {
            by_age
                .into_iter()
                .filter(|m| m.age() == n.age())
                .collect::<Vec<_>>()
        });
        candidates.and_then(|mut cand| {
            if cand.len() <= 1 {
                cand.first().cloned()
            } else {
                let total_xor = cand.iter().fold(0, |total, node| total ^ node.name().0);
                cand.sort_by_key(|node| node.name().0 ^ total_xor);
                cand.first().cloned()
            }
        })
    }

    /// Checks the hash of the NetworkEvent and returns any SectionEvents triggered by it due to
    /// node ageing - in particular, relocations
    fn check_ageing(&mut self, event: NetworkEvent) -> Vec<SectionEvent> {
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
        let node_to_age = self.choose_for_relocation(trailing_zeros);
        if let Some(node) = node_to_age {
            let _ = self.relocate(node.name());
            vec![SectionEvent::NeedRelocate(node)]
        } else {
            vec![]
        }
    }

    /// Adds a node to the section and returns whether the event was handled
    fn add(&mut self, node: Node, params: &Params) -> EventResult {
        if !params.norejectyoung && node.age() == params.init_age
            && self.nodes.values().any(|n| n.age() == params.init_age)
            && self.is_complete()
        {
            // disallow more than one node aged 1 per section if the section is complete
            // (all elders are adults)
            println!("Node {:?} refused in section {:?}", node, self.prefix);
            return EventResult::HandledWithEvent(SectionEvent::NodeRejected(node));
        }
        assert!(
            self.verifying_prefix.matches(node.name()),
            "Section {:?}: {:?} does not match {:?}!",
            self.prefix,
            node.name(),
            self.verifying_prefix
        );
        if node.is_adult() {
            self.adults.insert(node.name());
        } else {
            self.infants.insert(node.name());
        }
        self.nodes.insert(node.name(), node);
        self.update_elders();
        if !node.is_adult() && self.is_complete() {
            EventResult::Ignored
        } else {
            EventResult::Handled
        }
    }

    /// Removes a node from the section and returns whether the event was handled
    fn remove(&mut self, name: Name) -> EventResult {
        let node = self.nodes.remove(&name);
        let _ = self.adults.remove(&name);
        let _ = self.infants.remove(&name);
        self.update_elders();
        if let Some(node) = node {
            if !node.is_adult() && self.is_complete() {
                EventResult::Ignored
            } else {
                EventResult::HandledWithEvent(SectionEvent::NodeDropped(node))
            }
        } else {
            EventResult::Ignored
        }
    }

    /// Relocates a node from the section - that is, removes it, but doesn't generate a `Dropped`
    /// section event, which would cause the network to think that the node has actually left
    fn relocate(&mut self, name: Name) -> EventResult {
        let node = self.nodes.remove(&name);
        let _ = self.adults.remove(&name);
        let _ = self.infants.remove(&name);
        self.update_elders();
        if let Some(node) = node {
            if !node.is_adult() && self.is_complete() {
                EventResult::Ignored
            } else {
                EventResult::Handled
            }
        } else {
            EventResult::Ignored
        }
    }

    /// Returns the section's prefix
    pub fn prefix(&self) -> Prefix {
        self.prefix
    }

    /// Splits the section into two and generates the corresponding churn events
    pub fn split(mut self, params: &Params) -> (SplitData, SplitData) {
        self.splitting = false;
        let mut churn0 = vec![];
        let mut churn1 = vec![];
        let (prefix0, prefix1) = (self.prefix.extend(0), self.prefix.extend(1));
        println!(
            "Splitting {:?} into {:?} and {:?}",
            self.prefix, prefix0, prefix1
        );
        let (mut section0, mut section1) = (self.clone(), self);
        section0.prefix = prefix0;
        section0.verifying_prefix = prefix0;
        section1.prefix = prefix1;
        section1.verifying_prefix = prefix1;
        for (name, mut node) in &mut section0.nodes {
            if params.inc_age {
                node.increment_age();
            }
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

    /// Merges two sections into one
    /// The churn events for a merge are generated by the network, not the section, as the section
    /// has no knowledge of other sections it is merging with
    pub fn merge(self, other: Section, params: &Params) -> Section {
        assert!(
            self.prefix.is_sibling(&other.prefix),
            "Attempt to merge {:?} with {:?}",
            self.prefix,
            other.prefix
        );
        let merged_prefix = self.prefix.shorten();
        let mut result = Section::new(merged_prefix);
        // for multi-level merges - the next level must remember to verify against
        // the fully-merged prefix
        result.verifying_prefix = if self.verifying_prefix.len() < other.verifying_prefix.len() {
            self.verifying_prefix
        } else {
            other.verifying_prefix
        };
        if merged_prefix.len() < result.verifying_prefix.len() {
            result.verifying_prefix = merged_prefix;
        }
        for (_, mut node) in self.nodes.into_iter().chain(other.nodes.into_iter()) {
            if params.inc_age {
                node.increment_age();
            }
            result.add(node, params);
        }
        result
    }

    fn count_adults(&self, prefix: &Prefix) -> usize {
        self.adults.iter().filter(|&n| prefix.matches(*n)).count()
    }

    fn count_peers(&self, prefix: &Prefix) -> usize {
        if self.is_complete() {
            self.count_adults(prefix)
        } else {
            self.nodes
                .iter()
                .filter(|&(n, _)| prefix.matches(*n))
                .count()
        }
    }

    /// Returns whether the section should split. If we are already splitting, returns false
    pub fn should_split(&self, params: &Params) -> bool {
        use params::Strategy::*;
        let prefix0 = self.prefix.extend(0);
        let prefix1 = self.prefix.extend(1);
        let count_prefix = match params.split_strategy {
            Complete => Self::count_adults,
            Always => Self::count_peers,
        };
        let count0 = count_prefix(self, &prefix0);
        let count1 = count_prefix(self, &prefix1);
        !self.merging && !self.splitting && count0 >= GROUP_SIZE + BUFFER
            && count1 >= GROUP_SIZE + BUFFER
    }

    /// Returns whether the section should merge. If we are already merging, returns false
    pub fn should_merge(&self, params: &Params) -> bool {
        use params::Strategy::*;
        match params.split_strategy {
            Complete => {
                !self.merging && !self.splitting && self.prefix.len() > 0
                    && self.adults.len() <= GROUP_SIZE
            }
            Always => {
                !self.merging && !self.splitting && self.prefix.len() > 0 && if self.is_complete() {
                    self.adults.len() <= GROUP_SIZE
                } else {
                    self.nodes.len() <= GROUP_SIZE
                }
            }
        }
    }

    /// Returns a set of all the nodes in the section
    pub fn nodes(&self) -> BTreeSet<Node> {
        self.nodes.iter().map(|(_, n)| *n).collect()
    }

    /// Returns the section's Elders as `Node`s
    pub fn elders(&self) -> BTreeSet<Node> {
        self.elders
            .iter()
            .filter_map(|name| self.nodes.get(name))
            .cloned()
            .collect()
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
