use HashMap;
use HashSet;
use chain::{Block, Chain, Event, Hash};
use log;
use message::{Request, Response};
use node::{self, Node};
use params::{Params, RelocationStrategy};
use prefix::{Name, Prefix};
use random;
use std::collections::BTreeSet;
use std::fmt;
use std::mem;
use std::u64;

pub struct Section {
    prefix: Prefix,
    state: State,
    nodes: HashMap<Name, Node>,
    chain: Chain,
    requests: Vec<Request>,
    relocating_in_nodes: BTreeSet<Name>,
    relocating_out_nodes: BTreeSet<Name>,
}

impl Section {
    pub fn new(prefix: Prefix) -> Self {
        Section {
            prefix,
            state: State::Stable,
            nodes: HashMap::default(),
            chain: Chain::new(),
            requests: Vec::new(),
            relocating_in_nodes: BTreeSet::new(),
            relocating_out_nodes: BTreeSet::new(),
        }
    }

    pub fn prefix(&self) -> Prefix {
        self.prefix
    }

    pub fn nodes(&self) -> &HashMap<Name, Node> {
        &self.nodes
    }

    #[allow(unused)]
    pub fn is_complete(&self, params: &Params) -> bool {
        node::count_adults(params, self.nodes.values()) >= params.group_size
    }

    pub fn receive(&mut self, request: Request) {
        self.requests.push(request)
    }

    pub fn clear_relocating_cache(&mut self) {
        self.relocating_out_nodes.clear();
        self.relocating_in_nodes.clear();
    }

    pub fn handle_requests(&mut self, params: &Params) -> Vec<Response> {
        let mut responses = Vec::new();

        for request in mem::replace(&mut self.requests, Vec::new()) {
            debug!(
                "{}: received {}",
                log::prefix(&self.prefix),
                log::message(&request)
            );

            responses.extend(match request {
                Request::Live(node) => self.handle_live(params, node),
                Request::Dead(name) => self.handle_dead(params, name),
                Request::Merge(prefix) => self.handle_merge(params, prefix),
                Request::RelocateRequest(src, name, node_name) => {
                    self.handle_relocate_request(params, src, name, node_name)
                }
                Request::RelocateAccept(dst, node_name) => {
                    self.handle_relocate_accept(dst, node_name)
                }
                Request::Relocate(node) => self.handle_relocate(params, node),
                Request::RelocateReject(target, node_name) => {
                    self.handle_relocate_reject(target, node_name)
                }
            })
        }

        responses
    }

    pub fn merge(&mut self, params: &Params, other: Section) {
        assert_eq!(self.prefix, other.prefix);
        self.chain.extend(other.chain);
        self.nodes.extend(other.nodes);
        self.requests.extend(other.requests);
        self.relocating_out_nodes.clear();
        let _ = self.update_elders(params, false);
    }

    /// Handle new node attempt to join us.
    fn handle_live(&mut self, params: &Params, node: Node) -> Vec<Response> {
        // If we are splitting or merging, forward the request to the target
        // section(s) of the split/merge.
        match self.state {
            State::Stable => (),
            State::Splitting => {
                for prefix in &self.prefix.split() {
                    if prefix.matches(node.name()) {
                        debug!(
                            "{}: split in progress. Forwarding request to {}",
                            log::prefix(&self.prefix),
                            log::prefix(prefix)
                        );

                        return vec![Response::Send(*prefix, Request::Live(node))];
                    }
                }

                unreachable!()
            }
            State::Merging(prefix) => {
                debug!(
                    "{}: merge in progress. Forwarding request to {}",
                    log::prefix(&self.prefix),
                    log::prefix(&prefix)
                );

                return vec![Response::Send(prefix, Request::Live(node))];
            }
        }

        // During startup, nodes joining as adult (age of 5), and no relocation.
        let startup = self.prefix == Prefix::EMPTY;

        let new_node = if startup {
            Node::new(node.name(), params.adult_age)
        } else if node.is_infant(params) &&
                   node::count_infants(params, self.nodes.values()) >=
                       params.max_infants_per_section
        {
            return self.reject_node(node);
        } else {
            node
        };

        let age = new_node.age();
        let name = new_node.name();
        let is_adult = new_node.is_adult(params);

        self.add_node(new_node);
        // A relocated adult shall only trigger relocate once.
        // It's promotion shall not trigger relocation.
        let _ = self.update_elders(params, false);

        let responses = self.try_split(params);
        if !responses.is_empty() {
            responses
        } else if is_adult && !startup {
            self.try_relocate(params, Block::new(Event::Live, name, age))
        } else {
            Vec::new()
        }
    }

    fn handle_dead(&mut self, params: &Params, name: Name) -> Vec<Response> {
        if let Some(_node) = self.drop_node(name) {
            let mut responses = self.update_elders(params, true);
            responses.extend(self.try_merge(params));
            responses
        } else {
            Vec::new()
        }
    }

    fn handle_merge(&mut self, params: &Params, parent: Prefix) -> Vec<Response> {
        match self.state {
            State::Merging(old_parent) => {
                if old_parent.is_ancestor(&parent) {
                    return Vec::new();
                } else {
                    return vec![Response::Send(old_parent, Request::Merge(parent))];
                }
            }
            State::Splitting => {
                let prefixes = self.prefix.split();

                debug!(
                    "{}: split in progress. Forwarding request to {}, {}",
                    log::prefix(&self.prefix),
                    log::prefix(&prefixes[0]),
                    log::prefix(&prefixes[1])
                );

                return vec![
                    Response::Send(prefixes[0], Request::Merge(parent)),
                    Response::Send(prefixes[1], Request::Merge(parent)),
                ];
            }
            _ => (),
        }

        debug!(
            "{}: merging {} adults into {}",
            log::prefix(&self.prefix),
            node::count_adults(params, self.nodes.values()),
            log::prefix(&parent),
        );

        let mut section = Section::new(parent);
        section.chain = self.chain.clone();
        section.nodes = mem::replace(&mut self.nodes, HashMap::default());

        self.state = State::Merging(parent);

        vec![Response::Merge(section, self.prefix)]
    }

    fn handle_relocate(&mut self, params: &Params, node: Node) -> Vec<Response> {
        if !self.relocating_in_nodes.remove(&node.name()) {
            return Vec::new();
        }
        let new_name = random::gen();

        // Pick the new node name so it would fall into the subsection with
        // fewer members, to keep the section balanced.
        let prefixes = self.prefix.split();
        let count0 = node::count_matching_adults(params, prefixes[0], self.nodes.values());
        let count1 = node::count_matching_adults(params, prefixes[1], self.nodes.values());

        let new_name = if count0 < count1 {
            prefixes[0].substituted_in(new_name)
        } else {
            prefixes[1].substituted_in(new_name)
        };

        debug!(
            "relocating {} -> {} to {}",
            log::name(&node.name()),
            log::name(&new_name),
            log::prefix(&self.prefix),
        );

        self.handle_live(params, Node::new(new_name, node.age()))
    }

    fn handle_relocate_accept(&mut self, dst: Prefix, node_name: Name) -> Vec<Response> {
        if self.relocating_out_nodes.remove(&node_name) {
            if let Some(mut node) = self.nodes.remove(&node_name) {
                node.increment_age();
                if node.is_elder() {
                    node.demote();
                    self.chain.insert(Event::Dead, node_name, node.age());
                }

                return vec![Response::Send(dst, Request::Relocate(node))];
            }
        }
        Vec::new()
    }

    fn handle_relocate_reject(&self, _target: Name, node_name: Name) -> Vec<Response> {
        if self.relocating_out_nodes.contains(&node_name) {
            let dst: Name = random::gen();
            vec![Response::RelocateRequest(self.prefix, dst, node_name)]
        } else {
            Vec::new()
        }
    }

    fn handle_relocate_request(
        &mut self,
        params: &Params,
        src: Prefix,
        target: Name,
        node_name: Name,
    ) -> Vec<Response> {
        if !self.relocating_in_nodes.is_empty() || self.nodes.len() >= params.max_section_size {
            vec![
                Response::Send(src, Request::RelocateReject(target, node_name)),
            ]
        } else {
            let _ = self.relocating_in_nodes.insert(node_name);
            vec![
                Response::Send(src, Request::RelocateAccept(self.prefix, node_name)),
            ]
        }
    }

    fn try_split(&mut self, params: &Params) -> Vec<Response> {
        // We can only split if both section post-split would remain with at least
        // 2 * GROUP_SIZE - QUORUM adults.

        let prefixes = self.prefix.split();

        if prefixes[0] == self.prefix || prefixes[1] == self.prefix {
            panic!(
                "{:?}: Maximum prefix length reached. Can't split",
                self.prefix
            );
        }

        let num_adults0 = node::count_matching_adults(params, prefixes[0], self.nodes.values());
        let num_adults1 = node::count_matching_adults(params, prefixes[1], self.nodes.values());

        let limit = 2 * params.group_size - params.quorum();
        if num_adults0 >= limit && num_adults1 >= limit {
            debug!(
                "{}: initiating split into {} and {}",
                log::prefix(&self.prefix),
                log::prefix(&prefixes[0]),
                log::prefix(&prefixes[1])
            );

            let mut section0 = Section::new(prefixes[0]);
            let mut section1 = Section::new(prefixes[1]);

            section0.chain = self.chain.clone();
            section1.chain = self.chain.clone();

            let (nodes0, nodes1) = self.nodes.drain().partition(
                |&(name, _)| if prefixes[0].matches(
                    name,
                )
                {
                    true
                } else if prefixes[1].matches(name) {
                    false
                } else {
                    unreachable!()
                },
            );

            section0.nodes = nodes0;
            let _ = section0.update_elders(params, false);

            section1.nodes = nodes1;
            let _ = section1.update_elders(params, false);

            self.state = State::Splitting;

            vec![Response::Split(section0, section1, self.prefix)]
        } else {
            Vec::new()
        }
    }

    fn try_merge(&mut self, params: &Params) -> Vec<Response> {
        if self.prefix == Prefix::EMPTY {
            // We are the root section - nobody to merge with.
            return Vec::new();
        }

        if node::count_adults(params, self.nodes.values()) >= params.group_size {
            // We have enough adults, not need to merge.
            return Vec::new();
        }

        let sibling = self.prefix.sibling();
        let parent = self.prefix.shorten();

        debug!(
            "{}: initiating merge with {} into {}",
            log::prefix(&self.prefix),
            log::prefix(&sibling),
            log::prefix(&parent)
        );

        vec![
            Response::Send(self.prefix, Request::Merge(parent)),
            Response::Send(sibling, Request::Merge(parent)),
        ]
    }

    fn try_relocate(&mut self, params: &Params, live_block: Block) -> Vec<Response> {
        // If the relocation would trigger merge, don't relocate.
        if node::count_adults(params, self.nodes.values()) <= params.group_size {
            return Vec::new();
        }
        // When there is alread node waiting for relocation, don't relocate.
        if !self.relocating_out_nodes.is_empty() {
            return Vec::new();
        }

        let mut hash = live_block.hash();

        for _ in 0..params.max_relocation_attempts {
            if let Some(name) = self.check_relocate(params, &hash) {
                let _ = self.relocating_out_nodes.insert(name);
                let target: Name = random::gen();
                return vec![Response::RelocateRequest(self.prefix, target, name)];
            } else {
                hash = hash.hash();
            }
        }

        Vec::new()
    }

    fn check_relocate(&self, params: &Params, hash: &Hash) -> Option<Name> {
        // Find the youngest or oldest node depending on the `RelocationStrategy` for which
        // `hash % 2^age == 0`. If there is more than one, apply the tie-breaking rule.

        let mut candidates = self.relocation_candidates(params, hash);
        if candidates.is_empty() {
            return None;
        }

        match params.relocation_strategy {
            RelocationStrategy::YoungestFirst => {
                candidates.sort_by_key(|node| node.age());
            }
            RelocationStrategy::OldestFirst => {
                candidates.sort_by_key(|node| u64::MAX - node.age());
            }
        }

        let age = candidates[0].age();
        let index = candidates
            .iter()
            .position(|node| node.age() != age)
            .unwrap_or(candidates.len());
        candidates.truncate(index);

        if candidates.len() == 1 {
            Some(candidates[0].name())
        } else {
            break_ties(candidates)
        }
    }

    fn relocation_candidates(&self, _params: &Params, hash: &Hash) -> Vec<&Node> {
        // Formula: `hash % 2^age == 0`

        // let hash = BigUint::from_bytes_le(&hash[..]);
        // let two = BigUint::from(2u32);
        // let zero = BigUint::from(0u32);

        // self.nodes
        //     .values()
        //     .filter(|node| {
        //         hash.clone() % pow(two.clone(), node.age() as usize) == zero
        //     })
        //     .collect()

        // This is equivalent but more efficient:
        let trailing_zeros = hash.trailing_zeros();
        self.nodes
            .values()
            .filter(|node| node.age() <= trailing_zeros)
            .collect()
    }

    fn update_elders(&mut self, params: &Params, relocate: bool) -> Vec<Response> {
        let old: HashSet<_> = self.nodes
            .values()
            .filter(|node| node.is_elder())
            .map(|node| node.name())
            .collect();
        let new: HashSet<_> = {
            let mut new = node::by_age(self.nodes.values());
            new.reverse();
            new.into_iter()
                .take(params.group_size)
                .map(|node| node.name())
                .collect()
        };

        let mut promoted_nodes = vec![];
        for node in self.nodes.values_mut() {
            let old = old.contains(&node.name());
            let new = new.contains(&node.name());

            if old && !new {
                node.demote();
                self.chain.insert(Event::Gone, node.name(), node.age());
            }

            if new && !old {
                node.promote();
                self.chain.insert(Event::Live, node.name(), node.age());
                promoted_nodes.push(Node::new(node.name(), node.age()));
            }
        }

        if relocate && promoted_nodes.len() == 1 {
            let node = promoted_nodes.first().unwrap();
            self.try_relocate(params, Block::new(Event::Live, node.name(), node.age()))
        } else {
            Vec::new()
        }
    }

    fn add_node(&mut self, node: Node) {
        debug!(
            "{}: added {}",
            log::prefix(&self.prefix),
            log::name(&node.name())
        );
        let _ = self.nodes.insert(node.name(), node);
    }

    fn reject_node(&self, node: Node) -> Vec<Response> {
        debug!(
            "{}: rejected {}",
            log::prefix(&self.prefix),
            log::name(&node.name())
        );
        vec![Response::Reject(node)]
    }

    fn drop_node(&mut self, name: Name) -> Option<Node> {
        if let Some(node) = self.nodes.remove(&name) {
            debug!(
                "{}: dropped {}",
                log::prefix(&self.prefix),
                log::name(&name)
            );
            Some(node)
        } else {
            None
        }
    }
}

impl fmt::Debug for Section {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Section({})", self.prefix)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum State {
    Stable,
    Splitting,
    Merging(Prefix),
}

fn break_ties(mut nodes: Vec<&Node>) -> Option<Name> {
    let total = nodes.iter().fold(0, |total, node| total ^ node.name().0);
    nodes.sort_by_key(|node| node.name().0 ^ total);
    nodes.first().map(|node| node.name())
}
