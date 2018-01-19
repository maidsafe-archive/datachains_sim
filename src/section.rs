use HashMap;
use HashSet;
use chain::{Chain, Event, Hash};
use log;
use message::{Request, Response};
use node::{self, Node};
use params::Params;
use prefix::{Name, Prefix};
use random::random;
use std::fmt;
use std::mem;

pub struct Section {
    prefix: Prefix,
    nodes: HashMap<Name, Node>,
    chain: Chain,
    requests: Vec<Request>,
}

impl Section {
    pub fn new(prefix: Prefix) -> Self {
        Section {
            prefix,
            nodes: HashMap::default(),
            chain: Chain::new(),
            requests: Vec::new(),
        }
    }

    pub fn prefix(&self) -> Prefix {
        self.prefix
    }

    pub fn nodes(&self) -> &HashMap<Name, Node> {
        &self.nodes
    }

    pub fn has_infants(&self, params: &Params) -> bool {
        self.nodes.values().any(|node| node.is_infant(params))
    }

    pub fn is_complete(&self, params: &Params) -> bool {
        node::count_adults(params, self.nodes.values()) >= params.group_size
    }

    pub fn receive(&mut self, request: Request) {
        self.requests.push(request)
    }

    pub fn handle_requests(&mut self, params: &Params) -> Vec<Response> {
        let mut responses = Vec::new();

        for request in mem::replace(&mut self.requests, Vec::new()) {
            println!(
                "{}: received {}",
                log::prefix(&self.prefix),
                log::message(&request)
            );

            responses.extend(match request {
                Request::Live(node) => self.handle_live(params, node),
                Request::Dead(name) => self.handle_dead(params, name),
                Request::Merge(prefix) => self.handle_merge(params, prefix),
            })
        }

        responses
    }

    pub fn merge(&mut self, other: Section) {
        assert_eq!(self.prefix, other.prefix);
        self.chain.extend(other.chain);
        self.nodes.extend(other.nodes);
        self.requests.extend(other.requests);
    }

    /// Handle new node attempt to join us.
    fn handle_live(&mut self, params: &Params, node: Node) -> Vec<Response> {
        if self.prefix == Prefix::EMPTY {
            // If we are the root section (our prefix is empty), bump everyone's
            // (except the new node) age by one.
            for node in self.nodes.values_mut() {
                node.increment_age()
            }
        } else if node.is_infant(params) && self.has_infants(params) {
            return self.reject_node(node);
        }

        let name = node.name();
        let is_adult = node.is_adult(params);

        self.add_node(node);
        self.update_elders(params);

        // Check if we have too many nodes.
        if self.nodes.len() > params.max_section_size {
            return vec![Response::Fail(self.prefix)];
        }

        let responses = self.try_split(params);
        if !responses.is_empty() {
            responses
        } else if is_adult {
            self.try_relocate(params, Some(name))
        } else {
            Vec::new()
        }
    }

    fn handle_dead(&mut self, params: &Params, name: Name) -> Vec<Response> {
        if let Some(node) = self.remove_node(name) {
            self.update_elders(params);

            let responses = self.try_merge(params);
            if !responses.is_empty() {
                return responses;
            }

            if node.is_adult(params) {
                return self.try_relocate(params, None);
            }
        }

        Vec::new()
    }

    fn handle_merge(&mut self, _params: &Params, parent: Prefix) -> Vec<Response> {
        for node in self.nodes.values_mut() {
            node.increment_age();
            if node.is_elder() {
                node.demote();
                self.chain.insert(Event::Gone, node.name(), node.age());
            }
        }

        let mut section = Section::new(parent);
        section.chain = self.chain.clone();
        section.nodes = mem::replace(&mut self.nodes, HashMap::default());

        vec![Response::Add(section), Response::Remove(self.prefix)]
    }

    fn try_split(&mut self, params: &Params) -> Vec<Response> {
        // We can only split if both section post-split would remain with at least
        // 2 * GROUP_SIZE - QUORUM adults.

        let prefix0 = self.prefix.extend(0);
        let prefix1 = self.prefix.extend(1);

        let num_adults0 = node::count_adults(
            params,
            self.nodes.values().filter(
                |node| prefix0.matches(node.name()),
            ),
        );

        let num_adults1 = node::count_adults(
            params,
            self.nodes.values().filter(
                |node| prefix1.matches(node.name()),
            ),
        );

        let limit = 2 * params.group_size - params.quorum();
        if num_adults0 >= limit && num_adults1 >= limit {
            println!(
                "{}: {} into {} and {}",
                log::prefix(&self.prefix),
                log::important("initiating split"),
                log::prefix(&prefix0),
                log::prefix(&prefix1)
            );

            for node in self.nodes.values_mut() {
                node.increment_age();
                if node.is_elder() {
                    node.demote();
                    self.chain.insert(Event::Gone, node.name(), node.age());
                }
            }

            let mut section0 = Section::new(prefix0);
            let mut section1 = Section::new(prefix1);

            section0.chain = self.chain.clone();
            section1.chain = self.chain.clone();

            let (nodes0, nodes1) = self.nodes.drain().partition(
                |&(name, _)| if prefix0.matches(name) {
                    true
                } else if prefix1.matches(name) {
                    false
                } else {
                    unreachable!()
                },
            );

            section0.nodes = nodes0;
            section1.nodes = nodes1;

            vec![
                Response::Add(section0),
                Response::Add(section1),
                Response::Remove(self.prefix),
            ]
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

        println!(
            "{}: {} with {} into {}",
            log::prefix(&self.prefix),
            log::important("initiating merge"),
            log::prefix(&sibling),
            log::prefix(&parent)
        );

        vec![
            Response::Send(self.prefix, Request::Merge(parent)),
            Response::Send(sibling, Request::Merge(parent)),
        ]
    }

    fn try_relocate(&mut self, params: &Params, live_name: Option<Name>) -> Vec<Response> {
        // If the relocation would trigger merge, don't relocate.
        if node::count_adults(params, self.nodes.values()) <= params.group_size {
            return Vec::new();
        }

        let mut hash = live_name
            .and_then(|name| self.chain.last_live_of(name))
            .or_else(|| self.chain.last_live())
            .expect("no Live block in the chain")
            .hash();

        for _ in 0..params.max_relocation_attempts {
            if let Some(name) = self.check_relocate(&hash) {
                return self.relocate(name);
            } else {
                hash = hash.hash();
            }
        }

        Vec::new()
    }

    fn check_relocate(&self, hash: &Hash) -> Option<Name> {
        let candidates: Vec<_> = self.nodes
            .values()
            .filter(|node| hash.trailing_zeros() == node.age())
            .collect();

        match candidates.len() {
            0 | 1 => candidates.first().map(|node| node.name()),
            _ => break_ties(candidates),
        }
    }

    fn relocate(&mut self, name: Name) -> Vec<Response> {
        if let Some(mut node) = self.nodes.remove(&name) {
            node.set_name(random());

            println!(
                "{}: {} {} -> {}",
                log::prefix(&self.prefix),
                log::important("relocating"),
                log::name(&name),
                log::name(&node.name())
            );

            node.increment_age();
            if node.is_elder() {
                node.demote();
                self.chain.insert(Event::Dead, name, node.age());
            }

            vec![Response::Relocate(node)]
        } else {
            Vec::new()
        }
    }

    fn update_elders(&mut self, params: &Params) {
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
            }
        }
    }

    fn add_node(&mut self, node: Node) {
        println!(
            "{}: added {}",
            log::prefix(&self.prefix),
            log::name(&node.name())
        );

        let _ = self.nodes.insert(node.name(), node);
    }

    fn reject_node(&self, node: Node) -> Vec<Response> {
        println!(
            "{}: rejected {}",
            log::prefix(&self.prefix),
            log::name(&node.name())
        );
        vec![Response::Reject(node)]
    }

    fn remove_node(&mut self, name: Name) -> Option<Node> {
        if let Some(node) = self.nodes.remove(&name) {
            println!(
                "{}: removed {}",
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

fn break_ties(mut nodes: Vec<&Node>) -> Option<Name> {
    let total = nodes.iter().fold(0, |total, node| total ^ node.name().0);
    nodes.sort_by_key(|node| node.name().0 ^ total);
    nodes.first().map(|node| node.name())
}
