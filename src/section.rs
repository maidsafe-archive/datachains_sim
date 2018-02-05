use HashMap;
use HashSet;
use chain::{Block, Chain, Event, Hash};
use log;
use message::{Action, Message};
use node::{self, Node};
use params::Params;
use prefix::{Name, Prefix};
use random;
use std::collections::hash_map::{self, Entry};
use std::fmt;
use std::mem;
use std::u8;

pub struct Section {
    prefix: Prefix,
    nodes: HashMap<Name, Node>,
    chain: Chain,
    messages: Vec<Message>,
    incoming_relocations: HashMap<Name, Name>,
    outgoing_relocations: HashMap<Name, Name>,
    recent_join: bool,
    recent_drop: bool,
}

impl Section {
    pub fn new(prefix: Prefix) -> Self {
        Section {
            prefix,
            nodes: HashMap::default(),
            chain: Chain::new(),
            messages: Vec::new(),
            incoming_relocations: HashMap::default(),
            outgoing_relocations: HashMap::default(),
            recent_join: false,
            recent_drop: false,
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

    pub fn incoming_relocations(&self) -> hash_map::Keys<Name, Name> {
        self.incoming_relocations.keys()
    }

    pub fn outgoing_relocations(&self) -> hash_map::Keys<Name, Name> {
        self.outgoing_relocations.keys()
    }

    /// Call this at the begining of each simulation tick to reset some internal state.
    pub fn prepare(&mut self) {
        self.recent_join = false;
        self.recent_drop = false;
    }

    /// Single simulation iteration of this section.
    /// Note: there can be multiple section ticks per network tick.
    pub fn tick(&mut self, params: &Params) -> Vec<Action> {
        let mut actions = Vec::new();
        let mut relocated_in = false;

        for message in mem::replace(&mut self.messages, Vec::new()) {
            debug!(
                "{}: received {}",
                log::prefix(&self.prefix),
                log::message(&message)
            );

            match message {
                Message::RelocateRequest { node_name, target } => {
                    actions.push(if relocated_in {
                        Action::Send(Message::RelocateReject { node_name, target })
                    } else {
                        self.handle_relocate_request(params, node_name, target)
                    })
                }
                Message::RelocateAccept { node_name, target } => {
                    actions.extend(self.handle_relocate_accept(node_name, target))
                }
                Message::RelocateReject { node_name, target } => {
                    actions.extend(self.handle_relocate_reject(params, node_name, target));
                }
                Message::RelocateCommit { node, .. } => {
                    if let Some(action) = self.handle_relocate_commit(params, &node) {
                        relocated_in = true;
                        actions.push(action);
                    }
                }
                Message::RelocateCancel { node_name, .. } => self.handle_relocate_cancel(node_name),
            }
        }

        if !relocated_in {
            if self.incoming_relocations.is_empty() {
                if random::gen() {
                    actions.extend(self.random_join(params));
                    actions.extend(self.random_drop(params));
                } else {
                    actions.extend(self.random_drop(params));
                    actions.extend(self.random_join(params));
                }
            } else {
                actions.extend(self.random_drop(params));
            }
        }

        actions
    }

    /// Receive a message. The messages are actually handled later, during `tick`.
    pub fn receive(&mut self, message: Message) {
        self.messages.push(message)
    }

    pub fn split(self, params: &Params) -> (Section, Section) {
        let prefixes = self.prefix.split();

        debug!(
            "{}: splitting into {} and {}",
            log::prefix(&self.prefix),
            log::prefix(&prefixes[0]),
            log::prefix(&prefixes[1]),
        );

        let mut section0 = Section::new(prefixes[0]);
        let mut section1 = Section::new(prefixes[1]);

        section0.chain = self.chain.clone();
        section1.chain = self.chain;

        // Nodes
        let (nodes0, nodes1) = split(self.nodes, prefixes[0], prefixes[1], |&(name, _)| name);

        section0.nodes = nodes0;
        section0.update_elders(params);

        section1.nodes = nodes1;
        section1.update_elders(params);

        // Outgoing relocations
        let (nodes0, nodes1) = split(
            self.outgoing_relocations,
            prefixes[0],
            prefixes[1],
            |&(name, _)| name,
        );

        section0.outgoing_relocations = nodes0;
        section1.outgoing_relocations = nodes1;

        // Incoming relocations
        let (nodes0, nodes1) = split(
            self.incoming_relocations,
            prefixes[0],
            prefixes[1],
            |&(_, target)| target,
        );

        section0.incoming_relocations = nodes0;
        section1.incoming_relocations = nodes1;

        // Messages
        for message in self.messages {
            let target = message.target();

            if prefixes[0].matches(target) {
                section0.messages.push(message);
            } else if prefixes[1].matches(target) {
                section1.messages.push(message);
            } else {
                unreachable!()
            }
        }

        (section0, section1)
    }

    pub fn merge(&mut self, params: &Params, other: Section) {
        debug!(
            "{}: merging {} adults from {}",
            log::prefix(&self.prefix),
            node::count_adults(params, other.nodes.values()),
            log::prefix(&other.prefix),
        );

        self.chain.extend(other.chain);
        self.nodes.extend(other.nodes);
        self.messages.extend(other.messages);
        self.incoming_relocations.extend(other.incoming_relocations);
        self.outgoing_relocations.extend(other.outgoing_relocations);
        self.update_elders(params);
    }

    fn handle_live(&mut self, params: &Params, mut node: Node) -> Option<Action> {
        // During startup, nodes joining as adult (age of 5), and no relocation.
        if self.prefix == Prefix::EMPTY {
            node = Node::new(node.name(), params.adult_age)
        } else if node.is_infant(params) &&
                   node::count_infants(params, self.nodes.values()) >=
                       params.max_infants_per_section
        {
            return Some(self.reject_node(node));
        }

        let name = node.name();
        let age = node.age();
        let is_adult = node.is_adult(params);

        self.join_node(node);
        self.update_elders(params);

        if let Some(action) = self.try_split(params) {
            Some(action)
        } else if is_adult {
            self.try_relocate(params, &Block::new(Event::Live, name, age))
        } else {
            None
        }
    }

    fn handle_dead(&mut self, params: &Params, name: Name) -> Vec<Action> {
        let mut actions = Vec::new();

        if let Some(node) = self.drop_node(name) {
            if let Some(target) = self.outgoing_relocations.remove(&node.name()) {
                debug!(
                    "{}: cancelling relocation of {} (node dropped)",
                    log::prefix(&self.prefix),
                    log::name(&node.name())
                );

                actions.push(Action::Send(Message::RelocateCancel {
                    node_name: node.name(),
                    target,
                }));
            }

            actions.extend(self.try_merge(params));

            if node.is_adult(params) {
                self.update_elders(params);
                if let Some(block) = self.chain.last_live() {
                    actions.extend(self.try_relocate(params, &block));
                }
            }
        }

        actions
    }

    fn handle_relocate_request(
        &mut self,
        params: &Params,
        node_name: Name,
        target: Name,
    ) -> Action {
        if !self.incoming_relocations.is_empty() || self.nodes.len() >= params.max_section_size {
            debug!(
                "{}: rejecting relocation of {}",
                log::prefix(&self.prefix),
                log::name(&node_name),
            );

            Action::Send(Message::RelocateReject { node_name, target })
        } else {
            debug!(
                "{}: accepting relocation of {}",
                log::prefix(&self.prefix),
                log::name(&node_name),
            );

            let _ = self.incoming_relocations.insert(node_name, target);
            Action::Send(Message::RelocateAccept { node_name, target })
        }
    }

    fn handle_relocate_accept(&mut self, node_name: Name, target: Name) -> Option<Action> {
        if self.outgoing_relocations.remove(&node_name).is_some() {
            if let Some(mut node) = self.nodes.remove(&node_name) {
                node.increment_age();
                if node.is_elder() {
                    node.demote();
                    self.chain.insert(
                        Block::new(Event::Dead, node_name, node.age()),
                    );
                }

                return Some(Action::Send(Message::RelocateCommit { node, target }));
            }
        }

        None
    }

    fn handle_relocate_reject(
        &mut self,
        params: &Params,
        node_name: Name,
        target: Name,
    ) -> Option<Action> {
        match self.outgoing_relocations.entry(node_name) {
            Entry::Occupied(mut entry) => {
                // Do not retry the relocation during startup or if it would trigger merge.
                if self.prefix == Prefix::EMPTY ||
                    node::count_adults(params, self.nodes.values()) <= params.group_size
                {
                    debug!(
                        "{}: cancelling relocation of {} (not beneficial anymore)",
                        log::prefix(&self.prefix),
                        log::name(entry.key())
                    );

                    entry.remove();
                    None
                } else {
                    // Calculate new relocation target.
                    let target = Hash::from(target).rehash().into();

                    debug!(
                        "{}: re-initiating relocation of {} to {}",
                        log::prefix(&self.prefix),
                        log::name(entry.key()),
                        log::name(&target)
                    );

                    *entry.get_mut() = target;
                    Some(Action::Send(Message::RelocateRequest { node_name, target }))
                }
            }
            Entry::Vacant(_) => None,
        }
    }

    fn handle_relocate_commit(&mut self, params: &Params, node: &Node) -> Option<Action> {
        if self.incoming_relocations.remove(&node.name()).is_none() {
            panic!(
                "{}: cannot commit relocation of {}: not found in incoming relocation cache",
                log::prefix(&self.prefix),
                log::name(&node.name())
            );
        }

        // Pick the new node name so it would fall into the subsection with
        // fewer members, to keep the section balanced.
        let prefixes = self.prefix.split();
        let count0 = node::count_matching_adults(params, prefixes[0], self.nodes.values());
        let count1 = node::count_matching_adults(params, prefixes[1], self.nodes.values());

        let new_name = random::gen();
        let new_name = if count0 < count1 {
            prefixes[0].substituted_in(new_name)
        } else {
            prefixes[1].substituted_in(new_name)
        };

        debug!(
            "{}: relocating {} -> {}",
            log::prefix(&self.prefix),
            log::name(&node.name()),
            log::name(&new_name),
        );

        self.handle_live(params, Node::new(new_name, node.age()))
    }

    fn handle_relocate_cancel(&mut self, node_name: Name) {
        let _ = self.incoming_relocations.remove(&node_name);
    }

    // Simulate random node attempt to join this section.
    fn random_join(&mut self, params: &Params) -> Option<Action> {
        if self.recent_join {
            return None;
        }
        self.recent_join = true;

        let name = self.prefix.substituted_in(random::gen());
        self.handle_live(params, Node::new(name, params.init_age))
    }

    // Simulate random node disconnecting.
    fn random_drop(&mut self, params: &Params) -> Vec<Action> {
        if self.recent_drop {
            return Vec::new();
        }
        self.recent_drop = true;

        let name = node::by_age(self.nodes.values())
            .into_iter()
            .find(|node| {
                random::gen_bool_with_probability(node.drop_probability())
            })
            .map(|node| node.name());

        if let Some(name) = name {
            self.handle_dead(params, name)
        } else {
            Vec::new()
        }
    }

    fn try_split(&mut self, params: &Params) -> Option<Action> {
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

            Some(Action::Split(self.prefix))
        } else {
            None
        }
    }

    fn try_merge(&mut self, params: &Params) -> Option<Action> {
        if self.prefix == Prefix::EMPTY {
            // We are the root section - nobody to merge with.
            return None;
        }

        if node::count_adults(params, self.nodes.values()) >= params.group_size {
            // We have enough adults, not need to merge.
            return None;
        }

        let sibling = self.prefix.sibling();
        let target = self.prefix.shorten();

        debug!(
            "{}: initiating merge with {} into {}",
            log::prefix(&self.prefix),
            log::prefix(&sibling),
            log::prefix(&target)
        );

        Some(Action::Merge(target))
    }

    fn try_relocate(&mut self, params: &Params, live_block: &Block) -> Option<Action> {
        // Do not relocate during startup.
        if self.prefix == Prefix::EMPTY {
            return None;
        }

        // If the relocation would trigger merge, don't relocate.
        if node::count_adults(params, self.nodes.values()) <= params.group_size {
            return None;
        }

        // When there is alread node waiting for relocation, don't relocate.
        if !self.outgoing_relocations.is_empty() {
            return None;
        }

        let mut hash = live_block.hash();

        for _ in 0..params.max_relocation_attempts {
            if let Some(node_name) = self.check_relocate(&hash) {
                let target = hash.into();
                let _ = self.outgoing_relocations.insert(node_name, target);

                debug!(
                    "{}: initiating relocation of {} to {}",
                    log::prefix(&self.prefix),
                    log::name(&node_name),
                    log::name(&target)
                );

                return Some(Action::Send(Message::RelocateRequest { node_name, target }));
            } else {
                hash = hash.rehash();
            }
        }

        None
    }

    fn check_relocate(&self, hash: &Hash) -> Option<Name> {
        // Find the oldest node for which `hash % 2^age == 0`.
        // If there is more than one, apply the tie-breaking rule.

        let mut candidates = self.relocation_candidates(hash);
        if candidates.is_empty() {
            return None;
        }

        candidates.sort_by_key(|node| u8::MAX - node.age());

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

    fn relocation_candidates(&self, hash: &Hash) -> Vec<&Node> {
        // The actual formula is: `hash % 2^age == 0`, the following is equivalent
        // but more efficient:
        let trailing_zeros = hash.trailing_zeros() as u8;
        self.nodes
            .values()
            .filter(|node| node.age() <= trailing_zeros)
            .collect()
    }

    fn join_node(&mut self, node: Node) {
        debug!(
            "{}: added {}",
            log::prefix(&self.prefix),
            log::name(&node.name())
        );
        let _ = self.nodes.insert(node.name(), node);
    }

    fn reject_node(&self, node: Node) -> Action {
        debug!(
            "{}: rejected {}",
            log::prefix(&self.prefix),
            log::name(&node.name())
        );
        Action::Reject(node)
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

    // Promote/demote nodes so only the `GROUP_SIZE` oldest nodes are elders.
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
                self.chain.insert(
                    Block::new(Event::Gone, node.name(), node.age()),
                );
            }

            if new && !old {
                node.promote();
                self.chain.insert(
                    Block::new(Event::Live, node.name(), node.age()),
                );
            }
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

fn split<S, T, F>(nodes: S, prefix0: Prefix, prefix1: Prefix, mut name: F) -> (T, T)
where
    S: IntoIterator,
    T: Default + Extend<S::Item>,
    F: FnMut(&S::Item) -> Name,
{
    nodes.into_iter().partition(|node| {
        let name = name(node);
        if prefix0.matches(name) {
            true
        } else if prefix1.matches(name) {
            false
        } else {
            unreachable!()
        }
    })
}
