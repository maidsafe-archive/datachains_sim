use Age;
use HashMap;
use log;
use message::{Request, Response};
use node::{self, Node};
use params::Params;
use prefix::Prefix;
use random;
use section::Section;
use stats::Stats;
use std::collections::BTreeMap;
use std::ops::AddAssign;

pub struct Network {
    params: Params,
    stats: Stats,
    sections: HashMap<Prefix, Section>,
    num_nodes: u64,
}

impl Network {
    /// Create new simulated network with the given parameters.
    pub fn new(params: Params) -> Self {
        let mut sections = HashMap::default();
        let _ = sections.insert(Prefix::EMPTY, Section::new(Prefix::EMPTY));

        Network {
            params,
            stats: Stats::new(),
            sections,
            num_nodes: 0,
        }
    }

    /// Execute single iteration of the simulation. Returns `true` if the
    /// simulation is running successfuly so far, `false` if it failed and should
    /// be stopped.
    pub fn tick(&mut self) -> bool {
        self.generate_random_messages();
        let stats = self.handle_messages();

        self.stats.record(
            self.num_nodes,
            self.sections.len() as u64,
            stats.merges,
            stats.splits,
            stats.relocations,
            stats.rejections,
        );

        self.check_section_sizes()
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    pub fn num_complete_sections(&self) -> u64 {
        self.sections
            .values()
            .filter(|section| section.is_complete(&self.params))
            .count() as u64
    }

    pub fn age_dist(&self) -> BTreeMap<Age, u64> {
        let mut result = BTreeMap::new();
        for node in self.sections.values().flat_map(
            |section| section.nodes().values(),
        )
        {
            *result.entry(node.age()).or_insert(0) += 1;
        }

        result
    }

    pub fn section_size_dist(&self) -> BTreeMap<u64, u64> {
        let mut result = BTreeMap::new();
        for section in self.sections.values() {
            *result.entry(section.nodes().len() as u64).or_insert(0) += 1;
        }

        result
    }

    fn generate_random_messages(&mut self) {
        let mut adds = 0;
        let mut drops = 0;

        for section in self.sections.values_mut() {
            if random::gen() {
                add_random_node(&self.params, section);
                adds += 1;

                if drop_random_node(&self.params, section) {
                    drops += 1;
                }
            } else {
                if drop_random_node(&self.params, section) {
                    drops += 1;
                }

                add_random_node(&self.params, section);
                adds += 1;
            }
        }

        info!(
            "Random Adds: {} Drops: {}",
            log::important(adds),
            log::important(drops)
        );
    }

    fn handle_messages(&mut self) -> TickStats {
        let mut responses = Vec::new();
        let mut stats = TickStats::new();

        loop {
            for section in self.sections.values_mut() {
                responses.extend(section.handle_requests(&self.params));
            }

            if responses.is_empty() {
                break;
            }

            stats += self.handle_responses(&mut responses)
        }

        stats
    }

    fn handle_responses(&mut self, responses: &mut Vec<Response>) -> TickStats {
        let mut stats = TickStats::new();

        for response in responses.drain(..) {
            match response {
                Response::Merge(section, old_prefix) => {
                    stats.merges += 1;
                    self.sections
                        .entry(section.prefix())
                        .or_insert_with(|| Section::new(section.prefix()))
                        .merge(&self.params, section);
                    let _ = self.sections.remove(&old_prefix);
                }
                Response::Split(section0, section1, old_prefix) => {
                    stats.splits += 1;
                    assert!(self.sections.insert(section0.prefix(), section0).is_none());
                    assert!(self.sections.insert(section1.prefix(), section1).is_none());
                    let _ = self.sections.remove(&old_prefix);
                }
                Response::Reject(_) => {
                    stats.rejections += 1;
                }
                Response::Relocate(node) => {
                    stats.relocations += 1;
                    self.handle_relocate(node)
                }
                Response::Send(prefix, request) => {
                    match request {
                        Request::Merge(target_prefix) => {
                            // The receiver of `Merge` might not exists, because
                            // it might have already split. So send the request
                            // to every section with matching prefix.
                            for section in self.sections.values_mut().filter(|section| {
                                prefix.is_ancestor(&section.prefix())
                            })
                            {
                                section.receive(Request::Merge(target_prefix))
                            }
                        }
                        _ => {
                            if let Some(section) = self.sections.get_mut(&prefix) {
                                section.receive(request)
                            } else {
                                panic!(
                                    "{} {} {}",
                                    log::error("Section with prefix"),
                                    log::prefix(&prefix),
                                    log::error("not found")
                                );
                            }
                        }
                    }
                }
                Response::Add => {
                    self.num_nodes += 1;
                }
                Response::Drop => {
                    self.num_nodes -= 1;
                }
            }
        }

        stats
    }

    fn handle_relocate(&mut self, node: Node) {
        if let Some(section) = self.sections.values_mut().find(|section| {
            section.prefix().matches(node.name())
        })
        {
            section.receive(Request::Live(node))
        } else {
            unreachable!()
        }
    }

    fn check_section_sizes(&self) -> bool {
        if let Some(section) = self.sections.values().find(|section| {
            section.nodes().len() > self.params.max_section_size
        })
        {
            // TODO: print more info
            error!(
                "{}: {}",
                log::prefix(&section.prefix()),
                log::error("too many nodes")
            );
            false
        } else {
            true
        }
    }
}

// Generate random `Live` request in the given section.
fn add_random_node(params: &Params, section: &mut Section) {
    let name = section.prefix().substituted_in(random::gen());
    section.receive(Request::Live(Node::new(name, params.init_age)));
}

// Generate random `Dead` request in the given section.
fn drop_random_node(_params: &Params, section: &mut Section) -> bool {
    let name = node::by_age(section.nodes().values())
        .into_iter()
        .find(|node| {
            random::gen_bool_with_probability(node.drop_probability())
        })
        .map(|node| node.name());

    if let Some(name) = name {
        section.receive(Request::Dead(name));
        true
    } else {
        false
    }
}

struct TickStats {
    merges: u64,
    splits: u64,
    relocations: u64,
    rejections: u64,
}

impl TickStats {
    fn new() -> Self {
        TickStats {
            merges: 0,
            splits: 0,
            relocations: 0,
            rejections: 0,
        }
    }
}

impl AddAssign for TickStats {
    fn add_assign(&mut self, other: Self) {
        self.merges += other.merges;
        self.splits += other.splits;
        self.relocations += other.relocations;
        self.rejections += other.rejections;
    }
}
