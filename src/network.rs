use Age;
use HashMap;
use log;
use message::{Request, Response};
use node::{self, Node};
use params::Params;
use prefix::Prefix;
use random::{self, random};
use section::Section;
use stats::Stats;

pub struct Network {
    params: Params,
    stats: Stats,
    sections: HashMap<Prefix, Section>,
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
        }
    }

    /// Execute single iteration of the simulation. Returns `true` if the
    /// simulation is running successfuly so far, `false` if it failed and should
    /// be stopped.
    pub fn tick(&mut self) -> bool {
        self.generate_random_messages();
        let result = self.handle_messages();

        let num_nodes = self.sections
            .values()
            .map(|section| section.nodes().len() as u64)
            .sum();
        let num_sections = self.sections.len() as u64;
        let num_complete = self.sections
            .values()
            .filter(|section| section.is_complete(&self.params))
            .count() as u64;

        self.stats.record_sample(
            num_nodes,
            num_sections,
            num_complete,
        );

        self.stats.print_last();

        result
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    pub fn age_dist(&self) -> Vec<(Age, u64)> {
        let mut dist = HashMap::default();
        for node in self.sections.values().flat_map(
            |section| section.nodes().values(),
        )
        {
            *dist.entry(node.age()).or_insert(0) += 1;
        }

        let mut dist: Vec<_> = dist.into_iter().collect();
        dist.sort_by_key(|&(age, _)| age);
        dist
    }

    fn generate_random_messages(&mut self) {
        for section in self.sections.values_mut() {
            if random() {
                add_random_node(&self.params, section);
                drop_random_node(&self.params, section);
            } else {
                drop_random_node(&self.params, section);
                add_random_node(&self.params, section);
            }
        }
    }

    fn handle_messages(&mut self) -> bool {
        let mut responses = Vec::new();

        loop {
            for section in self.sections.values_mut() {
                responses.extend(section.handle_requests(&self.params));
            }

            if let Some(result) = self.handle_responses(&mut responses) {
                return result;
            }
        }
    }

    fn handle_responses(&mut self, responses: &mut Vec<Response>) -> Option<bool> {
        if responses.is_empty() {
            return Some(true);
        }

        for response in responses.drain(..) {
            match response {
                Response::Add(section) => {
                    self.sections
                        .entry(section.prefix())
                        .or_insert_with(|| {
                            println!(
                                "{}: {}",
                                log::prefix(&section.prefix()),
                                log::important("section added")
                            );
                            Section::new(section.prefix())
                        })
                        .merge(section)
                }
                Response::Remove(prefix) => {
                    println!(
                        "{}: {}",
                        log::prefix(&prefix),
                        log::important("section removed")
                    );
                    let _ = self.sections.remove(&prefix);
                }
                Response::Reject(_) => {
                    self.stats.record_reject();
                }
                Response::Relocate(node) => self.handle_relocate(node),
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
                    };
                }
                Response::Fail(prefix) => {
                    println!("{}: {}", log::prefix(&prefix), log::error("too many nodes"));
                    return Some(false);
                }
            }
        }

        None
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
}

// Generate random `Live` request in the given section.
fn add_random_node(params: &Params, section: &mut Section) {
    let name = section.prefix().substituted_in(random());
    section.receive(Request::Live(Node::new(name, params.init_age)))
}

// Generate random `Dead` request in the given section.
fn drop_random_node(_params: &Params, section: &mut Section) {
    let name = node::by_age(section.nodes().values())
        .into_iter()
        .find(|node| {
            random::gen_bool_with_probability(node.drop_probability())
        })
        .map(|node| node.name());

    if let Some(name) = name {
        section.receive(Request::Dead(name))
    }
}
