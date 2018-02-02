use HashMap;
use log;
use message::{Action, Message};
use node;
use params::Params;
use prefix::Prefix;
use section::Section;
use stats::{Distribution, Stats};
use std::ops::AddAssign;

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

    /// Execute single iteration of the simulation.
    pub fn tick(&mut self, iteration: u64) {
        let mut actions = Vec::new();
        let mut stats = TickStats::new();

        for section in self.sections.values_mut() {
            section.prepare();
        }

        loop {
            for section in self.sections.values_mut() {
                actions.extend(section.tick(&self.params));
            }

            if actions.is_empty() {
                break;
            }

            stats += self.handle_actions(&mut actions)
        }

        self.stats.record(
            iteration,
            self.sections
                .values()
                .map(|section| section.nodes().len() as u64)
                .sum(),
            self.sections.len() as u64,
            stats.merges,
            stats.splits,
            stats.relocations,
            stats.rejections,
        );

        let _ = self.validate();
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    #[allow(unused)]
    pub fn num_complete_sections(&self) -> u64 {
        self.sections
            .values()
            .filter(|section| section.is_complete(&self.params))
            .count() as u64
    }

    pub fn age_dist(&self) -> Distribution {
        Distribution::new(
            self.sections
                .values()
                .flat_map(|section| section.nodes().values())
                .map(|node| node.age() as u64),
        )
    }

    pub fn section_size_dist(&self) -> Distribution {
        Distribution::new(self.sections.values().map(
            |section| section.nodes().len() as u64,
        ))
    }

    pub fn prefix_len_dist(&self) -> Distribution {
        Distribution::new(self.sections.keys().map(|prefix| prefix.len() as u64))
    }


    fn handle_actions(&mut self, actions: &mut Vec<Action>) -> TickStats {
        let mut stats = TickStats::new();

        for action in actions.drain(..) {
            match action {
                Action::Reject(_) => {
                    stats.rejections += 1;
                }
                Action::Merge(target) => {
                    let sources: Vec<_> = self.sections
                        .keys()
                        .filter(|prefix| prefix.is_descendant(&target))
                        .cloned()
                        .collect();

                    if sources.is_empty() {
                        // Merge action with the same target can be potentially
                        // emitted multiple times per tick.
                        // This can happen for example when both pre-merge sections
                        // lose a node in the same tick, triggering merge in both of
                        // them. That's why not finding any pre-merge section is
                        // not an error and can be safely ignored.
                        debug!(
                            "Pre-merge sections not found (to be merged to {})",
                            log::prefix(&target)
                        );
                        continue;
                    }

                    let sources: Vec<_> = sources
                        .into_iter()
                        .map(|source| self.sections.remove(&source).unwrap())
                        .collect();

                    stats.merges += 1;

                    let section = self.sections.entry(target).or_insert_with(
                        || Section::new(target),
                    );
                    for source in sources {
                        section.merge(&self.params, source);
                    }
                }
                Action::Split(source) => {
                    stats.splits += 1;

                    let source = if let Some(section) = self.sections.remove(&source) {
                        section
                    } else {
                        // Split can be triggered only by join or relocation,
                        // which can happen at most once per section tick, so
                        // a split can also happen at most once, so it should not
                        // be possible to reach this line.
                        panic!("Pre-split section {} not found", log::prefix(&source));
                    };

                    let (target0, target1) = source.split(&self.params);
                    let prefix0 = target0.prefix();
                    let prefix1 = target1.prefix();

                    assert!(
                        self.sections.insert(prefix0, target0).is_none(),
                        "section with prefix [{}] already exists",
                        prefix0
                    );
                    assert!(
                        self.sections.insert(prefix1, target1).is_none(),
                        "section with prefix [{}] already exists",
                        prefix1
                    );
                }
                Action::Send(message) => {
                    let target = message.target();
                    if let Some(section) = self.sections.values_mut().find(|section| {
                        section.prefix().matches(target)
                    })
                    {
                        if let Message::RelocateCommit { .. } = message {
                            stats.relocations += 1;
                        }

                        section.receive(message)
                    } else {
                        panic!("No section maching {:?} found", target)
                    }
                }
            }
        }

        stats
    }

    fn validate(&self) {
        for section in self.sections.values() {
            if section.nodes().len() > self.params.max_section_size {
                let prefixes = section.prefix().split();
                let count0 = node::count_matching_adults(
                    &self.params,
                    prefixes[0],
                    section.nodes().values(),
                );
                let count1 = node::count_matching_adults(
                    &self.params,
                    prefixes[1],
                    section.nodes().values(),
                );

                error!(
                    "{}: too many nodes: {} (adults per subsections: [..0]: {}, [..1]: {})",
                    log::prefix(&section.prefix()),
                    section.nodes().len(),
                    count0,
                    count1,
                );
            }

            let incoming = section.incoming_relocations();
            if incoming.len() > 0 {
                panic!(
                    "{}: incoming relocation cache not cleared: {:?}",
                    log::prefix(&section.prefix()),
                    incoming,
                )
            }

            let outgoing = section.outgoing_relocations();
            if outgoing.len() > 0 {
                panic!(
                    "{}: outgoing relocation cache not cleared: {:?}",
                    log::prefix(&section.prefix()),
                    outgoing,
                )
            }
        }
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
