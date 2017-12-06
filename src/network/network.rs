use std::collections::BTreeMap;
use std::fmt;
use std::mem;
use std::iter::{Iterator, Sum};
use rand::Rng;
use network::prefix::Prefix;
use network::node::Node;
use network::section::Section;
use network::churn::{NetworkEvent, SectionEvent};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingMerge {
    complete: BTreeMap<Prefix, bool>,
}

impl PendingMerge {
    fn from_prefixes<I: IntoIterator<Item = Prefix>>(pfxs: I) -> Self {
        PendingMerge { complete: pfxs.into_iter().map(|pfx| (pfx, false)).collect() }
    }

    fn completed(&mut self, pfx: Prefix) {
        if let Some(entry) = self.complete.get_mut(&pfx) {
            *entry = true;
        }
    }

    fn is_done(&self) -> bool {
        self.complete.iter().all(|(_, &complete)| complete)
    }

    fn into_map(self) -> BTreeMap<Prefix, bool> {
        self.complete
    }
}

#[derive(Clone)]
pub struct Network {
    adds: u64,
    drops: u64,
    rejoins: u64,
    nodes: BTreeMap<Prefix, Section>,
    left_nodes: Vec<Node>,
    event_queue: BTreeMap<Prefix, Vec<NetworkEvent>>,
    pending_merges: BTreeMap<Prefix, PendingMerge>,
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
            event_queue: BTreeMap::new(),
            pending_merges: BTreeMap::new(),
        }
    }

    fn has_events(&self) -> bool {
        self.event_queue.values().any(|x| !x.is_empty())
    }

    pub fn process_events<R: Rng>(&mut self, rng: &mut R) {
        while self.has_events() {
            let queue = mem::replace(&mut self.event_queue, BTreeMap::new());
            for (prefix, events) in queue {
                let mut section_events = vec![];
                for event in events {
                    let result = self.nodes
                        .get_mut(&prefix)
                        .map(|section| section.handle_event(event))
                        .unwrap_or_else(Vec::new);
                    section_events.extend(result);
                    if let NetworkEvent::PrefixChange(pfx) = event {
                        if let Some(pending_merge) = self.pending_merges.get_mut(&pfx) {
                            pending_merge.completed(prefix);
                        }
                    }
                }
                for section_event in section_events {
                    self.process_single_event(rng, prefix, section_event);
                }
            }
        }
        let merges_to_finalise: Vec<_> = self.pending_merges
            .iter()
            .filter(|&(_, pm)| pm.is_done())
            .map(|(pfx, _)| *pfx)
            .collect();
        for pfx in merges_to_finalise {
            println!("Finalising a merge into {:?}", pfx);
            let pending_merge = self.pending_merges.remove(&pfx).unwrap().into_map();
            let merged_section = self.merged_section(pending_merge.keys(), true);
            self.nodes.insert(merged_section.prefix(), merged_section);
        }
    }

    fn process_single_event<R: Rng>(&mut self, rng: &mut R, prefix: Prefix, event: SectionEvent) {
        match event {
            SectionEvent::NodeDropped(node) => {
                self.left_nodes.push(node);
            }
            SectionEvent::NeedRelocate(node) => self.relocate(rng, node),
            SectionEvent::RequestMerge => {
                self.merge(prefix);
            }
            SectionEvent::RequestSplit => {
                if let Some(section) = self.nodes.remove(&prefix) {
                    let ((sec0, ev0), (sec1, ev1)) = section.split();
                    let _ = self.event_queue.remove(&prefix);
                    self.event_queue
                        .entry(sec0.prefix())
                        .or_insert_with(Vec::new)
                        .extend(ev0);
                    self.event_queue
                        .entry(sec1.prefix())
                        .or_insert_with(Vec::new)
                        .extend(ev1);
                    self.nodes.insert(sec0.prefix(), sec0);
                    self.nodes.insert(sec1.prefix(), sec1);
                }
            }
        }
    }

    fn merged_section<'a, I: IntoIterator<Item = &'a Prefix> + Clone>(
        &mut self,
        prefixes: I,
        destructive: bool,
    ) -> Section {
        let mut sections: Vec<_> = prefixes
            .clone()
            .into_iter()
            .filter_map(|pfx| if destructive {
                let _ = self.event_queue.remove(pfx);
                self.nodes.remove(pfx)
            } else {
                self.nodes.get(pfx).cloned()
            })
            .collect();

        while sections.len() > 1 {
            sections.sort_by_key(|s| s.prefix());
            let section1 = sections.pop().unwrap();
            let section2 = sections.pop().unwrap();
            let section = section1.merge(section2);
            sections.push(section);
        }

        sections.pop().unwrap()
    }

    fn merge(&mut self, prefix: Prefix) {
        let merged_pfx = prefix.shorten();
        if self.pending_merges.contains_key(&merged_pfx) {
            return;
        }
        println!("Initiating a merge into {:?}", merged_pfx);
        let prefixes: Vec<_> = self.nodes
            .keys()
            .filter(|&pfx| merged_pfx.is_ancestor(pfx))
            .cloned()
            .collect();

        let pending_merge = PendingMerge::from_prefixes(prefixes.iter().cloned());
        self.pending_merges.insert(merged_pfx, pending_merge);

        let merged_section = self.merged_section(prefixes.iter(), false);
        for pfx in prefixes {
            let events = self.calculate_merge_events(&merged_section, pfx);
            self.event_queue
                .entry(pfx)
                .or_insert_with(Vec::new)
                .extend(events);
        }
    }

    fn calculate_merge_events(&self, merged: &Section, pfx: Prefix) -> Vec<NetworkEvent> {
        let old_elders = self.nodes.get(&pfx).unwrap().elders();
        let new_elders = merged.elders();
        let mut events = vec![NetworkEvent::StartMerge(merged.prefix())];
        for lost_elder in &old_elders - &new_elders {
            events.push(NetworkEvent::Gone(lost_elder));
        }
        for gained_elder in &new_elders - &old_elders {
            events.push(NetworkEvent::Live(gained_elder));
        }
        events.push(NetworkEvent::PrefixChange(merged.prefix()));
        events
    }

    pub fn add_random_node<R: Rng>(&mut self, rng: &mut R) {
        self.adds += 1;
        let node = Node::new(rng.gen());
        println!("Adding node {:?}", node);
        let prefix = self.prefix_for_node(node).unwrap();
        self.event_queue
            .entry(prefix)
            .or_insert_with(Vec::new)
            .push(NetworkEvent::Live(node));
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

    fn relocate<R: Rng>(&mut self, rng: &mut R, mut node: Node) {
        let (node, neighbour) = {
            let src_section = self.nodes
                .keys()
                .find(|&pfx| pfx.matches(node.name()))
                .unwrap();
            let mut neighbours: Vec<_> = self.nodes
                .keys()
                .filter(|&pfx| pfx.is_neighbour(src_section))
                .collect();
            //if node.is_adult() {
            neighbours.sort_by_key(|pfx| (pfx.len(), self.nodes.get(pfx).unwrap().len()));
            //} else {
            //    rng.shuffle(&mut neighbours);
            //}
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
            (node, neighbour)
        };
        self.event_queue
            .entry(*neighbour)
            .or_insert_with(Vec::new)
            .push(NetworkEvent::Live(node));
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
                if n.drop_probability() > drop {
                    res = Some((p, n.name()));
                    break;
                }
                drop -= n.drop_probability();
            }
            res
        };
        node_and_prefix.map(|(prefix, name)| {
            println!("Dropping node {:?} from section {:?}", name, prefix);
            self.event_queue
                .entry(prefix)
                .or_insert_with(Vec::new)
                .push(NetworkEvent::Lost(name));
        });
    }

    pub fn rejoin_random_node<R: Rng>(&mut self, rng: &mut R) {
        self.rejoins += 1;
        rng.shuffle(&mut self.left_nodes);
        if let Some(mut node) = self.left_nodes.pop() {
            println!("Rejoining node {:?}", node);
            node.rejoined();
            let prefix = self.prefix_for_node(node).unwrap();
            self.event_queue
                .entry(prefix)
                .or_insert_with(Vec::new)
                .push(NetworkEvent::Live(node));
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
