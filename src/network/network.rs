use std::collections::BTreeMap;
use std::fmt;
use std::mem;
use std::iter::{Iterator, Sum};
use rand::Rng;
use network::prefix::Prefix;
use network::node::Node;
use network::section::Section;
use network::churn::NetworkEvent;

#[derive(Clone)]
pub struct Network {
    adds: u64,
    drops: u64,
    rejoins: u64,
    nodes: BTreeMap<Prefix, Section>,
    left_nodes: Vec<Node>,
    event_queue: BTreeMap<Prefix, Vec<NetworkEvent>>,
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
        }
    }

    fn has_events(&self) -> bool {
        self.event_queue.values().any(|x| !x.is_empty())
    }

    pub fn process_events<R: Rng>(&mut self, rng: &mut R) {
        while self.has_events() {
            let queue = mem::replace(&mut self.event_queue, BTreeMap::new());
            for (prefix, events) in queue {
                for event in events {
                    self.process_single_event(rng, prefix, event);
                }
            }
        }
    }

    fn process_single_event<R: Rng>(&mut self, rng: &mut R, prefix: Prefix, event: NetworkEvent) {
        match event {
            NetworkEvent::Live(_) |
            NetworkEvent::Gone(_) |
            NetworkEvent::Lost(_) => {
                if let Some(section) = self.nodes.get_mut(&prefix) {
                    let queue = self.event_queue.entry(prefix).or_insert_with(Vec::new);
                    queue.extend(section.handle_event(event));
                }
            }
            NetworkEvent::Relocated(n) => {
                self.relocate(rng, n);
            }
            NetworkEvent::PrefixChange(pfx) => {
                if pfx.len() < prefix.len() {
                    // merging
                } else {
                    // splitting
                    if let Some(section) = self.nodes.remove(&prefix) {
                        let _ = self.event_queue.remove(&prefix);
                        let (sd0, sd1) = section.split();
                        self.event_queue
                            .entry(sd0.0.prefix())
                            .or_insert_with(Vec::new)
                            .extend(sd0.1);
                        self.event_queue
                            .entry(sd1.0.prefix())
                            .or_insert_with(Vec::new)
                            .extend(sd1.1);
                        self.nodes.insert(sd0.0.prefix(), sd0.0);
                        self.nodes.insert(sd1.0.prefix(), sd1.0);
                    }
                }
            }
        }
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
