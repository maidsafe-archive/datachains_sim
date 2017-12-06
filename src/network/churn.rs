use network::prefix::{Prefix, Name};
use network::node::{Node, Digest};
use serde_json;
use tiny_keccak::sha3_256;

/// Events that can happen in the network.
/// The sections handle them and generate new ones
/// in the process. Some events can also be generated from
/// the outside.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum NetworkEvent {
    Live(Node),
    Lost(Name),
    Gone(Node),
    Relocated(Node),
    PrefixChange(Prefix),
    StartMerge(Prefix),
}

impl NetworkEvent {
    /// Returns the digest of some representation of the network event:
    /// used in ageing (to determine if a peer should be relocated).
    pub fn hash(&self) -> Digest {
        let serialized = serde_json::to_vec(self).unwrap();
        sha3_256(&serialized)
    }

    /// Returns the peer passed in the event (if any).
    pub fn get_node(&self) -> Option<Node> {
        match *self {
            NetworkEvent::Live(n) |
            NetworkEvent::Gone(n) |
            NetworkEvent::Relocated(n) => Some(n),
            _ => None,
        }
    }

    /// This function determines whether an event should count towards
    /// churn in ageing peers in the section. Currently true for all events.
    pub fn should_count(&self) -> bool {
        match *self {
            NetworkEvent::StartMerge(_) => false,
            _ => true,
        }
    }
}

/// Events reported by the sections to the network.
/// The network processes them and responds with churn
/// events that the nodes would add to their data chains
/// in the real network.
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SectionEvent {
    NodeDropped(Node),
    NeedRelocate(Node),
    RequestMerge,
    RequestSplit,
}
