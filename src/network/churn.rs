use network::prefix::Prefix;
use network::node::{Node, Digest};
use serde_json;
use tiny_keccak::sha3_256;

#[derive(Serialize, Deserialize)]
pub enum ChurnEvent {
    PeerAdded(Node),
    PeerRemoved(Node),
    PeerRelocated(Node),
    Merge(Prefix),
    Split(Prefix),
}

impl ChurnEvent {
    pub fn hash(&self) -> Digest {
        let serialized = serde_json::to_vec(self).unwrap();
        sha3_256(&serialized)
    }

    pub fn get_node(&self) -> Option<Node> {
        match *self {
            ChurnEvent::PeerAdded(n) |
            ChurnEvent::PeerRemoved(n) => Some(n),
            _ => None,
        }
    }

    pub fn should_count(&self) -> bool {
        match *self {
            //ChurnEvent::PeerRelocated(_) => false,
            _ => true,
        }
    }
}

pub enum ChurnResult {
    Dropped(Node),
    Relocate(Node),
}
