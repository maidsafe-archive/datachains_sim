use node::Node;
use prefix::{Name, Prefix};

/// Network message (RPC).
/// Note: these do not necessarily correspond to the RPCs of the real network,
/// because this simulation abstracts lot of the real stuff away.
#[derive(Debug)]
pub enum Message {
    /// Request to relocate a node with the given name to the given target.
    RelocateRequest { node_name: Name, target: Name },
    /// Positive reponse to a relocate request.
    RelocateAccept { node_name: Name, target: Name },
    /// Negative response to a relocate request.
    RelocateReject { node_name: Name, target: Name },
    /// Actually relocate the node.
    RelocateCommit { node: Node, target: Name },
    /// Cancel a previously accepted relocate request (due to the node to be
    /// relocated disconnecting)
    RelocateCancel { node_name: Name, target: Name },
}

impl Message {
    pub fn target(&self) -> Name {
        match *self {
            Message::RelocateRequest { target, .. } |
            Message::RelocateCommit { target, .. } |
            Message::RelocateCancel { target, .. } => target,
            Message::RelocateAccept { node_name, .. } |
            Message::RelocateReject { node_name, .. } => node_name,
        }
    }
}

/// Network action.
#[derive(Debug)]
pub enum Action {
    /// Reject an attempt to join a section.
    Reject(Node),
    /// Merge all descendants of the prefix.
    Merge(Prefix),
    /// Split the section.
    Split(Prefix),
    /// Send a message.
    Send(Message),
}
