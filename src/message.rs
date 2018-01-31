use node::Node;
use prefix::{Name, Prefix};
use section::Section;

/// Network message (RPC).
/// Note: these do not necessarily correspond to the RPCs of the real network,
/// because this simulation abstracts of the real stuff away.
#[derive(Debug)]
pub enum Request {
    /// A node joins the network.
    Live(Node),
    /// A node left the network (disconnected).
    Dead(Name),
    /// Initiate a merge into the section with the given prefix.
    Merge(Prefix),
    /// Request to relocate a node to a section matching `dst`.
    RelocateRequest {
        src: Prefix,
        dst: Name,
        node_name: Name,
    },
    /// Relocate the given node to section.
    Relocate(Node),
    /// Accept the relocation request.
    RelocateAccept { dst: Name, node_name: Name },
    /// Reject the relocation request.
    RelocateReject { dst: Name, node_name: Name },
}

#[derive(Debug)]
pub enum Response {
    /// Merge sections.
    Merge(Section, Prefix),
    /// Split section.
    Split(Section, Section, Prefix),
    /// Reject an attempt to join a section.
    Reject(Node),
    /// Request from `src` to relocate a node to a section matching `dst`.
    RelocateRequest {
        src: Prefix,
        dst: Name,
        node_name: Name,
    },
    /// Relocate the given node to a section matching `dst`.
    Relocate { dst: Name, node: Node },
    /// Send a request to the section with the given prefix.
    Send(Prefix, Request),
}
