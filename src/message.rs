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
    /// Request whether a node can be relocated to a prefix matching section. (src, target, node)
    RelocateRequest(Prefix, Name, Name),
    /// Relocate the given node to section.
    Relocate(Node),
    /// Accept of Relocation. (dst, node)
    RelocateAccept(Prefix, Name),
    /// Reject of Relocation. (target, node)
    RelocateReject(Name, Name),
}

#[derive(Debug)]
pub enum Response {
    /// Merge sections.
    Merge(Section, Prefix),
    /// Split section.
    Split(Section, Section, Prefix),
    /// Reject an attempt to join a section.
    Reject(Node),
    /// Request whether a node can be relocated to a prefix matching section. (src, target, node)
    RelocateRequest(Prefix, Name, Name),
    /// Send a request to the section with the given prefix.
    Send(Prefix, Request),
}
