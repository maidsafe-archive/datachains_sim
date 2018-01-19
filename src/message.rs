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
}

#[derive(Debug)]
pub enum Response {
    /// Add new section.
    Add(Section),
    /// Remove section with the given prefix.
    Remove(Prefix),
    /// Reject an attempt to join a section.
    Reject(Node),
    /// Relocate the given node to a section with matching prefix.
    Relocate(Node),
    /// Send a request to the section with the given prefix.
    Send(Prefix, Request),
    /// Fails the simulation due to a section having too many nodes.
    Fail(Prefix),
}
