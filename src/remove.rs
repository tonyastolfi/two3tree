use crate::node::Node;

pub enum RemoveResult {
    NotFound,
    Ok,
    Drained,
    Orphaned(Box<Node>),
}
