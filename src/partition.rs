use crate::algo::lower_bound_by_key;
use crate::node::Node;
use crate::update::Update;
use crate::K;

use std::ops::Deref;

pub fn partition<T, _Ignored>(items: &T, pivots: &Node<_Ignored, K>) -> Node<usize, K>
where
    T: Deref<Target = [Update]>,
{
    match pivots {
        Node::Binary(_, p1, _) => {
            let len0 = lower_bound_by_key(items, &p1, |msg| msg.key());
            let len1 = items.len() - len0;
            Node::Binary(len0, *p1, len1)
        }
        Node::Ternary(_, p1, _, p2, _) => {
            let len0 = lower_bound_by_key(items, &p1, |msg| msg.key());
            let len1 = lower_bound_by_key(&&items[len0..], &p2, |msg| msg.key());
            let len2 = items.len() - (len0 + len1);
            Node::Ternary(len0, *p1, len1, *p2, len2)
        }
    }
}
