use crate::node::Node;
use crate::update::Update;
use crate::K;

trait Partition {
    fn partition<_Ignored>(&self, pivots: &Node<_Ignored, K>) -> Node<usize, K>;
}

impl<T> Partition for T
where
    T: Deref<Target = [Update]>,
{
    fn partition<_Ignored>(&self, pivots: &Node<_Ignored, K>) -> Node<usize, K> {
        match pivots {
            Node::Binary(_, p1, _) => {
                let len0 = lower_bound_by_key(self, &p1, |msg| msg.key());
                let len1 = self.len() - len0;
                Node::Binary(len0, *p1, len1)
            }
            Node::Ternary(_, p1, _, p2, _) => {
                let len0 = lower_bound_by_key(self, &p1, |msg| msg.key());
                let len1 = lower_bound_by_key(&self[len0..], &p2, |msg| msg.key());
                let len2 = self.len() - (len0 + len1);
                Node::Ternary(len0, *p1, len1, *p2, len2)
            }
        }
    }
}
