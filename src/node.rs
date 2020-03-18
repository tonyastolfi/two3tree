use crate::Child;
use crate::K;

#[derive(Debug)]
pub enum Node<B, P> {
    Binary(B, P, B),
    Ternary(B, P, B, P, B),
}

impl<B, P: Copy> Node<B, P> {
    pub fn pivots(&self) -> Node<(), P> {
        use Node::*;

        match self {
            Binary(_, p0, _) => Binary((), *p0, ()),
            Ternary(_, p0, _, p1, _) => Ternary((), *p0, (), *p1, ()),
        }
    }
}

impl Node<Child, K> {
    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a> {
        match self {
            Node::Binary(b0, m1, b1) => Box::new(b0.iter().chain(b1.iter())),
            Node::Ternary(b0, m1, b1, m2, b2) => {
                Box::new(b0.iter().chain(b1.iter()).chain(b2.iter()))
            }
        }
    }
}
