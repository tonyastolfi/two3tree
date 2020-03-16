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
