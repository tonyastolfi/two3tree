use std::ops::{Deref, Range};

use crate::algo::lower_bound_by_key;
//use crate::subtree::Subtree;
use crate::update::Update;
use crate::K;

#[derive(Debug)]
pub enum Node<T> {
    Binary(T, T),
    Ternary(T, T, T),
}

impl<K, S> Node<(K, S)> {
    pub fn pivots(&self) -> Node<K>
    where
        K: Copy,
    {
        use Node::*;

        match self {
            Binary((p0, _), (p1, _)) => Binary(*p0, *p1),
            Ternary((p0, _), (p1, _), (p2, _)) => Ternary(*p0, *p1, *p2),
        }
    }

    pub fn partition<T>(&self, items: &T) -> Node<Range<usize>>
    where
        K: Ord,
        T: Deref<Target = [Update<K>]>,
    {
        use Node::*;

        match self {
            Binary(_, (p1, _)) => {
                let i = lower_bound_by_key(items, &p1, |msg| msg.key());
                Binary(0..i, i..items.len())
            }
            Ternary(_, (p1, _), (p2, _)) => {
                let i = lower_bound_by_key(items, &p1, |msg| msg.key());
                let j = i + lower_bound_by_key(&&items[i..], &p2, |msg| msg.key());
                Ternary(0..i, i..j, j..items.len())
            }
        }
    }
}

impl<T> Node<T> {
    pub fn as_ref<'a>(&'a self) -> Node<&'a T> {
        use Node::*;

        match self {
            Binary(a, b) => Binary(&a, &b),
            Ternary(a, b, c) => Ternary(&a, &b, &c),
        }
    }

    pub fn map<F, U>(self, f: F) -> Node<U>
    where
        F: Fn(T) -> U,
    {
        use Node::*;

        match self {
            Binary(a, b) => Binary(f(a), f(b)),
            Ternary(a, b, c) => Ternary(f(a), f(b), f(c)),
        }
    }
}

/*
impl Node<(K, Subtree<K>)> {
    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a> {
        match self {
            Node::Binary((_, b0), (_, b1)) => Box::new(b0.iter().chain(b1.iter())),
            Node::Ternary((_, b0), (_, b1), (_, b2)) => {
                Box::new(b0.iter().chain(b1.iter()).chain(b2.iter()))
            }
        }
    }
}
*/
