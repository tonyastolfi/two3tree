use std::iter::FromIterator;
use std::ops::{Deref, Range};

use smallvec::SmallVec;

use crate::algo::lower_bound_by_key;
use crate::batch::Batch;
use crate::flush::FlushPlan;
use crate::sorted_updates::{Sorted, SortedSlice};
use crate::subtree::Subtree;
use crate::update::Update;
use crate::TreeConfig;

#[derive(Debug, Clone)]
pub enum Node<T> {
    Binary(T, T),
    Ternary(T, T, T),
}

macro_rules! make_node {
    ($b0:expr, $b1:expr) => {
        crate::node::Node::Binary($b0.clone(), $b1.clone())
    };
    ($b0:expr, $b1:expr, $b2:expr) => {
        crate::node::Node::Ternary($b0.clone(), $b1.clone(), $b2.clone())
    };
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

    pub fn partition<Q>(&self, items: &Q) -> Node<Range<usize>>
    where
        K: Ord,
        Q: Sorted<Item = Update<K>>,
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

    pub fn flush<'a, Q, R>(
        &self,
        config: &TreeConfig,
        items: &'a Q,
    ) -> (Node<Option<Batch<SortedSlice<'a, Update<K>>>>>, R)
    where
        K: Ord + Clone,
        Q: Sorted<Item = Update<K>>,
        R: FromIterator<Update<K>>,
    {
        use Node::*;

        let plan = self.partition(items).plan_flush(config);

        if plan.all(|br| br.flush.is_none()) {
            return (plan.as_ref().map(|_| None), items.iter().cloned().collect());
        }

        let to_flush: Node<Option<Batch<SortedSlice<'a, Update<K>>>>> = plan.as_ref().map(|br| {
            br.flush
                .clone()
                .map(|r| Batch::new(config, items.sorted_slice(r)))
        });

        (
            to_flush,
            plan.as_ref() // => Node<&FlushPlan>
                .as_seq() // => SmallVec<&FlushPlan>
                .into_iter() // => Iterator<Item = &FlushPlan>
                .filter_map(|br| br.keep.clone()) // => Iterator<Item = Range<usize>>
                .flat_map(|r| &items[r]) // => Iterator<Item = &Update<K>>
                .cloned() // => Iterator<Item = Update<K>>
                .collect(),
        )
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

    pub fn all<F>(&self, f: F) -> bool
    where
        F: Fn(&T) -> bool,
    {
        use Node::*;

        match self {
            Binary(a, b) => f(a) && f(b),
            Ternary(a, b, c) => f(a) && f(b) && f(c),
        }
    }

    pub fn as_seq(self) -> impl IntoIterator<Item = T> {
        use Node::*;

        let sv: SmallVec<[T; 3]> = match self {
            Binary(a, b) => smallvec!(a, b),
            Ternary(a, b, c) => smallvec!(a, b, c),
        };
        sv
    }
}

impl Node<Range<usize>> {
    pub fn plan_flush(&self, config: &TreeConfig) -> Node<FlushPlan> {
        use Node::*;

        match self {
            Binary(a, b) => {
                assert!(a.len() + b.len() <= 2 * config.batch_size);

                if a.len() + b.len() <= config.batch_size {
                    Binary(FlushPlan::none(a), FlushPlan::none(b))
                } else {
                    if a.len() >= b.len() {
                        Binary(FlushPlan::clip(config, a), FlushPlan::none(b))
                    } else {
                        Binary(FlushPlan::none(a), FlushPlan::clip(config, b))
                    }
                }
            }
            Ternary(a, b, c) => {
                let total = a.len() + b.len() + c.len();

                if total <= config.batch_size {
                    Ternary(FlushPlan::none(a), FlushPlan::none(b), FlushPlan::none(c))
                } else {
                    if a.len() <= b.len() && a.len() <= c.len() {
                        // min == a
                        Ternary(
                            FlushPlan::none(a),
                            FlushPlan::clip(config, b),
                            FlushPlan::clip(config, c),
                        )
                    } else if b.len() <= a.len() && b.len() <= c.len() {
                        // min == b
                        Ternary(
                            FlushPlan::clip(config, a),
                            FlushPlan::none(b),
                            FlushPlan::clip(config, c),
                        )
                    } else {
                        // min == c
                        Ternary(
                            FlushPlan::clip(config, a),
                            FlushPlan::clip(config, b),
                            FlushPlan::none(c),
                        )
                    }
                }
            }
        }
    }
}

impl<K> Node<(K, Subtree<K>)>
where
    K: Ord,
{
    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a> {
        match self {
            Node::Binary((_, b0), (_, b1)) => Box::new(b0.iter().chain(b1.iter())),
            Node::Ternary((_, b0), (_, b1), (_, b2)) => {
                Box::new(b0.iter().chain(b1.iter()).chain(b2.iter()))
            }
        }
    }
}
