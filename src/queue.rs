use std::ops::{Deref, RangeBounds};

use crate::algo::*;
use crate::sorted_updates::SortedUpdates;
use crate::update::Update;
use crate::{Subtree, TreeConfig, K};

use itertools::Itertools;

#[derive(Debug)]
pub struct Queue(SortedUpdates);

impl<'a> From<(&'a TreeConfig, SortedUpdates)> for Queue {
    fn from((config, updates): (&'a TreeConfig, SortedUpdates)) -> Self {
        // When created, a Queue can be at most `B` elements large.
        assert!(updates.len() <= config.batch_size);
        Self(updates.into())
    }
}

impl Queue {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn push(self, u: Update) -> Self {
        self.merge_no_order_check(vec![u])
    }

    pub fn update(self, batch: Batch) -> Self {
        self.merge_no_order_check(batch.consume())
    }

    pub fn merge(self, other: Queue) -> Self {
        self.merge_no_order_check(other.consume())
    }

    fn merge_no_order_check(self, updates_sorted_by_key: Vec<Update>) -> Self {}

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = &'a Update> + 'a {
        self.0.iter()
    }

    pub fn merge_iter<'a, OtherIter: Iterator<Item = &'a K> + 'a>(
        &'a self,
        other_iter: OtherIter,
    ) -> impl Iterator<Item = &'a K> + 'a {
        use itertools::EitherOrBoth::{Both, Left, Right};

        other_iter
            .merge_join_by(self.iter(), |a, b| a.cmp(&b.key()))
            .filter_map(|either| match either {
                Left(from_child) => Some(from_child),
                Right(from_queue) => from_queue.resolve(),
                Both(_, from_queue) => from_queue.resolve(),
            })
    }

    pub fn find<'a>(&'a self, key: &K) -> Option<&'a Update> {
        match self.0.binary_search_by_key(key, |msg| *msg.key()) {
            Result::Ok(index) => Some(&self.0[index]),
            Result::Err(_) => None,
        }
    }

    pub fn split_at_key(self, key: &K) -> (Self, Self) {
        let Self(updates) = self;
        let index = lower_bound_by_key(&updates, key, |update| *update.key());
        Self::split_off(updates, index)
    }

    pub fn split_in_half(self) -> (Self, Self) {
        let Self(mut updates) = self;
        Self::split_off(updates, updates.len() / 2)
    }

    fn split_off(mut before: Vec<Update>, index: usize) -> (Self, Self) {
        let after: Vec<Update> = before.split_off(index);
        (Self(before), Self(after))
    }
}

pub fn plan_flush<T>(config: &TreeConfig, partition: &Node<usize, T>) -> Node<Option<usize>, ()> {
    let take_batch = |n: usize| {
        if n < config.batch_size / 2 {
            None
        } else if n > config.batch_size {
            Some(config.batch_size)
        } else {
            Some(n)
        }
    };

    match partition {
        Node::Binary(n0, _, n1) => {
            assert!(n0 + n1 <= 2 * config.batch_size);

            if n0 + n1 <= config.batch_size {
                Node::Binary(None, (), None)
            } else {
                if n0 >= n1 {
                    Node::Binary(take_batch(*n0), (), None)
                } else {
                    Node::Binary(None, (), take_batch(*n1))
                }
            }
        }
        Node::Ternary(n0, _, n1, _, n2) => {
            let total = n0 + n1 + n2;

            if total <= config.batch_size {
                Node::Ternary(None, (), None, (), None)
            } else {
                match (take_batch(*n0), take_batch(*n1), take_batch(*n2)) {
                    (Some(y0), Some(y1), Some(y2)) => {
                        if y0 <= y1 && y0 <= y2 {
                            Node::Ternary(None, (), Some(y1), (), Some(y2))
                        } else if y1 <= y0 && y1 <= y2 {
                            Node::Ternary(Some(y0), (), None, (), Some(y2))
                        } else {
                            Node::Ternary(Some(y0), (), Some(y1), (), None)
                        }
                    }
                    (b0, b1, b2) => Node::Ternary(b0, (), b1, (), b2),
                }
            }
        }
    }
}
