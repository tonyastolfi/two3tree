use std::ops::{Deref, RangeBounds};

use crate::algo::*;
use crate::batch::Batch;
use crate::node::Node;
use crate::partition::partition;
use crate::sorted_updates::SortedUpdates;
use crate::subtree::Subtree;
use crate::update::Update;
use crate::{TreeConfig, K};

use itertools::Itertools;

#[derive(Debug)]
pub struct Queue(SortedUpdates);

impl Deref for Queue {
    type Target = [Update];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Queue {
    pub fn default() -> Self {
        Self(SortedUpdates::default())
    }

    pub fn new(config: &TreeConfig, updates: SortedUpdates) -> Self {
        // When created, a Queue can be at most `B` elements large.
        assert!(updates.len() <= config.batch_size);
        Self(updates)
    }

    pub fn with_no_flush(
        config: &TreeConfig,
        updates: SortedUpdates,
        branch: Box<Node<Subtree, K>>,
    ) -> Subtree {
        use Node::{Binary, Ternary};

        match partition(&updates, &*branch) {
            Binary(n0, _, n1) => {
                assert!(n0 + n1 <= config.batch_size);
            }
            Ternary(n0, _, n1, _, n2) => {
                assert!(n0 + n1 + n2 <= config.batch_size * 3 / 2);
                assert!(n0 + n1 <= config.batch_size);
                assert!(n1 + n2 <= config.batch_size);
            }
        }

        Subtree::Branch(Self(updates), branch)
    }

    pub fn consume(self) -> SortedUpdates {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

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

    pub fn push(&mut self, config: &TreeConfig, v: Update) -> Option<Batch> {
        self.0.insert(v);
        if self.0.len() > config.batch_size {
            let flushed = self.0.split_off(self.0.len() - config.batch_size);
            Some(Batch::new(config, flushed).unwrap())
        } else {
            None
        }
    }

    pub fn split_at_key(self, config: &TreeConfig, key: &K) -> (Self, Self) {
        let mut updates = self.consume();
        let index = lower_bound_by_key(&updates, key, |update| *update.key());
        let q_right = Queue::new(config, updates.split_off(index));
        let q_left = Queue::new(config, updates);

        (q_left, q_right)
    }
}
