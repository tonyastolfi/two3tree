use std::ops::{Deref, RangeBounds};
use std::sync::Arc;

use crate::algo::*;
use crate::batch::Batch;
use crate::node::Node;
use crate::sorted_updates::{Itemized, Sorted, SortedUpdates};
use crate::subtree::Subtree;
use crate::update::Update;
use crate::{TreeConfig, K};

use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct Queue<K>(SortedUpdates<K>);

impl<K> Deref for Queue<K> {
    type Target = [Update<K>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> Into<SortedUpdates<K>> for Queue<K> {
    fn into(self) -> SortedUpdates<K> {
        self.0
    }
}

impl<K> Itemized for Queue<K> {
    type Item = Update<K>;
}

impl<K> Sorted for Queue<K> {}

impl<K> Queue<K> {
    pub fn default() -> Self {
        Self(SortedUpdates::default())
    }

    pub fn new<U>(config: &TreeConfig, updates: U) -> Self
    where
        U: Sorted<Item = Update<K>> + Into<SortedUpdates<K>>,
    {
        // When created, a Queue can be at most `B` elements large.
        assert!(updates.len() <= config.batch_size);
        Self(updates.into())
    }

    pub fn with_no_flush(
        config: &TreeConfig,
        updates: SortedUpdates<K>,
        branch: Arc<Node<(K, Subtree<K>)>>,
    ) -> Subtree<K>
    where
        K: Ord,
    {
        use Node::{Binary, Ternary};

        match branch.partition(&updates) {
            Binary(n0, n1) => {
                assert!(n0.len() + n1.len() <= config.batch_size);
            }
            Ternary(n0, n1, n2) => {
                assert!(n0.len() + n1.len() + n2.len() <= config.batch_size * 3 / 2);
                assert!(n0.len() + n1.len() <= config.batch_size);
                assert!(n1.len() + n2.len() <= config.batch_size);
            }
        }

        Subtree::Branch(Self(updates), branch)
    }

    pub fn consume(self) -> SortedUpdates<K> {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = &'a Update<K>> + 'a {
        self.0.iter()
    }

    pub fn merge_iter<'a, OtherIter: Iterator<Item = &'a K> + 'a>(
        &'a self,
        other_iter: OtherIter,
    ) -> impl Iterator<Item = &'a K> + 'a
    where
        K: Ord,
    {
        use itertools::EitherOrBoth::{Both, Left, Right};

        other_iter
            .merge_join_by(self.iter(), |a, b| a.cmp(&b.key()))
            .filter_map(|either| match either {
                Left(from_child) => Some(from_child),
                Right(from_queue) => from_queue.resolve(),
                Both(_, from_queue) => from_queue.resolve(),
            })
    }

    pub fn find<'a>(&'a self, key: &K) -> Option<&'a Update<K>>
    where
        K: Ord + Copy,
    {
        match self.0.binary_search_by_key(key, |msg| *msg.key()) {
            Result::Ok(index) => Some(&self.0[index]),
            Result::Err(_) => None,
        }
    }

    pub fn split_at_key(self, config: &TreeConfig, key: &K) -> (Self, Self)
    where
        K: Ord + Clone + Copy,
    {
        let mut updates = self.consume();
        let index = lower_bound_by_key(&updates, key, |update| *update.key());
        let (left, right) = updates.split_at(index);

        (Self::new(config, left), Self::new(config, right))
    }
}
