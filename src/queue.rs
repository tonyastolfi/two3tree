use std::ops::{Deref, RangeBounds};

use crate::algo::*;
use crate::node::Node;
use crate::update::Update;
use crate::{Subtree, TreeConfig, K};

use itertools::Itertools;

pub struct Queue(SortedUpdates);

#[derive(Debug, Clone)]
pub struct Batch(SortedUpdates);

#[derive(Debug, Clone)]
pub struct SortedUpdates(Vec<Update>);

impl SortedUpdates {
    pub fn new(mut updates: Vec<Update>) -> Self {
        updates.sort_by_cached_key(|update| *update.key());
        Self(updates)
    }

    pub fn merge(self, other: Self) -> Self {
        use itertools::EitherOrBoth::{Both, Left, Right};

        Self(
            self.0
                .into_iter()
                .merge_join_by(other.0.into_iter(), |a, b| a.key().cmp(b.key()))
                .map(|either| match either {
                    Left(update) => update,
                    Right(update) => update,
                    Both(_, latest) => latest,
                })
                .collect(),
        )
    }

    pub fn split_off(&mut self, index: usize) -> Self {
        Self(self.0.split_off(index))
    }

    pub fn drain<'a, R>(&'a mut self, range: R) -> impl Iterator<Item = Update> + 'a
    where
        R: RangeBounds<usize>,
    {
        self.0.drain(range)
    }
}

impl From<SortedUpdates> for Vec<Update> {
    fn from(sorted: SortedUpdates) -> Self {
        sorted.0
    }
}

impl Deref for SortedUpdates {
    type Target = [Update];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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

impl Batch {
    pub fn new(config: &TreeConfig, updates: SortedUpdates) -> Result<Self, SortedUpdates> {
        if config.batch_size / 2 <= updates.len() && updates.len() <= config.batch_size {
            Ok(Self(updates))
        } else {
            Err(updates)
        }
    }

    pub fn consume(self) -> SortedUpdates {
        self.0
    }
}

pub struct BatchSize<'a>(&'a TreeConfig, usize);

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

    pub fn flush(
        &mut self,
        config: &TreeConfig,
        partition: &Node<usize, K>,
        plan: &Node<Option<usize>, ()>,
    ) -> Node<Option<Batch>, ()> {
        use Node::*;

        let Queue(ref mut sorted_updates) = self;

        let prepare =
            |updates: Vec<Update>| -> Option<Batch> { Some(Batch::new(config, updates).unwrap()) };

        match (partition, plan) {
            // no flush
            //
            (_, Binary(None, _, None)) => Binary(None, (), None),
            (_, Ternary(None, _, None, _, None)) => Ternary(None, (), None, (), None),

            // flush left
            //
            (_, Binary(Some(y), _, None)) | (_, Ternary(Some(y), _, None, _, None)) => {
                let new_queue = sorted_updates.split_off(*y);
                let Queue(batch) = std::mem::replace(self, Queue(new_queue));
                match plan {
                    Binary(..) => Binary(prepare(batch), (), None),
                    Ternary(..) => Ternary(prepare(batch), (), None, (), None),
                }
            }

            // flush right
            //
            (_, Binary(None, _, Some(y))) | (_, Ternary(None, _, None, _, Some(y))) => {
                let batch = sorted_updates.split_off(sorted_updates.len() - y);
                match plan {
                    Binary(..) => Binary(None, (), prepare(batch)),
                    Ternary(..) => Ternary(None, (), None, (), prepare(batch)),
                }
            }

            (_, Binary(..)) => panic!("Illegal flush plan for binary node"),

            // flush middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(None, _, Some(y1), _, None)) => {
                let batch = sorted_updates.drain(*n0..(n0 + y1)).collect();
                Ternary(None, (), prepare(batch), (), None)
            }

            // flush left, middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(Some(y0), _, Some(y1), _, None)) => {
                let mut batch0: Vec<Update> = sorted_updates.drain((n0 - y0)..(n0 + y1)).collect();
                let batch1 = batch0.split_off(*y0);
                Ternary(prepare(batch0), (), prepare(batch1), (), None)
            }

            // flush left, right
            //
            (Ternary(n0, _, n1, _, n2), Ternary(Some(y0), _, None, _, Some(y2))) => {
                let new_queue = sorted_updates.split_off(*y0);
                let batch0 = std::mem::replace(sorted_updates, new_queue);
                let batch2 = sorted_updates.split_off(sorted_updates.len() - *y2);
                Ternary(prepare(batch0), (), None, (), prepare(batch2))
            }

            // flush middle, right
            //
            (Ternary(n0, _, n1, _, n2), Ternary(None, _, Some(y1), _, Some(y2))) => {
                let mut batch1: Vec<Update> = sorted_updates
                    .drain(((n0 + n1) - y1)..((n0 + n1) + y2))
                    .collect();
                let batch2 = batch1.split_off(*y1);
                Ternary(None, (), prepare(batch1), (), prepare(batch2))
            }

            _ => panic!("partition/plan mismatch"),
        }
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
