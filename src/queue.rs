use crate::algo::*;
use crate::node::Node;
use crate::update::Update;
use crate::{TreeConfig, K};

use itertools::Itertools;

#[derive(Debug)]
pub struct Queue(Vec<Update>);

fn sort_batch(mut batch: Vec<Update>) -> Vec<Update> {
    batch.sort_by_cached_key(|update| *update.key());
    batch
}

impl Queue {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn from_batch(batch: Vec<Update>) -> Self {
        Self(sort_batch(batch))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn consume(self) -> Vec<Update> {
        self.0
    }

    pub fn merge(self, batch: Vec<Update>) -> Self {
        use itertools::EitherOrBoth::{Both, Left, Right};

        let Self(items) = self;
        Self(
            items
                .into_iter()
                .merge_join_by(sort_batch(batch).into_iter(), |a, b| a.key().cmp(b.key()))
                .map(|either| match either {
                    Left(update) => update,
                    Right(update) => update,
                    Both(_, latest) => latest,
                })
                .collect(),
        )
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = &'a Update> + 'a {
        self.0.iter()
    }

    pub fn find<'a>(&'a self, key: &K) -> Option<&'a Update> {
        match self.0.binary_search_by_key(key, |msg| *msg.key()) {
            Result::Ok(index) => Some(&self.0[index]),
            Result::Err(_) => None,
        }
    }
    pub fn partition(&self, pivots: &Node<(), K>) -> Node<usize, K> {
        let Self(ref queue) = self;

        match pivots {
            Node::Binary(_, p1, _) => {
                let len0 = lower_bound_by_key(queue, &p1, |msg| msg.key());
                let len1 = queue.len() - len0;
                Node::Binary(len0, *p1, len1)
            }
            Node::Ternary(_, p1, _, p2, _) => {
                let len0 = lower_bound_by_key(queue, &p1, |msg| msg.key());
                let len1 = lower_bound_by_key(&&queue[len0..], &p2, |msg| msg.key());
                let len2 = queue.len() - (len0 + len1);
                Node::Ternary(len0, *p1, len1, *p2, len2)
            }
        }
    }

    pub fn split(self, m: &i32) -> (Self, Self) {
        let Self(mut queue) = self;
        let ind = lower_bound_by_key(&queue, m, |update| *update.key());
        let split: Vec<Update> = queue.split_off(ind);
        (Self(queue), Self(split))
    }

    fn flush(
        &mut self,
        config: &TreeConfig,
        partition: &Node<usize, K>,
        plan: &Node<usize, ()>,
    ) -> Node<Option<Vec<Update>>, ()> {
        use Node::*;

        let Queue(ref mut queue) = self;

        match (partition, plan) {
            // no flush
            //
            (_, Binary(0, _, 0)) => Binary(None, (), None),
            (_, Ternary(0, _, 0, _, 0)) => Ternary(None, (), None, (), None),

            // flush left
            //
            (_, Binary(y, _, 0)) | (_, Ternary(y, _, 0, _, 0)) if *y != 0 => {
                let new_queue = queue.split_off(*y);
                let Queue(batch) = std::mem::replace(self, Queue(new_queue));
                match plan {
                    Binary(..) => Binary(Some(batch), (), None),
                    Ternary(..) => Ternary(Some(batch), (), None, (), None),
                }
            }

            // flush right
            //
            (_, Binary(0, _, y)) | (_, Ternary(0, _, 0, _, y)) if *y != 0 => {
                let batch = queue.split_off(queue.len() - y);
                match plan {
                    Binary(..) => Binary(None, (), Some(batch)),
                    Ternary(..) => Ternary(None, (), None, (), Some(batch)),
                }
            }

            (_, Binary(..)) => panic!("Illegal flush plan for binary node"),

            // flush middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(0, _, y1, _, 0)) if *y1 != 0 => {
                let batch = queue.drain(*n0..(n0 + y1)).collect();
                Ternary(None, (), Some(batch), (), None)
            }

            // flush left, middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(y0, _, y1, _, 0)) if *y0 != 0 && *y1 != 0 => {
                let mut batch0: Vec<Update> = queue.drain((n0 - y0)..(n0 + y1)).collect();
                let batch1 = batch0.split_off(*y0);
                Ternary(Some(batch0), (), Some(batch1), (), None)
            }

            // flush left, right
            //
            (Ternary(n0, _, n1, _, n2), Ternary(y0, _, 0, _, y2)) if *y0 != 0 && *y2 != 0 => {
                let new_queue = queue.split_off(*y0);
                let batch0 = std::mem::replace(queue, new_queue);
                let batch2 = queue.split_off(queue.len() - *y2);
                Ternary(Some(batch0), (), None, (), Some(batch2))
            }

            // flush middle, right
            //
            (Ternary(n0, _, n1, _, n2), Ternary(0, _, y1, _, y2)) if *y1 != 0 && *y2 != 0 => {
                let mut batch1: Vec<Update> =
                    queue.drain(((n0 + n1) - y1)..((n0 + n1) + y2)).collect();
                let batch2 = batch1.split_off(*y1);
                Ternary(None, (), Some(batch1), (), Some(batch2))
            }

            _ => panic!("partition/plan mismatch"),
        }
    }
}
