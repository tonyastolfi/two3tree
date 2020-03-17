use crate::algo::*;
use crate::node::Node;
use crate::update::Update;
use crate::{TreeConfig, K};

use itertools::Itertools;

#[derive(Debug)]
pub struct Queue(pub Vec<Update>);

pub fn sort_batch(mut batch: Vec<Update>) -> Vec<Update> {
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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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
    pub fn partition<U>(&self, pivots: &Node<U, K>) -> Node<usize, K> {
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

    pub fn flush(
        &mut self,
        partition: &Node<usize, K>,
        plan: &Node<Option<usize>, ()>,
    ) -> Node<Option<Vec<Update>>, ()> {
        use Node::*;

        let Queue(ref mut queue) = self;

        match (partition, plan) {
            // no flush
            //
            (_, Binary(None, _, None)) => Binary(None, (), None),
            (_, Ternary(None, _, None, _, None)) => Ternary(None, (), None, (), None),

            // flush left
            //
            (_, Binary(Some(y), _, None)) | (_, Ternary(Some(y), _, None, _, None)) => {
                let new_queue = queue.split_off(*y);
                let Queue(batch) = std::mem::replace(self, Queue(new_queue));
                match plan {
                    Binary(..) => Binary(Some(batch), (), None),
                    Ternary(..) => Ternary(Some(batch), (), None, (), None),
                }
            }

            // flush right
            //
            (_, Binary(None, _, Some(y))) | (_, Ternary(None, _, None, _, Some(y))) => {
                let batch = queue.split_off(queue.len() - y);
                match plan {
                    Binary(..) => Binary(None, (), Some(batch)),
                    Ternary(..) => Ternary(None, (), None, (), Some(batch)),
                }
            }

            (_, Binary(..)) => panic!("Illegal flush plan for binary node"),

            // flush middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(None, _, Some(y1), _, None)) => {
                let batch = queue.drain(*n0..(n0 + y1)).collect();
                Ternary(None, (), Some(batch), (), None)
            }

            // flush left, middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(Some(y0), _, Some(y1), _, None)) => {
                let mut batch0: Vec<Update> = queue.drain((n0 - y0)..(n0 + y1)).collect();
                let batch1 = batch0.split_off(*y0);
                Ternary(Some(batch0), (), Some(batch1), (), None)
            }

            // flush left, right
            //
            (Ternary(n0, _, n1, _, n2), Ternary(Some(y0), _, None, _, Some(y2))) => {
                let new_queue = queue.split_off(*y0);
                let batch0 = std::mem::replace(queue, new_queue);
                let batch2 = queue.split_off(queue.len() - *y2);
                Ternary(Some(batch0), (), None, (), Some(batch2))
            }

            // flush middle, right
            //
            (Ternary(n0, _, n1, _, n2), Ternary(None, _, Some(y1), _, Some(y2))) => {
                let mut batch1: Vec<Update> =
                    queue.drain(((n0 + n1) - y1)..((n0 + n1) + y2)).collect();
                let batch2 = batch1.split_off(*y1);
                Ternary(None, (), Some(batch1), (), Some(batch2))
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
