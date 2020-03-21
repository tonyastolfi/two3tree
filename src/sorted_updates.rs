use crate::node::Node;
use crate::update::Update;

use std::ops::{Deref, RangeBounds};

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

impl Flush for SortedUpdates {
    pub fn flush(
        &mut self,
        config: &TreeConfig,
        partition: &Node<usize, K>,
        plan: &Node<Option<usize>, ()>,
    ) -> Node<Option<Batch>, ()> {
        use Node::*;

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
                let new_queue = self.split_off(*y);
                let Queue(batch) = std::mem::replace(self, Queue(new_queue));
                match plan {
                    Binary(..) => Binary(prepare(batch), (), None),
                    Ternary(..) => Ternary(prepare(batch), (), None, (), None),
                }
            }

            // flush right
            //
            (_, Binary(None, _, Some(y))) | (_, Ternary(None, _, None, _, Some(y))) => {
                let batch = self.split_off(self.len() - y);
                match plan {
                    Binary(..) => Binary(None, (), prepare(batch)),
                    Ternary(..) => Ternary(None, (), None, (), prepare(batch)),
                }
            }

            (_, Binary(..)) => panic!("Illegal flush plan for binary node"),

            // flush middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(None, _, Some(y1), _, None)) => {
                let batch = self.drain(*n0..(n0 + y1)).collect();
                Ternary(None, (), prepare(batch), (), None)
            }

            // flush left, middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(Some(y0), _, Some(y1), _, None)) => {
                let mut batch0: Vec<Update> = self.drain((n0 - y0)..(n0 + y1)).collect();
                let batch1 = batch0.split_off(*y0);
                Ternary(prepare(batch0), (), prepare(batch1), (), None)
            }

            // flush left, right
            //
            (Ternary(n0, _, n1, _, n2), Ternary(Some(y0), _, None, _, Some(y2))) => {
                let new_queue = self.split_off(*y0);
                let batch0 = std::mem::replace(self, new_queue);
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
