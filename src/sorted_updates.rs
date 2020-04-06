use std::ops::Range;
use std::sync::Arc;

use crate::batch::Batch;
use crate::flush::Flush;
use crate::node::Node;
use crate::update::Update;
use crate::{TreeConfig, K};

use itertools::Itertools;

use std::ops::{Deref, RangeBounds};

#[derive(Debug, Clone)]
pub struct SortedUpdates<K>(Arc<[Update<K>]>);

impl<K> SortedUpdates<K> {
    pub fn default() -> Self {
        Self(Arc::new([]))
    }

    pub fn new(mut updates: Vec<Update<K>>) -> Self {
        updates.sort_by_cached_key(|update| *update.key());
        Self(updates.into_boxed_slice().into())
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
                .cloned()
                .collect(),
        )
    }

    pub fn insert(&mut self, v: Update<K>) {
        match self.0.binary_search_by_key(v.key(), |u| *u.key()) {
            Ok(pos) => {
                self.0[pos] = v;
            }
            Err(pos) => {
                let mut tmp_vec: Vec<Update> = (*self.0).into();
                tmp_vec.insert(pos, v);
                self.0 = tmp_vec.into();
            }
        }
    }

    //pub fn split_off(&mut self, index: usize) -> Self {
    //    Self(self.0.split_off(index))
    //}

    pub fn drain<'a, R>(&'a mut self, range: R) -> Self
    where
        R: RangeBounds<usize>,
    {
        Self(self.0.drain(range).collect())
    }
}

impl<K> From<SortedUpdates<K>> for Vec<Update<K>> {
    fn from(sorted: SortedUpdates<K>) -> Self {
        (*sorted.0).into()
    }
}

impl<K> Deref for SortedUpdates<K> {
    type Target = [Update<K>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> Flush<K> for SortedUpdates<K> {
    fn flush(
        &mut self,
        config: &TreeConfig,
        plan: &Node<Option<Range<usize>>>,
    ) -> Node<Option<Batch<K>>> {
        use Node::*;

        let prepare = |updates: SortedUpdates| -> Option<Batch> {
            Some(Batch::new(config, updates).unwrap())
        };

        match plan {
            // no flush
            //
            (_, Binary(None, _, None)) => Binary(None, (), None),
            (_, Ternary(None, _, None, _, None)) => Ternary(None, (), None, (), None),

            // flush left
            //
            (_, Binary(Some(y), _, None)) | (_, Ternary(Some(y), _, None, _, None)) => {
                let new_self: Self = self.split_off(*y);
                let batch = prepare(std::mem::replace(self, new_self));
                match plan {
                    Binary(..) => Binary(batch, (), None),
                    Ternary(..) => Ternary(batch, (), None, (), None),
                }
            }

            // flush right
            //
            (_, Binary(None, _, Some(y))) | (_, Ternary(None, _, None, _, Some(y))) => {
                let batch = prepare(self.split_off(self.len() - y));
                match plan {
                    Binary(..) => Binary(None, (), batch),
                    Ternary(..) => Ternary(None, (), None, (), batch),
                }
            }

            (_, Binary(..)) => panic!("Illegal flush plan for binary node"),

            // flush middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(None, _, Some(y1), _, None)) => {
                let batch = prepare(self.drain(*n0..(n0 + y1)));
                Ternary(None, (), batch, (), None)
            }

            // flush left, middle
            //
            (Ternary(n0, _, n1, _, n2), Ternary(Some(y0), _, Some(y1), _, None)) => {
                let mut flushed = self.drain((n0 - y0)..(n0 + y1));
                let batch1 = prepare(flushed.split_off(*y0));
                let batch0 = prepare(flushed);
                Ternary(batch0, (), batch1, (), None)
            }

            // flush left, right
            //
            (Ternary(n0, _, n1, _, n2), Ternary(Some(y0), _, None, _, Some(y2))) => {
                let new_queue = self.split_off(*y0);
                let batch0 = prepare(std::mem::replace(self, new_queue));
                let batch2 = prepare(self.split_off(self.len() - *y2));
                Ternary(batch0, (), None, (), batch2)
            }

            // flush middle, right
            //
            (Ternary(n0, _, n1, _, n2), Ternary(None, _, Some(y1), _, Some(y2))) => {
                let mut flushed = self.drain(((n0 + n1) - y1)..((n0 + n1) + y2));
                let batch2 = prepare(flushed.split_off(*y1));
                let batch1 = prepare(flushed);
                Ternary(None, (), batch1, (), batch2)
            }

            _ => panic!("partition/plan mismatch"),
        }
    }
}
