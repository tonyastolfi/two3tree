use std::iter::FromIterator;
use std::ops::Range;
use std::sync::Arc;

use smallvec::SmallVec;

use crate::batch::Batch;
use crate::flush::FlushPlan;
use crate::node::Node;
use crate::update::{merge_updates, Update};
use crate::{TreeConfig, K};

use itertools::Itertools;

use std::ops::{Deref, RangeBounds};

pub trait Itemized {
    type Item;
}

pub struct SortedSlice<'a, T>(&'a [T]);

pub trait Sorted: Itemized + Deref<Target = [<Self as Itemized>::Item]> {
    fn sorted_slice<'a>(&'a self, r: Range<usize>) -> SortedSlice<'a, Self::Item> {
        SortedSlice(&self.deref()[r])
    }
}

#[derive(Debug, Clone)]
pub struct SortedUpdates<K>(Arc<[Update<K>]>);

pub struct MergedUpdates<K>(SmallVec<[Update<K>; 1024]>);

impl<K> SortedUpdates<K> {
    pub fn default() -> Self {
        Self(Arc::new([]))
    }

    pub fn new(mut updates: Vec<Update<K>>) -> Self
    where
        K: Ord + Copy,
    {
        updates.sort_by_cached_key(|update| *update.key());
        Self(updates.into_boxed_slice().into())
    }

    pub fn merge<Other>(&self, other: Other) -> MergedUpdates<K>
    where
        K: Ord + Clone,
        Other: Sorted<Item = Update<K>>,
    {
        use itertools::EitherOrBoth::{Both, Left, Right};

        MergedUpdates(
            merge_updates(self.0.iter(), (*other).iter())
                .cloned()
                .collect(),
        )
    }

    pub fn insert(&mut self, v: Update<K>)
    where
        K: Ord + Clone + Copy,
    {
        let mut tmp_vec: Vec<Update<K>> = (*self.0).into();
        match self.0.binary_search_by_key(v.key(), |u| *u.key()) {
            Ok(pos) => {
                tmp_vec[pos] = v;
            }
            Err(pos) => {
                tmp_vec.insert(pos, v);
            }
        }
        self.0 = tmp_vec.into();
    }

    pub fn split_at(&self, off: usize) -> (Self, Self)
    where
        K: Clone,
    {
        (
            Self(self.0[..off].iter().cloned().collect()),
            Self(self.0[off..].iter().cloned().collect()),
        )
    }
}

impl<K> From<MergedUpdates<K>> for SortedUpdates<K>
where
    K: Clone,
{
    fn from(other: MergedUpdates<K>) -> Self {
        Self(other.into_iter().cloned().collect())
    }
}

impl<'a, K> From<SortedSlice<'a, Update<K>>> for SortedUpdates<K>
where
    K: Clone,
{
    fn from(other: SortedSlice<'a, Update<K>>) -> Self {
        Self(other.into_iter().cloned().collect())
    }
}

impl<K> From<SortedUpdates<K>> for Vec<Update<K>>
where
    K: Clone,
{
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

impl<K> FromIterator<Update<K>> for SortedUpdates<K> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Update<K>>,
    {
        Self(iter.into_iter().collect())
    }
}

impl<K> Itemized for SortedUpdates<K> {
    type Item = Update<K>;
}
impl<K> Sorted for SortedUpdates<K> {}

impl<K> Deref for MergedUpdates<K> {
    type Target = [Update<K>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> Itemized for MergedUpdates<K> {
    type Item = Update<K>;
}
impl<K> Sorted for MergedUpdates<K> {}

impl<'a, T> Deref for SortedSlice<'a, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}
impl<'a, T> Itemized for SortedSlice<'a, T> {
    type Item = T;
}
impl<'a, T> Sorted for SortedSlice<'a, T> {}
