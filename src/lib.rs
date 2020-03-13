#![allow(dead_code)]
#![allow(unused_macros)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use itertools::Itertools;

pub type K = i32;

#[derive(Debug)]
pub struct TreeConfig {
    pub batch_size: usize,
}

pub trait Done {
    fn done(_: Subtree) -> Self;
}

pub trait Split {
    fn split(_: Subtree, _: i32, _: Subtree) -> Self;
}

pub trait Merge {
    fn merge(_: Orphan) -> Self;
}

pub trait Height {
    fn height(&self) -> u16;
}

pub trait Viable {
    fn is_viable(&self, config: &TreeConfig) -> bool;
}

fn lower_bound_by_key<'a, B, F, T: 'a, V: std::ops::Deref<Target = [T]>>(
    v: &'a V,
    b: &B,
    f: F,
) -> usize
where
    B: Ord,
    F: FnMut(&'a T) -> B,
{
    match v.binary_search_by_key(b, f) {
        Result::Ok(i) => i,
        Result::Err(i) => i,
    }
}

fn upper_bound_by_key<'a, B, F, T: 'a, V: std::ops::Deref<Target = [T]>>(
    v: &'a V,
    b: &B,
    mut f: F,
) -> usize
where
    B: Ord + Eq,
    F: FnMut(&'a T) -> B,
{
    match v.binary_search_by_key(b, |x| f(x)) {
        Result::Ok(mut i) => {
            while i < v.len() && &f(&v[i]) == b {
                i += 1
            }
            i
        }
        Result::Err(i) => i,
    }
}

#[derive(Debug)]
pub enum Node {
    Binary {
        height: u16,
        left: Subtree,
        right_min: i32,
        right: Subtree,
    },
    Ternary {
        height: u16,
        left: Subtree,
        middle_min: i32,
        middle: Subtree,
        right_min: i32,
        right: Subtree,
    },
}

impl Height for Node {
    fn height(&self) -> u16 {
        match self {
            Node::Binary { height, .. } => *height,
            Node::Ternary { height, .. } => *height,
        }
    }
}

impl Viable for Node {
    fn is_viable(&self, _: &TreeConfig) -> bool {
        true
    }
}

#[derive(Debug)]
pub enum Partition<T, P: Ord> {
    Part2(T, P, T),
    Part3(T, P, T, P, T),
}

impl<T, P: Ord> Partition<T, P> {
    fn left<'a>(&'a self) -> &'a T {
        use Partition::*;

        match self {
            Part2(left, ..) => left,
            Part3(left, ..) => left,
        }
    }
    fn middle<'a>(&'a self) -> Option<&'a T> {
        use Partition::*;

        match self {
            Part2(..) => None,
            Part3(_, _, middle, ..) => Some(middle),
        }
    }
    fn right<'a>(&'a self) -> &'a T {
        use Partition::*;

        match self {
            Part2(_, _, right) => right,
            Part3(_, _, _, _, right) => right,
        }
    }
}

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
    pub fn partition(&self, part: Partition<(), K>) -> Partition<usize, K> {
        use Partition::{Part2, Part3};

        let Self(ref queue) = self;

        match part {
            Part2(_, p1, _) => {
                let len0 = lower_bound_by_key(queue, &p1, |msg| *msg.key());
                let len1 = queue.len() - len0;
                Part2(len0, p1, len1)
            }
            Part3(_, p1, _, p2, _) => {
                let len0 = lower_bound_by_key(queue, &p1, |msg| *msg.key());
                let len1 = lower_bound_by_key(&&queue[len0..], &p2, |msg| *msg.key());
                let len2 = queue.len() - (len0 + len1);
                Part3(len0, p1, len1, p2, len2)
            }
        }
    }
    pub fn split(self, m: &i32) -> (Self, Self) {
        let Self(mut queue) = self;
        let ind = lower_bound_by_key(&queue, m, |update| *update.key());
        let split: Vec<Update> = queue.split_off(ind);
        (Self(queue), Self(split))
    }
    pub fn len(&self) -> usize {
        let Self(ref queue) = self;
        queue.len()
    }
    fn flush(
        &mut self,
        config: &TreeConfig,
        partition: &Partition<usize, K>,
        plan: &FlushPlan<usize>,
    ) -> FlushPlan<Vec<Update>> {
        use FlushPlan::*;
        use Partition::{Part2, Part3};

        let Queue(ref mut queue) = self;

        match (partition, plan) {
            (_, NoFlush) => NoFlush,
            (_, FlushLeft(prefix_len)) => {
                let new_queue = queue.split_off(*prefix_len);
                let Queue(batch) = std::mem::replace(self, Queue(new_queue));
                FlushLeft(batch)
            }
            (_, FlushRight(suffix_len)) => {
                let batch = queue.split_off(queue.len() - suffix_len);
                FlushRight(batch)
            }
            (Part3(len0, _, len1, _, len2), FlushMiddle(flush1)) => {
                assert!(*flush1 >= config.batch_size / 2);
                assert!(*flush1 <= config.batch_size);

                let batch = queue.drain(*len0..(len0 + flush1)).collect();
                FlushMiddle(batch)
            }
            (Part3(len0, _, len1, _, len2), FlushLeftMiddle(flush0, flush1)) => {
                assert!(*flush0 >= config.batch_size / 2);
                assert!(*flush0 <= config.batch_size);
                assert!(*flush1 >= config.batch_size / 2);
                assert!(*flush1 <= config.batch_size);

                let pivot = len0;
                let mut left_batch: Vec<Update> =
                    queue.drain((pivot - flush0)..(pivot + flush1)).collect();
                let middle_batch = left_batch.split_off(*flush0);

                FlushLeftMiddle(left_batch, middle_batch)
            }
            (Part3(len0, _, len1, _, len2), FlushLeftRight(flush0, flush2)) => {
                if let FlushLeft(left_batch) = self.flush(config, partition, &FlushLeft(*flush0)) {
                    if let FlushRight(right_batch) =
                        self.flush(config, partition, &FlushRight(*flush2))
                    {
                        return FlushLeftRight(left_batch, right_batch);
                    }
                }
                panic!("unreachable case!");
            }
            (Part3(len0, _, len1, _, len2), FlushMiddleRight(flush1, flush2)) => {
                assert!(*flush1 >= config.batch_size / 2);
                assert!(*flush1 <= config.batch_size);
                assert!(*flush2 >= config.batch_size / 2);
                assert!(*flush2 <= config.batch_size);

                let pivot = len0 + len1;
                let mut middle_batch: Vec<Update> =
                    queue.drain((pivot - flush1)..(pivot + flush2)).collect();
                let right_batch = middle_batch.split_off(*flush1);

                FlushMiddleRight(middle_batch, right_batch)
            }
            _ => panic!("partition/plan mismatch"),
        }
    }
}

#[derive(Debug)]
enum FlushPlan<T> {
    NoFlush,
    FlushLeft(T),
    FlushRight(T),
    FlushMiddle(T),
    FlushLeftMiddle(T, T),
    FlushLeftRight(T, T),
    FlushMiddleRight(T, T),
}

impl FlushPlan<usize> {
    fn new(config: &TreeConfig, part: &Partition<usize, K>) -> Self {
        use FlushPlan::*;
        use Partition::{Part2, Part3};

        let take_batch = |n: usize| {
            if n < config.batch_size / 2 {
                0
            } else if n > config.batch_size {
                config.batch_size
            } else {
                n
            }
        };

        match part {
            Part2(len0, _, len1) => {
                assert!(len0 + len1 <= 2 * config.batch_size);

                if len0 + len1 <= config.batch_size {
                    NoFlush
                } else {
                    if len0 >= len1 {
                        FlushLeft(take_batch(*len0))
                    } else {
                        FlushRight(take_batch(*len1))
                    }
                }
            }
            Part3(len0, _, len1, _, len2) => {
                let total = len0 + len1 + len2;
                let (pair0, pair1) = (len0 + len1, len1 + len2);

                //                assert!(total <= config.batch_size * 2);
                assert!(*len0 <= config.batch_size * 2);
                assert!(*len1 <= config.batch_size * 2);
                assert!(*len2 <= config.batch_size * 2);

                let good3 = |new_total, new_pair0, new_pair1| -> bool {
                    new_total <= config.batch_size * 2
                        && new_pair0 <= config.batch_size
                        && new_pair1 <= config.batch_size
                };
                let good2 = |new_total, new_pair0, new_pair1| -> bool {
                    new_total <= config.batch_size
                        && new_pair0 <= config.batch_size
                        && new_pair1 <= config.batch_size
                };

                let (batch0, batch1, batch2) =
                    (take_batch(*len0), take_batch(*len1), take_batch(*len2));

                if good3(total, pair0, pair1) {
                    if batch0 != 0 {
                        if batch1 != 0 {
                            FlushLeftMiddle(batch0, batch1)
                        } else if batch2 != 0 {
                            FlushLeftRight(batch0, batch2)
                        } else {
                            FlushLeft(batch0)
                        }
                    } else if batch1 != 0 {
                        if batch2 != 0 {
                            FlushMiddleRight(batch1, batch2)
                        } else {
                            FlushMiddle(batch1)
                        }
                    } else if batch2 != 0 {
                        FlushRight(batch2)
                    } else {
                        NoFlush
                    }
                } else if good2(total - batch1, pair0 - batch1, pair1 - batch1) {
                    FlushMiddle(batch1)
                } else if good2(total - batch0, pair0 - batch0, pair1) {
                    FlushLeft(batch0)
                } else if good2(total - batch2, pair0, pair1 - batch2) {
                    FlushRight(batch2)
                } else if good2(
                    total - (batch0 + batch1),
                    pair0 - (batch0 + batch1),
                    pair1 - batch1,
                ) {
                    FlushLeftMiddle(batch0, batch1)
                } else if good2(
                    total - (batch1 + batch2),
                    pair0 - batch1,
                    pair1 - (batch1 + batch2),
                ) {
                    FlushMiddleRight(batch1, batch2)
                } else if good2(total - (batch0 + batch2), pair0 - batch0, pair1 - batch2) {
                    FlushLeftRight(batch0, batch2)
                } else {
                    panic!("no good plans!  {:?}, config={:?}", part, config);
                }
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Update {
    Put(i32),
    Delete(i32),
}

impl Node {
    fn is_binary(&self) -> bool {
        match self {
            Node::Binary { .. } => true,
            _ => false,
        }
    }

    fn is_ternary(&self) -> bool {
        match self {
            Node::Ternary { .. } => true,
            _ => false,
        }
    }

    fn binary(b0: Subtree, m1: i32, b1: Subtree) -> Self {
        assert_eq!(b0.height(), b1.height());
        Node::Binary {
            height: b0.height() + 1,
            left: b0,
            right_min: m1,
            right: b1,
        }
    }

    fn ternary(b0: Subtree, m1: i32, b1: Subtree, m2: i32, b2: Subtree) -> Self {
        assert_eq!(b0.height(), b1.height());
        assert_eq!(b1.height(), b2.height());
        assert!(m1 < m2, "m1={}, m2={}", m1, m2);
        Node::Ternary {
            height: b0.height() + 1,
            left: b0,
            middle_min: m1,
            middle: b1,
            right_min: m2,
            right: b2,
        }
    }

    fn partition(&self) -> Partition<(), K> {
        use Partition::{Part2, Part3};

        match self {
            Node::Binary {
                height: _,
                left: _,
                right_min,
                right: _,
            } => Part2((), *right_min, ()),

            Node::Ternary {
                height: _,
                left: _,
                middle_min,
                middle: _,
                right_min,
                right: _,
            } => Part3((), *middle_min, (), *right_min, ()),
        }
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a i32> + 'a> {
        match self {
            Node::Binary {
                height: _,
                left,
                right_min: _,
                right,
            } => Box::new(left.iter().chain(right.iter())),

            Node::Ternary {
                height: _,
                left,
                middle_min: _,
                middle,
                right_min: _,
                right,
            } => Box::new(left.iter().chain(middle.iter()).chain(right.iter())),
        }
    }

    fn flush(self, config: &TreeConfig, plan: FlushPlan<Vec<Update>>) -> NodeBuilder {
        use FlushPlan::*;
        use UpdateResult::Done;

        match plan {
            NoFlush => NodeBuilder::from_node(self),
            FlushLeft(left_batch) => match self {
                Node::Binary {
                    height: _,
                    left,
                    right_min,
                    right,
                } => NodeBuilder::new(left.update(config, left_batch)).update(
                    config,
                    right_min,
                    Done(right),
                ),

                Node::Ternary {
                    height: _,
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => NodeBuilder::new(left.update(config, left_batch))
                    .update(config, middle_min, Done(middle))
                    .update(config, right_min, Done(right)),
            },
            FlushRight(right_batch) => match self {
                Node::Binary {
                    height: _,
                    left,
                    right_min,
                    right,
                } => NodeBuilder::new(Done(left)).update(
                    config,
                    right_min,
                    right.update(config, right_batch),
                ),

                Node::Ternary {
                    height: _,
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => NodeBuilder::new(Done(left))
                    .update(config, middle_min, Done(middle))
                    .update(config, right_min, right.update(config, right_batch)),
            },
            FlushMiddle(middle_batch) => match self {
                Node::Ternary {
                    height: _,
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => NodeBuilder::new(Done(left))
                    .update(config, middle_min, middle.update(config, middle_batch))
                    .update(config, right_min, Done(right)),

                _ => panic!("Ternary Node plan applied to a Binary Node!"),
            },
            FlushLeftMiddle(left_batch, middle_batch) => match self {
                Node::Ternary {
                    height: _,
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => NodeBuilder::new(left.update(config, left_batch))
                    .update(config, middle_min, middle.update(config, middle_batch))
                    .update(config, right_min, Done(right)),

                _ => panic!("Ternary Node plan applied to a Binary Node!"),
            },
            FlushLeftRight(left_batch, right_batch) => match self {
                Node::Ternary {
                    height: _,
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => NodeBuilder::new(left.update(config, left_batch))
                    .update(config, middle_min, Done(middle))
                    .update(config, right_min, right.update(config, right_batch)),

                _ => panic!("Ternary Node plan applied to a Binary Node!"),
            },
            FlushMiddleRight(middle_batch, right_batch) => match self {
                Node::Ternary {
                    height: _,
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => NodeBuilder::new(Done(left))
                    .update(config, middle_min, middle.update(config, middle_batch))
                    .update(config, right_min, right.update(config, right_batch)),

                _ => panic!("Ternary Node plan applied to a Binary Node!"),
            },
        }
    }
}

#[derive(Debug)]
pub struct BufferNode {
    // The invariant between `queue` and `node`:
    //
    // v = {left, right_min, right}
    // q {leftCount <= B, rightCount <= B}
    //
    // v = {left, middle_min, middle, right_min, right}
    // q {leftCount + middleCount <= B, middleCount + rightCount <= B}
    //
    // When batch-updating:
    //  - cascade is always B/2 to B
    //  - at most 1 branch of binary nodes and at most 2 branches of ternary
    //    nodes cascade: 1/2 to 2/3 of the branches off a node.
    //
    queue: Queue,
    node: Node,
}

impl Height for BufferNode {
    fn height(&self) -> u16 {
        self.node.height()
    }
}

impl Viable for BufferNode {
    fn is_viable(&self, config: &TreeConfig) -> bool {
        self.node.is_viable(config)
    }
}

//--------------------------------------------------------

impl Update {
    pub fn key<'a>(&'a self) -> &'a i32 {
        use Update::{Delete, Put};
        match self {
            Put(key) => key,
            Delete(key) => key,
        }
    }
    pub fn resolve<'a>(&'a self) -> Option<&'a i32> {
        use Update::{Delete, Put};
        match self {
            Put(key) => Some(key),
            Delete(key) => None,
        }
    }
}
//--------------------------------------------------------

pub enum UpdateResult {
    Done(Subtree),
    Split(Subtree, i32, Subtree),
    Merge(Orphan),     // child-level
    DeepMerge(Orphan), // grandchild-level
}

impl Split for UpdateResult {
    fn split(b0: Subtree, m1: i32, b1: Subtree) -> Self {
        UpdateResult::Split(b0, m1, b1)
    }
}

impl Done for UpdateResult {
    fn done(b0: Subtree) -> Self {
        UpdateResult::Done(b0)
    }
}

#[derive(Debug)]
pub enum Orphan {
    Items(Vec<i32>),
    Child(Subtree),
}

pub enum MergeResult {
    Done(Subtree),
    Split(Subtree, i32, Subtree),
}

impl Split for MergeResult {
    fn split(b0: Subtree, m1: i32, b1: Subtree) -> Self {
        MergeResult::Split(b0, m1, b1)
    }
}

impl Done for MergeResult {
    fn done(b0: Subtree) -> Self {
        MergeResult::Done(b0)
    }
}

macro_rules! make_node {
    [$child0:expr, $min1:expr, $child1:expr] => {
        Node::binary($child0, $min1, $child1)
    };
    [$child0:expr, $min1:expr, $child1:expr, $min2:expr, $child2:expr] => {
        Node::ternary($child0, $min1, $child1, $min2, $child2)
    };
}

macro_rules! make_buffer_node {
    [$queue:expr, $($x:expr),*] => {
        BufferNode {
            queue: $queue,
            node: make_node![$($x),*],
        }
    }
 }

macro_rules! make_branch {
    [$($x:expr),*] => {
        Subtree::Branch(Box::new(make_buffer_node![$($x),*]))
    };
}

macro_rules! split {
    [$branch:expr, $min:expr, $($x:expr),*] => {
        UpdateResult::Split(
            Subtree::Branch($branch),
            $min,
            make_branch![$($x),*],
        )
    };
}

pub fn fuse_vals(mut v0: Vec<i32>, mut v1: Vec<i32>) -> Vec<i32> {
    assert!(
        v0.last() <= v1.first().or(v0.last()),
        "fuse_vals: out-of-order\n  v0={:?}\n  v1={:?}",
        v0,
        v1
    );
    v0.append(&mut v1);
    v0
}

pub fn fuse_orphans(config: &TreeConfig, left: Orphan, right_min: i32, right: Orphan) -> Subtree {
    use Orphan::{Child, Items};

    match (left, right) {
        (Items(left_vals), Items(mut right_vals)) => {
            assert!(left_vals.len() + right_vals.len() >= config.batch_size);
            assert!(left_vals.len() + right_vals.len() <= config.batch_size * 2);

            Subtree::Leaf {
                vals: fuse_vals(left_vals, right_vals),
            }
        }
        (Child(left_subtree), Child(right_subtree)) => {
            make_branch![Queue::new(), left_subtree, right_min, right_subtree]
        }
        _ => {
            panic!("fuse must be called on like items.");
        }
    }
}

//--------------------------------------------------------
#[derive(Debug)]
pub enum Subtree {
    Leaf { vals: Vec<i32> },
    Branch(Box<BufferNode>),
}

impl Height for Subtree {
    fn height(&self) -> u16 {
        match self {
            Subtree::Leaf { .. } => 0,
            Subtree::Branch(ref branch) => branch.height(),
        }
    }
}

impl Viable for Subtree {
    fn is_viable(&self, config: &TreeConfig) -> bool {
        match self {
            Subtree::Leaf { ref vals } => {
                vals.len() >= config.batch_size && vals.len() <= config.batch_size * 2
            }
            Subtree::Branch(ref branch) => branch.is_viable(config),
        }
    }
}

impl Subtree {
    fn empty() -> Self {
        Subtree::Leaf { vals: vec![] }
    }

    fn new(batch: Vec<Update>) -> Self {
        Subtree::Leaf {
            vals: batch
                .iter()
                .filter_map(|update| update.resolve())
                .map(|item_ref| *item_ref)
                .collect(),
        }
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a i32> + 'a> {
        use itertools::EitherOrBoth::{Both, Left, Right};
        use Subtree::{Branch, Leaf};

        match self {
            Leaf { vals } => Box::new(vals.iter()),
            Branch(ref branch) => {
                let BufferNode {
                    queue: Queue(ref updates),
                    ref node,
                } = &**branch;

                Box::new(
                    node.iter()
                        .merge_join_by(updates.iter(), |a, b| a.cmp(&b.key()))
                        .filter_map(|either| match either {
                            Left(from_subtree) => Some(from_subtree),
                            Right(from_updates) => from_updates.resolve(),
                            Both(_, from_updates) => from_updates.resolve(),
                        }),
                )
            }
        }
    }

    fn to_vec(&self, dst: &mut Vec<i32>) {
        dst.extend(self.iter());
    }

    fn find(&self, key: &i32) -> Option<&i32> {
        use Subtree::{Branch, Leaf};

        match self {
            Leaf { vals } => match vals.binary_search(key) {
                Ok(index) => Some(&vals[index]),
                Err(_) => None,
            },
            Branch(ref branch) => match &**branch {
                BufferNode {
                    queue,
                    node:
                        Node::Binary {
                            height: _,
                            left,
                            right_min,
                            right,
                        },
                } => match queue.find(key) {
                    Some(ref update) => update.resolve(),
                    None => {
                        if key < right_min {
                            left.find(key)
                        } else {
                            right.find(key)
                        }
                    }
                },
                BufferNode {
                    queue,
                    node:
                        Node::Ternary {
                            height: _,
                            left,
                            middle_min,
                            middle,
                            right_min,
                            right,
                        },
                } => match queue.find(key) {
                    Some(ref update) => update.resolve(),
                    None => {
                        if key < middle_min {
                            left.find(key)
                        } else if key < right_min {
                            middle.find(key)
                        } else {
                            right.find(key)
                        }
                    }
                },
            },
        }
    }

    fn merge_left(self, config: &TreeConfig, orphan: Orphan, left_min: i32) -> MergeResult {
        use MergeResult::{Done, Split};
        use Orphan::{Child, Items};
        use Subtree::{Branch, Leaf};

        match (orphan, left_min, self) {
            (Items(mut v0), m1, Leaf { vals: mut v1 }) => {
                maybe_split_leaf(config, fuse_vals(v0, v1))
            }
            (Child(c0), m1, Branch(mut branch)) => match (c0, m1, *branch) {
                (
                    b0,
                    m1,
                    BufferNode {
                        queue,
                        node:
                            Node::Binary {
                                height: _,
                                left: b1,
                                right_min: m2,
                                right: b2,
                            },
                    },
                ) => {
                    assert!(m1 < m2);
                    *branch = make_buffer_node![queue, b0, m1, b1, m2, b2];
                    Done(Branch(branch))
                }
                (
                    b0,
                    m1,
                    BufferNode {
                        queue,
                        node:
                            Node::Ternary {
                                height: _,
                                left: b1,
                                middle_min: m2,
                                middle: b2,
                                right_min: m3,
                                right: b3,
                            },
                    },
                ) => {
                    assert!(m1 < m2);

                    let (left_queue, right_queue) = queue.split(&m2);
                    assert!(left_queue.len() <= config.batch_size);
                    assert!(right_queue.len() <= config.batch_size);

                    *branch = make_buffer_node![left_queue, b0, m1, b1];
                    Split(Branch(branch), m2, make_branch![right_queue, b2, m3, b3])
                }
            },
            _ => panic!("illegal merge"),
        }
    }

    fn deep_merge_left(self, config: &TreeConfig, orphan: Orphan, left_min: i32) -> MergeResult {
        use MergeResult::{Done, Split};
        use Subtree::{Branch, Leaf};

        match self {
            Leaf { .. } => panic!("Can't deep merge a leaf!"),
            Branch(mut branch) => match *branch {
                BufferNode {
                    queue,
                    node:
                        Node::Binary {
                            height: _,
                            left: b0,
                            right_min: m2,
                            right: b2,
                        },
                } => {
                    match b0.merge_left(config, orphan, left_min) {
                        Done(b0) => {
                            *branch = make_buffer_node![queue, b0, m2, b2];
                        }
                        Split(b0, m1, b1) => {
                            *branch = make_buffer_node![queue, b0, m1, b1, m2, b2];
                        }
                    }
                    Done(Branch(branch))
                }
                BufferNode {
                    queue,
                    node:
                        Node::Ternary {
                            height: _,
                            left: b0,
                            middle_min: m2,
                            middle: b2,
                            right_min: m3,
                            right: b3,
                        },
                } => match b0.merge_left(config, orphan, left_min) {
                    Done(b0) => {
                        *branch = make_buffer_node![queue, b0, m2, b2, m3, b3];
                        Done(Branch(branch))
                    }
                    Split(b0, m1, b1) => {
                        let (left_queue, right_queue) = queue.split(&m2);
                        assert!(left_queue.len() <= config.batch_size);
                        assert!(right_queue.len() <= config.batch_size);

                        *branch = make_buffer_node![left_queue, b0, m1, b1];
                        Split(Branch(branch), m2, make_branch![right_queue, b2, m3, b3])
                    }
                },
            },
        }
    }

    fn merge_right(self, config: &TreeConfig, orphan_min: i32, orphan: Orphan) -> MergeResult {
        use MergeResult::{Done, Split};
        use Orphan::{Child, Items};
        use Subtree::{Branch, Leaf};

        match (self, orphan_min, orphan) {
            (Leaf { vals: mut v0 }, m1, Items(mut v1)) => {
                maybe_split_leaf(config, fuse_vals(v0, v1))
            }
            (Branch(mut branch), child_min, Child(child)) => match (*branch, child_min, child) {
                (
                    BufferNode {
                        queue,
                        node:
                            Node::Binary {
                                height: _,
                                left: b0,
                                right_min: m1,
                                right: b1,
                            },
                    },
                    m2,
                    b2,
                ) => {
                    assert!(m1 < m2, "m1={}, m2={}", m1, m2);
                    *branch = make_buffer_node![queue, b0, m1, b1, m2, b2];
                    Done(Branch(branch))
                }
                (
                    BufferNode {
                        queue,
                        node:
                            Node::Ternary {
                                height: _,
                                left: b0,
                                middle_min: m1,
                                middle: b1,
                                right_min: m2,
                                right: b2,
                            },
                    },
                    m3,
                    b3,
                ) => {
                    assert!(m2 < m3, "m2={}, m3={}", m2, m3);

                    let (left_queue, right_queue) = queue.split(&m2);
                    assert!(left_queue.len() <= config.batch_size);
                    assert!(right_queue.len() <= config.batch_size);

                    *branch = make_buffer_node![left_queue, b0, m1, b1];
                    Split(Branch(branch), m2, make_branch![right_queue, b2, m3, b3])
                }
            },
            _ => panic!("illegal merge"),
        }
    }

    fn deep_merge_right(self, config: &TreeConfig, orphan_min: i32, orphan: Orphan) -> MergeResult {
        use MergeResult::{Done, Split};
        use Subtree::{Branch, Leaf};

        match self {
            Leaf { .. } => panic!("Can't deep merge a leaf!"),
            Branch(mut branch) => match *branch {
                BufferNode {
                    queue,
                    node:
                        Node::Binary {
                            height: _,
                            left: b0,
                            right_min: m1,
                            right: b1,
                        },
                } => {
                    match b1.merge_right(config, orphan_min, orphan) {
                        Done(b1) => {
                            *branch = make_buffer_node![queue, b0, m1, b1];
                        }
                        Split(b1, m2, b2) => {
                            *branch = make_buffer_node![queue, b0, m1, b1, m2, b2];
                        }
                    }
                    Done(Branch(branch))
                }
                BufferNode {
                    queue,
                    node:
                        Node::Ternary {
                            height: _,
                            left: b0,
                            middle_min: m1,
                            middle: b1,
                            right_min: m2,
                            right: b2,
                        },
                } => match b2.merge_right(config, orphan_min, orphan) {
                    Done(b2) => {
                        *branch = make_buffer_node![queue, b0, m1, b1, m2, b2];
                        Done(Branch(branch))
                    }
                    Split(b2, m3, b3) => {
                        let (left_queue, right_queue) = queue.split(&m2);
                        assert!(left_queue.len() <= config.batch_size);
                        assert!(right_queue.len() <= config.batch_size);

                        *branch = make_buffer_node![left_queue, b0, m1, b1];
                        Split(Branch(branch), m2, make_branch![right_queue, b2, m3, b3])
                    }
                },
            },
        }
    }

    fn update(self, config: &TreeConfig, batch: Vec<Update>) -> UpdateResult {
        assert!(
            batch.len() <= config.batch_size,
            "batch too large (B={}), batch={:?}",
            config.batch_size,
            batch
        );

        match self {
            Subtree::Leaf { vals } => update_leaf(config, batch, vals),
            Subtree::Branch(mut branch) => update_node(config, branch, batch),
        }
    }
}

pub fn update_leaf(config: &TreeConfig, batch: Vec<Update>, vals: Vec<i32>) -> UpdateResult {
    use itertools::EitherOrBoth::{Both, Left, Right};

    let mut merged: Vec<i32> = vals
        .iter()
        .merge_join_by(batch.iter(), |old, update| old.cmp(&update.key()))
        .filter_map(|either| match either {
            Left(old) => Some(*old),
            Right(update) => update.resolve().map(|item_ref| *item_ref),
            Both(_old, update) => update.resolve().map(|item_ref| *item_ref),
        })
        .collect();

    assert!(merged.len() <= config.batch_size * 3);

    if merged.len() < config.batch_size {
        return UpdateResult::Merge(Orphan::Items(merged));
    }

    maybe_split_leaf(config, merged)
}

pub fn maybe_split_leaf<Result: Split + Done>(config: &TreeConfig, mut vals: Vec<i32>) -> Result {
    use Subtree::Leaf;

    if vals.len() <= config.batch_size * 2 {
        Result::done(Leaf { vals })
    } else {
        let split_vals: Vec<i32> = vals.drain((vals.len() / 2)..).collect();
        let split_min: i32 = split_vals[0];
        Result::split(Leaf { vals }, split_min, Leaf { vals: split_vals })
    }
}

#[derive(Debug)]
enum NodeBuilder {
    MergeLeft(Orphan),
    DeepMergeLeft(Orphan),
    Branch1(Subtree),
    Branch2(Subtree, i32, Subtree),
    Branch3(Subtree, i32, Subtree, i32, Subtree),
    Branch4(Subtree, i32, Subtree, i32, Subtree, i32, Subtree),
    Branch5(
        Subtree,
        i32,
        Subtree,
        i32,
        Subtree,
        i32,
        Subtree,
        i32,
        Subtree,
    ),
    Branch6(
        Subtree,
        i32,
        Subtree,
        i32,
        Subtree,
        i32,
        Subtree,
        i32,
        Subtree,
        i32,
        Subtree,
    ),
}

impl NodeBuilder {
    fn new(first: UpdateResult) -> Self {
        use NodeBuilder::*;
        use UpdateResult::*;

        match first {
            Done(b0) => Branch1(b0),
            Split(b0, m1, b1) => Branch2(b0, m1, b1),
            Merge(orphan) => MergeLeft(orphan),
            DeepMerge(orphan) => DeepMergeLeft(orphan),
        }
    }
    fn update(self, config: &TreeConfig, next_min: i32, next: UpdateResult) -> Self {
        use NodeBuilder::*;
        use Orphan::{Child, Items};
        use UpdateResult::*;

        match (self, next_min, next) {
            // Fuse case - 0 => 0,1
            //
            (MergeLeft(o0), m1, Merge(o1)) => Branch1(fuse_orphans(config, o0, m1, o1)),
            (DeepMergeLeft(oo0), m1, DeepMerge(oo1)) => {
                MergeLeft(Child(fuse_orphans(config, oo0, m1, oo1)))
            }

            // Done cases - grow by 1
            //
            (Branch1(b0), m1, Done(b1)) => Branch2(b0, m1, b1),
            (Branch2(b0, m1, b1), m2, Done(b2)) => Branch3(b0, m1, b1, m2, b2),
            (Branch3(b0, m1, b1, m2, b2), m3, Done(b3)) => Branch4(b0, m1, b1, m2, b2, m3, b3),
            (Branch4(b0, m1, b1, m2, b2, m3, b3), m4, Done(b4)) => {
                Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4)
            }
            (Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4), m5, Done(b5)) => {
                Branch6(b0, m1, b1, m2, b2, m3, b3, m4, b4, m5, b5)
            }

            // Split cases - grow by 2
            //
            (Branch1(b0), m1, Split(b1, m2, b2)) => Branch3(b0, m1, b1, m2, b2),
            (Branch2(b0, m1, b1), m2, Split(b2, m3, b3)) => Branch4(b0, m1, b1, m2, b2, m3, b3),
            (Branch3(b0, m1, b1, m2, b2), m3, Split(b3, m4, b4)) => {
                Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4)
            }
            (Branch4(b0, m1, b1, m2, b2, m3, b3), m4, Split(b4, m5, b5)) => {
                Branch6(b0, m1, b1, m2, b2, m3, b3, m4, b4, m5, b5)
            }

            // Merge cases - grow by 0 or 1
            //
            (MergeLeft(o0), m0, Done(b0)) => match b0.merge_left(config, o0, m0) {
                MergeResult::Done(b0) => Branch1(b0),
                MergeResult::Split(b0, m1, b1) => Branch2(b0, m1, b1),
            },
            (MergeLeft(o0), m0, Split(b0, m2, b2)) => match b0.merge_left(config, o0, m0) {
                MergeResult::Done(b0) => Branch2(b0, m2, b2),
                MergeResult::Split(b0, m1, b1) => Branch3(b0, m1, b1, m2, b2),
            },
            (Branch1(b0), m1, Merge(orphan)) => match b0.merge_right(config, m1, orphan) {
                MergeResult::Done(b0) => Branch1(b0),
                MergeResult::Split(b0, m1, b1) => Branch2(b0, m1, b1),
            },
            (Branch2(b0, m1, b1), m2, Merge(orphan)) => match b1.merge_right(config, m2, orphan) {
                MergeResult::Done(b1) => Branch2(b0, m1, b1),
                MergeResult::Split(b1, m2, b2) => Branch3(b0, m1, b1, m2, b2),
            },
            (Branch3(b0, m1, b1, m2, b2), m3, Merge(orphan)) => {
                match b2.merge_right(config, m3, orphan) {
                    MergeResult::Done(b2) => Branch3(b0, m1, b1, m2, b2),
                    MergeResult::Split(b2, m3, b3) => Branch4(b0, m1, b1, m2, b2, m3, b3),
                }
            }
            (Branch4(b0, m1, b1, m2, b2, m3, b3), m4, Merge(orphan)) => {
                match b3.merge_right(config, m4, orphan) {
                    MergeResult::Done(b3) => Branch4(b0, m1, b1, m2, b2, m3, b3),
                    MergeResult::Split(b3, m4, b4) => Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4),
                }
            }
            (Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4), m5, Merge(orphan)) => {
                match b4.merge_right(config, m5, orphan) {
                    MergeResult::Done(b4) => Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4),
                    MergeResult::Split(b4, m5, b5) => {
                        Branch6(b0, m1, b1, m2, b2, m3, b3, m4, b4, m5, b5)
                    }
                }
            }
            (Branch5(..), _, Split(..)) => panic!("NodeBuilder is full!"),
            (Branch6(..), _, _) => panic!("NodeBuilder is full!"),

            // Deep Merge cases
            //
            (DeepMergeLeft(oo0), m0, Done(b0)) => match b0.deep_merge_left(config, oo0, m0) {
                MergeResult::Done(b0) => Branch1(b0),
                MergeResult::Split(b0, m1, b1) => Branch2(b0, m1, b1),
            },
            (DeepMergeLeft(oo0), m1, Split(b1, m2, b2)) => {
                match b1.deep_merge_left(config, oo0, m1) {
                    MergeResult::Done(b1) => Branch2(b1, m2, b2),
                    MergeResult::Split(b0, m1, b1) => Branch3(b0, m1, b1, m2, b2),
                }
            }
            (DeepMergeLeft(oo0), m1, Merge(Child(o1))) => match o1.merge_left(config, oo0, m1) {
                MergeResult::Done(o1) => MergeLeft(Child(o1)),
                MergeResult::Split(o1, m2, o2) => {
                    Branch1(fuse_orphans(config, Child(o1), m2, Child(o2)))
                }
            },
            (MergeLeft(Items(_)), _, DeepMerge(_)) => {
                panic!("tried to deep merge right below leaf");
            }
            (DeepMergeLeft(_), _, Merge(Items(_))) => {
                panic!("tried to deep merge left below leaf");
            }
            (MergeLeft(Child(o0)), m1, DeepMerge(oo1)) => match o0.merge_right(config, m1, oo1) {
                MergeResult::Done(o0) => MergeLeft(Child(o0)),
                MergeResult::Split(o0, m1, o1) => {
                    Branch1(fuse_orphans(config, Child(o0), m1, Child(o1)))
                }
            },
            (Branch1(b0), m1, DeepMerge(oo1)) => match b0.deep_merge_right(config, m1, oo1) {
                MergeResult::Done(b0) => Branch1(b0),
                MergeResult::Split(b0, m1, b1) => Branch2(b0, m1, b1),
            },
            (Branch2(b0, m1, b1), m2, DeepMerge(oo2)) => match b1.deep_merge_right(config, m2, oo2)
            {
                MergeResult::Done(b1) => Branch2(b0, m1, b1),
                MergeResult::Split(b1, m2, b2) => Branch3(b0, m1, b1, m2, b2),
            },
            (Branch3(b0, m1, b1, m2, b2), m3, DeepMerge(oo3)) => {
                match b2.deep_merge_right(config, m3, oo3) {
                    MergeResult::Done(b2) => Branch3(b0, m1, b1, m2, b2),
                    MergeResult::Split(b2, m3, b3) => Branch4(b0, m1, b1, m2, b2, m3, b3),
                }
            }
            (Branch4(b0, m1, b1, m2, b2, m3, b3), m4, DeepMerge(oo4)) => {
                match b3.deep_merge_right(config, m4, oo4) {
                    MergeResult::Done(b3) => Branch4(b0, m1, b1, m2, b2, m3, b3),
                    MergeResult::Split(b3, m4, b4) => Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4),
                }
            }
            (Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4), m5, DeepMerge(oo5)) => {
                match b4.deep_merge_right(config, m5, oo5) {
                    MergeResult::Done(b4) => Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4),
                    MergeResult::Split(b4, m5, b5) => {
                        Branch6(b0, m1, b1, m2, b2, m3, b3, m4, b4, m5, b5)
                    }
                }
            }
        }
    }

    fn from_node(node: Node) -> Self {
        match node {
            Node::Binary {
                height: _,
                left,
                right_min,
                right,
            } => Self::Branch2(left, right_min, right),

            Node::Ternary {
                height: _,
                left,
                middle_min,
                middle,
                right_min,
                right,
            } => Self::Branch3(left, middle_min, middle, right_min, right),
        }
    }
}

pub fn update_node(
    config: &TreeConfig,
    mut branch: Box<BufferNode>,
    mut batch: Vec<Update>,
) -> UpdateResult {
    use NodeBuilder::*;
    use Orphan::Child;
    use Subtree::Branch;
    use UpdateResult::{DeepMerge, Done, Merge, Split};

    if batch.is_empty() {
        return Done(Subtree::Branch(branch));
    }

    let BufferNode { mut queue, node } = *branch;

    use itertools::EitherOrBoth::{Both, Left, Right};

    assert!(batch.len() <= config.batch_size);
    assert!(!node.is_binary() || queue.len() <= config.batch_size);
    assert!(!node.is_ternary() || queue.len() <= config.batch_size * 3 / 2);

    let mut queue = queue.merge(batch);
    let queue_partition = queue.partition(node.partition());
    let queue_parts_debug = format!("{:?}", queue_partition);
    let queue_plan = FlushPlan::new(config, &queue_partition);
    let queue_plan_debug = format!("{:?}", queue_plan);
    let batch_plan = queue.flush(config, &queue_partition, &queue_plan);
    let builder = node.flush(config, batch_plan);

    return match builder {
        MergeLeft(o0) => DeepMerge(o0),
        Branch1(b0) =>
        //
        // TODO - Before we return in this case, send any remaining queued updates down to `b0`.
        //   This is needed because there is no affordance for bubbling unflushed queued updates
        //   on a merge, which in turn is because the merge operations must be O(1) to achieve
        //   our overall algorithmic complexity aim.  It is safe/correct to do this because:
        //     1. Any remaining `queue` updates must be <= batch_size in number.  This is because
        //        we can only trigger a merge if the node was binary when `update_node` was called.
        //        (TODO: it would be great to hint/guarantee this more strongly using types)
        //     2. If we are down to one child branch at this point, then by definition its bounding
        //        key range has expanded to fill the entire range of its parent; there is no other
        //        Subtree that can 'claim' ownership of the key range of the defunct sibling of `b0`.
        {
            if queue.len() == 0 {
                Merge(Child(b0))
            } else {
                //let b0_debug = format!("{:?}", b0);
                //let q_debug = format!("{:?}", queue);
                match b0.update(config, queue.consume()) {
                    Done(b0) => Merge(Child(b0)),
                    Split(b0, m1, b1) => {
                        assert!(b0.is_viable(config));
                        assert!(b1.is_viable(config));

                        *branch = make_buffer_node![Queue::new(), b0, m1, b1];
                        Done(Branch(branch))
                    }
                    Merge(orphan) => DeepMerge(orphan),
                    DeepMerge(_) => panic!("implement arbitrarily deep merges"),
                }
            }
        }
        Branch2(b0, m1, b1) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            assert!(
                queue.len() <= config.batch_size,
                "queue is too long!\n  queue.len()={}\n  b0={:#?}\n  b1={:#?}\n  queue={:?}, plan={:?}, parts={:?}",
                queue.len(),
                b0,
                b1,
                queue,
                queue_plan_debug,
                queue_parts_debug
            );

            *branch = make_buffer_node![queue, b0, m1, b1];
            Done(Branch(branch))
        }
        Branch3(b0, m1, b1, m2, b2) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            assert!(b2.is_viable(config));
            assert!(
                queue.len() <= config.batch_size * 3 / 2,
                "queue is too long!\n  queue.len()={}\n  b0={:#?}\n  b1={:#?}\n  b2={:#?}\n  queue={:?}, plan={:?}, parts={:?}",
                queue.len(),
                b0,
                b1,b2,
                queue,
                queue_plan_debug,
                queue_parts_debug
            );

            *branch = make_buffer_node![queue, b0, m1, b1, m2, b2];
            Done(Branch(branch))
        }
        Branch4(b0, m1, b1, m2, b2, m3, b3) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            assert!(b2.is_viable(config));
            assert!(b3.is_viable(config));

            let (q0, q1) = queue.split(&m2);
            assert!(q0.len() <= config.batch_size);
            assert!(q1.len() <= config.batch_size);

            *branch = make_buffer_node![q0, b0, m1, b1];
            split![branch, m2, q1, b2, m3, b3]
        }
        Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            assert!(b2.is_viable(config));
            assert!(b3.is_viable(config));
            assert!(b4.is_viable(config));

            let (q0, q1) = queue.split(&m3);
            assert!(q0.len() <= config.batch_size * 3 / 2);
            assert!(q1.len() <= config.batch_size);

            *branch = make_buffer_node![q0, b0, m1, b1, m2, b2];
            split![branch, m3, q1, b3, m4, b4]
        }
        Branch6(b0, m1, b1, m2, b2, m3, b3, m4, b4, m5, b5) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            assert!(b2.is_viable(config));
            assert!(b3.is_viable(config));
            assert!(b4.is_viable(config));
            assert!(b5.is_viable(config));

            let (q0, q1) = queue.split(&m3);
            assert!(q0.len() <= config.batch_size * 3 / 2);
            assert!(q1.len() <= config.batch_size * 3 / 2);

            *branch = make_buffer_node![q0, b0, m1, b1, m2, b2];
            split![branch, m3, q1, b3, m4, b4, m5, b5]
        }
        _ => panic!("update error! builder={:?}", builder),
    };
}

#[derive(Debug)]
struct Tree {
    config: TreeConfig,
    root: Subtree,
}

impl Tree {
    fn new(config: TreeConfig) -> Self {
        Self {
            config,
            root: Subtree::empty(),
        }
    }

    fn insert(&mut self, val: i32) {
        self.update(vec![Update::Put(val)])
    }

    fn remove(&mut self, val: i32) {
        self.update(vec![Update::Delete(val)])
    }

    fn update(&mut self, batch: Vec<Update>) {
        use Orphan::{Child, Items};
        use Subtree::{Branch, Leaf};
        use UpdateResult::{DeepMerge, Done, Merge, Split};

        let root = std::mem::replace(&mut self.root, Subtree::empty());
        self.root = match root.update(&self.config, batch) {
            // Tree height stays the same.
            //
            Done(b0) => b0,

            // Tree height grows (due to split).
            //
            Split(b0, m1, b1) => make_branch![Queue::new(), b0, m1, b1],

            // Tree height shrinks (due to merge).
            //
            Merge(orphan) => match orphan {
                Items(vals) => Leaf { vals },
                Child(branch) => branch,
            },
            DeepMerge(orphan) => match orphan {
                Items(vals) => Leaf { vals },
                Child(branch) => branch,
            },
        };
    }

    fn to_vec(&self) -> Vec<i32> {
        let mut v: Vec<i32> = Vec::new();
        self.root.to_vec(&mut v);
        v
    }

    fn find<'a>(&'a self, val: i32) -> Option<&'a i32> {
        self.root.find(&val)
    }
    fn height(&self) -> u16 {
        self.root.height()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_test() {
        assert_eq!(2 + 2, 4);

        let mut t = Tree::new(TreeConfig { batch_size: 8 });

        assert!(t.find(10) == None);

        t.insert(10);
        println!("{:#?}", t);

        assert_eq!(t.find(10), Some(&10));

        for k in 0..1000 {
            t.insert(k);
        }

        for k in 0..1000 {
            assert_eq!(t.find(k), Some(&k));
        }

        assert_eq!(t.height(), 6);

        for k in 1000..100000 {
            t.insert(k);
        }

        for k in 1000..100000 {
            assert_eq!(t.find(k), Some(&k));
        }

        assert_eq!(t.height(), 12);
    }

    #[test]
    fn remove_test() {
        let mut t = Tree::new(TreeConfig { batch_size: 8 });
        let max_k: i32 = 100000;

        for k in 0..max_k {
            t.insert(k);
        }

        for k in 0..max_k {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 12);

        for k in 0..max_k {
            assert!(t.find(k) == Some(&k));
            //println!("tree={:#?}, k={}", t, k);
            t.remove(k);
            assert!(t.find(k) == None, "k={}, tree={:#?}", k, t);
        }

        for k in 0..max_k {
            assert!(t.find(k) == None);
        }

        assert_eq!(t.height(), 0);
    }

    #[test]
    fn random_update_test() {
        use rand::distributions::{Distribution, Uniform};
        use rand::prelude::*;

        let mut rng = rand::thread_rng();
        for n in 0..100 {
            let mut x: Vec<Update> = (0..1024).map(Update::Put).collect();
            let mut y: Vec<Update> = Vec::new();

            while !x.is_empty() {
                let i = Uniform::from(0..x.len()).sample(&mut rng);
                let c: Update = x[i];
                match c {
                    Update::Put(k) => {
                        x[i] = Update::Delete(k);
                    }
                    Update::Delete(k) => {
                        x.remove(i);
                    }
                }
                y.push(c);
            }

            let mut t = Tree::new(TreeConfig { batch_size: 8 });

            let batches: Vec<Vec<Update>> = y
                .chunks(t.config.batch_size)
                .map(|chunk| {
                    let mut tmp: Vec<Update> = Vec::from(chunk);
                    tmp.sort_by_key(|update| *update.key());
                    let mut batch: Vec<Update> = Vec::new();
                    for i in 0..(tmp.len() - 1) {
                        if tmp[i].key() != tmp[i + 1].key() {
                            batch.push(tmp[i]);
                        }
                    }
                    batch.push(tmp[tmp.len() - 1]);
                    batch
                })
                .collect();

            use std::collections::BTreeSet;

            let mut good: BTreeSet<i32> = BTreeSet::new();

            let mut max_height = 0;

            for batch in &batches {
                for update in batch {
                    match &update {
                        Update::Put(k) => {
                            good.insert(*k);
                        }
                        Update::Delete(k) => {
                            good.remove(k);
                        }
                    }
                }
                t.update(batch.clone());
                max_height = std::cmp::max(max_height, t.height());
                assert_eq!(
                    t.to_vec(),
                    good.iter().map(|x| *x).collect::<Vec<i32>>(),
                    "t={:#?}",
                    t
                );
            }

            assert!(max_height >= 4, "max_height={}", max_height);
        }
    }
}
