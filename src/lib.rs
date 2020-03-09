#![allow(dead_code)]
#![allow(unused_macros)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use itertools::Itertools;

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

impl Node {
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

//--------------------------------------------------------
#[derive(Debug, Copy, Clone)]
pub enum Update {
    Put(i32),
    Delete(i32),
}

impl Update {
    pub fn key<'a>(&'a self) -> &'a i32 {
        use Update::{Delete, Put};
        match self {
            Put(key) => key,
            Delete(key) => key,
        }
    }
    pub fn resolve(&self) -> Option<i32> {
        use Update::{Delete, Put};
        match self {
            Put(key) => Some(*key),
            Delete(key) => None,
        }
    }
}
//--------------------------------------------------------

pub enum UpdateResult {
    Done(Subtree),
    Split(Subtree, i32, Subtree),
    Merge(Orphan),
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

macro_rules! make_branch {
    [$($x:expr),*] => {
        Subtree::Branch(Box::new(make_node![$($x),*]))
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

macro_rules! update_branch {
    ($branch:expr, $child0:expr) => {{
        UpdateResult::Merge(Orphan::Child($child0))
    }};
    ($branch:expr, $child0:expr, $min1:expr, $child1:expr) => {{
        *$branch = make_node![$child0, $min1, $child1];
        UpdateResult::Done(Subtree::Branch($branch))
    }};
    ($branch:expr, $child0:expr, $min1:expr, $child1:expr, $min2:expr, $child2:expr) => {{
        *$branch = make_node![$child0, $min1, $child1, $min2, $child2];
        UpdateResult::Done(Subtree::Branch($branch))
    }};
    ($branch:expr, $child0:expr, $min1:expr, $child1:expr,
     $min2:expr, $child2:expr, $min3:expr, $child3:expr) => {{
        *$branch = make_node![$child0, $min1, $child1];
        split![$branch, $min2, $child2, $min3, $child3]
    }};
    ($branch:expr, $child0:expr, $min1:expr, $child1:expr, $min2:expr, $child2:expr,
     $min3:expr, $child3:expr, $min4:expr, $child4:expr) => {{
        *$branch = make_node![$child0, $min1, $child1, $min2, $child2];
        split![$branch, $min3, $child3, $min4, $child4]
    }};
    ($branch:expr, $child0:expr, $min1:expr, $child1:expr, $min2:expr, $child2:expr,
     $min3:expr, $child3:expr, $min4:expr, $child4:expr, $min5:expr, $child5:expr) => {{
        *$branch = make_node![$child0, $min1, $child1, $min2, $child2];
        split![$branch, $min3, $child3, $min4, $child4, $min5, $child5]
    }};
}

pub fn fuse_vals(mut v0: Vec<i32>, mut v1: Vec<i32>) -> Vec<i32> {
    v0.append(&mut v1);
    v0
}

pub fn fuse2(config: &TreeConfig, left: Orphan, right_min: i32, right: Orphan) -> Subtree {
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
            make_branch![left_subtree, right_min, right_subtree]
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
    Branch(Box<Node>),
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
    fn new(batch: Vec<Update>) -> Self {
        Subtree::Leaf {
            vals: batch.iter().filter_map(|update| update.resolve()).collect(),
        }
    }

    fn to_vec(&self, dst: &mut Vec<i32>) {
        use Subtree::{Branch, Leaf};

        match self {
            Leaf { vals } => {
                dst.extend(vals);
            }
            Branch(ref branch) => match &**branch {
                Node::Binary {
                    height: _,
                    left,
                    right_min,
                    right,
                } => {
                    left.to_vec(dst);
                    right.to_vec(dst);
                }
                Node::Ternary {
                    height: _,
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => {
                    left.to_vec(dst);
                    middle.to_vec(dst);
                    right.to_vec(dst);
                }
            },
        }
    }

    fn find(&self, key: &i32) -> Option<&i32> {
        use Subtree::{Branch, Leaf};

        match self {
            Leaf { vals } => match vals.binary_search(key) {
                Ok(index) => Some(&vals[index]),
                Err(_) => None,
            },
            Branch(ref branch) => match &**branch {
                Node::Binary {
                    height: _,
                    left,
                    right_min,
                    right,
                } => {
                    if key < right_min {
                        left.find(key)
                    } else {
                        right.find(key)
                    }
                }
                Node::Ternary {
                    height: _,
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => {
                    if key < middle_min {
                        left.find(key)
                    } else if key < right_min {
                        middle.find(key)
                    } else {
                        right.find(key)
                    }
                }
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
                    Node::Binary {
                        height: _,
                        left: b1,
                        right_min: m2,
                        right: b2,
                    },
                ) => {
                    assert!(m1 < m2);
                    *branch = make_node![b0, m1, b1, m2, b2];
                    Done(Branch(branch))
                }
                (
                    b0,
                    m1,
                    Node::Ternary {
                        height: _,
                        left: b1,
                        middle_min: m2,
                        middle: b2,
                        right_min: m3,
                        right: b3,
                    },
                ) => {
                    assert!(m1 < m2);
                    *branch = make_node![b0, m1, b1];
                    Split(Branch(branch), m2, make_branch![b2, m3, b3])
                }
            },
            _ => panic!("illegal merge"),
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
                    Node::Binary {
                        height: _,
                        left: b0,
                        right_min: m1,
                        right: b1,
                    },
                    m2,
                    b2,
                ) => {
                    assert!(m1 < m2, "m1={}, m2={}", m1, m2);
                    *branch = make_node![b0, m1, b1, m2, b2];
                    Done(Branch(branch))
                }
                (
                    Node::Ternary {
                        height: _,
                        left: b0,
                        middle_min: m1,
                        middle: b1,
                        right_min: m2,
                        right: b2,
                    },
                    m3,
                    b3,
                ) => {
                    assert!(m2 < m3, "m2={}, m3={}", m2, m3);
                    *branch = make_node![b0, m1, b1];
                    Split(Branch(branch), m2, make_branch![b2, m3, b3])
                }
            },
            _ => panic!("illegal merge"),
        }
    }

    fn update(self, config: &TreeConfig, batch: Vec<Update>) -> UpdateResult {
        assert!(batch.len() <= config.batch_size);

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
            Right(update) => update.resolve(),
            Both(_old, update) => update.resolve(),
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
        }
    }
    fn update(self, config: &TreeConfig, next_min: i32, next: UpdateResult) -> Self {
        use NodeBuilder::*;
        use UpdateResult::*;

        match (self, next_min, next) {
            // Fuse case - 0 => 1
            //
            (MergeLeft(o0), m1, Merge(o1)) => Branch1(fuse2(config, o0, m1, o1)),
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
        }
    }
}

pub fn update_node(config: &TreeConfig, mut branch: Box<Node>, batch: Vec<Update>) -> UpdateResult {
    use NodeBuilder::*;
    use Orphan::Child;
    use UpdateResult::{Done, Merge, Split};

    let builder = {
        if let Node::Binary {
            height: _,
            left,
            right_min,
            right,
        } = *branch
        {
            if batch.is_empty() {
                return update_branch!(branch, left, right_min, right);
            }

            let (left_batch, right_batch): (Vec<Update>, Vec<Update>) = batch
                .into_iter()
                .partition(|update| update.key() < &right_min);

            NodeBuilder::new(left.update(config, left_batch)).update(
                config,
                right_min,
                right.update(config, right_batch),
            )
        } else if let Node::Ternary {
            height: _,
            left,
            middle_min,
            middle,
            right_min,
            right,
        } = *branch
        {
            if batch.is_empty() {
                return update_branch!(branch, left, middle_min, middle, right_min, right);
            }

            let (left_batch, non_left_batch): (Vec<Update>, Vec<Update>) = batch
                .into_iter()
                .partition(|update| update.key() < &middle_min);

            let (middle_batch, right_batch): (Vec<Update>, Vec<Update>) = non_left_batch
                .into_iter()
                .partition(|update| update.key() < &right_min);

            NodeBuilder::new(left.update(config, left_batch))
                .update(config, middle_min, middle.update(config, middle_batch))
                .update(config, right_min, right.update(config, right_batch))
        } else {
            panic!("update_node called on non-node: {:?}", branch);
        }
    };

    return match builder {
        Branch1(b0) => update_branch!(branch, b0),
        Branch2(b0, m1, b1) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            update_branch!(branch, b0, m1, b1)
        }
        Branch3(b0, m1, b1, m2, b2) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            assert!(b2.is_viable(config));
            update_branch!(branch, b0, m1, b1, m2, b2)
        }
        Branch4(b0, m1, b1, m2, b2, m3, b3) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            assert!(b2.is_viable(config));
            assert!(b3.is_viable(config));
            update_branch!(branch, b0, m1, b1, m2, b2, m3, b3)
        }
        Branch5(b0, m1, b1, m2, b2, m3, b3, m4, b4) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            assert!(b2.is_viable(config));
            assert!(b3.is_viable(config));
            assert!(b4.is_viable(config));
            update_branch!(branch, b0, m1, b1, m2, b2, m3, b3, m4, b4)
        }
        Branch6(b0, m1, b1, m2, b2, m3, b3, m4, b4, m5, b5) => {
            assert!(b0.is_viable(config));
            assert!(b1.is_viable(config));
            assert!(b2.is_viable(config));
            assert!(b3.is_viable(config));
            assert!(b4.is_viable(config));
            assert!(b5.is_viable(config));
            update_branch!(branch, b0, m1, b1, m2, b2, m3, b3, m4, b4, m5, b5)
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
            root: Subtree::Leaf { vals: Vec::new() },
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
        use UpdateResult::{Done, Merge, Split};

        let root = std::mem::replace(&mut self.root, Leaf { vals: Vec::new() });
        match root.update(&self.config, batch) {
            Done(b0) => {
                self.root = b0;
            }
            Split(b0, m1, b1) => {
                self.root = make_branch![b0, m1, b1];
            }
            Merge(orphan) => {
                self.root = match orphan {
                    Items(vals) => Leaf { vals },
                    Child(branch) => branch,
                }
            }
        }
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
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 6);

        for k in 1000..100000 {
            t.insert(k);
        }

        for k in 1000..100000 {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 13);
    }

    #[test]
    fn remove_test() {
        let mut t = Tree::new(TreeConfig { batch_size: 8 });

        for k in 0..100000 {
            t.insert(k);
        }

        for k in 0..100000 {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 13);

        for k in 0..100000 {
            assert!(t.find(k) == Some(&k));
            t.remove(k);
            assert!(t.find(k) == None, "k={}, tree={:#?}", k, t);
        }

        for k in 0..100000 {
            assert!(t.find(k) == None);
        }

        assert_eq!(t.height(), 0);
    }

    #[test]
    fn random_update_test() {
        use rand::distributions::{Distribution, Uniform};
        use rand::prelude::*;

        let mut rng = rand::thread_rng();
        for n in 0..10000 {
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
