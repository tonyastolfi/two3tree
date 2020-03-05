#![allow(dead_code)]
#![allow(unused_macros)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

//mod node;
//use crate::node::{Node, Subtree, TreeConfig};

//mod update;
//use crate::update::{
//    update_binary_node, update_leaf, update_ternary_node, MergeResult, Update, UpdateResult,
//};

use itertools::Itertools;

pub struct TreeConfig {
    pub batch_size: usize,
}

#[derive(Debug)]
pub enum Node {
    Binary {
        left: Subtree,
        right_min: i32,
        right: Subtree,
    },
    Ternary {
        left: Subtree,
        middle_min: i32,
        middle: Subtree,
        right_min: i32,
        right: Subtree,
    },
}

//--------------------------------------------------------
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

pub enum Orphan {
    Items(Vec<i32>),
    Child(Subtree),
}

pub enum MergeResult {
    Done(Subtree),
    Split(Subtree, i32, Subtree),
}

macro_rules! make_node {
    [$child0:expr, $min1:expr, $child1:expr] => {
        Node::Binary {
            left: $child0,
            right_min: $min1,
            right: $child1,
        }
    };
    [$child0:expr, $min1:expr, $child1:expr, $min2:expr, $child2:expr] => {
        Node::Ternary {
            left: $child0,
            middle_min: $min1,
            middle: $child1,
            right_min: $min2,
            right: $child2,
        }
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

pub fn fuse2(config: &TreeConfig, left: Orphan, right_min: i32, right: Orphan) -> Subtree {
    use Orphan::{Child, Items};

    match (left, right) {
        (Items(left_vals), Items(mut right_vals)) => {
            assert!(left_vals.len() + right_vals.len() >= config.batch_size);

            let mut vals = left_vals;
            vals.append(&mut right_vals);

            Subtree::Leaf { vals }
        }
        (Child(left_subtree), Child(right_subtree)) => {
            make_branch![left_subtree, right_min, right_subtree]
        }
        _ => {
            panic!("fuse must be called on like items.");
        }
    }
}

pub fn fuse3(
    config: &TreeConfig,
    left: Orphan,
    middle_min: i32,
    middle: Orphan,
    right_min: i32,
    right: Orphan,
) -> MergeResult {
    let left_middle: Subtree = fuse2(config, left, middle_min, middle);
    left_middle.merge_right(config, right_min, right)
}

//--------------------------------------------------------
#[derive(Debug)]
pub enum Subtree {
    Leaf { vals: Vec<i32> },
    Branch(Box<Node>),
}

impl Subtree {
    fn new(batch: Vec<Update>) -> Self {
        Subtree::Leaf {
            vals: batch.iter().filter_map(|update| update.resolve()).collect(),
        }
    }

    fn merge_left(self, config: &TreeConfig, orphan: Orphan, left_min: i32) -> MergeResult {
        MergeResult::Done(self)
    }

    fn merge_right(self, config: &TreeConfig, orphan_min: i32, orphan: Orphan) -> MergeResult {
        MergeResult::Done(self)
    }

    fn update(self, config: &TreeConfig, batch: Vec<Update>) -> UpdateResult {
        assert!(batch.len() <= config.batch_size);

        use Node::{Binary, Ternary};
        use Orphan::{Child, Items};
        use Subtree::{Branch, Leaf};
        use UpdateResult::{Done, Merge, Split};

        match self {
            Leaf { vals } => update_leaf(config, batch, vals),
            Branch(mut branch) => match &*branch {
                Node::Binary { .. } => update_binary_node(config, branch, batch),
                Node::Ternary { .. } => update_ternary_node(config, branch, batch),
            },
        }
    }
}

pub fn update_leaf(config: &TreeConfig, batch: Vec<Update>, vals: Vec<i32>) -> UpdateResult {
    use itertools::EitherOrBoth::{Both, Left, Right};
    use Orphan::Items;
    use Subtree::Leaf;
    use UpdateResult::{Done, Merge, Split};

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
        return Merge(Items(merged));
    }

    if merged.len() <= config.batch_size * 2 {
        return Done(Leaf { vals: merged });
    }

    let split_vals: Vec<i32> = merged.drain((merged.len() / 2)..).collect();
    let split_min: i32 = split_vals[0];
    return Split(Leaf { vals: merged }, split_min, Leaf { vals: split_vals });
}

pub fn update_binary_node(
    config: &TreeConfig,
    mut branch: Box<Node>,
    batch: Vec<Update>,
) -> UpdateResult {
    use Orphan::Child;
    use UpdateResult::{Done, Merge, Split};

    if let Node::Binary {
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

        return match left.update(config, left_batch) {
            Done(left) => match right.update(config, right_batch) {
                Done(right) => update_branch!(branch, left, right_min, right),
                Split(right, right_split_min, right_split) => {
                    update_branch!(branch, left, right_min, right, right_split_min, right_split)
                }
                Merge(right_orphan) => match left.merge_right(config, right_min, right_orphan) {
                    MergeResult::Done(left) => update_branch!(branch, left),
                    MergeResult::Split(left, split_min, split) => {
                        update_branch!(branch, left, split_min, split)
                    }
                },
            },
            Split(left, left_split1_min, mut left_split1) => {
                match right.update(config, right_batch) {
                    Done(right) => {
                        update_branch!(branch, left, left_split1_min, left_split1, right_min, right)
                    }
                    Split(right, right_split_min, right_split) => update_branch!(
                        branch,
                        left,
                        left_split1_min,
                        left_split1,
                        right_min,
                        right,
                        right_split_min,
                        right_split
                    ),
                    Merge(right_orphan) => {
                        match left_split1.merge_right(config, right_min, right_orphan) {
                            MergeResult::Done(left_split1) => {
                                update_branch!(branch, left, left_split1_min, left_split1)
                            }
                            MergeResult::Split(left_split1, left_split2_min, left_split2) => {
                                update_branch!(
                                    branch,
                                    left,
                                    left_split1_min,
                                    left_split1,
                                    left_split2_min,
                                    left_split2
                                )
                            }
                        }
                    }
                }
            }
            Merge(left_orphan) => match right.update(config, right_batch) {
                Done(right) => match right.merge_left(config, left_orphan, right_min) {
                    MergeResult::Done(right) => update_branch!(branch, right),
                    MergeResult::Split(right, right_split_min, right_split) => {
                        update_branch!(branch, right, right_split_min, right_split)
                    }
                },
                Split(right, right_split_min, right_split) => {
                    update_branch!(branch, right, right_split_min, right_split)
                }
                Merge(right_orphan) => {
                    update_branch!(branch, fuse2(config, left_orphan, right_min, right_orphan))
                }
            },
        };
    } else {
        panic!("Not a binary node!");
    }
}

/*
update_subtree!

([finalized...], (child0, batch0), (child1, batch1), ...) => {

match child0 {
    Done(child0) => {}
    Split(child0, child0_split_min, child0_split) => {}
    Merge(orphan0) => {}
}
}

*/

pub fn update_ternary_node(
    config: &TreeConfig,
    mut branch: Box<Node>,
    batch: Vec<Update>,
) -> UpdateResult {
    use Orphan::Child;
    use UpdateResult::{Done, Merge, Split};

    if let Node::Ternary {
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

        match left.update(config, left_batch) {
            Done(left) => match middle.update(config, middle_batch) {
                Done(middle) => match right.update(config, right_batch) {
                    Done(right) => {
                        update_branch!(branch, left, middle_min, middle, right_min, right)
                    }
                    Split(right, right_split1_min, right_split1) => update_branch!(
                        branch,
                        left,
                        middle_min,
                        middle,
                        right_min,
                        right,
                        right_split1_min,
                        right_split1
                    ),
                    Merge(right_orphan) => {
                        match middle.merge_right(config, right_min, right_orphan) {
                            MergeResult::Done(middle) => {
                                update_branch!(branch, left, middle_min, middle)
                            }
                            MergeResult::Split(middle, middle_split0_min, middle_split0) => {
                                update_branch!(
                                    branch,
                                    left,
                                    middle_min,
                                    middle,
                                    middle_split0_min,
                                    middle_split0
                                )
                            }
                        }
                    }
                },
                Split(middle, middle_split1_min, middle_split1) => {
                    match right.update(config, right_batch) {
                        Done(right) => update_branch!(
                            branch,
                            left,
                            middle_min,
                            middle,
                            middle_split1_min,
                            middle_split1,
                            right_min,
                            right
                        ),
                        Split(right, right_split1_min, right_split1) => update_branch!(
                            branch,
                            left,
                            middle_min,
                            middle,
                            middle_split1_min,
                            middle_split1,
                            right_min,
                            right,
                            right_split1_min,
                            right_split1
                        ),
                        Merge(right_orphan) => {
                            match middle_split1.merge_right(config, right_min, right_orphan) {
                                MergeResult::Done(middle_split1) => update_branch!(
                                    branch,
                                    left,
                                    middle_min,
                                    middle,
                                    middle_split1_min,
                                    middle_split1
                                ),
                                MergeResult::Split(
                                    middle_split1,
                                    middle_split2_min,
                                    middle_split2,
                                ) => update_branch!(
                                    branch,
                                    left,
                                    middle_min,
                                    middle,
                                    middle_split1_min,
                                    middle_split1,
                                    middle_split2_min,
                                    middle_split2
                                ),
                            }
                        }
                    }
                }
                Merge(middle_orphan) => match right.update(config, right_batch) {
                    Done(right) => match right.merge_left(config, middle_orphan, right_min) {
                        MergeResult::Done(right) => update_branch!(branch, left, right_min, right),
                        MergeResult::Split(right, right_split_min, right_split) => update_branch!(
                            branch,
                            left,
                            right_min,
                            right,
                            right_split_min,
                            right_split
                        ),
                    },
                    Split(right, right_split_min, right_split) => {
                        match left.merge_right(config, middle_min, middle_orphan) {
                            MergeResult::Done(left) => update_branch!(
                                branch,
                                left,
                                right_min,
                                right,
                                right_split_min,
                                right_split
                            ),
                            MergeResult::Split(left, left_split_min, left_split) => update_branch!(
                                branch,
                                left,
                                left_split_min,
                                left_split,
                                right_min,
                                right,
                                right_split_min,
                                right_split
                            ),
                        }
                    }
                    Merge(right_orphan) => update_branch!(
                        branch,
                        left,
                        middle_min,
                        fuse2(config, middle_orphan, right_min, right_orphan)
                    ),
                },
            },
            Split(left, left_split_min, left_split) => match middle.update(config, middle_batch) {
                Done(middle) => match right.update(config, right_batch) {
                    Done(right) => update_branch!(
                        branch,
                        left,
                        left_split_min,
                        left_split,
                        middle_min,
                        middle,
                        right_min,
                        right
                    ),
                    Split(right, right_split_min, right_split) => update_branch!(
                        branch,
                        left,
                        left_split_min,
                        left_split,
                        middle_min,
                        middle,
                        right_min,
                        right,
                        right_split_min,
                        right_split
                    ),
                    Merge(right_orphan) => {
                        match middle.merge_right(config, right_min, right_orphan) {
                            MergeResult::Done(middle) => update_branch!(
                                branch,
                                left,
                                left_split_min,
                                left_split,
                                middle_min,
                                middle
                            ),
                            MergeResult::Split(middle, middle_split_min, middle_split) => {
                                update_branch!(
                                    branch,
                                    left,
                                    left_split_min,
                                    left_split,
                                    middle_min,
                                    middle,
                                    middle_split_min,
                                    middle_split
                                )
                            }
                        }
                    }
                },
                Split(middle, middle_split1_min, middle_split1) => {
                    match right.update(config, right_batch) {
                        Done(right) => update_branch!(
                            branch,
                            left,
                            left_split_min,
                            left_split,
                            middle_min,
                            middle,
                            middle_split1_min,
                            middle_split1,
                            right_min,
                            right
                        ),
                        Split(right, right_split_min, right_split) => update_branch!(
                            branch,
                            left,
                            left_split_min,
                            left_split,
                            middle_min,
                            middle,
                            middle_split1_min,
                            middle_split1,
                            right_min,
                            right,
                            right_split_min,
                            right_split
                        ),
                        Merge(right_orphan) => {
                            match middle_split1.merge_right(config, right_min, right_orphan) {
                                MergeResult::Done(middle_split1) => update_branch!(
                                    branch,
                                    left,
                                    left_split_min,
                                    left_split,
                                    middle_min,
                                    middle,
                                    middle_split1_min,
                                    middle_split1
                                ),
                                MergeResult::Split(
                                    middle_split1,
                                    middle_split2_min,
                                    middle_split2,
                                ) => update_branch!(
                                    branch,
                                    left,
                                    left_split_min,
                                    left_split,
                                    middle_min,
                                    middle,
                                    middle_split1_min,
                                    middle_split1,
                                    middle_split2_min,
                                    middle_split2
                                ),
                            }
                        }
                    }
                }
                Merge(middle_orphan) => match right.update(config, right_batch) {
                    Done(right) => match right.merge_left(config, middle_orphan, right_min) {
                        MergeResult::Done(right) => update_branch!(
                            branch,
                            left,
                            left_split_min,
                            left_split,
                            right_min,
                            right
                        ),
                        MergeResult::Split(right, right_split_min, right_split) => update_branch!(
                            branch,
                            left,
                            left_split_min,
                            left_split,
                            right_min,
                            right,
                            right_split_min,
                            right_split
                        ),
                    },
                    Split(right, right_split1_min, right_split1) => {
                        match right.merge_left(config, middle_orphan, right_min) {
                            MergeResult::Done(right) => update_branch!(
                                branch,
                                left,
                                left_split_min,
                                left_split,
                                right_min,
                                right,
                                right_split1_min,
                                right_split1
                            ),
                            MergeResult::Split(right, right_split0_min, right_split0) => {
                                update_branch!(
                                    branch,
                                    left,
                                    left_split_min,
                                    left_split,
                                    right_min,
                                    right,
                                    right_split0_min,
                                    right_split0,
                                    right_split1_min,
                                    right_split1
                                )
                            }
                        }
                    }
                    Merge(right_orphan) => update_branch!(
                        branch,
                        left,
                        left_split_min,
                        left_split,
                        middle_min,
                        fuse2(config, middle_orphan, right_min, right_orphan)
                    ),
                },
            },
            Merge(left_orphan) => match middle.update(config, middle_batch) {
                Done(middle) => match right.update(config, right_batch) {
                    Done(right) => match middle.merge_left(config, left_orphan, middle_min) {
                        MergeResult::Done(middle) => {
                            update_branch!(branch, middle, right_min, right)
                        }
                        MergeResult::Split(middle, middle_split_min, middle_split) => {
                            update_branch!(
                                branch,
                                middle,
                                middle_split_min,
                                middle_split,
                                right_min,
                                right
                            )
                        }
                    },
                    Split(right, right_split_min, right_split) => {
                        match middle.merge_left(config, left_orphan, middle_min) {
                            MergeResult::Done(middle) => update_branch!(
                                branch,
                                middle,
                                right_min,
                                right,
                                right_split_min,
                                right_split
                            ),
                            MergeResult::Split(middle, middle_split_min, middle_split) => {
                                update_branch!(
                                    branch,
                                    middle,
                                    middle_split_min,
                                    middle_split,
                                    right_min,
                                    right,
                                    right_split_min,
                                    right_split
                                )
                            }
                        }
                    }
                    Merge(right_orphan) => match middle.merge_left(config, left_orphan, middle_min)
                    {
                        MergeResult::Done(middle) => {
                            match middle.merge_right(config, right_min, right_orphan) {
                                MergeResult::Done(middle) => {
                                    // TODO - is this even possible?
                                    update_branch!(branch, middle)
                                }
                                MergeResult::Split(middle, middle_split_min, middle_split) => {
                                    update_branch!(branch, middle, middle_split_min, middle_split)
                                }
                            }
                        }
                        MergeResult::Split(middle, middle_split1_min, middle_split1) => {
                            match middle_split1.merge_right(config, right_min, right_orphan) {
                                MergeResult::Done(middle_split1) => {
                                    update_branch!(branch, middle, middle_split1_min, middle_split1)
                                }
                                MergeResult::Split(
                                    middle_split1,
                                    middle_split2_min,
                                    middle_split2,
                                ) => update_branch!(
                                    branch,
                                    middle,
                                    middle_split1_min,
                                    middle_split1,
                                    middle_split2_min,
                                    middle_split2
                                ),
                            }
                        }
                    },
                },
                Split(middle, middle_split1_min, middle_split1) => {
                    match right.update(config, right_batch) {
                        Done(right) => match middle.merge_left(config, left_orphan, middle_min) {
                            MergeResult::Done(middle) => update_branch!(
                                branch,
                                middle,
                                middle_split1_min,
                                middle_split1,
                                right_min,
                                right
                            ),
                            MergeResult::Split(middle, middle_split0_min, middle_split0) => {
                                update_branch!(
                                    branch,
                                    middle,
                                    middle_split0_min,
                                    middle_split0,
                                    middle_split1_min,
                                    middle_split1,
                                    right_min,
                                    right
                                )
                            }
                        },
                        Split(right, right_split_min, right_split) => {
                            match middle.merge_left(config, left_orphan, middle_min) {
                                MergeResult::Done(middle) => update_branch!(
                                    branch,
                                    middle,
                                    middle_split1_min,
                                    middle_split1,
                                    right_min,
                                    right,
                                    right_split_min,
                                    right_split
                                ),
                                MergeResult::Split(middle, middle_split0_min, middle_split0) => {
                                    update_branch!(
                                        branch,
                                        middle,
                                        middle_split0_min,
                                        middle_split0,
                                        middle_split1_min,
                                        middle_split1,
                                        right_min,
                                        right,
                                        right_split_min,
                                        right_split
                                    )
                                }
                            }
                        }
                        Merge(right_orphan) => {
                            match middle.merge_left(config, left_orphan, middle_min) {
                                MergeResult::Done(middle) => {
                                    match middle_split1.merge_right(config, right_min, right_orphan)
                                    {
                                        MergeResult::Done(middle_split1) => update_branch!(
                                            branch,
                                            middle,
                                            middle_split1_min,
                                            middle_split1
                                        ),
                                        MergeResult::Split(
                                            middle_split1,
                                            middle_split2_min,
                                            middle_split2,
                                        ) => update_branch!(
                                            branch,
                                            middle,
                                            middle_split1_min,
                                            middle_split1,
                                            middle_split2_min,
                                            middle_split2
                                        ),
                                    }
                                }
                                MergeResult::Split(middle, middle_split0_min, middle_split0) => {
                                    match middle_split1.merge_right(config, right_min, right_orphan)
                                    {
                                        MergeResult::Done(middle_split1) => update_branch!(
                                            branch,
                                            middle,
                                            middle_split0_min,
                                            middle_split0,
                                            middle_split1_min,
                                            middle_split1
                                        ),
                                        MergeResult::Split(
                                            middle_split1,
                                            middle_split2_min,
                                            middle_split2,
                                        ) => update_branch!(
                                            branch,
                                            middle,
                                            middle_split0_min,
                                            middle_split0,
                                            middle_split1_min,
                                            middle_split1,
                                            middle_split2_min,
                                            middle_split2
                                        ),
                                    }
                                }
                            }
                        }
                    }
                }
                Merge(middle_orphan) => match right.update(config, right_batch) {
                    Done(right) => update_branch!(
                        branch,
                        fuse2(config, left_orphan, middle_min, middle_orphan),
                        right_min,
                        right
                    ),
                    Split(right, right_split_min, right_split) => update_branch!(
                        branch,
                        fuse2(config, left_orphan, middle_min, middle_orphan),
                        right_min,
                        right,
                        right_split_min,
                        right_split
                    ),
                    Merge(right_orphan) => match fuse3(
                        config,
                        left_orphan,
                        middle_min,
                        middle_orphan,
                        right_min,
                        right_orphan,
                    ) {
                        MergeResult::Done(fused_orphan) => update_branch!(branch, fused_orphan),
                        MergeResult::Split(fused_left, fused_right_min, fused_right) => {
                            update_branch!(branch, fused_left, fused_right_min, fused_right)
                        }
                    },
                },
            },
        }
    } else {
        panic!("Not a ternary node!");
    }
}

/*
    fn merge_left(&mut self, config: &TreeConfig, subtree: Subtree, left_min: i32) -> MergeResult {
        use MergeResult::{Done, Split};
        use Subtree::{Branch, Leaf};

        match (subtree, std::mem::replace(self, Nil)) {
            (
                Leaf {
                    vals: mut subtree_vals,
                },
                Leaf { vals },
            ) => {
                assert!(subtree_vals.len() < config.batch_size);

                subtree_vals.append(&mut vals);
                let mut merged = subtree_vals;

                assert!(merged.len() < config.batch_size * 3);
                assert!(merged.len() >= config.batch_size);

                if merged.len() <= config.batch_size * 2 {
                    *self = Leaf { vals: merged };
                    return Done;
                } else {
                    let split_vals: Vec<i32> = merged.drain((merged.len() / 2)..).collect();
                    let split_min: i32 = split_vals[0];
                    *self = Leaf { vals: merged };
                    return Split(split_min, Leaf { vals: split_vals });
                }
            }
            (Branch(mut subtree_branch), Branch(mut branch)) => match (*subtree_branch, *branch) {},
            Branch(mut box_node) => match *box_node {
                Node::Binary {
                    left,
                    right_min,
                    right,
                } => {
                    *box_node = Node::Ternary {
                        left: subtree,
                        middle_min: left_min,
                        middle: left,
                        right_min,
                        right,
                    };
                    return Done;
                }
                Node::Ternary {
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => {
                    let result = Split(
                        middle_min,
                        Branch(Box::new(Node::Binary {
                            left: middle,
                            right_min,
                            right,
                        })),
                    );
                    *box_node = Node::Binary {
                        left: subtree,
                        right_min: left_min,
                        right: left,
                    };
                    return result;
                }
                Node::Nullary => Done,
            },
            _ => panic!("Merging a subtree with Nil does not produce a valid subtree!"),
        }
    }

    fn merge_right(
        &mut self,
        config: &TreeConfig,
        subtree_min: i32,
        subtree: Subtree,
    ) -> MergeResult {
        use MergeResult::{Done, Split};
        use Subtree::{Branch, Leaf, Nil};

        match std::mem::replace(self, Nil) {
            Nil => panic!("Merging a subtree with Nil does not produce a valid subtree!"),
            Leaf { mut vals } => {
                if let Leaf {
                    vals: mut subtree_vals,
                } = subtree
                {
                    assert!(subtree_vals.len() < config.batch_size);

                    vals.append(&mut subtree_vals);
                    let mut merged = vals;

                    assert!(merged.len() <= config.batch_size * 3);
                    assert!(merged.len() >= config.batch_size);

                    if merged.len() <= config.batch_size * 2 {
                        *self = Leaf { vals: merged };
                        return Done;
                    } else {
                        let split_vals: Vec<i32> = merged.drain((merged.len() / 2)..).collect();
                        let split_min: i32 = split_vals[0];
                        *self = Leaf { vals: merged };
                        return Split(split_min, Leaf { vals: split_vals });
                    }
                } else {
                    panic!("Tried to merge a leaf with a non-leaf!");
                }
            }
            Branch(mut box_node) => match *box_node {
                Node::Binary {
                    left,
                    right_min,
                    right,
                } => {
                    *box_node = Node::Ternary {
                        left,
                        middle_min: right_min,
                        middle: right,
                        right_min: subtree_min,
                        right: subtree,
                    };
                    return Done;
                }
                Node::Ternary {
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => {
                    let result = Split(
                        right_min,
                        Branch(Box::new(Node::Binary {
                            left: right,
                            right_min: subtree_min,
                            right: subtree,
                        })),
                    );
                    *box_node = Node::Binary {
                        left,
                        right_min: middle_min,
                        right: middle,
                    };
                    return result;
                }
                Node::Nullary => Done,
            },
        }
    }
}

impl Node {
    fn update(&mut self, &config: TreeConfig, batch: Vec<Update>) -> UpdateResult {
        assert_eq!(batch.len(), config.batch_size);

        match std::mem::replace(self, Nil) {
            Inner2 {
                mut left,
                right_min,
                mut right,
            } => {
                let (node, result) = insert_inner2(new_val, left, right_min, right);
                *self = node;
                return result;
            }

            Inner3 {
                mut left,
                middle_min,
                mut middle,
                right_min,
                mut right,
            } => {
                let (node, result) =
                    insert_inner3(new_val, left, middle_min, middle, right_min, right);
                *self = node;
                return result;
            }
        }
    }

    fn remove(&mut self, rm_val: i32) -> RemoveResult {
        use RemoveResult::{Drained, NotFound, Ok, Orphaned};
        match std::mem::replace(self, Nil) {
            Nil => NotFound,
            Leaf2 { val } => {
                if rm_val == val {
                    // self stays Nil
                    Drained
                } else {
                    *self = Leaf2 { val };
                    NotFound
                }
            }
            Leaf3 { val1, val2 } => {
                if rm_val == val1 {
                    *self = Leaf2 { val: val2 };
                    Ok
                } else if rm_val == val2 {
                    *self = Leaf2 { val: val1 };
                    Ok
                } else {
                    *self = Leaf3 { val1, val2 };
                    NotFound
                }
            }
            Inner2 {
                mut left,
                right_min,
                mut right,
            } => {
                let result = if rm_val < right_min {
                    match left.remove(rm_val) {
                        NotFound => NotFound,
                        Ok => Ok,
                        Drained => {
                            return Orphaned(right);
                        }
                        Orphaned(to_merge) => match right.merge_left(to_merge, right_min) {
                            InsertResult::Ok => {
                                return Orphaned(right);
                            }
                            InsertResult::Split(split_min, split) => {
                                *self = Inner2 {
                                    left: right,
                                    right_min: split_min,
                                    right: split,
                                };
                                return Ok;
                            }
                        },
                    }
                } else {
                    match right.remove(rm_val) {
                        NotFound => NotFound,
                        Ok => Ok,
                        Drained => {
                            return Orphaned(left);
                        }
                        Orphaned(to_merge) => match left.merge_right(right_min, to_merge) {
                            InsertResult::Ok => {
                                return Orphaned(left);
                            }
                            InsertResult::Split(split_min, split) => {
                                *self = Inner2 {
                                    left,
                                    right_min: split_min,
                                    right: split,
                                };
                                return Ok;
                            }
                        },
                    }
                };
                *self = Inner2 {
                    left,
                    right_min,
                    right,
                };
                return result;
            }
            Inner3 {
                mut left,
                middle_min,
                mut middle,
                right_min,
                mut right,
            } => {
                let result = if rm_val < middle_min {
                    match left.remove(rm_val) {
                        Ok => Ok,
                        NotFound => NotFound,
                        Drained => {
                            *self = Inner2 {
                                left: middle,
                                right_min,
                                right,
                            };
                            return Ok;
                        }
                        Orphaned(to_merge) => {
                            match middle.merge_left(to_merge, middle_min) {
                                InsertResult::Ok => {
                                    *self = Inner2 {
                                        left: middle,
                                        right_min,
                                        right,
                                    };
                                }
                                InsertResult::Split(split_min, split) => {
                                    *self = Inner3 {
                                        left: middle,
                                        middle_min: split_min,
                                        middle: split,
                                        right_min,
                                        right,
                                    };
                                }
                            }
                            return Ok;
                        }
                    }
                } else if rm_val < right_min {
                    match middle.remove(rm_val) {
                        Ok => Ok,
                        NotFound => NotFound,
                        Drained => {
                            *self = Inner2 {
                                left,
                                right_min,
                                right,
                            };
                            return Ok;
                        }
                        Orphaned(to_merge) => {
                            match right.merge_left(to_merge, right_min) {
                                InsertResult::Ok => {
                                    *self = Inner2 {
                                        left,
                                        right_min,
                                        right,
                                    };
                                }
                                InsertResult::Split(split_min, split) => {
                                    *self = Inner3 {
                                        left,
                                        middle_min: right_min,
                                        middle: right,
                                        right_min: split_min,
                                        right: split,
                                    };
                                }
                            }
                            return Ok;
                        }
                    }
                } else {
                    match right.remove(rm_val) {
                        Ok => Ok,
                        NotFound => NotFound,
                        Drained => {
                            *self = Inner2 {
                                left,
                                right_min: middle_min,
                                right: middle,
                            };
                            return Ok;
                        }
                        Orphaned(to_merge) => {
                            match middle.merge_right(right_min, to_merge) {
                                InsertResult::Ok => {
                                    *self = Inner2 {
                                        left,
                                        right_min: middle_min,
                                        right: middle,
                                    };
                                }
                                InsertResult::Split(split_min, split) => {
                                    *self = Inner3 {
                                        left,
                                        middle_min,
                                        middle,
                                        right_min: split_min,
                                        right: split,
                                    };
                                }
                            }
                            return Ok;
                        }
                    }
                };
                *self = Inner3 {
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                };
                return result;
            }
        }
    }

    // Merges subtree as a child on the left side of this node; may result in a split.
    //
    fn merge_left(&mut self, subtree: Box<Node>, left_min: i32) -> InsertResult {
        let node = std::mem::replace(self, Nil);
        if let Inner2 {
            left,
            right_min,
            right,
        } = node
        {
            *self = Inner3 {
                left: subtree,
                middle_min: left_min,
                middle: left,
                right_min,
                right,
            };
            return InsertResult::Ok;
        }
        if let Inner3 {
            left,
            middle_min,
            middle,
            right_min,
            right,
        } = node
        {
            *self = Inner2 {
                left: subtree,
                right_min: left_min,
                right: left,
            };
            return InsertResult::Split(
                middle_min,
                Box::new(Inner2 {
                    left: middle,
                    right_min,
                    right,
                }),
            );
        }
        panic!("insert_subtree may only be called on an inner node!")
    }

    fn merge_right(&mut self, subtree_min: i32, subtree: Box<Node>) -> InsertResult {
        let node = std::mem::replace(self, Nil);
        if let Inner2 {
            left,
            right_min,
            right,
        } = node
        {
            *self = Inner3 {
                left,
                middle_min: right_min,
                middle: right,
                right_min: subtree_min,
                right: subtree,
            };
            return InsertResult::Ok;
        }
        if let Inner3 {
            left,
            middle_min,
            middle,
            right_min,
            right,
        } = node
        {
            *self = Inner2 {
                left,
                right_min: middle_min,
                right: middle,
            };
            return InsertResult::Split(
                right_min,
                Box::new(Inner2 {
                    left: right,
                    right_min: subtree_min,
                    right: subtree,
                }),
            );
        }
        panic!("insert_subtree may only be called on an inner node!")
    }

    fn find<'a>(&'a self, key: i32) -> Option<&'a i32> {
        match self {
            Nil => None,

            Leaf2 { val } => {
                if key == *val {
                    Some(val)
                } else {
                    None
                }
            }

            Leaf3 { val1, val2 } => {
                if key == *val1 {
                    Some(val1)
                } else if key == *val2 {
                    Some(val2)
                } else {
                    None
                }
            }

            Inner2 {
                left,
                right_min,
                right,
            } => {
                if key < *right_min {
                    left.find(key)
                } else {
                    right.find(key)
                }
            }

            Inner3 {
                left,
                middle_min,
                middle,
                right_min,
                right,
            } => {
                if key < *middle_min {
                    left.find(key)
                } else if key < *right_min {
                    middle.find(key)
                } else {
                    right.find(key)
                }
            }
        }
    }

    fn height(&self) -> usize {
        match self {
            Nil => 0,
            Leaf2 { .. } => 1,
            Leaf3 { .. } => 1,
            Inner2 { left, .. } => left.height() + 1,
            Inner3 { left, .. } => left.height() + 1,
        }
    }
}

#[derive(Debug)]
struct Tree {
    root: Box<Node>,
}

impl Tree {
    fn new() -> Self {
        Self {
            root: Box::new(Nil),
        }
    }
    fn insert(&mut self, val: i32) {
        match self.root.insert(val) {
            InsertResult::Ok => {}
            InsertResult::Split(split_min, split) => {
                let tmp = std::mem::replace(&mut self.root, Box::new(Nil));
                self.root = Box::new(Inner2 {
                    left: tmp,
                    right_min: split_min,
                    right: split,
                });
            }
        }
    }
    fn remove(&mut self, val: i32) {
        use RemoveResult::{Drained, NotFound, Ok, Orphaned};
        match self.root.remove(val) {
            NotFound => {
                println!("remove => NotFound");
            }
            Ok => {
                println!("remove => Ok");
            }
            Drained => {
                std::mem::replace(&mut self.root, Box::new(Nil));
                println!("remove => Drained");
            }
            Orphaned(new_root) => {
                std::mem::replace(&mut self.root, new_root);
                println!("remove => Orphaned");
            }
        }
    }
    fn find<'a>(&'a self, val: i32) -> Option<&'a i32> {
        self.root.find(val)
    }
    fn height(&self) -> usize {
        self.root.height()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_test() {
        assert_eq!(2 + 2, 4);

        let mut t = Tree::new();

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

        assert_eq!(t.height(), 10);

        for k in 1000..100000 {
            t.insert(k);
        }

        for k in 1000..100000 {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 17);
    }

    #[test]
    fn remove_test() {
        let mut t = Tree::new();

        for k in 0..100000 {
            t.insert(k);
        }

        for k in 0..100000 {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 17);

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
}
*/
