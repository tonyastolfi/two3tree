use crate::node::{Node, Subtree, TreeConfig};

use itertools::Itertools;

pub enum MergeResult {
    Done,
    Split(i32, Subtree),
}

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

pub enum UpdateResult {
    Done,
    Split(i32, Subtree),
    Merge(Subtree),
}

use Subtree::{Branch, Leaf, Nil};

pub fn update_leaf(
    config: &TreeConfig,
    batch: Vec<Update>,
    vals: Vec<i32>,
) -> (Subtree, UpdateResult) {
    use itertools::EitherOrBoth::{Both, Left, Right};
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
        return (Nil, Merge(Leaf { vals: merged }));
    } else if merged.len() <= config.batch_size * 2 {
        return (Leaf { vals: merged }, Done);
    }

    let split_vals: Vec<i32> = merged.drain((merged.len() / 2)..).collect();
    let split_min: i32 = split_vals[0];
    return (
        Leaf { vals: merged },
        Split(split_min, Leaf { vals: split_vals }),
    );
}

pub fn update_binary_node(
    config: &TreeConfig,
    mut batch: Vec<Update>,
    mut left: Subtree,
    right_min: i32,
    mut right: Subtree,
) -> (Node, UpdateResult) {
    use Subtree::{Branch, Leaf, Nil};
    use UpdateResult::{Done, Merge, Split};

    if batch.is_empty() {
        return (
            Node::Binary {
                left,
                right_min,
                right,
            },
            Done,
        );
    }

    let (left_batch, right_batch): (Vec<Update>, Vec<Update>) = batch
        .drain(..)
        .partition(|update| update.key() < &right_min);

    match left.update(config, left_batch) {
        Split(left_split_min, mut left_split) => match right.update(config, right_batch) {
            Split(right_split_min, right_split) => {
                // left: split, right: split
                return (
                    Node::Binary {
                        left,
                        right_min: left_split_min,
                        right: left_split,
                    },
                    Split(
                        right_min,
                        Branch(Box::new(Node::Binary {
                            left: right,
                            right_min: right_split_min,
                            right: right_split,
                        })),
                    ),
                );
            }
            Merge(right_orphan) => {
                // left: split, right: merge
                match left_split.merge_right(config, right_min, right_orphan) {
                    MergeResult::Done => {
                        return (
                            Node::Binary {
                                left,
                                right_min: left_split_min,
                                right: left_split,
                            },
                            Done,
                        );
                    }
                    MergeResult::Split(left_split2_min, left_split2) => {
                        return (
                            Node::Ternary {
                                left,
                                middle_min: left_split_min,
                                middle: left_split,
                                right_min: left_split2_min,
                                right: left_split2,
                            },
                            Done,
                        );
                    }
                }
            }
            Done => {
                // left: split, right: done
                return (
                    Node::Ternary {
                        left,
                        middle_min: left_split_min,
                        middle: left_split,
                        right_min,
                        right,
                    },
                    Done,
                );
            }
        },
        Merge(left_orphan) => (Node::Nullary, Done),
        /* match right.update(config, right_batch) {
            Split(right_split_min, right_split) => {
                // left: merge, right: split
                match right.merge_left(left_merge, right_min) {
                    Merge::Done => {
                        return (Node::Binary{ left: right, right_min: right_split_min, right: right_split}, Done);}
                    Merge::Split(right_split0_min, right_split0) => {
                        return                        (Node::Ternary{ left: right, middle_min: right_split0_min, middle: right_split0, right_min: right_split_min, right: right_split}, Done);}
                }
            }
            Merge(right_merge) => {
                // left: merge, right: merge
            }
            Done => {
                // left: merge, right: done
            }
        },*/
        Done => match right.update(config, right_batch) {
            Split(right_split_min, right_split) => {
                // left: done, right: split
                return (
                    Node::Ternary {
                        left,
                        middle_min: right_min,
                        middle: right,
                        right_min: right_split_min,
                        right: right_split,
                    },
                    Done,
                );
            }
            Merge(right_orphan) =>
            // left: done, right: merge
            {
                match left.merge_right(config, right_min, right_orphan) {
                    MergeResult::Done => (
                        Node::Binary {
                            left,
                            right_min,
                            right,
                        },
                        Done,
                    ),
                    MergeResult::Split(split_min, split) => (
                        Node::Binary {
                            left,
                            right_min: split_min,
                            right: split,
                        },
                        Done,
                    ),
                }
            }
            Done =>
            // left: done, right: done
            {
                (
                    Node::Binary {
                        left,
                        right_min,
                        right,
                    },
                    Done,
                )
            }
        },
    }
}

/*

pub fn update_ternary_node(
    config: &TreeConfig,
    batch: Vec<Update>,
    mut left: Subtree<Node>,
    middle_min: i32,
    mut middle: Subtree<Node>,
    right_min: i32,
    mut right: Subtree<Node>,
) -> (Node, InsertResult) {
    if batch.is_empty() {
        return (
            Node::Ternary {
                left,
                middle_min,
                middle,
                right_min,
                right,
            },
            Ok,
        );
    }

    let (left_batch, non_left_batch): (Vec<Update>, Vec<Update>) =
        batch.drain().partition(|update| update.key() < middle_min);

    let (middle_batch, right_batch): (Vec<Update>, Vec<Update>) = non_left_batch
        .drain()
        .partition(|update| update.key() < right_min);

    if let Split(left_split_min, left_split) = left.update(config, left_batch) {
        if let Split(middle_split_min, middle_split) = middle.update(config, middle_batch) {
            if let Split(right_split_min, right_split) = right.update(config, right_batch) {
                return (
                    Node::Ternary {
                        left: left,
                        middle_min: left_split_min,
                        middle: left_split,
                        right_min: middle_min,
                        right: middle,
                    },
                    Split(
                        middle_split_min,
                        Node::Ternary {
                            left: middle_split,
                            middle_min: right_min,
                            middle: right,
                            right_min: right_split_min,
                            right: right_split,
                        },
                    ),
                );
            } else {
                return (
                    Node::Ternary {
                        left: left,
                        middle_min: left_split_min,
                        middle: left_split,
                        right_min: middle_min,
                        right: middle,
                    },
                    Split(
                        middle_split_min,
                        Node::Binary {
                            left: middle_split,
                            right_min,
                            right,
                        },
                    ),
                );
            }
        } else {
            if let Split(right_split_min, right_split) = right.update(config, right_batch) {
            } else {
            }
        }
    } else {
        if let Split(middle_split_min, middle_split) = middle.update(config, middle_batch) {
            if let Split(right_split_min, right_split) = right.update(config, right_batch) {
            } else {
            }
        } else {
            if let Split(right_split_min, right_split) = right.update(config, right_batch) {
            } else {
                return (
                    Node::Ternary {
                        left,
                        middle_min,
                        middle,
                        right_min,
                        right,
                    },
                    Ok,
                );
            }
        }
    }
}
*/
