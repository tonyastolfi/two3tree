use crate::node::{Node, Subtree, TreeConfig};

use itertools::Itertools;

pub trait BatchUpdate {
    fn update(&mut self, config: &TreeConfig, batch: Vec<Update>) -> UpdateResult;
}

use Subtree::{Branch, Leaf};

macro_rules! split {
    [$min:expr, $($x:expr),*] => {
        UpdateResult::Split($min, make_branch![$($x),*])
    };
}

pub fn update_leaf(config: &TreeConfig, batch: Vec<Update>, vals: Vec<i32>) -> UpdateResult {
    use itertools::EitherOrBoth::{Both, Left, Right};
    use Orphan::Items;
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
    batch: Vec<Update>,
    left: Subtree,
    right_min: i32,
    right: Subtree,
) -> (Node, UpdateResult) {
}

pub fn update_ternary_node(
    config: &TreeConfig,
    batch: Vec<Update>,
    mut left: Subtree,
    middle_min: i32,
    mut middle: Subtree,
    right_min: i32,
    mut right: Subtree,
) -> (Node, UpdateResult) {
    use UpdateResult::{Done, Merge, Split};

    if batch.is_empty() {
        return updated![left, middle_min, middle, right_min, right];
    }

    let (left_batch, non_left_batch): (Vec<Update>, Vec<Update>) = batch
        .into_iter()
        .partition(|update| update.key() < &middle_min);

    let (middle_batch, right_batch): (Vec<Update>, Vec<Update>) = non_left_batch
        .into_iter()
        .partition(|update| update.key() < &right_min);

    match left.update(config, left_batch) {
        Done => match middle.update(config, middle_batch) {
            Done => match right.update(config, right_batch) {
                Done => updated![left, middle_min, middle, right_min, right],
                Split(right_split1_min, right_split1) => updated![
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                    right_split1_min,
                    right_split1
                ],
                Merge(right_orphan) => match middle.merge_right(config, right_min, right_orphan) {
                    MergeResult::Done => updated![left, middle_min, middle],
                    MergeResult::Split(middle_split0_min, middle_split0) => {
                        updated![left, middle_min, middle, middle_split0_min, middle_split0]
                    }
                },
            },
            Split(middle_split1_min, mut middle_split1) => {
                match right.update(config, right_batch) {
                    Done => updated![
                        left,
                        middle_min,
                        middle,
                        middle_split1_min,
                        middle_split1,
                        right_min,
                        right
                    ],
                    Split(right_split1_min, right_split1) => updated![
                        left,
                        middle_min,
                        middle,
                        middle_split1_min,
                        middle_split1,
                        right_min,
                        right,
                        right_split1_min,
                        right_split1
                    ],
                    Merge(right_orphan) => {
                        match middle_split1.merge_right(config, right_min, right_orphan) {
                            MergeResult::Done => {
                                updated![left, middle_min, middle, middle_split1_min, middle_split1]
                            }
                            MergeResult::Split(middle_split2_min, middle_split2) => updated![
                                left,
                                middle_min,
                                middle,
                                middle_split1_min,
                                middle_split1,
                                middle_split2_min,
                                middle_split2
                            ],
                        }
                    }
                }
            }
            Merge(middle_orphan) => match right.update(config, right_batch) {
                Done => match right.merge_left(config, middle_orphan, right_min) {
                    MergeResult::Done => updated![left, right_min, right],
                    MergeResult::Split(right_split_min, right_split) => {
                        updated![left, right_min, right, right_split_min, right_split]
                    }
                },
                Split(right_split_min, right_split) => {
                    match left.merge_right(config, middle_min, middle_orphan) {
                        MergeResult::Done => {
                            updated![left, right_min, right, right_split_min, right_split]
                        }
                        MergeResult::Split(left_split_min, left_split) => updated![
                            left,
                            left_split_min,
                            left_split,
                            right_min,
                            right,
                            right_split_min,
                            right_split
                        ],
                    }
                }
                Merge(right_orphan) => updated![
                    left,
                    middle_min,
                    make_branch![middle_orphan, right_min, right_orphan]
                ],
            },
        },
        Split(left_split_min, left_split) => match middle.update(config, middle_batch) {
            Done => match right.update(config, right_batch) {
                Done => updated![
                    left,
                    left_split_min,
                    left_split,
                    middle_min,
                    middle,
                    right_min,
                    right
                ],
                Split(right_split_min, right_split) => updated![
                    left,
                    left_split_min,
                    left_split,
                    middle_min,
                    middle,
                    right_min,
                    right,
                    right_split_min,
                    right_split
                ],
                Merge(right_orphan) => match middle.merge_right(config, right_min, right_orphan) {
                    MergeResult::Done => {
                        updated![left, left_split_min, left_split, middle_min, middle]
                    }
                    MergeResult::Split(middle_split_min, middle_split) => updated![
                        left,
                        left_split_min,
                        left_split,
                        middle_min,
                        middle,
                        middle_split_min,
                        middle_split
                    ],
                },
            },
            Split(middle_split1_min, mut middle_split1) => {
                match right.update(config, right_batch) {
                    Done => updated![
                        left,
                        left_split_min,
                        left_split,
                        middle_min,
                        middle,
                        middle_split1_min,
                        middle_split1,
                        right_min,
                        right
                    ],
                    Split(right_split_min, right_split) => updated![
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
                    ],
                    Merge(right_orphan) => {
                        match middle_split1.merge_right(config, right_min, right_orphan) {
                            MergeResult::Done => updated![
                                left,
                                left_split_min,
                                left_split,
                                middle_min,
                                middle,
                                middle_split1_min,
                                middle_split1
                            ],
                            MergeResult::Split(middle_split2_min, middle_split2) => updated![
                                left,
                                left_split_min,
                                left_split,
                                middle_min,
                                middle,
                                middle_split1_min,
                                middle_split1,
                                middle_split2_min,
                                middle_split2
                            ],
                        }
                    }
                }
            }
            Merge(middle_orphan) => match right.update(config, right_batch) {
                Done => match right.merge_left(config, middle_orphan, right_min) {
                    MergeResult::Done => {
                        updated![left, left_split_min, left_split, right_min, right]
                    }
                    MergeResult::Split(right_split_min, right_split) => updated![
                        left,
                        left_split_min,
                        left_split,
                        right_min,
                        right,
                        right_split_min,
                        right_split
                    ],
                },
                Split(right_split1_min, right_split1) => {
                    match right.merge_left(config, middle_orphan, right_min) {
                        MergeResult::Done => updated![
                            left,
                            left_split_min,
                            left_split,
                            right_min,
                            right,
                            right_split1_min,
                            right_split1
                        ],
                        MergeResult::Split(right_split0_min, right_split0) => updated![
                            left,
                            left_split_min,
                            left_split,
                            right_min,
                            right,
                            right_split0_min,
                            right_split0,
                            right_split1_min,
                            right_split1
                        ],
                    }
                }
                Merge(right_orphan) => updated![
                    left,
                    left_split_min,
                    left_split,
                    middle_min,
                    make_branch![middle_orphan, right_min, right_orphan]
                ],
            },
        },
        Merge(left_orphan) => match middle.update(config, middle_batch) {
            Done => match right.update(config, right_batch) {
                Done => match middle.merge_left(config, left_orphan, middle_min) {
                    MergeResult::Done => updated![middle, right_min, right],
                    MergeResult::Split(middle_split_min, middle_split) => {
                        updated![middle, middle_split_min, middle_split, right_min, right]
                    }
                },
                Split(right_split_min, right_split) => {
                    match middle.merge_left(config, left_orphan, middle_min) {
                        MergeResult::Done => {
                            updated![middle, right_min, right, right_split_min, right_split]
                        }
                        MergeResult::Split(middle_split_min, middle_split) => updated![
                            middle,
                            middle_split_min,
                            middle_split,
                            right_min,
                            right,
                            right_split_min,
                            right_split
                        ],
                    }
                }
                Merge(right_orphan) => match middle.merge_left(config, left_orphan, middle_min) {
                    MergeResult::Done => {
                        match middle.merge_right(config, right_min, right_orphan) {
                            MergeResult::Done => merge_required(middle),
                            MergeResult::Split(middle_split_min, middle_split) => {
                                updated![middle, middle_split_min, middle_split]
                            }
                        }
                    }
                    MergeResult::Split(middle_split1_min, mut middle_split1) => {
                        match middle_split1.merge_right(config, right_min, right_orphan) {
                            MergeResult::Done => updated![middle, middle_split1_min, middle_split1],
                            MergeResult::Split(middle_split2_min, middle_split2) => updated![
                                middle,
                                middle_split1_min,
                                middle_split1,
                                middle_split2_min,
                                middle_split2
                            ],
                        }
                    }
                },
            },
            Split(middle_split1_min, mut middle_split1) => {
                match right.update(config, right_batch) {
                    Done => match middle.merge_left(config, left_orphan, middle_min) {
                        MergeResult::Done => {
                            updated![middle, middle_split1_min, middle_split1, right_min, right]
                        }
                        MergeResult::Split(middle_split0_min, middle_split0) => updated![
                            middle,
                            middle_split0_min,
                            middle_split0,
                            middle_split1_min,
                            middle_split1,
                            right_min,
                            right
                        ],
                    },
                    Split(right_split_min, right_split) => {
                        match middle.merge_left(config, left_orphan, middle_min) {
                            MergeResult::Done => updated![
                                middle,
                                middle_split1_min,
                                middle_split1,
                                right_min,
                                right,
                                right_split_min,
                                right_split
                            ],
                            MergeResult::Split(middle_split0_min, middle_split0) => updated![
                                middle,
                                middle_split0_min,
                                middle_split0,
                                middle_split1_min,
                                middle_split1,
                                right_min,
                                right,
                                right_split_min,
                                right_split
                            ],
                        }
                    }
                    Merge(right_orphan) => match middle.merge_left(config, left_orphan, middle_min)
                    {
                        MergeResult::Done => {
                            match middle_split1.merge_right(config, right_min, right_orphan) {
                                MergeResult::Done => {
                                    updated![middle, middle_split1_min, middle_split1]
                                }
                                MergeResult::Split(middle_split2_min, middle_split2) => updated![
                                    middle,
                                    middle_split1_min,
                                    middle_split1,
                                    middle_split2_min,
                                    middle_split2
                                ],
                            }
                        }
                        MergeResult::Split(middle_split0_min, middle_split0) => {
                            match middle_split1.merge_right(config, right_min, right_orphan) {
                                MergeResult::Done => updated![
                                    middle,
                                    middle_split0_min,
                                    middle_split0,
                                    middle_split1_min,
                                    middle_split1
                                ],
                                MergeResult::Split(middle_split2_min, middle_split2) => updated![
                                    middle,
                                    middle_split0_min,
                                    middle_split0,
                                    middle_split1_min,
                                    middle_split1,
                                    middle_split2_min,
                                    middle_split2
                                ],
                            }
                        }
                    },
                }
            }
            Merge(middle_orphan) => match right.update(config, right_batch) {
                Done => updated![
                    make_branch![left_orphan, middle_min, middle_orphan],
                    right_min,
                    right
                ],
                Split(right_split_min, right_split) => updated![
                    make_branch![left_orphan, middle_min, middle_orphan],
                    right_min,
                    right,
                    right_split_min,
                    right_split
                ],
                Merge(right_orphan) => merge_required(make_branch![
                    left_orphan,
                    middle_min,
                    middle_orphan,
                    right_min,
                    right_orphan
                ]),
            },
        },
    }
}
