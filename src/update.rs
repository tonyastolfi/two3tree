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

macro_rules! updated {
    [$child0:expr, $min1:expr, $child1:expr] => {
        (make_node![$child0, $min1, $child1], Done)
    };
    [$child0:expr, $min1:expr, $child1:expr, $min2:expr, $child2:expr] => {
        (make_node![$child0, $min1, $child1, $min2, $child2], Done)
    };
    [$child0:expr, $min1:expr, $child1:expr, $min2:expr, $child2:expr, $min3:expr, $child3:expr] => {
        (
            make_node![$child0, $min1, $child1],
            Split($min2, Branch(Box::new(make_node![$child2, $min3, $child3]))),
        )
    };
}

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
    batch: Vec<Update>,
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
        .into_iter()
        .partition(|update| update.key() < &right_min);

    match left.update(config, left_batch) {
        Split(left_split_min, mut left_split) => {
            //
            match right.update(config, right_batch) {
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
            }
        }
        Merge(left_orphan) => {
            match right.update(config, right_batch) {
                Split(right_split1_min, right_split1) => {
                    // left: merge, right: split
                    match right.merge_left(config, left_orphan, right_min) {
                        MergeResult::Done => {
                            return (
                                Node::Binary {
                                    left: right,
                                    right_min: right_split1_min,
                                    right: right_split1,
                                },
                                Done,
                            );
                        }
                        MergeResult::Split(right_split0_min, right_split0) => {
                            return (
                                Node::Ternary {
                                    left: right,
                                    middle_min: right_split0_min,
                                    middle: right_split0,
                                    right_min: right_split1_min,
                                    right: right_split1,
                                },
                                Done,
                            );
                        }
                    }
                }
                Merge(right_orphan) => {
                    // left: merge, right: merge
                    return (
                        Node::Nullary,
                        Merge(Branch(Box::new(Node::Binary {
                            left: left_orphan,
                            right_min,
                            right: right_orphan,
                        }))),
                    );
                }
                Done => {
                    // left: merge, right: done
                    match right.merge_left(config, left_orphan, right_min) {
                        MergeResult::Done => {
                            return (Node::Nullary, Merge(right));
                        }
                        MergeResult::Split(split_min, split) => {
                            return (
                                Node::Binary {
                                    left: right,
                                    right_min: split_min,
                                    right: split,
                                },
                                Done,
                            );
                        }
                    }
                }
            }
        }
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
        return (
            Node::Ternary {
                left,
                middle_min,
                middle,
                right_min,
                right,
            },
            Done,
        );
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
                Done => {
                    // left: done, middle: done, right: done
                    return (
                        Node::Ternary {
                            left,
                            middle_min,
                            middle,
                            right_min,
                            right,
                        },
                        Done,
                    );
                }
                Split(right_split_min, right_split) => {
                    // left: done, middle: done, right: split
                    return (
                        Node::Binary {
                            left,
                            right_min: middle_min,
                            right: middle,
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
                    // left: done, middle: done, right: merge
                    match middle.merge_right(config, right_min, right_orphan) {
                        MergeResult::Done => {
                            return (
                                Node::Binary {
                                    left,
                                    right_min: middle_min,
                                    right: middle,
                                },
                                Done,
                            );
                        }
                        MergeResult::Split(middle_split_min, middle_split) => {
                            return (
                                Node::Ternary {
                                    left,
                                    middle_min,
                                    middle,
                                    right_min: middle_split_min,
                                    right: middle_split,
                                },
                                Done,
                            );
                        }
                    }
                }
            },
            Split(middle_split1_min, mut middle_split1) => {
                match right.update(config, right_batch) {
                    Done => {
                        // left: done, middle: split, right: done
                        return (
                            Node::Binary {
                                left,
                                right_min: middle_min,
                                right: middle,
                            },
                            Split(
                                middle_split1_min,
                                Branch(Box::new(Node::Binary {
                                    left: middle_split1,
                                    right_min,
                                    right,
                                })),
                            ),
                        );
                    }
                    Split(right_split1_min, right_split1) => {
                        // left: done, middle: split, right: split
                        return (
                            Node::Ternary {
                                left,
                                middle_min,
                                middle,
                                right_min: middle_split1_min,
                                right: middle_split1,
                            },
                            Split(
                                right_min,
                                Branch(Box::new(Node::Binary {
                                    left: right,
                                    right_min: right_split1_min,
                                    right: right_split1,
                                })),
                            ),
                        );
                    }
                    Merge(right_orphan) => {
                        // left: done, middle: split, right: merge
                        match middle_split1.merge_right(config, right_min, right_orphan) {
                            MergeResult::Done => {
                                return (
                                    Node::Ternary {
                                        left,
                                        middle_min,
                                        middle,
                                        right_min: middle_split1_min,
                                        right: middle_split1,
                                    },
                                    Done,
                                );
                            }
                            MergeResult::Split(middle_split2_min, middle_split2) => {
                                return (
                                    Node::Binary {
                                        left,
                                        right_min: middle_min,
                                        right: middle,
                                    },
                                    Split(
                                        middle_split1_min,
                                        Branch(Box::new(Node::Binary {
                                            left: middle_split1,
                                            right_min: middle_split2_min,
                                            right: middle_split2,
                                        })),
                                    ),
                                );
                            }
                        }
                    }
                }
            }
            Merge(middle_orphan) => {
                match right.update(config, right_batch) {
                    Done => {
                        // left: done, middle: merge, right: done
                        match left.merge_right(config, middle_min, middle_orphan) {
                            MergeResult::Done => {
                                return (
                                    Node::Binary {
                                        left,
                                        right_min,
                                        right,
                                    },
                                    Done,
                                );
                            }
                            MergeResult::Split(left_split_min, left_split) => {
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
                        }
                    }
                    Split(right_split1_min, right_split1) => {
                        // left: done, middle: merge, right: split
                        match left.merge_right(config, middle_min, middle_orphan) {
                            MergeResult::Done => {
                                return (
                                    Node::Ternary {
                                        left,
                                        middle_min: right_min,
                                        middle: right,
                                        right_min: right_split1_min,
                                        right: right_split1,
                                    },
                                    Done,
                                );
                            }
                            MergeResult::Split(left_split_min, left_split) => {
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
                                            right_min: right_split1_min,
                                            right: right_split1,
                                        })),
                                    ),
                                );
                            }
                        }
                    }
                    Merge(right_orphan) => {
                        // left: done, middle: merge, right: merge
                        return (
                            Node::Binary {
                                left,
                                right_min: middle_min,
                                right: Branch(Box::new(Node::Binary {
                                    left: middle_orphan,
                                    right_min,
                                    right: right_orphan,
                                })),
                            },
                            Done,
                        );
                    }
                }
            }
        },
        Split(left_split1_min, left_split1) => {
            match middle.update(config, middle_batch) {
                Done => {
                    match right.update(config, right_batch) {
                        Done => {
                            // left: split, middle: done, right: done
                        }
                        Split(right_split1_min, right_split1) => {
                            // left: split, middle: done, right: split
                        }
                        Merge(right_orphan) => {
                            // left: split, middle: done, right: merge
                        }
                    }
                }
                Split(middle_split1_min, middle_split1) => {
                    match right.update(config, right_batch) {
                        Done => {
                            // left: split, middle: split, right: done
                        }
                        Split(right_split1_min, right_split1) => {
                            // left: split, middle: split, right: split
                        }
                        Merge(right_orphan) => {
                            // left: split, middle: split, right: merge
                        }
                    }
                }
                Merge(middle_orphan) => {
                    match right.update(config, right_batch) {
                        Done => {
                            // left: split, middle: merge, right: done
                        }
                        Split(right_split1_min, right_split1) => {
                            // left: split, middle: merge, right: split
                        }
                        Merge(right_orphan) => {
                            // left: split, middle: merge, right: merge
                        }
                    }
                }
            }
        }
        Merge(left_orphan) => {
            match middle.update(config, middle_batch) {
                Done => {
                    match right.update(config, right_batch) {
                        Done => {
                            // left: merge, middle: done, right: done
                            match middle.merge_left(config, left_orphan, middle_min) {
                                MergeResult::Done => {
                                    return updated![
                                        middle,    //
                                        right_min, //
                                        right
                                    ];
                                }
                                MergeResult::Split(middle_split1_min, mut middle_split1) => {
                                    return updated![
                                        middle,
                                        middle_split1_min,
                                        middle_split1,
                                        right_min,
                                        right
                                    ];
                                }
                            }
                        }
                        Split(right_split1_min, right_split1) => {
                            // left: merge, middle: done, right: split
                            match middle.merge_left(config, left_orphan, middle_min) {
                                MergeResult::Done => {
                                    return updated![
                                        middle,
                                        right_min,
                                        right,
                                        right_split1_min,
                                        right_split1
                                    ];
                                }
                                MergeResult::Split(middle_split1_min, mut middle_split1) => {
                                    return updated![
                                        middle,
                                        middle_split1_min,
                                        middle_split1,
                                        right_min,
                                        right,
                                        right_split1_min,
                                        right_split1
                                    ];
                                }
                            }
                        }
                        Merge(right_orphan) => {
                            // left: merge, middle: done, right: merge
                            match middle.merge_left(config, left_orphan, middle_min) {
                                MergeResult::Done => {
                                    match middle.merge_right(config, right_min, right_orphan) {
                                        MergeResult::Done => {
                                            return (Node::Nullary, Merge(middle));
                                        }
                                        MergeResult::Split(middle_split1_min, middle_split1) => {
                                            return (
                                                Node::Binary {
                                                    left: middle,
                                                    right_min: middle_split1_min,
                                                    right: middle_split1,
                                                },
                                                Done,
                                            );
                                        }
                                    }
                                }
                                MergeResult::Split(middle_split1_min, mut middle_split1) => {
                                    match middle_split1.merge_right(config, right_min, right_orphan)
                                    {
                                        MergeResult::Done => {
                                            return (
                                                Node::Binary {
                                                    left: middle,
                                                    right_min: middle_split1_min,
                                                    right: middle_split1,
                                                },
                                                Done,
                                            );
                                        }
                                        MergeResult::Split(middle_split2_min, middle_split2) => {
                                            return (
                                                Node::Ternary {
                                                    left: middle,
                                                    middle_min: middle_split1_min,
                                                    middle: middle_split1,
                                                    right_min: middle_split2_min,
                                                    right: middle_split2,
                                                },
                                                Done,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Split(middle_split1_min, mut middle_split1) => {
                    match right.update(config, right_batch) {
                        Done => {
                            // left: merge, middle: split, right: done
                            match middle.merge_left(config, left_orphan, middle_min) {
                                MergeResult::Done => {
                                    return (
                                        Node::Ternary {
                                            left: middle,
                                            middle_min: middle_split1_min,
                                            middle: middle_split1,
                                            right_min,
                                            right,
                                        },
                                        Done,
                                    );
                                }
                                MergeResult::Split(middle_split0_min, middle_split0) => {
                                    return (
                                        Node::Binary {
                                            left: middle,
                                            right_min: middle_split0_min,
                                            right: middle_split0,
                                        },
                                        Split(
                                            middle_split1_min,
                                            Branch(Box::new(Node::Binary {
                                                left: middle_split1,
                                                right_min,
                                                right,
                                            })),
                                        ),
                                    );
                                }
                            }
                        }
                        Split(right_split1_min, right_split1) => {
                            // left: merge, middle: split, right: split
                            match middle.merge_left(config, left_orphan, middle_min) {
                                MergeResult::Done => {
                                    return (
                                        Node::Binary {
                                            left: middle,
                                            right_min: middle_split1_min,
                                            right: middle_split1,
                                        },
                                        Split(
                                            right_min,
                                            Branch(Box::new(Node::Binary {
                                                left: right,
                                                right_min: right_split1_min,
                                                right: right_split1,
                                            })),
                                        ),
                                    );
                                }
                                MergeResult::Split(middle_split0_min, middle_split0) => {
                                    return (
                                        Node::Ternary {
                                            left: middle,
                                            middle_min: middle_split0_min,
                                            middle: middle_split0,
                                            right_min: middle_split1_min,
                                            right: middle_split1,
                                        },
                                        Split(
                                            right_min,
                                            Branch(Box::new(Node::Binary {
                                                left: right,
                                                right_min: right_split1_min,
                                                right: right_split1,
                                            })),
                                        ),
                                    );
                                }
                            }
                        }
                        Merge(right_orphan) => {
                            // left: merge, middle: split, right: merge
                            match middle.merge_left(config, left_orphan, middle_min) {
                                MergeResult::Done => {
                                    match middle_split1.merge_right(config, right_min, right_orphan)
                                    {
                                        MergeResult::Done => {
                                            return (
                                                Node::Binary {
                                                    left: middle,
                                                    right_min: middle_split1_min,
                                                    right: middle_split1,
                                                },
                                                Done,
                                            );
                                        }
                                        MergeResult::Split(middle_split2_min, middle_split2) => {
                                            return (
                                                Node::Ternary {
                                                    left: middle,
                                                    middle_min: middle_split1_min,
                                                    middle: middle_split1,
                                                    right_min: middle_split2_min,
                                                    right: middle_split2,
                                                },
                                                Done,
                                            );
                                        }
                                    }
                                }
                                MergeResult::Split(middle_split0_min, middle_split0) => {
                                    match middle_split1.merge_right(
                                        config,
                                        middle_split1_min,
                                        right_orphan,
                                    ) {
                                        MergeResult::Done => {
                                            return (
                                                Node::Ternary {
                                                    left: middle,
                                                    middle_min: middle_split0_min,
                                                    middle: middle_split0,
                                                    right_min: middle_split1_min,
                                                    right: middle_split1,
                                                },
                                                Done,
                                            );
                                        }
                                        MergeResult::Split(middle_split2_min, middle_split2) => {
                                            return (
                                                Node::Binary {
                                                    left: middle,
                                                    right_min: middle_split0_min,
                                                    right: middle_split0,
                                                },
                                                Split(
                                                    middle_split1_min,
                                                    Branch(Box::new(Node::Binary {
                                                        left: middle_split1,
                                                        right_min: middle_split2_min,
                                                        right: middle_split2,
                                                    })),
                                                ),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Merge(middle_orphan) => {
                    match right.update(config, right_batch) {
                        Done => {
                            // left: merge, middle: merge, right: done
                            return (
                                Node::Binary {
                                    left: Branch(Box::new(Node::Binary {
                                        left: left_orphan,
                                        right_min: middle_min,
                                        right: middle_orphan,
                                    })),
                                    right_min,
                                    right,
                                },
                                Done,
                            );
                        }
                        Split(right_split1_min, right_split1) => {
                            // left: merge, middle: merge, right: split
                            return (
                                Node::Ternary {
                                    left: Branch(Box::new(Node::Binary {
                                        left: left_orphan,
                                        right_min: middle_min,
                                        right: middle_orphan,
                                    })),
                                    middle_min: right_min,
                                    middle: right,
                                    right_min: right_split1_min,
                                    right: right_split1,
                                },
                                Done,
                            );
                        }
                        Merge(right_orphan) => {
                            // left: merge, middle: merge, right: merge
                            return (
                                Node::Nullary,
                                Merge(Branch(Box::new(Node::Ternary {
                                    left: left_orphan,
                                    middle_min,
                                    middle: middle_orphan,
                                    right_min,
                                    right: right_orphan,
                                }))),
                            );
                        }
                    }
                }
            }
        }
    }

    (Node::Nullary, Done)
}
