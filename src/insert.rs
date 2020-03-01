use crate::node::Node;

use Node::{Inner2, Inner3, Leaf2, Leaf3, Nil};

pub enum InsertResult {
    Ok,
    Split(i32, Box<Node>),
}

use InsertResult::{Ok, Split};

pub fn insert_leaf3(new_val: i32, (val1, val2): (i32, i32)) -> (Node, InsertResult) {
    if new_val < val1 {
        (
            Leaf2 { val: new_val },
            Split(val1, Box::new(Leaf3 { val1, val2 })),
        )
    } else if new_val < val2 {
        (
            Leaf2 { val: val1 },
            Split(
                new_val,
                Box::new(Leaf3 {
                    val1: new_val,
                    val2,
                }),
            ),
        )
    } else {
        (
            Leaf2 { val: val1 },
            Split(
                val2,
                Box::new(Leaf3 {
                    val1: val2,
                    val2: new_val,
                }),
            ),
        )
    }
}

pub fn insert_inner2(
    new_val: i32,
    mut left: Box<Node>,
    right_min: i32,
    mut right: Box<Node>,
) -> (Node, InsertResult) {
    if new_val < right_min {
        if let Split(split_min, split) = left.insert(new_val) {
            return (
                Inner3 {
                    left,
                    middle_min: split_min,
                    middle: split,
                    right_min,
                    right,
                },
                Ok,
            );
        }
    } else {
        if let Split(split_min, split) = right.insert(new_val) {
            return (
                Inner3 {
                    left,
                    middle_min: right_min,
                    middle: right,
                    right_min: split_min,
                    right: split,
                },
                Ok,
            );
        }
    }
    return (
        Inner2 {
            left,
            right_min,
            right,
        },
        Ok,
    );
}

pub fn insert_inner3(
    new_val: i32,
    mut left: Box<Node>,
    middle_min: i32,
    mut middle: Box<Node>,
    right_min: i32,
    mut right: Box<Node>,
) -> (Node, InsertResult) {
    if new_val < middle_min {
        if let Split(split_min, split) = left.insert(new_val) {
            return (
                Inner2 {
                    left,
                    right_min: split_min,
                    right: split,
                },
                Split(
                    middle_min,
                    Box::new(Inner2 {
                        left: middle,
                        right_min,
                        right,
                    }),
                ),
            );
        }
    } else if new_val < right_min {
        if let Split(split_min, split) = middle.insert(new_val) {
            return (
                Inner2 {
                    left,
                    right_min: middle_min,
                    right: middle,
                },
                Split(
                    split_min,
                    Box::new(Inner2 {
                        left: split,
                        right_min,
                        right,
                    }),
                ),
            );
        }
    } else {
        if let Split(split_min, split) = right.insert(new_val) {
            return (
                Inner2 {
                    left,
                    right_min: middle_min,
                    right: middle,
                },
                Split(
                    right_min,
                    Box::new(Inner2 {
                        left: right,
                        right_min: split_min,
                        right: split,
                    }),
                ),
            );
        }
    }
    return (
        Inner3 {
            left,
            middle_min,
            middle,
            right_min,
            right,
        },
        Ok,
    );
}
