#![allow(dead_code)]
#![allow(unused_macros)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use itertools::Itertools;

pub type K = i32;

pub mod algo;
pub mod node;
pub mod queue;
pub mod update;

use algo::{lower_bound_by_key, upper_bound_by_key};
use node::Node;
use update::Update;

#[derive(Debug)]
pub struct TreeConfig {
    pub batch_size: usize,
}

pub enum Subtree {
    Leaf(Vec<K>),
    Branch(Box<Node<Subtree, K>>),
}

impl Subtree {
    pub fn consume_leaf(self) -> Vec<K> {
        match self {
            Subtree::Leaf(vals) => vals,
            _ => panic!("not a Leaf!"),
        }
    }
}

pub type Height = u16;

pub struct Tree {
    height: Height,
    root: Subtree,
}

macro_rules! make_tree {
    ($branch: expr, $h:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr) => {{
        *$branch = Node::Ternary($b0, $m1, $b1, $m2, $b2);
        Tree {
            height: $h,
            root: Subtree::Branch($branch),
        }
    }};
    ($branch: expr, $h:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr, $m3:expr, $b3:expr) => {{
        *$branch = Node::Binary($b0, $m1, $b1);
        Tree {
            height: $h + 1,
            root: Subtree::Branch(Box::new(Node::Binary(
                Subtree::Branch($branch),
                $m2,
                Subtree::Branch(Box::new(Node::Binary($b2, $m3, $b3))),
            ))),
        }
    }};
}

macro_rules! update_branch {
    ($branch:expr, $b0:expr, $m1:expr, $b1:expr) => {
        Subtree::Branch({
            *$branch = Node::Binary($b0, $m1, $b1);
            $branch
        })
    };
}

impl Tree {
    pub fn new() -> Self {
        Self {
            height: 0,
            root: Subtree::Leaf(vec![]),
        }
    }

    pub fn join(self, config: &TreeConfig, other_min: K, other: Tree) -> Self {
        match (self.height, other.height) {
            // Join the leaf values, producing either a single new leaf or two
            // leaves under a new binary node.
            //
            (0, 0) => {
                let mut vals = self.root.consume_leaf();
                vals.append(&mut other.root.consume_leaf());
                if vals.len() <= config.batch_size * 2 {
                    return Self {
                        height: 0,
                        root: Subtree::Leaf(vals),
                    };
                }
                let split_vals: Vec<i32> = vals.drain((vals.len() / 2)..).collect();
                let split_min: i32 = split_vals[0];
                return Self {
                    height: 1,
                    root: Subtree::Branch(Box::new(Node::Binary(
                        Subtree::Leaf(vals),
                        split_min,
                        Subtree::Leaf(split_vals),
                    ))),
                };
            }

            // Grow the tree under a new binary node.
            //
            (h, other_h) if h != 0 && other_h == h => {
                return Self {
                    height: h + 1,
                    root: Subtree::Branch(Box::new(Node::Binary(self.root, other_min, other.root))),
                };
            }

            //  (h + 1, h) -> destructure self, merge other as right child
            //
            (h, other_h) if h == other_h + 1 => match self.root {
                Subtree::Branch(mut branch) => match (*branch, other_min, other.root) {
                    (Node::Binary(b0, m1, b1), m2, b2) => {
                        return make_tree!(branch, h, b0, m1, b1, m2, b2);
                    }
                    (Node::Ternary(b0, m1, b1, m2, b2), m3, b3) => {
                        return make_tree!(branch, h, b0, m1, b1, m2, b2, m3, b3);
                    }
                },
                _ => panic!("self.root is leaf, but self.height > 0!"),
            },

            //  (h, h + 1) -> destructure other, merge self as left child
            (self_h, h) if self_h + 1 == h => match other.root {
                Subtree::Branch(mut branch) => match (self.root, other_min, *branch) {
                    (b0, m1, Node::Binary(b1, m2, b2)) => {
                        return make_tree!(branch, h, b0, m1, b1, m2, b2);
                    }
                    (b0, m1, Node::Ternary(b1, m2, b2, m3, b3)) => {
                        return make_tree!(branch, h, b0, m1, b1, m2, b2, m3, b3);
                    }
                },
                _ => panic!("other.root is leaf, but other.height > 0!"),
            },

            //  (h + d, h), d > 1  -> recursive case
            //
            (h, other_h) if h > other_h + 1 => match self.root {
                Subtree::Branch(mut branch) => match *branch {
                    Node::Binary(b0, m1, b1) => {
                        let initial = Tree {
                            height: h - 1,
                            root: b0,
                        };
                        let last = Tree {
                            height: h - 1,
                            root: b1,
                        };
                        return initial.join(config, m1, last.join(config, other_min, other));
                    }
                    Node::Ternary(b0, m1, b1, m2, b2) => {
                        let initial = Tree {
                            height: h,
                            root: update_branch!(branch, b0, m1, b1),
                        };
                        let last = Tree {
                            height: h - 1,
                            root: b2,
                        };
                        return initial.join(config, m2, last.join(config, other_min, other));
                    }
                },
                _ => panic!("self.root is leaf, but self.height > 1!"),
            },

            //  (h, h + d), d > 1  -> recursive case
            //
            (h_self, h) if h > h_self + 1 => match other.root {
                Subtree::Branch(mut branch) => match *branch {
                    Node::Binary(b1, m2, b2) => {
                        let first = Tree {
                            height: h - 1,
                            root: b1,
                        };
                        let rest = Tree {
                            height: h - 1,
                            root: b2,
                        };
                        return self.join(config, other_min, first).join(config, m2, rest);
                    }
                    Node::Ternary(b1, m2, b2, m3, b3) => {
                        let first = Tree {
                            height: h - 1,
                            root: b1,
                        };
                        let rest = Tree {
                            height: h,
                            root: update_branch!(branch, b2, m3, b3),
                        };
                        return self.join(config, other_min, first).join(config, m2, rest);
                    }
                },
                _ => panic!("other.root is leaf, but other.height > 1!"),
            },

            _ => panic!(
                "illegal case: self.height={}, other.height={}",
                self.height, other.height
            ),
        }
    }
}
