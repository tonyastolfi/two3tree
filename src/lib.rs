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

            // TODO :
            //  (h + 1, h) -> destructure self, merge other as right child
            //  (h, h + 1) -> destructure other, merge self as left child
            //  (h + d, h), d > 1  -> recursive case
            //  (h, h + d), d > 1  -> recursive case
            //
            _ => panic!("todo"),
        }
    }
}
