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
use queue::{plan_flush, sort_batch, Queue};
use update::Update;

#[derive(Debug)]
pub struct TreeConfig {
    pub batch_size: usize,
}

#[derive(Debug)]
pub enum Subtree {
    Leaf(Vec<K>),
    Branch(Queue, Box<Node<Subtree, K>>),
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
    ($queue:expr, $branch:expr, $h:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr) => {{
        *$branch = Node::Ternary($b0, $m1, $b1, $m2, $b2);
        Tree {
            height: $h,
            root: Subtree::Branch($queue, $branch),
        }
    }};
    ($queue:expr, $branch:expr, $h:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr, $m3:expr, $b3:expr) => {{
        // TODO : split $queue at key=$m2
        let (q01, q23) = $queue.split(&$m2);
        *$branch = Node::Binary($b0, $m1, $b1);
        Tree {
            height: $h + 1,
            root: Subtree::Branch(
                Queue::new(),
                Box::new(Node::Binary(
                    Subtree::Branch(q01, $branch),
                    $m2,
                    Subtree::Branch(q23, Box::new(Node::Binary($b2, $m3, $b3))),
                )),
            ),
        }
    }};
}

macro_rules! update_branch {
    ($queue:expr, $branch:expr, $b0:expr, $m1:expr, $b1:expr) => {
        Subtree::Branch($queue, {
            *$branch = Node::Binary($b0, $m1, $b1);
            $branch
        })
    };
}

macro_rules! rebuild_tree {
    ($height:expr, $queue:expr, $branch:expr, $children:expr) => {
        Tree {
            height: $height,
            root: Subtree::Branch($queue, {
                *$branch = build_node($children);
                $branch
            }),
        }
    };
}

fn destruct_node(height: Height, node: Node<Subtree, K>) -> Node<Tree, K> {
    match node {
        Node::Binary(b0, m1, b1) => Node::Binary(
            Tree {
                height: height - 1,
                root: b0,
            },
            m1,
            Tree {
                height: height - 1,
                root: b1,
            },
        ),
        Node::Ternary(b0, m1, b1, m2, b2) => Node::Ternary(
            Tree {
                height: height - 1,
                root: b0,
            },
            m1,
            Tree {
                height: height - 1,
                root: b1,
            },
            m2,
            Tree {
                height: height - 1,
                root: b2,
            },
        ),
    }
}

fn build_node(children: Node<Tree, K>) -> Node<Subtree, K> {
    match children {
        Node::Binary(t0, m1, t1) => {
            assert_eq!(t0.height, t1.height);
            Node::Binary(t0.root, m1, t1.root)
        }
        Node::Ternary(t0, m1, t1, m2, t2) => {
            assert_eq!(t0.height, t1.height);
            assert_eq!(t1.height, t2.height);
            Node::Ternary(t0.root, m1, t1.root, m2, t2.root)
        }
    }
}

impl Tree {
    pub fn new() -> Self {
        Self {
            height: 0,
            root: Subtree::Leaf(vec![]),
        }
    }

    pub fn from_vals(config: &TreeConfig, mut vals: Vec<K>) -> Self {
        if vals.len() <= config.batch_size * 2 {
            return Self {
                height: 0,
                root: Subtree::Leaf(vals),
            };
        }
        let split_vals: Vec<i32> = vals.split_off(vals.len() / 2);
        let split_min: i32 = split_vals[0];
        return Self {
            height: 1,
            root: Subtree::Branch(
                Queue::new(),
                Box::new(Node::Binary(
                    Subtree::Leaf(vals),
                    split_min,
                    Subtree::Leaf(split_vals),
                )),
            ),
        };
    }

    pub fn update(self, config: &TreeConfig, batch: Vec<Update>) -> Self {
        match self.root {
            Subtree::Leaf(vals) => {
                use itertools::EitherOrBoth::{Both, Left, Right};

                let batch = sort_batch(batch);
                let mut merged: Vec<i32> = vals
                    .iter()
                    .merge_join_by(batch.iter(), |old, update| old.cmp(&update.key()))
                    .filter_map(|either| match either {
                        Left(old) => Some(*old),
                        Right(update) => update.resolve().map(|item_ref| *item_ref),
                        Both(_old, update) => update.resolve().map(|item_ref| *item_ref),
                    })
                    .collect();

                return Tree::from_vals(config, merged);
            }
            Subtree::Branch(queue, mut branch) => {
                use Node::{Binary, Ternary};

                if queue.is_empty() {
                    return Self {
                        height: self.height,
                        root: Subtree::Branch(Queue(batch), branch),
                    };
                }

                let mut queue = queue.merge(batch);
                let partition = queue.partition(&*branch);
                let plan = plan_flush(config, &partition);

                match (
                    destruct_node(self.height, *branch),
                    queue.flush(&partition, &plan),
                ) {
                    // No-flush cases.
                    //
                    (children, Binary(None, _, None)) => {
                        return rebuild_tree!(self.height, queue, branch, children);
                    }
                    (children, Ternary(None, _, None, _, None)) => {
                        return rebuild_tree!(self.height, queue, branch, children);
                    }

                    // Flush left.
                    //
                    (Binary(b0, m1, b1), Binary(Some(x0), _, None)) => {
                        return (b0.update(config, x0))
                            .join(config, m1, b1)
                            .update(config, queue.consume());
                    }
                    (Ternary(b0, m1, b1, m2, b2), Ternary(Some(x0), _, None, _, None)) => {
                        return (b0.update(config, x0))
                            .join(config, m1, b1)
                            .join(config, m2, b2)
                            .update(config, queue.consume());
                    }

                    // Flush right.
                    //
                    (Binary(b0, m1, b1), Binary(None, _, Some(x1))) => {
                        return b0
                            .join(config, m1, b1.update(config, x1))
                            .update(config, queue.consume())
                    }
                    (Ternary(b0, m1, b1, m2, b2), Ternary(None, _, None, _, Some(x2))) => {
                        return b0
                            .join(config, m1, b1)
                            .join(config, m2, b2.update(config, x2))
                            .update(config, queue.consume());
                    }

                    // Flush middle.
                    //
                    (Ternary(b0, m1, b1, m2, b2), Ternary(None, _, Some(x1), _, None)) => {
                        return b0
                            .join(config, m1, b1.update(config, x1))
                            .join(config, m2, b2)
                            .update(config, queue.consume());
                    }

                    // Flush left, middle.
                    //
                    (Ternary(b0, m1, b1, m2, b2), Ternary(Some(x0), _, Some(x1), _, None)) => {
                        return (b0.update(config, x0))
                            .join(config, m1, b1.update(config, x1))
                            .join(config, m2, b2)
                            .update(config, queue.consume());
                    }

                    // Flush left, right.
                    //
                    (Ternary(b0, m1, b1, m2, b2), Ternary(Some(x0), _, None, _, Some(x2))) => {
                        return (b0.update(config, x0))
                            .join(config, m1, b1)
                            .join(config, m2, b2.update(config, x2))
                            .update(config, queue.consume());
                    }

                    // Flush middle, right.
                    //
                    (Ternary(b0, m1, b1, m2, b2), Ternary(None, _, Some(x1), _, Some(x2))) => {
                        return b0
                            .join(config, m1, b1.update(config, x1))
                            .join(config, m2, b2.update(config, x2))
                            .update(config, queue.consume());
                    }

                    // Badness.
                    //
                    (children, _) => panic!(
                        "illegal plan {:?} for node {:?}",
                        plan,
                        build_node(children)
                    ),
                }
            }
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
                return Tree::from_vals(config, vals);
            }

            // Grow the tree under a new binary node.
            //
            (h, other_h) if h != 0 && other_h == h => {
                return Self {
                    height: h + 1,
                    root: Subtree::Branch(
                        Queue::new(),
                        Box::new(Node::Binary(self.root, other_min, other.root)),
                    ),
                };
            }

            //  (h + 1, h) -> destructure self, merge other as right child
            //
            (h, other_h) if h == other_h + 1 => match self.root {
                Subtree::Branch(queue, mut branch) => match (*branch, other_min, other.root) {
                    (Node::Binary(b0, m1, b1), m2, b2) => {
                        return make_tree!(queue, branch, h, b0, m1, b1, m2, b2);
                    }
                    (Node::Ternary(b0, m1, b1, m2, b2), m3, b3) => {
                        return make_tree!(queue, branch, h, b0, m1, b1, m2, b2, m3, b3);
                    }
                },
                _ => panic!("self.root is leaf, but self.height > 0!"),
            },

            //  (h, h + 1) -> destructure other, merge self as left child
            (self_h, h) if self_h + 1 == h => match other.root {
                Subtree::Branch(queue, mut branch) => match (self.root, other_min, *branch) {
                    (b0, m1, Node::Binary(b1, m2, b2)) => {
                        return make_tree!(queue, branch, h, b0, m1, b1, m2, b2);
                    }
                    (b0, m1, Node::Ternary(b1, m2, b2, m3, b3)) => {
                        return make_tree!(queue, branch, h, b0, m1, b1, m2, b2, m3, b3);
                    }
                },
                _ => panic!("other.root is leaf, but other.height > 0!"),
            },

            //  (h + d, h), d > 1  -> recursive case
            //
            (h, other_h) if h > other_h + 1 => match self.root {
                Subtree::Branch(queue, mut branch) => {
                    match (destruct_node(h, *branch), other_min, other) {
                        (Node::Binary(b0, m1, b1), m2, b2) => {
                            return b0
                                .join(config, m1, b1.join(config, m2, b2))
                                .update(config, queue.consume());
                        }
                        (Node::Ternary(b0, m1, b1, m2, b2), m3, b3) => {
                            return b0
                                .join(config, m1, b1)
                                .join(config, m2, b2.join(config, m3, b3))
                                .update(config, queue.consume());
                        }
                    }
                }
                _ => panic!("self.root is leaf, but self.height > 1!"),
            },

            //  (h, h + d), d > 1  -> recursive case
            //
            (h_self, h) if h > h_self + 1 => match other.root {
                Subtree::Branch(queue, mut branch) => {
                    match (self, other_min, destruct_node(h, *branch)) {
                        (b0, m1, Node::Binary(b1, m2, b2)) => {
                            return (b0.join(config, m1, b1))
                                .join(config, m2, b2)
                                .update(config, queue.consume());
                        }
                        (b0, m1, Node::Ternary(b1, m2, b2, m3, b3)) => {
                            return (b0.join(config, m1, b1))
                                .join(config, m2, b2)
                                .join(config, m3, b3)
                                .update(config, queue.consume());
                        }
                    }
                }
                _ => panic!("other.root is leaf, but other.height > 1!"),
            },

            _ => panic!(
                "illegal case: self.height={}, other.height={}",
                self.height, other.height
            ),
        }
    }
}
