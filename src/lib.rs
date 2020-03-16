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

                let mut queue = queue.merge(batch);
                let partition = queue.partition(&*branch);
                let plan = plan_flush(config, &partition);

                match (*branch, queue.flush(&partition, &plan)) {
                    // No-flush cases.
                    //
                    (Binary(b0, m1, b1), Binary(None, _, None)) => {
                        *branch = Binary(b0, m1, b1);
                        return Tree {
                            height: self.height,
                            root: Subtree::Branch(queue, branch),
                        };
                    }
                    (Ternary(b0, m1, b1, m2, b2), Ternary(None, _, None, _, None)) => {
                        *branch = Ternary(b0, m1, b1, m2, b2);
                        return Tree {
                            height: self.height,
                            root: Subtree::Branch(queue, branch),
                        };
                    }

                    // Flush one subtree cases.
                    //
                    (Binary(b0, m1, b1), Binary(Some(x0), _, None)) => {
                        return Tree {
                            height: self.height - 1,
                            root: b0,
                        }
                        .update(config, x0)
                        .join(
                            config,
                            m1,
                            Tree {
                                height: self.height - 1,
                                root: b1,
                            },
                        )
                        .update(config, queue.consume());
                    }
                    (Binary(b0, m1, b1), Binary(None, _, Some(x1))) => {
                        return Tree {
                            height: self.height - 1,
                            root: b0,
                        }
                        .join(
                            config,
                            m1,
                            Tree {
                                height: self.height - 1,
                                root: b1,
                            }
                            .update(config, x1),
                        )
                        .update(config, queue.consume())
                    }
                    (Ternary(b0, m1, b1, m2, b2), Ternary(Some(x0), _, None, _, None)) => {
                        return Tree {
                            height: self.height - 1,
                            root: b0,
                        }
                        .update(config, x0)
                        .join(
                            config,
                            m1,
                            Tree {
                                height: self.height,
                                root: update_branch!(queue, branch, b1, m2, b2),
                            },
                        );
                    }
                    (Ternary(b0, m1, b1, m2, b2), Ternary(None, _, Some(x1), _, None)) => {
                        return Tree {
                            height: self.height - 1,
                            root: b0,
                        }
                        .join(
                            config,
                            m1,
                            Tree {
                                height: self.height - 1,
                                root: b1,
                            }
                            .update(config, x1),
                        )
                        .join(
                            config,
                            m2,
                            Tree {
                                height: self.height - 1,
                                root: b2,
                            },
                        )
                        .update(config, queue.consume());
                    }
                    (Ternary(b0, m1, b1, m2, b2), Ternary(None, _, None, _, Some(x2))) => {
                        return Tree {
                            height: self.height,
                            root: update_branch!(queue, branch, b0, m1, b1),
                        }
                        .join(
                            config,
                            m2,
                            Tree {
                                height: self.height - 1,
                                root: b2,
                            }
                            .update(config, x2),
                        );
                    }

                    // Flush two subtree cases.
                    //
                    (Ternary(b0, m1, b1, m2, b2), Ternary(Some(x0), _, Some(x1), _, None)) => {
                        return Tree {
                            height: self.height - 1,
                            root: b0,
                        }
                        .update(config, x0)
                        .join(
                            config,
                            m1,
                            Tree {
                                height: self.height - 1,
                                root: b1,
                            }
                            .update(config, x1),
                        )
                        .join(
                            config,
                            m2,
                            Tree {
                                height: self.height - 1,
                                root: b2,
                            },
                        )
                        .update(config, queue.consume());
                    }
                    (Ternary(b0, m1, b1, m2, b2), Ternary(Some(x0), _, None, _, Some(x2))) => {
                        return Tree {
                            height: self.height - 1,
                            root: b0,
                        }
                        .update(config, x0)
                        .join(
                            config,
                            m1,
                            Tree {
                                height: self.height - 1,
                                root: b1,
                            },
                        )
                        .join(
                            config,
                            m2,
                            Tree {
                                height: self.height - 1,
                                root: b2,
                            }
                            .update(config, x2),
                        )
                        .update(config, queue.consume());
                    }
                    (Ternary(b0, m1, b1, m2, b2), Ternary(None, _, Some(x1), _, Some(x2))) => {
                        return Tree {
                            height: self.height - 1,
                            root: b0,
                        }
                        .join(
                            config,
                            m1,
                            Tree {
                                height: self.height - 1,
                                root: b1,
                            }
                            .update(config, x1),
                        )
                        .join(
                            config,
                            m2,
                            Tree {
                                height: self.height - 1,
                                root: b2,
                            }
                            .update(config, x2),
                        )
                        .update(config, queue.consume());
                    }
                    (node, _) => panic!("illegal plan {:?} for node {:?}", plan, node),
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
                Subtree::Branch(queue, mut branch) => match *branch {
                    Node::Binary(b0, m1, b1) => {
                        let initial = Tree {
                            height: h - 1,
                            root: b0,
                        };
                        let last = Tree {
                            height: h - 1,
                            root: b1,
                        };
                        return initial
                            .join(config, m1, last.join(config, other_min, other))
                            .update(config, queue.consume());
                    }
                    Node::Ternary(b0, m1, b1, m2, b2) => {
                        let (q01, q2) = queue.split(&m2);
                        let initial = Tree {
                            height: h,
                            root: update_branch!(q01, branch, b0, m1, b1),
                        };
                        let last = Tree {
                            height: h - 1,
                            root: b2,
                        };
                        return initial
                            .join(config, m2, last.join(config, other_min, other))
                            .update(config, q2.consume());
                    }
                },
                _ => panic!("self.root is leaf, but self.height > 1!"),
            },

            //  (h, h + d), d > 1  -> recursive case
            //
            (h_self, h) if h > h_self + 1 => match other.root {
                Subtree::Branch(queue, mut branch) => match *branch {
                    Node::Binary(b1, m2, b2) => {
                        let first = Tree {
                            height: h - 1,
                            root: b1,
                        };
                        let rest = Tree {
                            height: h - 1,
                            root: b2,
                        };
                        return self
                            .join(config, other_min, first)
                            .join(config, m2, rest)
                            .update(config, queue.consume());
                    }
                    Node::Ternary(b1, m2, b2, m3, b3) => {
                        let (q1, q23) = queue.split(&m2);
                        let first = Tree {
                            height: h - 1,
                            root: b1,
                        };
                        let rest = Tree {
                            height: h,
                            root: update_branch!(q23, branch, b2, m3, b3),
                        };
                        return self
                            .join(config, other_min, first)
                            .join(config, m2, rest)
                            .update(config, q1.consume());
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
