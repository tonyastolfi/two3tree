use itertools::Itertools;

use crate::batch::Batch;
use crate::flush::{plan_flush, Flush};
use crate::node::Node;
use crate::partition::partition;
use crate::queue::Queue;
use crate::sorted_updates::SortedUpdates;
use crate::subtree::Subtree;
use crate::{Height, TreeConfig, K};

#[derive(Debug)]
pub struct Tree {
    height: Height,
    root: Subtree,
}

macro_rules! update_branch {
    ($queue:expr, $branch:expr, $b0:expr, $m1:expr, $b1:expr) => {
        Subtree::Branch($queue, {
            *$branch = Node::Binary($b0, $m1, $b1);
            $branch
        })
    };
    ($queue:expr, $branch:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr) => {
        Subtree::Branch($queue, {
            *$branch = Node::Ternary($b0, $m1, $b1, $m2, $b2);
            $branch
        })
    };
}

macro_rules! rebuild_tree {
    ($config:expr, $height:expr, $updates:expr, $branch:expr, $subtrees:expr) => {
        Tree {
            height: $height,
            root: Queue::with_no_flush($config, $updates, {
                *$branch = node_from_trees($subtrees);
                $branch
            }),
        }
    };
}

macro_rules! make_tree {
    ($config:expr, $queue:expr, $branch:expr, $h:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr) => {{
        Tree {
            height: $h,
            root: update_branch!($queue, $branch, $b0, $m1, $b1, $m2, $b2),
        }
    }};
    ($config:expr, $queue:expr, $branch:expr, $h:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr, $m3:expr, $b3:expr) => {{
        let (q01, q23) = $queue.split_at_key($config, &$m2);
        Tree {
            height: $h + 1,
            root: Subtree::Branch(
                Queue::default(),
                Box::new(Node::Binary(
                    update_branch!(q01, $branch, $b0, $m1, $b1),
                    $m2,
                    Subtree::Branch(q23, Box::new(Node::Binary($b2, $m3, $b3))),
                )),
            ),
        }
    }};
}

macro_rules! join_subtrees {
    ($config:expr, $updates:expr, $b0:expr, $m1:expr, $b1:expr) => {{
        $b0.join($config, $m1, $b1)
            .enqueue_or_flush($config, $updates, true)
    }};
    ($config:expr, $updates:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr) => {{
        $b0.join($config, $m1, $b1)
            .join($config, $m2, $b2)
            .enqueue_or_flush($config, $updates, true)
    }};
}

fn trees_from_node(height: Height, node: Node<Subtree, K>) -> Node<Tree, K> {
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

fn node_from_trees(subtrees: Node<Tree, K>) -> Node<Subtree, K> {
    match subtrees {
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
                height: if vals.len() < config.batch_size { 0 } else { 1 },
                root: Subtree::Leaf(vals),
            };
        }
        let split_vals: Vec<i32> = vals.split_off(vals.len() / 2);
        let split_min: i32 = split_vals[0];
        return Self {
            height: 2,
            root: Subtree::Branch(
                Queue::default(),
                Box::new(Node::Binary(
                    Subtree::Leaf(vals),
                    split_min,
                    Subtree::Leaf(split_vals),
                )),
            ),
        };
    }

    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a> {
        self.root.iter()
    }

    pub fn height(&self) -> Height {
        self.height
    }

    pub fn check_height(&self, config: &TreeConfig) -> Height {
        assert_eq!(self.height, self.root.check_height(config));
        self.height
    }

    pub fn check_invariants(&self, config: &TreeConfig) {
        assert_eq!(self.height, self.root.check_height(config));
        self.root.check_invariants(config, self.height, true, None);
    }

    pub fn find(&self, key: &K) -> Option<&K> {
        self.root.find(key)
    }

    pub fn update(self, config: &TreeConfig, batch: Batch) -> Self {
        self.enqueue_or_flush(config, batch.consume(), true)
    }

    fn update_opt(self, config: &TreeConfig, opt_batch: Option<Batch>) -> Self {
        match opt_batch {
            Some(batch) => self.update(config, batch),
            None => self,
        }
    }

    fn enqueue_or_flush(
        self,
        config: &TreeConfig,
        updates: SortedUpdates,
        allow_flush: bool,
    ) -> Self {
        assert!(updates.len() <= config.batch_size * 3 / 2);

        match self.root {
            Subtree::Leaf(vals) => {
                use itertools::EitherOrBoth::{Both, Left, Right};

                let mut merged: Vec<K> = vals
                    .iter()
                    .merge_join_by(updates.into_iter(), |old, update| old.cmp(&update.key()))
                    .filter_map(|either| match either {
                        Left(old) => Some(*old),
                        Right(update) => update.resolve().map(|item_ref| *item_ref),
                        Both(_old, update) => update.resolve().map(|item_ref| *item_ref),
                    })
                    .collect();

                Tree::from_vals(config, merged)
            }
            Subtree::Branch(queue, mut branch) => {
                use Node::{Binary, Ternary};

                if queue.is_empty() && updates.len() <= config.batch_size {
                    Self {
                        height: self.height,
                        root: Subtree::Branch(Queue::new(config, updates), branch),
                    }
                } else {
                    let mut merged_updates = queue.consume().merge(updates);
                    let partition = partition(&merged_updates, &*branch);
                    let plan = if !allow_flush {
                        match &partition {
                            Node::Binary(ref n0, _, ref n1) => {
                                assert!(n0 + n1 <= config.batch_size);
                                Node::Binary(None, (), None)
                            }
                            Node::Ternary(ref n0, _, ref n1, _, ref n2) => {
                                assert!(
                                    n0 + n1 <= config.batch_size,
                                    "node is not 2/3 balanced: {:?}, queue={:?}, branch={:#?}",
                                    partition,
                                    merged_updates,
                                    branch
                                );
                                assert!(
                                    n1 + n2 <= config.batch_size,
                                    "node is not 2/3 balanced: {:?}, queue={:?}, branch={:#?}",
                                    partition,
                                    merged_updates,
                                    branch
                                );
                                Node::Ternary(None, (), None, (), None)
                            }
                        }
                    } else {
                        plan_flush(config, &partition)
                    };

                    match (
                        trees_from_node(self.height, *branch),
                        merged_updates.flush(config, &partition, &plan),
                    ) {
                        // No-flush cases.
                        //
                        (subtrees, Binary(None, _, None)) => {
                            rebuild_tree!(config, self.height, merged_updates, branch, subtrees)
                        }
                        (subtrees, Ternary(None, _, None, _, None)) => {
                            rebuild_tree!(config, self.height, merged_updates, branch, subtrees)
                        }

                        // Illegal cases.
                        //
                        (_subtrees, Binary(Some(_), _, Some(_)))
                        | (_subtrees, Ternary(Some(_), _, Some(_), _, Some(_))) => {
                            panic!("Too many branches flushed!");
                        }

                        // Binary node flush.
                        //
                        (Binary(b0, m1, b1), Binary(opt_x0, _, opt_x1)) => {
                            let b0 = b0.update_opt(config, opt_x0);
                            let b1 = b1.update_opt(config, opt_x1);

                            join_subtrees!(config, merged_updates, b0, m1, b1)
                        }

                        // Ternary node flush.
                        //
                        (Ternary(b0, m1, b1, m2, b2), Ternary(opt_x0, _, opt_x1, _, opt_x2)) => {
                            let b0 = b0.update_opt(config, opt_x0);
                            let b1 = b1.update_opt(config, opt_x1);
                            let b2 = b2.update_opt(config, opt_x2);

                            join_subtrees!(config, merged_updates, b0, m1, b1, m2, b2)
                        }

                        // Badness.
                        //
                        (subtrees, _) => panic!(
                            "illegal plan {:?} for node {:?}",
                            plan,
                            node_from_trees(subtrees)
                        ),
                    }
                }
            }
        }
    }

    pub fn join(self, config: &TreeConfig, other_min: K, other: Tree) -> Self {
        match (self.height, other.height) {
            // Join the leaf values, producing either a single new leaf or two
            // leaves under a new binary node.
            //
            (h, other_h) if h <= 1 && other_h <= 1 => {
                let mut vals = self.root.consume_leaf();
                vals.append(&mut other.root.consume_leaf());
                Tree::from_vals(config, vals)
            }

            // Grow the tree under a new binary node.
            //
            (h, other_h) if h != 1 && other_h == h => {
                return Self {
                    height: h + 1,
                    root: Subtree::Branch(
                        Queue::default(), // TODO maybe allow this to pull from the calling context?
                        Box::new(Node::Binary(self.root, other_min, other.root)),
                    ),
                };
            }

            //  (h + 1, h) -> destructure self, merge other as right child
            //
            (h, other_h) if h == other_h + 1 => match self.root {
                Subtree::Branch(queue, mut branch) => match (*branch, other_min, other.root) {
                    (Node::Binary(b0, m1, b1), m2, b2) => {
                        return make_tree!(config, queue, branch, h, b0, m1, b1, m2, b2);
                    }
                    (Node::Ternary(b0, m1, b1, m2, b2), m3, b3) => {
                        return make_tree!(config, queue, branch, h, b0, m1, b1, m2, b2, m3, b3);
                    }
                },
                _ => panic!("self.root is leaf, but self.height > 0!"),
            },

            //  (h, h + 1) -> destructure other, merge self as left child
            (self_h, h) if self_h + 1 == h => match other.root {
                Subtree::Branch(queue, mut branch) => match (self.root, other_min, *branch) {
                    (b0, m1, Node::Binary(b1, m2, b2)) => {
                        return make_tree!(config, queue, branch, h, b0, m1, b1, m2, b2);
                    }
                    (b0, m1, Node::Ternary(b1, m2, b2, m3, b3)) => {
                        return make_tree!(config, queue, branch, h, b0, m1, b1, m2, b2, m3, b3);
                    }
                },
                _ => panic!("other.root is leaf, but other.height > 0!"),
            },

            //  (h + d, h), d > 1  -> recursive case
            //
            (h, other_h) if h > other_h + 1 => match self.root {
                Subtree::Branch(queue, mut branch) => {
                    match (trees_from_node(h, *branch), other_min, other) {
                        (Node::Binary(b0, m1, b1), m2, b2) => {
                            let b1 = b1.join(config, m2, b2);
                            return join_subtrees!(config, queue.consume(), b0, m1, b1);
                        }
                        (Node::Ternary(b0, m1, b1, m2, b2), m3, b3) => {
                            let b2 = b2.join(config, m3, b3);
                            return join_subtrees!(config, queue.consume(), b0, m1, b1, m2, b2);
                        }
                    }
                }
                _ => panic!("self.root is leaf, but self.height > 1!"),
            },

            //  (h, h + d), d > 1  -> recursive case
            //
            (h_self, h) if h > h_self + 1 => match other.root {
                Subtree::Branch(queue, mut branch) => {
                    match (self, other_min, trees_from_node(h, *branch)) {
                        (b0, m1, Node::Binary(b1, m2, b2)) => {
                            let b1 = b0.join(config, m1, b1);
                            return join_subtrees!(config, queue.consume(), b1, m2, b2);
                        }
                        (b0, m1, Node::Ternary(b1, m2, b2, m3, b3)) => {
                            let b1 = b0.join(config, m1, b1);
                            return join_subtrees!(config, queue.consume(), b1, m2, b2, m3, b3);
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
