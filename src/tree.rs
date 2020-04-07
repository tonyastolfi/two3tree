use std::sync::Arc;

use itertools::Itertools;
use smallvec::SmallVec;

use crate::batch::Batch;
use crate::node::Node;
use crate::queue::Queue;
use crate::sorted_updates::{Sorted, SortedUpdates};
use crate::subtree::Subtree;
use crate::update::{apply_updates, Update};
use crate::{Height, TreeConfig};

#[derive(Debug, Clone)]
pub struct Tree<K> {
    height: Height,
    root: (K, Subtree<K>),
}

macro_rules! make_tree {
    ($config:expr, $queue:expr, $h:expr, $b0:expr, $b1:expr, $b2:expr) => {{
        Tree {
            height: $h,
            root: ($b0.0, make_subtree!($queue, $b0, $b1, $b2)),
        }
    }};
    ($config:expr, $queue:expr, $h:expr, $b0:expr, $b1:expr, $b2:expr, $b3:expr) => {{
        let (q_left, q_right) = $queue.split_at_key($config, &$b2.0);
        Tree {
            height: $h + 1,
            root: (
                $b0.0,
                make_subtree!(
                    Queue::default(),
                    ($b0.0, make_subtree!(q_left, $b0, $b1)),
                    ($b2.0, make_subtree!(q_right, $b2, $b3))
                ),
            ),
        }
    }};
}

macro_rules! join_subtrees {
    ($config:expr, $updates:expr, $b0:expr, $b1:expr) => {{
        $b0.join($config, $b1).enqueue_or_flush($config, $updates)
    }};
    ($config:expr, $updates:expr, $b0:expr, $b1:expr, $b2:expr) => {{
        $b0.join($config, $b1)
            .join($config, $b2)
            .enqueue_or_flush($config, $updates)
    }};
}

fn trees_from_node<K>(height: Height, node: &Node<(K, Subtree<K>)>) -> Node<Tree<K>>
where
    K: Clone,
{
    node.as_ref().map(|(k, s)| Tree {
        height: height - 1,
        root: (k.clone(), (*s).clone()),
    })
}

fn node_from_trees<K>(trees: Node<Tree<K>>) -> Node<(K, Subtree<K>)> {
    match trees {
        Node::Binary(t0, t1) => {
            assert_eq!(t0.height, t1.height);
            Node::Binary(t0.root, t1.root)
        }
        Node::Ternary(t0, t1, t2) => {
            assert_eq!(t0.height, t1.height);
            assert_eq!(t1.height, t2.height);
            Node::Ternary(t0.root, t1.root, t2.root)
        }
    }
}

impl<K> Tree<K>
where
    K: Ord + std::fmt::Debug + Copy + Default,
{
    pub fn new() -> Self {
        Self {
            height: 0,
            root: (K::default(), Subtree::Leaf(Arc::new([]))),
        }
    }

    pub fn from_vals(config: &TreeConfig, n_vals: usize, vals: impl Iterator<Item = K>) -> Self {
        if n_vals <= config.batch_size * 2 {
            let vals: Arc<[K]> = vals.collect();
            let first = *(*vals).first().unwrap_or(&K::default());
            return Self {
                height: if n_vals < config.batch_size { 0 } else { 1 },
                root: (first, Subtree::Leaf(vals)),
            };
        }

        let mut split_vals: Vec<Arc<[K]>> = vals
            .chunks((n_vals + 1) / 2)
            .into_iter()
            .map(|subvals| subvals.collect::<Arc<[K]>>())
            .collect();

        assert_eq!(split_vals.len(), 2);
        assert!(split_vals[0].len() > 0);
        assert!(split_vals[1].len() > 0);

        let left_vals = split_vals.remove(0);
        let right_vals = split_vals.remove(0);

        let first_key = left_vals[0];
        let middle_key = right_vals[0];

        return Self {
            height: 2,
            root: (
                first_key,
                make_subtree!(
                    Queue::default(),
                    (first_key, Subtree::Leaf(left_vals)),
                    (middle_key, Subtree::Leaf(right_vals))
                ),
            ),
        };
    }

    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a> {
        self.root.1.iter()
    }

    pub fn height(&self) -> Height {
        self.height
    }

    pub fn check_height(&self, config: &TreeConfig) -> Height {
        assert_eq!(self.height, self.root.1.check_height(config));
        self.height
    }

    pub fn check_invariants(&self, config: &TreeConfig) {
        assert_eq!(self.height, self.root.1.check_height(config));
        self.root
            .1
            .check_invariants(config, self.height, true, None);
    }

    pub fn find(&self, key: &K) -> Option<&K> {
        self.root.1.find(key)
    }

    pub fn update<'a, U>(self, config: &TreeConfig, batch: Batch<'a, U>) -> Self
    where
        U: Sorted<Item = Update<K>>,
    {
        let updates: SortedUpdates<K> = batch.consume();
        self.enqueue_or_flush(config, updates)
    }

    fn update_opt<'a, U>(self, config: &TreeConfig, opt_batch: Option<Batch<'a, U>>) -> Self
    where
        U: Sorted<Item = Update<K>>,
    {
        match opt_batch {
            Some(batch) => self.update(config, batch),
            None => self,
        }
    }

    fn enqueue_or_flush<U>(self, config: &TreeConfig, updates: U) -> Self
    where
        U: Sorted<Item = Update<K>> + Into<SortedUpdates<K>>,
    {
        assert!(updates.len() <= config.batch_size * 3 / 2);

        match self.root {
            (_, Subtree::Leaf(vals)) => {
                use itertools::EitherOrBoth::{Both, Left, Right};

                let merged: SmallVec<[K; 1024]> = apply_updates(vals.iter(), updates.into_iter())
                    .cloned()
                    .collect();

                let n_merged = merged.len();

                Tree::from_vals(config, n_merged, merged.into_iter())
            }
            (min_key, Subtree::Branch(queue, branch)) => {
                use Node::{Binary, Ternary};

                if queue.is_empty() && updates.len() <= config.batch_size {
                    Self {
                        height: self.height,
                        root: (
                            min_key,
                            Subtree::Branch(Queue::new(config, updates), branch.clone()),
                        ),
                    }
                } else {
                    let mut merged_updates = queue.consume().merge(updates);
                    let partition = branch.partition(&merged_updates);

                    match (&*branch).flush(config, &mut merged_updates) {
                        // No flush.
                        //
                        (Binary(None, None), unflushed)
                        | (Ternary(None, None, None), unflushed) => Tree {
                            height: self.height,
                            root: (min_key, Queue::with_no_flush(config, unflushed, branch)),
                        },

                        // Illegal cases.
                        //
                        (Binary(Some(_), Some(_)), _) | (Ternary(Some(_), Some(_), Some(_)), _) => {
                            panic!("Too many branches flushed!");
                        }

                        // Binary node flush.
                        //
                        (Binary(batch0, batch1), unflushed) => {
                            match trees_from_node(self.height, &*branch) {
                                Binary(child0, child1) => {
                                    let child0 = child0.update_opt(config, batch0);
                                    let child1 = child1.update_opt(config, batch1);

                                    join_subtrees!(config, unflushed, child0, child1)
                                }
                                Ternary(..) => {
                                    panic!("illegal plan (binary flush plan for a ternary node)")
                                }
                            }
                        }

                        // Ternary node flush.
                        //
                        (Ternary(batch0, batch1, batch2), unflushed) => {
                            match trees_from_node(self.height, &*branch) {
                                Ternary(child0, child1, child2) => {
                                    let child0 = child0.update_opt(config, batch0);
                                    let child1 = child1.update_opt(config, batch1);
                                    let child2 = child2.update_opt(config, batch2);

                                    join_subtrees!(config, unflushed, child0, child1, child2)
                                }
                                Binary(..) => {
                                    panic!("illegal plan (ternary flush plan for a binary node)")
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn join(self, config: &TreeConfig, other: Self) -> Self {
        match (self.height, other.height) {
            // Join the leaf values, producing either a single new leaf or two
            // leaves under a new binary node.
            //
            (h, other_h) if h <= 1 && other_h <= 1 => {
                let left_vals: Arc<[K]> = self.root.1.consume_leaf();
                let right_vals: Arc<[K]> = other.root.1.consume_leaf();
                let n_vals = left_vals.len() + right_vals.len();

                Tree::from_vals(
                    config,
                    n_vals,
                    left_vals.iter().chain(right_vals.iter()).cloned(),
                )
            }

            // Grow the tree under a new binary node.
            //
            (h, other_h) if h != 1 && other_h == h => {
                return Self {
                    height: h + 1,
                    root: (
                        self.root.0,
                        Subtree::Branch(
                            Queue::default(), // TODO maybe allow this to pull from the calling context?
                            Arc::new(Node::Binary(self.root, other.root)),
                        ),
                    ),
                };
            }

            //  (h + 1, h) -> destructure self, merge other as right child
            //
            (h, other_h) if h == other_h + 1 => match self.root {
                (_, Subtree::Branch(queue, mut branch)) => match (&*branch, &other.root) {
                    (Node::Binary(b0, b1), b2) => {
                        return make_tree!(config, queue, h, b0, b1, b2);
                    }
                    (Node::Ternary(b0, b1, b2), b3) => {
                        return make_tree!(config, queue, h, b0, b1, b2, b3);
                    }
                },
                _ => panic!("self.root is leaf, but self.height > 0!"),
            },

            //  (h, h + 1) -> destructure other, merge self as left child
            (self_h, h) if self_h + 1 == h => match other.root {
                (_, Subtree::Branch(queue, mut branch)) => match (&self.root, &*branch) {
                    (b0, Node::Binary(b1, b2)) => {
                        return make_tree!(config, queue, h, b0, b1, b2);
                    }
                    (b0, Node::Ternary(b1, b2, b3)) => {
                        return make_tree!(config, queue, h, b0, b1, b2, b3);
                    }
                },
                _ => panic!("other.root is leaf, but other.height > 0!"),
            },

            //  (h + d, h), d > 1  -> recursive case
            //
            (h, other_h) if h > other_h + 1 => match self.root {
                (_, Subtree::Branch(queue, mut branch)) => {
                    match (trees_from_node(h, &*branch), other) {
                        (Node::Binary(b0, b1), b2) => {
                            let b1 = b1.join(config, b2);
                            return join_subtrees!(config, queue.consume(), b0, b1);
                        }
                        (Node::Ternary(b0, b1, b2), b3) => {
                            let b2 = b2.join(config, b3);
                            return join_subtrees!(config, queue.consume(), b0, b1, b2);
                        }
                    }
                }
                _ => panic!("self.root is leaf, but self.height > 1!"),
            },

            //  (h, h + d), d > 1  -> recursive case
            //
            (h_self, h) if h > h_self + 1 => match other.root {
                (_, Subtree::Branch(queue, mut branch)) => {
                    match (self, trees_from_node(h, &*branch)) {
                        (b0, Node::Binary(b1, b2)) => {
                            let b1 = b0.join(config, b1);
                            return join_subtrees!(config, queue.consume(), b1, b2);
                        }
                        (b0, Node::Ternary(b1, b2, b3)) => {
                            let b1 = b0.join(config, b1);
                            return join_subtrees!(config, queue.consume(), b1, b2, b3);
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
