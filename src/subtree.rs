use std::sync::Arc;

use crate::node::Node;
use crate::queue::Queue;
use crate::{Height, TreeConfig};

#[derive(Debug)]
pub enum Subtree<K> {
    Leaf(Arc<[K]>),
    Branch(Queue<K>, Arc<Node<(K, Subtree<K>)>>),
}

impl<K> Subtree<K> {
    pub fn consume_leaf(self) -> Vec<K> {
        match self {
            Subtree::Leaf(vals) => (*vals).into(),
            _ => panic!("not a Leaf!"),
        }
    }

    pub fn check_height(&self, config: &TreeConfig) -> Height {
        match self {
            Subtree::Leaf(vals) => {
                if vals.len() < config.batch_size {
                    0
                } else {
                    1
                }
            }
            Subtree::Branch(_, ref branch) => match &**branch {
                Node::Binary(b0, _, b1) => {
                    let h0 = b0.check_height(config);
                    let h1 = b1.check_height(config);
                    assert_eq!(h0, h1);
                    h0 + 1
                }
                Node::Ternary(b0, _, b1, _, b2) => {
                    let h0 = b0.check_height(config);
                    let h1 = b1.check_height(config);
                    let h2 = b2.check_height(config);
                    assert_eq!(h0, h1);
                    assert_eq!(h1, h2);
                    h0 + 1
                }
            },
        }
    }

    pub fn check_invariants(
        &self,
        config: &TreeConfig,
        height: Height,
        deep: bool,
        info: Option<&str>,
    ) {
        match self {
            Subtree::Leaf(ref vals) => {
                assert!(
                    (vals.len() >= config.batch_size && height == 1)
                        || (height == 0 && vals.len() < config.batch_size),
                    "leaf too small: {:?}, {:?}",
                    vals,
                    info
                );
                assert!(
                    vals.len() <= config.batch_size * 2,
                    "leaf too big: {:?}, {:?}",
                    vals,
                    info
                );
            }
            Subtree::Branch(ref queue, ref branch) => {
                assert!(height > 1, "all non-leaf children must be at height > 1");
                let node = &**branch;
                match node {
                    Node::Binary(b0, _, b1) => {
                        assert!(
                            queue.len() <= config.batch_size,
                            "queue is over-full: B={}, partition={:?}, queue={:?}, old={:?}",
                            config.batch_size,
                            node.partition(queue),
                            queue,
                            info,
                        );
                        if deep {
                            b0.check_invariants(config, height - 1, deep, None);
                            b1.check_invariants(config, height - 1, deep, None);
                        }
                    }
                    Node::Ternary(b0, _, b1, _, b2) => {
                        assert!(queue.len() <= config.batch_size * 3 / 2);
                        if let Node::Ternary(n0, _, n1, _, n2) = node.partition(queue) {
                            assert!(n0 + n1 <= config.batch_size);
                            assert!(n1 + n2 <= config.batch_size);
                        } else {
                            panic!("Queue::partition should have returned a ternary node");
                        }
                        if deep {
                            b0.check_invariants(config, height - 1, deep, None);
                            b1.check_invariants(config, height - 1, deep, None);
                            b2.check_invariants(config, height - 1, deep, None);
                        }
                    }
                }
            }
        }
    }

    pub fn find(&self, key: &K) -> Option<&K> {
        match self {
            Subtree::Leaf(vals) => match vals.binary_search(key) {
                Ok(index) => Some(&vals[index]),
                Err(_) => None,
            },
            Subtree::Branch(ref queue, ref branch) => match queue.find(key) {
                Some(ref update) => update.resolve(),
                None => match &**branch {
                    Node::Binary(b0, m1, b1) => {
                        if key < m1 {
                            b0.find(key)
                        } else {
                            b1.find(key)
                        }
                    }
                    Node::Ternary(b0, m1, b1, m2, b2) => {
                        if key < m1 {
                            b0.find(key)
                        } else if key < m2 {
                            b1.find(key)
                        } else {
                            b2.find(key)
                        }
                    }
                },
            },
        }
    }

    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a> {
        use Subtree::{Branch, Leaf};

        match self {
            Leaf(vals) => Box::new(vals.iter()),
            Branch(ref queue, ref branch) => {
                let node = &**branch;
                Box::new(queue.merge_iter(node.iter()))
            }
        }
    }
}
