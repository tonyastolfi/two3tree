#![allow(dead_code)]
#![allow(unused_macros)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use itertools::Itertools;

pub type K = i32;

pub mod algo;
pub mod batch;
pub mod flush;
pub mod node;
pub mod partition;
pub mod queue;
pub mod sorted_updates;
pub mod update;

use algo::{lower_bound_by_key, upper_bound_by_key};
use batch::Batch;
use flush::{plan_flush, Flush};
use node::Node;
use partition::partition;
use queue::Queue;
use sorted_updates::SortedUpdates;
use update::Update;

#[derive(Debug)]
pub struct TreeConfig {
    pub batch_size: usize,
}

#[derive(Debug)]
pub enum Child {
    Leaf(Vec<K>),
    Branch(Queue, Box<Node<Child, K>>),
}

impl Child {
    pub fn consume_leaf(self) -> Vec<K> {
        match self {
            Child::Leaf(vals) => vals,
            _ => panic!("not a Leaf!"),
        }
    }

    pub fn check_height(&self, config: &TreeConfig) -> Height {
        match self {
            Child::Leaf(vals) => {
                if vals.len() < config.batch_size {
                    0
                } else {
                    1
                }
            }
            Child::Branch(_, ref branch) => match &**branch {
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
            Child::Leaf(ref vals) => {
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
            Child::Branch(ref queue, ref branch) => {
                assert!(height > 1, "all non-leaf children must be at height > 1");
                let node = &**branch;
                match node {
                    Node::Binary(b0, _, b1) => {
                        assert!(
                            queue.len() <= config.batch_size,
                            "queue is over-full: B={}, partition={:?}, queue={:?}, old={:?}",
                            config.batch_size,
                            partition(queue, &node),
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
                        if let Node::Ternary(n0, _, n1, _, n2) = partition(queue, node) {
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
            Child::Leaf(vals) => match vals.binary_search(key) {
                Ok(index) => Some(&vals[index]),
                Err(_) => None,
            },
            Child::Branch(ref queue, ref branch) => match queue.find(key) {
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
        use Child::{Branch, Leaf};

        match self {
            Leaf(vals) => Box::new(vals.iter()),
            Branch(ref queue, ref branch) => {
                let node = &**branch;
                Box::new(queue.merge_iter(node.iter()))
            }
        }
    }
}

pub type Height = u16;

#[derive(Debug)]
pub struct Subtree {
    height: Height,
    root: Child,
}

fn subtrees_from_node(height: Height, node: Node<Child, K>) -> Node<Subtree, K> {
    match node {
        Node::Binary(b0, m1, b1) => Node::Binary(
            Subtree {
                height: height - 1,
                root: b0,
            },
            m1,
            Subtree {
                height: height - 1,
                root: b1,
            },
        ),
        Node::Ternary(b0, m1, b1, m2, b2) => Node::Ternary(
            Subtree {
                height: height - 1,
                root: b0,
            },
            m1,
            Subtree {
                height: height - 1,
                root: b1,
            },
            m2,
            Subtree {
                height: height - 1,
                root: b2,
            },
        ),
    }
}

fn node_from_subtrees(subtrees: Node<Subtree, K>) -> Node<Child, K> {
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

macro_rules! update_branch {
    ($queue:expr, $branch:expr, $b0:expr, $m1:expr, $b1:expr) => {
        Child::Branch($queue, {
            *$branch = Node::Binary($b0, $m1, $b1);
            $branch
        })
    };
    ($queue:expr, $branch:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr) => {
        Child::Branch($queue, {
            *$branch = Node::Ternary($b0, $m1, $b1, $m2, $b2);
            $branch
        })
    };
}

macro_rules! rebuild_tree {
    ($config:expr, $height:expr, $updates:expr, $branch:expr, $subtrees:expr) => {
        Subtree {
            height: $height,
            root: Queue::with_no_flush($config, $updates, {
                *$branch = node_from_subtrees($subtrees);
                $branch
            }),
        }
    };
}

macro_rules! make_tree {
    ($config:expr, $queue:expr, $branch:expr, $h:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr) => {{
        Subtree {
            height: $h,
            root: update_branch!($queue, $branch, $b0, $m1, $b1, $m2, $b2),
        }
    }};
    ($config:expr, $queue:expr, $branch:expr, $h:expr, $b0:expr, $m1:expr, $b1:expr, $m2:expr, $b2:expr, $m3:expr, $b3:expr) => {{
        let (q01, q23) = $queue.split_at_key($config, &$m2);
        Subtree {
            height: $h + 1,
            root: Child::Branch(
                Queue::default(),
                Box::new(Node::Binary(
                    update_branch!(q01, $branch, $b0, $m1, $b1),
                    $m2,
                    Child::Branch(q23, Box::new(Node::Binary($b2, $m3, $b3))),
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

impl Subtree {
    pub fn new() -> Self {
        Self {
            height: 0,
            root: Child::Leaf(vec![]),
        }
    }

    pub fn from_vals(config: &TreeConfig, mut vals: Vec<K>) -> Self {
        if vals.len() <= config.batch_size * 2 {
            return Self {
                height: if vals.len() < config.batch_size { 0 } else { 1 },
                root: Child::Leaf(vals),
            };
        }
        let split_vals: Vec<i32> = vals.split_off(vals.len() / 2);
        let split_min: i32 = split_vals[0];
        return Self {
            height: 2,
            root: Child::Branch(
                Queue::default(),
                Box::new(Node::Binary(
                    Child::Leaf(vals),
                    split_min,
                    Child::Leaf(split_vals),
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
            Child::Leaf(vals) => {
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

                Subtree::from_vals(config, merged)
            }
            Child::Branch(queue, mut branch) => {
                use Node::{Binary, Ternary};

                if queue.is_empty() && updates.len() <= config.batch_size {
                    Self {
                        height: self.height,
                        root: Child::Branch(Queue::new(config, updates), branch),
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
                        subtrees_from_node(self.height, *branch),
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
                            node_from_subtrees(subtrees)
                        ),
                    }
                }
            }
        }
    }

    pub fn join(self, config: &TreeConfig, other_min: K, other: Subtree) -> Self {
        match (self.height, other.height) {
            // Join the leaf values, producing either a single new leaf or two
            // leaves under a new binary node.
            //
            (h, other_h) if h <= 1 && other_h <= 1 => {
                let mut vals = self.root.consume_leaf();
                vals.append(&mut other.root.consume_leaf());
                Subtree::from_vals(config, vals)
            }

            // Grow the tree under a new binary node.
            //
            (h, other_h) if h != 1 && other_h == h => {
                return Self {
                    height: h + 1,
                    root: Child::Branch(
                        Queue::default(), // TODO maybe allow this to pull from the calling context?
                        Box::new(Node::Binary(self.root, other_min, other.root)),
                    ),
                };
            }

            //  (h + 1, h) -> destructure self, merge other as right child
            //
            (h, other_h) if h == other_h + 1 => match self.root {
                Child::Branch(queue, mut branch) => match (*branch, other_min, other.root) {
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
                Child::Branch(queue, mut branch) => match (self.root, other_min, *branch) {
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
                Child::Branch(queue, mut branch) => {
                    match (subtrees_from_node(h, *branch), other_min, other) {
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
                Child::Branch(queue, mut branch) => {
                    match (self, other_min, subtrees_from_node(h, *branch)) {
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

#[derive(Debug)]
pub struct Tree {
    pub config: TreeConfig,
    buffer: Queue,
    trunk: Subtree,
}

impl Tree {
    pub fn new(config: TreeConfig) -> Self {
        Self {
            config,
            trunk: Subtree::new(),
            buffer: Queue::default(),
        }
    }

    pub fn check_invariants(&self) {
        self.trunk.check_invariants(&self.config);
    }

    pub fn height(&self) -> Height {
        self.trunk.height()
    }

    pub fn find(&self, key: K) -> Option<&K> {
        match self.buffer.find(&key) {
            Some(ref update) => update.resolve(),
            None => self.trunk.find(&key),
        }
    }

    fn to_vec(&self) -> Vec<i32> {
        self.buffer
            .merge_iter(self.trunk.iter())
            .map(|k_ref| *k_ref)
            .collect()
    }

    pub fn insert(&mut self, key: K) {
        self.update_one(Update::Put(key));
    }

    pub fn remove(&mut self, key: K) {
        self.update_one(Update::Delete(key));
    }

    pub fn update_one(&mut self, v: Update) {
        if let Some(batch) = self.buffer.push(&self.config, v) {
            self.update(batch);
        }
    }

    pub fn update(&mut self, batch: Batch) {
        let tmp = std::mem::replace(&mut self.trunk, Subtree::new());
        self.trunk = tmp.update(&self.config, batch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_test() {
        assert_eq!(2 + 2, 4);

        let mut t = Tree::new(TreeConfig { batch_size: 8 });

        assert!(t.find(10) == None);

        t.insert(10);
        t.check_invariants();

        assert_eq!(t.find(10), Some(&10));

        for k in 0..1000 {
            t.insert(k);
            t.check_invariants();
        }

        for k in 0..1000 {
            assert_eq!(t.find(k), Some(&k));
        }

        assert_eq!(t.height(), 6);

        for k in 1000..100000 {
            t.insert(k);
        }
        t.check_invariants();

        for k in 1000..100000 {
            assert_eq!(t.find(k), Some(&k));
        }

        assert_eq!(t.height(), 13);
    }

    #[test]
    fn remove_test() {
        let mut t = Tree::new(TreeConfig { batch_size: 8 });
        let max_k: i32 = 100000;

        for k in 0..max_k {
            t.insert(k);
        }

        for k in 0..max_k {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 13);

        for k in 0..max_k {
            assert!(t.find(k) == Some(&k));
            t.remove(k);
            t.check_invariants();
            assert!(t.find(k) == None, "k={}, tree={:#?}", k, t);
        }

        for k in 0..max_k {
            assert!(t.find(k) == None);
        }

        assert_eq!(t.height(), 0);
    }

    #[test]
    fn random_update_test() {
        use rand::distributions::{Distribution, Uniform};
        use rand::prelude::*;

        let mut rng = rand::thread_rng();
        for n in 0..100000 {
            let mut x: Vec<Update> = (0..1024).map(Update::Put).collect();
            let mut y: Vec<Update> = Vec::new();

            if n % 100 == 0 {
                eprintln!("{}", n);
            }

            while !x.is_empty() {
                let i = Uniform::from(0..x.len()).sample(&mut rng);
                let c: Update = x[i];
                match c {
                    Update::Put(k) => {
                        x[i] = Update::Delete(k);
                    }
                    Update::Delete(k) => {
                        x.remove(i);
                    }
                }
                y.push(c);
            }

            let mut t = Tree::new(TreeConfig { batch_size: 8 });

            let batches: Vec<Vec<Update>> = y
                .chunks(t.config.batch_size)
                .map(|chunk| {
                    let mut tmp: Vec<Update> = Vec::from(chunk);
                    tmp.sort_by_key(|update| *update.key());
                    let mut batch: Vec<Update> = Vec::new();
                    for i in 0..(tmp.len() - 1) {
                        if tmp[i].key() != tmp[i + 1].key() {
                            batch.push(tmp[i]);
                        }
                    }
                    batch.push(tmp[tmp.len() - 1]);
                    batch
                })
                .collect();

            use std::collections::BTreeSet;

            let mut good: BTreeSet<i32> = BTreeSet::new();

            let mut max_height = 0;

            for batch in &batches {
                for update in batch {
                    match &update {
                        Update::Put(k) => {
                            good.insert(*k);
                        }
                        Update::Delete(k) => {
                            good.remove(k);
                        }
                    }
                }
                t.update(Batch::new(&t.config, SortedUpdates::new(batch.clone())).unwrap());
                max_height = std::cmp::max(max_height, t.height());
                assert_eq!(
                    t.to_vec(),
                    good.iter().map(|x| *x).collect::<Vec<i32>>(),
                    "t={:#?}",
                    t
                );
                t.check_invariants();
            }

            assert!(max_height >= 4, "max_height={}", max_height);
        }
    }
}
