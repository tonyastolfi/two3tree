#![allow(dead_code)]
#![allow(unused_macros)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use itertools::Itertools;

pub mod algo;
pub mod batch;
pub mod flush;
pub mod node;
pub mod partition;
pub mod queue;
pub mod sorted_updates;
pub mod subtree;
#[macro_use]
pub mod tree;
pub mod update;

use batch::Batch;
use queue::Queue;
use sorted_updates::SortedUpdates;
use tree::Tree;
use update::Update;

#[derive(Debug)]
pub struct TreeConfig {
    pub batch_size: usize,
}

pub type K = i32;
pub type Height = u16;

#[derive(Debug)]
pub struct TreeMut {
    pub config: TreeConfig,
    buffer: Queue,
    trunk: Tree,
}

impl TreeMut {
    pub fn new(config: TreeConfig) -> Self {
        Self {
            config,
            trunk: Tree::new(),
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
        let tmp = std::mem::replace(&mut self.trunk, Tree::new());
        self.trunk = tmp.update(&self.config, batch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_test() {
        assert_eq!(2 + 2, 4);

        let mut t = TreeMut::new(TreeConfig { batch_size: 8 });

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
        let mut t = TreeMut::new(TreeConfig { batch_size: 8 });
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

            let mut t = TreeMut::new(TreeConfig { batch_size: 8 });

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
