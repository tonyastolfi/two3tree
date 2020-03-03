#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

mod node;
use crate::node::{Node, Subtree, TreeConfig};

mod update;
use crate::update::{update_leaf, MergeResult, Update, UpdateResult};

use itertools::Itertools;

pub trait BatchUpdate {
    fn update(&mut self, config: &TreeConfig, batch: Vec<Update>) -> UpdateResult;
}

pub trait SubtreeMerge {
    fn merge_left(
        &mut self, //
        config: &TreeConfig,
        subtree: Subtree,
        left_min: i32,
    ) -> MergeResult;

    fn merge_right(
        &mut self, //
        config: &TreeConfig,
        subtree_min: i32,
        subtree: Subtree,
    ) -> MergeResult;
}

impl Subtree {
    fn update(&mut self, config: &TreeConfig, batch: Vec<Update>) -> UpdateResult {
        use Node::{Binary, Nullary, Ternary};
        use Subtree::{Branch, Leaf, Nil};
        use UpdateResult::{Done, Merge, Split};

        match std::mem::replace(self, Nil) {
            Nil => {
                *self = Leaf {
                    vals: batch.iter().filter_map(|update| update.resolve()).collect(),
                };
                Done
            }
            Leaf { vals } => {
                let (leaf, result) = update_leaf(config, batch, vals);
                *self = leaf;
                return result;
            }
            Branch(mut box_node) => match *box_node {
                Binary {
                    left,
                    right_min,
                    right,
                } => Done,
                Ternary {
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => Done,
                Nullary => Done,
            },
        }
    }

    fn merge_left(&mut self, config: &TreeConfig, subtree: Subtree, left_min: i32) -> MergeResult {
        use MergeResult::Done;
        use Subtree::{Branch, Leaf, Nil};

        match std::mem::replace(self, Nil) {
            Nil => panic!("Merging a subtree with Nil does not produce a valid subtree!"),
            Leaf { vals } => {
                if let Leaf {
                    vals: mut subtree_vals,
                } = subtree
                {
                    let (leaf, result) = update_leaf(
                        config,
                        subtree_vals.drain(..).map(|val| Update::Put(val)).collect(),
                        vals,
                    );
                    *self = leaf;
                    Done
                } else {
                    panic!("Tried to merge a leaf with a non-leaf!");
                }
            }
            Branch(box_node) => match *box_node {
                Node::Binary {
                    left,
                    right_min,
                    right,
                } => Done,
                Node::Ternary {
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                } => Done,
                Node::Nullary => Done,
            },
        }
    }

    fn merge_right(
        &mut self,
        config: &TreeConfig,
        subtree_min: i32,
        subtree: Subtree,
    ) -> MergeResult {
        use MergeResult::Done;
        use Subtree::{Branch, Leaf, Nil};

        Done
    }
}

/*
impl Node {
    fn update(&mut self, &config: TreeConfig, batch: Vec<Update>) -> UpdateResult {
        assert_eq!(batch.len(), config.batch_size);

        match std::mem::replace(self, Nil) {
            Inner2 {
                mut left,
                right_min,
                mut right,
            } => {
                let (node, result) = insert_inner2(new_val, left, right_min, right);
                *self = node;
                return result;
            }

            Inner3 {
                mut left,
                middle_min,
                mut middle,
                right_min,
                mut right,
            } => {
                let (node, result) =
                    insert_inner3(new_val, left, middle_min, middle, right_min, right);
                *self = node;
                return result;
            }
        }
    }

    fn remove(&mut self, rm_val: i32) -> RemoveResult {
        use RemoveResult::{Drained, NotFound, Ok, Orphaned};
        match std::mem::replace(self, Nil) {
            Nil => NotFound,
            Leaf2 { val } => {
                if rm_val == val {
                    // self stays Nil
                    Drained
                } else {
                    *self = Leaf2 { val };
                    NotFound
                }
            }
            Leaf3 { val1, val2 } => {
                if rm_val == val1 {
                    *self = Leaf2 { val: val2 };
                    Ok
                } else if rm_val == val2 {
                    *self = Leaf2 { val: val1 };
                    Ok
                } else {
                    *self = Leaf3 { val1, val2 };
                    NotFound
                }
            }
            Inner2 {
                mut left,
                right_min,
                mut right,
            } => {
                let result = if rm_val < right_min {
                    match left.remove(rm_val) {
                        NotFound => NotFound,
                        Ok => Ok,
                        Drained => {
                            return Orphaned(right);
                        }
                        Orphaned(to_merge) => match right.merge_left(to_merge, right_min) {
                            InsertResult::Ok => {
                                return Orphaned(right);
                            }
                            InsertResult::Split(split_min, split) => {
                                *self = Inner2 {
                                    left: right,
                                    right_min: split_min,
                                    right: split,
                                };
                                return Ok;
                            }
                        },
                    }
                } else {
                    match right.remove(rm_val) {
                        NotFound => NotFound,
                        Ok => Ok,
                        Drained => {
                            return Orphaned(left);
                        }
                        Orphaned(to_merge) => match left.merge_right(right_min, to_merge) {
                            InsertResult::Ok => {
                                return Orphaned(left);
                            }
                            InsertResult::Split(split_min, split) => {
                                *self = Inner2 {
                                    left,
                                    right_min: split_min,
                                    right: split,
                                };
                                return Ok;
                            }
                        },
                    }
                };
                *self = Inner2 {
                    left,
                    right_min,
                    right,
                };
                return result;
            }
            Inner3 {
                mut left,
                middle_min,
                mut middle,
                right_min,
                mut right,
            } => {
                let result = if rm_val < middle_min {
                    match left.remove(rm_val) {
                        Ok => Ok,
                        NotFound => NotFound,
                        Drained => {
                            *self = Inner2 {
                                left: middle,
                                right_min,
                                right,
                            };
                            return Ok;
                        }
                        Orphaned(to_merge) => {
                            match middle.merge_left(to_merge, middle_min) {
                                InsertResult::Ok => {
                                    *self = Inner2 {
                                        left: middle,
                                        right_min,
                                        right,
                                    };
                                }
                                InsertResult::Split(split_min, split) => {
                                    *self = Inner3 {
                                        left: middle,
                                        middle_min: split_min,
                                        middle: split,
                                        right_min,
                                        right,
                                    };
                                }
                            }
                            return Ok;
                        }
                    }
                } else if rm_val < right_min {
                    match middle.remove(rm_val) {
                        Ok => Ok,
                        NotFound => NotFound,
                        Drained => {
                            *self = Inner2 {
                                left,
                                right_min,
                                right,
                            };
                            return Ok;
                        }
                        Orphaned(to_merge) => {
                            match right.merge_left(to_merge, right_min) {
                                InsertResult::Ok => {
                                    *self = Inner2 {
                                        left,
                                        right_min,
                                        right,
                                    };
                                }
                                InsertResult::Split(split_min, split) => {
                                    *self = Inner3 {
                                        left,
                                        middle_min: right_min,
                                        middle: right,
                                        right_min: split_min,
                                        right: split,
                                    };
                                }
                            }
                            return Ok;
                        }
                    }
                } else {
                    match right.remove(rm_val) {
                        Ok => Ok,
                        NotFound => NotFound,
                        Drained => {
                            *self = Inner2 {
                                left,
                                right_min: middle_min,
                                right: middle,
                            };
                            return Ok;
                        }
                        Orphaned(to_merge) => {
                            match middle.merge_right(right_min, to_merge) {
                                InsertResult::Ok => {
                                    *self = Inner2 {
                                        left,
                                        right_min: middle_min,
                                        right: middle,
                                    };
                                }
                                InsertResult::Split(split_min, split) => {
                                    *self = Inner3 {
                                        left,
                                        middle_min,
                                        middle,
                                        right_min: split_min,
                                        right: split,
                                    };
                                }
                            }
                            return Ok;
                        }
                    }
                };
                *self = Inner3 {
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                };
                return result;
            }
        }
    }

    // Merges subtree as a child on the left side of this node; may result in a split.
    //
    fn merge_left(&mut self, subtree: Box<Node>, left_min: i32) -> InsertResult {
        let node = std::mem::replace(self, Nil);
        if let Inner2 {
            left,
            right_min,
            right,
        } = node
        {
            *self = Inner3 {
                left: subtree,
                middle_min: left_min,
                middle: left,
                right_min,
                right,
            };
            return InsertResult::Ok;
        }
        if let Inner3 {
            left,
            middle_min,
            middle,
            right_min,
            right,
        } = node
        {
            *self = Inner2 {
                left: subtree,
                right_min: left_min,
                right: left,
            };
            return InsertResult::Split(
                middle_min,
                Box::new(Inner2 {
                    left: middle,
                    right_min,
                    right,
                }),
            );
        }
        panic!("insert_subtree may only be called on an inner node!")
    }

    fn merge_right(&mut self, subtree_min: i32, subtree: Box<Node>) -> InsertResult {
        let node = std::mem::replace(self, Nil);
        if let Inner2 {
            left,
            right_min,
            right,
        } = node
        {
            *self = Inner3 {
                left,
                middle_min: right_min,
                middle: right,
                right_min: subtree_min,
                right: subtree,
            };
            return InsertResult::Ok;
        }
        if let Inner3 {
            left,
            middle_min,
            middle,
            right_min,
            right,
        } = node
        {
            *self = Inner2 {
                left,
                right_min: middle_min,
                right: middle,
            };
            return InsertResult::Split(
                right_min,
                Box::new(Inner2 {
                    left: right,
                    right_min: subtree_min,
                    right: subtree,
                }),
            );
        }
        panic!("insert_subtree may only be called on an inner node!")
    }

    fn find<'a>(&'a self, key: i32) -> Option<&'a i32> {
        match self {
            Nil => None,

            Leaf2 { val } => {
                if key == *val {
                    Some(val)
                } else {
                    None
                }
            }

            Leaf3 { val1, val2 } => {
                if key == *val1 {
                    Some(val1)
                } else if key == *val2 {
                    Some(val2)
                } else {
                    None
                }
            }

            Inner2 {
                left,
                right_min,
                right,
            } => {
                if key < *right_min {
                    left.find(key)
                } else {
                    right.find(key)
                }
            }

            Inner3 {
                left,
                middle_min,
                middle,
                right_min,
                right,
            } => {
                if key < *middle_min {
                    left.find(key)
                } else if key < *right_min {
                    middle.find(key)
                } else {
                    right.find(key)
                }
            }
        }
    }

    fn height(&self) -> usize {
        match self {
            Nil => 0,
            Leaf2 { .. } => 1,
            Leaf3 { .. } => 1,
            Inner2 { left, .. } => left.height() + 1,
            Inner3 { left, .. } => left.height() + 1,
        }
    }
}

#[derive(Debug)]
struct Tree {
    root: Box<Node>,
}

impl Tree {
    fn new() -> Self {
        Self {
            root: Box::new(Nil),
        }
    }
    fn insert(&mut self, val: i32) {
        match self.root.insert(val) {
            InsertResult::Ok => {}
            InsertResult::Split(split_min, split) => {
                let tmp = std::mem::replace(&mut self.root, Box::new(Nil));
                self.root = Box::new(Inner2 {
                    left: tmp,
                    right_min: split_min,
                    right: split,
                });
            }
        }
    }
    fn remove(&mut self, val: i32) {
        use RemoveResult::{Drained, NotFound, Ok, Orphaned};
        match self.root.remove(val) {
            NotFound => {
                println!("remove => NotFound");
            }
            Ok => {
                println!("remove => Ok");
            }
            Drained => {
                std::mem::replace(&mut self.root, Box::new(Nil));
                println!("remove => Drained");
            }
            Orphaned(new_root) => {
                std::mem::replace(&mut self.root, new_root);
                println!("remove => Orphaned");
            }
        }
    }
    fn find<'a>(&'a self, val: i32) -> Option<&'a i32> {
        self.root.find(val)
    }
    fn height(&self) -> usize {
        self.root.height()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_test() {
        assert_eq!(2 + 2, 4);

        let mut t = Tree::new();

        assert!(t.find(10) == None);

        t.insert(10);
        println!("{:#?}", t);

        assert_eq!(t.find(10), Some(&10));

        for k in 0..1000 {
            t.insert(k);
        }

        for k in 0..1000 {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 10);

        for k in 1000..100000 {
            t.insert(k);
        }

        for k in 1000..100000 {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 17);
    }

    #[test]
    fn remove_test() {
        let mut t = Tree::new();

        for k in 0..100000 {
            t.insert(k);
        }

        for k in 0..100000 {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 17);

        for k in 0..100000 {
            assert!(t.find(k) == Some(&k));
            t.remove(k);
            assert!(t.find(k) == None, "k={}, tree={:#?}", k, t);
        }

        for k in 0..100000 {
            assert!(t.find(k) == None);
        }

        assert_eq!(t.height(), 0);
    }
}
*/
