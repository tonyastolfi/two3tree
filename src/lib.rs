#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use interval::Interval;

#[derive(Clone, Debug)]
enum Node {
    Inner2 {
        left: Box<Node>,
        right_min: i32,
        right: Box<Node>,
    },
    Inner3 {
        left: Box<Node>,
        middle_min: i32,
        middle: Box<Node>,
        right_min: i32,
        right: Box<Node>,
    },
    Leaf2 {
        val: i32,
    },
    Leaf3 {
        val1: i32,
        val2: i32,
    },
    Nil,
}

enum Insert {
    Ok,
    Split(i32, Box<Node>),
}

enum Remove {
    NotFound,
    Ok,
    Drained,
    Orphaned(Box<Node>),
}

enum SubtreeBound {
    Lower(i32),
    Upper(i32),
}

use Insert::Split;
use Node::{Inner2, Inner3, Leaf2, Leaf3, Nil};

impl Node {
    fn remove(&mut self, rm_val: i32) -> Remove {
        use Remove::{Drained, NotFound, Ok, Orphaned};
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
                            Insert::Ok => {
                                return Orphaned(right);
                            }
                            Insert::Split(split_min, split) => {
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
                            Insert::Ok => {
                                return Orphaned(left);
                            }
                            Insert::Split(split_min, split) => {
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
                                Insert::Ok => {
                                    *self = Inner2 {
                                        left: middle,
                                        right_min,
                                        right,
                                    };
                                }
                                Split(split_min, split) => {
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
                                Insert::Ok => {
                                    *self = Inner2 {
                                        left,
                                        right_min,
                                        right,
                                    };
                                }
                                Split(split_min, split) => {
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
                                Insert::Ok => {
                                    *self = Inner2 {
                                        left,
                                        right_min: middle_min,
                                        right: middle,
                                    };
                                }
                                Split(split_min, split) => {
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
    fn merge_left(&mut self, subtree: Box<Node>, left_min: i32) -> Insert {
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
            return Insert::Ok;
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
            return Split(
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

    fn merge_right(&mut self, subtree_min: i32, subtree: Box<Node>) -> Insert {
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
            return Insert::Ok;
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
            return Split(
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

    fn insert(&mut self, new_val: i32) -> Insert {
        match std::mem::replace(self, Nil) {
            Nil => {
                *self = Leaf2 { val: new_val };
                Insert::Ok
            }

            Leaf2 { val } => {
                *self = Leaf3 {
                    val1: std::cmp::min(new_val, val),
                    val2: std::cmp::max(new_val, val),
                };
                Insert::Ok
            }

            Leaf3 { val1, val2 } => {
                if new_val < val1 {
                    *self = Leaf2 { val: new_val };
                    Split(val1, Box::new(Leaf3 { val1, val2 }))
                } else if new_val < val2 {
                    *self = Leaf2 { val: val1 };
                    Split(
                        new_val,
                        Box::new(Leaf3 {
                            val1: new_val,
                            val2,
                        }),
                    )
                } else {
                    *self = Leaf2 { val: val1 };
                    Split(
                        val2,
                        Box::new(Leaf3 {
                            val1: val2,
                            val2: new_val,
                        }),
                    )
                }
            }

            Inner2 {
                mut left,
                right_min,
                mut right,
            } => {
                if new_val < right_min {
                    if let Split(split_min, split) = left.insert(new_val) {
                        *self = Inner3 {
                            left,
                            middle_min: split_min,
                            middle: split,
                            right_min,
                            right,
                        };
                        return Insert::Ok;
                    }
                } else {
                    if let Split(split_min, split) = right.insert(new_val) {
                        *self = Inner3 {
                            left,
                            middle_min: right_min,
                            middle: right,
                            right_min: split_min,
                            right: split,
                        };
                        return Insert::Ok;
                    }
                }
                *self = Inner2 {
                    left,
                    right_min,
                    right,
                };
                Insert::Ok
            }

            Inner3 {
                mut left,
                middle_min,
                mut middle,
                right_min,
                mut right,
            } => {
                if new_val < middle_min {
                    if let Split(split_min, split) = left.insert(new_val) {
                        *self = Inner2 {
                            left,
                            right_min: split_min,
                            right: split,
                        };
                        return Split(
                            middle_min,
                            Box::new(Inner2 {
                                left: middle,
                                right_min,
                                right,
                            }),
                        );
                    }
                } else if new_val < right_min {
                    if let Split(split_min, split) = middle.insert(new_val) {
                        *self = Inner2 {
                            left,
                            right_min: middle_min,
                            right: middle,
                        };
                        return Split(
                            split_min,
                            Box::new(Inner2 {
                                left: split,
                                right_min,
                                right,
                            }),
                        );
                    }
                } else {
                    if let Split(split_min, split) = right.insert(new_val) {
                        *self = Inner2 {
                            left,
                            right_min: middle_min,
                            right: middle,
                        };
                        return Split(
                            right_min,
                            Box::new(Inner2 {
                                left: right,
                                right_min: split_min,
                                right: split,
                            }),
                        );
                    }
                }
                *self = Inner3 {
                    left,
                    middle_min,
                    middle,
                    right_min,
                    right,
                };
                Insert::Ok
            }
        }
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
            Insert::Ok => {}
            Split(split_min, split) => {
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
        use Remove::{Drained, NotFound, Ok, Orphaned};
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
