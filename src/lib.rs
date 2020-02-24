#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]

#[derive(Clone, Debug)]
enum Node {
    Inner2 {
        left: Box<Node>,
        val: i32,
        right: Box<Node>,
    },
    Inner3 {
        left: Box<Node>,
        val1: i32,
        middle: Box<Node>,
        val2: i32,
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
    Split(i32, Box<Node>),
    Ok,
}

impl Node {
    fn insert(&mut self, new_val: i32) -> Insert {
        match self {
            Node::Nil => {
                *self = Node::Leaf2 { val: new_val };
                Insert::Ok
            }

            Node::Leaf2 { val } => {
                if new_val < *val {
                    *self = Node::Leaf3 {
                        val1: new_val,
                        val2: *val,
                    }
                } else {
                    *self = Node::Leaf3 {
                        val1: *val,
                        val2: new_val,
                    }
                }
                Insert::Ok
            }

            Node::Leaf3 { val1, val2 } => {
                if new_val < *val1 {
                    let result = Insert::Split(*val1, Box::new(Node::Leaf2 { val: *val2 }));
                    *self = Node::Leaf2 { val: new_val };
                    return result;
                } else if new_val < *val2 {
                    let result = Insert::Split(new_val, Box::new(Node::Leaf2 { val: *val2 }));
                    *self = Node::Leaf2 { val: *val1 };
                    return result;
                } else {
                    let result = Insert::Split(*val2, Box::new(Node::Leaf2 { val: new_val }));
                    *self = Node::Leaf2 { val: *val1 };
                    return result;
                }
            }

            Node::Inner2 { left, val, right } => {
                if new_val < *val {
                    match left.insert(new_val) {
                        Insert::Ok => {}
                        Insert::Split(split_val, second) => {
                            let tmp = Node::Inner3 {
                                left: std::mem::replace(left, Box::new(Node::Nil)),
                                val1: split_val,
                                middle: second,
                                val2: *val,
                                right: std::mem::replace(right, Box::new(Node::Nil)),
                            };
                            *self = tmp;
                        }
                    }
                } else {
                    match right.insert(new_val) {
                        Insert::Ok => {}
                        Insert::Split(split_val, second) => {
                            *self = Node::Inner3 {
                                left: (*left).clone(),
                                val1: *val,
                                middle: (*right).clone(),
                                val2: split_val,
                                right: second,
                            };
                        }
                    }
                }
                Insert::Ok
            }

            Node::Inner3 {
                left,
                val1,
                middle,
                val2,
                right,
            } => {
                if new_val < *val1 {
                    match left.insert(new_val) {
                        Insert::Ok => Insert::Ok,
                        Insert::Split(split_val, second) => {
                            let result = Insert::Split(
                                *val1,
                                Box::new(Node::Inner2 {
                                    left: std::mem::replace(middle, Box::new(Node::Nil)),
                                    val: *val2,
                                    right: std::mem::replace(right, Box::new(Node::Nil)),
                                }),
                            );
                            *self = Node::Inner2 {
                                left: std::mem::replace(left, Box::new(Node::Nil)),
                                val: split_val,
                                right: second,
                            };
                            result
                        }
                    }
                } else if new_val < *val2 {
                    match middle.insert(new_val) {
                        Insert::Ok => Insert::Ok,
                        Insert::Split(split_val, second) => {
                            let result = Insert::Split(
                                split_val,
                                Box::new(Node::Inner2 {
                                    left: second,
                                    val: *val2,
                                    right: std::mem::replace(right, Box::new(Node::Nil)),
                                }),
                            );
                            *self = Node::Inner2 {
                                left: std::mem::replace(left, Box::new(Node::Nil)),
                                val: *val1,
                                right: std::mem::replace(middle, Box::new(Node::Nil)),
                            };
                            result
                        }
                    }
                } else {
                    match right.insert(new_val) {
                        Insert::Ok => Insert::Ok,
                        Insert::Split(split_val, second) => {
                            let result = Insert::Split(
                                *val2,
                                Box::new(Node::Inner2 {
                                    left: std::mem::replace(right, Box::new(Node::Nil)),
                                    val: split_val,
                                    right: second,
                                }),
                            );
                            *self = Node::Inner2 {
                                left: std::mem::replace(left, Box::new(Node::Nil)),
                                val: *val1,
                                right: std::mem::replace(middle, Box::new(Node::Nil)),
                            };
                            result
                        }
                    }
                }
            }
        }
    }

    fn find<'a>(&'a self, key: i32) -> Option<&'a i32> {
        match self {
            Node::Nil => None,

            Node::Leaf2 { val } => {
                if key == *val {
                    Some(val)
                } else {
                    None
                }
            }

            Node::Leaf3 { val1, val2 } => {
                if key == *val1 {
                    Some(val1)
                } else if key == *val2 {
                    Some(val2)
                } else {
                    None
                }
            }

            Node::Inner2 { left, val, right } => {
                if key == *val {
                    Some(val)
                } else if key < *val {
                    left.find(key)
                } else {
                    right.find(key)
                }
            }

            Node::Inner3 {
                left,
                val1,
                middle,
                val2,
                right,
            } => {
                if key < *val1 {
                    left.find(key)
                } else if key > *val2 {
                    right.find(key)
                } else if key > *val1 {
                    if key < *val2 {
                        middle.find(key)
                    } else {
                        Some(val2)
                    }
                } else {
                    Some(val1)
                }
            }
        }
    }

    fn height(&self) -> usize {
        match self {
            Node::Nil => 0,
            Node::Leaf2 { .. } => 1,
            Node::Leaf3 { .. } => 1,
            Node::Inner2 { left, .. } => left.height() + 1,
            Node::Inner3 { left, .. } => left.height() + 1,
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
            root: Box::new(Node::Nil),
        }
    }
    fn insert(&mut self, val: i32) {
        match self.root.insert(val) {
            Insert::Ok => {}
            Insert::Split(split_val, second) => {
                let tmp = std::mem::replace(&mut self.root, Box::new(Node::Nil));
                self.root = Box::new(Node::Inner2 {
                    left: tmp,
                    val: split_val,
                    right: second,
                });
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
    fn it_works() {
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

        assert_eq!(t.height(), 9);

        for k in 1000..100000 {
            t.insert(k);
        }

        for k in 1000..100000 {
            assert!(t.find(k) == Some(&k));
        }

        assert_eq!(t.height(), 16);
    }
}
