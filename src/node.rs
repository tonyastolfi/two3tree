#[derive(Clone, Debug)]
pub enum Node {
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
