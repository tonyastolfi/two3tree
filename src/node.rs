pub struct TreeConfig {
    pub batch_size: usize,
}

#[derive(Debug)]
pub enum Subtree {
    Leaf { vals: Vec<i32> },
    Branch(Box<Node>),
    Nil,
}

#[derive(Debug)]
pub enum Node {
    Binary {
        left: Subtree,
        right_min: i32,
        right: Subtree,
    },
    Ternary {
        left: Subtree,
        middle_min: i32,
        middle: Subtree,
        right_min: i32,
        right: Subtree,
    },
    Nullary,
}
