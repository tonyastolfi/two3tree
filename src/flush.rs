use crate::batch::Batch;
use crate::node::Node;
use crate::TreeConfig;
use crate::K;

pub trait Flush {
    fn flush(
        &mut self,
        config: &TreeConfig,
        partition: &Node<usize, K>,
        plan: &Node<Option<usize>, ()>,
    ) -> Node<Option<Batch>, ()>;
}

pub fn plan_flush<T>(config: &TreeConfig, partition: &Node<usize, T>) -> Node<Option<usize>, ()> {
    let take_batch = |n: usize| {
        if n < config.batch_size / 2 {
            None
        } else if n > config.batch_size {
            Some(config.batch_size)
        } else {
            Some(n)
        }
    };

    match partition {
        Node::Binary(n0, _, n1) => {
            assert!(n0 + n1 <= 2 * config.batch_size);

            if n0 + n1 <= config.batch_size {
                Node::Binary(None, (), None)
            } else {
                if n0 >= n1 {
                    Node::Binary(take_batch(*n0), (), None)
                } else {
                    Node::Binary(None, (), take_batch(*n1))
                }
            }
        }
        Node::Ternary(n0, _, n1, _, n2) => {
            let total = n0 + n1 + n2;

            if total <= config.batch_size {
                Node::Ternary(None, (), None, (), None)
            } else {
                match (take_batch(*n0), take_batch(*n1), take_batch(*n2)) {
                    (Some(y0), Some(y1), Some(y2)) => {
                        if y0 <= y1 && y0 <= y2 {
                            Node::Ternary(None, (), Some(y1), (), Some(y2))
                        } else if y1 <= y0 && y1 <= y2 {
                            Node::Ternary(Some(y0), (), None, (), Some(y2))
                        } else {
                            Node::Ternary(Some(y0), (), Some(y1), (), None)
                        }
                    }
                    (b0, b1, b2) => Node::Ternary(b0, (), b1, (), b2),
                }
            }
        }
    }
}
