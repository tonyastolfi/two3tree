use std::ops::Range;

use crate::batch::Batch;
use crate::node::Node;
use crate::TreeConfig;
use crate::K;

pub type FlushPlan = Node<Option<Range<usize>>>;

pub trait Flush<K> {
    fn flush(&mut self, config: &TreeConfig, plan: &FlushPlan) -> Node<Option<Batch<K>>>;
}

// TODO - input and output Node<RangeBounds<usize>>
pub fn plan_flush(config: &TreeConfig, partition: &Node<Range<usize>>) -> FlushPlan {
    let take_batch = |r: &Range<usize>| -> Option<Range<usize>> {
        if r.len() < config.batch_size / 2 {
            None
        } else if r.len() > config.batch_size {
            Some(Range {
                start: r.start,
                end: r.start + config.batch_size,
            })
        } else {
            Some(r.clone())
        }
    };

    match partition {
        Node::Binary(r0, r1) => {
            assert!(r0.len() + r1.len() <= 2 * config.batch_size);

            if r0.len() + r1.len() <= config.batch_size {
                Node::Binary(None, None)
            } else {
                if r0.len() >= r1.len() {
                    Node::Binary(take_batch(r0), None)
                } else {
                    Node::Binary(None, take_batch(r1))
                }
            }
        }
        Node::Ternary(r0, r1, r2) => {
            let total = r0.len() + r1.len() + r2.len();

            if total <= config.batch_size {
                Node::Ternary(None, None, None)
            } else {
                match (take_batch(r0), take_batch(r1), take_batch(r2)) {
                    (Some(y0), Some(y1), Some(y2)) => {
                        if y0.len() <= y1.len() && y0.len() <= y2.len() {
                            Node::Ternary(None, Some(y1), Some(y2))
                        } else if y1.len() <= y0.len() && y1.len() <= y2.len() {
                            Node::Ternary(Some(y0), None, Some(y2))
                        } else {
                            Node::Ternary(Some(y0), Some(y1), None)
                        }
                    }
                    (b0, b1, b2) => Node::Ternary(b0, b1, b2),
                }
            }
        }
    }
}
