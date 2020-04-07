use std::ops::Range;

use crate::batch::Batch;
use crate::node::Node;
use crate::TreeConfig;
use crate::K;

pub struct FlushPlan {
    pub flush: Option<Range<usize>>,
    pub keep: Option<Range<usize>>,
}

impl FlushPlan {
    pub fn none(r: &Range<usize>) -> Self {
        Self {
            flush: None,
            keep: Some(r.clone()),
        }
    }

    pub fn all(r: &Range<usize>) -> Self {
        Self {
            flush: Some(r.clone()),
            keep: None,
        }
    }

    pub fn clip(config: &TreeConfig, r: &Range<usize>) -> Self {
        if r.len() < config.batch_size / 2 {
            Self::none(r)
        } else if r.len() <= config.batch_size {
            Self::all(r)
        } else {
            Self {
                flush: Some(Range {
                    start: r.start,
                    end: r.start + config.batch_size,
                }),
                keep: Some(Range {
                    start: r.start + config.batch_size,
                    end: r.end,
                }),
            }
        }
    }
}
