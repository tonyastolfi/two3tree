use crate::node::{Node, Subtree, TreeConfig};

use itertools::Itertools;

pub trait BatchUpdate {
    fn update(&mut self, config: &TreeConfig, batch: Vec<Update>) -> UpdateResult;
}

use Subtree::{Branch, Leaf};

macro_rules! split {
    [$min:expr, $($x:expr),*] => {
        UpdateResult::Split($min, make_branch![$($x),*])
    };
}

pub fn update_leaf(config: &TreeConfig, batch: Vec<Update>, vals: Vec<i32>) -> UpdateResult {
    use itertools::EitherOrBoth::{Both, Left, Right};
    use Orphan::Items;
    use UpdateResult::{Done, Merge, Split};

    let mut merged: Vec<i32> = vals
        .iter()
        .merge_join_by(batch.iter(), |old, update| old.cmp(&update.key()))
        .filter_map(|either| match either {
            Left(old) => Some(*old),
            Right(update) => update.resolve(),
            Both(_old, update) => update.resolve(),
        })
        .collect();

    assert!(merged.len() <= config.batch_size * 3);

    if merged.len() < config.batch_size {
        return Merge(Items(merged));
    }

    if merged.len() <= config.batch_size * 2 {
        return Done(Leaf { vals: merged });
    }

    let split_vals: Vec<i32> = merged.drain((merged.len() / 2)..).collect();
    let split_min: i32 = split_vals[0];
    return Split(Leaf { vals: merged }, split_min, Leaf { vals: split_vals });
}

pub fn update_binary_node(
    config: &TreeConfig,
    batch: Vec<Update>,
    left: Subtree,
    right_min: i32,
    right: Subtree,
) -> (Node, UpdateResult) {
}
