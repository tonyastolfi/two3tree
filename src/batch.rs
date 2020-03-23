use crate::sorted_updates::SortedUpdates;
use crate::TreeConfig;

#[derive(Debug, Clone)]
pub struct Batch(SortedUpdates);

#[derive(Debug, Clone, Copy)]
pub struct BatchSize<'a>(&'a TreeConfig, usize);

impl Batch {
    pub fn new(config: &TreeConfig, updates: SortedUpdates) -> Result<Self, SortedUpdates> {
        if config.batch_size / 2 <= updates.len() && updates.len() <= config.batch_size {
            Ok(Self(updates))
        } else {
            Err(updates)
        }
    }

    pub fn consume(self) -> SortedUpdates {
        self.0
    }
}
