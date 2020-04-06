use crate::sorted_updates::SortedUpdates;
use crate::TreeConfig;

#[derive(Debug, Clone)]
pub struct Batch<K>(SortedUpdates<K>);

#[derive(Debug, Clone, Copy)]
pub struct BatchSize<'a>(&'a TreeConfig, usize);

impl<K> Batch<K> {
    pub fn new(config: &TreeConfig, updates: SortedUpdates<K>) -> Result<Self, SortedUpdates<K>> {
        if config.batch_size / 2 <= updates.len() && updates.len() <= config.batch_size {
            Ok(Self(updates))
        } else {
            Err(updates)
        }
    }

    pub fn consume(self) -> SortedUpdates<K> {
        self.0
    }
}
