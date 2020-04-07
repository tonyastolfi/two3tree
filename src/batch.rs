use crate::sorted_updates::Sorted;
use crate::update::Update;
use crate::TreeConfig;

#[derive(Debug, Clone)]
pub struct Batch<U>(U)
where
    U: Sorted;

#[derive(Debug, Clone, Copy)]
pub struct BatchSize<'a>(&'a TreeConfig, usize);

impl<U> Batch<U>
where
    U: Sorted,
{
    pub fn new(config: &TreeConfig, updates: U) -> Self {
        assert!(config.batch_size / 2 <= updates.len() && updates.len() <= config.batch_size);
        Self(updates)
    }

    pub fn consume(self) -> U {
        self.0
    }
}
