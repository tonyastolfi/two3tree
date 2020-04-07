use crate::sorted_updates::Sorted;
use crate::update::Update;
use crate::TreeConfig;

#[derive(Debug, Clone)]
pub struct Batch<'a, U>(&'a U);

#[derive(Debug, Clone, Copy)]
pub struct BatchSize<'a>(&'a TreeConfig, usize);

impl<'a, U> Batch<'a, U>
where
    U: Sorted,
{
    pub fn new(config: &TreeConfig, updates: &'a U) -> Self {
        assert!(config.batch_size / 2 <= updates.len() && updates.len() <= config.batch_size);
        Self(updates)
    }

    pub fn consume(self) -> &'a U {
        self.0
    }
}
