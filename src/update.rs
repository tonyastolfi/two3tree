use crate::K;

#[derive(Debug, Copy, Clone)]
pub enum Update {
    Put(i32),
    Delete(i32),
}

impl Update {
    pub fn key<'a>(&'a self) -> &'a i32 {
        use Update::{Delete, Put};
        match self {
            Put(key) => key,
            Delete(key) => key,
        }
    }
    pub fn resolve<'a>(&'a self) -> Option<&'a i32> {
        use Update::{Delete, Put};
        match self {
            Put(key) => Some(key),
            Delete(key) => None,
        }
    }
}
