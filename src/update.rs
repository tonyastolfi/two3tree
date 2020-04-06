#[derive(Debug, Copy, Clone)]
pub enum Update<K> {
    Put(K),
    Delete(K),
}

impl<K> Update<K> {
    pub fn key<'a>(&'a self) -> &'a K {
        use Update::{Delete, Put};
        match self {
            Put(key) => key,
            Delete(key) => key,
        }
    }
    pub fn resolve<'a>(&'a self) -> Option<&'a K> {
        use Update::{Delete, Put};
        match self {
            Put(key) => Some(key),
            Delete(key) => None,
        }
    }
}
