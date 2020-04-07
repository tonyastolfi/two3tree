use itertools::Itertools;

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

pub fn merge_updates<'a, First, Second, K>(
    first: First,
    second: Second,
) -> impl Iterator<Item = &'a Update<K>> + 'a
where
    First: Iterator<Item = &'a Update<K>> + 'a,
    Second: Iterator<Item = &'a Update<K>> + 'a,
    K: Ord + 'a,
{
    use itertools::EitherOrBoth::{Both, Left, Right};

    first
        .merge_join_by(second, |a, b| a.key().cmp(b.key()))
        .map(|either| match either {
            Left(from_first) => from_first,
            Right(from_second) => from_second,
            Both(_, from_second) => from_second,
        })
}

pub fn apply_updates<'a, First, Second, K>(
    first: First,
    second: Second,
) -> impl Iterator<Item = &'a K> + 'a
where
    First: Iterator<Item = &'a K> + 'a,
    Second: Iterator<Item = &'a Update<K>> + 'a,
    K: Ord + 'a,
{
    use itertools::EitherOrBoth::{Both, Left, Right};

    first
        .merge_join_by(second, |a, b| a.cmp(&b.key()))
        .filter_map(|either| match either {
            Left(from_first) => Some(from_first),
            Right(from_second) => from_second.resolve(),
            Both(_, from_second) => from_second.resolve(),
        })
}
