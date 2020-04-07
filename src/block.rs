use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::node::Node;

const B: usize = 32;

#[derive(Debug, Clone, Copy)]
pub struct ObjectId(u64);

#[derive(Debug, Clone)]
pub struct CachedRef<T>(Arc<T>)
where
    K: std::fmt::Debug;

#[derive(Debug, Clone, Copy)]
pub struct StoredBlock(ObjectId);

#[derive(Debug, Clone)]
pub enum BlockRef<T>
where
    T: std::fmt::Debug,
{
    Cached(CachedRef<T>),
    Stored(StoredBlock),
}

pub trait BlockStorage {
    fn load<T>(&self, stored: &StoredBlock) -> std::io::Result<CachedRef<T>>
    where
        K: std::fmt::Debug;

    fn resolve<T>(&self, target: &BlockRef<T>) -> std::io::Result<CachedRef<T>>
    where
        K: std::fmt::Debug,
    {
        use BlockRef::*;

        match target {
            Cached(cached) => Ok(cached),
            Stored(stored) => self.load(stored),
        }
    }
}

pub enum SubtreeBlock<K> {
    Branch(NodeBlock<K>),
    Leaf(LeafBlock<K>),
}

pub struct NodeBlock<K>
where
    K: std::fmt::Debug,
{
    dirty: bool,
    id: ObjectId,

    // |<--B/4-->|<--B/4-->|<--B/4-->|<--B/4-->|
    // |   top   |____________bottom___________|
    //          /_______________________________\
    //         /_________________________________\
    //        |<-------------B*3/4*3------------->|
    //        |                refs               |
    //
    pool: [Option<Node<K>>; B],
    refs: [Option<BlockRef<SubtreeBlock<K>>>; B * 9 / 4],
}

pub struct LeafBlock<K> {
    dirty: bool,
    id: ObjectId,

    items: [K; B * 8],
}

impl<K> std::fmt::Debug for NodeBlock<K>
where
    K: std::fmt::Debug,
{
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        Ok(())
    }
}

pub struct NodeBlockPtr<K, D>
where
    K: Ord + Clone + std::fmt::Debug,
    D: BlockStorage,
{
    device: Arc<D>,
    arena: NodeBlockRef<K>,
    index: usize,
}

impl<K, D> NodeBlockPtr<K, D>
where
    K: Ord + Clone + std::fmt::Debug,
    D: BlockStorage,
{
    pub fn chase(&self) -> std::io::Result<Node<(K, NodeBlockPtr<K, D>)>> {
        // The arena must be cached in-memory for us to chase this pointer down.
        //
        let cached = self.device.resolve(self.arena.clone())?;

        let is_top = self.index < (B / 4);
        let get_child = |num: usize| -> Self {
            let offset = self.index * 3 + num;

            if is_top {
                // For 'top' nodes, child references point within the arena.
                //
                Self {
                    device: self.device.clone(),
                    arena: self.arena.clone(),
                    index: offset,
                }
            } else {
                // For 'bottom' nodes, child references point outside the arena.
                //
                Self {
                    device: self.device.clone(),
                    arena: cached.0.refs[offset - B].as_ref().unwrap().clone(),
                    index: 0,
                }
            }
        };

        Ok(match cached.0.pool[self.index].as_ref().unwrap().clone() {
            Node::Binary(k0, k1) => Node::Binary((k0, get_child(1)), (k1, get_child(2))),
            Node::Ternary(k0, k1, k2) => {
                Node::Ternary((k0, get_child(1)), (k1, get_child(2)), (k2, get_child(3)))
            }
        })
    }
}

pub struct DirtyNodePtr<K>(Rc<Node<(K, SubtreePtr<K>)>>);

pub enum SubtreePtr<K> {
    Placed(NodeBlockPtr),
}
