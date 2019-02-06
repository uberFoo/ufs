//! Logical File Blocks
//!
//! This file system is comprized of blocks; file contents and metadata are stored in blocks.  The
//! blocks will generally exist on a disk, but net necessarily.  Even in the case when the blocks
//! are on disk, they may be stored as files on another file system.
//!
//! There isn't really too much to a block, but we spice it up a bit. To start, for any given file
//! system, the blocks are all the same size, which is generally accepted to be a `u32`.  More
//! specifically, when dealing with physical disks 512, 1024, and 2048 are popular choices.  Larger
//! block sizes exist, but that takes some special mojo spinning disk.
//!
//! Similarly, there are generally a fixed number of blocks in a file system, and that number is
//! determined like the block size: when the file system is created.
mod hash;
mod meta;

pub(crate) mod manager;
pub(crate) mod storage;
pub(crate) mod tree;

use core::ops::Deref;

use serde_derive::{Deserialize, Serialize};

pub(crate) use self::{
    manager::BlockManager,
    storage::{file::FileStore, memory::MemoryStore},
};

use self::{hash::BlockHash, tree::BlockTree};

pub type BlockCardinality = u64;

/// Available Block Sizes
///
/// Why not let someone choose a weird block size?  This isn't the Wild West!  Constraints exist so
/// that we can work with physical media.
///
/// FIXME: I'm not sure allowing an option is the best idea.  I think that there may be an optimal
/// block size, given this file system's unique characteristics.  We can always map a block across
/// multiple physical sectors.
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum BlockSize {
    /// 512 byte block size
    ///
    FiveTwelve = 512,
    /// 1024 byte block size
    ///
    TenTwentyFour = 1024,
    /// 2048 byte block size
    ///
    TwentyFortyEight = 2048,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Block {
    number: BlockCardinality,
    hash: BlockHash,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct BlockList {
    blocks: Vec<Block>,
    hash_tree: BlockTree,
}

impl BlockList {
    pub(crate) fn new(blocks: Vec<Block>) -> Self {
        BlockList {
            hash_tree: BlockTree::new(&blocks),
            blocks,
        }
    }
}

// This impl allows us to treat a BlockList like a Vec, as far as method calls are concerned.
impl Deref for BlockList {
    type Target = Vec<Block>;

    fn deref(&self) -> &Vec<Block> {
        &self.blocks
    }
}
