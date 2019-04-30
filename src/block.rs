//! Logical File Blocks
//!
//! This file system is comprised of blocks; file contents and metadata are stored in blocks.  The
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
//!
//! FIXME: BlockLists should serialize when dropped.

mod hash;
pub(crate) mod manager;
mod meta;
pub(crate) mod storage;
pub(crate) mod tree;

use std::path::Path;

use failure::Error;
use serde_derive::{Deserialize, Serialize};

pub(crate) use self::{manager::BlockManager, storage::file::FileStore};

use self::hash::BlockHash;
use crate::UfsUuid;

/// A logical block number.
pub type BlockNumber = u64;
/// Where a block lives?
///
/// The address is a two-tuple consisting of the file system ID, and a logical block number.
pub struct BlockAddress(UfsUuid, BlockNumber);
/// The number of blocks in a file system.
pub type BlockCardinality = u64;
/// The size of a block, in bytes.
pub type BlockSizeType = u16;

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

impl From<u32> for BlockSize {
    fn from(n: u32) -> Self {
        match n {
            512 => BlockSize::FiveTwelve,
            1024 => BlockSize::TenTwentyFour,
            2048 => BlockSize::TwentyFortyEight,
            _ => panic!("Invalid Block Size"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum BlockType {
    Pointer(Block),
}

impl BlockType {
    pub(crate) fn serialize(&self) -> bincode::Result<Vec<u8>> {
        bincode::serialize(&self)
    }

    pub(crate) fn deserialize<T>(bytes: T) -> bincode::Result<Self>
    where
        T: AsRef<[u8]>,
    {
        bincode::deserialize(bytes.as_ref())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Block {
    byte_count: BlockSizeType,
    number: Option<BlockCardinality>,
    hash: Option<BlockHash>,
}

impl Block {
    pub(crate) fn nasty_hack(
        number: BlockCardinality,
        size: BlockSizeType,
        hash: BlockHash,
    ) -> Self {
        Block {
            byte_count: size,
            number: Some(number),
            hash: Some(hash),
        }
    }

    pub(crate) fn new<B>(number: BlockCardinality, bytes: Option<B>) -> Self
    where
        B: AsRef<[u8]>,
    {
        match bytes {
            Some(bytes) => {
                let bytes = bytes.as_ref();
                Block {
                    byte_count: bytes.len() as BlockSizeType,
                    number: Some(number),
                    hash: Some(BlockHash::new(bytes)),
                }
            }
            None => Block {
                byte_count: 0,
                number: Some(number),
                hash: None,
            },
        }
    }

    pub(crate) fn null_block() -> Self {
        Block {
            byte_count: 0,
            number: None,
            hash: None,
        }
    }

    pub(crate) fn number(&self) -> Option<BlockCardinality> {
        self.number
    }

    pub(crate) fn size(&self) -> usize {
        self.byte_count as usize
    }

    pub(crate) fn hash(&self) -> Option<BlockHash> {
        self.hash
    }
}
