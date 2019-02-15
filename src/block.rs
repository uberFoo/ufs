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
//!
//! FIXME: BlockLists should serialize when dropped.

mod hash;
pub(crate) mod manager;
mod meta;
pub(crate) mod storage;
pub(crate) mod tree;

use serde_derive::{Deserialize, Serialize};

pub(crate) use self::storage::file::FileStore;

use self::hash::BlockHash;

pub type BlockCardinality = u64;
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Block {
    byte_count: BlockSizeType,
    number: Option<BlockCardinality>,
    hash: Option<BlockHash>,
}

impl Block {
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

    pub(crate) fn size(&self) -> usize {
        self.byte_count as usize
    }
}
