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
pub(crate) mod map;
pub(crate) mod storage;
pub(crate) mod wrapper;

use {
    serde_derive::{Deserialize, Serialize},
    std::{fmt, str::FromStr},
};

pub(crate) use {
    self::hash::BlockHash, self::storage::memory::MemoryStore, self::storage::network::NetworkStore,
};

pub use self::storage::{file::FileStore, BlockReader, BlockStorage, BlockWriter};

use self::map::BlockType;
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
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
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

impl fmt::Display for BlockSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BlockSize::FiveTwelve => "512-byte".fmt(f),
            BlockSize::TenTwentyFour => "1k".fmt(f),
            BlockSize::TwentyFortyEight => "2k".fmt(f),
        }
    }
}

#[derive(Debug)]
pub struct ParseBlockSizeError {
    kind: BlockSizeErrorKind,
}

#[derive(Debug)]
pub enum BlockSizeErrorKind {
    /// Parsing error
    ///
    /// Error parsing the string to an integer
    ParseIntError,
    /// Invalid size error
    ///
    /// The string parsed ok, but the block size is not valid.
    ///
    InvalidBlockSize,
}

impl fmt::Display for ParseBlockSizeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            BlockSizeErrorKind::ParseIntError => "Cannot parse string as integer".fmt(f),
            BlockSizeErrorKind::InvalidBlockSize => "Invalid block size".fmt(f),
        }
    }
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

impl From<u64> for BlockSize {
    fn from(n: u64) -> Self {
        match n {
            512 => BlockSize::FiveTwelve,
            1024 => BlockSize::TenTwentyFour,
            2048 => BlockSize::TwentyFortyEight,
            _ => panic!("Invalid Block Size"),
        }
    }
}

impl From<BlockSize> for BlockSizeType {
    fn from(n: BlockSize) -> Self {
        match n {
            BlockSize::FiveTwelve => 512,
            BlockSize::TenTwentyFour => 1024,
            BlockSize::TwentyFortyEight => 2048,
        }
    }
}

impl From<BlockSize> for usize {
    fn from(n: BlockSize) -> Self {
        match n {
            BlockSize::FiveTwelve => 512,
            BlockSize::TenTwentyFour => 1024,
            BlockSize::TwentyFortyEight => 2048,
        }
    }
}

impl FromStr for BlockSize {
    type Err = ParseBlockSizeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(size) = s.parse::<u32>() {
            match size {
                512 => Ok(BlockSize::FiveTwelve),
                1024 => Ok(BlockSize::TenTwentyFour),
                2048 => Ok(BlockSize::TwentyFortyEight),
                _ => Err(ParseBlockSizeError {
                    kind: BlockSizeErrorKind::InvalidBlockSize,
                }),
            }
        } else {
            Err(ParseBlockSizeError {
                kind: BlockSizeErrorKind::ParseIntError,
            })
        }
    }
}

/// Fundamental File System Block Metadata
///
/// This is the record keeping associated with a physical block on some media. It does not contain
/// any data. It contains the number of bytes in the block, the number of the block from the
/// perspective of the media, the SHA-256 hash of the block's data, and the type of block.
///
/// This is stored in the `BlockMap`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Block {
    byte_count: BlockSizeType,
    number: BlockNumber,
    hash: Option<BlockHash>,
    block_type: BlockType,
}

impl Block {
    pub(in crate::block) fn new(number: BlockNumber) -> Self {
        Block {
            byte_count: 0,
            number: number,
            hash: None,
            block_type: BlockType::new_free(),
        }
    }

    /// Mark a block as being free
    pub(in crate::block) fn tag_free(&mut self) {
        self.block_type = BlockType::new_free();
    }

    /// Mark a block as containing data
    pub(in crate::block) fn tag_data(&mut self) {
        self.block_type = BlockType::new_data();
    }

    /// Mark a block as containing the block map
    pub(in crate::block) fn tag_map(&mut self) {
        self.block_type = BlockType::new_map();
    }

    /// Mark a block as containing metadata
    pub(in crate::block) fn tag_metadata(&mut self) {
        self.block_type = BlockType::new_metadata();
    }

    /// Check if a block is free
    #[allow(dead_code)]
    pub(in crate::block) fn is_free(&self) -> bool {
        self.block_type.is_free()
    }

    /// Check if a block contains data
    #[allow(dead_code)]
    pub(in crate::block) fn is_data(&self) -> bool {
        self.block_type.is_data()
    }

    /// Check if a block contains metadata
    #[allow(dead_code)]
    pub(in crate::block) fn is_map(&self) -> bool {
        self.block_type.is_map()
    }

    /// Check if a block contains metadata
    #[allow(dead_code)]
    pub(in crate::block) fn is_metadata(&self) -> bool {
        self.block_type.is_metadata()
    }

    /// Return the block number
    ///
    pub(crate) fn number(&self) -> BlockNumber {
        self.number
    }

    /// Return the block type
    ///
    pub(crate) fn block_type(&self) -> &BlockType {
        &self.block_type
    }

    /// Return the number of bytes stored in this block
    ///
    pub(crate) fn size(&self) -> BlockSizeType {
        self.byte_count
    }

    /// Set the number of bytes in this block
    ///
    pub(in crate::block) fn set_size(&mut self, size: BlockSizeType) {
        self.byte_count = size
    }

    /// Set the SHA-256 hash of this block
    ///
    pub(in crate::block) fn set_hash(&mut self, hash: BlockHash) {
        self.hash = Some(hash);
    }

    /// Return the SHA-256 hash of this block
    ///
    #[allow(dead_code)]
    pub(in crate) fn hash(&self) -> Option<BlockHash> {
        self.hash
    }
}
