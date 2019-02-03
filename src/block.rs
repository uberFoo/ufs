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
// mod bundle;
mod file;
mod memory;
mod tree;

pub use self::file::FileStore;
pub use self::memory::MemoryStore;

use self::tree::BlockTree;

use core::ops::Deref;
use std::fmt;

use failure::{format_err, Error};
use ring::digest;

pub type BlockNumber = u64;

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct BlockHash {
    inner: [u8; 32],
}

impl BlockHash {
    pub(crate) fn new(data: &[u8]) -> Self {
        BlockHash::from(digest::digest(&digest::SHA256, &data[..]).as_ref())
    }
}

impl AsRef<[u8]> for BlockHash {
    fn as_ref(&self) -> &[u8] {
        &self.inner
    }
}

impl From<&[u8]> for BlockHash {
    fn from(data: &[u8]) -> Self {
        let mut hash: [u8; 32] = [0; 32];
        hash.copy_from_slice(data);
        BlockHash { inner: hash }
    }
}

impl fmt::Debug for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in &self.inner {
            write!(f, "{:02x}", i)?;
        }
        // write!(f, "{:?}", self.0);
        Ok(())
    }
}

/// Available Block Sizes
///
/// Why not let someone choose a weird block size?  This isn't the Wild West!  Constraints exist so
/// that we can work with physical media.
#[derive(Debug, Copy, Clone)]
pub enum BlockSize {
    FiveTwelve = 512,
    TenTwentyFour = 1024,
    TwentyFortyEight = 2048,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Block {
    number: BlockNumber,
    hash: BlockHash,
}

#[derive(Debug, PartialEq)]
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

/// Persistent Storage for Blocks
///
/// This trait is an abstraction for the underlying block storage.  An implementor is taking
/// responsibility for mapping block numbers to _some_ storage location.  Additionally they are
/// able to read and write data to blocks.
pub(crate) trait BlockStorage {
    /// The system-wide Block Size, in bytes.
    ///
    fn block_size(&self) -> BlockSize;

    /// The number of Blocks in this file System
    ///
    fn block_count(&self) -> BlockNumber;

    /// Storage Initialization
    ///
    /// This method is meant to be invoked once, when a new block storage device is created.  The
    /// method needs only the block size, and the number of them.
    // fn reserve(&mut self, count: BlockNumber, size: BlockSize) -> Result<(), Error>;
    // fn reserve_blocks(&mut self) -> Result<(), Error>;

    /// Write a Block
    ///
    /// Passing a block number, and a slice of bytes, this method will copy the bytes the to
    /// specified block.  If the slice is smaller than the block size, zeroes will be used to pad
    /// the missing bytes.
    fn write_block(&mut self, bn: BlockNumber, data: &[u8]) -> Result<Block, Error>;

    /// Read a Block
    ///
    /// Return a fresh copy of the bytes contained in the specified block, as a `Vec<u8>`.
    fn read_block(&self, block: &Block) -> Result<Vec<u8>, Error>;
}

/// Manager of Blocks
///
/// This sits atop a BlockStorage and provides higher-level operations over blocks.  For example,
/// reads and writes of arbitrary size (files) are aggregated across multiple blocks.  Per-block
/// hashes are calculated when writing, and validated when reading, a block.  Data written across
/// multiple blocks are stored as a [BlockList], etc.
pub(crate) trait BlockManager: BlockStorage {
    /// The number of available, un-allocated Blocks.
    ///
    fn free_block_count(&self) -> BlockNumber;

    /// Request a Block
    ///
    /// The implementor maintains a pool of available blocks, and if there is one available, this
    /// method will return it.
    fn get_free_block(&mut self) -> Option<BlockNumber>;

    /// Recycle a Block
    ///
    /// The block is no longer being used, and may be returned to the free block pool.
    fn recycle_block(&mut self, block: BlockNumber);

    /// Write Some Bytes
    ///
    /// The bytes are written to the minimum number of blocks required to store the furnished slice.
    /// The list of blocks that now contain the bytes is returned.  Hashes will be created,
    /// Merkle tree, blah, blah.
    ///
    /// FIXME: I wonder is using slice::chunks() would be better?
    fn write(&mut self, data: &[u8]) -> Result<BlockList, Error> {
        let block_size = self.block_size() as usize;
        let block_count =
            data.len() / block_size + if data.len() % block_size == 0 { 0 } else { 1 };
        if block_count <= self.free_block_count() as usize {
            let mut blocks = Vec::with_capacity(block_count);

            for i in 0..block_count {
                // Size checked above.  If we were to rely on the option result here, what would be
                // the best way of returning already allocated blocks to the free list?  Would they
                // have already been written to the BlockStorage?
                let free_block_num = self.get_free_block().unwrap();
                let block_data_slice = &data[i * block_size..data.len().min((i + 1) * block_size)];

                // Sort of the same question here as above.  Given that this may fail as well, I'm
                // leaning in the direction of not checking at the beginning.
                let block = self.write_block(free_block_num, block_data_slice)?;

                blocks.push(block);
            }

            Ok(BlockList::new(blocks))
        } else {
            Err(format_err!(
                "write would require {} blocks, and only {} are free",
                block_count,
                self.free_block_count()
            ))
        }
    }

    /// Read Some Bytes
    ///
    /// Given a [BlockList], the bytes previously written to the list will be returned. Hashes
    /// will be checked, blah, blah, blah.
    ///
    /// FIXME
    fn read(&self, blocks: &BlockList) -> Result<Vec<u8>, Error> {
        let mut data = Vec::<u8>::with_capacity(blocks.len() * self.block_size() as usize);
        for b in blocks.iter() {
            let mut data_block = self.read_block(&b)?;
            data.append(&mut data_block);
        }

        Ok(data)
    }
}

#[cfg(test)]
mod test {
    use hex_literal::{hex, hex_impl};

    use super::*;

}
