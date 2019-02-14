pub mod file;
pub mod memory;

use failure::Error;

use crate::block::{Block, BlockCardinality, BlockSize, BlockSizeType};

/// Persistent Storage for Blocks
///
/// This trait is an abstraction for the underlying block storage.  An implementor is taking
/// responsibility for mapping block numbers to _some_ storage location.  Additionally they are
/// able to read and write data to blocks.
pub trait BlockStorage {
    /// The system-wide Block Size, in bytes.
    ///
    fn block_size(&self) -> BlockSize;

    /// The number of Blocks in this file System
    ///
    fn block_count(&self) -> BlockCardinality;

    /// Write a Block
    ///
    /// Passing a block number, and a slice of bytes, this method will copy the bytes the to
    /// specified block.  If the slice is smaller than the block size, zeroes will be used to pad
    /// the missing bytes.
    ///
    /// FIXME:
    /// * Implementations should check that the size of the data is not larger than the block size.
    /// * Create some Error type that we can use when something like the above happens.
    fn write_block<T>(&mut self, bn: BlockCardinality, data: T) -> Result<BlockSizeType, Error>
    where
        T: AsRef<[u8]>;

    /// Read a Block
    ///
    /// Return a fresh copy of the bytes contained in the specified block, as a `Vec<u8>`.
    fn read_block(&self, bn: BlockCardinality) -> Result<Vec<u8>, Error>;
}
