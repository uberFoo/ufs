pub mod file;
pub mod memory;
pub mod network;

use crate::block::{map::BlockMap, BlockCardinality, BlockNumber, BlockSize, BlockSizeType};

/// Persistent Storage for Blocks
///
/// This trait is an abstraction for the underlying block storage.  An implementor is taking
/// responsibility for mapping block numbers to _some_ storage location.  Additionally they are
/// able to read and write data to blocks.
pub trait BlockStorage: BlockWriter + BlockReader {
        /// Get an immutable reference to the block map.
        ///
        fn metadata(&self) -> &BlockMap;

        /// Get a mutable reference to the block map.
        ///
        fn metadata_mut(&mut self) -> &mut BlockMap;

        /// The system-wide Block Size, in bytes.
        ///
        fn block_size(&self) -> BlockSize;

        /// The number of Blocks in this file System
        ///
        fn block_count(&self) -> BlockCardinality;
}

pub trait BlockWriter {
        /// Write a Block
        ///
        /// Passing a block number, and a slice of bytes, this method will copy the bytes the to
        /// specified block.  If the slice is smaller than the block size, zeroes will be used to pad
        /// the missing bytes.
        ///
        /// FIXME:
        /// * Implementations should check that the size of the data is not larger than the block size.
        /// * Create some Error type that we can use when something like the above happens.
        fn write_block<T>(
                &mut self,
                bn: BlockNumber,
                data: T,
        ) -> Result<BlockSizeType, failure::Error>
        where
                T: AsRef<[u8]>;
}

impl<'a, T> BlockWriter for &'a mut T
where
        T: BlockWriter,
{
        fn write_block<D>(
                &mut self,
                bn: BlockNumber,
                data: D,
        ) -> Result<BlockSizeType, failure::Error>
        where
                D: AsRef<[u8]>,
        {
                self.write_block(bn, data.as_ref())
        }
}

pub trait BlockReader {
        /// Read a Block
        ///
        /// Return a fresh copy of the bytes contained in the specified block, as a `Vec<u8>`.
        fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, failure::Error>;
}

impl<'a, T> BlockReader for &'a mut T
where
        T: BlockReader,
{
        fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, failure::Error> {
                self.read_block(bn)
        }
}
