use std::collections::{HashMap, VecDeque};

use bincode;
use failure::{format_err, Error};
use log::trace;

use crate::{
    block::{
        hash::BlockHash, storage::BlockStorage, Block, BlockCardinality, BlockSize, BlockSizeType,
    },
    metadata::{DirectoryMetadata, Metadata},
};

/// Manager of Blocks
///
/// This sits atop a BlockStorage and provides higher-level operations over blocks.  For example,
/// reads and writes of arbitrary size (files) are aggregated across multiple blocks.  Per-block
/// hashes are calculated when writing, and validated when reading, a block.  Data written across
/// multiple blocks are stored as a [BlockList], etc.
#[derive(Debug, PartialEq)]
pub struct BlockManager<BS>
where
    BS: BlockStorage,
{
    store: BS,
    free_blocks: VecDeque<BlockCardinality>,
    directory: DirectoryMetadata,
}

impl<'a, BS> BlockManager<BS>
where
    BS: BlockStorage,
{
    pub fn new(store: BS) -> Self {
        BlockManager {
            free_blocks: (1..store.block_count()).collect(),
            directory: DirectoryMetadata::new_root(),
            store,
        }
    }

    /// FIXME: This may be nice in a From<BlockMetadata>
    pub(crate) fn load(store: BS) -> Self {
        let block_0 = store.read_block(0).unwrap();
        let metadata = Metadata::deserialize(block_0).unwrap();
        let free_blocks = match metadata.next_free_block {
            Some(n) => (n..store.block_count()).collect(),
            None => VecDeque::new(),
        };
        BlockManager {
            free_blocks,
            directory: metadata.root_dir,
            store,
        }
    }

    pub(crate) fn block_count(&self) -> BlockCardinality {
        self.store.block_count()
    }

    pub(crate) fn block_size(&self) -> BlockSize {
        self.store.block_size()
    }

    /// The number of available, un-allocated Blocks.
    ///
    pub(crate) fn free_block_count(&self) -> BlockCardinality {
        self.free_blocks.len() as BlockCardinality
    }

    /// Request a Block
    ///
    /// The implementor maintains a pool of available blocks, and if there is one available, this
    /// method will return it.
    pub(crate) fn get_free_block(&mut self) -> Option<BlockCardinality> {
        self.free_blocks.pop_front()
    }

    /// Recycle a Block
    ///
    /// The block is no longer being used, and may be returned to the free block pool.
    pub(crate) fn recycle_block(&mut self, block: BlockCardinality) {
        self.free_blocks.push_back(block);
    }

    /// Save the state of the BlockManager
    ///
    /// This method stores the metadata in the [BlockStorage], starting at block 0.
    ///
    /// FIXME: If this fails, then what?
    pub(crate) fn serialize(&mut self) {
        let metadata = Metadata {
            next_free_block: self.free_blocks.get(0).cloned(),
            root_dir: self.directory.clone(),
        };

        self.store.write_block(0, metadata.serialize()).unwrap();
    }

    /// Write a slice to a Block Storage
    ///
    /// This function will write up to `self.store.block_size()` bytes from the given slice to a
    /// free block.  A new [Block] is returned.
    pub(crate) fn write<T: AsRef<[u8]>>(&mut self, data: T) -> Result<Block, Error> {
        let data = data.as_ref();
        if let Some(number) = self.get_free_block() {
            let end = data.len().min(self.store.block_size() as usize);
            let bytes = &data[..end];
            let byte_count = self.store.write_block(number, bytes)?;
            trace!("write block 0x{:x?}", number);
            Ok(Block {
                byte_count,
                number: Some(number),
                hash: Some(BlockHash::new(bytes)),
            })
        } else {
            Err(format_err!(
                "I was unable to complete the write operation.  I could not find a free block!"
            ))
        }
    }

    /// Read data from a Block into a u8 vector
    ///
    /// FIXME: Thinking about memory and the like last night, it occurred to me why `std::io::Read`
    /// takes a reference to a slice of bytes, rather than what I'm doing here.  The reason (as I
    /// see it anyway) is to avoid copying memory.  At this point, we can't know how the memory
    /// is going to be used.  By returning a `Vec<u8>` the caller is forced to use the vector --
    /// even if they have their own buffer allocated to take the bytes.
    pub(crate) fn read(&self, block: &Block) -> Result<Vec<u8>, Error> {
        if let Block {
            number: Some(block_number),
            hash: Some(block_hash),
            byte_count: _,
        } = block
        {
            let bytes = self.store.read_block(*block_number)?;
            let hash = BlockHash::new(&bytes);
            if hash == *block_hash {
                trace!("read block 0x{:x?}", *block_number);
                Ok(bytes)
            } else {
                Err(format_err!(
                    "hash mismatch: expected {:?}, but calculated {:?}",
                    block.hash,
                    hash
                ))
            }
        } else {
            Err(format_err!("cannot read null Block"))
        }
    }
}

impl<'a, BS> Drop for BlockManager<BS>
where
    BS: BlockStorage,
{
    fn drop(&mut self) {
        self.serialize();
    }
}

#[cfg(test)]
// FIXME: It seems like I should be able to make these tests generic over all of the available
//        BlockStorage implementations?
mod test {
    use hex_literal::{hex, hex_impl};

    use super::*;
    use crate::block::{
        storage::{file::FileStore, memory::MemoryStore},
        BlockSize,
    };

    #[test]
    fn not_enough_free_blocks_error() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 1));

        let blocks = bm.write(&vec![0x0; 513][..]);
        assert_eq!(
            blocks.is_err(),
            true,
            "verify that more blocks are needed for write"
        );
    }

    #[test]
    fn tiny_test() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 2));

        let block = bm.write(b"abc").unwrap();
        println!("{:#?}", block);

        assert_eq!(bm.free_block_count(), 0);
        let hash = block.hash.unwrap();
        assert_eq!(
            hash.as_ref(),
            hex!("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
            "validate hash"
        );

        assert_eq!(
            bm.read(&block).unwrap(),
            b"abc",
            "compare stored data with expected values"
        );
    }

    #[test]
    fn write_data_smaller_than_blocksize() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 2));

        let block = bm.write(&vec![0x38; 511][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(
            bm.read(&block).unwrap(),
            &vec![0x38; 511][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", block);
    }

    #[test]
    fn write_data_larger_than_blocksize() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 3));

        let block = bm.write(&vec![0x38; 513][..]).unwrap();
        assert_eq!(bm.free_block_count(), 1);
        assert_eq!(
            bm.read(&block).unwrap(),
            &vec![0x38; 512][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", block);
    }

    #[test]
    fn check_read_and_write_hashing() {
        unimplemented!();
    }

    #[test]
    fn read_block_bad_hash() {
        unimplemented!();
    }
}
