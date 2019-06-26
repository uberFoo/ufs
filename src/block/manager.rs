//! Block Manager
//!
//! High level access to block storage.  The block manager checks block hash consistency, handles
//! encryption, etc.  It also contains the `BlockMap` and handles directory and file metadata.

use failure::format_err;
use log::{debug, error};

use crate::{
    block::{
        wrapper::{read_metadata, write_metadata},
        Block, BlockCardinality, BlockHash, BlockNumber, BlockSize, BlockStorage,
    },
    metadata::DirectoryMetadata,
};

/// Manager of Blocks
///
/// This sits atop a BlockStorage and provides higher-level operations over blocks.  For example,
/// reads and writes of arbitrary size (files) are aggregated across multiple blocks.  Per-block
/// hashes are calculated when writing, and validated when reading, a block.  Data written across
/// multiple blocks are stored using the [`BlockMap`], etc.
///
/// [`BlockMap`]: crate::block::map::BlockMap
#[derive(Debug, PartialEq)]
pub struct BlockManager<BS>
where
    BS: BlockStorage,
{
    store: BS,
    root_dir: DirectoryMetadata,
}

impl<'a, BS> BlockManager<BS>
where
    BS: BlockStorage,
{
    /// Layer metadata atop a block storage
    pub fn new(store: BS) -> Self {
        BlockManager {
            root_dir: DirectoryMetadata::new(),
            store,
        }
    }

    /// FIXME: This may be nice in a From<BlockMetadata>
    pub(crate) fn load(mut store: BS) -> Result<Self, failure::Error> {
        match store.map().root_block() {
            Some(root_block) => {
                debug!("Reading root directory from block {}", root_block);
                match read_metadata(&mut store, root_block) {
                    Ok(root_dir) => {
                        debug!("loaded metadata");

                        Ok(BlockManager {
                            root_dir: root_dir,
                            store,
                        })
                    }
                    Err(e) => Err(format_err!("Problem loading file system metadata: {}", e)),
                }
            }
            None => Err(format_err!("Missing root_block!")),
        }
    }

    pub(crate) fn root_dir(&self) -> &DirectoryMetadata {
        &self.root_dir
    }

    pub(crate) fn root_dir_mut(&mut self) -> &mut DirectoryMetadata {
        &mut self.root_dir
    }

    pub(crate) fn block_count(&self) -> BlockCardinality {
        self.store.block_count()
    }

    pub(crate) fn block_size(&self) -> BlockSize {
        self.store.block_size()
    }

    pub(crate) fn get_block(&self, number: BlockNumber) -> Option<&Block> {
        self.store.map().get(number)
    }

    /// The number of available, un-allocated Blocks.
    ///
    pub(crate) fn free_block_count(&self) -> BlockCardinality {
        self.store.map().free_blocks().len() as BlockCardinality
    }

    /// Request a Block
    ///
    /// The implementor maintains a pool of available blocks, and if there is one available, this
    /// method will return it.
    pub(in crate::block) fn get_free_block(&mut self) -> Option<BlockCardinality> {
        self.store.map_mut().free_blocks_mut().pop_front()
    }

    // /// Recycle a Block
    // ///
    // /// The block is no longer being used, and may be returned to the free block pool.
    // pub(crate) fn recycle_block(&mut self, block: BlockCardinality) {
    //     self.free_blocks.push_back(block);
    // }

    /// Save the state of the BlockManager
    ///
    /// This method stores the metadata in the [BlockStorage], starting at block 0.
    ///
    /// FIXME: If this fails, then what?
    pub(crate) fn serialize(&mut self) {
        if self.root_dir.is_dirty() {
            match write_metadata(&mut self.store, &mut self.root_dir) {
                Ok(block) => {
                    debug!("Stored new root block {}", block);
                    self.store.map_mut().set_root_block(block);
                    self.store.commit_map();
                }
                Err(e) => {
                    error!("error writing metadata: {}", e);
                    error!("Did not store new root block");
                }
            };
        }
    }

    /// Write a slice to a Block Storage
    ///
    /// This function will write up to `self.store.block_size()` bytes from the given slice to a
    /// free block.  A new [Block] is returned.
    pub(crate) fn write<T: AsRef<[u8]>>(&mut self, data: T) -> Result<&Block, failure::Error> {
        let data = data.as_ref();
        if let Some(number) = self.get_free_block() {
            let end = data.len().min(self.store.block_size() as usize);
            let bytes = &data[..end];
            let byte_count = self.store.write_block(number, bytes)?;
            debug!("wrote block 0x{:x?}", number);
            let block = self.store.map_mut().get_mut(number).unwrap();
            block.set_size(byte_count);
            block.set_hash(BlockHash::new(bytes));
            block.tag_data();

            Ok(block)
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
    pub(crate) fn read(&self, block: &Block) -> Result<Vec<u8>, failure::Error> {
        if let Block {
            number: block_number,
            hash: Some(block_hash),
            byte_count: _,
            block_type: _,
        } = block
        {
            let bytes = self.store.read_block(*block_number)?;
            let hash = BlockHash::new(&bytes);
            if hash == *block_hash {
                debug!("read block 0x{:x?}", *block_number);
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
        debug!("Dropping BlockManager");
        self.serialize();
    }
}

#[cfg(test)]
// FIXME: It seems like I should be able to make these tests generic over all of the available
//        BlockStorage implementations?
mod test {
    use hex_literal::hex;

    use super::*;
    use crate::{
        block::{map::BlockMap, BlockSize, MemoryStore},
        UfsUuid,
    };

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn check_metadata() {
        init();
        let mut bm = BlockManager::new(MemoryStore::new(BlockMap::new(
            UfsUuid::new("test"),
            BlockSize::FiveTwelve,
            10,
        )));

        print!("root dir {:#?}", bm.root_dir);
    }

    #[test]
    fn not_enough_free_blocks_error() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockMap::new(
            UfsUuid::new("test"),
            BlockSize::FiveTwelve,
            1,
        )));

        let blocks = bm.write(&vec![0x0; 513][..]);
        assert_eq!(
            blocks.is_err(),
            true,
            "verify that more blocks are needed for write"
        );
    }

    #[test]
    fn tiny_test() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockMap::new(
            UfsUuid::new("test"),
            BlockSize::FiveTwelve,
            2,
        )));

        let block = bm.write(b"abc").unwrap().clone();
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
        let mut bm = BlockManager::new(MemoryStore::new(BlockMap::new(
            UfsUuid::new("test"),
            BlockSize::FiveTwelve,
            2,
        )));

        let block = bm.write(&vec![0x38; 511][..]).unwrap().clone();
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
        let mut bm = BlockManager::new(MemoryStore::new(BlockMap::new(
            UfsUuid::new("test"),
            BlockSize::FiveTwelve,
            3,
        )));

        let block = bm.write(&vec![0x38; 513][..]).unwrap().clone();
        assert_eq!(bm.free_block_count(), 1);
        assert_eq!(
            bm.read(&block).unwrap(),
            &vec![0x38; 512][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", block);
    }

    #[test]
    fn read_block_bad_hash() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockMap::new(
            UfsUuid::new("test"),
            BlockSize::FiveTwelve,
            2,
        )));

        let mut block = bm.write(b"abc").unwrap().clone();

        // Replace the hash of the block with something else.
        block.hash.replace(BlockHash::new("abcd"));

        assert!(bm.read(&block).is_err(), "hash validation failure");
    }
}
