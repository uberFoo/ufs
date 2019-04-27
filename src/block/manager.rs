use std::collections::{HashMap, VecDeque};

use bincode;
use failure::{format_err, Error};
use log::trace;

use crate::block::{
    hash::BlockHash, meta::BlockMetadata, storage::BlockStorage, Block, BlockCardinality,
    BlockSize, BlockSizeType,
};

/// Manager of Blocks
///
/// This sits atop a BlockStorage and provides higher-level operations over blocks.  For example,
/// reads and writes of arbitrary size (files) are aggregated across multiple blocks.  Per-block
/// hashes are calculated when writing, and validated when reading, a block.  Data written across
/// multiple blocks are stored as a [BlockList], etc.
#[derive(Clone, Debug, PartialEq)]
pub struct BlockManager<BS>
where
    BS: BlockStorage,
{
    store: BS,
    free_blocks: VecDeque<BlockCardinality>,
    directory: HashMap<String, Block>,
}

impl<'a, BS> BlockManager<BS>
where
    BS: BlockStorage,
{
    pub fn new(store: BS) -> Self {
        BlockManager {
            free_blocks: (1..store.block_count()).collect(),
            directory: HashMap::new(),
            store,
        }
    }

    /// FIXME: This may be nice in a From<BlockMetadata>
    pub(crate) fn load(store: BS, metadata: BlockMetadata) -> Self {
        let free_blocks = match metadata.next_free_block {
            Some(n) => (n..metadata.count).collect(),
            None => VecDeque::new(),
        };
        BlockManager {
            free_blocks,
            directory: metadata.directory,
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
        let meta = BlockMetadata {
            size: self.store.block_size(),
            count: self.store.block_count(),
            next_free_block: self.free_blocks.get(0).cloned(),
            directory: self.directory.clone(),
        };

        self.store
            .write_block(0, bincode::serialize(&meta).unwrap())
            .unwrap();
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

    pub fn read_raw_number(&self, number: BlockCardinality) -> Result<Vec<u8>, Error> {
        let bytes = self.store.read_block(number)?;
        Ok(bytes)
    }

    pub(crate) fn reserve_metadata<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        self.directory
            .entry(key.into())
            .or_insert(Block::null_block());
    }

    pub(crate) fn write_metadata<K, D>(&mut self, key: K, data: D) -> Result<BlockSizeType, Error>
    where
        K: Into<String>,
        D: AsRef<[u8]>,
    {
        let block = self.write(data)?;
        let size = block.size();
        self.directory.insert(key.into(), block);
        Ok(size as BlockSizeType)
    }

    /// Return file system-level matadata
    ///
    /// FIXME: Should this instead return an `Option`?
    pub(crate) fn read_metadata<K>(&self, key: K) -> Result<Vec<u8>, Error>
    where
        K: AsRef<str>,
    {
        if let Some(block) = self.directory.get(key.as_ref()) {
            self.read(block)
        } else {
            Err(format_err!("key not found"))
        }
    }

    pub(crate) fn metadata(&self) -> std::collections::hash_map::Iter<String, Block> {
        self.directory.iter()
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
        // let data = hex!(
        //     "451101250ec6f26652249d59dc974b7361d571a8101cdfd36aba3b5854d3ae086b5fdd4597721b66e3c0dc5
        //     d8c606d9657d0e323283a5217d1f53f2f284f57b85c8a61ac8924711f895c5ed90ef17745ed2d728abd22a5f
        //     7a13479a462d71b56c19a74a40b655c58edfe0a188ad2cf46cbf30524f65d423c837dd1ff2bf462ac4198007
        //     345bb44dbb7b1c861298cdf61982a833afc728fae1eda2f87aa2c9480858bec"
        // );
        // let mut expected_block = vec![0x0; BlockSize::FiveTwelve as usize];
        // expected_block[..data.len()].copy_from_slice(&data[..]);

        // let mut ms = MemoryStore::new(BlockSize::FiveTwelve, 3);
        // ms.blocks[0] = vec![0x0; BlockSize::FiveTwelve as usize];
        // ms.blocks[0].copy_from_slice(&expected_block[..]);

        // // Corrupt the block data
        // ms.blocks[0][0] = 0;

        // let block = Block {
        //     number: 0,
        //     hash: BlockHash::from(
        //         &hex!("62c2eacaf26c12f80eeb0b5b849c8805e0295db339dd793620190680799bec95")[..],
        //     ),
        // };

        // println!("block {:?}", &block);

        // assert_eq!(
        //     ms.read(&BlockList::new(vec![block])).is_err(),
        //     true,
        //     "detect a hash mismatch"
        // );
    }

    #[test]
    fn metadata() {
        let path = "/tmp/ufs_test/meta";
        let mut bm = BlockManager::new(FileStore::new(&path, BlockSize::FiveTwelve, 5).unwrap());

        bm.serialize();
        let (fs, metadata) = FileStore::load(&path).unwrap();
        let bm2 = BlockManager::load(fs, metadata);
        assert_eq!(bm, bm2);

        bm.write_metadata("test", b"Hello World!").unwrap();
        assert_eq!(bm.read_metadata("test").unwrap(), b"Hello World!");

        // Just another random change.
        bm.get_free_block().unwrap();
        bm.serialize();

        let (fs, metadata) = FileStore::load(&path).unwrap();
        let bm2 = BlockManager::load(fs, metadata);
        assert_eq!(bm, bm2);
    }
}
