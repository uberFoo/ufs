use std::collections::{HashMap, VecDeque};

use bincode;
use failure::{format_err, Error};

use crate::block::{
    hash::BlockHash, meta::BlockMetadata, storage::BlockStorage, Block, BlockCardinality, BlockList,
};

/// Manager of Blocks
///
/// This sits atop a BlockStorage and provides higher-level operations over blocks.  For example,
/// reads and writes of arbitrary size (files) are aggregated across multiple blocks.  Per-block
/// hashes are calculated when writing, and validated when reading, a block.  Data written across
/// multiple blocks are stored as a [BlockList], etc.
#[derive(Debug, PartialEq)]
pub(crate) struct BlockManager<BS>
where
    BS: BlockStorage,
{
    store: BS,
    free_blocks: VecDeque<BlockCardinality>,
    // FIXME: This should be pulled out into a struct, and the keys should be SHA256 hashes for
    // security sake.
    block_list_map: HashMap<String, BlockList>,
}

impl<BS> BlockManager<BS>
where
    BS: BlockStorage,
{
    pub(crate) fn new(store: BS) -> Self {
        BlockManager {
            free_blocks: (1..store.block_count()).collect(),
            block_list_map: HashMap::new(),
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
            block_list_map: metadata.block_list_map,
            store,
        }
    }

    /// Store some bytes and reference them with a name.
    ///
    /// FIXME:
    ///  * This does not belong here -- it's hacky and gross.
    ///  * The `name` should be hashed and stored that way.
    pub(crate) fn write_bytes<T>(&mut self, name: String, data: T) -> Result<(), Error>
    where
        T: AsRef<[u8]>,
    {
        match self.write(data) {
            Ok(blocks) => {
                let _previous_blocks = self
                    .block_list_map
                    .insert(name, blocks)
                    .unwrap_or(BlockList::new(vec![]));
                Ok(())
            }
            Err(e) => Err(format_err!("failed to write bytes {}", e)),
        }
    }

    /// Retrieve some bytes, given a name
    ///
    /// FIXME: Same as `write_bites` above.
    pub(crate) fn read_bytes<S>(&self, name: S) -> Result<Vec<u8>, Error>
    where
        S: Into<String>,
    {
        let name = name.into();
        match self.block_list_map.get(&name) {
            Some(blocks) => self.read(blocks),
            None => Err(format_err!("key '{}' not found", name)),
        }
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
            block_list_map: self.block_list_map.clone(),
        };

        self.store
            .write_block(0, bincode::serialize(&meta).unwrap())
            .unwrap();
    }

    /// Write Some Bytes
    ///
    /// The bytes are written to the minimum number of blocks required to store the furnished slice.
    /// The list of blocks that now contain the bytes is returned.  Hashes will be created,
    /// Merkle tree, blah, blah.
    ///
    /// FIXME: I wonder is using slice::chunks() would be better?
    pub(crate) fn write<T>(&mut self, data: T) -> Result<BlockList, Error>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        let block_size = self.store.block_size() as usize;
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
                self.store.write_block(free_block_num, block_data_slice)?;

                blocks.push(Block {
                    number: free_block_num,
                    hash: BlockHash::new(block_data_slice),
                });
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
    /// FIXME: This is where we should also run a proof on the Merkle Tree.
    pub(crate) fn read(&self, blocks: &BlockList) -> Result<Vec<u8>, Error> {
        let mut data = Vec::<u8>::with_capacity(blocks.len() * self.store.block_size() as usize);
        for b in blocks.iter() {
            let mut data_block = self.store.read_block(b.number)?;
            let hash = BlockHash::new(&data_block);
            if hash == b.hash {
                data.append(&mut data_block);
            } else {
                // Do we want to introduce the idea of a "bad hash sentinel block"?
                return Err(format_err!(
                    "hash mismatch: expected {:?}, but calculated {:?}",
                    b.hash,
                    hash
                ));
            }
        }

        Ok(data)
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

        let blocks = bm.write(b"abc").unwrap();
        println!("{:#?}", blocks);

        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 1);
        assert_eq!(
            blocks[0].hash.as_ref(),
            hex!("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
            "validate hash"
        );

        assert_eq!(
            bm.read(&blocks).unwrap(),
            b"abc",
            // &expected[..],
            "compare stored data with expected values"
        );
    }

    #[test]
    fn write_data_smaller_than_blocksize() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 2));

        let blocks = bm.write(&vec![0x38; 511][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 1);
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &vec![0x38; 511][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", blocks);
        println!("{:?}", bm.read(&blocks));
    }

    #[test]
    fn write_data_larger_than_blocksize() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 3));

        let blocks = bm.write(&vec![0x38; 513][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 2);
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &vec![0x38; 513][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", blocks);
    }

    #[test]
    fn write_data_multiple_of_blocksize() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 3));

        let blocks = bm.write(&vec![0x38; 1024][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 2);
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &vec![0x38; 1024][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", blocks);
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
        let mut bm = BlockManager::new(FileStore::new(&path, BlockSize::FiveTwelve, 4).unwrap());

        bm.serialize();
        let (fs, metadata) = FileStore::load(&path).unwrap();
        let bm2 = BlockManager::load(fs, metadata);
        assert_eq!(bm, bm2);

        bm.write_bytes("test".to_string(), b"Hello World!");
        assert_eq!(bm.read_bytes("test").unwrap(), b"Hello World!");

        // Just another random change.
        bm.get_free_block().unwrap();
        bm.serialize();
        let (fs, metadata) = FileStore::load(&path).unwrap();
        let bm2 = BlockManager::load(fs, metadata);
        assert_eq!(bm, bm2);
    }
}
