//! Memory-based Block Storage
//!
//! This is a toy that is useful for testing.  The blocks are stored in a Vec.
use std::collections::VecDeque;

use failure::{format_err, Error};

use crate::block::{Block, BlockCardinality, BlockHash, BlockManager, BlockSize, BlockStorage};

#[derive(Debug)]
pub struct MemoryStore {
    block_size: BlockSize,
    block_count: BlockCardinality,
    free_blocks: VecDeque<BlockCardinality>,
    blocks: Vec<Vec<u8>>,
}

impl MemoryStore {
    pub fn new(size: BlockSize, count: BlockCardinality) -> Self {
        MemoryStore {
            block_size: size,
            block_count: count,
            free_blocks: (0..count).collect(),
            blocks: (0..count)
                .map(|_| Vec::with_capacity(size as usize))
                .collect(),
        }
    }
}

impl BlockStorage for MemoryStore {
    fn block_count(&self) -> BlockCardinality {
        self.block_count
    }

    fn block_size(&self) -> BlockSize {
        self.block_size
    }

    fn write_block(&mut self, bn: BlockCardinality, data: &[u8]) -> Result<Block, Error> {
        if data.len() > self.block_size as usize {
            return Err(format_err!("data is larger than block size"));
        }

        if let Some(memory) = self.blocks.get_mut(bn as usize) {
            // Need to either initialize a new block, or clean up one that has been recycled.
            // I'd love to know if there is a faster way to do this.
            for _ in 0..self.block_size as usize {
                memory.push(0x0);
            }
            memory[..data.len()].copy_from_slice(&data[..]);

            Ok(Block {
                number: bn,
                hash: BlockHash::new(&memory),
            })
        } else {
            Err(format_err!("request for bogus block {}", bn))
        }
    }

    fn read_block(&self, block: &Block) -> Result<Vec<u8>, Error> {
        if let Some(memory) = self.blocks.get(block.number as usize) {
            let hash = BlockHash::new(&memory);
            if block.hash == hash {
                Ok(memory.clone())
            } else {
                Err(format_err!(
                    "hash mismatch: expected {:?}, but calculated {:?}",
                    block.hash,
                    hash
                ))
            }
        } else {
            Err(format_err!("request for bogus block {}", block.number))
        }
    }
}

impl BlockManager for MemoryStore {
    fn free_block_count(&self) -> BlockCardinality {
        self.free_blocks.len() as BlockCardinality
    }

    fn get_free_block(&mut self) -> Option<BlockCardinality> {
        self.free_blocks.pop_front()
    }

    fn recycle_block(&mut self, block: BlockCardinality) {
        self.free_blocks.push_back(block);
    }
}

#[cfg(test)]
mod test {
    use hex_literal::{hex, hex_impl};

    use super::*;

    #[test]
    fn bad_block_number() {
        let data = [0x0; BlockSize::FiveTwelve as usize];
        let mut ms = MemoryStore::new(BlockSize::FiveTwelve, 3);

        let block = Block {
            number: 7,
            hash: BlockHash::from(
                &hex!("62c2eacaf26c12f80eeb0b5b849c8805e0295db339dd793620190680799bec95")[..],
            ),
        };

        assert_eq!(
            ms.read_block(&block).is_err(),
            true,
            "read should fail with block number out of range"
        );
        assert_eq!(
            ms.write_block(7, &data).is_err(),
            true,
            "write should fail with block number out of range"
        );
    }

    #[test]
    fn block_too_bukoo() {
        let data = [0x0; BlockSize::FiveTwelve as usize + 1];
        let mut ms = MemoryStore::new(BlockSize::FiveTwelve, 3);
        assert_eq!(ms.write_block(1, &data).is_err(), true);
    }

    #[test]
    fn write_block() {
        let data = hex!(
            "451101250ec6f26652249d59dc974b7361d571a8101cdfd36aba3b5854d3ae086b5fdd4597721b66e3c0dc5
            d8c606d9657d0e323283a5217d1f53f2f284f57b85c8a61ac8924711f895c5ed90ef17745ed2d728abd22a5f
            7a13479a462d71b56c19a74a40b655c58edfe0a188ad2cf46cbf30524f65d423c837dd1ff2bf462ac4198007
            345bb44dbb7b1c861298cdf61982a833afc728fae1eda2f87aa2c9480858bec"
        );
        let mut data_block = vec![0x0; BlockSize::FiveTwelve as usize];
        data_block[..data.len()].copy_from_slice(&data[..]);

        let mut ms = MemoryStore::new(BlockSize::FiveTwelve, 3);
        let block = ms.write_block(1, &data[..]).unwrap();

        assert_eq!(block.number, 1);
        assert_eq!(
            block.hash.as_ref(),
            hex!("62c2eacaf26c12f80eeb0b5b849c8805e0295db339dd793620190680799bec95"),
            "validate hash"
        );

        assert_eq!(
            ms.blocks[1], data_block,
            "API write to block, and compare directly"
        );
    }

    #[test]
    fn read_block() {
        let data = hex!(
            "451101250ec6f26652249d59dc974b7361d571a8101cdfd36aba3b5854d3ae086b5fdd4597721b66e3c0dc5
            d8c606d9657d0e323283a5217d1f53f2f284f57b85c8a61ac8924711f895c5ed90ef17745ed2d728abd22a5f
            7a13479a462d71b56c19a74a40b655c58edfe0a188ad2cf46cbf30524f65d423c837dd1ff2bf462ac4198007
            345bb44dbb7b1c861298cdf61982a833afc728fae1eda2f87aa2c9480858bec"
        );
        let mut expected_block = vec![0x0; BlockSize::FiveTwelve as usize];
        expected_block[..data.len()].copy_from_slice(&data[..]);

        let mut ms = MemoryStore::new(BlockSize::FiveTwelve, 3);
        ms.blocks[0] = vec![0x0; BlockSize::FiveTwelve as usize];
        ms.blocks[0].copy_from_slice(&expected_block[..]);

        let block = Block {
            number: 0,
            hash: BlockHash::from(
                &hex!("62c2eacaf26c12f80eeb0b5b849c8805e0295db339dd793620190680799bec95")[..],
            ),
        };

        assert_eq!(
            ms.read_block(&block).unwrap(),
            expected_block,
            "write directly to block, and compare via the API"
        );
    }

    #[test]
    fn read_block_bad_hash() {
        let data = hex!(
            "451101250ec6f26652249d59dc974b7361d571a8101cdfd36aba3b5854d3ae086b5fdd4597721b66e3c0dc5
            d8c606d9657d0e323283a5217d1f53f2f284f57b85c8a61ac8924711f895c5ed90ef17745ed2d728abd22a5f
            7a13479a462d71b56c19a74a40b655c58edfe0a188ad2cf46cbf30524f65d423c837dd1ff2bf462ac4198007
            345bb44dbb7b1c861298cdf61982a833afc728fae1eda2f87aa2c9480858bec"
        );
        let mut expected_block = vec![0x0; BlockSize::FiveTwelve as usize];
        expected_block[..data.len()].copy_from_slice(&data[..]);

        let mut ms = MemoryStore::new(BlockSize::FiveTwelve, 3);
        ms.blocks[0] = vec![0x0; BlockSize::FiveTwelve as usize];
        ms.blocks[0].copy_from_slice(&expected_block[..]);

        // Corrupt the block data
        ms.blocks[0][0] = 0;

        let block = Block {
            number: 0,
            hash: BlockHash::from(
                &hex!("62c2eacaf26c12f80eeb0b5b849c8805e0295db339dd793620190680799bec95")[..],
            ),
        };

        println!("block {:?}", &block);

        assert_eq!(
            ms.read_block(&block).is_err(),
            true,
            "detect a hash mismatch"
        );
    }

    #[test]
    fn construction_sanity() {
        let bm = MemoryStore::new(BlockSize::FiveTwelve, 3);
        assert_eq!(
            bm.free_block_count(),
            3,
            "verify that there are three free blocks"
        );
        assert_eq!(
            bm.block_size() as usize,
            512,
            "verify block size as 512 bytes"
        );
        assert_eq!(
            bm.block_count(),
            3,
            "verify that there are three blocks total"
        );
    }

    #[test]
    fn not_enough_free_blocks_error() {
        let mut bm = MemoryStore::new(BlockSize::FiveTwelve, 1);
        let blocks = bm.write(&vec![0x0; 513][..]);
        assert_eq!(
            blocks.is_err(),
            true,
            "verify that more blocks are needed for write"
        );
    }

    #[test]
    fn tiny_test() {
        let mut bm = MemoryStore::new(BlockSize::FiveTwelve, 1);
        let blocks = bm.write(b"abc").unwrap();
        println!("{:#?}", blocks);

        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 1);
        assert_eq!(
            blocks[0].hash.as_ref(),
            hex!("b064446561934ed673ed230b6c0e68ebde7d574bf81288b00ac88ff6e518ade4"),
            "validate hash"
        );

        let mut expected = vec![0x0; 512];
        expected[0] = 97;
        expected[1] = 98;
        expected[2] = 99;
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &expected[..],
            "compare stored data with expected values"
        );
    }

    #[test]
    fn write_data_smaller_than_blocksize() {
        let mut bm = MemoryStore::new(BlockSize::FiveTwelve, 1);
        let blocks = bm.write(&vec![0x38; 511][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 1);
        let mut expected = vec![0x38; 511];
        expected.push(0x0);
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &expected[..],
            "compare stored data with expected values"
        );
        println!("{:#?}", blocks);
        println!("{:?}", bm.read(&blocks));
    }

    #[test]
    fn write_data_larger_than_blocksize() {
        let mut bm = MemoryStore::new(BlockSize::FiveTwelve, 2);
        let blocks = bm.write(&vec![0x38; 513][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 2);
        let mut expected = vec![0x0; 1024];
        expected[..513].copy_from_slice(&vec![0x38; 513][..]);
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &expected[..],
            "compare stored data with expected values"
        );
        println!("{:#?}", blocks);
    }

    #[test]
    fn write_data_multiple_of_blocksize() {
        let mut bm = MemoryStore::new(BlockSize::FiveTwelve, 2);
        let blocks = bm.write(&vec![0x38; 1024][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 2);
        // let mut expected = vec![0x0; 1024];
        // expected[..513].copy_from_slice(&vec![0x38; 513][..]);
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &vec![0x38; 1024][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", blocks);
    }
}
