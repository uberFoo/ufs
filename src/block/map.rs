//! Block Map
//!
//! Mapping from block number to black type.
//!
//! At this time block 0 is reserved as the starting place for the block map.  Blocks are then
//! dynamically allocated, and written with the Block Map as necessary.
//!
use std::collections::VecDeque;

use failure::{format_err, Error};
use log::{debug, error, trace};
use serde_derive::{Deserialize, Serialize};

use crate::{
    block::{
        hash::BlockHash, storage::BlockStorage, Block, BlockCardinality, BlockNumber, BlockSize,
        BlockSizeType,
    },
    metadata::Metadata,
    UfsUuid,
};

#[derive(Debug, Deserialize, Serialize)]
/// Block Map Wrapper Type
///
/// The size of the block map changes over time, and while a maximum  _could_ be determined at
/// runtime, I prefer a dynamic solution -- for now anyway.
///
/// This type chunks the block map data across the disk, starting at block 0.  Each block contains a
///  pointer to the next block, and the data is aggregated and reconstituted when read.
struct BlockMapWrapper {
    /// Underlying data
    data: Vec<u8>,
    /// Hash value for the Block's data, excluding the hash value itself.
    hash: BlockHash,
    next_block: Option<BlockNumber>,
}

// impl BlockMapWrapper {
//     pub(in crate::block) fn deserialize<BS>(store: BS) -> Self
//     where
//         BS: BlockStorage,
//     {
//         // We know that we always start at block 0.
//         let start = bincode::deserialize(store.read_block(0).unwrap()).unwrap();
//     }
// }

#[derive(Debug, Deserialize, PartialEq, Serialize)]
/// Block Map
///
/// A mapping from block number to Blocks.  Each block is one of several block types, where each
/// type may include metadata about the underlying block.  For instance, the hash value of the
/// block, and the next block to come after it.
struct BlockMap {
    id: UfsUuid,
    size: BlockSize,
    count: BlockCardinality,
    metadata_blocks: Vec<BlockNumber>,
    free_blocks: VecDeque<BlockNumber>,
    map: Vec<Block>,
}

enum SND<T> {
    None,
    Some(T),
    Done,
}

impl BlockMap {
    /// Create a new Block Map
    ///
    /// The resultant block map will contain a metadata block at block 0.
    pub(crate) fn new(id: UfsUuid, size: BlockSize, count: BlockCardinality) -> Self {
        BlockMap {
            id,
            size,
            count,
            metadata_blocks: vec![0],
            free_blocks: (1..count).collect(),
            map: (0..count).map(|b| Block::new_free(b)).collect::<Vec<_>>(),
        }
    }

    pub(crate) fn get(&self, number: BlockNumber) -> Option<&Block> {
        self.map.get(number as usize)
    }

    pub(crate) fn get_mut(&mut self, number: BlockNumber) -> Option<&mut Block> {
        self.map.get_mut(number as usize)
    }

    // I'm deciding to overwrite the block map here.  We reuse blocks that were
    // previously allocated as metadata blocks, and add more if necessary.  I don't
    // think that this is terrible, as the map is the current state of the file system,
    // and any versioned files will still be versioned.
    // FIXME: This isn't really a map any longer.
    pub(in crate::block) fn serialize<BS: BlockStorage>(
        &mut self,
        store: &mut BS,
    ) -> Result<(), Error> {
        let zero_wrapper = BlockMapWrapper {
            data: vec![0; 0],
            hash: BlockHash::new(b""),
            next_block: Some(0 as BlockCardinality),
        };
        let chunk_size = BlockSizeType::from(self.size) as u64
            - bincode::serialized_size(&zero_wrapper).unwrap();

        // Determine the number of blocks we need.
        let mut bytes = bincode::serialize(&self).unwrap();
        let block_count = bytes.len() as u64 / chunk_size
            + if bytes.len() as u64 % chunk_size > 0 {
                1
            } else {
                0
            };

        debug!(
            "BlockMap is {} bytes; chunk size is {} bytes: {} blocks needed.",
            bytes.len(),
            chunk_size,
            block_count
        );

        // Collect a list of block numbers we'll use to write the block map.
        if block_count > self.metadata_blocks.len() as u64 {
            while block_count > self.metadata_blocks.len() as u64 {
                let meta_block = match self.free_blocks.pop_front() {
                    Some(b) => b,
                    None => return Err(format_err!("No free blocks.")),
                };
                debug!("Allocating new metadata block {}", meta_block);
                // self.get_mut(meta_block).unwrap().tag_metadata();
                self.map[meta_block as usize].tag_metadata();
                self.metadata_blocks.push(meta_block);
            }

            // Grab a fresh version of ourself to serialize since we converted free blocks to
            // metadata blocks
            bytes = bincode::serialize(&self).unwrap();
        }

        // Iterate over the chunks of serialized block map, and writing them to the block store.
        bytes
            .chunks(chunk_size as usize)
            .enumerate()
            .map(|(count, chunk)| {
                let block = self.metadata_blocks[count];
                let next_block = if count < block_count as usize - 1 {
                    Some(self.metadata_blocks[count + 1])
                } else {
                    None
                };

                let wrapper = BlockMapWrapper {
                    data: chunk.to_vec(),
                    hash: BlockHash::new(&chunk),
                    next_block,
                };

                debug!(
                    "Writing blockmap wrapper number {}; next block {:?}",
                    count, next_block
                );
                trace!("{:?}", wrapper);

                match store.write_block(block, bincode::serialize(&wrapper).unwrap()) {
                    Ok(b) => {
                        debug!("Wrote {} blockmap bytes to block {}", b, block);
                        Ok(())
                    }
                    Err(e) => {
                        error!("Error writing blockmap bytes: {}", e);
                        Err(e)
                    }
                }
            })
            .collect()
    }

    pub(in crate::block) fn deserialize<BS: BlockStorage>(store: &BS) -> Self {
        let mut map = Vec::<u8>::new();

        // We know that we always start at block 0.
        let mut block = read_wrapper_block(store, 0);
        map.append(&mut block.data);

        while let Some(next) = block.next_block {
            block = read_wrapper_block(store, next);
            map.append(&mut block.data);
        }

        bincode::deserialize(&map).unwrap_or_else(|e| {
            error!("Failed to deserialize block map.");
            panic!(e)
        })
    }
}

fn read_wrapper_block<BS: BlockStorage>(store: &BS, number: BlockNumber) -> BlockMapWrapper {
    if let Ok(bytes) = store.read_block(number) {
        if let Ok(block) = bincode::deserialize::<BlockMapWrapper>(&bytes) {
            if block.hash.validate(&block.data) {
                return block;
            } else {
                error!("Error validating block {}", number);
                panic!();
            }
        } else {
            error!("Error deserializing block {}", number);
            panic!();
        }
    } else {
        error!("Error reading block {}", number);
        panic!()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(in crate::block) enum BlockType {
    Free,
    Data,
    Metadata,
}

impl BlockType {
    pub(in crate::block) fn new_free() -> Self {
        BlockType::Free
    }

    pub(in crate::block) fn new_data() -> Self {
        // BlockType::Data(DataBlock { next_block: None })
        BlockType::Data
    }

    pub(in crate::block) fn new_metadata() -> Self {
        // BlockType::Metadata(DataBlock { next_block: None })
        BlockType::Metadata
    }

    pub(in crate::block) fn is_free(&self) -> bool {
        match self {
            BlockType::Free => true,
            _ => false,
        }
    }

    pub(in crate::block) fn is_data(&self) -> bool {
        match self {
            BlockType::Data => true,
            _ => false,
        }
    }

    pub(in crate::block) fn is_metadata(&self) -> bool {
        match self {
            BlockType::Metadata => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {
    use assert_matches::assert_matches;
    use env_logger;

    use super::*;

    use crate::block::storage::memory::MemoryStore;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn one_block_simple() {
        init();
        let id = UfsUuid::new("test");
        let mut ms = MemoryStore::new(BlockSize::FiveTwelve, 10);

        let mut map = BlockMap::new(id, BlockSize::FiveTwelve, 10);

        // This tests that we pickup a metadata block.
        map.get_mut(0).unwrap().tag_metadata();

        assert!(map.serialize(&mut ms).is_ok());
        let map_2 = BlockMap::deserialize(&ms);

        assert!(
            map_2.get(0).unwrap().is_metadata(),
            "block 0 should be BlockType::Metadata"
        );

        for x in 1..10 {
            assert!(
                map_2.get(x).unwrap().is_free(),
                "block {} should be BlockType::Free",
                x
            );
        }
    }

    #[test]
    fn not_enough_blocks() {
        init();
        let id = UfsUuid::new("test");
        let mut ms = MemoryStore::new(BlockSize::FiveTwelve, 100);

        let mut map = BlockMap::new(id, BlockSize::FiveTwelve, 100);

        for x in 1..100 {
            map.free_blocks.pop_front();
        }
        assert!(map.serialize(&mut ms).is_err());
    }

    #[test]
    fn test_large_blocks() {
        init();
        let id = UfsUuid::new("test");
        let mut ms = MemoryStore::new(BlockSize::TwentyFortyEight, 100);

        let mut map = BlockMap::new(id, BlockSize::TwentyFortyEight, 100);
        // This tests that we pickup a metadata block.
        map.get_mut(0).unwrap().tag_metadata();

        // This tests that we skip data blocks.
        for x in 1..8 {
            map.free_blocks.pop_front();
            map.get_mut(x).unwrap().tag_data();
        }

        assert!(map.serialize(&mut ms).is_ok());
        let map_2 = BlockMap::deserialize(&ms);

        // Two, 2048-byte blocks are needed for 100 blocks.
        assert!(
            map_2.get(0).unwrap().is_metadata(),
            "block 0 should be BlockType::Metadata"
        );
        assert!(
            map_2.get(8).unwrap().is_metadata(),
            "block 8 should be BlockType::Metadata",
        );

        for x in 1..8 {
            assert!(
                map_2.get(x).unwrap().is_data(),
                "block {} shohuld be BlockType::Data",
                x
            );
        }
        for x in 9..100 {
            assert!(
                map_2.get(x).unwrap().is_free(),
                "block {} should be BlockType::Free",
                x
            );
        }
    }

    #[test]
    fn test_allocate_more_blocks_complex() {
        init();
        let id = UfsUuid::new("test");
        let mut ms = MemoryStore::new(BlockSize::FiveTwelve, 100);

        let mut map = BlockMap::new(id, BlockSize::FiveTwelve, 100);
        // This tests that we pickup a metadata block.
        map.get_mut(0).unwrap().tag_metadata();

        // This tests that we skip data blocks.
        for x in 1..8 {
            map.free_blocks.pop_front();
            map.get_mut(x).unwrap().tag_data();
        }

        assert!(map.serialize(&mut ms).is_ok());
        let map_2 = BlockMap::deserialize(&ms);

        assert!(
            map_2.get(0).unwrap().is_metadata(),
            "block 0 should be BlockType::Metadata"
        );

        for x in 1..8 {
            assert!(
                map_2.get(x).unwrap().is_data(),
                "block {} shohuld be BlockType::Data",
                x
            );
        }

        // Four, 512-byte blocks are needed for 100 blocks.
        for x in 8..12 {
            assert!(
                map_2.get(x).unwrap().is_metadata(),
                "block {} should be BlockType::Metadata",
                x
            );
        }

        for x in 12..100 {
            assert!(
                map_2.get(x).unwrap().is_free(),
                "block {} should be BlockType::Free",
                x
            );
        }
    }
}
