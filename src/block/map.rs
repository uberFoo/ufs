//! Block Map
//!
//! Mapping from block number to block type.
//!
//! The block map stores the file system id, the block size, and count, the list of free blocks, the
//! root block (where the file system metadata is located), as well as the list of blocks in the
//! file system.
//!
//! At this time block 0 is reserved as the starting place for the block map.  Blocks are then
//! dynamically allocated, and written with the Block Map as necessary.
//!
use std::collections::VecDeque;

use failure::format_err;
use log::{debug, error, info, trace};
use serde_derive::{Deserialize, Serialize};

use crate::{
    block::{
        Block, BlockCardinality, BlockHash, BlockNumber, BlockReader, BlockSize, BlockSizeType,
        BlockWriter,
    },
    UfsUuid,
};

/// Block Map Wrapper Type
///
/// The size of the block map changes over time, and while a maximum  _could_ be determined at
/// runtime, I prefer a dynamic solution -- for now anyway.
///
/// This type chunks the block map data across the disk, starting at block 0.  Each block contains a
///  pointer to the next block, and the data is aggregated and reconstituted when read.
#[derive(Debug, Deserialize, Serialize)]
struct BlockMapWrapper {
    /// Underlying data
    data: Vec<u8>,
    /// Hash value for the Block's data, excluding the hash value itself.
    hash: BlockHash,
    next_block: Option<BlockNumber>,
}

/// Block Map
///
/// A mapping from block number to Blocks.  Each block is one of several block types, where each
/// type may include metadata about the underlying block.  For instance, the hash value of the
/// block, and the next block to come after it.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BlockMap {
    /// The UUID of this file system
    ///
    id: UfsUuid,
    /// The size of each block, in bytes
    ///
    size: BlockSize,
    /// The number of blocks in the file system
    ///
    count: BlockCardinality,
    /// The first block in the block map -- it contains the first metadata block
    ///
    root_block: Option<BlockNumber>,
    /// A list of blocks that contain the block map (as `BlockMapWrapper`s)
    ///
    block_map_blocks: Vec<BlockNumber>,
    /// The list of free blocks
    ///
    free_blocks: VecDeque<BlockNumber>,
    /// The map itself
    ///
    map: Vec<Block>,
}

impl BlockMap {
    /// Create a new Block Map
    ///
    /// The resultant block map will contain a metadata block at block 0.
    pub fn new(id: UfsUuid, size: BlockSize, count: BlockCardinality) -> Self {
        // Mark the 0 block as metadata
        let mut map = (0..count).map(|b| Block::new(b)).collect::<Vec<_>>();
        map[0].tag_map();

        BlockMap {
            id,
            size,
            count,
            block_map_blocks: vec![0],
            free_blocks: (1..count).collect(),
            root_block: None,
            map,
        }
    }

    /// Return the file system id
    pub(in crate::block) fn id(&self) -> &UfsUuid {
        &self.id
    }

    /// Return the file system block size, as a `BlockSize` struct
    pub(in crate::block) fn block_size(&self) -> BlockSize {
        self.size
    }

    /// Return the total number of blocks in the file system
    pub(in crate::block) fn block_count(&self) -> BlockCardinality {
        self.count
    }

    /// Return a reference to the list of free blocks in the file system
    pub(crate) fn free_blocks(&self) -> &VecDeque<BlockNumber> {
        &self.free_blocks
    }

    /// Return a mutable reference to the list of free blocks in the file system
    pub(crate) fn free_blocks_mut(&mut self) -> &mut VecDeque<BlockNumber> {
        &mut self.free_blocks
    }

    pub(crate) fn set_root_block(&mut self, block: BlockNumber) {
        self.root_block = Some(block);
    }

    pub(crate) fn root_block(&self) -> Option<BlockNumber> {
        self.root_block
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
    pub(in crate::block) fn serialize<BS: BlockWriter>(
        &mut self,
        store: &mut BS,
    ) -> Result<(), failure::Error> {
        // Determine the amount of data that we can store in each Block -- its the block size minus
        // the amount of data it takes to encode a BlockMapWrapper with no data.
        let zero_wrapper = BlockMapWrapper {
            data: vec![0; 0],
            hash: BlockHash::new(b""),
            next_block: Some(0 as BlockCardinality),
        };
        let chunk_size = BlockSizeType::from(self.size) as u64
            - bincode::serialized_size(&zero_wrapper).unwrap();

        // Determine the number of blocks we need.
        let mut bytes = bincode::serialize(&self).unwrap();
        let mut block_count = bytes.len() as u64 / chunk_size
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
        while block_count > self.block_map_blocks.len() as u64 {
            while block_count > self.block_map_blocks.len() as u64 {
                let meta_block = match self.free_blocks.pop_front() {
                    Some(b) => b,
                    None => return Err(format_err!("No free blocks.")),
                };
                debug!("Allocating new blockmap wrapper block {}", meta_block);

                // We can tag this block as containing block map data, but we can't give it a size
                // nor a hash, because we don't know that information now. By the time we do know
                // that information, we've already serialized the block.
                self.map[meta_block as usize].tag_map();
                self.block_map_blocks.push(meta_block);
            }

            // Grab a fresh version of ourself to serialize since we converted free blocks to
            // metadata blocks
            bytes = bincode::serialize(&self)?;

            block_count = bytes.len() as u64 / chunk_size
                + if bytes.len() as u64 % chunk_size > 0 {
                    1
                } else {
                    0
                };
        }

        // Iterate over the chunks of serialized block map, and write them to the block store.
        bytes
            .chunks(chunk_size as usize)
            .enumerate()
            .map(|(count, chunk)| {
                let block = self.block_map_blocks[count];
                let next_block = if count < block_count as usize - 1 {
                    Some(self.block_map_blocks[count + 1])
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

    pub(in crate::block) fn deserialize<BS: BlockReader>(
        store: &BS,
    ) -> Result<Self, failure::Error> {
        let mut map = Vec::<u8>::new();

        // We know that we always start at block 0.
        let mut block = read_wrapper_block(store, 0)?;
        map.append(&mut block.data);

        while let Some(next) = block.next_block {
            block = read_wrapper_block(store, next)?;
            map.append(&mut block.data);
        }

        match bincode::deserialize::<BlockMap>(&map) {
            Ok(map) => {
                info!("Loaded BlockMap:");
                info!("\tid: {}", map.id);
                info!("\tblock size: {}", map.size);
                info!("\tblock count: {}", map.count);
                info!("\tfree blocks: {}", map.free_blocks.len());
                match map.root_block {
                    Some(b) => info!("\troot block: {}", b),
                    None => (),
                };
                Ok(map)
            }
            Err(e) => {
                error!("Failed to deserialize block map.");
                Err(e.into())
            }
        }
    }
}

fn read_wrapper_block<BS: BlockReader>(
    store: &BS,
    number: BlockNumber,
) -> Result<BlockMapWrapper, failure::Error> {
    debug!("Reading block map from block {}", number);
    let bytes = store.read_block(number)?;
    match bincode::deserialize::<BlockMapWrapper>(&bytes) {
        Ok(block) => {
            if block.hash.validate(&block.data) {
                return Ok(block);
            } else {
                error!("Error validating block {}", number);
                return Err(format_err!("Error validating block {}", number));
            }
        }
        Err(e) => Err(format_err!("Error deserializing block {}: {}", number, e)),
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(in crate) enum BlockType {
    Free,
    Data,
    Map,
    Metadata,
}

impl BlockType {
    pub(in crate::block) fn new_free() -> Self {
        BlockType::Free
    }

    pub(in crate::block) fn new_data() -> Self {
        BlockType::Data
    }

    pub(in crate::block) fn new_map() -> Self {
        BlockType::Map
    }

    pub(in crate::block) fn new_metadata() -> Self {
        BlockType::Metadata
    }

    #[allow(dead_code)]
    pub(in crate::block) fn is_free(&self) -> bool {
        match self {
            BlockType::Free => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub(in crate::block) fn is_data(&self) -> bool {
        match self {
            BlockType::Data => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub(in crate::block) fn is_map(&self) -> bool {
        match self {
            BlockType::Map => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub(in crate::block) fn is_metadata(&self) -> bool {
        match self {
            BlockType::Metadata => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {
    use env_logger;

    use super::*;

    use crate::block::MemoryStore;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn one_block_simple() {
        init();
        let id = UfsUuid::new_root_fs("test");
        let mut map = BlockMap::new(id, BlockSize::FiveTwelve, 10);

        // This is ugly, but ok for testing I think.
        let mut ms = MemoryStore::new(map.clone());
        assert!(map.serialize(&mut ms).is_ok());

        let map_2 = BlockMap::deserialize(&ms).unwrap();

        assert!(
            map_2.get(0).unwrap().is_map(),
            "block 0 should be BlockType::Map"
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
        let id = UfsUuid::new_root_fs("test");
        let mut map = BlockMap::new(id, BlockSize::FiveTwelve, 100);

        for _ in 1..100 {
            map.free_blocks.pop_front();
        }

        let mut ms = MemoryStore::new(map.clone());
        assert!(map.serialize(&mut ms).is_err());
    }

    #[test]
    fn test_large_blocks() {
        init();
        let id = UfsUuid::new_root_fs("test");
        let mut map = BlockMap::new(id, BlockSize::TwentyFortyEight, 100);

        // This tests that we skip data blocks.
        for x in 1..8 {
            map.free_blocks.pop_front();
            map.get_mut(x).unwrap().tag_data();
        }

        let mut ms = MemoryStore::new(map.clone());
        assert!(map.serialize(&mut ms).is_ok());

        let map_2 = BlockMap::deserialize(&ms).unwrap();

        // Two, 2048-byte blocks are needed for 100 blocks.
        assert!(
            map_2.get(0).unwrap().is_map(),
            "block 0 should be BlockType::Map"
        );
        assert!(
            map_2.get(8).unwrap().is_map(),
            "block 8 should be BlockType::Map",
        );

        for x in 1..8 {
            assert!(
                map_2.get(x).unwrap().is_data(),
                "block {} should be BlockType::Data",
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
        let id = UfsUuid::new_root_fs("test");
        let mut map = BlockMap::new(id, BlockSize::FiveTwelve, 100);

        // This tests that we skip data blocks.
        for x in 1..8 {
            map.free_blocks.pop_front();
            map.get_mut(x).unwrap().tag_data();
        }

        let mut ms = MemoryStore::new(map.clone());
        assert!(map.serialize(&mut ms).is_ok());

        let map_2 = BlockMap::deserialize(&ms).unwrap();

        assert!(
            map_2.get(0).unwrap().is_map(),
            "block 0 should be BlockType::Map"
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
                map_2.get(x).unwrap().is_map(),
                "block {} should be BlockType::Map",
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
