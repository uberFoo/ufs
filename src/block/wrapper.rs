//! Block Wrapping
//!
//! Metadata blocks don't have the benefit of the `BlockManager` to break large buffers into blocks.
//! That's where the `BlockWrapper` comes in. It is a sort of low-level block manager.
//!
use std::collections::VecDeque;

use failure::format_err;
use log::{debug, error, trace};
use serde_derive::{Deserialize, Serialize};

use crate::block::{
    hash::BlockHash,
    storage::{BlockReader, BlockStorage},
    BlockCardinality, BlockNumber, BlockSizeType,
};

#[derive(Debug, Deserialize, Serialize)]
/// Block Wrapper
///
/// This type chunks the metadata across the disk. Each block contains a pointer to the next block,
/// and the data is aggregated and reconstituted when read.
struct BlockWrapper {
    /// Underlying data
    data: Vec<u8>,
    /// Hash value for the Block's data, excluding the hash value itself.
    hash: BlockHash,
    next_block: Option<BlockNumber>,
}

pub(crate) trait MetadataSerialize {
    fn serialize(&mut self) -> Result<Vec<u8>, failure::Error>;
}

pub(crate) trait MetadataDeserialize: Sized {
    // type Meta;
    fn deserialize(bytes: Vec<u8>) -> Result<Self, failure::Error>;
}

/// Serialize Metadata
///
/// Metadata is special because it exists outside of the Block Manager, and needs to exist before
/// we know anything about blocks that make up files, etc. So we need to incorporate the list of
/// blocks that store the metadata with the data itself. That's where the `BlockWrapper` struct
/// comes in: it stores the metadata along side a hash for the metadata, and the next block in the
/// chain.
///
/// Serialization involves chunking the metadata into blocks that will fit inside the wrapper block,
/// and writing the blocks to storage.
///
/// # Paramaters
/// * `metadata` -- what we will be serializing
/// * `writer` -- BlockWriter where we write the wrapper blocks
/// * `reader` -- BlockReader is necessary for reading existing metadata information. Specifically
/// when we're dealing with the BlockMap, which lives at block 0 -- always. So we need to be able to
/// locate all the wrapper blocks starting there, and reuse them.
/// * `start_block` -- where to start writing (reading) metadata (blocks)
/// * `block_size` -- the size of the underlying blocks to which hmetadata is written
/// * `map` -- the block map that is storing all of this stuff
///
/// Note that the reader/writer and map are separate from the storage due to borrowing and lifetime
/// rules. Otherwise we could just pass in a storage implementation.
pub(in crate::block) fn write_metadata<B, M>(
    store: &mut B,
    metadata: &mut M,
) -> Result<BlockNumber, failure::Error>
where
    B: BlockStorage,
    M: MetadataSerialize,
{
    debug!("");
    debug!("*******");
    debug!("write_metadata");
    let bytes = metadata.serialize()?;
    let zero_wrapper = BlockWrapper {
        data: vec![0; 0],
        hash: BlockHash::new(b""),
        next_block: Some(0 as BlockCardinality),
    };
    let chunk_size = BlockSizeType::from(store.block_size()) as u64
        - bincode::serialized_size(&zero_wrapper).unwrap();

    // Determine the number of blocks we need.
    let block_count = bytes.len() as u64 / chunk_size
        + if bytes.len() as u64 % chunk_size > 0 {
            1
        } else {
            0
        };

    debug!(
        "serializing {} metadata bytes; chunk size is {} bytes; {} blocks needed.",
        bytes.len(),
        chunk_size,
        block_count
    );

    let mut block_array = VecDeque::new();

    // Add to our list with free blocks until we have enough blocks.
    while block_count > block_array.len() as u64 {
        let meta_block = match store.metadata_mut().free_blocks_mut().pop_front() {
            Some(b) => b,
            None => return Err(format_err!("no free blocks")),
        };
        debug!("allocating new blockmap wrapper block {}", meta_block);
        store
            .metadata_mut()
            .get_mut(meta_block)
            .unwrap()
            .tag_metadata();
        block_array.push_back(meta_block);
    }

    let start_block = block_array.front().unwrap();

    // Iterate over the chunks of serialized block map, and writing them to the block store.
    bytes
        .chunks(chunk_size as usize)
        .enumerate()
        .for_each(|(count, chunk)| {
            let block = block_array[count];
            let next_block = if count < block_count as usize - 1 {
                Some(block_array[count + 1])
            } else {
                None
            };

            let wrapper = BlockWrapper {
                data: chunk.to_vec(),
                hash: BlockHash::new(&chunk),
                next_block,
            };

            debug!(
                "writing blockmap wrapper number {}; next block {:?}",
                count, next_block
            );
            trace!("{:?}", wrapper);

            match store.write_block(block, bincode::serialize(&wrapper).unwrap()) {
                Ok(b) => {
                    debug!("wrote {} metadata bytes to block {}", b, block);
                }
                Err(e) => {
                    error!("error writing metadata bytes: {}", e);
                }
            }
        });

    Ok(*start_block)
}

pub(in crate::block) fn read_metadata<R, M>(
    store: &R,
    start_block: BlockNumber,
) -> Result<M, failure::Error>
where
    R: BlockReader,
    M: MetadataDeserialize,
{
    let mut bytes = Vec::<u8>::new();
    let mut block = read_wrapper_block(store, start_block)?;
    bytes.append(&mut block.data);

    while let Some(next) = block.next_block {
        block = read_wrapper_block(store, next)?;
        bytes.append(&mut block.data);
    }

    M::deserialize(bytes)
}

fn read_wrapper_block<BS: BlockReader>(
    store: &BS,
    number: BlockNumber,
) -> Result<BlockWrapper, failure::Error> {
    debug!("reading metadata block {}", number);
    let bytes = store.read_block(number)?;
    if let Ok(block) = bincode::deserialize::<BlockWrapper>(&bytes) {
        if block.hash.validate(&block.data) {
            return Ok(block);
        } else {
            error!("error validating block {}", number);
            return Err(format_err!("error validating block {}", number));
        }
    } else {
        error!("error deserializing block {}", number);
        return Err(format_err!("error deserializing block {}", number));
    }
}
