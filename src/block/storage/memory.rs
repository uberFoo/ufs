//! Memory-based Block Storage
//!
//! This is a toy that is useful for testing.  The blocks are stored in a Vec.
//!
//! FIXME: Note that using serde, I could serialize this entire memory structure, and get a bundle
//! implementation, albeit one that would be memory constrained.

use failure::format_err;
use log::{debug, trace};

use crate::{
    block::{
        map::BlockMap, BlockCardinality, BlockReader, BlockSize, BlockSizeType, BlockStorage,
        BlockWriter,
    },
    uuid::UfsUuid,
};

/// An in-memory [BlockStorage]
///
/// This is a transient block storage implementation.  It's certainly useful for testing, and may
/// turn out to be so otherwise.  Perhaps as a fast cache, or something, in the future.  Especially
/// if we implement a means of converting between different block storage implementations, which is
/// something I think we'll want.  Especially given the scenario of mounting remote file systems.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryStore {
    id: UfsUuid,
    block_size: BlockSize,
    block_count: BlockCardinality,
    blocks: Vec<Vec<u8>>,
    map: BlockMap,
}

impl MemoryStore {
    /// Create a new MemoryStore
    ///
    /// Return a new in-memory [BlockStorage] given a [BlockSize] and the number of blocks.
    ///
    /// Note that block 0 is reserved to store block-level metadata.
    pub(crate) fn new(map: BlockMap) -> Self {
        MemoryStore {
            id: map.id().clone(),
            block_size: map.block_size(),
            block_count: map.block_count(),
            blocks: (0..map.block_count())
                .map(|_| Vec::with_capacity(map.block_size() as usize))
                .collect(),
            map,
        }
    }
}

impl BlockStorage for MemoryStore {
    fn id(&self) -> &UfsUuid {
        &self.id
    }

    fn commit_map(&mut self) {}

    fn map(&self) -> &BlockMap {
        &self.map
    }

    fn map_mut(&mut self) -> &mut BlockMap {
        &mut self.map
    }

    fn block_count(&self) -> BlockCardinality {
        self.block_count
    }

    fn block_size(&self) -> BlockSize {
        self.block_size
    }
}

impl BlockWriter for MemoryStore {
    fn write_block<T>(
        &mut self,
        bn: BlockCardinality,
        data: T,
    ) -> Result<BlockSizeType, failure::Error>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        if data.len() > self.block_size as usize {
            return Err(format_err!(
                "data ({}) is larger than block size",
                data.len()
            ));
        }

        if let Some(memory) = self.blocks.get_mut(bn as usize) {
            memory.extend_from_slice(data);

            debug!("wrote {} bytes to block {}", data.len(), bn);
            trace!("{:#?}", data);
            Ok(data.len() as BlockSizeType)
        } else {
            Err(format_err!("request for bogus block {}", bn))
        }
    }
}

impl BlockReader for MemoryStore {
    fn read_block(&self, bn: BlockCardinality) -> Result<Vec<u8>, failure::Error> {
        if let Some(memory) = self.blocks.get(bn as usize) {
            debug!("read {} bytes from block {}", memory.len(), bn);
            trace!("{:#?}", memory);
            Ok(memory.clone())
        } else {
            Err(format_err!("request for bogus block {}", bn))
        }
    }
}

#[cfg(test)]
mod test {
    use hex_literal::hex;

    use crate::UfsUuid;

    use super::*;

    #[test]
    fn bad_block_number() {
        let map = BlockMap::new(UfsUuid::new_root("test"), BlockSize::FiveTwelve, 3);
        let mut ms = MemoryStore::new(map);
        let data = [0x0; BlockSize::FiveTwelve as usize];

        assert!(
            ms.read_block(7).is_err(),
            "read should fail with block number out of range"
        );
        assert!(
            ms.write_block(7, &data[..]).is_err(),
            "write should fail with block number out of range"
        );
    }

    #[test]
    fn block_too_bukoo() {
        let map = BlockMap::new(UfsUuid::new_root("test"), BlockSize::FiveTwelve, 3);
        let mut ms = MemoryStore::new(map);
        let data = [0x0; BlockSize::FiveTwelve as usize + 1];
        assert_eq!(ms.write_block(1, &data[..]).is_err(), true);
    }

    #[test]
    fn write_block() {
        let map = BlockMap::new(UfsUuid::new_root("test"), BlockSize::FiveTwelve, 3);
        let mut ms = MemoryStore::new(map);
        let data = hex!(
            "451101250ec6f26652249d59dc974b7361d571a8101cdfd36aba3b5854d3ae086b5fdd4597721b66e3c0dc5
            d8c606d9657d0e323283a5217d1f53f2f284f57b85c8a61ac8924711f895c5ed90ef17745ed2d728abd22a5f
            7a13479a462d71b56c19a74a40b655c58edfe0a188ad2cf46cbf30524f65d423c837dd1ff2bf462ac4198007
            345bb44dbb7b1c861298cdf61982a833afc728fae1eda2f87aa2c9480858bec"
        );

        assert_eq!(
            ms.write_block(1, &data[..]).unwrap(),
            data.len() as BlockSizeType
        );

        assert_eq!(
            ms.blocks[1],
            &data[..],
            "API write to block, and compare directly"
        );
    }

    #[test]
    fn read_block() {
        let map = BlockMap::new(UfsUuid::new_root("test"), BlockSize::FiveTwelve, 3);
        let mut ms = MemoryStore::new(map);
        let data = hex!(
            "451101250ec6f26652249d59dc974b7361d571a8101cdfd36aba3b5854d3ae086b5fdd4597721b66e3c0dc5
            d8c606d9657d0e323283a5217d1f53f2f284f57b85c8a61ac8924711f895c5ed90ef17745ed2d728abd22a5f
            7a13479a462d71b56c19a74a40b655c58edfe0a188ad2cf46cbf30524f65d423c837dd1ff2bf462ac4198007
            345bb44dbb7b1c861298cdf61982a833afc728fae1eda2f87aa2c9480858bec"
        );
        let mut expected_block = vec![0x0; BlockSize::FiveTwelve as usize];
        expected_block[..data.len()].copy_from_slice(&data[..]);

        ms.blocks[0] = vec![0x0; BlockSize::FiveTwelve as usize];
        ms.blocks[0].copy_from_slice(&expected_block[..]);

        assert_eq!(
            ms.read_block(0).unwrap(),
            expected_block,
            "write directly to block, and compare via the API"
        );
    }

    #[test]
    fn construction_sanity() {
        let map = BlockMap::new(UfsUuid::new_root("test"), BlockSize::FiveTwelve, 4);
        let ms = MemoryStore::new(map);
        assert_eq!(
            ms.block_size() as usize,
            512,
            "verify block size as 512 bytes"
        );
        assert_eq!(
            ms.block_count(),
            4,
            "verify that there are four blocks total"
        );
    }
}
