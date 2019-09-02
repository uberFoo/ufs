//! Block Manager
//!
//! High level access to block storage.  The block manager checks block hash consistency, handles
//! encryption, etc.  It also contains the `BlockMap` and handles directory and file metadata.

use c2_chacha::{
    stream_cipher::{NewStreamCipher, SyncStreamCipher, SyncStreamCipherSeek},
    XChaCha20,
};
use failure::format_err;
use hmac::Hmac;
use log::{debug, error};
use sha2::Sha256;

use crate::{
    block::{
        wrapper::{read_metadata, write_metadata},
        Block, BlockCardinality, BlockHash, BlockNumber, BlockSize, BlockStorage,
    },
    metadata::Metadata,
    uuid::UfsUuid,
};

/// Manager of Blocks
///
/// This sits atop a `BlockStorage` and provides higher-level operations over blocks.  For example,
/// reads and writes of arbitrary size (files) are aggregated across multiple blocks.  Per-block
/// hashes are calculated when writing, and validated when reading, a block.
///
/// The physical blocks are managed by a [`BlockMap`], which is owned by the `BlockStorage`
/// instance.
///
/// Files and Directories are managed by a `Metadata` structure, owned by this data structure.
///
/// [`BlockMap`]: crate::block::map::BlockMap
#[derive(Debug, PartialEq)]
pub struct BlockManager<BS>
where
    BS: BlockStorage,
{
    /// The UUID of the File System
    id: UfsUuid,
    /// The physical storage medium for the File System blocks
    store: BS,
    /// File and Directory metadata
    metadata: Metadata,
    /// Master file system key
    key: [u8; 32],
}

fn make_fs_key(password: &str, id: &UfsUuid) -> [u8; 32] {
    let mut key = [0; 32];
    pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_bytes(), id.as_bytes(), 88, &mut key);
    key
}

impl<'a, BS> BlockManager<BS>
where
    BS: BlockStorage,
{
    /// Layer metadata atop a block storage
    pub fn new<S: AsRef<str>>(password: S, store: BS) -> Self {
        BlockManager {
            id: store.id().clone(),
            metadata: Metadata::new(*store.id()),
            key: make_fs_key(password.as_ref(), &store.id()),
            store,
        }
    }

    /// Load an existing BlockManager, using metadata from an existing BlockStorage
    ///
    /// FIXME: This may be nice in a From<BlockMetadata>
    pub(crate) fn load<S: AsRef<str>>(password: S, mut store: BS) -> Result<Self, failure::Error> {
        match store.map().root_block() {
            Some(root_block) => {
                debug!("Reading root directory from block {}", root_block);
                match read_metadata(&mut store, root_block) {
                    Ok(metadata) => {
                        debug!("loaded metadata");

                        Ok(BlockManager {
                            id: store.id().clone(),
                            metadata,
                            key: make_fs_key(password.as_ref(), &store.id()),
                            store,
                        })
                    }
                    Err(e) => Err(format_err!("Problem loading file system metadata: {}", e)),
                }
            }
            None => Err(format_err!("Missing root_block!")),
        }
    }

    pub(crate) fn id(&self) -> &UfsUuid {
        &self.id
    }

    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub(crate) fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
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

    /// Recycle a Block
    ///
    /// The block is no longer being used, and may be returned to the free block pool.
    pub(crate) fn recycle_block(&mut self, bn: BlockNumber) {
        let block = self.store.map_mut().get_mut(bn).unwrap();
        block.tag_free();
        self.store.map_mut().free_blocks_mut().push_back(bn);
        debug!("Freed block 0x{:x?}", bn);
    }

    /// Save the state of the BlockManager
    ///
    /// This method stores the metadata in the [BlockStorage], starting at block 0.
    ///
    /// FIXME: If this fails, then what?
    pub(crate) fn serialize(&mut self) {
        if self.metadata.is_dirty() {
            match write_metadata(&mut self.store, &mut self.metadata) {
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
    pub(crate) fn write<T: AsRef<[u8]>>(
        &mut self,
        nonce: Vec<u8>,
        offset: u64,
        data: T,
    ) -> Result<&Block, failure::Error> {
        let data = data.as_ref();
        if let Some(number) = self.get_free_block() {
            let mut cipher = XChaCha20::new_var(&self.key, &nonce).unwrap();
            cipher.seek(offset);

            let end = data.len().min(self.store.block_size() as usize);
            let mut bytes = data[..end].to_vec();
            cipher.apply_keystream(&mut bytes);

            let byte_count = self.store.write_block(number, &bytes)?;
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
    pub(crate) fn read(
        &self,
        nonce: Vec<u8>,
        offset: u64,
        block: &Block,
    ) -> Result<Vec<u8>, failure::Error> {
        if let Block {
            number: block_number,
            hash: Some(block_hash),
            byte_count: _,
            block_type: _,
        } = block
        {
            let mut cipher = XChaCha20::new_var(&self.key, &nonce).unwrap();
            cipher.seek(offset);

            let mut bytes = self.store.read_block(*block_number)?;

            let hash = BlockHash::new(&bytes);
            if hash == *block_hash {
                debug!("read block 0x{:x?}", *block_number);
                cipher.apply_keystream(&mut bytes);
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
        block::{map::BlockMap, storage::BlockReader, BlockSize, MemoryStore},
        UfsUuid,
    };

    const NONCE: [u8; 24] = [
        0x23, 0x97, 0xb0, 0xa7, 0xa5, 0x06, 0xea, 0xa8, 0x72, 0x36, 0x53, 0xf9, 0xa7, 0xed, 0x90,
        0x02, 0xfd, 0xc6, 0xa5, 0xb9, 0x05, 0xd1, 0xab, 0x8b,
    ];

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn not_enough_free_blocks_error() {
        let mut bm = BlockManager::new(
            "foobar",
            MemoryStore::new(BlockMap::new(
                UfsUuid::new_root("test"),
                BlockSize::FiveTwelve,
                1,
            )),
        );

        let blocks = bm.write(NONCE.to_vec(), 0, &vec![0x0; 513][..]);
        assert_eq!(
            blocks.is_err(),
            true,
            "verify that more blocks are needed for write"
        );
    }

    #[test]
    fn tiny_test() {
        let mut bm = BlockManager::new(
            "foobar",
            MemoryStore::new(BlockMap::new(
                UfsUuid::new_root("test"),
                BlockSize::FiveTwelve,
                2,
            )),
        );

        let block = bm.write(NONCE.to_vec(), 0, b"abc").unwrap().clone();
        println!("{:#?}", block);

        assert_eq!(bm.free_block_count(), 0);
        let hash = block.hash.unwrap();
        assert_eq!(
            hash.as_ref(),
            hex!("fb070c9a1aae25f61f93d7b158962852565620d3c97ec0f8c1a69286fd617496"),
            "validate hash"
        );

        assert_eq!(
            bm.read(NONCE.to_vec(), 0, &block).unwrap(),
            b"abc",
            "compare stored data with expected values"
        );
    }

    #[test]
    fn write_data_smaller_than_blocksize() {
        let mut bm = BlockManager::new(
            "foobar",
            MemoryStore::new(BlockMap::new(
                UfsUuid::new_root("test"),
                BlockSize::FiveTwelve,
                2,
            )),
        );

        let block = bm
            .write(NONCE.to_vec(), 0, &vec![0x38; 511][..])
            .unwrap()
            .clone();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(
            bm.read(NONCE.to_vec(), 0, &block).unwrap(),
            &vec![0x38; 511][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", block);
    }

    #[test]
    fn write_data_larger_than_blocksize() {
        let mut bm = BlockManager::new(
            "foobar",
            MemoryStore::new(BlockMap::new(
                UfsUuid::new_root("test"),
                BlockSize::FiveTwelve,
                3,
            )),
        );

        let block = bm
            .write(NONCE.to_vec(), 0, &vec![0x38; 513][..])
            .unwrap()
            .clone();
        assert_eq!(bm.free_block_count(), 1);
        assert_eq!(
            bm.read(NONCE.to_vec(), 0, &block).unwrap(),
            &vec![0x38; 512][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", block);
    }

    #[test]
    fn read_block_bad_hash() {
        let mut bm = BlockManager::new(
            "foobar",
            MemoryStore::new(BlockMap::new(
                UfsUuid::new_root("test"),
                BlockSize::FiveTwelve,
                2,
            )),
        );

        let mut block = bm.write(NONCE.to_vec(), 0, b"abc").unwrap().clone();

        // Replace the hash of the block with something else.
        block.hash.replace(BlockHash::new("abcd"));

        assert!(
            bm.read(NONCE.to_vec(), 0, &block).is_err(),
            "hash validation failure"
        );
    }

    #[test]
    fn recycle_blocks() {
        let mut bm = BlockManager::new(
            "foobar",
            MemoryStore::new(BlockMap::new(
                UfsUuid::new_root("test"),
                BlockSize::FiveTwelve,
                10,
            )),
        );

        // One block is taken by the block map
        assert_eq!(bm.free_block_count(), 9);

        let block = bm
            .write(NONCE.to_vec(), 0, &vec![0x38; 512][..])
            .unwrap()
            .clone();
        assert_eq!(bm.free_block_count(), 8);
        let from_map = bm.store.map().get(block.number).unwrap();
        assert_eq!(from_map, &block);
        assert!(from_map.is_data());

        bm.recycle_block(block.number);
        assert_eq!(bm.free_block_count(), 9);
        assert!(bm.store.map().get(block.number).unwrap().is_free());
    }

    #[test]
    fn encrypt_and_decrypt_two_blocks_with_different_stream_positions() {
        let mut bm = BlockManager::new(
            "foobar",
            MemoryStore::new(BlockMap::new(
                UfsUuid::new_root("test"),
                BlockSize::FiveTwelve,
                10,
            )),
        );

        let block1 = bm
            .write(NONCE.to_vec(), 0, &vec![0x38; 512][..])
            .unwrap()
            .clone();
        let block2 = bm
            .write(NONCE.to_vec(), 512, &vec![0x38; 512][..])
            .unwrap()
            .clone();

        let c_data_1 = bm.store.read_block(block1.number).unwrap();
        let c_data_2 = bm.store.read_block(block2.number).unwrap();

        assert_ne!(c_data_1, c_data_2, "encrypted blocks should differ");
        assert_ne!(c_data_1[511], c_data_2[0], "no overlap");

        let data_1 = bm.read(NONCE.to_vec(), 0, &block1).unwrap();
        let data_2 = bm.read(NONCE.to_vec(), 512, &block2).unwrap();

        assert_eq!(data_1, data_2, "decrypted blocks should be identical");
    }
}
