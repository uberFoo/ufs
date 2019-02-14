#![warn(missing_docs)]
//! Another file system: UberFS
//!
use std::path::Path;

use failure::Error;

mod block;
pub mod fuse;

pub(crate) use block::{BlockCardinality, BlockSize};

pub use block::{
    manager::BlockManager,
    storage::{file::FileStore, BlockStorage},
};

pub struct UberFileSystem<BS: BlockStorage> {
    pub block_manager: BlockManager<BS>,
}

impl<'a> UberFileSystem<FileStore> {
    pub fn new_file_backed<P, B>(
        path: P,
        block_size: B,
        block_count: BlockCardinality,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        B: Into<BlockSize>,
    {
        let file_store = FileStore::new(path.as_ref(), block_size.into(), block_count)?;
        Ok(UberFileSystem {
            block_manager: BlockManager::new(file_store),
        })
    }

    pub fn load_file_backed<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let (file_store, metadata) = FileStore::load(path.as_ref())?;
        Ok(UberFileSystem {
            block_manager: BlockManager::load(file_store, metadata),
        })
    }

    pub fn save_file_backed(&mut self) {
        self.block_manager.serialize();
    }
}
