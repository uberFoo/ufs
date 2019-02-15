#![warn(missing_docs)]
//! Another file system: UberFS
//!
use std::path::Path;

use failure::Error;

mod block;
mod directory;
pub mod fuse;

pub(crate) use block::{BlockCardinality, BlockSize};

pub use block::{
    manager::BlockManager,
    storage::{file::FileStore, BlockStorage},
};

/// Main File System Implementation
///
pub struct UberFileSystem<BS: BlockStorage> {
    /// Where we store blocks.
    ///
    pub block_manager: BlockManager<BS>,
}

impl<'a> UberFileSystem<FileStore> {
    /// Create a new file-backed File System
    ///
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

    /// Load an existing file-backed File System
    ///
    pub fn load_file_backed<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let (file_store, metadata) = FileStore::load(path.as_ref())?;
        Ok(UberFileSystem {
            block_manager: BlockManager::load(file_store, metadata),
        })
    }

    /// Persist file-backed File System
    ///
    pub fn save_file_backed(&mut self) {
        self.block_manager.serialize();
    }
}
