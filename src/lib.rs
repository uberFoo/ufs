#![warn(missing_docs)]
//! A different sort of file system: UberFS
//!
//! # FIXME
//! I need to touch on the following topics:
//!  * Distributed block storage
//!  * Block level encryption
//!  * Merkle Tree usage
//!  * WASM support
//!
//! # File System Structure
//!
//! Like every other file system, with some possible few exceptions, fixed-size blocks are the
//! foundation of UberFS. What's different across all file systems is how they are utilized.
//! Particularly, in how the file system metadata is represented.  We'll touch on blocks, and other
//! miscellany, but the primary focus of this section will be metadata representation.
//!
//! ## Metadata
//!
//! All metadata is stored in a dictionary. The dictionary is serialized with to a `Vec<u8>` with
//! `Serde` and `Bincode`, and written to blocks. If the serialized dictionary does not fit within a
//! single block, it will contain a pointer to the next block.  The pointer is stored under the key
//! `@next_block`. Additionally, eac block is identified by a `@type` key, where the value may be
//! something like `directory`, `fs-metadata`, etc.  Additional metadata for each specific block
//! type may exist under the `@metadata` key.  Data specific to the block type lives under the
//! `@data` key. Finally, each dictionary contains a `@checksum` key that contains the checksum
//! for the entire dictionary.
//!
//! ### Block Trees
//!
//!
//! ## Blocks
//!
//! The blocks are fixed-size, and uniform for a 044iven file system.  Blocks contain nothing more
//! than the raw data that is written to them.  Put another way, there is no metadata _about_ the
//! block stored in the block _itself_.  Rather, metadata blocks contain information about blocks
//! that make up files and directories.
//!
//! ### Block 0
//!
//! The first block is the file system is special.  It contains information about the file system
//! itself, such as the number of blocks, the block size, a free block list, etc.
//!
//! The following is currently how block 0 is organized; it's serialized to a `Vec<u8>` using
//! `Serde` and `Bincode`:
//!
//! ```
//! pub(crate) struct BlockMetadata {
//!    pub size: BlockSize,
//!    pub count: BlockCardinality,
//!    pub next_free_block: Option<BlockCardinality>,
//!    pub directory: HashMap<String, Block>,
//!}
//! ```
//!
//! Note that the above flies in the face of what was described above -- this is clearly not a
//! dictionary.  Instead, it's legacy code that needs to be updated.
//!
use std::{cell::RefCell, path::Path, rc::Rc};

use failure::Error;
use log::trace;

mod block;
mod directory;
pub mod fuse;
pub mod io;
// mod vm;

pub(crate) use {
    block::{BlockSize, BlockType},
    directory::{Directory, DirectoryEntryReader, DirectoryEntryWriter, MutableDirectory},
};

pub use block::{
    load_file_store,
    manager::BlockManager,
    storage::{file::FileStore, BlockStorage},
    BlockCardinality,
};

use crate::io::{BlockTreeReader, BlockTreeWriter};

/// Main File System Implementation
///
pub struct UberFileSystem<B: BlockStorage> {
    /// Where we store blocks.
    ///
    block_manager: Rc<RefCell<BlockManager<B>>>,
    ///
    /// Handle to the root directory.
    ///
    root_dir: Rc<RefCell<Directory>>,
    // root_dir: MutableDirectory<'a, B>,
    dirty: bool,
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
        let mut block_manager = BlockManager::new(file_store);

        // Setup the root directory.
        let root_dir = Directory::new();
        let root_dir_block_ptr =
            BlockType::Pointer(block_manager.write(root_dir.serialize().unwrap()).unwrap());

        println!("root_dir block {:?}", root_dir_block_ptr);

        block_manager
            .write_metadata("@root_dir_ptr", root_dir_block_ptr.serialize().unwrap())
            .unwrap();

        // let block_manager = Rc::new(RefCell::new(block_manager));

        Ok(UberFileSystem {
            block_manager: Rc::new(RefCell::new(block_manager)),
            // root_dir: MutableDirectory::new(Rc::clone(&block_manager), &mut root_dir),
            root_dir: Rc::new(RefCell::new(root_dir)),
            dirty: false,
        })
    }

    /// Load an existing file-backed File System
    ///
    pub fn load_file_backed<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let (file_store, metadata) = FileStore::load(path.as_ref())?;
        let block_manager = BlockManager::load(file_store, metadata);

        let root_dir = match block_manager.read_metadata("@root_dir_ptr") {
            Ok(block_ptr_bytes) => {
                trace!("read block_ptr_bytes {:?}", block_ptr_bytes);
                match BlockType::deserialize(block_ptr_bytes) {
                    Ok(BlockType::Pointer(block)) => {
                        trace!("deserialized block_ptr_bytes to {:#?}", block);
                        match block_manager.read(&block) {
                            Ok(tree_bytes) => {
                                trace!("read block_ptr block bytes {:?}", tree_bytes);
                                match Directory::deserialize(tree_bytes) {
                                    Ok(root_dir) => {
                                        trace!("deserialized root_dir {:#?}", root_dir);
                                        root_dir
                                    }
                                    Err(e) => {
                                        panic!("problem deserializing root directory {:?}", e);
                                    }
                                }
                            }

                            Err(e) => panic!("problem reading root_dir blocks {:?}", e),
                        }
                    }
                    Err(e) => panic!("problem deserializing root direcory block_ptr {:?}", e),
                }
            }
            Err(e) => panic!("problem reading metadata for `@root_dir_ptr` {:?}", e),
        };

        Ok(UberFileSystem {
            block_manager: Rc::new(RefCell::new(block_manager)),
            // root_dir: MutableDirectory::new(Rc::clone(&block_manager), &mut root_dir),
            root_dir: Rc::new(RefCell::new(root_dir)),
            dirty: false,
        })
    }

    /// List the contents of a Directory
    ///
    /// This function takes a Path and returns a Vec of (name, size) tuples -- one for each file
    /// contained within the specified directory.
    ///
    /// TODO: Verify that the path exists, and do something with it!
    pub fn list_files<P>(&self, path: P) -> Vec<(String, u64)>
    where
        P: AsRef<Path>,
    {
        self.root_dir
            .borrow()
            .iter()
            .map(|(k, v)| {
                println!("{}: {:?}", k, v);
                let size = if let Some(bt) = v { bt.size() } else { 0 };
                (k.clone(), size)
            })
            .collect()
    }

    /// Create a file
    ///
    ///
    pub fn create_file<P>(&mut self, path: P)
    where
        P: AsRef<Path>,
    {
        if let Some(ostr_name) = path.as_ref().file_name() {
            if let Some(name) = ostr_name.to_str() {
                let mut mrd = MutableDirectory::new(
                    Rc::clone(&self.root_dir),
                    Rc::clone(&self.block_manager),
                );
                mrd.create_entry(name);
            }
        }
    }

    /// Write bytes to a file
    ///
    ///
    pub fn file_writer<P>(&mut self, path: P) -> DirectoryEntryWriter<FileStore>
    where
        P: AsRef<Path>,
    {
        let mrd = MutableDirectory::new(Rc::clone(&self.root_dir), Rc::clone(&self.block_manager));
        let btw = BlockTreeWriter::new(Rc::clone(&self.block_manager));
        DirectoryEntryWriter::new(path, Rc::new(RefCell::new(mrd)), Rc::new(RefCell::new(btw)))
    }

    /// Read bytes from a file
    ///
    ///
    pub fn file_reader<P>(&mut self, path: P) -> DirectoryEntryReader<FileStore>
    where
        P: AsRef<Path>,
    {
        let mut mrd =
            MutableDirectory::new(Rc::clone(&self.root_dir), Rc::clone(&self.block_manager));
        let file = path.as_ref().file_name().unwrap().to_str().unwrap();
        let tree = mrd.read_entry(&file).unwrap();
        let btw = BlockTreeReader::new(Box::new(tree), Rc::clone(&self.block_manager));
        DirectoryEntryReader::new(path, Rc::new(RefCell::new(mrd)), Rc::new(RefCell::new(btw)))
    }
}

// impl<B> Drop for UberFileSystem<B>
// where
//     B: BlockStorage,
// {
//     fn drop(&mut self) {
//         if self.dirty {
//             let ptr = BlockType::Pointer(
//                 self.block_manager
//                     .write(self.root_dir.serialize().unwrap())
//                     .unwrap(),
//             );
//             self.block_manager
//                 .write_metadata("@root_dir_ptr", ptr.serialize().unwrap())
//                 .unwrap();
//         }
//     }
// }
