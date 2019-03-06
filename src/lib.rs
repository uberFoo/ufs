#![warn(missing_docs)]
//! Another file system: UberFS
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
    block::{BlockCardinality, BlockSize, BlockType},
    directory::{Directory, DirectoryEntryReader, DirectoryEntryWriter, MutableDirectory},
};

pub use block::{
    manager::BlockManager,
    storage::{file::FileStore, BlockStorage},
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
