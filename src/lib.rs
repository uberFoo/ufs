#![warn(missing_docs)]
//! Another file system: UberFS
//!
use std::{cell::RefCell, path::Path, rc::Rc};

use failure::Error;

mod block;
mod directory;
pub mod fuse;
pub mod io;

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

        let root_dir = if let Ok(block_ptr_bytes) = block_manager.read_metadata("@root_dir_ptr") {
            if let Ok(BlockType::Pointer(block)) = BlockType::deserialize(block_ptr_bytes) {
                if let Ok(tree_bytes) = block_manager.read(&block) {
                    if let Ok(root_dir) = Directory::deserialize(tree_bytes) {
                        root_dir
                    } else {
                        panic!("Serious problem reading file system root directory");
                    }
                } else {
                    panic!("Serious problem reading block tree");
                }
            } else {
                panic!("Serious problem reading block");
            }
        } else {
            panic!("Serious problem reading file system root directory location");
        };

        // let block_manager = Rc::new(RefCell::new(block_manager));

        Ok(UberFileSystem {
            block_manager: Rc::new(RefCell::new(block_manager)),
            // root_dir: MutableDirectory::new(Rc::clone(&block_manager), &mut root_dir),
            root_dir: Rc::new(RefCell::new(root_dir)),
            dirty: false,
        })
    }

    /// Return the contents of a Directory
    ///
    ///
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
    pub fn entry_writer<P>(&mut self, path: P) -> DirectoryEntryWriter<FileStore>
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
    pub fn entry_reader<P>(&mut self, path: P) -> DirectoryEntryReader<FileStore>
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
