#![warn(missing_docs)]
//! A different sort of file system: *uberFS*
//!
//! # File System Structure
//!
//! Like every other file system, with some possible few exceptions, fixed-size blocks are the
//! foundation of *uberFS*. What's different across all file systems is how they are utilized.
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
//! ## Blocks
//!
//! The blocks are fixed-size, and uniform for a given file system.  Blocks contain nothing more
//! than the raw data that is written to them.  Put another way, there is no metadata _about_ the
//! block stored in the block _itself_.  Rather, metadata blocks contain information about blocks
//! that make up files and directories.
//!
//! ### Working with Blocks
//!
//! Blocks storage is abstracted by the [BlockStorage] trait.  There are implementations that
//! support storing blocks as files in on a host file system, in memory, and over the network.
//!
//! The BlockStorage trait has methods for reading (`read_block`) and, writing (`write_block`)
//! blocks to implemented media.
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
//! ## Addressing
//!
//! Each file system has a unique ID, as discussed elsewhere. This ID forms a namespace for block
//! numbers. Each block has a number, assigned by the file system starting at 0, and increasing
//! until the file system is full.  Note that this does not preclude growing the file system.
//!
//! ## Block Lists
//!
//! A block list is how data (files, directories, metadata, etc.) is stitched together from
//! disparate blocks. Below the discussion degrades to talking about files, but it is equally
//! applicable to other entities stored on the file system.
//!
//! As files change, the history of it's contents is preserved via a block list. Blocks may be
//! inserted into, and deleted from a block list, and a history is maintained. It is imagined that a
//! user may intentionally delete an entire file, or some portion of it's history, but that is not
//! the default use case. To this end blocks are for the most part write-once.
//!
//! Blocks are encrypted. Period. One of the main intentions of *uberFS* is privacy, and that's
//! simply not possible without encryption. Additionally, the data must be preserved, and secured
//! from bit-rot, intentional tampering, etc.
//!
//! Blocks are encrypted using an AEAD algorithm, and it's MAC is stored, along with the block
//! number in a block list.  Prior to encryption each block is hashed with SHA256, and it's hash
//! is also stored in the block list.  While this may seem a bit belt-and-suspenders, it allows
//! validation of files based on either encrypted or clear-text blocks, and ensures against
//! man-in-the-middle attacks on remote blocks.
//!
//! A block list is thus an array of tuples that include an *operation* (insert, or delete), a
//! *block address* (note that this allows for files comprised of blocks distributed across file
//! systems), a plain-text *hash*, and an encryption *MAC*.
//!
//! # API
//!
//! There are a number of services that need to be built on top of this library, and likely as not
//! quite a few that I have not yet conceived. Therefore the API needs flexibility and generality.
//!
//! The services that I have in mind include:
//!
//!  * A remote *Block* service with a RESTful API. The purpose of this service is to provide an
//! online block storage, with read/write capability. Encrypted blocks may be read, and written by
//! remote file systems.
//!
//!  * A remote *execution* service that, with appropriate authentication, will execute WASM code
//! against files, returning possibly transformed information based on the underlying data. This is
//! distinct from the block service, but may be integrated into the same.
//!
//! * A FUSE-based file system adaptor to mount an *uberFS* as a native file system.
//!
//! * A web-based view for *uberFS*.
//!
//! ## Block Server
//!
//! The block service's main contribution is distributing blocks. It may or may not decrypt blocks,
//! depending on whether proper authentication is provided. It is however intended to utilize TLS
//! in order to preserve encryption in the event that it is returning decrypted blocks.

//! ### Required End-Points
//!
//! * `read_block(number)`
//! * `write_block(number)`
//!
use std::{cell::RefCell, path::Path, rc::Rc};

use failure::Error;
use lazy_static::lazy_static;
use log::trace;
use uuid::Uuid;

mod block;
mod directory;
pub mod fuse;
pub mod io;
// mod vm;

pub(crate) use {
    block::BlockType,
    directory::{Directory, DirectoryEntryReader, DirectoryEntryWriter, MutableDirectory},
};

pub use block::{
    manager::BlockManager,
    storage::{file::FileStore, BlockStorage},
    BlockAddress, BlockCardinality, BlockNumber, BlockSize,
};

lazy_static! {
    /// The UUID to rule them all
    ///
    /// This is the main V5 uuid namespace from which all UUIDs in ufs are derived.
    static ref ROOT_UUID: Uuid = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"uberfoo.com");
}

use crate::io::{BlockTreeReader, BlockTreeWriter};

/// uberFS unique ID
///
pub type UfsUuid = Uuid;

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
        let (file_store, metadata) = FileStore::load_and_return_metadata(path.as_ref())?;
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
