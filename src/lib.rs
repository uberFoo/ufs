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
//! `@next_block`. Additionally, each block is identified by a `@type` key, where the value may be
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
//! ```ignore
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
use std::{cmp, collections::HashMap, io, path::Path};

use failure::Error;
use lazy_static::lazy_static;
use log::{debug, error, trace};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

mod block;
mod metadata;
mod runtime;

pub mod fuse;

pub use block::{
    manager::BlockManager,
    map::BlockMap,
    storage::{file::FileStore, BlockReader, BlockStorage, BlockWriter},
    BlockAddress, BlockCardinality, BlockNumber, BlockSize,
};

use metadata::{DirectoryEntry, FileMetadata, FileSize, FileVersion};

lazy_static! {
    /// The UUID to rule them all
    ///
    /// This is the main V5 uuid namespace from which all UUIDs in ufs are derived.
    static ref ROOT_UUID: Uuid = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"uberfoo.com");
}

/// uberFS unique ID
///
/// The ID is a version 5 UUID wit it's base namespace as "uberfoo.com".  New ID's are derived from
/// that root.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct UfsUuid {
    inner: Uuid,
}

impl UfsUuid {
    /// Create a new UfsUuid
    ///
    /// The UUID is generated based on the UFS UUID ROOT, and the supplied name.
    pub fn new<N>(name: N) -> Self
    where
        N: AsRef<[u8]>,
    {
        UfsUuid {
            inner: Uuid::new_v5(&ROOT_UUID, name.as_ref()),
        }
    }
}

impl AsRef<Uuid> for UfsUuid {
    fn as_ref(&self) -> &Uuid {
        &self.inner
    }
}

/// Main File System Implementation
///
pub struct UberFileSystem<B: BlockStorage> {
    /// Where we store blocks.
    ///
    block_manager: BlockManager<B>,
    open_files: HashMap<String, FileVersion>,
    dirty: bool,
}

impl UberFileSystem<FileStore> {
    /// Load an existing file-backed File System
    ///
    pub fn load_file_backed<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let file_store = FileStore::load(path.as_ref())?;
        let block_manager = BlockManager::load(file_store)?;

        Ok(UberFileSystem {
            block_manager,
            open_files: HashMap::new(),
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
        debug!("listing files for {:?}", path.as_ref());
        self.block_manager
            .root_dir()
            .entries()
            .iter()
            .map(|(name, e)| match e {
                DirectoryEntry::Directory(d) => {
                    debug!("dir: {}", name);
                    (name.clone(), 0)
                }
                DirectoryEntry::File(f) => {
                    let size = match f.versions.last() {
                        Some(v) => v.size(),
                        None => {
                            error!("Found a file ({}) with no version information.", name);
                            0
                        }
                    };
                    debug!("file: {}, bytes: {}", name, size);
                    (name.clone(), size)
                }
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
                self.block_manager.root_dir_mut().new_file(name);
                self.open_files.insert(name.to_string(), FileVersion::new());
            }
        }
    }

    // /// Open a file
    // ///
    // ///
    // pub fn open_file<P>(&mut self, path: P) where P: AsRef<Path>
    // {
    //     if let Some(ostr_name) = path.as_ref().file_name() {
    //         if let Some(name) = ostr_name.to_str() {
    //             if let Some(file) = self.block_manager.root_dir.entries.get(name) {
    //                     match file {
    //                         DirectoryEntry::Directory(_) => {
    //                             error!(
    //                                 "Attempt to open a directory: {:?}",
    //                                 path.as_ref()
    //                             );
    //                         }
    //                         DirectoryEntry::File(file) => {

    //                         }
    //             }
    //         }
    //     }
    // }

    /// Write bytes to a file
    ///
    ///
    // pub fn write_bytes_to_file<P>(&mut self, path: P, bytes: &[u8]) -> io::Result<usize>
    // where
    //     P: AsRef<Path>,
    // {
    //     match path.as_ref().file_name() {
    //         Some(file_name) => match file_name.to_str() {
    //             Some(name) => {
    //                 let mut written = 0;
    //                 let mut file_version = FileVersion {
    //                     size: bytes.len() as FileSize,
    //                     start_block: None,
    //                     block_count: 0,
    //                 };
    //                 if let Some(file) = self.block_manager.root_dir.entries.get(name) {
    //                     match file {
    //                         DirectoryEntry::Directory(_) => {
    //                             error!(
    //                                 "Attempt to write bytes to a directory: {:?}",
    //                                 path.as_ref()
    //                             );
    //                         }
    //                         DirectoryEntry::File(file) => {
    //                             while written < bytes.len() {
    //                                 match self.block_manager.write(&bytes[written..]) {
    //                                     Ok(block) => {
    //                                         if file_version.start_block() == None {
    //                                             file_version.start_block = block.number();
    //                                         }
    //                                         file_version.block_count += 1;
    //                                         written += block.size();
    //                                     }
    //                                     Err(e) => {
    //                                         error!("problem writing data {}", e);
    //                                     }
    //                                 }
    //                             }
    //                             debug!(
    //                                 "wrote {} bytes starting at block {} for {} blocks",
    //                                 written,
    //                                 file_version.start_block.unwrap(),
    //                                 file_version.block_count
    //                             );

    //                             // Ok(written)
    //                         }
    //                     }
    //                 } else {
    //                     error!("File lookup error: {:?}", path.as_ref());
    //                     // Ok(0)
    //                 }
    //                 if written > 0 {
    //                     if let Some(file) = self.block_manager.root_dir.entries.get_mut(name) {
    //                         match file {
    //                             DirectoryEntry::File(file) => {
    //                                 file.versions.push(file_version);
    //                                 self.dirty = true;
    //                                 Ok(written)
    //                             }
    //                             _ => unreachable!(),
    //                         }
    //                     } else {
    //                         error!("WTF");
    //                         return Ok(written);
    //                     }
    //                 } else {
    //                     return Ok(0);
    //                 }
    //             }
    //             None => {
    //                 error!("invalid utf-8 in path");
    //                 Ok(0)
    //             }
    //         },
    //         None => {
    //             error!("malformed path {:?}", path.as_ref());
    //             Ok(0)
    //         }
    //     }
    // }

    pub fn write_file<P>(&mut self, path: P) -> FileWriter
    where
        P: AsRef<Path>,
    {
        FileWriter { fs: self }
    }

    /// Read bytes from a file
    ///
    ///
    pub fn read_file<P>(&mut self, path: P) -> Option<FileReader>
    where
        P: AsRef<Path>,
    {
        if let Some(ostr_name) = path.as_ref().file_name() {
            if let Some(name) = ostr_name.to_str() {
                if let Some(file) = self.block_manager.root_dir().entries().get(name) {
                    match file {
                        DirectoryEntry::Directory(_) => {
                            error!("Can't read a directory {:?}", path.as_ref());
                            None
                        }
                        DirectoryEntry::File(file) => Some(FileReader::new(file)),
                    }
                } else {
                    error!("Asked to read a file I cannot find: {:?}", path.as_ref());
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
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

pub struct FileWriter<'a> {
    fs: &'a mut UberFileSystem<FileStore>,
}

impl<'a> io::Write for FileWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(0)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct FileReader {}

impl FileReader {
    pub(crate) fn new(file: &FileMetadata) -> Self {
        FileReader {}
    }
}

impl io::Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
}
