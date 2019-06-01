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
use std::{collections::HashMap, path::Path};

use ::time::Timespec;
use failure::format_err;
use lazy_static::lazy_static;
use log::{debug, error, trace, warn};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

mod block;
mod metadata;
mod runtime;
mod time;

pub mod fuse;

pub use block::{
    manager::BlockManager,
    map::BlockMap,
    storage::{file::FileStore, memory::MemoryStore, BlockReader, BlockStorage, BlockWriter},
    BlockAddress, BlockCardinality, BlockNumber, BlockSize,
};

use crate::metadata::{DirectoryEntry, File, FileVersion};

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

#[derive(Debug)]
pub enum OpenFileMode {
    Read,
    Write,
    ReadWrite,
}

/// Main File System Implementation
///
pub struct UberFileSystem<B: BlockStorage> {
    /// Where we store blocks.
    ///
    block_manager: BlockManager<B>,
    open_files: HashMap<u64, File>,
    open_file_counter: u64,
}

impl UberFileSystem<FileStore> {
    // /// Create a file system with a Memory-backed block storage
    // ///
    // /// This is useful for testing, and not much else -- unless an ephemeral file system is
    // /// warranted.
    // ///
    // pub fn new_memory(size: BlockSize, count: BlockCardinality) -> Self {
    //     let mem_store = MemoryStore::new(BlockMap::new(UfsUuid::new("test"), size, count));
    //     let block_manager = BlockManager::new(mem_store);

    //     UberFileSystem {
    //         block_manager,
    //         open_files: HashMap::new(),
    //         open_file_counter: 0,
    //     }
    // }

    /// Load an existing file-backed File System
    ///
    pub fn load_file_backed<P>(path: P) -> Result<Self, failure::Error>
    where
        P: AsRef<Path>,
    {
        let file_store = FileStore::load(path.as_ref())?;
        let block_manager = BlockManager::load(file_store)?;

        Ok(UberFileSystem {
            block_manager,
            open_files: HashMap::new(),
            open_file_counter: 0,
        })
    }

    /// List the contents of a Directory
    ///
    /// This function takes a Path and returns a Vec of (name, size) tuples -- one for each file
    /// contained within the specified directory.
    ///
    /// TODO: Verify that the path exists, and do something with it!
    pub fn list_files<P>(&self, path: P) -> Vec<(String, u64, Timespec)>
    where
        P: AsRef<Path>,
    {
        debug!("-------");
        debug!("`list_files`: {:?}", path.as_ref());
        self.block_manager
            .root_dir()
            .entries()
            .iter()
            .map(|(name, e)| match e {
                DirectoryEntry::Directory(d) => {
                    debug!("dir: {}", name);
                    (name.clone(), 0, d.write_time().into())
                }
                DirectoryEntry::File(f) => {
                    let size = f.get_current_version().size();
                    debug!("file: {}, bytes: {}", name, size);
                    (name.clone(), size, f.write_time().into())
                }
            })
            .collect()
    }

    /// Create a file
    ///
    pub fn create_file<P>(&mut self, path: P) -> Option<(u64, Timespec)>
    where
        P: AsRef<Path>,
    {
        if let Some(ostr_name) = path.as_ref().file_name() {
            if let Some(name) = ostr_name.to_str() {
                let file = self.block_manager.root_dir_mut().new_file(name);
                let time = file.version.write_time();

                let fh = self.open_file_counter;
                self.open_file_counter += self.open_file_counter.overflowing_add(1).0;
                self.open_files.insert(fh, file);

                return Some((fh, time.into()));
            }
        }

        None
    }

    /// Open a file
    ///
    pub fn open_file<P>(&mut self, path: P, mode: OpenFileMode) -> Option<u64>
    where
        P: AsRef<Path>,
    {
        debug!("-------");
        if let Some(mut file) = self.block_manager.root_dir().get_file(&path) {
            match mode {
                OpenFileMode::Write | OpenFileMode::ReadWrite => file.version = FileVersion::new(),
                _ => (),
            }
            let fh = self.open_file_counter;
            self.open_file_counter += self.open_file_counter.overflowing_add(1).0;
            self.open_files.insert(fh, file);
            debug!(
                "`open_file`: {:?}, handle: {}, mode: {:?}",
                path.as_ref(),
                fh,
                mode
            );
            Some(fh)
        } else {
            None
        }
    }

    /// Close a file
    ///
    pub fn close_file(&mut self, handle: u64) {
        debug!("-------");
        match self.open_files.remove(&handle) {
            Some(file) => {
                debug!("`close_file`: {:?}, handle: {}", file.path, handle);
                self.block_manager.root_dir_mut().update_file(file);
            }
            None => {
                warn!("asked to close a file not in the map {}", handle);
            }
        }
    }

    /// Write bytes to a file.
    ///
    pub fn write_file(&mut self, handle: u64, bytes: &[u8]) -> Result<usize, failure::Error> {
        debug!("-------");
        debug!("`write_file`: handle: {}", handle);

        match &mut self.open_files.get_mut(&handle) {
            Some(file) => {
                let mut written = 0;
                while written < bytes.len() {
                    match self.block_manager.write(&bytes[written..]) {
                        Ok(block) => {
                            written += block.size() as usize;
                            file.version.append_block(&block);
                        }
                        Err(e) => {
                            error!("problem writing data to file: {}", e);
                        }
                    }
                }
                debug!("wrote {} bytes", written,);

                Ok(written)
            }
            None => {
                warn!("asked to write file not in the map {}", handle);
                Ok(0)
            }
        }
    }

    /// Read bytes from a file
    ///
    ///
    pub fn read_file(
        &mut self,
        handle: u64,
        offset: i64,
        size: usize,
    ) -> Result<Vec<u8>, failure::Error> {
        debug!("-------");
        debug!(
            "`read_file`: handle: {}, reading offset {}, size {}",
            handle, offset, size
        );

        let file = &self.open_files.get(&handle).unwrap();
        let block_size = self.block_manager.block_size();

        let start_block = (offset / block_size as i64) as usize;
        let mut start_offset = (offset % block_size as i64) as usize;

        let mut blocks = file.version.blocks().clone();
        trace!("reading from blocks {:?}", &blocks);
        let block_iter = &mut blocks.iter_mut().skip(start_block);
        trace!("current iterator {:?}", block_iter);

        let mut read = 0;
        let mut buffer = vec![0; size];
        while read < size {
            if let Some(block_number) = block_iter.next() {
                if let Some(block) = self.block_manager.get_block(*block_number) {
                    trace!("reading block {:?}", &block);
                    if let Ok(bytes) = self.block_manager.read(block) {
                        trace!("read bytes\n{:?}", &bytes);
                        let block_len = bytes.len();
                        let width = std::cmp::min(size - read, block_len - start_offset);

                        trace!(
                            "copying to buffer[{}..{}] from bytes[{}..{}]",
                            read,
                            read + width,
                            start_offset,
                            start_offset + width
                        );
                        buffer[read..read + width]
                            .copy_from_slice(&bytes[start_offset..start_offset + width]);

                        read += width;
                        trace!("buffer is now {:?}", &buffer);
                    }
                }
                start_offset = 0;
            }
        }

        if buffer.len() == size {
            Ok(buffer)
        } else {
            Err(format_err!("Error reading file {:?}", file.path))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn open_file() {
        init();

        let mut ufs = UberFileSystem::load_file_backed("bundles/test").unwrap();
        let test_file = "/test_open_file";
        let (h0, _) = ufs.create_file(test_file).unwrap();
        let h1 = ufs.open_file(test_file).unwrap();
        assert!(
            h0 != h1,
            "two open calls to the same file should return different handles"
        );
    }

    #[test]
    fn read_and_write_file() {
        init();

        let mut ufs = UberFileSystem::load_file_backed("bundles/test").unwrap();
        let test = include_str!("lib.rs").as_bytes();

        let (h, _) = ufs.create_file("/lib.rs").unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test).unwrap());
        let bytes = ufs.read_file(h, 0, test.len()).unwrap();
        assert_eq!(test, bytes.as_slice());
    }

    #[test]
    fn small_chunks() {
        init();

        let chunk_size = 88;
        let mut ufs = UberFileSystem::load_file_backed("bundles/test").unwrap();
        let test = include_str!("lib.rs").as_bytes();

        let (h, _) = ufs.create_file("/lib.rs").unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test).unwrap());

        let mut offset = 0;
        test.chunks(chunk_size).for_each(|test_bytes| {
            let bytes = ufs.read_file(h, offset, test_bytes.len()).unwrap();
            let len = bytes.len();
            assert_eq!(
                std::str::from_utf8(test_bytes).unwrap(),
                String::from_utf8(bytes).unwrap(),
                "failed at offset {}",
                offset
            );
            offset += len as i64;
        });
    }

    #[test]
    fn large_chunks() {
        init();

        let chunk_size = 8888;
        let mut ufs = UberFileSystem::load_file_backed("bundles/test").unwrap();
        let test = include_str!("lib.rs").as_bytes();

        let (h, _) = ufs.create_file("/lib.rs").unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test).unwrap());

        let mut offset = 0;
        test.chunks(chunk_size).for_each(|test_bytes| {
            let bytes = ufs.read_file(h, offset, test_bytes.len()).unwrap();
            let len = bytes.len();
            assert_eq!(
                std::str::from_utf8(test_bytes).unwrap(),
                String::from_utf8(bytes).unwrap(),
                "failed at offset {}",
                offset
            );
            offset += len as i64;
        });
    }
}
