//! File System Metadata
//!
//! Metadata is stored in blocks, which are managed by the [`BlockMap`]. The file system begins life
//! with a root directory, stored at block 0. As the file system mutates, changes are stored in
//! memory.  When unmounted the [`BlockManager`] writes the metadata to the `BlockMap` via a
//! [`BlockWrapper`], and the metadata is written to blocks in the `BlockMap`.
//!
//! Metadata is versioned. Each time a file is written, a new copy in created.
//!
//! [`BlockWrapper`]: crate::block::wrapper::BlockWrapper
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use failure::format_err;
use log::{debug, error, trace};
use serde_derive::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
use crate::{
    block::{
        wrapper::{MetadataDeserialize, MetadataSerialize},
        Block, BlockNumber,
    },
    time::UfsTime,
};

pub(crate) type FileSize = u64;

/// The size of a FileHandle
pub type FileHandle = u64;

/// UFS internal definition of a File
///
/// Here we associate a path with a particular file, and it's version.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct File {
    /// Path to file
    ///
    pub path: PathBuf,
    /// Version of the file
    ///
    pub version: FileVersion,
    /// The file wrapper, itself
    ///
    pub file: FileMetadata,
}

/// Entries in [`DirectoryMetadata`] structures
///
/// A directory may contain files, or other directories. Here we capture that dualism.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum DirectoryEntry {
    /// A directory
    ///
    Directory(DirectoryMetadata),
    /// A file
    ///
    File(FileMetadata),
}

/// Metadata for Directories
///
/// This struct stores all the various necessary time stamps, as well as a map of files (and
/// other directories) that it contains. These are stored as [`DirectoryEntry`] structures.
///
/// FIXME: The directory data is not versioned. What happens to deleted files?  What do we do when
/// a directory goes away?
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DirectoryMetadata {
    /// A flag indicating that the directory's data has been modified and needs to be written.
    ///
    #[serde(skip)]
    dirty: bool,
    /// Time directory was created (crtime)
    ///
    birth_time: UfsTime,
    /// Time directory was last written to (mtime)
    ///
    write_time: UfsTime,
    /// Time the directory was last changed (ctime)
    /// This includes ownership and permission changes
    ///
    change_time: UfsTime,
    /// Time the directory was last accessed (atime)
    ///
    access_time: UfsTime,
    /// HashMap of directory contents, from name to `DirectoryEntry`
    entries: HashMap<String, DirectoryEntry>,
}

#[cfg(not(target_arch = "wasm32"))]
impl DirectoryMetadata {
    pub(crate) fn new_root() -> Self {
        let time = UfsTime::now();
        DirectoryMetadata {
            dirty: true,
            birth_time: time,
            write_time: time,
            change_time: time,
            access_time: time,
            entries: HashMap::new(),
        }
    }

    /// Create a new file in this directory
    pub(crate) fn new_file(&mut self, name: &str) -> File {
        let file = FileMetadata::new();
        self.entries
            .insert(name.to_owned(), DirectoryEntry::File(file.clone()));
        self.dirty = true;
        debug!("`new_file`: {}", name);
        File {
            path: ["/", name].iter().collect(),
            version: file.get_current_version(),
            file: file.clone(),
        }
    }

    /// Retrieve a file by name from this directory
    pub(crate) fn get_file<P>(&self, path: P) -> Option<File>
    where
        P: AsRef<Path>,
    {
        debug!("-------");
        debug!("`get_file`: {:?}", path.as_ref());
        match path.as_ref().file_name() {
            Some(file_name) => match file_name.to_str() {
                Some(name) => match self.entries.get(name) {
                    Some(entry) => match entry {
                        DirectoryEntry::File(file) => Some(File {
                            path: path.as_ref().to_path_buf(),
                            version: file.get_current_version(),
                            file: file.clone(),
                        }),
                        _ => None,
                    },
                    _ => None,
                },
                _ => {
                    error!("invalid utf-8 in path {:?}", path.as_ref());
                    None
                }
            },
            _ => {
                error!("malformed path {:?}", path.as_ref());
                None
            }
        }
    }

    /// Update a file under this directory
    ///
    /// The current version is committed, and written if necessary.
    pub(crate) fn update_file(&mut self, file: File) {
        if file.version.dirty {
            if let Some(file_name) = file.path.file_name() {
                if let Some(name) = file_name.to_str() {
                    if let Some(ref mut entry) = self.entries.get_mut(name) {
                        match entry {
                            DirectoryEntry::File(ref mut my_file) => {
                                self.dirty = true;
                                my_file.commit_version(file.version)
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }
    }

    /// Return a HashMap from entry name to DirectoryEntry structures
    pub(crate) fn entries(&self) -> &HashMap<String, DirectoryEntry> {
        &self.entries
    }

    /// Return the `write_time` timestamp
    pub(crate) fn write_time(&self) -> UfsTime {
        self.write_time
    }

    /// Return true if the directory needs to be serialized
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MetadataSerialize for DirectoryMetadata {
    fn serialize(&mut self) -> Result<Vec<u8>, failure::Error> {
        match bincode::serialize(&self) {
            Ok(r) => {
                self.dirty = false;
                Ok(r)
            }
            Err(e) => Err(format_err!("unable to serialize directory metadata {}", e)),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MetadataDeserialize for DirectoryMetadata {
    fn deserialize(bytes: Vec<u8>) -> Result<Self, failure::Error> {
        match bincode::deserialize(&bytes) {
            Ok(r) => {
                debug!("-------");
                debug!("`deserialize`: {:#?}", r);
                Ok(r)
            }
            Err(e) => Err(format_err!(
                "unable to deserialize directory metadata {}",
                e
            )),
        }
    }
}

/// File storage
///
/// Files are just lists of blocks (data) with some metadata associated. In UFS, files are
/// versioned, and so to must the metadata of each file. Thus, the top-level file structure is a
/// list of [`FileVersion`]s.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FileMetadata {
    versions: Vec<FileVersion>,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileMetadata {
    /// Create a new `FileMetadata`
    ///
    /// When a new file is created, a default, empty, [`FileVersion`] is created. This is mostly so
    /// that we capture a time stamp of when the file was created.
    pub(crate) fn new() -> Self {
        FileMetadata {
            versions: vec![FileVersion::new()],
        }
    }

    /// Return the latest version number of the file.
    pub(crate) fn version(&self) -> usize {
        self.versions.len() - 1
    }

    /// Return a list of all of the versions of the file.
    pub(crate) fn versions(&self) -> &Vec<FileVersion> {
        &self.versions
    }

    /// Return the latest `FileVersion` of the file.
    pub(crate) fn get_current_version(&self) -> FileVersion {
        let version = self.versions.last().unwrap().clone();
        debug!("-------");
        debug!("`get_current_version`:");
        trace!("{:#?}", version);
        version
    }

    /// Commit a new version of the file.
    pub(crate) fn commit_version(&mut self, mut version: FileVersion) {
        if version.dirty {
            debug!("-------");
            debug!("`commit_version`: {:#?}", version);
            version.dirty = false;
            self.versions.push(version);
        }
    }

    /// Return the `write_time` timestamp of the latest version.
    pub(crate) fn write_time(&self) -> UfsTime {
        self.versions.last().unwrap().write_time()
    }

    /// Return the size of the latest version.
    pub(crate) fn size(&self) -> FileSize {
        self.versions.last().unwrap().size()
    }
}

/// The meat of a file
///
/// This is where metadata and block numbers are actually stored. These are cheap: they just have a
/// few time stamps, and a list of `BlockNumber`s that comprise the file.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FileVersion {
    /// A flag indicating that the directory's data has been modified and needs to be written.
    ///
    #[serde(skip)]
    dirty: bool,
    /// Time file was created (crtime)
    ///
    birth_time: UfsTime,
    /// Time file was last written to (mtime)
    ///
    write_time: UfsTime,
    /// Time the file was last changed (ctime)
    /// This includes ownership and permission changes
    ///
    change_time: UfsTime,
    /// Time the file was last accessed (atime)
    ///
    access_time: UfsTime,
    /// The size of the file in bytes.
    ///
    size: FileSize,
    /// The blocks that comprise the file
    ///
    blocks: Vec<BlockNumber>,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileVersion {
    /// Create a new `FileVersion`
    ///
    /// An empty file is just timestamps. The size of the file is 0, and it contains no blocks.
    pub(crate) fn new() -> Self {
        let time = UfsTime::now();
        FileVersion {
            dirty: true,
            birth_time: time,
            write_time: time,
            change_time: time,
            access_time: time,
            size: 0,
            blocks: vec![],
        }
    }

    /// Return the size of the file, in bytes
    pub(crate) fn size(&self) -> FileSize {
        self.size
    }

    /// Return the size of the file, in whole blocks
    pub(crate) fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Return a reference to the list of blocks that comprise the file
    pub(crate) fn blocks(&self) -> &Vec<BlockNumber> {
        &self.blocks
    }

    /// Append a block
    ///
    /// When a file is written to, it's done over time -- not all at once. Thus as blocks are
    /// filled, they are added, one at a time, to the list of blocks.
    pub(crate) fn append_block(&mut self, block: &Block) {
        self.dirty = true;
        self.blocks.push(block.number());
        debug!("adding block {} to blocklist", block.number());
        self.size += block.size() as FileSize;
        debug!("new size {}", self.size);
    }

    /// Return the `write_time` timestamp
    pub(crate) fn write_time(&self) -> UfsTime {
        self.write_time
    }
}
