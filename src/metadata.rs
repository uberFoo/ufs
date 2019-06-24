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
    path::{Component, Components, Path, PathBuf},
};

use failure::format_err;
use log::{debug, error, trace, warn};
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
    /// The file wrapper, itself
    ///
    pub file: FileMetadata,
}

/// UFS internal definition of a directory
///
/// This struct associates a path with a directory.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Directory {
    /// Path to the directory
    ///
    pub path: PathBuf,
    /// The directory wrapper
    ///
    pub directory: DirectoryMetadata,
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
    pub(crate) fn new() -> Self {
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

    /// Create a new directory in this directory
    /// FIXME: This should return a Result maybe?
    pub(crate) fn new_directory<P>(&mut self, path: P) -> Option<Directory>
    where
        P: AsRef<Path>,
    {
        debug!("--------");
        debug!("`new_directory`: {:?}", path.as_ref());
        if let Some(dir_name) = path.as_ref().file_name() {
            if let Some(root) = path.as_ref().parent() {
                let mut iter = root.components();
                match iter.next() {
                    Some(Component::RootDir) => {
                        if let Some(dir_root) = self.new_dir_r(root, &mut iter) {
                            let dir = DirectoryMetadata::new();

                            dir_root.entries.insert(
                                dir_name.to_str().unwrap().to_owned(),
                                DirectoryEntry::Directory(dir.clone()),
                            );
                            dir_root.dirty = true;
                            debug!("\treturning: {:#?}", dir);
                            return Some(Directory {
                                path: path.as_ref().to_owned(),
                                directory: dir,
                            });
                        }
                    }
                    _ => {
                        error!("\tcalled with absolute path");
                        return None;
                    }
                }
            }
        }
        None
    }

    fn new_dir_r<'a>(
        &mut self,
        path: &Path,
        mut components: &mut Components<'a>,
    ) -> Option<&mut DirectoryMetadata> {
        debug!("--------");
        debug!(
            "`new_dir_r`: {:#?}, path: {:?}, components {:?}",
            self, path, components
        );
        match components.next() {
            Some(Component::Normal(name)) => match name.to_str() {
                Some(name) => match self.entries.get_mut(name) {
                    Some(DirectoryEntry::Directory(sub_dir)) => {
                        DirectoryMetadata::new_dir_r(sub_dir, path, &mut components)
                    }
                    _ => {
                        warn!("`get_dir_r`: couldn't find {:?}", path);
                        None
                    }
                },
                _ => {
                    error!("`get_dir_r`: invalid utf-8 in path {:?}", path);
                    None
                }
            },
            None => Some(self),
            _ => {
                error!("`get_dir_r`: wonky path: {:?}", path);
                None
            }
        }
    }

    /// Create a new file in this directory
    pub(crate) fn new_file(&mut self, name: &str) -> File {
        debug!("--------");
        debug!("`new_file`: {}", name);
        let file = FileMetadata::new();
        self.entries
            .insert(name.to_owned(), DirectoryEntry::File(file.clone()));
        self.dirty = true;
        File {
            path: ["/", name].iter().collect(),
            file: file.clone(),
        }
    }

    /// Recrieve a directory by name, from this directory
    pub(crate) fn get_directory<P>(&self, path: P) -> Option<Directory>
    where
        P: AsRef<Path>,
    {
        debug!("--------");
        debug!("`get_directory`: {:?}", path.as_ref());
        let mut iter = path.as_ref().components();
        match iter.next() {
            Some(Component::RootDir) => {
                let dir = self.get_dir_r(path.as_ref(), &mut iter);
                debug!("\treturning: {:#?}", dir);
                dir
            }
            _ => {
                error!("`get_dir` called with absolute path");
                None
            }
        }
    }

    fn get_dir_r<'a>(&self, path: &Path, mut components: &mut Components<'a>) -> Option<Directory> {
        debug!("--------");
        debug!(
            "`get_dir_r`: {:#?}, path: {:?}, components {:?}",
            self, path, components
        );
        match components.next() {
            Some(Component::Normal(name)) => match name.to_str() {
                Some(name) => match self.entries.get(name) {
                    Some(DirectoryEntry::Directory(sub_dir)) => {
                        DirectoryMetadata::get_dir_r(sub_dir, path, &mut components)
                    }
                    _ => {
                        warn!("`get_dir_r`: couldn't find {:?}", path);
                        None
                    }
                },
                _ => {
                    error!("`get_dir_r`: invalid utf-8 in path {:?}", path);
                    None
                }
            },
            None => Some(Directory {
                path: path.to_path_buf(),
                directory: self.clone(),
            }),
            _ => {
                error!("`get_dir_r`: wonky path: {:?}", path);
                None
            }
        }
    }

    /// Retrieve a file by name from this directory
    pub(crate) fn get_file_read_only<P>(&self, path: P) -> Option<File>
    where
        P: AsRef<Path>,
    {
        debug!("--------");
        debug!("`get_file_read_only`: {:?}", path.as_ref());
        match path.as_ref().file_name() {
            Some(file_name) => match file_name.to_str() {
                Some(name) => match self.entries.get(name) {
                    Some(entry) => match entry {
                        DirectoryEntry::File(file) => {
                            let mut file = file.clone();
                            let v = if file.version_count() > 0 {
                                file.versions[file.version_count() - 1].clone()
                            } else {
                                FileVersion::new()
                            };
                            file.current = Some(v);
                            Some(File {
                                path: path.as_ref().to_path_buf(),
                                file,
                            })
                        }
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

    /// Retrieve a file by name from this directory
    pub(crate) fn get_file_read_write<P>(&mut self, path: P) -> Option<File>
    where
        P: AsRef<Path>,
    {
        debug!("--------");
        debug!("`get_file_read_write`: {:?}", path.as_ref());
        // Mark the directory as dirty.
        self.dirty = true;

        match path.as_ref().file_name() {
            Some(file_name) => match file_name.to_str() {
                Some(name) => match self.entries.get(name) {
                    Some(entry) => match entry {
                        DirectoryEntry::File(file) => {
                            let mut file = file.clone();
                            let v = if file.version_count() > 0 {
                                file.versions[file.version_count() - 1].clone()
                            } else {
                                FileVersion::new()
                            };
                            file.current = Some(v);
                            Some(File {
                                path: path.as_ref().to_path_buf(),
                                file,
                            })
                        }
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

    /// Retrieve a file by name from this directory
    pub(crate) fn get_file_write_only<P>(&mut self, path: P) -> Option<File>
    where
        P: AsRef<Path>,
    {
        debug!("--------");
        debug!("`get_file_write_only`: {:?}", path.as_ref());
        // Mark the directory as dirty.
        self.dirty = true;

        match path.as_ref().file_name() {
            Some(file_name) => match file_name.to_str() {
                Some(name) => match self.entries.get(name) {
                    Some(entry) => match entry {
                        DirectoryEntry::File(file) => {
                            let mut file = file.clone();
                            file.current = Some(FileVersion::new());
                            Some(File {
                                path: path.as_ref().to_path_buf(),
                                file: file.clone(),
                            })
                        }
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
        debug!("--------");
        debug!("`update_file`: {:#?}", file);
        if file.file.current_version().unwrap().dirty {
            if let Some(file_name) = file.path.file_name() {
                if let Some(name) = file_name.to_str() {
                    if let Some(ref mut entry) = self.entries.get_mut(name) {
                        match entry {
                            DirectoryEntry::File(ref mut my_file) => {
                                self.dirty = true;
                                my_file
                                    .commit_version(file.file.current_version().unwrap().clone());
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
                debug!("--------");
                debug!("`serialize: {:#?}", self);
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
                debug!("--------");
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
    #[serde(skip)]
    current: Option<FileVersion>,
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
            current: Some(FileVersion::new()),
            versions: vec![],
        }
    }

    /// Return the number of versions of the file
    pub(crate) fn version_count(&self) -> usize {
        self.versions.len()
    }

    /// Return a list of all of the versions of the file
    pub(crate) fn get_versions(&self) -> &Vec<FileVersion> {
        &self.versions
    }

    /// Return the latest `FileVersion` of the file
    pub(crate) fn current_version(&self) -> Option<&FileVersion> {
        debug!("--------");
        debug!("`current_version`: {:#?}", self.current);
        self.current.as_ref()
    }

    /// Return the latest `FileVersion` of the file
    pub(crate) fn current_version_mut(&mut self) -> Option<&mut FileVersion> {
        debug!("--------");
        debug!("`current_version_mut`: {:#?}", self.current);
        self.current.as_mut()
    }

    /// Commit the current version of the file
    pub(crate) fn commit(&mut self) {
        debug!("--------");
        debug!("`commit`: {:#?}", self);
        match &self.current {
            Some(v) => {
                if v.dirty {
                    self.versions.push(v.clone());
                    self.current = None
                }
            }
            None => warn!("called commit with empty FileVersion"),
        }
    }

    /// Commit a new version of the file
    pub(crate) fn commit_version(&mut self, mut version: FileVersion) {
        debug!("--------");
        debug!("`commit_version`: {:#?}", version);
        if version.dirty {
            version.dirty = false;
            self.versions.push(version);
        }
    }

    /// Return the `write_time` timestamp of the latest version
    pub(crate) fn write_time(&self) -> UfsTime {
        match &self.current {
            Some(v) => v.write_time(),
            None => match self.versions.last() {
                Some(v) => v.write_time(),
                None => {
                    panic!("called write_time(), but no version exists");
                }
            },
        }
    }

    /// Return the size of the latest version
    pub(crate) fn size(&self) -> FileSize {
        match &self.current {
            Some(v) => v.size(),
            None => match self.versions.last() {
                Some(v) => v.size(),
                None => {
                    panic!("called size(), but no version exists");
                }
            },
        }
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
        debug!("{:#?}", self);
    }

    /// Return the `write_time` timestamp
    pub(crate) fn write_time(&self) -> UfsTime {
        self.write_time
    }
}
