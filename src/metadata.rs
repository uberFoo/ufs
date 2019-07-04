//! File System Metadata
//!
//! Note that metadata contains id's that point at other metadata. Using id's instead of references
//! allows for easy cloning.
//!
//! Metadata is stored in blocks, which are managed by the [`BlockMap`]. The file system begins life
//! with a root directory, stored at block 0. As the file system mutates, changes are stored in
//! memory.  When unmounted the [`BlockManager`] writes the metadata to the `BlockMap` via a
//! [`BlockWrapper`], and the metadata is written to blocks in the `BlockMap`.
//!
//! Metadata is versioned. Each time a file is written, a new copy in created.
//!
//! [`BlockWrapper`]: crate::block::wrapper::BlockWrapper
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use failure::format_err;
use log::{debug, trace};
use serde_derive::{Deserialize, Serialize};

pub(crate) mod dir;
pub(crate) mod file;

use crate::uuid::UfsUuid;

pub(crate) type FileSize = u64;

/// The size of a FileHandle
pub type FileHandle = u64;

pub(crate) use dir::DirectoryMetadata;
pub(crate) use dir::{WASM_DIR, WASM_EXT};
pub(crate) use file::{FileMetadata, FileVersion};

use crate::block::wrapper::{MetadataDeserialize, MetadataSerialize};

/// UFS internal definition of a File
///
/// Here we associate a path with a particular file, and it's version. This gets indexed by a "file
/// handle", which is returned to the FUSE implementation.
/// We need to store the path because sometimes FUSE hands us paths, and not file handles.
/// FIXME: I don't know that this should be public.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, PartialEq)]
pub struct File {
    /// Path to file
    ///
    // pub path: PathBuf,
    /// UfsUuid of the file
    ///
    pub file_id: UfsUuid,
    /// The file wrapper, itself
    ///
    pub version: FileVersion,
}

/// UFS internal definition of a directory
///
/// This struct associates a path with a directory. This gets indexed by a "file handle", which is
/// returned to the FUSE implementation.
/// We need to store the path because sometimes FUSE hands us paths, and not file handles.
/// FIXME: I don't know that this should be public.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, PartialEq)]
pub struct Directory {
    /// Path to the directory
    ///
    // pub path: PathBuf,
    /// UfsUuid of the directory
    ///
    pub id: UfsUuid,
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

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Metadata {
    /// The dirty flag
    ///
    /// Indicates that there is data to be serialized.
    #[serde(skip)]
    dirty: bool,
    /// The UUID of the File System
    ///
    id: UfsUuid,
    /// The Root Directory
    ///
    root_directory: DirectoryMetadata,
}

impl Metadata {
    /// Create a new file system metadata instance
    ///
    /// The UUID of the file system is saved with the metadata.
    /// A new root directory is initialized.
    pub(crate) fn new(file_system_id: UfsUuid) -> Self {
        Metadata {
            dirty: true,
            id: file_system_id.clone(),
            root_directory: DirectoryMetadata::new(file_system_id.new("/"), None),
        }
    }

    /// Create a new directory
    ///
    pub(crate) fn new_directory(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<DirectoryMetadata, failure::Error> {
        debug!("--------");
        debug!("`new_directory`: {}", name);

        if let Some(root) = self.lookup_dir_mut(dir_id) {
            let new_dir = root.new_subdirectory(name.to_owned())?;
            self.dirty = true;
            debug!("\tcreated directory with id {:?}", dir_id);
            Ok(new_dir)
        } else {
            Err(format_err!("unable to find directory with id {:?}", dir_id))
        }
    }

    /// Retrieve a directory
    ///
    pub(crate) fn get_directory(
        &self,
        dir_id: UfsUuid,
    ) -> Result<DirectoryMetadata, failure::Error> {
        debug!("--------");
        debug!("`get_directory`: {:?}", dir_id);
        if let Some(dir) = self.lookup_dir(dir_id) {
            let mut dir = dir.clone();

            // Populate the special "versions" directory.
            if dir.is_vers_dir() {
                let mut files = HashMap::<String, DirectoryEntry>::new();
                if let Some(parent_dir_id) = dir.parent_id() {
                    if let Some(parent_dir) = self.lookup_dir(parent_dir_id) {
                        for (name, entry) in parent_dir.entries() {
                            if let DirectoryEntry::File(file) = entry {
                                for (index, version) in file.get_versions().iter() {
                                    let mut name = name.clone();
                                    name.push('@');
                                    name.push_str(&index.to_string());
                                    trace!("\tfound version {}", name);
                                    files.insert(
                                        name,
                                        DirectoryEntry::File(version.as_file_metadata()),
                                    );
                                }
                            }
                        }

                        dir.set_entries(files);
                    }
                }
            }

            trace!("\treturning {:#?}", dir);
            Ok(dir)
        } else {
            Err(format_err!("unable to find directory with id {:?}", dir_id))
        }
    }

    /// Create a new file
    ///
    pub(crate) fn new_file(&mut self, dir_id: UfsUuid, name: &str) -> Result<File, failure::Error> {
        debug!("--------");
        debug!("`new_file`: {}", name);

        if let Some(root) = self.lookup_dir_mut(dir_id) {
            let new_file = root.new_file(name.to_owned())?;
            self.dirty = true;
            Ok(File {
                file_id: new_file.id(),
                version: new_file.get_latest(),
            })
        } else {
            Err(format_err!("unable to find directory with id {:?}", dir_id))
        }
    }

    /// Get FileMetadata
    ///
    pub(crate) fn get_file_metadata(&self, id: UfsUuid) -> Result<FileMetadata, failure::Error> {
        if let Some(file) = self.lookup_file(id) {
            Ok(file.clone())
        } else {
            Err(format_err!("unable to find file with id {:?}", id))
        }
    }

    /// Get a file for read-only access
    ///
    pub(crate) fn get_file_read_only(&self, id: UfsUuid) -> Result<File, failure::Error> {
        debug!("--------");
        debug!("`get_file_read_only: {:?}", id);

        if let Some(file) = self.lookup_file(id) {
            Ok(File {
                file_id: file.id(),
                version: file.get_latest(),
            })
        } else {
            Err(format_err!("unable to find file with id {:?}", id))
        }
    }

    /// Get a file for read-write access
    ///
    pub(crate) fn get_file_read_write(&mut self, id: UfsUuid) -> Result<File, failure::Error> {
        debug!("--------");
        debug!("`get_file_read_write: {:?}", id);

        if let Some(file) = self.lookup_file_mut(id) {
            Ok(File {
                file_id: file.id(),
                version: file.get_latest(),
            })
        } else {
            Err(format_err!("unable to find file with id {:?}", id))
        }
    }

    /// Get a file for write-only access
    ///
    pub(crate) fn get_file_write_only(&mut self, id: UfsUuid) -> Result<File, failure::Error> {
        debug!("--------");
        debug!("`get_file_write_only: {:?}", id);

        if let Some(file) = self.lookup_file_mut(id) {
            Ok(File {
                file_id: file.id(),
                version: file.new_version(),
            })
        } else {
            Err(format_err!("unable to find file with id {:?}", id))
        }
    }

    /// Commit changes to an open file
    ///
    pub(crate) fn commit_file(&mut self, f: File) -> Result<(), failure::Error> {
        debug!("--------");
        debug!("`commit_file`: {:?}", f);

        if f.version.is_dirty() {
            if let Some(file) = self.lookup_file_mut(f.file_id) {
                file.commit_version(f.version.clone())?;
                Ok(())
            } else {
                Err(format_err!("unable to find file {:#?}", f))
            }
        } else {
            Ok(())
        }
    }

    /// Remove a file from a directory
    ///
    pub(crate) fn unlink_file(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<(), failure::Error> {
        debug!("--------");
        debug!("`unlink_file`: {}, dir: {:#?}", name, dir_id);

        if let Some(dir) = self.lookup_dir_mut(dir_id) {
            // If this is a file in the special versions directory, then we are removing a version
            // from the parent.
            if dir.is_vers_dir() {
                debug!("\tremoving version");
                Ok(())
            } else {
                match dir.entries_mut().remove(name) {
                    Some(file) => {
                        debug!("\tremoved {:#?}\nfrom {:#?}", file, dir);
                        self.dirty = true;
                        Ok(())
                    }
                    None => Err(format_err!("did not find {} in {:#?}", name, dir)),
                }
            }
        } else {
            Err(format_err!("unable to find directory {:#?}", dir_id))
        }
    }

    /// Return a reference to the root directory.
    ///
    pub(crate) fn root_directory(&self) -> &DirectoryMetadata {
        &self.root_directory
    }

    /// Return a mutable reference to the root directory.
    ///
    pub(crate) fn root_directory_mut(&mut self) -> &mut DirectoryMetadata {
        &mut self.root_directory
    }

    /// Indicator that the metedata needs to be written.
    ///
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Return the DirectoryMetadata corresponding to the given UfsUuid.
    /// FIXME: Maintain a cache.
    pub(crate) fn lookup_dir(&self, id: UfsUuid) -> Option<&DirectoryMetadata> {
        debug!("--------");
        debug!("`lookup_dir`: {:?}", id);
        trace!("{:#?}", self);

        if self.root_directory.id() == id {
            Some(&self.root_directory)
        } else {
            self.root_directory.lookup_dir(id)
        }
    }

    pub(crate) fn lookup_dir_mut(&mut self, id: UfsUuid) -> Option<&mut DirectoryMetadata> {
        debug!("--------");
        debug!("`lookup_dir_mut`: {:?}", id);
        trace!("{:#?}", self);

        self.root_directory.lookup_dir_mut(id)
    }

    pub(crate) fn lookup_file(&self, id: UfsUuid) -> Option<&FileMetadata> {
        debug!("--------");
        debug!("`lookup_file`: {:?}", id);
        trace!("{:#?}", self);

        self.root_directory.lookup_file(id)
    }

    pub(crate) fn lookup_file_mut(&mut self, id: UfsUuid) -> Option<&mut FileMetadata> {
        debug!("--------");
        debug!("`lookup_file_mut`: {:?}", id);
        trace!("{:#?}", self);

        self.root_directory.lookup_file_mut(id)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MetadataSerialize for Metadata {
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
impl MetadataDeserialize for Metadata {
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

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn new_metadata() {
        let mut m = Metadata::new(UfsUuid::new_root("test"));
        let root = m.root_directory();

        assert_eq!(m.is_dirty(), true);
        assert_eq!(root.is_dirty(), false);
        assert_eq!(root.parent_id(), None);
    }

    #[test]
    fn new_directory() {
        let mut m = Metadata::new(UfsUuid::new_root("test"));
        let root_id = m.root_directory().id();
        let d = m.new_directory(root_id, "test").unwrap();
        let d2 = m.new_directory(d.id(), "test2").unwrap();

        assert_eq!(d.parent_id(), Some(root_id));
        assert_eq!(d2.parent_id(), Some(d.id()));
    }
}
