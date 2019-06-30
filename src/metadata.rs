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
use std::path::PathBuf;

use serde_derive::{Deserialize, Serialize};

pub(crate) mod dir;
pub(crate) mod file;

pub(crate) type FileSize = u64;

/// The size of a FileHandle
pub type FileHandle = u64;

pub(crate) use dir::DirectoryMetadata;
pub(crate) use dir::{WASM_DIR, WASM_EXT};
pub(crate) use file::{FileMetadata, FileVersion};

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
    pub version: FileVersion, // pub version: FileMetadataInstance<'a>,
                              // pub file: FileMetadata
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
