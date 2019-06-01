//! Metadata
//!
//! Version one is a hashmap that fits in a single block, and lives at Block 0.
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use failure::format_err;
use log::{debug, error};
use serde_derive::{Deserialize, Serialize};

use crate::{
    block::{
        wrapper::{MetadataDeserialize, MetadataSerialize},
        Block, BlockNumber,
    },
    time::UfsTime,
};

pub(crate) type FileSize = u64;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct File {
    pub path: PathBuf,
    pub version: FileVersion,
    pub file: FileMetadata,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) enum DirectoryEntry {
    Directory(DirectoryMetadata),
    File(FileMetadata),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct DirectoryMetadata {
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
    entries: HashMap<String, DirectoryEntry>,
}

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

    pub(crate) fn new_file(&mut self, name: &str) -> File {
        let file = FileMetadata::new();
        self.entries
            .insert(name.to_owned(), DirectoryEntry::File(file.clone()));
        self.dirty = true;
        File {
            path: ["/", name].iter().collect(),
            version: file.get_current_version(),
            file: file.clone(),
        }
    }

    pub(crate) fn get_file<P>(&self, path: P) -> Option<File>
    where
        P: AsRef<Path>,
    {
        debug!("attempting to fetch file {:?}", path.as_ref());
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

    pub(crate) fn update_file(&mut self, file: File) {
        if let Some(file_name) = file.path.file_name() {
            if let Some(name) = file_name.to_str() {
                if let Some(ref mut entry) = self.entries.get_mut(name) {
                    match entry {
                        DirectoryEntry::File(ref mut my_file) => {
                            my_file.commit_version(file.version)
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    pub(crate) fn entries(&self) -> &HashMap<String, DirectoryEntry> {
        &self.entries
    }

    pub(crate) fn write_time(&self) -> UfsTime {
        self.write_time
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }
}

impl MetadataSerialize for DirectoryMetadata {
    fn serialize(&self) -> Result<Vec<u8>, failure::Error> {
        match bincode::serialize(&self) {
            Ok(r) => Ok(r),
            Err(e) => Err(format_err!("unable to serialize directory metadata {}", e)),
        }
    }
}

impl MetadataDeserialize for DirectoryMetadata {
    fn deserialize(bytes: Vec<u8>) -> Result<Self, failure::Error> {
        match bincode::deserialize(&bytes) {
            Ok(r) => {
                debug!("");
                debug!("*******");
                debug!("deserialize: {:#?}", r);
                Ok(r)
            }
            Err(e) => Err(format_err!(
                "unable to deserialize directory metadata {}",
                e
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct FileMetadata {
    versions: Vec<FileVersion>,
}

impl FileMetadata {
    pub(crate) fn new() -> Self {
        FileMetadata {
            versions: vec![FileVersion::new()],
        }
    }

    pub(crate) fn version(&self) -> usize {
        self.versions.len() - 1
    }

    pub(crate) fn get_current_version(&self) -> FileVersion {
        self.versions.last().unwrap().clone()
    }

    pub(crate) fn commit_version(&mut self, version: FileVersion) {
        if version.dirty {
            debug!("updating file version\n{:#?}", version);
            self.versions.push(version);
        }
    }

    pub(crate) fn write_time(&self) -> UfsTime {
        self.versions[0].write_time()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct FileVersion {
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

impl FileVersion {
    pub(crate) fn new() -> Self {
        let time = UfsTime::now();
        FileVersion {
            dirty: false,
            birth_time: time,
            write_time: time,
            change_time: time,
            access_time: time,
            size: 0,
            blocks: vec![],
        }
    }

    pub(crate) fn size(&self) -> FileSize {
        self.size
    }

    pub(crate) fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub(crate) fn blocks(&self) -> &Vec<BlockNumber> {
        &self.blocks
    }

    pub(crate) fn append_block(&mut self, block: &Block) {
        self.dirty = true;
        self.blocks.push(block.number());
        debug!("adding block {} to blocklist", block.number());
        self.size += block.size() as FileSize;
        debug!("new size {}", self.size);
    }

    pub(crate) fn write_time(&self) -> UfsTime {
        self.write_time
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn metadata_block() {
        // let id = UfsUuid::new("metadata");
        // let meta = Metadata::new(id, BlockSize::FiveTwelve, 10);

        // assert_eq!(
        //     Uuid::parse_str("e7f3f656-3bcb-50ff-8e46-e395e7fae538").unwrap(),
        //     *meta.id().as_ref()
        // );
        // assert_eq!(BlockSize::FiveTwelve, meta.block_size());
        // assert_eq!(10, meta.block_count());
    }
}
