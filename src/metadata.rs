//! Metadata
//!
//! Version one is a hashmap that fits in a single block, and lives at Block 0.
use std::collections::HashMap;

use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};

use crate::{
    block::{BlockCardinality, BlockNumber, BlockSize},
    UfsUuid,
};

pub(crate) type FileSize = u64;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) enum DirectoryEntry {
    Directory(DirectoryMetadata),
    File(FileMetadata),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct DirectoryMetadata {
    #[serde(skip)]
    dirty: bool,
    entries: HashMap<String, DirectoryEntry>,
}

impl DirectoryMetadata {
    pub(crate) fn new_root() -> Self {
        DirectoryMetadata {
            dirty: true,
            entries: HashMap::new(),
        }
    }

    pub(crate) fn new_file(&mut self, name: &str) {
        let file = FileMetadata::new();
        self.entries
            .insert(name.to_owned(), DirectoryEntry::File(file));
        self.dirty = true;
    }

    pub(crate) fn entries(&self) -> &HashMap<String, DirectoryEntry> {
        &self.entries
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub(crate) fn serialize(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }

    pub(crate) fn deserialize<T>(bytes: T) -> Result<Self, Error>
    where
        T: AsRef<[u8]>,
    {
        match bincode::deserialize(bytes.as_ref()) {
            Ok(d) => Ok(d),
            Err(e) => Err(format_err!(
                "Failed to deserialize directory metadata: {}",
                e
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct FileMetadata {
    pub versions: Vec<FileVersion>,
}

impl FileMetadata {
    pub(crate) fn new() -> Self {
        FileMetadata { versions: vec![] }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct FileVersion {
    size: FileSize,
    start_block: Option<BlockNumber>,
    block_count: BlockCardinality,
}

impl FileVersion {
    pub(crate) fn new() -> Self {
        FileVersion {
            size: 0,
            start_block: None,
            block_count: 0,
        }
    }

    pub(crate) fn size(&self) -> FileSize {
        self.size
    }

    pub(crate) fn start_block(&self) -> Option<BlockNumber> {
        self.start_block
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
