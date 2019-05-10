//! Metadata
//!
//! Fuck below.  Version one is a hashmap that fits in a single block, and lives at Block 0.  Here's
//! some JSON that illustrates what it looks like:
//!
//! ```JSON
//! {
//!     @fs-metadata: {
//!         next-free-block: 88
//!     },
//!     @root_dir: {
//!         @type: "directory",
//!         @entries: [
//!             {
//!                 @type: "directory",
//!                 @name: ""
//!                 @entries: [
//!                     {
//!                         @type: "file",
//!                         @name: "README.md",
//!                         @start_block: 77,
//!                         @block_count: 5
//!                         @version: 0
//!                         @hash: "06a2643f85279ae68043bb27654408282d996942e3f313c079f819a29299979c"
//!                     },
//!                     {
//!                         @type: "directory",
//!                         @name: "home"
//!                         @entries: []
//!                     }
//!                 ]
//!             }
//!         ]
//!     }
//! }
//! ```
//!
//! For now, there is a single block of file-system level metadata stored as a `HashMap` -- nothing
//! fancy.  It is written to blocks by first serializing it to a `Vec<u8`, and chunking into `n`
//! block-sized chunks.  A header block is written that contains some metadata and information to
//! load the chunks, and reassemble later.  The header block is again a `HashMap`.
//!
//! The basic layout for the header block looks as follows (as JSON):
//!
//! ```JSON
//! {
//!     @type: "metadata"
//!     @version: <monotonically increasing number>
//!     @next_block: "<block number>"
//! }
//! ```
use std::collections::HashMap;

use serde_derive::{Deserialize, Serialize};

use crate::block::{BlockCardinality, BlockNumber};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Metadata {
    pub next_free_block: Option<BlockNumber>,
    pub root_dir: DirectoryMetadata,
}

impl Metadata {
    pub(crate) fn serialize(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }

    pub(crate) fn deserialize<T>(bytes: T) -> bincode::Result<Self>
    where
        T: AsRef<[u8]>,
    {
        bincode::deserialize(bytes.as_ref())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) enum DirectoryEntry {
    Directory(DirectoryMetadata),
    File(FileMetadata),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct DirectoryMetadata {
    pub name: String,
    pub entries: Vec<DirectoryEntry>,
}

impl DirectoryMetadata {
    pub(crate) fn new_root() -> Self {
        DirectoryMetadata {
            name: "root".to_string(),
            entries: vec![],
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct FileMetadata {
    pub name: String,
    pub start_block: BlockNumber,
    pub block_count: BlockCardinality,
}
