//! Block Directory
//!
//! This is a directory in the standard file system definition.  It's a mapping from strings (file
//! names) to blocks that contain the file data.  Currently, this means [String] -> [BlockTree].
//!
//! This is however insufficient, as we need to store file metadata, which I do not believe will
//! live in a BlockTree, but rather something like an inode.
//!
//! FIXME: File names should be SHA256 hashes for improved security.
use std::collections::HashMap;

use serde_derive::{Deserialize, Serialize};

use crate::block::tree::BlockTree;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Directory {
    inner: HashMap<String, BlockTree>,
}

impl Directory {
    pub(crate) fn new() -> Self {
        Directory {
            inner: HashMap::new(),
        }
    }

    pub(crate) fn create_entry<N>(&mut self, name: N)
    where
        N: Into<String>,
    {
        self.inner
            .entry(name.into())
            .or_insert(BlockTree::new(&vec![]));
    }

    pub(crate) fn add_entry<N>(&mut self, name: N, blocks: BlockTree)
    where
        N: Into<String>,
    {
        self.inner.insert(name.into(), blocks);
    }

    pub(crate) fn get_entry<N>(&self, name: N) -> Option<&BlockTree>
    where
        N: AsRef<str>,
    {
        self.inner.get(name.as_ref())
    }
}

#[cfg(test)]
mod test_directory {
    use super::*;

    use crate::block::Block;

    #[test]
    fn add_entry_get_entry() {
        let mut dir = Directory::new();

        let bl_0 = BlockTree::new(&vec![Block::new(0, Some(b""))]);
        let bl_1 = BlockTree::new(&vec![Block::new(1, Some(b""))]);

        // Create some entries.
        dir.create_entry("test");

        dir.add_entry("test", bl_0.clone());
        dir.add_entry("another test".to_string(), bl_1.clone());
        dir.add_entry("hard link", bl_1.clone());

        // Read them back.
        assert_eq!(dir.get_entry("test".to_string()), Some(&bl_0));

        assert_eq!(dir.get_entry("another test"), Some(&bl_1));
        assert_eq!(dir.get_entry("hard link"), Some(&bl_1));

        // OCD test for non-equality
        assert_ne!(dir.get_entry("test"), Some(&bl_1));

        assert_eq!(dir.get_entry("missing"), None);
    }
}
