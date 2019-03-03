//! Block Directory
//!
//! This is a directory in the standard file system definition.  It's a mapping from strings (file
//! names) to blocks that contain the file data.  Currently, this means [String] -> [BlockTree].
//!
//! This is however insufficient, as we need to store file metadata, which I do not believe will
//! live in a BlockTree, but rather something like an inode.
//!
//! FIXME: File names should be SHA256 hashes for improved security.
use std::{
    cell::RefCell,
    collections::HashMap,
    io::{self, prelude::*},
    path::Path,
    rc::Rc,
};

use bincode;
use log::trace;
use serde_derive::{Deserialize, Serialize};

use crate::{
    block::{manager::BlockManager, storage::BlockStorage, tree::BlockTree, BlockType},
    io::{BlockTreeReader, BlockTreeWriter},
};

pub struct DirectoryEntryReader<BS>
where
    BS: BlockStorage,
{
    name: String,
    directory: Rc<RefCell<MutableDirectory<BS>>>,
    reader: Rc<RefCell<BlockTreeReader<BS>>>,
}

impl<BS> DirectoryEntryReader<BS>
where
    BS: BlockStorage,
{
    pub(crate) fn new<P>(
        path: P,
        directory: Rc<RefCell<MutableDirectory<BS>>>,
        reader: Rc<RefCell<BlockTreeReader<BS>>>,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        let file = path.as_ref().file_name().unwrap().to_str().unwrap();
        DirectoryEntryReader {
            name: String::from(file),
            directory,
            reader,
        }
    }
}

impl<BS> Read for DirectoryEntryReader<BS>
where
    BS: BlockStorage,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.borrow_mut().read(buf)
    }
}

pub struct DirectoryEntryWriter<BS>
where
    BS: BlockStorage,
{
    name: String,
    directory: Rc<RefCell<MutableDirectory<BS>>>,
    writer: Rc<RefCell<BlockTreeWriter<BS>>>,
}

impl<BS> DirectoryEntryWriter<BS>
where
    BS: BlockStorage,
{
    pub(crate) fn new<P>(
        path: P,
        directory: Rc<RefCell<MutableDirectory<BS>>>,
        writer: Rc<RefCell<BlockTreeWriter<BS>>>,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        let file = path.as_ref().file_name().unwrap().to_str().unwrap();
        DirectoryEntryWriter {
            name: String::from(file),
            directory,
            writer,
        }
    }
}

impl<BS> Write for DirectoryEntryWriter<BS>
where
    BS: BlockStorage,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.borrow_mut().flush()
    }
}

impl<BS> Drop for DirectoryEntryWriter<BS>
where
    BS: BlockStorage,
{
    fn drop(&mut self) {
        self.directory.borrow_mut().write_entry(
            self.name.clone(),
            Some(
                self.writer
                    .borrow_mut()
                    .get_tree()
                    .expect("Freeze problem:"),
            ),
        );
    }
}

pub struct MutableDirectory<BS>
where
    BS: BlockStorage,
{
    inner: Rc<RefCell<Directory>>,
    manager: Rc<RefCell<BlockManager<BS>>>,
    dirty: bool,
}

impl<BS> MutableDirectory<BS>
where
    BS: BlockStorage,
{
    pub(crate) fn new(
        mut inner: Rc<RefCell<Directory>>,
        mut manager: Rc<RefCell<BlockManager<BS>>>,
    ) -> Self {
        MutableDirectory {
            inner,
            manager,
            dirty: false,
        }
    }

    /// Create an empty entry.
    ///
    /// This is useful when you need to create an entry, but don't yet have the data.  Or to
    /// implement `touch`, for instance.
    ///
    /// FIXME: I'd rather have a sentinel value for an empty entry than an empty BlockTree.
    pub(crate) fn create_entry<S>(&mut self, name: S)
    //-> &mut Option<BlockTree>
    where
        S: Into<String>,
    {
        self.dirty = true;

        self.inner
            .borrow_mut()
            .inner
            .entry(name.into())
            .or_insert(None);
    }

    pub(crate) fn write_entry<S>(&mut self, name: S, tree: Option<BlockTree>)
    where
        S: Into<String>,
    {
        let name = name.into();
        trace!("write_entry({}, {:?})", name, tree);

        self.dirty = true;

        self.inner.borrow_mut().inner.insert(name, tree);
    }

    pub(crate) fn read_entry<S>(&mut self, name: S) -> Option<BlockTree>
    where
        S: AsRef<str>,
    {
        let name = name.as_ref();
        trace!("read_entry({})", name);

        if let Some(value) = self.inner.borrow_mut().inner.get(name) {
            if let Some(tree) = value.clone() {
                Some(tree)
            } else {
                None
            }
        } else {
            None
        }
    }

    // pub(crate) fn get_entry<s>(&mut self, name: S) ->
}

impl<S> Drop for MutableDirectory<S>
where
    S: BlockStorage,
{
    fn drop(&mut self) {
        if self.dirty {
            let ptr = BlockType::Pointer(
                self.manager
                    .borrow_mut()
                    .write(self.inner.borrow_mut().serialize().unwrap())
                    .unwrap(),
            );
            self.manager
                .borrow_mut()
                .write_metadata("@root_dir_ptr", ptr.serialize().unwrap())
                .unwrap();
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct Directory {
    inner: HashMap<String, Option<BlockTree>>,
}

impl Directory {
    pub(crate) fn new() -> Self {
        Directory {
            inner: HashMap::new(),
        }
    }

    pub(crate) fn serialize(&self) -> bincode::Result<Vec<u8>> {
        bincode::serialize(&self)
    }

    pub(crate) fn deserialize<T>(bytes: T) -> bincode::Result<Self>
    where
        T: AsRef<[u8]>,
    {
        bincode::deserialize(bytes.as_ref())
    }

    pub(crate) fn iter(&self) -> std::collections::hash_map::Iter<String, Option<BlockTree>> {
        self.inner.iter()
    }

    // /// Create an empty entry.
    // ///
    // /// This is useful when you need to create an entry, but don't yet have the data.  Or to
    // /// implement `touch`, for instance.
    // ///
    // /// FIXME: I'd rather have a sentinel value for an empty entry than an empty BlockTree.
    // pub(crate) fn create_entry<S>(&mut self, name: S)
    // where
    //     S: Into<String>,
    // {
    //     self.inner.entry(name.into()).or_insert(None);
    // }

    // pub(crate) fn entry_writer<S, BS>(
    //     &mut self,
    //     name: S,
    //     mut bm: Box<BlockManager<BS>>,
    // ) -> DirectoryEntryWriter<BS>
    // where
    //     S: Into<String>,
    //     BS: BlockStorage,
    // {
    //     DirectoryEntryWriter {
    //         entry: self.inner.entry(name.into()).or_insert(None),
    //         writer: BlockTreeWriter::new(&mut bm),
    //     }
    // }

    // pub(crate) fn entry_writer<'s: 'bm, 'bm, S, BS>(
    //     &'s mut self,
    //     name: S,
    //     mut bm: &'bm mut BlockManager<BS>,
    // ) -> DirectoryEntryWriter<'bm, BS>
    // where
    //     S: Into<String>,
    //     BS: BlockStorage + 'bm,
    // {
    //     DirectoryEntryWriter {
    //         entry: self.inner.entry(name.into()).or_insert(None),
    //         writer: BlockTreeWriter::new(&mut bm),
    //     }
    // }

    // /// Create an entry with data.
    // ///
    // pub(crate) fn add_entry<N>(&mut self, name: N, blocks: BlockTree)
    // where
    //     N: Into<String>,
    // {
    //     self.inner.insert(name.into(), blocks);
    // }

    // /// Get the data for a given, named entry.
    // ///
    // pub(crate) fn get_entry<N>(&self, name: N) -> Option<&BlockTree>
    // where
    //     N: AsRef<str>,
    // {
    //     self.inner.get(name.as_ref())
    // }
}

#[cfg(test)]
mod test_directory {
    use super::*;

    use crate::block::Block;

    // #[test]
    // fn add_entry_get_entry() {
    //     let mut dir = Directory::new();

    //     let bl_0 = BlockTree::new(&vec![Block::new(0, Some(b""))]);
    //     let bl_1 = BlockTree::new(&vec![Block::new(1, Some(b""))]);

    //     // Create some entries.
    //     dir.create_entry("test");

    //     dir.add_entry("test", bl_0.clone());
    //     dir.add_entry("another test".to_string(), bl_1.clone());
    //     dir.add_entry("hard link", bl_1.clone());

    //     // Read them back.
    //     assert_eq!(dir.get_entry("test".to_string()), Some(&bl_0));

    //     assert_eq!(dir.get_entry("another test"), Some(&bl_1));
    //     assert_eq!(dir.get_entry("hard link"), Some(&bl_1));

    //     // OCD test for non-equality
    //     assert_ne!(dir.get_entry("test"), Some(&bl_1));

    //     assert_eq!(dir.get_entry("missing"), None);
    // }
}
