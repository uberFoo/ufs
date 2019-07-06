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
use std::path::{Component, Components, Path, PathBuf};

use failure::format_err;
use log::{debug, trace, warn};
use serde_derive::{Deserialize, Serialize};

pub(crate) mod dir;
pub(crate) mod file;

#[cfg(not(target_arch = "wasm32"))]
use crate::uuid::UfsUuid;

pub(crate) type FileSize = u64;

/// The size of a FileHandle
pub type FileHandle = u64;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use dir::DirectoryMetadata;
pub(crate) use dir::WASM_EXT;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use file::{FileMetadata, FileVersion};

#[cfg(not(target_arch = "wasm32"))]
use crate::block::{
    wrapper::{MetadataDeserialize, MetadataSerialize},
    BlockNumber,
};

/// UFS internal definition of a File
///
/// This structure is used by the file system implementation as a file handle. It is a watered-down
/// FileMetadata that is cheaply cloneable. It contains the metadata id of the parent FileMetadata,
/// and a single, usually the latest, FileVersion of the file.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, PartialEq)]
pub struct File {
    /// UfsUuid of the file
    ///
    pub file_id: UfsUuid,
    /// The file wrapper, itself
    ///
    pub version: FileVersion,
}

/// File and Directory Permissions
///
/// Basic read, write, execute permissions. I expect that this list will grow.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Permission {
    /// No permissions
    ///
    Nada,
    /// Permission to read
    ///
    Read,
    /// Permission to write
    ///
    Write,
    /// Permission to executer
    ///
    Execute,
    /// Permission to read, write, and execute
    ///
    ReadWriteExecute,
    /// Permission to read and write
    ///
    ReadWrite,
    /// Permission to read and execute
    ///
    ReadExecute,
    /// Permission to write and execute
    ///
    WriteExecute,
}

impl Permission {
    pub fn as_u16(&self) -> u16 {
        match self {
            Permission::Nada => 0,
            Permission::Read => 4,
            Permission::Write => 2,
            Permission::Execute => 1,
            Permission::ReadWriteExecute => 7,
            Permission::ReadWrite => 6,
            Permission::ReadExecute => 5,
            Permission::WriteExecute => 3,
        }
    }
}

/// File Permission Groups
///
/// Basic organization of file and directory permissions, that align with unix permissions.
/// This is necessary, but likely not sufficiont, and I expect this will need to evolve to meet the
/// needs of the full-blown file system.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PermissionGroups {
    user: Permission,
    group: Permission,
    other: Permission,
}

impl PermissionGroups {
    fn as_u16(&self) -> u16 {
        let mut perms = self.user.as_u16();
        perms <<= 3;
        perms += self.group.as_u16();
        perms <<= 3;
        perms += self.other.as_u16();
        perms
    }
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

///

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

#[cfg(not(target_arch = "wasm32"))]
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

    /// Get FileMetadata given a parent directory, and a name
    ///
    pub(crate) fn get_file_metadata_from_dir_and_name(
        &self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<FileMetadata, failure::Error> {
        if let Some(dir) = self.lookup_dir(dir_id) {
            match dir.entries().get(name) {
                Some(DirectoryEntry::File(f)) => Ok(f.clone()),
                _ => Err(format_err!(
                    "unable to find file {} under directory {}",
                    name,
                    dir_id
                )),
            }
        } else {
            Err(format_err!("unable to find directory with id {}", dir_id))
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
    ) -> Result<Vec<BlockNumber>, failure::Error> {
        debug!("--------");
        debug!("`unlink_file`: {}, dir: {:#?}", name, dir_id);

        if let Some(dir) = self.lookup_dir_mut(dir_id) {
            // If this is a file in the special versions directory, then we are removing a version
            // from the parent.
            if dir.is_vers_dir() {
                debug!("\teventually, we'll be able to remove specific versions of the file");
                debug!("\tsomeday, I'd even like to make removing the root file, save it");
                debug!("\tsomeplace until all of the versions are removed");
                Ok(vec![])
            } else {
                match dir.entries_mut().remove(name) {
                    Some(DirectoryEntry::File(file)) => {
                        debug!("\tremoved {:#?}\nfrom {:#?}", file, dir);
                        self.dirty = true;
                        // We need to collect all of the blocks, for all of the versions of the file
                        // and return them as a single list to be deleted by the caller
                        let mut blocks = vec![];
                        for v in file.get_versions().values() {
                            for b in v.blocks() {
                                blocks.push(*b);
                            }
                            // blocks.append(v.blocks());
                        }
                        Ok(blocks)
                    }
                    _ => Err(format_err!("did not find {} in {:#?}", name, dir)),
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

    pub(crate) fn id_from_path<P: AsRef<Path>>(&self, path: P) -> Option<UfsUuid> {
        fn from_path_r(
            components: &mut Components,
            dir: &DirectoryMetadata,
        ) -> Option<DirectoryEntry> {
            match components.next() {
                Some(Component::RootDir) => from_path_r(components, dir),
                Some(Component::Normal(name)) => match name.to_str() {
                    Some(name) => match dir.entries().get(name) {
                        Some(entry) => match entry {
                            DirectoryEntry::Directory(d) => from_path_r(components, d),
                            DirectoryEntry::File(f) => Some(DirectoryEntry::File(f.clone())),
                        },
                        None => None,
                    },
                    None => {
                        warn!("invalid UTF-8 in path: {:?}", name);
                        None
                    }
                },
                None => Some(DirectoryEntry::Directory(dir.clone())),
                _ => {
                    warn!("malformed path: {:?}", components);
                    None
                }
            }
        }

        match from_path_r(&mut path.as_ref().components(), &self.root_directory) {
            Some(DirectoryEntry::File(f)) => Some(f.id()),
            Some(DirectoryEntry::Directory(d)) => Some(d.id()),
            None => None,
        }
    }

    pub(crate) fn path_from_file_id(&self, id: UfsUuid) -> PathBuf {
        let mut path = PathBuf::new();

        fn make_path_file(path: &mut PathBuf, f: &FileMetadata, metadata: &Metadata) {
            make_path_dir(
                path,
                metadata.lookup_dir(f.dir_id()).unwrap(),
                f.id(),
                metadata,
            );
        }

        fn make_path_dir(
            path: &mut PathBuf,
            d: &DirectoryMetadata,
            id: UfsUuid,
            metadata: &Metadata,
        ) {
            if let Some(parent_id) = d.parent_id() {
                make_path_dir(
                    path,
                    metadata.lookup_dir(parent_id).unwrap(),
                    d.id(),
                    metadata,
                );
            } else {
                path.push("/");
            }

            for (name, entry) in d.entries() {
                if id
                    == match entry {
                        DirectoryEntry::Directory(d) => d.id(),
                        DirectoryEntry::File(f) => f.id(),
                    }
                {
                    path.push(name);
                    break;
                }
            }
        }

        make_path_file(&mut path, self.lookup_file(id).unwrap(), &self);
        path
    }

    pub(crate) fn path_from_dir_id(&self, id: UfsUuid) -> PathBuf {
        let mut path = PathBuf::new();

        fn make_path_dir(
            path: &mut PathBuf,
            d: &DirectoryMetadata,
            id: UfsUuid,
            metadata: &Metadata,
        ) {
            if let Some(parent_id) = d.parent_id() {
                make_path_dir(
                    path,
                    metadata.lookup_dir(parent_id).unwrap(),
                    d.id(),
                    metadata,
                );
            } else {
                path.push("/");
            }

            for (name, entry) in d.entries() {
                if id
                    == match entry {
                        DirectoryEntry::Directory(d) => d.id(),
                        DirectoryEntry::File(f) => f.id(),
                    }
                {
                    path.push(name);
                    break;
                }
            }
        }

        make_path_dir(&mut path, self.lookup_dir(id).unwrap(), id, &self);
        path
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

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn new_metadata() {
        init();

        let m = Metadata::new(UfsUuid::new_root("test"));
        let root = m.root_directory();

        assert_eq!(m.is_dirty(), true);
        assert_eq!(root.is_dirty(), false);
        assert_eq!(root.parent_id(), None);
    }

    #[test]
    fn new_directory() {
        init();

        let mut m = Metadata::new(UfsUuid::new_root("test"));
        let root_id = m.root_directory().id();
        let d = m.new_directory(root_id, "test").unwrap();
        let d2 = m.new_directory(d.id(), "test2").unwrap();

        assert_eq!(d.parent_id(), Some(root_id));
        assert_eq!(d2.parent_id(), Some(d.id()));
    }

    #[test]
    fn id_for_path() {
        init();

        let mut m = Metadata::new(UfsUuid::new_root("test"));
        let root_id = m.root_directory().id();
        let dir = m.new_directory(root_id, "foo").unwrap();
        let wasm = dir.entries().get(".wasm").unwrap();
        let wasm_id = if let DirectoryEntry::Directory(d) = wasm {
            d.id()
        } else {
            panic!("got a DirectoryEntry::File");
        };
        let file = m.new_file(wasm_id, "test_program.wasm").unwrap();

        assert_eq!(m.id_from_path(Path::new("/")), Some(root_id), "id for /");
        assert_eq!(
            m.id_from_path(Path::new("/foo")),
            Some(dir.id()),
            "id for /foo"
        );
        assert_eq!(
            m.id_from_path(Path::new("/foo/.wasm")),
            Some(wasm_id),
            "id for /foo/.wasm"
        );
        assert_eq!(
            m.id_from_path(Path::new("/foo/.wasm/test_program.wasm")),
            Some(file.file_id),
            "id for /foo/.wasm/test_program.wasm"
        );
    }

    #[test]
    fn path_for_id() {
        init();

        let mut m = Metadata::new(UfsUuid::new_root("test"));
        let root_id = m.root_directory().id();
        let dir = m.new_directory(root_id, "foo").unwrap();
        let wasm = dir.entries().get(".wasm").unwrap();
        let wasm_id = if let DirectoryEntry::Directory(d) = wasm {
            d.id()
        } else {
            panic!("got a DirectoryEntry::File");
        };
        let file = m.new_file(wasm_id, "test_program.wasm").unwrap();

        assert_eq!(
            Path::new("/foo/.wasm/test_program.wasm"),
            m.path_from_file_id(file.file_id)
        );

        assert_eq!(Path::new("/"), m.path_from_dir_id(root_id));
        assert_eq!(Path::new("/foo/.wasm"), m.path_from_dir_id(wasm_id));
    }

    #[test]
    fn permissions() {
        let p755 = PermissionGroups {
            user: Permission::ReadWriteExecute,
            group: Permission::ReadExecute,
            other: Permission::ReadExecute,
        };
        assert_eq!(0o755, p755.as_u16());

        let p644 = PermissionGroups {
            user: Permission::ReadWrite,
            group: Permission::Read,
            other: Permission::Read,
        };
        assert_eq!(0o644, p644.as_u16());

        let p201 = PermissionGroups {
            user: Permission::Write,
            group: Permission::Nada,
            other: Permission::Execute,
        };
        assert_eq!(0o201, p201.as_u16());
    }
}
