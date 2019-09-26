//! File storage
//!
//! Files are just lists of blocks (data) with some metadata associated. In UFS, files are
//! versioned, and so to must the metadata of each file. Thus, the top-level file structure is a
//! list of [`FileVersion`]s.
use std::collections::HashMap;

use failure::format_err;
use log::{debug, error, trace};
use serde_derive::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
use crate::{
    block::{Block, BlockNumber},
    time::UfsTime,
    uuid::UfsUuid,
};

use super::{user::User, FileSize, Permission, PermissionGroups};

/// Data about Files
///
/// The primary purpose if this struct is to store information about the existing versions of a
/// file.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FileMetadata {
    /// The UUID of this file
    ///
    id: UfsUuid,
    /// The UUID of the parent directory
    ///
    dir_id: UfsUuid,
    /// Owner of this file
    ///
    owner: UfsUuid,
    /// Permission Groups for this file
    ///
    perms: PermissionGroups,
    /// The most recent version of this file
    ///
    last_version: usize,
    /// A map of all versions of this file
    ///
    versions: HashMap<usize, FileVersion>,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileMetadata {
    /// Create a new `FileMetadata`
    ///
    /// When a new file is created, a default, empty, [`FileVersion`] is created. This is mostly so
    /// that we capture a time stamp of when the file was created.
    pub(crate) fn new(id: UfsUuid, p_id: UfsUuid, owner: UfsUuid) -> Self {
        let mut versions = HashMap::new();
        versions.insert(0, FileVersion::new(id.random(), &id));
        FileMetadata {
            id,
            dir_id: p_id,
            owner,
            perms: PermissionGroups {
                user: Permission::ReadWrite,
                group: Permission::Read,
                other: Permission::Read,
            },
            last_version: 0,
            versions,
        }
    }

    fn new_with_version(file: &FileMetadata, v: FileVersion) -> Self {
        let mut versions = HashMap::new();
        let id = v.file_id.clone();
        versions.insert(0, v);
        FileMetadata {
            id,
            dir_id: file.dir_id,
            owner: file.owner,
            perms: file.perms.clone(),
            last_version: 0,
            versions,
        }
    }

    /// Return the UUID of this file
    pub(crate) fn id(&self) -> UfsUuid {
        self.id
    }

    /// Return the directory id of this file
    pub(crate) fn dir_id(&self) -> UfsUuid {
        self.dir_id
    }

    /// Return the file permissions, as a unix octal number
    ///
    pub(crate) fn unix_perms(&self) -> u16 {
        self.perms.as_u16()
    }

    /// Set the file permissions
    ///
    pub(crate) fn set_unix_perms(&mut self, perms: u16) {
        self.perms = perms.into();
    }

    pub(crate) fn new_version(&mut self) -> FileVersion {
        self.last_version += 1;
        self.versions.insert(
            self.last_version,
            FileVersion::new(self.id.new(self.last_version.to_string()), &self.id),
        );
        self.get_latest()
    }

    pub(crate) fn get_latest(&self) -> FileVersion {
        let version = self.versions.get(&self.last_version).unwrap();
        version.clone()
    }

    pub(crate) fn version_at(&self, v: usize) -> Option<FileVersion> {
        if let Some(version) = self.versions.get(&v) {
            Some(version.clone())
        } else {
            None
        }
    }

    /// Return the number of versions of the file
    pub(crate) fn version_count(&self) -> usize {
        self.versions.len()
    }

    /// Return a list of all of the versions of the file
    pub(crate) fn get_versions(&self) -> &HashMap<usize, FileVersion> {
        &self.versions
    }

    /// Returns a specific version of the file
    pub(in crate::metadata) fn get_version(&self, version: usize) -> Option<&FileVersion> {
        self.versions.get(&version)
    }

    pub(crate) fn commit_version(
        &mut self,
        mut version: FileVersion,
    ) -> Result<(), failure::Error> {
        debug!("--------");
        debug!("`commit_version`: {:?}", self);
        version.dirty = false;
        self.last_version += 1;
        match self.versions.insert(self.last_version, version) {
            None => Ok(()),
            Some(v) => {
                error!("version existed during commit {:#?}", v);
                Err(format_err!("unable to insert version into version table"))
            }
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
    /// The UUID of this version of the file
    ///
    id: UfsUuid,
    /// The UUID of the file to which this version belongs
    ///
    file_id: UfsUuid,
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
    /// Note that this does not need to start life as "dirty", because the `FileMetadata` is
    /// "dirty", and this will be written. The dirty flag is used when a version changes.
    fn new(id: UfsUuid, file_id: &UfsUuid) -> Self {
        let time = UfsTime::now();
        FileVersion {
            id,
            file_id: file_id.clone(),
            dirty: false,
            birth_time: time,
            write_time: time,
            change_time: time,
            access_time: time,
            size: 0,
            blocks: vec![],
        }
    }

    /// Return the nonce used to encrypt this version
    ///
    /// The nonce consists of the first four bytes of the version's UUID, followed by all 16 bytes
    /// of the file's UUID, ending with the last 4 bytes of the version's UUID.
    pub(crate) fn nonce(&self) -> Vec<u8> {
        let mut nonce = Vec::with_capacity(24);
        let ver_uuid = self.id.as_bytes();
        let file_uuid = self.file_id.as_bytes();

        nonce.extend_from_slice(&ver_uuid[0..4]);
        nonce.extend_from_slice(&file_uuid[..]);
        nonce.extend_from_slice(&ver_uuid[12..16]);
        nonce
    }

    /// Check the dirty flag
    ///
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Return the UUID of this file version's file
    pub(crate) fn file_id(&self) -> &UfsUuid {
        &self.file_id
    }

    /// Return the size of the file, in bytes
    pub(crate) fn size(&self) -> FileSize {
        self.size
    }

    /// Return a reference to the list of blocks that comprise the file
    pub(crate) fn blocks(&self) -> &Vec<BlockNumber> {
        &self.blocks
    }

    /// Convert a copy of this FileVersion into a FileMetadata
    ///
    /// Note that the returned FileMetadata will contain only this version of the file
    pub(crate) fn as_file_metadata(&self, file: &FileMetadata) -> FileMetadata {
        FileMetadata::new_with_version(file, self.clone())
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
        trace!("{:?}", self);
    }

    /// Return the `write_time` timestamp
    pub(crate) fn write_time(&self) -> UfsTime {
        self.write_time
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn nonce() {
        let root = UfsUuid::new_root_fs("test");
        let id = root.new("test_file"); // a506eaa8-7236-53f9-a7ed-9002fdc6a5b9
        let did = root.new("test_dir");
        let vid = root.new("test_version"); // 2397b0a7-2f31-5d27-9a37-795d05d1ab8b

        let version = FileVersion::new(vid, &id);

        // First 4 bytes of the file version UUID, followed by all 16 bytes of file UUID,
        // ending with last 4 bytes of file version UUID
        let mut expected: [u8; 24] = [
            0x23, 0x97, 0xb0, 0xa7, 0xa5, 0x06, 0xea, 0xa8, 0x72, 0x36, 0x53, 0xf9, 0xa7, 0xed,
            0x90, 0x02, 0xfd, 0xc6, 0xa5, 0xb9, 0x05, 0xd1, 0xab, 0x8b,
        ];

        assert_eq!(expected.to_vec(), version.nonce(), "incorrect nonce");
    }
}
