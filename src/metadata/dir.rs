//! Metadata for Directories
//!
//! This struct stores all the various necessary time stamps, as well as a map of files (and
//! other directories) that it contains. These are stored as [`DirectoryEntry`] structures.
//!
//! FIXME: The directory data is not versioned. What happens to deleted files?  What do we do when
//! a directory goes away?
use {
    crate::{time::UfsTime, uuid::UfsUuid, IOFSErrorKind},
    failure::format_err,
    log::debug,
    serde_derive::{Deserialize, Serialize},
    std::collections::HashMap,
};

pub(crate) const WASM_DIR: &'static str = ".wasm";
pub(crate) const WASM_EXT: &'static str = "wasm";
pub(crate) const VERS_DIR: &'static str = ".vers";

use super::{DirectoryEntry, FileMetadata, Permission, PermissionGroups};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DirectoryMetadata {
    /// A flag indicating that the directory's data has been modified and needs to be written.
    ///
    #[serde(skip)]
    dirty: bool,
    /// The UUID of this directory
    ///
    id: UfsUuid,
    /// The UUID of this directory's parent
    ///
    parent_id: Option<UfsUuid>,
    /// The owner of this directory
    ///
    owner: UfsUuid,
    /// Permission Groups for this directory
    ///
    perms: PermissionGroups,
    /// Special ".wasm" directory flag
    /// FIXME: I don't know if I like this. If I keep this, or similar, we should have one for the
    /// ".vers" directory as well. This would be good as an extended attribute.
    ///
    wasm_dir: bool,
    /// Special ".vers" directory flag
    /// FIXME: See above
    vers_dir: bool,
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
    ///
    entries: HashMap<String, DirectoryEntry>,
}

impl DirectoryMetadata {
    /// Create a new directory
    ///
    /// Note that every directory has two special subdirectories. One, ".wasm" is meant to contain
    /// WASM programs to execute in the file system. The other, ".vers", contains older versions
    /// of files, located in the directory.
    ///
    pub(crate) fn new(id: UfsUuid, p_id: Option<UfsUuid>, owner: UfsUuid) -> Self {
        let time = UfsTime::now();
        let perms = PermissionGroups {
            user: Permission::ReadWriteExecute,
            group: Permission::ReadExecute,
            other: Permission::ReadExecute,
        };
        let mut d = DirectoryMetadata {
            dirty: false,
            id: id,
            parent_id: p_id,
            owner,
            perms: perms.clone(),
            wasm_dir: false,
            vers_dir: false,
            birth_time: time,
            write_time: time,
            change_time: time,
            access_time: time,
            entries: HashMap::new(),
        };
        // Create the directory for WASM programs
        d.entries.insert(
            WASM_DIR.to_string(),
            DirectoryEntry::Directory(DirectoryMetadata {
                dirty: false,
                id: id.new(WASM_DIR),
                parent_id: Some(id),
                owner,
                perms: perms.clone(),
                wasm_dir: true,
                vers_dir: false,
                birth_time: time,
                write_time: time,
                change_time: time,
                access_time: time,
                entries: HashMap::new(),
            }),
        );
        // Create the directory for file versions
        d.entries.insert(
            VERS_DIR.to_string(),
            DirectoryEntry::Directory(DirectoryMetadata {
                dirty: false,
                id: id.new(VERS_DIR),
                parent_id: Some(id),
                owner,
                perms,
                wasm_dir: false,
                vers_dir: true,
                birth_time: time,
                write_time: time,
                change_time: time,
                access_time: time,
                entries: HashMap::new(),
            }),
        );
        d
    }

    /// Return a reference to the HashMap from entry name to DirectoryEntry structures
    ///
    pub(crate) fn entries(&self) -> &HashMap<String, DirectoryEntry> {
        &self.entries
    }

    /// Return a mutable reference to the name -> DirectoryEntry HashMap
    ///
    pub(crate) fn entries_mut(&mut self) -> &mut HashMap<String, DirectoryEntry> {
        &mut self.entries
    }

    /// Set the entries
    ///
    pub(crate) fn set_entries(&mut self, entries: HashMap<String, DirectoryEntry>) {
        self.entries = entries;
    }

    /// Return the UUID
    ///
    pub(crate) fn id(&self) -> UfsUuid {
        self.id
    }

    /// Return the parent UUID
    ///
    pub(crate) fn parent_id(&self) -> Option<UfsUuid> {
        self.parent_id
    }

    /// Return the Owner
    ///
    pub(crate) fn owner(&self) -> UfsUuid {
        self.owner
    }

    /// Return the directory permissions, as a unix octal number
    ///
    pub(crate) fn unix_perms(&self) -> u16 {
        self.perms.as_u16()
    }

    /// Set the directory permissions
    ///
    pub(crate) fn set_unix_perms(&mut self, perms: u16) {
        self.dirty = true;
        self.perms = perms.into();
    }

    /// Return the `write_time` timestamp
    ///
    pub(crate) fn write_time(&self) -> UfsTime {
        self.write_time
    }

    /// Return if this is a ".wasm" directory
    ///
    pub(crate) fn is_wasm_dir(&self) -> bool {
        self.wasm_dir
    }

    /// Return if this is a ".vers" directory
    ///
    pub(crate) fn is_vers_dir(&self) -> bool {
        self.vers_dir
    }

    /// Return true if the directory needs to be serialized
    ///
    #[allow(dead_code)]
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Set to serialize directory
    ///
    pub(crate) fn dirty(&mut self) {
        self.dirty = true;
    }

    /// Create a new directory as a child of this directory.
    ///
    pub(crate) fn new_subdirectory(
        &mut self,
        name: String,
        owner: UfsUuid,
    ) -> Result<DirectoryMetadata, failure::Error> {
        debug!("--------");
        debug!("`new_subdirectory`: {:?}", name);

        if self.entries.contains_key(&name) {
            Err(IOFSErrorKind::DirectoryExists.into())
        } else {
            let new_id = self.id.new(&name);
            let dir = DirectoryMetadata::new(new_id, Some(self.id), owner);
            match self
                .entries
                .insert(name, DirectoryEntry::Directory(dir.clone()))
            {
                None => {
                    debug!("\tcreated sub directory {:?}", new_id);
                    Ok(dir)
                }
                Some(_) => Err(format_err!("unable to store directory entry")),
            }
        }
    }

    /// Create a new file in this directory
    ///
    pub(crate) fn new_file(&mut self, name: String) -> Result<FileMetadata, failure::Error> {
        debug!("--------");
        debug!("`new_file`: {:?}", name);

        if self.entries.contains_key(&name) {
            Err(format_err!("file already exists"))
        } else {
            let new_id = self.id.new(&name);
            let file = FileMetadata::new(new_id, self.id, self.owner);
            match self
                .entries
                .insert(name, DirectoryEntry::File(file.clone()))
            {
                None => {
                    debug!("\tcreated file {:?}", new_id);
                    Ok(file)
                }
                Some(_) => Err(format_err!("unable to store directory entry")),
            }
        }
    }

    /// Lookup a subdirectory by id, and return a reference to it.
    ///
    pub(in crate::metadata) fn lookup_dir(&self, id: UfsUuid) -> Option<&DirectoryMetadata> {
        debug!("--------");
        debug!("`lookup_dir`: {:#?}, parent {:#?}", self.id, self.parent_id);

        for e in self.entries.values() {
            if let DirectoryEntry::Directory(d) = e {
                if d.id == id {
                    debug!("\tfound {:#?}", d.id());
                    return Some(d);
                } else {
                    debug!("\tsearching {:#?}", d.id());
                    if let Some(d) = DirectoryMetadata::lookup_dir(d, id) {
                        debug!("\treturning {:#?}", d.id());
                        return Some(d);
                    }
                }
            }
        }

        None
    }

    /// Lookup a subdirectory by id, and return a mutable reference to it.
    ///
    pub(in crate::metadata) fn lookup_dir_mut(
        &mut self,
        id: UfsUuid,
    ) -> Option<&mut DirectoryMetadata> {
        debug!("--------");
        debug!(
            "`lookup_dir_mut`: {:#?}, parent {:#?}",
            self.id, self.parent_id
        );

        // Do a "stupid" search for the given ID
        // Not tail recursion because I need to make this as dirty, and I can't borrow it twice in
        // Metadata::lookup_dir_mut.
        if self.id == id {
            self.dirty = true;
            debug!("\tfound {:#?}", self.id());
            return Some(self);
        } else {
            for e in self.entries.values_mut() {
                if let DirectoryEntry::Directory(ref mut d) = e {
                    debug!("\tsearching {:#?}", d.id());
                    if let Some(d) = DirectoryMetadata::lookup_dir_mut(d, id) {
                        d.dirty = true;
                        debug!("\treturning {:#?}", d.id());
                        return Some(d);
                    }
                }
            }
        }
        None
    }

    /// Lookup a file by id, and return a reference to it.
    ///
    pub(in crate::metadata) fn lookup_file(&self, id: UfsUuid) -> Option<&FileMetadata> {
        debug!("--------");
        debug!(
            "`lookup_file`: {:#?}, parent {:#?}",
            self.id, self.parent_id
        );

        for e in self.entries.values() {
            match e {
                DirectoryEntry::File(f) => {
                    if f.id() == id {
                        return Some(f);
                    }
                }
                DirectoryEntry::Directory(d) => {
                    if let Some(f) = DirectoryMetadata::lookup_file(d, id) {
                        return Some(f);
                    }
                }
            }
        }

        None
    }

    /// Lookup a file by id, and return a mutable reference to it.
    ///
    pub(in crate::metadata) fn lookup_file_mut(
        &mut self,
        id: UfsUuid,
    ) -> Option<&mut FileMetadata> {
        debug!("--------");
        debug!(
            "`lookup_file_mut`: {:#?}, parent {:#?}",
            self.id, self.parent_id
        );

        self.dirty = true;

        for e in self.entries.values_mut() {
            match e {
                DirectoryEntry::File(f) => {
                    if f.id() == id {
                        return Some(f);
                    }
                }
                DirectoryEntry::Directory(d) => {
                    if let Some(f) = DirectoryMetadata::lookup_file_mut(d, id) {
                        return Some(f);
                    }
                }
            }
        }

        None
    }
}
