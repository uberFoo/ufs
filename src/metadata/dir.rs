//! Metadata for Directories
//!
//! This struct stores all the various necessary time stamps, as well as a map of files (and
//! other directories) that it contains. These are stored as [`DirectoryEntry`] structures.
//!
//! FIXME: The directory data is not versioned. What happens to deleted files?  What do we do when
//! a directory goes away?
use failure::format_err;
use log::{debug, error, trace, warn};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ffi::OsStr,
    path::{Component, Components, Path},
};

use crate::{
    block::wrapper::{MetadataDeserialize, MetadataSerialize},
    time::UfsTime,
    uuid::UfsUuid,
};

pub(crate) const WASM_DIR: &'static str = ".wasm";
pub(crate) const WASM_EXT: &'static str = "wasm";
pub(crate) const VERS_DIR: &'static str = ".vers";

use super::{Directory, DirectoryEntry, File, FileMetadata, FileVersion};

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
    entries: HashMap<String, DirectoryEntry>,
}

#[cfg(not(target_arch = "wasm32"))]
impl DirectoryMetadata {
    pub(crate) fn new(id: UfsUuid, p_id: Option<UfsUuid>) -> Self {
        let time = UfsTime::now();
        let mut d = DirectoryMetadata {
            dirty: true,
            id: id,
            parent_id: p_id,
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
                birth_time: time,
                write_time: time,
                change_time: time,
                access_time: time,
                entries: HashMap::new(),
            }),
        );
        d
    }

    pub(crate) fn new_subdirectory(
        &mut self,
        name: String,
    ) -> Result<DirectoryMetadata, failure::Error> {
        debug!("--------");
        debug!("`new_subdirectory`: {:?}", name);

        if self.entries.contains_key(&name) {
            Err(format_err!("directory already exists"))
        } else {
            let new_id = self.id.new(&name);
            let dir = DirectoryMetadata::new(new_id, Some(self.id));
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
    pub(crate) fn new_file(&mut self, name: String) -> Result<FileMetadata, failure::Error> {
        debug!("--------");
        debug!("`new_file`: {:?}", name);

        if self.entries.contains_key(&name) {
            Err(format_err!("file already exists"))
        } else {
            let new_id = self.id.new(&name);
            let file = FileMetadata::new(new_id, self.id);
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

    // fn get_directory_metadata<'a>(
    //     &mut self,
    //     path: &Path,
    //     mark_dirty: bool,
    //     mut components: &mut Components<'a>,
    // ) -> Option<&mut DirectoryMetadata> {
    //     debug!("--------");
    //     debug!(
    //         "`get_directory_metadata`: path: {:?}, components {:?}",
    //         path, components
    //     );
    //     match components.next() {
    //         Some(Component::RootDir) => {
    //             if mark_dirty {
    //                 self.dirty = true;
    //             }
    //             self.get_directory_metadata(path, mark_dirty, &mut components)
    //         }
    //         Some(Component::Normal(name)) => match name.to_str() {
    //             Some(name) => match self.entries.get_mut(name) {
    //                 Some(DirectoryEntry::Directory(sub_dir)) => {
    //                     if mark_dirty {
    //                         sub_dir.dirty = true;
    //                     }
    //                     DirectoryMetadata::get_directory_metadata(
    //                         sub_dir,
    //                         path,
    //                         mark_dirty,
    //                         &mut components,
    //                     )
    //                 }
    //                 _ => {
    //                     warn!("`get_directory_metadata`: couldn't find {:?}", path);
    //                     None
    //                 }
    //             },
    //             _ => {
    //                 error!("`get_directory_metadata`: invalid utf-8 in path {:?}", path);
    //                 None
    //             }
    //         },
    //         None => Some(self),
    //         _ => {
    //             error!("`get_directory_metadata`: wonky path: {:?}", path);
    //             None
    //         }
    //     }
    // }

    // /// Create a new directory in this directory
    // pub(crate) fn new_directory_o<P>(
    //     &mut self,
    //     fs_id: &UfsUuid,
    //     path: P,
    // ) -> Result<Directory, failure::Error>
    // where
    //     P: AsRef<Path>,
    // {
    //     let path = path.as_ref();
    //     debug!("--------");
    //     debug!("`new_directory`: {:?}", path);
    //     if let Some(dir_name) = path.file_name() {
    //         if let Some(root) = path.parent() {
    //             let mut iter = root.components();
    //             if let Some(dir_root) = self.get_directory_metadata(root, true, &mut iter) {
    //                 let dir = DirectoryMetadata::new(fs_id.new(path.to_str().unwrap()));

    //                 dir_root.entries.insert(
    //                     dir_name.to_str().unwrap().to_owned(),
    //                     DirectoryEntry::Directory(dir.clone()),
    //                 );

    //                 debug!("\treturning: {:#?}", dir);
    //                 return Ok(Directory {
    //                     path: path.to_owned(),
    //                     id: dir.id.clone(),
    //                     directory: dir,
    //                 });
    //             }
    //         }
    //     }
    //     Err(format_err!(
    //         "`new_directory` could not create directory {:?}",
    //         path
    //     ))
    // }

    // pub(crate) fn new_file_o<P>(&mut self, fs_id: &UfsUuid, path: P) -> Result<File, failure::Error>
    // where
    //     P: AsRef<Path>,
    // {
    //     let path = path.as_ref();
    //     debug!("--------");
    //     debug!("`new_file`: {:?}", path);
    //     if let Some(file_name) = path.file_name() {
    //         if let Some(root) = path.parent() {
    //             let mut iter = root.components();
    //             if let Some(dir_root) = self.get_directory_metadata(root, true, &mut iter) {
    //                 let file =
    //                     FileMetadata::new(dir_root.id().clone(), fs_id.new(path.to_str().unwrap()));

    //                 dir_root.entries.insert(
    //                     file_name.to_str().unwrap().to_owned(),
    //                     DirectoryEntry::File(file.clone()),
    //                 );

    //                 debug!("\treturning {:#?}", file);

    //                 Ok(File {
    //                     path: path.to_owned(),
    //                     file_id: file.file_id().clone(),
    //                     version: file.get_latest(),
    //                 })
    //             } else {
    //                 Err(format_err!("bogus root directory"))
    //             }
    //         } else {
    //             Err(format_err!("malformed path"))
    //         }
    //     } else {
    //         Err(format_err!("malformed path"))
    //     }
    // }

    // /// Remove a file from a directory
    // pub(crate) fn unlink_file<P>(&mut self, path: P) -> Result<(), failure::Error>
    // where
    //     P: AsRef<Path>,
    // {
    //     let path = path.as_ref();
    //     debug!("--------");
    //     debug!("`unlink_file` {:?}", path);
    //     let file_version = self.get_file(&path, true);

    //     // let mut iter = path.components();
    //     // if let Some(dir_root) = self.get_directory_metadata(path, true, &mut iter) {

    //     // }
    //     Ok(())
    // }

    // /// Retrieve a directory by name, from this directory
    // pub(crate) fn get_directory<P>(&mut self, path: P) -> Result<Directory, failure::Error>
    // where
    //     P: AsRef<Path>,
    // {
    //     let path = path.as_ref();
    //     debug!("--------");
    //     debug!("`get_directory`: {:?}", path);
    //     //
    //     // Populate the special `.vers` directory with existing file versions.
    //     let mut vers = false;
    //     let mut files = HashMap::<String, DirectoryEntry>::new();
    //     if path.file_name() == Some(OsStr::new(VERS_DIR)) {
    //         vers = true;
    //         if let Some(parent_path) = path.parent() {
    //             let mut iter = parent_path.components();
    //             if let Some(parent_dir) = self.get_directory_metadata(parent_path, false, &mut iter)
    //             {
    //                 for (name, entry) in &parent_dir.entries {
    //                     if let DirectoryEntry::File(file) = entry {
    //                         for (n, _) in file.get_versions().iter().enumerate() {
    //                             let mut name = name.clone();
    //                             name.push('@');
    //                             name.push_str(&n.to_string());
    //                             if let Some(file) = file.version_at(n) {
    //                                 files.insert(
    //                                     name,
    //                                     DirectoryEntry::File(file.into_file_metadata()),
    //                                 );
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //     let mut iter = path.components();
    //     if let Some(dir) = self.get_directory_metadata(path, false, &mut iter) {
    //         let mut dir = dir.clone();
    //         if vers {
    //             dir.entries = files;
    //         }
    //         Ok(Directory {
    //             path: path.to_owned(),
    //             id: dir.id.clone(),
    //             directory: dir,
    //         })
    //     } else {
    //         Err(format_err!("`get_directory` malformed path"))
    //     }
    // }

    // fn get_file<P>(&mut self, path: P, dirty: bool) -> Result<FileVersion, failure::Error>
    // where
    //     P: AsRef<Path>,
    // {
    //     let path = path.as_ref();
    //     debug!("--------");
    //     debug!("`get_file`: {:?}", path);
    //     // Check to see if the file we are opening is in the "versions" subdirectory. If so,
    //     // populate a `FileMetadata` with the requested file version.
    //     if let Some(root_path) = path.parent() {
    //         if root_path.file_name() == Some(OsStr::new(VERS_DIR)) {
    //             debug!("\tworking in {} directory", VERS_DIR);
    //             if let Some(root_path) = root_path.parent() {
    //                 let mut iter = root_path.components();
    //                 if let Some(dir_root) = self.get_directory_metadata(root_path, dirty, &mut iter)
    //                 {
    //                     if let Some(versioned_file_name) = path.file_name() {
    //                         // FIXME: This will break if there are @'s in the file name
    //                         if let Some(vfn_str) = versioned_file_name.to_str() {
    //                             let mut i = vfn_str.split(|c| c == '@');
    //                             if let Some(file_name) = i.next() {
    //                                 if let Some(v_str) = i.next() {
    //                                     if let Ok(version) = v_str.parse::<usize>() {
    //                                         if let Some(DirectoryEntry::File(file)) =
    //                                             dir_root.entries.get(file_name)
    //                                         {
    //                                             if let Some(file_version) = file.version_at(version)
    //                                             {
    //                                                 debug!("\treturning file {:#?}", file_version);
    //                                                 Ok(file_version)
    //                                             } else {
    //                                                 Err(format_err!(
    //                                                     "can't find version {} for {:?}",
    //                                                     version,
    //                                                     path
    //                                                 ))
    //                                             }
    //                                         } else {
    //                                             Err(format_err!(
    //                                                 "can't find {} in directory {:?}",
    //                                                 file_name,
    //                                                 root_path
    //                                             ))
    //                                         }
    //                                     } else {
    //                                         Err(format_err!(
    //                                             "can't parse version number {:?}",
    //                                             path
    //                                         ))
    //                                     }
    //                                 } else {
    //                                     Err(format_err!("file name missing version {:?}", path))
    //                                 }
    //                             } else {
    //                                 Err(format_err!("malformed versioned file {:?}", path))
    //                             }
    //                         } else {
    //                             Err(format_err!("malformed file name {:?}", versioned_file_name))
    //                         }
    //                     } else {
    //                         Err(format_err!("malformed path {:?}", path))
    //                     }
    //                 } else {
    //                     Err(format_err!("bogus root directory"))
    //                 }
    //             } else {
    //                 Err(format_err!("malformed path {:?}", path))
    //             }
    //         } else {
    //             // This is a request for a "regular" file, not a "versioned" file.
    //             if let Some(file_name) = path.file_name() {
    //                 if let Some(root) = path.parent() {
    //                     let mut iter = root.components();
    //                     if let Some(dir_root) = self.get_directory_metadata(root, dirty, &mut iter)
    //                     {
    //                         if let Some(DirectoryEntry::File(file)) =
    //                             dir_root.entries.get(file_name.to_str().unwrap())
    //                         {
    //                             let latest = file.get_latest();
    //                             debug!("\treturning file {:#?}", latest);
    //                             Ok(latest)
    //                         } else {
    //                             Err(format_err!("can't find file: {:?}", path))
    //                         }
    //                     } else {
    //                         Err(format_err!("bogus root directory"))
    //                     }
    //                 } else {
    //                     Err(format_err!("malformed path {:?}", path))
    //                 }
    //             } else {
    //                 Err(format_err!("malformed path {:?}", path))
    //             }
    //         }
    //     } else {
    //         Err(format_err!(""))
    //     }
    // }

    // /// Retrieve a file by name from this directory
    // pub(crate) fn get_file_read_only<P>(&mut self, path: P) -> Result<File, failure::Error>
    // where
    //     P: AsRef<Path>,
    // {
    //     let path = path.as_ref();
    //     debug!("--------");
    //     debug!("`get_file_read_only`: {:?}", path);
    //     let file_version = self.get_file(&path, false)?;

    //     debug!("\treturning file {:#?}", file_version);

    //     Ok(File {
    //         path: path.to_path_buf(),
    //         file_id: file_version.file_id().clone(),
    //         version: file_version,
    //     })
    // }

    // /// Retrieve a file by name from this directory
    // pub(crate) fn get_file_read_write<P>(&mut self, path: P) -> Result<File, failure::Error>
    // where
    //     P: AsRef<Path>,
    // {
    //     let path = path.as_ref();
    //     debug!("--------");
    //     debug!("`get_file_read_write`: {:?}", path);
    //     let file_version = self.get_file(&path, true)?;

    //     debug!("\treturning file {:#?}", file_version);

    //     Ok(File {
    //         path: path.to_path_buf(),
    //         file_id: file_version.file_id().clone(),
    //         version: file_version,
    //     })
    // }

    // /// Retrieve a file by name from this directory
    // pub(crate) fn get_file_write_only<P>(&mut self, path: P) -> Result<File, failure::Error>
    // where
    //     P: AsRef<Path>,
    // {
    //     let path = path.as_ref();
    //     debug!("--------");
    //     debug!("`get_file_write_only`: {:?}", path);
    //     let file_version = self.get_file(&path, true)?;
    //     let new_version = file_version.new_sibling();

    //     debug!("\treturning file {:#?}", new_version);

    //     Ok(File {
    //         path: path.to_path_buf(),
    //         file_id: new_version.file_id().clone(),
    //         version: new_version,
    //     })
    // }

    // /// Commit changes to a file under this directory
    // ///
    // /// The current version is committed, and written if necessary.
    // pub(crate) fn commit_file(&mut self, file: File) -> Result<(), failure::Error> {
    //     debug!("--------");
    //     debug!("`commit_file`: {:#?}", file);
    //     if file.version.is_dirty() {
    //         if let Some(file_name) = file.path.file_name() {
    //             if let Some(root) = file.path.parent() {
    //                 let mut iter = root.components();
    //                 if let Some(dir_root) = self.get_directory_metadata(root, false, &mut iter) {
    //                     if let Some(name) = file_name.to_str() {
    //                         if let Some(ref mut entry) = dir_root.entries.get_mut(name) {
    //                             match entry {
    //                                 DirectoryEntry::File(ref mut my_file) => {
    //                                     dir_root.dirty = true;
    //                                     my_file.commit_version(file.version.clone());
    //                                     return Ok(());
    //                                 }
    //                                 _ => unreachable!(),
    //                             }
    //                         } else {
    //                             return Err(format_err!(
    //                                 "`commit_file` can't find file {:?}",
    //                                 file.path
    //                             ));
    //                         }
    //                     } else {
    //                         return Err(format_err!(
    //                             "`commit_file` malformed file name {:?}",
    //                             file_name
    //                         ));
    //                     }
    //                 } else {
    //                     return Err(format_err!("`commit_file` bogus root directory"));
    //                 }
    //             } else {
    //                 return Err(format_err!("`commit_file` malformed path"));
    //             }
    //         } else {
    //             return Err(format_err!("`commit_file` malformed path"));
    //         }
    //     } else {
    //         return Ok(());
    //     }
    // }

    /// Return a HashMap from entry name to DirectoryEntry structures
    pub(crate) fn entries(&self) -> &HashMap<String, DirectoryEntry> {
        &self.entries
    }

    /// Return the UUID
    pub(crate) fn id(&self) -> UfsUuid {
        self.id
    }

    /// Return the `write_time` timestamp
    pub(crate) fn write_time(&self) -> UfsTime {
        self.write_time
    }

    /// Return true if the directory needs to be serialized
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Set to serialize directory
    pub(crate) fn dirty(&mut self) {
        self.dirty = true;
    }

    pub(in crate::metadata) fn lookup_dir(&self, id: UfsUuid) -> Option<&DirectoryMetadata> {
        debug!("--------");
        debug!("`lookup_dir`: {:#?}, parent {:#?}", self.id, self.parent_id);

        for e in self.entries.values() {
            if let DirectoryEntry::Directory(d) = e {
                if d.id == id {
                    return Some(d);
                } else {
                    if let Some(d) = DirectoryMetadata::lookup_dir(d, id) {
                        return Some(d);
                    }
                }
            }
        }

        None
    }

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
            return Some(self);
        } else {
            for e in self.entries.values_mut() {
                if let DirectoryEntry::Directory(ref mut d) = e {
                    if let Some(d) = DirectoryMetadata::lookup_dir_mut(d, id) {
                        d.dirty = true;
                        return Some(d);
                    }
                }
            }
        }
        None
    }

    pub(in crate::metadata) fn lookup_file(&self, id: UfsUuid) -> Option<&FileMetadata> {
        debug!("--------");
        debug!(
            "`lookup_file_mut`: {:#?}, parent {:#?}",
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

    pub(in crate::metadata) fn lookup_file_mut(
        &mut self,
        id: UfsUuid,
    ) -> Option<&mut FileMetadata> {
        debug!("--------");
        debug!(
            "`lookup_file_mut`: {:#?}, parent {:#?}",
            self.id, self.parent_id
        );

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

    // pub(in crate::metadata) fn lookup(&self, id: UfsUuid) -> Option<&DirectoryEntry> {
    //     debug!("--------");
    //     debug!("`lookup_dir`: {:#?}", self);

    //     if self.id == id {
    //         return Some(self);
    //     } else {
    //         // Do a "stupid" search for the given ID
    //         for e in self.entries.values() {
    //             match e {
    //                 DirectoryEntry::File(f) => {
    //                     if f.file_id() == id {
    //                         return Some(e);
    //                     }
    //                 }
    //                 DirectoryEntry::Directory(d) => {
    //                     return DirectoryMetadata::lookup_dir(d, id);
    //                 }
    //             }
    //         }
    //     }

    //     None
    // }
}
