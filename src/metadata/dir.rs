//! Metadata for Directories
//!
//! This struct stores all the various necessary time stamps, as well as a map of files (and
//! other directories) that it contains. These are stored as [`DirectoryEntry`] structures.
//!
//! FIXME: The directory data is not versioned. What happens to deleted files?  What do we do when
//! a directory goes away?
use failure::format_err;
use log::{debug, error, warn};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ffi::OsStr,
    path::{Component, Components, Path},
};

#[cfg(not(target_arch = "wasm32"))]
use crate::{
    block::wrapper::{MetadataDeserialize, MetadataSerialize},
    time::UfsTime,
    uuid::UfsUuid,
};

pub(crate) const WASM_DIR: &'static str = ".wasm";
pub(crate) const WASM_EXT: &'static str = "wasm";
pub(crate) const VERS_DIR: &'static str = ".vers";

use super::{Directory, DirectoryEntry, File, FileMetadata, FileVersion};

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DirectoryMetadata {
    /// A flag indicating that the directory's data has been modified and needs to be written.
    ///
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
    /// HashMap of directory contents, from name to `DirectoryEntry`
    entries: HashMap<String, DirectoryEntry>,
}

#[cfg(not(target_arch = "wasm32"))]
impl DirectoryMetadata {
    pub(crate) fn new() -> Self {
        let time = UfsTime::now();
        let mut d = DirectoryMetadata {
            dirty: true,
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
                birth_time: time,
                write_time: time,
                change_time: time,
                access_time: time,
                entries: HashMap::new(),
            }),
        );
        d
    }

    fn get_directory_metadata<'a>(
        &mut self,
        path: &Path,
        mark_dirty: bool,
        mut components: &mut Components<'a>,
    ) -> Option<&mut DirectoryMetadata> {
        debug!("--------");
        debug!(
            "`get_directory_metadata`: path: {:?}, components {:?}",
            path, components
        );
        match components.next() {
            Some(Component::RootDir) => {
                if mark_dirty {
                    self.dirty = true;
                }
                self.get_directory_metadata(path, mark_dirty, &mut components)
            }
            Some(Component::Normal(name)) => match name.to_str() {
                Some(name) => match self.entries.get_mut(name) {
                    Some(DirectoryEntry::Directory(sub_dir)) => {
                        if mark_dirty {
                            sub_dir.dirty = true;
                        }
                        DirectoryMetadata::get_directory_metadata(
                            sub_dir,
                            path,
                            mark_dirty,
                            &mut components,
                        )
                    }
                    _ => {
                        warn!("`get_directory_metadata`: couldn't find {:?}", path);
                        None
                    }
                },
                _ => {
                    error!("`get_directory_metadata`: invalid utf-8 in path {:?}", path);
                    None
                }
            },
            None => Some(self),
            _ => {
                error!("`get_directory_metadata`: wonky path: {:?}", path);
                None
            }
        }
    }

    /// Create a new directory in this directory
    pub(crate) fn new_directory<P>(&mut self, path: P) -> Result<Directory, failure::Error>
    where
        P: AsRef<Path>,
    {
        debug!("--------");
        debug!("`new_directory`: {:?}", path.as_ref());
        if let Some(dir_name) = path.as_ref().file_name() {
            if let Some(root) = path.as_ref().parent() {
                let mut iter = root.components();
                if let Some(dir_root) = self.get_directory_metadata(root, true, &mut iter) {
                    let dir = DirectoryMetadata::new();

                    dir_root.entries.insert(
                        dir_name.to_str().unwrap().to_owned(),
                        DirectoryEntry::Directory(dir.clone()),
                    );

                    debug!("\treturning: {:#?}", dir);
                    return Ok(Directory {
                        path: path.as_ref().to_owned(),
                        directory: dir,
                    });
                }
            }
        }
        Err(format_err!(
            "`new_directory` could not create directory {:?}",
            path.as_ref()
        ))
    }

    /// Create a new file in this directory
    pub(crate) fn new_file<P>(&mut self, fs_id: &UfsUuid, path: P) -> Result<File, failure::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        debug!("--------");
        debug!("`new_file`: {:?}", path);
        if let Some(file_name) = path.file_name() {
            if let Some(root) = path.parent() {
                let mut iter = root.components();
                if let Some(dir_root) = self.get_directory_metadata(root, true, &mut iter) {
                    let file = FileMetadata::new(fs_id.new(path.to_str().unwrap()));

                    dir_root.entries.insert(
                        file_name.to_str().unwrap().to_owned(),
                        DirectoryEntry::File(file.clone()),
                    );

                    debug!("\treturning {:#?}", file);
                    Ok(File {
                        path: path.to_owned(),
                        version: file.get_latest(),
                    })
                } else {
                    Err(format_err!("bogus root directory"))
                }
            } else {
                Err(format_err!("malformed path"))
            }
        } else {
            Err(format_err!("malformed path"))
        }
    }

    /// Retrieve a directory by name, from this directory
    pub(crate) fn get_directory<P>(&mut self, path: P) -> Result<Directory, failure::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        debug!("--------");
        debug!("`get_directory`: {:?}", path);
        //
        // Populate the special `.vers` directory with existing file versions.
        let mut vers = false;
        let mut files = HashMap::<String, DirectoryEntry>::new();
        if path.file_name() == Some(OsStr::new(VERS_DIR)) {
            vers = true;
            if let Some(parent_path) = path.parent() {
                let mut iter = parent_path.components();
                if let Some(parent_dir) = self.get_directory_metadata(parent_path, false, &mut iter)
                {
                    for (name, entry) in &parent_dir.entries {
                        if let DirectoryEntry::File(file) = entry {
                            for (n, _) in file.get_versions().iter().enumerate() {
                                let mut name = name.clone();
                                name.push('@');
                                name.push_str(&n.to_string());
                                if let Some(file) = file.version_at(n) {
                                    files.insert(
                                        name,
                                        DirectoryEntry::File(file.into_file_metadata()),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut iter = path.components();
        if let Some(dir) = self.get_directory_metadata(path, false, &mut iter) {
            let mut dir = dir.clone();
            if vers {
                dir.entries = files;
            }
            Ok(Directory {
                path: path.to_owned(),
                directory: dir,
            })
        } else {
            Err(format_err!("`get_directory` malformed path"))
        }
    }

    fn get_file<P>(&mut self, path: P, dirty: bool) -> Result<FileVersion, failure::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        debug!("--------");
        debug!("`get_file`: {:?}", path);
        // Check to see if the file we are opening is in the "versions" subdirectory. If so,
        // populate a `FileMetadata` with the requested file version.
        if let Some(root_path) = path.parent() {
            if root_path.file_name() == Some(OsStr::new(VERS_DIR)) {
                debug!("\tworking in {} directory", VERS_DIR);
                if let Some(root_path) = root_path.parent() {
                    let mut iter = root_path.components();
                    if let Some(dir_root) = self.get_directory_metadata(root_path, dirty, &mut iter)
                    {
                        if let Some(versioned_file_name) = path.file_name() {
                            // FIXME: This will break if there are @'s in the file name
                            if let Some(vfn_str) = versioned_file_name.to_str() {
                                let mut i = vfn_str.split(|c| c == '@');
                                if let Some(file_name) = i.next() {
                                    if let Some(v_str) = i.next() {
                                        if let Ok(version) = v_str.parse::<usize>() {
                                            if let Some(DirectoryEntry::File(file)) =
                                                dir_root.entries.get(file_name)
                                            {
                                                if let Some(file_version) = file.version_at(version)
                                                {
                                                    debug!("\treturning file {:#?}", file_version);
                                                    Ok(file_version)
                                                } else {
                                                    Err(format_err!(
                                                        "can't find version {} for {:?}",
                                                        version,
                                                        path
                                                    ))
                                                }
                                            } else {
                                                Err(format_err!(
                                                    "can't find {} in directory {:?}",
                                                    file_name,
                                                    root_path
                                                ))
                                            }
                                        } else {
                                            Err(format_err!(
                                                "can't parse version number {:?}",
                                                path
                                            ))
                                        }
                                    } else {
                                        Err(format_err!("file name missing version {:?}", path))
                                    }
                                } else {
                                    Err(format_err!("malformed versioned file {:?}", path))
                                }
                            } else {
                                Err(format_err!("malformed file name {:?}", versioned_file_name))
                            }
                        } else {
                            Err(format_err!("malformed path {:?}", path))
                        }
                    } else {
                        Err(format_err!("bogus root directory"))
                    }
                } else {
                    Err(format_err!("malformed path {:?}", path))
                }
            } else {
                // This is a request for a "regular" file, not a "versioned" file.
                if let Some(file_name) = path.file_name() {
                    if let Some(root) = path.parent() {
                        let mut iter = root.components();
                        if let Some(dir_root) = self.get_directory_metadata(root, dirty, &mut iter)
                        {
                            if let Some(DirectoryEntry::File(file)) =
                                dir_root.entries.get(file_name.to_str().unwrap())
                            {
                                let latest = file.get_latest();
                                debug!("\treturning file {:#?}", latest);
                                Ok(latest)
                            } else {
                                Err(format_err!("can't find file: {:?}", path))
                            }
                        } else {
                            Err(format_err!("bogus root directory"))
                        }
                    } else {
                        Err(format_err!("malformed path {:?}", path))
                    }
                } else {
                    Err(format_err!("malformed path {:?}", path))
                }
            }
        } else {
            Err(format_err!(""))
        }
    }

    /// Retrieve a file by name from this directory
    pub(crate) fn get_file_read_only<P>(&mut self, path: P) -> Result<File, failure::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        debug!("--------");
        debug!("`get_file_read_only`: {:?}", path);
        let file_version = self.get_file(&path, false)?;

        debug!("\treturning file {:#?}", file_version);

        Ok(File {
            path: path.to_path_buf(),
            version: file_version,
        })
    }

    /// Retrieve a file by name from this directory
    pub(crate) fn get_file_read_write<P>(&mut self, path: P) -> Result<File, failure::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        debug!("--------");
        debug!("`get_file_read_write`: {:?}", path);
        let file_version = self.get_file(&path, true)?;

        debug!("\treturning file {:#?}", file_version);

        Ok(File {
            path: path.to_path_buf(),
            version: file_version,
        })
    }

    /// Retrieve a file by name from this directory
    pub(crate) fn get_file_write_only<P>(&mut self, path: P) -> Result<File, failure::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        debug!("--------");
        debug!("`get_file_write_only`: {:?}", path);
        let file_version = self.get_file(&path, true)?;
        let new_version = file_version.new_sibling();

        debug!("\treturning file {:#?}", new_version);

        Ok(File {
            path: path.to_path_buf(),
            version: new_version,
        })
    }

    /// Commit changes to a file under this directory
    ///
    /// The current version is committed, and written if necessary.
    pub(crate) fn commit_file(&mut self, file: File) -> Result<(), failure::Error> {
        debug!("--------");
        debug!("`commit_file`: {:#?}", file);
        if file.version.is_dirty() {
            if let Some(file_name) = file.path.file_name() {
                if let Some(root) = file.path.parent() {
                    let mut iter = root.components();
                    if let Some(dir_root) = self.get_directory_metadata(root, false, &mut iter) {
                        if let Some(name) = file_name.to_str() {
                            if let Some(ref mut entry) = dir_root.entries.get_mut(name) {
                                match entry {
                                    DirectoryEntry::File(ref mut my_file) => {
                                        dir_root.dirty = true;
                                        my_file.commit_version(file.version.clone());
                                        return Ok(());
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                return Err(format_err!(
                                    "`commit_file` can't find file {:?}",
                                    file.path
                                ));
                            }
                        } else {
                            return Err(format_err!(
                                "`commit_file` malformed file name {:?}",
                                file_name
                            ));
                        }
                    } else {
                        return Err(format_err!("`commit_file` bogus root directory"));
                    }
                } else {
                    return Err(format_err!("`commit_file` malformed path"));
                }
            } else {
                return Err(format_err!("`commit_file` malformed path"));
            }
        } else {
            return Ok(());
        }
    }

    /// Return a HashMap from entry name to DirectoryEntry structures
    pub(crate) fn entries(&self) -> &HashMap<String, DirectoryEntry> {
        &self.entries
    }

    /// Return the `write_time` timestamp
    pub(crate) fn write_time(&self) -> UfsTime {
        self.write_time
    }

    /// Return true if the directory needs to be serialized
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MetadataSerialize for DirectoryMetadata {
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
impl MetadataDeserialize for DirectoryMetadata {
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
