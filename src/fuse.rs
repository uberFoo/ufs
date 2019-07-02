#![cfg(not(target_arch = "wasm32"))]
//! FUSE Interface for uberFS
//!
use std::{collections::HashMap, ffi::OsStr, path::PathBuf};

use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};
use libc::{c_int, ENOENT, O_RDONLY, O_RDWR, O_WRONLY};
use log::{debug, error, trace, warn};
use time::Timespec;

use crate::{
    block::BlockStorage, metadata::DirectoryEntry, uuid::UfsUuid, OpenFileMode, UfsMounter,
};

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };
const TIME: Timespec = Timespec {
    sec: 10634562,
    nsec: 0,
};

#[derive(Debug)]
enum Inode {
    Dir(DirInode),
    File(FileInode),
}

impl Inode {
    fn file_attr(&self) -> FileAttr {
        match self {
            Inode::Dir(i) => i.file_attr(),
            Inode::File(i) => i.file_attr(),
        }
    }
}

/// FIXME: try getting rid of path
#[derive(Debug)]
struct DirInode {
    number: u64,
    path: PathBuf,
    id: UfsUuid,
    time: Timespec,
    files: HashMap<String, u64>,
}

impl DirInode {
    fn file_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.number,
            size: 0,
            blocks: 0,
            atime: self.time,
            mtime: self.time,
            ctime: self.time,
            crtime: self.time,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
        }
    }
}

#[derive(Debug)]
struct FileInode {
    number: u64,
    path: PathBuf,
    id: UfsUuid,
    time: Timespec,
    size: u64,
}

impl FileInode {
    fn file_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.number,
            size: self.size,
            blocks: 1,
            atime: self.time,
            mtime: self.time,
            ctime: self.time,
            crtime: self.time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
        }
    }
}

/// FUSE integration
///
pub struct UberFSFuse<B: BlockStorage + 'static> {
    file_system: UfsMounter<B>,
    // `inodes` is a mapping from "inode" number to an Inode
    inodes: Vec<Inode>,
}

impl<B: BlockStorage> UberFSFuse<B> {
    /// Create a new file system
    ///
    pub fn new(file_system: UfsMounter<B>) -> Self {
        let mut fs = UberFSFuse {
            file_system,
            inodes: Vec::new(),
        };

        {
            let guard = fs.file_system.lock().expect("poisoned ufs lock");
            let root_id = guard.get_root_directory_id();
            // The first inode is always the root of the file system.  The zeroith is well, a hack.
            fs.inodes.push(Inode::Dir(DirInode {
                number: 0,
                path: PathBuf::from("hack"),
                id: UfsUuid::new_root("hack"),
                time: TIME,
                files: HashMap::new(),
            }));
            fs.inodes.push(Inode::Dir(DirInode {
                number: 1,
                id: root_id,
                path: PathBuf::from("/"),
                time: TIME,
                files: HashMap::new(),
            }));
        }

        fs
    }

    // fn file_system(&self) ->
}

/// Talking nice with the kernel...
///
/// When mounted, the following methods are invoked (in order).  Note that with the exception  of
/// `init`, on startup all methods are invoked on `inode` 1.
///  * `init`
///  * `getattr`
///  * `statfs`
///  * `access` (mask 0b000)
///
/// `cat`ing a file requires the following, in order:
///  * `open`
///  * `read`
///  * `flush`
///  * `release`
///
impl<B: BlockStorage> Filesystem for UberFSFuse<B> {
    /// Start-up
    ///
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        Ok(())
    }

    /// Shutdown
    ///
    fn destroy(&mut self, _req: &Request) {
        self.file_system.shutdown().unwrap();
    }

    /// Return inode attributes
    ///
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match self.inodes.get(ino as usize) {
            Some(inode) => {
                trace!("getattr {:#?}", inode);
                reply.attr(&TTL, &inode.file_attr())
            }
            None => {
                error!("`getattr` can't find requested inode {}", ino);
                reply.error(ENOENT)
            }
        };
    }

    // Return a directory entry given a name and parent inode
    // parent is the parent directory inode
    // The mapping from file name -> inode number is stored in the parent inode.
    // File name is relative to the parent inode.
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        trace!("--------");
        trace!("`lookup`: parent: {}, name: {:?}", parent, name);

        if let Some(Inode::Dir(dir_ino)) = self.inodes.get(parent as usize) {
            if let Some(name) = name.to_str() {
                if let Some(index) = dir_ino.files.get(name) {
                    if let Some(inode) = self.inodes.get(*index as usize) {
                        reply.entry(&TTL, &inode.file_attr(), 0);
                        return;
                    }
                }
            }

            trace!("can't find ({:?}) under parent ({})", name, parent);
            reply.error(ENOENT);
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<Timespec>,
        _mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        trace!("setattr inode: {}\nmode: {:x?}, flags: {:#x?}, uid: {:?}, gid: {:?}, size: {:?}, atime: {:?}, mtime: {:?}, fh: {:?}, crtime: {:?}, chgtime: {:?}, bkuptime: {:?}",_ino, _mode, _flags, _uid, _gid, _size, _atime, _mtime, _fh, _crtime, _chgtime, _bkuptime);

        self.getattr(_req, _ino, reply);
    }

    /// Open a directory
    fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: ReplyOpen) {
        debug!("--------");
        debug!("`opendir`: ino: {}, flags: {:x}", ino, _flags);
        if let Some(Inode::Dir(inode)) = self.inodes.get(ino as usize) {
            let path = PathBuf::from(&inode.path);
            debug!("\tpath: {:?}", path);

            let mut number = (self.inodes.len() - 1) as u64;
            let mut inodes = vec![];

            let mut dir_file_map = if let Some(Inode::Dir(dir_ino)) = self.inodes.get(ino as usize)
            {
                dir_ino.files.clone()
            } else {
                panic!("opendir called with something not a directory");
            };
            let mut guard = self.file_system.lock().expect("poisoned ufs lock");
            match &mut guard.open_directory(inode.id) {
                Ok(fh) => {
                    debug!("handle: {}", fh);
                    // Get the files for this directory (file handle) from the BlockManager.
                    // We are returned a HashMap of file name -> DirectoryEntry.
                    // Iterate over the map, updating our INode structures.
                    // If an entry in the map is not already in our file name -> inode hashmap,
                    // then add it.  Otherwise, update the inode with any changes.
                    if let Some(file_map) = guard.list_files(*fh) {
                        for (name, entry) in file_map {
                            match entry {
                                DirectoryEntry::Directory(d) => {
                                    let inode_n =
                                        dir_file_map.entry(name.clone()).or_insert_with(|| {
                                            number += 1;
                                            let mut dir_path = path.clone();
                                            dir_path.push(name);
                                            debug!(
                                                "\tadding directory: {:?}, ino: {}",
                                                dir_path, number
                                            );
                                            let inode = DirInode {
                                                number,
                                                path: dir_path,
                                                id: d.id().clone(),
                                                time: d.write_time().into(),
                                                files: HashMap::new(),
                                            };
                                            inodes.push(Inode::Dir(inode));
                                            number
                                        });

                                    // I'd rather only run this if we didn't insert above.
                                    if let Some(Inode::Dir(ino)) =
                                        self.inodes.get_mut(*inode_n as usize)
                                    {
                                        ino.time = d.write_time().into();
                                    }
                                }
                                DirectoryEntry::File(f) => {
                                    let file = f.get_latest();
                                    let inode_n =
                                        dir_file_map.entry(name.clone()).or_insert_with(|| {
                                            let mut file_path = path.clone();
                                            file_path.push(name);
                                            number += 1;
                                            debug!(
                                                "\tadding file {:?}, size: {}, time: {:?}, ino: {}",
                                                file_path,
                                                file.size(),
                                                file.write_time(),
                                                number
                                            );
                                            let inode = FileInode {
                                                number,
                                                path: file_path,
                                                id: file.file_id().clone(),
                                                time: file.write_time().into(),
                                                size: file.size(),
                                            };
                                            inodes.push(Inode::File(inode));
                                            number
                                        });

                                    // I'd rather only run this if we didn't insert above.
                                    if let Some(Inode::File(ino)) =
                                        self.inodes.get_mut(*inode_n as usize)
                                    {
                                        debug!(
                                            "\tupdating file {:?}, size: {}, time: {:?}, ino: {}",
                                            name,
                                            file.size(),
                                            file.write_time(),
                                            *inode_n
                                        );
                                        ino.time = file.write_time().into();
                                        ino.size = file.size();
                                    }
                                }
                            };
                        }

                        self.inodes.append(&mut inodes);
                        // Update the directory's file map.
                        if let Some(Inode::Dir(ref mut dir_ino)) = self.inodes.get_mut(ino as usize)
                        {
                            dir_ino.files = dir_file_map
                        }

                        reply.opened(*fh as u64, 0);
                    }
                }
                Err(e) => {
                    warn!("\tcouldn't open directory {:?}: {}", path, e);
                    reply.error(ENOENT)
                }
            }
        // }
        } else {
            warn!("\tcan't find inode {}", ino);
            reply.error(ENOENT);
        }
    }

    /// Return files in a directory
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("--------");
        debug!("`readdir`: ino: {}, fh: {}, offset: {}", ino, fh, offset);

        if let Some(Inode::Dir(dir_ino)) = self.inodes.get(ino as usize) {
            for (i, (name, index)) in dir_ino.files.iter().enumerate().skip(offset as usize) {
                if let Some(inode) = self.inodes.get(*index as usize) {
                    match inode {
                        Inode::Dir(dir) => {
                            trace!(
                                "adding to reply: inode {}, offset {}, Directory, name {}",
                                dir.number,
                                i + 1,
                                name
                            );
                            // i + 1 means the index of the next entry
                            reply.add(dir.number, (i + 1) as i64, FileType::Directory, name);
                        }
                        Inode::File(file) => {
                            trace!(
                                "adding to reply: inode {}, offset {}, File, name {}",
                                file.number,
                                i + 1,
                                name
                            );
                            // i + 1 means the index of the next entry
                            reply.add(file.number, (i + 1) as i64, FileType::RegularFile, name);
                        }
                    }
                } else {
                    warn!("\t can't find inode {}", index);
                    reply.error(ENOENT);
                    return;
                }
            }
            reply.ok();
        } else {
            warn!("`readdir`: can't find inode {}", ino);
            reply.error(ENOENT);
        }
    }

    /// Close an opened directory
    fn releasedir(&mut self, _req: &Request, ino: u64, fh: u64, flags: u32, reply: ReplyEmpty) {
        debug!("--------");
        debug!("`releasedir` ino: {}, fh: {}, flags: {:#x}", ino, fh, flags);

        let mut guard = self.file_system.lock().expect("poisoned ufs lock");
        &mut guard.close_directory(fh);
        reply.ok();
    }

    // // Open a file
    // fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
    //     debug!("open ino: {}, flags {:x}", ino, flags);

    //     if let Some(Inode::File(inode)) = self.inodes.get_mut(ino as usize) {
    //         let open_flags = flags as i32;
    //         let mode = match open_flags {
    //             O_RDONLY => OpenFileMode::Read,
    //             O_WRONLY => {
    //                 inode.size = 0;
    //                 OpenFileMode::Write
    //             }
    //             O_RDWR => OpenFileMode::ReadWrite,
    //             _ => unreachable!(),
    //         };

    //         let mut guard = self.file_system.lock().expect("poisoned ufs lock");
    //         match &mut guard.open_file(inode.path.as_path(), mode) {
    //             Ok(fh) => reply.opened(*fh as u64, 0),
    //             _ => reply.error(ENOENT),
    //         }
    //     } else {
    //         warn!("\tcan't find inode {}", ino);
    //         reply.error(ENOENT);
    //     }
    // }

    // Make a new directory
    // There's something very bogus about this function: it doesn't allow for returning a file
    // handle like it's "sibling", create, below.
    // parent is the inode of the parent directory
    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        debug!("--------");
        debug!(
            "`mkdir`: {:?}, parent: {}, mode: {:#05o}",
            name, parent, _mode
        );
        let new_inode_number = self.inodes.len() as u64;

        if let Some(Inode::Dir(parent_ino)) = self.inodes.get_mut(parent as usize) {
            let name = String::from(name.to_str().unwrap());
            let mut path = parent_ino.path.clone();
            path.push(&name);

            let mut guard = self.file_system.lock().expect("poisoned ufs lock");
            let inode = match &mut guard.create_directory(parent_ino.id, &name) {
                Ok(dir) => {
                    let inode = DirInode {
                        path: path,
                        id: dir.id().clone(),
                        number: new_inode_number,
                        time: TIME,
                        files: HashMap::new(),
                    };

                    reply.entry(&TTL, &inode.file_attr(), 0);

                    parent_ino.files.insert(name, new_inode_number);
                    Some(inode)
                }
                Err(e) => {
                    error!("Unable to create directory {}: {}", name, e);
                    None
                }
            };

            if let Some(inode) = inode {
                self.inodes.push(Inode::Dir(inode));
            }
        } else {
            warn!("\tcan't find parent inode {}", parent);
            reply.error(ENOENT);
        }
    }

    // Create and open a file
    // parent is the inode of the parent directory
    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        flags: u32,
        reply: ReplyCreate,
    ) {
        debug!("--------");
        debug!(
            "`create`: {:?}, parent: {}, mode: {:#05o}, flags: {:#x}",
            name, parent, _mode, flags
        );
        let new_inode_number = self.inodes.len() as u64;

        if let Some(Inode::Dir(ref mut parent_ino)) = self.inodes.get_mut(parent as usize) {
            let name = String::from(name.to_str().unwrap());
            let mut path = parent_ino.path.clone();
            path.push(&name);

            let mut guard = self.file_system.lock().expect("poisoned ufs lock");
            let inode = match &mut guard.create_file(parent_ino.id, &name) {
                Ok((fh, file)) => {
                    let inode = FileInode {
                        path: path,
                        id: file.file_id.clone(),
                        number: new_inode_number,
                        time: file.version.write_time().into(),
                        size: 0,
                    };
                    debug!("inode: {}", inode.number);

                    reply.created(&TTL, &inode.file_attr(), 0, *fh, flags);

                    parent_ino.files.insert(name, new_inode_number);
                    Some(inode)
                }
                Err(e) => {
                    error!("Unable to create file {}: {}", name, e);
                    None
                }
            };

            if let Some(inode) = inode {
                self.inodes.push(Inode::File(inode));
            }
        } else {
            warn!("\tcan't find parent inode {}", parent);
            reply.error(ENOENT);
        }
    }

    // // Remove a file from the file system
    // fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
    //     debug!("--------");
    //     debug!("`unlink`: {:?}, parent: {}", name, parent);

    //     if let Some(Inode::Dir(parent_ino)) = self.inodes.get_mut(parent as usize) {
    //         let name = name.to_str().unwrap();
    //         let mut path = parent_ino.path.clone();
    //         path.push(&name);

    //         let mut guard = self.file_system.lock().expect("poisoned ufs lock");
    //         match guard.remove_file(&path) {
    //             Ok(_) => reply.ok(),
    //             Err(e) => {
    //                 error!("unlinking file {}", e);
    //                 reply.error(ENOENT);
    //             }
    //         }
    //     } else {
    //         warn!("can't find parent inode {}", parent);
    //         reply.error(ENOENT);
    //     }
    // }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        flags: u32,
        _lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("--------");
        debug!(
            "`release`: ino: {}, fh: {}, flags: {:#x}, flush: {}",
            ino, fh, flags, flush
        );

        let mut guard = self.file_system.lock().expect("poisoned ufs lock");
        &mut guard.close_file(fh);
        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        debug!(
            "read ino: {}, offset: {}, chunk size: {}",
            ino, offset, size
        );

        let guard = self.file_system.lock().expect("poisoned ufs lock");
        if let Ok(buffer) = &mut guard.read_file(fh, offset, size as usize) {
            debug!("read {} bytes", buffer.len());
            trace!("{:?}", &buffer);
            reply.data(&buffer)
        } else {
            reply.error(ENOENT)
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        debug!(
            "write ino: {}, offset: {}, data.len() {}",
            ino,
            offset,
            data.len()
        );

        if let Some(Inode::File(inode)) = self.inodes.get_mut(ino as usize) {
            let mut guard = self.file_system.lock().expect("poisoned ufs lock");
            if let Ok(len) = &mut guard.write_file(fh, data) {
                debug!("wrote {} bytes", len);
                trace!("{:?}", &data[..*len]);

                inode.size = inode.size + *len as u64;

                reply.written(*len as u32);
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    /// Return File System Statistics
    ///
    /// Given an inode, return statistics in the `ReplyStatfs` struct, which is poorly documented.
    /// Therefore, best as I can determine (`man statvfs`, and `man statfs` ):
    /// ``` ignore
    /// ReplyStatfs::statfs(
    ///     blocks: u64,    // total blocks in the file system
    ///     bfree: u64,     // free blocks in the file system
    ///     bavail: u64,    // free blocks available to non-superuser
    ///     files: u64,     // total number of file nodes in the file system
    ///     ffree: u64,     // number of free file nodes in the file system
    ///     bsize: u32,     // preferred length of an I/O request
    ///     namelen: u32,   // maximum file name length, in bytes
    ///     frsize: u32     // minimum allocation unit, in bytes, i.e., block size
    /// )
    /// ```
    ///
    /// # Questions:
    /// * Why pass an inode?  The libfuse `passthrough_ll.c` impl uses the inode to lookup a file
    /// descriptor so that it may then call `fstatvfs`.
    ///
    /// FIXME: What to do about maximum file name length?
    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        trace!("statfs ino {}", _ino);
        let guard = self.file_system.lock().expect("poisoned ufs lock");
        let block_manager = &guard.block_manager();
        trace!(
            "blocks: {}, free blocks: {}, block size: {}",
            block_manager.block_count(),
            block_manager.free_block_count(),
            block_manager.block_size()
        );
        reply.statfs(
            block_manager.block_count(),
            block_manager.free_block_count(),
            block_manager.free_block_count(),
            // I'm using i64 below, because it's consistent with what I'm seeing from APFS.
            i64::max_value() as u64,
            // i64::max_value() as u64 - self.files.len() as u64,
            i64::max_value() as u64,
            block_manager.block_size() as u32, // I'd had 2048 hardcoded here once...
            0xff,
            block_manager.block_size() as u32,
        );
    }
}
