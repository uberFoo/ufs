//! FUSE Interface for uberFS
//!
use std::{collections::HashMap, ffi::OsStr, path::PathBuf};

use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};
use libc::{c_int, ENOENT, O_RDONLY, O_RDWR, O_WRONLY};
use log::{debug, error, trace};
use time::Timespec;

use crate::{
    block::{BlockCardinality, FileStore},
    OpenFileMode, UberFileSystem,
};

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };
const TIME: Timespec = Timespec {
    sec: 10634562,
    nsec: 0,
};

#[derive(Debug)]
struct Inode {
    number: u64,
    name: String,
    time: Timespec,
    size: Option<u64>,
}

impl Inode {
    fn kind(&self) -> FileType {
        // Ugly, but ok for first attempt.
        match self.number {
            1 => FileType::Directory,
            _ => FileType::RegularFile,
        }
    }

    fn file_attr(&self) -> FileAttr {
        let kind = self.kind();

        match self.size {
            Some(s) => FileAttr {
                ino: self.number,
                size: s,
                blocks: 1,
                atime: self.time,
                mtime: self.time,
                ctime: self.time,
                crtime: self.time,
                kind,
                perm: 0o644,
                nlink: 1,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
            },
            None => FileAttr {
                ino: self.number,
                size: 0,
                blocks: 0,
                atime: self.time,
                mtime: self.time,
                ctime: self.time,
                crtime: self.time,
                kind,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
            },
        }
    }
}

/// FUSE intergation
///
pub struct UberFSFuse {
    file_system: UberFileSystem<FileStore>,
    // `inodes` is a mapping from "inode" number to an Inode
    inodes: Vec<Inode>,
    // `files` is a mapping from a file name to an Inode *index* in the `inodes` vector.
    files: HashMap<String, BlockCardinality>,
}

impl UberFSFuse {
    /// Create a new file system
    ///
    pub fn new(file_system: UberFileSystem<FileStore>) -> Self {
        let mut fs = UberFSFuse {
            file_system,
            inodes: Vec::new(),
            files: HashMap::new(),
        };

        // Populate the name->inode and inode_num->inode tables.
        // The first inode is always the root of the file system
        fs.inodes.push(Inode {
            name: "hack".to_string(),
            number: 0,
            time: TIME,
            size: None,
        });
        fs.inodes.push(Inode {
            name: "root".to_string(),
            number: 1,
            time: TIME,
            size: None,
        });

        fs
    }

    /// Query the block manager for files in the root directory.
    ///
    /// Post construction method to initialize the root directory.  I'm not sure that this still
    /// needs to be separate from `new`.
    pub fn load_root_directory(&mut self) {
        let mut number = self.inodes.len() as u64;

        for (name, size, time) in self.file_system.list_files("/") {
            let inode = Inode {
                number,
                name: name.clone(),
                time,
                size: Some(size),
            };

            self.inodes.push(inode);
            self.files.insert(name.clone(), number);
            number += 1;
        }

        debug!("load_root_directory {:?}", self.files);
    }
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
impl Filesystem for UberFSFuse {
    /// Start-up
    ///
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        self.file_system.initialize();
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
                error!("can't find requested inode {}", ino);
                reply.error(ENOENT)
            }
        };
    }

    // Return directory entries by name
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        trace!("lookup parent: {}, name: {:?}", parent, name);

        if parent == 1 {
            if let Some(name) = name.to_str() {
                if let Some(index) = self.files.get(name) {
                    if let Some(inode) = self.inodes.get(*index as usize) {
                        reply.entry(&TTL, &inode.file_attr(), 0);
                        return;
                    }
                }
            }
        }

        trace!("can't find ({:?}) under parent ({})", name, parent);
        reply.error(ENOENT);
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

    /// Return files in a directory
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        trace!("readdir ino: {}, offset: {}", ino, offset);
        if ino == 1 {
            let skip = if offset == 0 { offset } else { offset + 1 } as usize;
            for (i, (name, index)) in self.files.iter().enumerate().skip(skip) {
                if let Some(inode) = self.inodes.get(*index as usize) {
                    trace!(
                        "adding to reply: inode {}, offset {}, kind {:?}, name {}",
                        inode.number,
                        i + 1,
                        inode.kind(),
                        name
                    );
                    reply.add(inode.number, (i + 1) as i64, inode.kind(), name);
                } else {
                    reply.error(ENOENT);
                    return;
                }
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("open ino: {}, flags {:x}", ino, flags);

        if let Some(inode) = self.inodes.get_mut(ino as usize) {
            let path: PathBuf = ["/", inode.name.as_str()].iter().collect();

            let open_flags = flags as i32;
            let mode = match open_flags {
                O_RDONLY => OpenFileMode::Read,
                O_WRONLY => {
                    inode.size.replace(0);
                    OpenFileMode::Write
                }
                O_RDWR => OpenFileMode::ReadWrite,
                _ => unreachable!(),
            };

            match self.file_system.open_file(path, mode) {
                Some(fh) => reply.opened(fh as u64, 0),
                _ => reply.error(ENOENT),
            }
        } else {
            reply.error(ENOENT);
        }
    }

    // Create and apen a file
    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        flags: u32,
        reply: ReplyCreate,
    ) {
        debug!(
            "create name: {:?}, parent: {}, mode: {:#05o}, flags: {:#x}",
            name, parent, _mode, flags
        );
        if parent == 1 {
            let name = String::from(name.to_str().unwrap());

            let (fh, time) = match self.file_system.create_file(&name) {
                Some((fh, t)) => (fh as u64, t),
                None => (0, TIME),
            };
            let number = self.inodes.len() as u64;
            let inode = Inode {
                name: name.clone(),
                number,
                time,
                size: None,
            };

            reply.created(&TTL, &inode.file_attr(), 0, fh, flags);

            self.inodes.push(inode);
            self.files.insert(name, number);
        } else {
            reply.error(ENOENT);
        }
    }

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
        debug!(
            "release ino: {}, flags: {:#x}, flush: {}",
            ino, flags, flush
        );

        self.file_system.close_file(fh);
        reply.ok();
    }

    /// FIXME:
    ///  * BlockManager read should probably take an offset and a size.
    ///  * Also may want to consider caching something here and using the file handle?
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

        if let Ok(buffer) = self.file_system.read_file(fh, offset, size as usize) {
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

        if let Some(inode) = self.inodes.get_mut(ino as usize) {
            if let Ok(len) = self.file_system.write_file(fh, data) {
                debug!("wrote {} bytes", len);
                trace!("{:?}", &data[..len]);

                let new_size: u64 = if let Some(n) = inode.size {
                    n + len as u64
                } else {
                    len as u64
                };
                inode.size.replace(new_size);

                reply.written(len as u32);
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
        let block_manager = &self.file_system.block_manager;
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
