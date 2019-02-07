use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    path::Path,
};

use failure::{format_err, Error};
use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};
use libc::ENOENT;
use log::{debug, error, info};
use time::Timespec;

use crate::block::{BlockCardinality, BlockList, BlockManager, BlockSize, FileStore};

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };
const TIME: Timespec = Timespec {
    sec: 10634562,
    nsec: 0,
};

/// FIXME: blocks should be an Option to a reference of BlockList
#[derive(Clone, Debug)]
struct Inode {
    number: u64,
    name: String,
    blocks: Option<BlockList>,
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

        match &self.blocks {
            Some(b) => FileAttr {
                ino: self.number,
                size: b.size(),
                blocks: b.block_count(),
                atime: TIME,
                mtime: TIME,
                ctime: TIME,
                crtime: TIME,
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
                atime: TIME,
                mtime: TIME,
                ctime: TIME,
                crtime: TIME,
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

pub struct UberFSFuse {
    next_inode: BlockCardinality,
    block_manager: BlockManager<FileStore>,
    inodes: BTreeMap<BlockCardinality, Inode>,
    // FIXME: Grrrr.  should be reference...
    files: HashMap<String, Inode>,
}

impl UberFSFuse {
    /// Create a new file system
    ///
    /// FIXME: This is ok for testing and building, but otherwise crap.  Need now to accept
    /// block size, and block count, etc.  Furthermore, there should be a load method for something
    /// that already exists.
    pub fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        match FileStore::new(path.as_ref(), BlockSize::TwentyFortyEight, 0x100) {
            Ok(store) => {
                let mut fs = UberFSFuse {
                    next_inode: 0,
                    block_manager: BlockManager::new(store),
                    inodes: BTreeMap::new(),
                    files: HashMap::new(),
                };

                // Put some files in there.
                fs.block_manager
                    .write_bytes("test.txt".to_string(), b"Hello World!\n")
                    .unwrap();
                fs.block_manager
                    .write_bytes(
                        "foo.sh".to_string(),
                        &b"#!/bin/bash\n\necho \"Hello World!\"\n"[..],
                    )
                    .unwrap();

                let mut number = 1;
                fs.inodes.insert(
                    number,
                    Inode {
                        name: "root".to_string(),
                        number,
                        blocks: None,
                    },
                );

                // FIXME: should block_list_map be pub?
                for (name, blocks) in &fs.block_manager.block_list_map {
                    number += 1;

                    let inode = Inode {
                        number,
                        // Bad clone!
                        name: name.to_owned(),
                        blocks: blocks.to_owned(),
                    };

                    fs.inodes.insert(number, inode.to_owned());
                    fs.files.insert(name.to_owned(), inode);
                }

                fs.next_inode = number + 1;

                Ok(fs)
            }
            Err(e) => Err(format_err!("something went stoopid-wrong {}", e)),
        }
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
    /// Return inode attributes
    ///
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        info!("getattr inode: {}", ino);
        match self.inodes.get(&ino) {
            Some(inode) => reply.attr(&TTL, &inode.file_attr()),
            None => reply.error(ENOENT),
        };
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 {
            if let Some(name) = name.to_str() {
                if let Some(inode) = self.files.get(name) {
                    reply.entry(&TTL, &inode.file_attr(), 0);
                    return;
                }
            }
        }

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
        info!("setattr inode: {}, mode: {:#x?}, flags: {:#x?}, uid: {:?}, gid: {:?}, size: {:?}, atime: {:?}, mtime: {:?}, fh: {:?}, crtime: {:?}, chgtime: {:?}, bkuptime: {:?}",_ino, _mode, _flags, _uid, _gid, _size, _atime, _mtime, _fh, _crtime, _chgtime, _bkuptime);

        self.getattr(_req, _ino, reply);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        info!(
            "readdir ino: {}, offset: {}, reply: {:?}",
            ino, offset, reply
        );
        if ino == 1 {
            let skip = if offset == 0 { offset } else { offset + 1 } as usize;
            for (i, (name, inode)) in self.files.iter().enumerate().skip(skip) {
                reply.add(inode.number, i as i64, inode.kind(), name.as_str());
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    /// FIXME: TOO MANY CLONES IN HERE!!!
    /// FIXME: Need a method in the FS to manage the busy work;
    /// FIXME: have inode lifetimes in maps depend on FS
    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        flags: u32,
        reply: ReplyCreate,
    ) {
        info!(
            "create name: {:?}, parent: {}, mode: {:#05o}, flags: {:#x}",
            name, parent, _mode, flags
        );
        if parent == 1 {
            let name = String::from(name.to_str().unwrap());
            self.block_manager.write_bytes(name.to_owned(), vec![]);
            let number = self.next_inode;
            self.next_inode += 1;
            let inode = Inode {
                name: name.to_owned(),
                number,
                blocks: None,
            };
            self.inodes.insert(number, inode.to_owned());
            self.files.insert(name, inode.to_owned());

            reply.created(&TTL, &inode.file_attr(), 0, 0, flags);
        } else {
            reply.error(ENOENT);
        }
    }

    /// FIXME:
    ///  * BlockManager read should probably take an offset and a size.
    ///  * Also may want to consider caching something here and using the file handle?
    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        info!("read ino: {}, offset: {}, size: {}", ino, offset, size);
        if let Some(inode) = self.inodes.get(&ino) {
            match &inode.blocks {
                Some(blocks) => match self.block_manager.read(&blocks) {
                    Ok(bytes) => {
                        reply.data(&bytes[offset as usize..size as usize]);
                        return;
                    }
                    Err(e) => {
                        error!("error reading inode {:?}: {}", inode, e);
                    }
                },
                None => (),
            };
        };

        reply.error(ENOENT);
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        info!("write ino: {}, offset: {}, data: {:?}", ino, offset, data);
        let bm = &mut self.block_manager;

        if self.inodes.contains_key(&ino) {
            self.inodes.entry(ino).and_modify(|inode| {
                bm.write_bytes(inode.name.to_owned(), &data[offset as usize..])
                    .unwrap();
                inode.blocks = bm.block_list_map.get(&inode.name).unwrap().clone();
                reply.written(data[offset as usize..].len() as u32);
            });
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
        debug!("statfs");
        reply.statfs(
            self.block_manager.block_count(),
            self.block_manager.free_block_count(),
            self.block_manager.free_block_count(),
            // I'm using i64 below, because it's consistent with what I'm seeing from APFS.
            i64::max_value() as u64,
            // i64::max_value() as u64 - self.files.len() as u64,
            i64::max_value() as u64,
            self.block_manager.block_size() as u32, // I'd had 2048 hardcoded here once...
            0xff,
            self.block_manager.block_size() as u32,
        );
    }
}
