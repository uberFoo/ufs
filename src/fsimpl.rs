use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{Arc, Mutex, RwLock},
    thread::JoinHandle,
};

use ::time::Timespec;
use crossbeam::crossbeam_channel;
use failure::format_err;
use log::{debug, error, trace, warn};

use crate::block::{
    manager::BlockManager, map::BlockMap, BlockCardinality, BlockSize, BlockStorage, FileStore,
    MemoryStore,
};
use crate::metadata::{DirectoryEntry, File, FileHandle, FileSize, FileVersion};
use crate::runtime::{init_runtime, FileSystemOperator, Process, UfsMessage};
use crate::UfsUuid;

#[derive(Debug)]
pub enum OpenFileMode {
    Read,
    Write,
    ReadWrite,
}

pub struct UfsMounter<B: BlockStorage + 'static> {
    // FIXME: I think that the Mutex can be an RwLock...
    inner: Arc<Mutex<UberFileSystem<B>>>,
    threads: Vec<Option<JoinHandle<Result<(), failure::Error>>>>,
}

impl<B: BlockStorage> UfsMounter<B> {
    pub fn new(ufs: UberFileSystem<B>) -> Self {
        let mut new_ufs = UfsMounter {
            inner: Arc::new(Mutex::new(ufs)),
            threads: vec![],
        };

        new_ufs.initialize();

        new_ufs
    }

    /// Initialization
    ///
    pub fn initialize(&mut self) {
        let mut ufs = self.inner.lock().expect("poisoned ufs lock");

        for process in init_runtime().unwrap() {
            ufs.listeners.push(process.get_sender());
            self.threads.push(Some(Process::start(
                process,
                Box::new(FileSystemOperator::new(self.inner.clone())),
            )));
        }
    }

    /// Shutdown
    ///
    pub fn shutdown(&mut self) -> Result<(), failure::Error> {
        let ufs = self.inner.lock().expect("poisoned ufs lock");
        ufs.notify_listeners(UfsMessage::Shutdown);

        for thread in &mut self.threads {
            if let Some(thread) = thread.take() {
                thread.join().unwrap().unwrap();
            }
        }
        Ok(())
    }
}

impl<B: BlockStorage> Deref for UfsMounter<B> {
    type Target = Arc<Mutex<UberFileSystem<B>>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<B: BlockStorage> DerefMut for UfsMounter<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Main File System Implementation
///
pub struct UberFileSystem<B: BlockStorage + 'static> {
    /// Where we store blocks.
    ///
    block_manager: BlockManager<B>,
    open_files: HashMap<FileHandle, File>,
    open_file_counter: FileHandle,
    listeners: Vec<crossbeam_channel::Sender<UfsMessage>>,
}

impl UberFileSystem<MemoryStore> {
    /// Create a file system with a Memory-backed block storage
    ///
    /// This is useful for testing, and not much else -- unless an ephemeral file system is
    /// warranted.
    ///
    pub fn new_memory(size: BlockSize, count: BlockCardinality) -> Self {
        let mem_store = MemoryStore::new(BlockMap::new(UfsUuid::new("test"), size, count));
        let block_manager = BlockManager::new(mem_store);

        UberFileSystem {
            block_manager,
            open_files: HashMap::new(),
            open_file_counter: 0,
            listeners: vec![],
        }
    }
}

impl UberFileSystem<FileStore> {
    /// Load an existing file-backed File System
    ///
    pub fn load_file_backed<P>(path: P) -> Result<Self, failure::Error>
    where
        P: AsRef<Path>,
    {
        let file_store = FileStore::load(path.as_ref())?;
        let block_manager = BlockManager::load(file_store)?;

        Ok(UberFileSystem {
            block_manager,
            open_files: HashMap::new(),
            open_file_counter: 0,
            listeners: vec![],
        })
    }
}

// impl UberFileSystem<NetworkStore> {
//     pub fn new_networked(url: Url) -> Result<Self, failure::Error> {
//         let net_store = NetworkStore::new(url)?;
//         let block_manager = BlockManager::load(net_store)?;

//         Ok(UberFileSystem {
//             block_manager,
//             open_files: HashMap::new(),
//             open_file_counter: 0,
//             listeners: vec![],
//         })
//     }
// }

impl<B: BlockStorage> UberFileSystem<B> {
    fn notify_listeners(&self, msg: UfsMessage) {
        for listener in &self.listeners {
            listener.send(msg.clone()).unwrap();
        }
    }

    pub(crate) fn block_manager(&self) -> &BlockManager<B> {
        &self.block_manager
    }

    /// List the contents of a Directory
    ///
    /// This function takes a Path and returns a Vec of (name, size) tuples -- one for each file
    /// contained within the specified directory.
    ///
    /// TODO: Verify that the path exists, and do something with it!
    pub(crate) fn list_files(&self, path: &Path) -> Vec<(String, FileSize, Timespec)> {
        debug!("-------");
        debug!("`list_files`: {:?}", path);
        // let mut files = Vec::new();
        // for (name, entry) in self.block_manager.root_dir().entries() {
        //     match entry {
        //         DirectoryEntry::Directory(d) => {
        //             debug!("dir: {}", name);
        //             files.push((name.clone(), 0, d.write_time().into()));
        //         }
        //         DirectoryEntry::File(f) => {
        //             for (n, version) in f.versions().iter().enumerate() {
        //                 let mut name = name.clone();
        //                 name.push('@');
        //                 name.push_str(&n.to_string());
        //                 let size = version.size();
        //                 let write_time = version.write_time();
        //                 debug!("file: {}, size: {}", name, size);
        //                 files.push((name.clone(), size, write_time.into()));
        //             }
        //             let size = f.get_current_version().size();
        //             debug!("file: {}, size: {}", name, size);
        //             files.push((name.clone(), size, f.write_time().into()));
        //         }
        //     }
        // }

        // files
        self.block_manager
            .root_dir()
            .entries()
            .iter()
            .map(|(name, e)| match e {
                DirectoryEntry::Directory(d) => {
                    debug!("dir: {}", name);
                    (name.clone(), 0, d.write_time().into())
                }
                DirectoryEntry::File(f) => {
                    let size = f.get_current_version().size();
                    debug!("file: {}, size: {}", name, size);
                    (name.clone(), size, f.write_time().into())
                }
            })
            .collect()
    }

    /// Create a file
    ///
    pub(crate) fn create_file(&mut self, path: &Path) -> Option<(FileHandle, Timespec)> {
        if let Some(ostr_name) = path.file_name() {
            if let Some(name) = ostr_name.to_str() {
                let file = self.block_manager.root_dir_mut().new_file(name);
                let time = file.version.write_time();

                let fh = self.open_file_counter;
                self.open_file_counter = self.open_file_counter.wrapping_add(1);

                self.open_files.insert(fh, file);

                debug!("`create_file`: {:?}, handle: {}", path, fh);

                self.notify_listeners(UfsMessage::FileCreate(path.to_path_buf()));

                return Some((fh, time.into()));
            }
        }

        None
    }

    /// Open a file
    ///
    pub(crate) fn open_file(&mut self, path: &Path, mode: OpenFileMode) -> Option<FileHandle> {
        debug!("-------");
        if let Some(mut file) = self.block_manager.root_dir().get_file(&path) {
            // If the file is opened for writing, allocate a new FileVersion for it's bits.
            match mode {
                OpenFileMode::Write | OpenFileMode::ReadWrite => file.version = FileVersion::new(),
                _ => (),
            }
            let fh = self.open_file_counter;
            self.open_file_counter = self.open_file_counter.wrapping_add(1);

            self.open_files.insert(fh, file);

            debug!("`open_file`: {:?}, handle: {}, mode: {:?}", path, fh, mode);

            self.notify_listeners(UfsMessage::FileOpen(path.to_path_buf()));

            Some(fh)
        } else {
            None
        }
    }

    /// Close a file
    ///
    pub(crate) fn close_file(&mut self, handle: FileHandle) {
        debug!("-------");

        match self.open_files.remove(&handle) {
            Some(file) => {
                let path = file.path.clone();

                debug!("`close_file`: {:?}, handle: {}", path, handle);
                self.block_manager.root_dir_mut().update_file(file);

                self.notify_listeners(UfsMessage::FileClose(path));
            }
            None => {
                warn!("asked to close a file not in the map {}", handle);
            }
        }
    }

    /// Write bytes to a file.
    ///
    pub(crate) fn write_file(
        &mut self,
        handle: FileHandle,
        bytes: &[u8],
    ) -> Result<usize, failure::Error> {
        debug!("-------");
        debug!("`write_file`: handle: {}", handle);

        match &mut self.open_files.get_mut(&handle) {
            Some(file) => {
                let mut written = 0;
                while written < bytes.len() {
                    match self.block_manager.write(&bytes[written..]) {
                        Ok(block) => {
                            written += block.size() as usize;
                            file.version.append_block(&block);
                        }
                        Err(e) => {
                            error!("problem writing data to file: {}", e);
                        }
                    }
                }
                debug!("wrote {} bytes", written,);

                let path = file.path.clone();
                self.notify_listeners(UfsMessage::FileWrite(path, bytes.to_vec()));

                Ok(written)
            }
            None => {
                warn!("asked to write file not in the map {}", handle);
                Ok(0)
            }
        }
    }

    /// Read bytes from a file
    ///
    ///
    pub(crate) fn read_file(
        &mut self,
        handle: FileHandle,
        offset: i64,
        size: usize,
    ) -> Result<Vec<u8>, failure::Error> {
        debug!("-------");
        debug!(
            "`read_file`: handle: {}, reading offset {}, size {}",
            handle, offset, size
        );

        let file = self.open_files.get(&handle).unwrap();
        let block_size = self.block_manager.block_size();

        let start_block = (offset / block_size as i64) as usize;
        let mut start_offset = (offset % block_size as i64) as usize;

        let mut blocks = file.version.blocks().clone();
        trace!("reading from blocks {:?}", &blocks);
        let block_iter = &mut blocks.iter_mut().skip(start_block);
        trace!("current iterator {:?}", block_iter);

        let mut read = 0;
        let mut buffer = vec![0; size];
        while read < size {
            if let Some(block_number) = block_iter.next() {
                if let Some(block) = self.block_manager.get_block(*block_number) {
                    trace!("reading block {:?}", &block);
                    if let Ok(bytes) = self.block_manager.read(block) {
                        trace!("read bytes\n{:?}", &bytes);
                        let block_len = bytes.len();
                        let width = std::cmp::min(size - read, block_len - start_offset);

                        trace!(
                            "copying to buffer[{}..{}] from bytes[{}..{}]",
                            read,
                            read + width,
                            start_offset,
                            start_offset + width
                        );
                        buffer[read..read + width]
                            .copy_from_slice(&bytes[start_offset..start_offset + width]);

                        read += width;
                        trace!("buffer is now {:?}", &buffer);
                    }
                }
                start_offset = 0;
            }
        }

        if buffer.len() == size {
            let path = file.path.clone();
            self.notify_listeners(UfsMessage::FileRead(path, buffer.clone()));

            Ok(buffer)
        } else {
            Err(format_err!("Error reading file {:?}", file.path))
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn open_file() {
        init();

        let mut ufs = UberFileSystem::new_memory(BlockSize::TwentyFortyEight, 100);
        let test_file = PathBuf::from("/test_open_file");
        let (h0, _) = ufs.create_file(&test_file).unwrap();
        let h1 = ufs.open_file(&test_file, OpenFileMode::Read).unwrap();
        assert!(
            h0 != h1,
            "two open calls to the same file should return different handles"
        );
    }

    #[test]
    fn read_and_write_file() {
        init();

        let mut ufs = UberFileSystem::new_memory(BlockSize::TwentyFortyEight, 100);
        let test = include_str!("lib.rs").as_bytes();

        let (h, _) = ufs.create_file(&PathBuf::from("/lib.rs")).unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test).unwrap());
        let bytes = ufs.read_file(h, 0, test.len()).unwrap();
        assert_eq!(test, bytes.as_slice());
    }

    #[test]
    fn small_chunks() {
        init();

        let chunk_size = 88;
        let mut ufs = UberFileSystem::new_memory(BlockSize::TwentyFortyEight, 100);
        let test = include_str!("lib.rs").as_bytes();

        let (h, _) = ufs.create_file(&PathBuf::from("/lib.rs")).unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test).unwrap());

        let mut offset = 0;
        test.chunks(chunk_size).for_each(|test_bytes| {
            let bytes = ufs.read_file(h, offset, test_bytes.len()).unwrap();
            let len = bytes.len();
            assert_eq!(
                std::str::from_utf8(test_bytes).unwrap(),
                String::from_utf8(bytes).unwrap(),
                "failed at offset {}",
                offset
            );
            offset += len as i64;
        });
    }

    #[test]
    fn large_chunks() {
        init();

        let chunk_size = 8888;
        let mut ufs = UberFileSystem::new_memory(BlockSize::TwentyFortyEight, 100);
        let test = include_str!("lib.rs").as_bytes();

        let (h, _) = ufs.create_file(&PathBuf::from("/lib.rs")).unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test).unwrap());

        let mut offset = 0;
        test.chunks(chunk_size).for_each(|test_bytes| {
            let bytes = ufs.read_file(h, offset, test_bytes.len()).unwrap();
            let len = bytes.len();
            assert_eq!(
                std::str::from_utf8(test_bytes).unwrap(),
                String::from_utf8(bytes).unwrap(),
                "failed at offset {}",
                offset
            );
            offset += len as i64;
        });
    }
}
