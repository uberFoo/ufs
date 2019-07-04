use std::{
    collections::HashMap,
    ffi::OsStr,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::{spawn, JoinHandle},
};

use ::time::Timespec;
use crossbeam::crossbeam_channel;
use failure::format_err;
use log::{debug, error, info, trace, warn};
use reqwest::IntoUrl;

use crate::block::{
    manager::BlockManager, map::BlockMap, BlockCardinality, BlockSize, BlockStorage, FileStore,
    MemoryStore, NetworkStore,
};
use crate::metadata::{
    DirectoryEntry, DirectoryMetadata, File, FileHandle, FileVersion, WASM_DIR, WASM_EXT,
};
use crate::runtime::{FileSystemOperator, FileSystemOps, Process, UfsMessage};
use crate::UfsUuid;

/// File mode for `open` call.
///
#[derive(Debug)]
pub enum OpenFileMode {
    /// Open file for reading
    ///
    Read,
    /// Open file for writing
    ///
    Write,
    /// Open file for reading and writing
    ///
    ReadWrite,
}

enum RuntimeManagerMsg {
    Shutdown,
    Program(WasmProgram),
}

struct WasmProgram {
    name: PathBuf,
    program: Vec<u8>,
}

/// File System integration with WASM interpreter
///
/// This struct contains the file system implementation, and a WASM runtime implementation.
/// The former is wrapped in a `Mutex`, wrapped in an `Arc`, which is passed to WASM programs so
/// that they may invoke callbacks to the file system. The runtime manages the WASM threads.
///
/// The two communicate via a channel. When a .wasm file is found on the file system, it uses the
/// channel to have the runtime create a thread for the wasm program.
pub struct UfsMounter<B: BlockStorage + 'static> {
    // FIXME: I think that the Mutex can be an RwLock...
    inner: Arc<Mutex<UberFileSystem<B>>>,
    runtime_mgr_channel: crossbeam_channel::Sender<RuntimeManagerMsg>,
    runtime_mgr_thread: Option<JoinHandle<Result<(), failure::Error>>>,
}

impl<B: BlockStorage> UfsMounter<B> {
    /// Constructor
    ///
    pub fn new(mut ufs: UberFileSystem<B>) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<RuntimeManagerMsg>();

        ufs.init_runtime(sender.clone());
        let inner = Arc::new(Mutex::new(ufs));

        let runtime_mgr = RuntimeManager::new(inner.clone(), receiver);
        let runtime_mgr_thread = RuntimeManager::start(runtime_mgr);

        let mounter = UfsMounter {
            inner,
            runtime_mgr_channel: sender,
            runtime_mgr_thread: Some(runtime_mgr_thread),
        };

        mounter
    }

    /// Shutdown
    ///
    pub fn shutdown(&mut self) -> Result<(), failure::Error> {
        self.runtime_mgr_channel
            .send(RuntimeManagerMsg::Shutdown)
            .unwrap();
        if let Some(thread) = self.runtime_mgr_thread.take() {
            info!("Waiting for RuntimeManager to shutdown.");
            thread.join().unwrap().unwrap();
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

pub struct RuntimeManager<B: BlockStorage + 'static> {
    ufs: Arc<Mutex<UberFileSystem<B>>>,
    receiver: crossbeam_channel::Receiver<RuntimeManagerMsg>,
    threads: HashMap<PathBuf, JoinHandle<Result<(), failure::Error>>>,
}

impl<B: BlockStorage> RuntimeManager<B> {
    fn new(
        ufs: Arc<Mutex<UberFileSystem<B>>>,
        receiver: crossbeam_channel::Receiver<RuntimeManagerMsg>,
    ) -> Self {
        RuntimeManager {
            ufs,
            receiver,
            threads: HashMap::new(),
        }
    }

    fn start(mut runtime: RuntimeManager<B>) -> JoinHandle<Result<(), failure::Error>> {
        spawn(move || {
            loop {
                let msg = runtime.receiver.recv().unwrap();
                match msg {
                    RuntimeManagerMsg::Shutdown => break,
                    RuntimeManagerMsg::Program(wasm) => {
                        info!("Adding WASM program {:?}", wasm.name);
                        let process = Process::new(wasm.name.clone(), wasm.program);
                        let mut ufs = runtime.ufs.lock().expect("poisoned ufs lock");
                        ufs.listeners.push(process.get_sender());
                        runtime.threads.insert(
                            wasm.name,
                            Process::start(
                                process,
                                Box::new(FileSystemOperator::new(runtime.ufs.clone()))
                                    as Box<dyn FileSystemOps>,
                            ),
                        );
                    }
                }
            }

            let ufs = runtime.ufs.lock().expect("poisoned ufs lock");
            info!("Shutting down WASM programs");
            ufs.notify_listeners(UfsMessage::Shutdown);

            for (_, thread) in runtime.threads {
                thread.join().unwrap().unwrap();
            }

            Ok(())
        })
    }
}

/// Main File System Implementation
///
pub struct UberFileSystem<B: BlockStorage> {
    /// Where we store blocks.
    ///
    id: UfsUuid,
    block_manager: BlockManager<B>,
    open_files: HashMap<FileHandle, File>,
    open_dirs: HashMap<FileHandle, DirectoryMetadata>,
    open_file_counter: FileHandle,
    listeners: Vec<crossbeam_channel::Sender<UfsMessage>>,
    program_mgr: Option<crossbeam_channel::Sender<RuntimeManagerMsg>>,
}

impl UberFileSystem<MemoryStore> {
    /// Create a file system with a Memory-backed block storage
    ///
    /// This is useful for testing, and not much else -- unless an ephemeral file system is
    /// warranted.
    ///
    pub fn new_memory(size: BlockSize, count: BlockCardinality) -> Self {
        let mem_store = MemoryStore::new(BlockMap::new(UfsUuid::new_root("test"), size, count));
        let block_manager = BlockManager::new(mem_store);

        UberFileSystem {
            id: block_manager.id().clone(),
            block_manager,
            open_files: HashMap::new(),
            open_dirs: HashMap::new(),
            open_file_counter: 0,
            listeners: vec![],
            program_mgr: None,
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
            id: block_manager.id().clone(),
            block_manager,
            open_files: HashMap::new(),
            open_dirs: HashMap::new(),
            open_file_counter: 0,
            listeners: vec![],
            program_mgr: None,
        })
    }
}

impl UberFileSystem<NetworkStore> {
    pub fn new_networked<U: IntoUrl>(url: U) -> Result<Self, failure::Error> {
        let net_store = NetworkStore::new(url)?;
        let block_manager = BlockManager::load(net_store)?;

        Ok(UberFileSystem {
            id: block_manager.id().clone(),
            block_manager,
            open_files: HashMap::new(),
            open_dirs: HashMap::new(),
            open_file_counter: 0,
            listeners: vec![],
            program_mgr: None,
        })
    }
}

impl<B: BlockStorage> UberFileSystem<B> {
    pub(crate) fn get_root_directory_id(&self) -> UfsUuid {
        self.block_manager.metadata().root_directory().id()
    }

    fn notify_listeners(&self, msg: UfsMessage) {
        for listener in &self.listeners {
            match listener.send(msg.clone()) {
                Ok(_) => (),
                Err(e) => error!("unable to send on channel {}", e),
            }
        }
    }

    /// Initialize for the Runtime
    ///
    /// We setup our channel to the `RuntimeManager`. Then we search for any .wasm files in .wasm
    /// directories, and create runtimes for them.
    fn init_runtime(&mut self, mgr: crossbeam_channel::Sender<RuntimeManagerMsg>) {
        self.program_mgr = Some(mgr);

        // Find .wasm directories
        // FIXME: This needs to recurse the subdirectories.
        let mut programs = Vec::<(PathBuf, FileVersion)>::new();
        for (d_name, d) in self.block_manager.metadata().root_directory().entries() {
            if let DirectoryEntry::Directory(dir) = d {
                if d_name == WASM_DIR {
                    for (f_name, f) in dir.entries() {
                        if let DirectoryEntry::File(file) = f {
                            let path = Path::new(f_name);
                            if let Some(ext) = path.extension() {
                                if ext == WASM_EXT {
                                    let program = file.get_latest();
                                    programs.push(([d_name, f_name].iter().collect(), program));
                                }
                            }
                        }
                    }
                }
            }
        }

        // if let Some(program_mgr) = self.program_mgr.clone() {
        //     for (path, file) in programs {
        //         if let Ok(fh) = self.open_file(&path, OpenFileMode::Read) {
        //             let size = file.size();
        //             if let Ok(program) = self.read_file(fh, 0, size as usize) {
        //                 info!("Adding program {:?} to runtime.", path);
        //                 program_mgr
        //                     .send(RuntimeManagerMsg::Program(WasmProgram {
        //                         name: path.to_path_buf(),
        //                         program,
        //                     }))
        //                     .unwrap()
        //             }
        //         }
        //     }
        // }
    }

    /// Return a reference to the `BlockManager`
    ///
    pub(crate) fn block_manager(&self) -> &BlockManager<B> {
        &self.block_manager
    }

    // /// Retrieve the root directory
    // ///
    // pub(crate) fn get_root_directory(&self) -> DirectoryEntry {
    //     DirectoryEntry::Directory(self.block_manager.root_dir().clone())
    // }

    /// List the contents of a Directory
    ///
    pub(crate) fn list_files(
        &self,
        handle: FileHandle,
    ) -> Option<&HashMap<String, DirectoryEntry>> {
        debug!("-------");
        debug!("`list_files`: {}", handle);
        match self.open_dirs.get(&handle) {
            Some(dir) => {
                trace!("\t{:#?}", dir.entries());
                Some(dir.entries())
            }
            None => {
                warn!("\tdirectory not opened");
                None
            }
        }
    }

    /// Create a directory
    ///
    pub(crate) fn create_directory(
        &mut self,
        parent_id: UfsUuid,
        name: &str,
    ) -> Result<DirectoryMetadata, failure::Error> {
        debug!("--------");
        debug!("`create_directory`: {}", name);

        let dir = self
            .block_manager
            .metadata_mut()
            .new_directory(parent_id, name);

        debug!("end `create_directory`");
        dir
    }

    /// Create a file
    ///
    pub(crate) fn create_file(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<(FileHandle, File), failure::Error> {
        debug!("--------");
        debug!("`create_file`: {:?}", name);

        let file = self.block_manager.metadata_mut().new_file(dir_id, name)?;

        let fh = self.open_file_counter;
        self.open_file_counter = self.open_file_counter.wrapping_add(1);
        self.open_files.insert(fh, file.clone());

        // FIXME
        // self.notify_listeners(UfsMessage::FileCreate(dir_id, name.to_owned()));

        Ok((fh, file))
    }

    /// Open a directory
    ///
    pub(crate) fn open_directory(&mut self, id: UfsUuid) -> Result<FileHandle, failure::Error> {
        debug!("--------");
        debug!("`open_directory`: {:?}", id);
        let dir = self.block_manager.metadata().get_directory(id)?;

        let fh = self.open_file_counter;
        self.open_file_counter = self.open_file_counter.wrapping_add(1);

        trace!("\t{:#?}", dir);
        self.open_dirs.insert(fh, dir);

        Ok(fh)
    }

    /// Close a directory
    ///
    /// This call is super important. When the file system changes, FUSE calls this function, which
    /// eventually allows us to refresh the file system contents.
    pub(crate) fn close_directory(&mut self, handle: FileHandle) {
        debug!("--------");

        match self.open_dirs.remove(&handle) {
            Some(dir) => {
                debug!("`close_directory`: handle: {}", handle);
                trace!("{:#?}", dir);
            }
            None => warn!("asked to close a directory not in the map {}", handle),
        }
    }

    /// Remove a file
    ///
    pub(crate) fn remove_file(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<(), failure::Error> {
        debug!("--------");
        debug!("`remove_file`: {}, dir: {:?}", name, dir_id);

        self.block_manager
            .metadata_mut()
            .unlink_file(dir_id, name)?;
        Ok(())
    }

    /// Open a file
    ///
    pub(crate) fn open_file(
        &mut self,
        id: UfsUuid,
        mode: OpenFileMode,
    ) -> Result<FileHandle, failure::Error> {
        debug!("--------");
        debug!("`open_file` {:?}, mode: {:?}", id, mode);
        let file = match mode {
            OpenFileMode::Write => self.block_manager.metadata_mut().get_file_write_only(id)?,
            OpenFileMode::Read => self.block_manager.metadata().get_file_read_only(id)?,
            OpenFileMode::ReadWrite => self.block_manager.metadata_mut().get_file_read_write(id)?,
        };

        let fh = self.open_file_counter;
        self.open_file_counter = self.open_file_counter.wrapping_add(1);

        self.open_files.insert(fh, file);

        debug!("\thandle: {}", fh);

        // FIXME
        // self.notify_listeners(UfsMessage::FileOpen(path.to_path_buf()));

        Ok(fh)
    }

    /// Close a file
    ///
    pub(crate) fn close_file(&mut self, handle: FileHandle) {
        debug!("-------");
        debug!("`close_file`: {}", handle);

        // Commit the file
        if let Some(file) = self.open_files.get(&handle) {
            debug!("\t{:?}", file);
            self.block_manager.metadata_mut().commit_file(file.clone());
        }

        // Add any .wasm files, located in a .wasm directory, to the runtime.
        if let Some(program_mgr) = &self.program_mgr {
            if let Some(file) = self.open_files.get(&handle) {
                // Check to see if this file is in the special ".wasm" directory.
                let file_id = file.file_id;
                let file_a = self
                    .block_manager
                    .metadata()
                    .get_file_metadata(file_id)
                    .unwrap();
                let dir = self
                    .block_manager
                    .metadata()
                    .get_directory(file_a.dir_id())
                    .unwrap();
                if dir.is_wasm_dir() {
                    // Get the file's name and check for the correct extension
                    for (name, entry) in dir.entries() {
                        if let DirectoryEntry::File(f) = entry {
                            if f.id() == file_id {
                                let path = Path::new(name);
                                if let Some(ext) = path.extension() {
                                    if ext == WASM_EXT {
                                        info!("adding {:?} to runtime", name);
                                        let size = file.version.size();
                                        if let Ok(program) =
                                            self.read_file(handle, 0, size as usize)
                                        {
                                            program_mgr
                                                .send(RuntimeManagerMsg::Program(WasmProgram {
                                                    name: path.to_path_buf(),
                                                    program,
                                                }))
                                                .unwrap()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        match self.open_files.remove(&handle) {
            Some(file) => {
                // FIXME
                // self.notify_listeners(UfsMessage::FileClose(path));
            }
            None => warn!("asked to close a file not in the map {}", handle),
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

                // FIXME
                // self.notify_listeners(UfsMessage::FileWrite(path, bytes.to_vec()));

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
        &self,
        handle: FileHandle,
        offset: i64,
        size: usize,
    ) -> Result<Vec<u8>, failure::Error> {
        debug!("-------");
        debug!(
            "`read_file`: handle: {}, reading offset {}, size {}",
            handle, offset, size
        );

        if let Some(file) = self.open_files.get(&handle) {
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
                // FIXME
                // self.notify_listeners(UfsMessage::FileRead(path, buffer.clone()));

                Ok(buffer)
            } else {
                Err(format_err!("Error reading file {}", handle))
            }
        } else {
            Err(format_err!("File not open {}", handle))
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

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h0, file) = ufs.create_file(root_id, "test_open_file").unwrap();

        let h1 = ufs.open_file(file.file_id, OpenFileMode::Read).unwrap();
        assert!(
            h0 != h1,
            "two open calls to the same file should return different handles"
        );
    }

    #[test]
    fn read_and_write_file_networked() {
        init();

        let mut ufs = UberFileSystem::new_networked("http://localhost:8888/test").unwrap();
        let test = include_str!("lib.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();

        assert_eq!(test.len(), ufs.write_file(h, test).unwrap());
        let bytes = ufs.read_file(h, 0, test.len()).unwrap();
        assert_eq!(test, bytes.as_slice());
    }

    #[test]
    fn read_and_write_file() {
        init();

        let mut ufs = UberFileSystem::new_memory(BlockSize::TwentyFortyEight, 100);
        let test = include_str!("lib.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();

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

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();
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

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();
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
