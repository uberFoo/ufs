use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::{spawn, JoinHandle},
};

use crossbeam::crossbeam_channel;
use failure::format_err;
use log::{debug, error, info, trace, warn};
use reqwest::IntoUrl;

use crate::{
    block::{
        manager::BlockManager, map::BlockMap, BlockCardinality, BlockSize, BlockStorage, FileStore,
        MemoryStore, NetworkStore,
    },
    crypto::make_fs_key,
    metadata::{
        DirectoryEntry, DirectoryMetadata, File, FileHandle, FileMetadata, Metadata, WASM_EXT,
    },
    server::UfsRemoteServer,
    UfsUuid,
};

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

/// Runtime Manager Messages
///
/// These are used to communicate messages to the Runtime Manager from the file system
/// implementation.
enum RuntimeManagerMsg {
    /// Shutdown WASM Runtime
    ///
    /// The file system is shutting down, and this allows the WASM programs the same opportunity.
    Shutdown,
    /// Add a new WASM Program
    ///
    /// The file system contains a WASM program, and wishes it to be loaded and run.
    Start(WasmProgram),
    /// Stop a running WASM Program
    ///
    /// There is a running program that must needs be stopped.
    Stop(PathBuf),
}

/// Information necessary to start running a WASM program
///
/// This is the contents of the RuntimeManagerMsg::Start message.
struct WasmProgram {
    /// A non-unique identifier for the WASM program.  Uniqueness may be virtuous.
    name: PathBuf,
    /// The bytes that comprise the program.
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
    remote: Option<JoinHandle<Result<(), failure::Error>>>,
    runtime_mgr_channel: crossbeam_channel::Sender<RuntimeManagerMsg>,
    runtime_mgr_thread: Option<JoinHandle<Result<(), failure::Error>>>,
}

impl<B: BlockStorage> UfsMounter<B> {
    /// Constructor
    ///
    pub fn new(mut ufs: UberFileSystem<B>, remote_port: Option<u16>) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<RuntimeManagerMsg>();

        // Initialize the UFS
        info!("Initializing file system");
        ufs.init_runtime(sender.clone());
        let inner = Arc::new(Mutex::new(ufs));

        // Start the Runtime
        info!("Initializing WASM runtime");
        let runtime_mgr = RuntimeManager::new(inner.clone(), receiver);
        let runtime_mgr_thread = RuntimeManager::start(runtime_mgr);

        // Start the remote FS listener
        let remote = if let Some(port) = remote_port {
            info!("Initializing remote file system listener");
            match UfsRemoteServer::new(inner.clone(), port) {
                Ok(listener) => Some(UfsRemoteServer::start(listener)),
                Err(e) => {
                    error!("Error starting remote listener: {}", e);
                    None
                }
            }
        } else {
            None
        };

        let mounter = UfsMounter {
            inner,
            remote,
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

/// WASM Thread Management
///
/// The sole purpose of this struct is to provide a means by which the `UberFileSystem` may start
/// and stop WASM programs. There is a channel that the UFS uses to send start and stop messages to
/// the `RuntimeManager`. This then handles the work of doing so.
///
/// The `UfsMounter` will also send a shutdown message, on the same channel, when the file system is
/// going away. Here, we use that message to nicely stop the WASM programs before exiting.
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

    /// Start the RuntimeManager
    ///
    /// Note that this does not take `self`, but has access via `runtime`.
    fn start(mut runtime: RuntimeManager<B>) -> JoinHandle<Result<(), failure::Error>> {
        spawn(move || {
            loop {
                let msg = runtime.receiver.recv().unwrap();
                match msg {
                    // Shutdown comes from the UfsMounter, thus we need to shutdown the running
                    // programs (via our UberFileSystem reference) before joining the threads,
                    // see below.
                    RuntimeManagerMsg::Shutdown => break,
                    // Stop the WASM program and remove it from the listeners map.
                    RuntimeManagerMsg::Stop(name) => {
                        info!("Stopping WASM program {:?}", name);
                        let mut ufs = runtime.ufs.lock().expect("poisoned ufs lock");
                        if let Some(thread) = runtime.threads.remove(&name) {
                            // if let Some(sender) = ufs.listeners.remove(&name) {
                            //     sender.send(UfsMessage::Shutdown);
                            //     thread.join().unwrap().unwrap();
                            // }
                        }
                    }
                    // Start the WASM program and add it to the listeners map.
                    RuntimeManagerMsg::Start(wasm) => {
                        info!("Starting WASM program {:?}", wasm.name);
                        // let process = Process::new(wasm.name.clone(), wasm.program);
                        // let mut ufs = runtime.ufs.lock().expect("poisoned ufs lock");
                        // ufs.listeners
                        //     .insert(wasm.name.clone(), process.get_sender());
                        // runtime.threads.insert(
                        //     wasm.name,
                        //     Process::start(
                        //         process,
                        //         Box::new(FileSystemOperator::new(runtime.ufs.clone()))
                        //             as Box<dyn FileSystemOps>,
                        //     ),
                        // );
                    }
                }
            }

            let ufs = runtime.ufs.lock().expect("poisoned ufs lock");
            info!("Shutting down WASM programs");
            // ufs.notify_listeners(UfsMessage::Shutdown);

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
    id: UfsUuid,
    user: UfsUuid,
    block_manager: BlockManager<B>,
    open_files: HashMap<FileHandle, File>,
    open_dirs: HashMap<FileHandle, DirectoryMetadata>,
    open_file_counter: FileHandle,
    // listeners: HashMap<PathBuf, crossbeam_channel::Sender<UfsMessage>>,
    program_mgr: Option<crossbeam_channel::Sender<RuntimeManagerMsg>>,
}

impl UberFileSystem<MemoryStore> {
    /// Create a file system with a Memory-backed block storage
    ///
    /// This is useful for testing, and not much else -- unless an ephemeral file system is
    /// warranted.
    ///
    pub fn new_memory<S: AsRef<str>>(
        user: S,
        password: S,
        name: S,
        size: BlockSize,
        count: BlockCardinality,
    ) -> Self {
        let id = UfsUuid::new_root_fs(name.as_ref());

        let mem_store = MemoryStore::new(BlockMap::new(id, size, count));
        let block_manager = BlockManager::new(&user, &password, mem_store);

        UberFileSystem {
            id: block_manager.id().clone(),
            user: UfsUuid::new_user(user.as_ref()),
            block_manager,
            open_files: HashMap::new(),
            open_dirs: HashMap::new(),
            open_file_counter: 0,
            // listeners: HashMap::new(),
            program_mgr: None,
        }
    }
}

impl UberFileSystem<FileStore> {
    /// Load an existing file-backed File System
    ///
    pub fn load_file_backed<S, P>(
        master_password: S,
        user: S,
        password: S,
        path: P,
    ) -> Result<Self, failure::Error>
    where
        S: AsRef<str>,
        P: AsRef<Path>,
    {
        let key = make_fs_key(
            master_password.as_ref(),
            &UfsUuid::new_root_fs(
                path.as_ref()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .as_bytes(),
            ),
        );
        let file_store = FileStore::load(key.clone(), path.as_ref())?;
        let block_manager = BlockManager::load(user.as_ref(), password.as_ref(), file_store)?;

        Ok(UberFileSystem {
            id: block_manager.id().clone(),
            user: UfsUuid::new_user(user.as_ref()),
            block_manager,
            open_files: HashMap::new(),
            open_dirs: HashMap::new(),
            open_file_counter: 0,
            // listeners: HashMap::new(),
            program_mgr: None,
        })
    }
}

impl UberFileSystem<NetworkStore> {
    /// Load blocks from a remote block server
    ///
    pub fn new_networked<S, U>(
        user: S,
        password: S,
        name: S,
        url: U,
    ) -> Result<Self, failure::Error>
    where
        S: AsRef<str>,
        U: IntoUrl,
    {
        let net_store = NetworkStore::new(name, url)?;
        let block_manager = BlockManager::load(&user, &password, net_store)?;

        Ok(UberFileSystem {
            id: block_manager.id().clone(),
            user: UfsUuid::new_user(user.as_ref()),
            block_manager,
            open_files: HashMap::new(),
            open_dirs: HashMap::new(),
            open_file_counter: 0,
            // listeners: HashMap::new(),
            program_mgr: None,
        })
    }
}

impl<B: BlockStorage> UberFileSystem<B> {
    /// Add a user to the file system
    pub fn add_user(&mut self, user: String, password: String) {
        self.block_manager.metadata_mut().add_user(user, password);
    }

    /// Get a list of existing users
    pub fn get_users(&self) -> Vec<String> {
        self.block_manager.metadata().get_users()
    }

    /// This is used by the fuse implementation as an inode ID.
    pub(crate) fn get_root_directory_id(&self) -> UfsUuid {
        self.block_manager.metadata().root_directory().id()
    }

    /// Send a message to all listening WASM programs.
    // fn notify_listeners(&self, msg: UfsMessage) {
    //     for (_, listener) in &self.listeners {
    //         match listener.send(msg.clone()) {
    //             Ok(_) => (),
    //             Err(e) => error!("unable to send on channel {}", e),
    //         }
    //     }
    // }

    /// Initialize for the Runtime
    ///
    /// We setup our channel to the `RuntimeManager`. Then we search for any .wasm files in .wasm
    /// directories, and create runtimes for them.
    fn init_runtime(&mut self, mgr: crossbeam_channel::Sender<RuntimeManagerMsg>) {
        self.program_mgr = Some(mgr);

        fn find_wasm_pgms(
            programs: &mut Vec<(PathBuf, FileMetadata)>,
            metadata: &Metadata,
            dir: &DirectoryMetadata,
        ) {
            // Find .wasm directories
            for (_, d) in dir.entries() {
                if let DirectoryEntry::Directory(dir) = d {
                    if dir.is_wasm_dir() {
                        for (f_name, e) in dir.entries() {
                            if let DirectoryEntry::File(file) = e {
                                let path = Path::new(f_name);
                                if let Some(ext) = path.extension() {
                                    if ext == WASM_EXT {
                                        programs.push((
                                            metadata.path_from_file_id(file.id()),
                                            file.clone(),
                                        ));
                                    }
                                }
                            }
                        }
                    } else {
                        find_wasm_pgms(programs, metadata, dir);
                    }
                }
            }
        }

        let mut programs = Vec::<(PathBuf, FileMetadata)>::new();
        find_wasm_pgms(
            &mut programs,
            self.block_manager.metadata(),
            self.block_manager.metadata().root_directory(),
        );

        // Broken out to make borrowing happy.
        if let Some(program_mgr) = self.program_mgr.clone() {
            for (path, file) in programs {
                if let Ok(fh) = self.open_file(file.id(), OpenFileMode::Read) {
                    let version = file.get_latest();
                    let size = version.size();
                    if let Ok(program) = self.read_file(fh, 0, size as usize) {
                        info!("Adding existing program {:?} to runtime.", path);
                        program_mgr
                            .send(RuntimeManagerMsg::Start(WasmProgram {
                                name: path,
                                program,
                            }))
                            .unwrap()
                    }
                }
            }
        }
    }

    /// Return a reference to the `BlockManager`
    ///
    pub(crate) fn block_manager(&self) -> &BlockManager<B> {
        &self.block_manager
    }

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
            .new_directory(parent_id, name, self.user)?;

        // self.notify_listeners(UfsMessage::DirCreate(
        //     self.block_manager.metadata().path_from_dir_id(dir.id()),
        // ));

        debug!("end `create_directory`");
        Ok(dir)
    }

    /// Create a file
    ///
    pub(crate) fn create_file(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<(FileHandle, File), failure::Error> {
        debug!("--------");

        let file = self.block_manager.metadata_mut().new_file(dir_id, name)?;

        let fh = self.open_file_counter;
        self.open_file_counter = self.open_file_counter.wrapping_add(1);
        self.open_files.insert(fh, file.clone());

        // self.notify_listeners(UfsMessage::FileCreate(
        //     self.block_manager
        //         .metadata()
        //         .path_from_file_id(file.file_id),
        // ));

        debug!("`create_file`: {:?}, handle: {}", name, fh);
        Ok((fh, file))
    }

    /// Open a directory
    ///
    pub(crate) fn open_directory(&mut self, id: UfsUuid) -> Result<FileHandle, failure::Error> {
        debug!("--------");
        let dir = self.block_manager.metadata().get_directory(id)?;

        let fh = self.open_file_counter;
        self.open_file_counter = self.open_file_counter.wrapping_add(1);

        trace!("\t{:#?}", dir);
        self.open_dirs.insert(fh, dir);

        debug!("`open_directory`: {:?}, handle: {}", id, fh);
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

        // It seems reasonable to allow the WASM program an opportunity to do something with the
        // soon-to-be-deleted file, prior to it being relegated to the bit-bucket.
        if let Ok(file) = self
            .block_manager
            .metadata()
            .get_file_metadata_from_dir_and_name(dir_id, name)
        {
            // self.notify_listeners(UfsMessage::FileRemove(
            //     self.block_manager.metadata().path_from_file_id(file.id()),
            // ));

            if let Some(program_mgr) = &self.program_mgr {
                if let Ok(dir) = self.block_manager.metadata().get_directory(dir_id) {
                    if dir.is_wasm_dir() {
                        program_mgr.send(RuntimeManagerMsg::Stop(
                            self.block_manager.metadata().path_from_file_id(file.id()),
                        )).expect("unable to send message to Runtime Manager");
                    }
                }
            }
        }

        let free_blocks = self
            .block_manager
            .metadata_mut()
            .unlink_file(dir_id, name)?;

        for b in free_blocks {
            self.block_manager.recycle_block(b)
        }

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
        let file = match mode {
            OpenFileMode::Write => self.block_manager.metadata_mut().get_file_write_only(id)?,
            OpenFileMode::Read => self.block_manager.metadata().get_file_read_only(id)?,
            OpenFileMode::ReadWrite => self.block_manager.metadata_mut().get_file_read_write(id)?,
        };

        let fh = self.open_file_counter;
        self.open_file_counter = self.open_file_counter.wrapping_add(1);

        // self.notify_listeners(UfsMessage::FileOpen(
        //     self.block_manager
        //         .metadata()
        //         .path_from_file_id(file.file_id),
        // ));

        self.open_files.insert(fh, file);

        debug!("`open_file` {:?}, mode: {:?}, handle: {}", id, mode, fh);
        Ok(fh)
    }

    /// Close a file
    ///
    pub(crate) fn close_file(&mut self, handle: FileHandle) {
        debug!("-------");
        debug!("`close_file`: {}", handle);

        // Commit the file first, so that we can read it's contents if it's a program file to run.
        if let Some(file) = self.open_files.get(&handle) {
            debug!("\t{:?}", file);
            if let Err(e) = self.block_manager.metadata_mut().commit_file(file.clone()) {
                error!("{}", e);
            }
        }

        // Add any .wasm files, located in a .wasm directory, to the runtime.
        if let Some(program_mgr) = &self.program_mgr {
            if let Some(file) = self.open_files.get(&handle) {
                // This check is a bit of a hack. Basically, we only want to load the program if
                // it's new. For some reason FUSE will open and close a newly created file after the
                // new file is closed. So we check to see if the FileVersion is dirty here, since it
                // will only be so if we haven't already written it.
                if file.version.is_dirty() {
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
                                    let path = self
                                        .block_manager
                                        .metadata()
                                        .path_from_file_id(file.file_id);
                                    if let Some(ext) = path.extension() {
                                        if ext == WASM_EXT {
                                            info!("Adding program {:?} to runtime", name);
                                            let size = file.version.size();
                                            if let Ok(program) =
                                                self.read_file(handle, 0, size as usize)
                                            {
                                                program_mgr
                                                    .send(RuntimeManagerMsg::Start(WasmProgram {
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
        }

        match self.open_files.remove(&handle) {
            Some(file) => {
                // self.notify_listeners(UfsMessage::FileClose(
                //     self.block_manager
                //         .metadata()
                //         .path_from_file_id(file.file_id),
                // ));
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
        offset: u64,
    ) -> Result<usize, failure::Error> {
        debug!("-------");
        debug!("`write_file`: handle: {}", handle);

        let result = match &mut self.open_files.get_mut(&handle) {
            Some(file) => {
                let mut written = 0;
                while written < bytes.len() {
                    match self.block_manager.write(
                        file.version.nonce(),
                        offset + written as u64,
                        &bytes[written..],
                    ) {
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

                Ok(written)
            }
            None => {
                warn!("asked to write file not in the map {}", handle);
                Ok(0)
            }
        };

        // Down here to appease the Borrow Checker Gods
        if let Some(file) = self.open_files.get(&handle) {
            // self.notify_listeners(UfsMessage::FileWrite(
            //     self.block_manager
            //         .metadata()
            //         .path_from_file_id(file.file_id),
            //     bytes.to_vec(),
            // ));
        }

        result
    }

    /// Read bytes from a file
    ///
    ///
    pub(crate) fn read_file(
        &self,
        handle: FileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, failure::Error> {
        debug!("-------");
        debug!(
            "`read_file`: handle: {}, reading offset {}, size {}",
            handle, offset, size
        );

        if let Some(file) = self.open_files.get(&handle) {
            let block_size = self.block_manager.block_size();

            let start_block = (offset / block_size as u64) as usize;
            let mut start_offset = (offset % block_size as u64) as usize;

            let mut blocks = file.version.blocks().clone();
            trace!("reading from blocks {:?}", &blocks);
            let block_iter = &mut blocks.iter_mut().skip(start_block);
            trace!("current iterator {:?}", block_iter);

            let mut read = 0;
            let mut buffer = vec![0; size];
            let mut blocks_read = 0;
            while read < size {
                if let Some(block_number) = block_iter.next() {
                    if let Some(block) = self.block_manager.get_block(*block_number) {
                        trace!("reading block {:?}", &block);
                        if let Ok(bytes) = self.block_manager.read(
                            file.version.nonce(),
                            ((start_block + blocks_read) * block_size as usize) as u64,
                            block,
                        ) {
                            blocks_read += 1;

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
                // self.notify_listeners(UfsMessage::FileRead(
                //     self.block_manager
                //         .metadata()
                //         .path_from_file_id(file.file_id),
                //     buffer.clone(),
                // ));

                Ok(buffer)
            } else {
                Err(format_err!("Error reading file {}", handle))
            }
        } else {
            Err(format_err!("File not open {}", handle))
        }
    }

    pub(crate) fn set_permissions(&mut self, id: UfsUuid, perms: u16) {
        self.block_manager
            .metadata_mut()
            .set_unix_permissions(id, perms);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn open_file() {
        init();

        let mut ufs =
            UberFileSystem::new_memory("test", "foobar", "test", BlockSize::TwentyFortyEight, 100);

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

        let mut ufs =
            UberFileSystem::new_networked("test", "test", "test", "http://localhost:8888").unwrap();
        let test = include_str!("lib.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();

        assert_eq!(test.len(), ufs.write_file(h, test, 0).unwrap());
        let bytes = ufs.read_file(h, 0, test.len()).unwrap();
        assert_eq!(test, bytes.as_slice());

        // If we don't remove the file, the test fails on the next run.
        ufs.remove_file(root_id, "lib.rs");
    }

    #[test]
    fn read_and_write_file() {
        init();

        let mut ufs =
            UberFileSystem::new_memory("test", "foobar", "test", BlockSize::TwentyFortyEight, 100);
        let test = include_str!("lib.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();

        assert_eq!(test.len(), ufs.write_file(h, test, 0).unwrap());
        let bytes = ufs.read_file(h, 0, test.len()).unwrap();
        assert_eq!(test, bytes.as_slice());
    }

    #[test]
    fn small_chunks() {
        init();

        let chunk_size = 88;
        let mut ufs =
            UberFileSystem::new_memory("test", "foobar", "test", BlockSize::TwentyFortyEight, 100);
        let test = include_str!("lib.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test, 0).unwrap());

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
            offset += len as u64;
        });
    }

    #[test]
    fn large_chunks() {
        init();

        let chunk_size = 8888;
        let mut ufs =
            UberFileSystem::new_memory("test", "foobar", "test", BlockSize::TwentyFortyEight, 100);
        let test = include_str!("lib.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test, 0).unwrap());

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
            offset += len as u64;
        });
    }
}
