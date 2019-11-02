use {
    crate::{
        block::{
            manager::BlockManager, map::BlockMap, BlockCardinality, BlockSize, BlockStorage,
            FileStore, MemoryStore, NetworkStore,
        },
        crypto::make_fs_key,
        jwt::{new_jwt, UserClaims, JWT},
        metadata::{
            DirectoryEntry, DirectoryMetadata, File, FileHandle, FileMetadata, FileSize, Metadata,
            WASM_EXT,
        },
        server::UfsRemoteServer,
        wasm::{
            IofsDirMessage, IofsFileMessage, IofsMessage, IofsMessagePayload, ProtoWasmProgram,
            RuntimeManager, RuntimeManagerMsg,
        },
        UfsUuid,
    },
    crossbeam::crossbeam_channel,
    failure::format_err,
    futures::sync::oneshot,
    log::{debug, error, info, trace, warn},
    reqwest::IntoUrl,
    std::{
        collections::HashMap,
        ops::{Deref, DerefMut},
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        thread::JoinHandle,
    },
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
    remote_stop_signal: Option<oneshot::Sender<()>>,
    remote_thread: Option<JoinHandle<Result<(), failure::Error>>>,
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
        info!("Initializing Wasm runtime");
        let mut runtime_mgr = RuntimeManager::new(inner.clone(), receiver);

        // Start the remote FS listener
        let (remote_stop_signal, remote_thread) = match remote_port {
            Some(port) => {
                info!("Initializing Web Server");
                let (tx, rx) = oneshot::channel();
                let remote = UfsRemoteServer::new(inner.clone(), port);
                runtime_mgr.set_http_receiver(remote.get_http_receiver());

                let remote_thread = UfsRemoteServer::start(remote, rx);
                (Some(tx), Some(remote_thread))
            }
            None => (None, None),
        };

        let runtime_mgr_thread = RuntimeManager::start(runtime_mgr);

        let mounter = UfsMounter {
            inner,
            remote_stop_signal,
            remote_thread,
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
            thread
                .join()
                .expect("unable to join RuntimeManager thread")
                .expect("error running RuntimeManager thread");
        }

        if let Some(oneshot) = self.remote_stop_signal.take() {
            oneshot.send(()).unwrap();
        }
        if let Some(thread) = self.remote_thread.take() {
            info!("Waiting for HTTP Server to shutdown.");
            thread
                .join()
                .expect("unable to join HTTP Server thread")
                .expect("error running HTTP Server thread");
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
pub struct UberFileSystem<B: BlockStorage> {
    /// The ID of the file system
    id: UfsUuid,
    /// JWTs are passed out as authentication tokens. This is a mapping from user ID to a tuple
    /// of the JWT ID and the user's encryption/decryption key
    tokens: HashMap<UfsUuid, (UfsUuid, [u8; 32])>,
    /// The ID of the user that mounted the file system
    user: UfsUuid,
    /// The block manager -- where all the magic happens
    block_manager: BlockManager<B>,
    /// A mapping of file handles to File structures
    open_files: HashMap<FileHandle, File>,
    /// A mapping of file handles to DirectoryMetadata structures
    open_dirs: HashMap<FileHandle, DirectoryMetadata>,
    /// A counter so that we know what the next file handle should be
    open_file_counter: FileHandle,
    /// The Wasm program manager
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
            tokens: HashMap::new(),
            user: UfsUuid::new_user(user.as_ref()),
            block_manager,
            open_files: HashMap::new(),
            open_dirs: HashMap::new(),
            open_file_counter: 0,
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
            tokens: HashMap::new(),
            user: UfsUuid::new_user(user.as_ref()),
            block_manager,
            open_files: HashMap::new(),
            open_dirs: HashMap::new(),
            open_file_counter: 0,
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
            tokens: HashMap::new(),
            user: UfsUuid::new_user(user.as_ref()),
            block_manager,
            open_files: HashMap::new(),
            open_dirs: HashMap::new(),
            open_file_counter: 0,
            program_mgr: None,
        })
    }
}

impl<B: BlockStorage> UberFileSystem<B> {
    /// Log a user into the file system
    pub fn login(&mut self, user: String, password: String) -> Option<JWT> {
        if let Some(user) = self.block_manager.metadata().get_user(user, password) {
            let jti = user.0.new_with_timestamp();
            self.tokens
                .entry(user.0)
                .and_modify(|t| *t = (jti, user.1))
                .or_insert((jti, user.1));

            Some(new_jwt(
                UserClaims {
                    iss: self.id,
                    sub: user.0,
                    jti,
                },
                "secret".as_ref(),
            ))
        } else {
            None
        }
    }

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
                    if let Ok(program) = self.read_file(fh, 0, size as u32) {
                        info!("Adding existing program {:?} to runtime.", path);
                        program_mgr
                            .send(RuntimeManagerMsg::Start(ProtoWasmProgram::new(
                                path, program,
                            )))
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

    /// Return a mutable reference to the `BlockManager`
    ///
    pub(crate) fn block_manager_mut(&mut self) -> &mut BlockManager<B> {
        &mut self.block_manager
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

        if let Some(program_mgr) = &self.program_mgr {
            program_mgr
                .send(RuntimeManagerMsg::IofsMessage(IofsMessage::DirMessage(
                    IofsDirMessage::Create(IofsMessagePayload {
                        target_path: self.block_manager.metadata().path_from_dir_id(dir.id()),
                        target_id: dir.id(),
                        parent_id,
                    }),
                )))
                .expect("Wasm Runtime went away");
        }

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

        if let Some(program_mgr) = &self.program_mgr {
            program_mgr
                .send(RuntimeManagerMsg::IofsMessage(IofsMessage::FileMessage(
                    IofsFileMessage::Create(IofsMessagePayload {
                        target_path: self
                            .block_manager
                            .metadata()
                            .path_from_file_id(file.file_id),
                        target_id: file.file_id,
                        parent_id: dir_id,
                    }),
                )))
                .expect("Wasm Runtime went away");
        }

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

    /// Remove a directory
    ///
    pub(crate) fn remove_directory(
        &mut self,
        parent_id: UfsUuid,
        name: &str,
    ) -> Result<(), failure::Error> {
        if let Ok(dir) = self
            .block_manager
            .metadata()
            .get_dir_metadata_from_dir_and_name(parent_id, name)
        {
            if let Some(program_mgr) = &self.program_mgr {
                program_mgr
                    .send(RuntimeManagerMsg::IofsMessage(IofsMessage::DirMessage(
                        IofsDirMessage::Delete(IofsMessagePayload {
                            target_path: self.block_manager.metadata().path_from_dir_id(dir.id()),
                            target_id: dir.id(),
                            parent_id,
                        }),
                    )))
                    .expect("Wasm Runtime went away");
            }
        }

        self.block_manager
            .metadata_mut()
            .remove_directory(parent_id, name)
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
            if let Some(program_mgr) = &self.program_mgr {
                program_mgr
                    .send(RuntimeManagerMsg::IofsMessage(IofsMessage::FileMessage(
                        IofsFileMessage::Delete(IofsMessagePayload {
                            target_path: self.block_manager.metadata().path_from_file_id(file.id()),
                            target_id: file.id(),
                            parent_id: dir_id,
                        }),
                    )))
                    .expect("Wasm Runtime went away");
            }

            // self.notify_listeners(UfsMessage::FileRemove(
            //     self.block_manager.metadata().path_from_file_id(file.id()),
            // ));

            if let Some(program_mgr) = &self.program_mgr {
                if let Ok(dir) = self.block_manager.metadata().get_directory(dir_id) {
                    if dir.is_wasm_dir() {
                        let path = self.block_manager.metadata().path_from_file_id(file.id());
                        program_mgr
                            .send(RuntimeManagerMsg::Stop(path.clone()))
                            .expect("unable to send message to Runtime Manager");
                        self.block_manager
                            .metadata_mut()
                            .remove_wasm_program_grants(&path)
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

        if let Some(program_mgr) = &self.program_mgr {
            program_mgr
                .send(RuntimeManagerMsg::IofsMessage(IofsMessage::FileMessage(
                    IofsFileMessage::Open(IofsMessagePayload {
                        target_path: self
                            .block_manager
                            .metadata()
                            .path_from_file_id(file.file_id),
                        target_id: file.file_id,
                        parent_id: self
                            .block_manager
                            .metadata()
                            .get_file_metadata(file.file_id)
                            .expect("should not fail in open_file")
                            .dir_id(),
                    }),
                )))
                .expect("Wasm Runtime went away");
        }

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
    pub(crate) fn close_file(&mut self, handle: FileHandle) -> Result<(), ()> {
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
                                                self.read_file(handle, 0, size as u32)
                                            {
                                                // Add the Wasm program to the runtime
                                                self.block_manager
                                                    .metadata_mut()
                                                    .add_wasm_program_grants(path.to_path_buf());
                                                program_mgr
                                                    .send(RuntimeManagerMsg::Start(
                                                        ProtoWasmProgram::new(
                                                            path.to_path_buf(),
                                                            program,
                                                        ),
                                                    ))
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
                if let Some(program_mgr) = &self.program_mgr {
                    program_mgr
                        .send(RuntimeManagerMsg::IofsMessage(IofsMessage::FileMessage(
                            IofsFileMessage::Close(IofsMessagePayload {
                                target_path: self
                                    .block_manager
                                    .metadata()
                                    .path_from_file_id(file.file_id),
                                target_id: file.file_id,
                                parent_id: self
                                    .block_manager
                                    .metadata()
                                    .get_file_metadata(file.file_id)
                                    .expect("should not fail in close_file")
                                    .dir_id(),
                            }),
                        )))
                        .expect("Wasm Runtime went away");
                }

                Ok(())

                // self.notify_listeners(UfsMessage::FileClose(
                //     self.block_manager
                //         .metadata()
                //         .path_from_file_id(file.file_id),
                // ));
            }
            None => {
                warn!("asked to close a file not in the map {}", handle);
                Err(())
            }
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
            if let Some(program_mgr) = &self.program_mgr {
                program_mgr
                    .send(RuntimeManagerMsg::IofsMessage(IofsMessage::FileMessage(
                        IofsFileMessage::Write(IofsMessagePayload {
                            target_path: self
                                .block_manager
                                .metadata()
                                .path_from_file_id(file.file_id),
                            target_id: file.file_id,
                            parent_id: self
                                .block_manager
                                .metadata()
                                .get_file_metadata(file.file_id)
                                .expect("should not fail in write_file")
                                .dir_id(),
                        }),
                    )))
                    .expect("Wasm Runtime went away");
            }

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
        size: u32,
    ) -> Result<Vec<u8>, failure::Error> {
        debug!("-------");
        debug!(
            "`read_file`: handle: {}, reading offset {}, size {}",
            handle, offset, size
        );

        if let Some(file) = self.open_files.get(&handle) {
            let blocks = file.version.blocks().clone();
            // This is the index into the file version's blocks from which we're reading.
            let mut read_block = 0;
            // This offset is the length of the blocks skipped over to get to the file offset.
            let mut block_length_offset: u64 = 0;
            for block_number in &blocks {
                let block = self
                    .block_manager
                    .get_block(*block_number)
                    .expect("block doesn't exist in read_file");
                if (block_length_offset + block.size() as u64) < offset {
                    block_length_offset += block.size() as u64;
                    read_block += 1;
                } else {
                    break;
                }
            }

            let mut read: u32 = 0;
            let mut block_read_offset = (offset - block_length_offset) as u32;
            let mut buffer = vec![0; size as usize];
            while read < size {
                if let Some(block) = self.block_manager.get_block(blocks[read_block]) {
                    if let Ok(bytes) =
                        self.block_manager
                            .read(file.version.nonce(), block_length_offset, block)
                    {
                        let block_len = bytes.len() as u32;
                        let bytes_to_read =
                            std::cmp::min(size - read, block_len - block_read_offset);

                        buffer[read as usize..(read + bytes_to_read) as usize].copy_from_slice(
                            &bytes[block_read_offset as usize
                                ..(block_read_offset + bytes_to_read) as usize],
                        );
                        read += bytes_to_read;
                        if read < size {
                            assert!(read_block + 1 < blocks.len());
                            read_block += 1;
                            block_length_offset += block_len as u64;
                        }
                    }
                }
                block_read_offset = 0;
            }

            if buffer.len() == size as usize {
                if let Some(program_mgr) = &self.program_mgr {
                    program_mgr
                        .send(RuntimeManagerMsg::IofsMessage(IofsMessage::FileMessage(
                            IofsFileMessage::Read(IofsMessagePayload {
                                target_path: self
                                    .block_manager
                                    .metadata()
                                    .path_from_file_id(file.file_id),
                                target_id: file.file_id,
                                parent_id: self
                                    .block_manager
                                    .metadata()
                                    .get_file_metadata(file.file_id)
                                    .expect("should not fail in write_file")
                                    .dir_id(),
                            }),
                        )))
                        .expect("Wasm Runtime went away");
                }

                // self.notify_listeners(UfsMessage::FileRead(
                //     self.block_manager
                //         .metadata()
                //         .path_from_file_id(file.file_id),
                //     buffer.clone(),
                // ));

                debug!("read_file success");

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

    //
    //
    // Functions specifically for Rust-side WASM related use.
    //
    //

    /// Return file size
    ///
    /// Used in the WASM file read implementation in order to know how many bytes to read.
    ///
    pub(crate) fn get_file_size(&self, handle: FileHandle) -> Result<FileSize, failure::Error> {
        if let Some(file) = self.open_files.get(&handle) {
            Ok(file.version.size())
        } else {
            Err(format_err!("File not open {}", handle))
        }
    }

    /// Open a sub-directory
    ///
    pub(crate) fn open_sub_directory(
        &mut self,
        pid: UfsUuid,
        name: &str,
    ) -> Result<UfsUuid, failure::Error> {
        match self
            .block_manager
            .metadata()
            .get_dir_metadata_from_dir_and_name(pid, name)
        {
            Ok(dir_meta) => Ok(dir_meta.id()),
            Err(e) => Err(e),
        }
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

        // User and password on test file system are both empty
        UberFileSystem::new_networked("", "", "test", "http://localhost:8888").unwrap();
        let test = include_str!("wasm.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();

        assert_eq!(test.len(), ufs.write_file(h, test, 0).unwrap());
        let bytes = ufs.read_file(h, 0, test.len() as u32).unwrap();
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
        let bytes = ufs.read_file(h, 0, test.len() as u32).unwrap();
        assert_eq!(test, bytes.as_slice());
    }

    #[test]
    fn read_small_chunks() {
        init();

        let chunk_size = 88;
        let mut ufs =
            UberFileSystem::new_memory("test", "foobar", "test", BlockSize::TwentyFortyEight, 100);
        let test = include_str!("fuse.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test, 0).unwrap());

        let mut offset = 0;
        test.chunks(chunk_size).for_each(|test_bytes| {
            let bytes = ufs.read_file(h, offset, test_bytes.len() as u32).unwrap();
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
    fn read_large_chunks() {
        init();

        let chunk_size = 8888;
        let mut ufs =
            UberFileSystem::new_memory("test", "foobar", "test", BlockSize::TwentyFortyEight, 100);
        let test = include_str!("fuse.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();
        assert_eq!(test.len(), ufs.write_file(h, test, 0).unwrap());

        let mut offset = 0;
        test.chunks(chunk_size).for_each(|test_bytes| {
            let bytes = ufs.read_file(h, offset, test_bytes.len() as u32).unwrap();
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
    fn small_chunks() {
        init();

        let write_chunk_size = 77;
        let read_chunk_size = 88;
        let mut ufs =
            UberFileSystem::new_memory("test", "foobar", "test", BlockSize::TwentyFortyEight, 1000);
        let test = include_str!("fuse.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();
        let mut offset = 0;
        test.chunks(write_chunk_size).for_each(|write_bytes| {
            assert_eq!(
                write_bytes.len(),
                ufs.write_file(h, write_bytes, offset).unwrap()
            );
            offset += write_chunk_size as u64;
        });

        let mut offset = 0;
        test.chunks(read_chunk_size).for_each(|test_bytes| {
            let bytes = ufs.read_file(h, offset, test_bytes.len() as u32).unwrap();
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

        let write_chunk_size = 7777;
        let read_chunk_size = 8888;
        let mut ufs =
            UberFileSystem::new_memory("test", "foobar", "test", BlockSize::TwentyFortyEight, 100);
        let test = include_str!("fuse.rs").as_bytes();

        let root_id = ufs.block_manager.metadata().root_directory().id();
        let (h, _) = ufs.create_file(root_id, "lib.rs").unwrap();
        let mut offset = 0;
        test.chunks(write_chunk_size).for_each(|write_bytes| {
            assert_eq!(
                write_bytes.len(),
                ufs.write_file(h, write_bytes, offset).unwrap()
            );
            offset += write_chunk_size as u64;
        });

        let mut offset = 0;
        test.chunks(read_chunk_size).for_each(|test_bytes| {
            let bytes = ufs.read_file(h, offset, test_bytes.len() as u32).unwrap();
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
