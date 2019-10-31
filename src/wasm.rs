//! Module for WASM Runtime
//!
//! We use wasmer as our WASM interpreter.
//!
mod callbacks;
pub(crate) mod manager;
pub(crate) mod message;

pub(crate) use {
    manager::{IofsEventRegistration, ProtoWasmProgram, RuntimeManager, RuntimeManagerMsg},
    message::{
        IofsDirMessage, IofsFileMessage, IofsMessage, IofsMessagePayload, IofsSystemMessage,
        WasmMessageSender,
    },
};

use {
    self::callbacks::*,
    crate::{
        block::BlockStorage,
        metadata::{DirectoryMetadata, File, FileHandle, Grant, GrantType},
        server::IofsNetworkMessage,
        OpenFileMode, UberFileSystem, UfsUuid,
    },
    crossbeam::crossbeam_channel,
    failure::{Backtrace, Context, Fail},
    log::{debug, error, info},
    std::{
        collections::HashMap,
        ffi::c_void,
        fmt::{self, Display},
        path::PathBuf,
        str,
        sync::{Arc, Mutex},
        thread::{spawn, JoinHandle},
    },
    wasm_exports::WasmMessage,
    wasmer_runtime::{func, imports, instantiate},
};

const WRITE_BUF_SIZE: usize = 2048;

struct FileWriteBuffer {
    buffer: [u8; WRITE_BUF_SIZE],
    len: usize,
    file_offset: u64,
}

pub(crate) enum WasmProcessMessage {
    IofsEvent(IofsMessage),
    NetworkEvent(IofsNetworkMessage),
}

/// The main interface between the file system and WASM
///
/// One of these is created when the file system loads a new WASM program. This struct maintains a
/// channel which the file system uses to send file system events to the WASM program. The WASM
/// program itself is started in the `start` associated function. Messages are received there and
/// forwarded to the executing WASM program.
pub(crate) struct WasmProcess<B: BlockStorage + 'static> {
    /// A unique identifier for the WASM program -- it's the path, and there can be only one.
    path: PathBuf,
    /// The bytes that comprise the program.
    program: Vec<u8>,
    /// The file system sends messages with sender...
    sender: crossbeam_channel::Sender<WasmProcessMessage>,
    /// we receive them using this.
    receiver: crossbeam_channel::Receiver<WasmProcessMessage>,
    /// A list of IDs of the files or directories which were the subjects of the synchronous method
    /// invocations to the file system -- we can filter notifications with these.
    sync_func_ids: Vec<UfsUuid>,
    /// IOFS access
    iofs: Arc<Mutex<UberFileSystem<B>>>,
    /// Write buffers for write_file
    write_buffers: HashMap<FileHandle, FileWriteBuffer>,
    /// Message registration channel sender
    message_registration_sender: crossbeam_channel::Sender<IofsEventRegistration>,
}

impl<B: BlockStorage> WasmProcess<B> {
    pub(in crate::wasm) fn new(
        path: PathBuf,
        program: Vec<u8>,
        message_registration_sender: crossbeam_channel::Sender<IofsEventRegistration>,
        iofs: Arc<Mutex<UberFileSystem<B>>>,
    ) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<WasmProcessMessage>();

        WasmProcess {
            path,
            program,
            sender,
            receiver,
            sync_func_ids: vec![],
            iofs,
            write_buffers: HashMap::new(),
            message_registration_sender,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.path.file_name().unwrap().to_str().unwrap()
    }

    pub(crate) fn path(&self) -> &str {
        self.path.to_str().unwrap()
    }

    pub(crate) fn get_sender(&self) -> crossbeam_channel::Sender<WasmProcessMessage> {
        self.sender.clone()
    }

    pub(crate) fn set_handles_message(&mut self, msg: WasmMessage) {
        self.message_registration_sender
            .send(IofsEventRegistration::Register(msg))
            .unwrap();
    }

    pub(crate) fn register_get_callback(&mut self, route: String) {
        self.message_registration_sender
            .send(IofsEventRegistration::RegisterHttpGet(route))
            .unwrap();
    }

    pub(crate) fn register_post_callback(&mut self, route: String) {
        self.message_registration_sender
            .send(IofsEventRegistration::RegisterHttpPost(route))
            .unwrap();
    }

    pub(crate) fn register_put_callback(&mut self, route: String) {
        self.message_registration_sender
            .send(IofsEventRegistration::RegisterHttpPut(route))
            .unwrap();
    }

    pub(crate) fn register_patch_callback(&mut self, route: String) {
        self.message_registration_sender
            .send(IofsEventRegistration::RegisterHttpPatch(route))
            .unwrap();
    }

    pub(crate) fn register_delete_callback(&mut self, route: String) {
        self.message_registration_sender
            .send(IofsEventRegistration::RegisterHttpDelete(route))
            .unwrap();
    }

    /// Check incoming message to see if we're the source.
    ///
    /// We don't want to be notified about things that we've done to the file system, so we maintain
    /// a list of ID's that are associated with each synchronous file system call. When a message
    /// arrives, we check to see if it's in our list, and if so, don't notify the WASM program.
    ///
    /// We maintain a simple FIFO queue of ID's. We know that messages arrive in the order that they
    /// are generated, so we just have to check the top of the list for a matching ID.
    ///
    /// This feels like it might be brittle. For one, we are just tracking ID's, and not message
    /// types. So if two messages have the same ID, then it's possible that one notification is due
    /// to something we did, and the other to another process in the file system.
    fn should_send_notification(&mut self, id: &UfsUuid) -> bool {
        debug!(
            "should_send_notifications: id: {}, list: {:#?}",
            id, self.sync_func_ids
        );
        if self.sync_func_ids.len() > 0 && *id == self.sync_func_ids[0] {
            self.sync_func_ids.remove(0);
            false
        } else {
            true
        }
    }

    pub(crate) fn open_file(
        &mut self,
        id: UfsUuid,
        mode: OpenFileMode,
    ) -> Result<FileHandle, failure::Error> {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        match guard
            .block_manager_mut()
            .metadata_mut()
            .check_wasm_program_grant(&self.path, GrantType::OpenFileInvocation)
        {
            Some(Grant::Allow) => match guard.open_file(id, mode) {
                Ok(handle) => {
                    self.sync_func_ids.push(id);
                    Ok(handle)
                }
                Err(e) => Err(e),
            },
            _ => Err(RuntimeErrorKind::IofsPermission.into()),
        }
    }

    pub(crate) fn close_file(&mut self, id: UfsUuid, handle: FileHandle) {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        // Flush the write buffer if necessary before closing the file.
        if let Some(buffer) = self.write_buffers.remove(&handle) {
            if buffer.len != 0 {
                // FIXME: What should we do if the write fails, but the close succeeds?
                // We'll just assume that since we have a write_buffer that we've got a write grant.
                guard
                    .write_file(handle, &buffer.buffer[0..buffer.len], buffer.file_offset)
                    .unwrap();
            }
        }

        match guard
            .block_manager_mut()
            .metadata_mut()
            .check_wasm_program_grant(&self.path, GrantType::CloseFileInvocation)
        {
            Some(Grant::Allow) => match guard.close_file(handle) {
                Ok(_) => {
                    self.sync_func_ids.push(id);
                }
                Err(_) => (),
            },
            _ => {}
        };
    }

    pub(crate) fn read_file(
        &mut self,
        id: UfsUuid,
        handle: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, failure::Error> {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        match guard
            .block_manager_mut()
            .metadata_mut()
            .check_wasm_program_grant(&self.path, GrantType::ReadFileInvocation)
        {
            Some(Grant::Allow) => match guard.read_file(handle, offset, size) {
                Ok(v) => {
                    self.sync_func_ids.push(id);
                    Ok(v)
                }
                Err(e) => Err(e),
            },
            _ => Err(RuntimeErrorKind::IofsPermission.into()),
        }
    }

    pub(crate) fn write_file<T: AsRef<[u8]>>(
        &mut self,
        id: UfsUuid,
        handle: FileHandle,
        bytes: T,
    ) -> Result<usize, failure::Error> {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        match guard
            .block_manager_mut()
            .metadata_mut()
            .check_wasm_program_grant(&self.path, GrantType::WriteFileInvocation)
        {
            Some(Grant::Allow) => {
                let bytes = bytes.as_ref();

                let buffer = self.write_buffers.entry(handle).or_insert(FileWriteBuffer {
                    buffer: [0; WRITE_BUF_SIZE],
                    len: 0,
                    file_offset: 0,
                });

                let mut bytes_written = 0;
                while bytes_written < bytes.len() {
                    let write_len =
                        std::cmp::min(WRITE_BUF_SIZE - buffer.len, bytes.len() - bytes_written);
                    buffer.buffer[buffer.len..buffer.len + write_len]
                        .copy_from_slice(&bytes[bytes_written..bytes_written + write_len]);
                    buffer.len += write_len;
                    bytes_written += write_len;

                    if buffer.len == WRITE_BUF_SIZE {
                        guard
                            .write_file(handle, &buffer.buffer, buffer.file_offset)
                            .expect("error writing bytes in WasmProcess::write_file");
                        buffer.file_offset += WRITE_BUF_SIZE as u64;
                        buffer.len = 0;

                        // Only post this if we actually did a write, and it was successful.
                        self.sync_func_ids.push(id);
                    }
                }

                Ok(bytes_written)
            }
            _ => Err(RuntimeErrorKind::IofsPermission.into()),
        }
    }

    pub(crate) fn create_file(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<(FileHandle, File), failure::Error> {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        match guard
            .block_manager_mut()
            .metadata_mut()
            .check_wasm_program_grant(&self.path, GrantType::CreateFileInvocation)
        {
            Some(Grant::Allow) => match guard.create_file(dir_id, name) {
                Ok((h, f)) => {
                    self.sync_func_ids.push(dir_id);
                    Ok((h, f))
                }
                Err(e) => Err(e),
            },
            _ => Err(RuntimeErrorKind::IofsPermission.into()),
        }
    }

    pub(crate) fn create_directory(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<DirectoryMetadata, failure::Error> {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        match guard
            .block_manager_mut()
            .metadata_mut()
            .check_wasm_program_grant(&self.path, GrantType::CreateDirectoryInvocation)
        {
            Some(Grant::Allow) => match guard.create_directory(dir_id, name) {
                Ok(dm) => {
                    self.sync_func_ids.push(dir_id);
                    Ok(dm)
                }
                Err(e) => Err(e),
            },
            _ => Err(RuntimeErrorKind::IofsPermission.into()),
        }
    }

    pub(crate) fn open_directory(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<UfsUuid, failure::Error> {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        match guard
            .block_manager_mut()
            .metadata_mut()
            .check_wasm_program_grant(&self.path, GrantType::OpenDirectoryInvocation)
        {
            Some(Grant::Allow) => guard.open_sub_directory(dir_id, name),
            _ => Err(RuntimeErrorKind::IofsPermission.into()),
        }
    }
}

impl<B: BlockStorage> WasmProcess<B> {
    pub(crate) fn start(mut process: WasmProcess<B>) -> JoinHandle<Result<(), failure::Error>> {
        debug!("--------");
        debug!("start {:?}", process.path);
        spawn(move || {
            // This is the mapping of functions imported to the WASM interpreter.
            let import_object = imports! {
                "env" => {
                    "__register_for_callback" => func!(__register_for_callback<B>),
                    "__register_get_handler" => func!(__register_get_handler<B>),
                    "__register_post_handler" => func!(__register_post_handler<B>),
                    "__register_put_handler" => func!(__register_put_handler<B>),
                    "__register_patch_handler" => func!(__register_patch_handler<B>),
                    "__register_delete_handler" => func!(__register_delete_handler<B>),
                    "__print" => func!(__print<B>),
                    "__open_file" => func!(__open_file<B>),
                    "__close_file" => func!(__close_file<B>),
                    "__read_file" => func!(__read_file<B>),
                    "__write_file" => func!(__write_file<B>),
                    "__create_file" => func!(__create_file<B>),
                    "__create_directory" => func!(__create_directory<B>),
                    "__open_directory" => func!(__open_directory<B>),
                    "pong" => func!(pong),
                },
            };

            let mut instance = match instantiate(process.program.as_slice(), &import_object) {
                Ok(i) => {
                    info!("Instantiated WASM program {}", process.name());
                    i
                }
                Err(e) => {
                    error!(
                        "Error {} -- unable to instantiate WASM program: {}",
                        e,
                        process.path()
                    );
                    return Err(RuntimeErrorKind::ProgramInstantiation.into());
                }
            };

            // Clear the program buffer, and save a little memory?
            process.program = vec![];

            instance.context_mut().data = &mut process as *mut _ as *mut c_void;

            let root_id;
            {
                let guard = process.iofs.clone();
                let guard = guard.lock().expect("poisoned iofs lock");
                root_id = guard.get_root_directory_id();
            }

            let mut msg_sender = WasmMessageSender::new(&mut instance, root_id);

            loop {
                let message = process.receiver.recv().unwrap();
                match message {
                    WasmProcessMessage::IofsEvent(message) => {
                        debug!(
                            "{:?} dispatching file system message {:#?}",
                            process.path, message
                        );
                        match &message {
                            IofsMessage::SystemMessage(m) => match m {
                                IofsSystemMessage::Shutdown => {
                                    msg_sender.send_shutdown()?;
                                }
                                IofsSystemMessage::Ping => {
                                    msg_sender.send_ping()?;
                                }
                            },
                            IofsMessage::FileMessage(m) => match m {
                                IofsFileMessage::Create(payload) => {
                                    if process.should_send_notification(&payload.parent_id) {
                                        msg_sender.send_file_create(&payload)?;
                                    }
                                }
                                IofsFileMessage::Delete(payload) => {
                                    if process.should_send_notification(&payload.target_id) {
                                        msg_sender.send_file_delete(&payload)?;
                                    }
                                }
                                IofsFileMessage::Open(payload) => {
                                    if process.should_send_notification(&payload.target_id) {
                                        msg_sender.send_file_open(&payload)?;
                                    }
                                }
                                IofsFileMessage::Close(payload) => {
                                    if process.should_send_notification(&payload.target_id) {
                                        msg_sender.send_file_close(&payload)?;
                                    }
                                }
                                IofsFileMessage::Write(payload) => {
                                    if process.should_send_notification(&payload.target_id) {
                                        msg_sender.send_file_write(&payload)?;
                                    }
                                }
                                IofsFileMessage::Read(payload) => {
                                    if process.should_send_notification(&payload.target_id) {
                                        msg_sender.send_file_read(&payload)?;
                                    }
                                }
                            },
                            IofsMessage::DirMessage(m) => match m {
                                IofsDirMessage::Create(payload) => {
                                    if process.should_send_notification(&payload.parent_id) {
                                        msg_sender.send_dir_create(&payload)?;
                                    }
                                }
                                IofsDirMessage::Delete(payload) => {
                                    if process.should_send_notification(&payload.target_id) {
                                        msg_sender.send_dir_delete(&payload)?;
                                    }
                                }
                            },
                        };
                        if let IofsMessage::SystemMessage(IofsSystemMessage::Shutdown) = message {
                            info!("WASM program {} shutting down", process.name());
                            break;
                        }
                    }
                    WasmProcessMessage::NetworkEvent(mut message) => {
                        debug!(
                            "{:?} dispatching network message {:#?}",
                            process.path, message
                        );
                        match &mut message {
                            IofsNetworkMessage::Get(msg) => {
                                match msg_sender.send_http_get(msg) {
                                    Ok(response) => msg.respond(response),
                                    Err(e) => msg.respond(e.to_string()),
                                };
                            }
                            IofsNetworkMessage::Post(msg) => match msg_sender.send_http_post(msg) {
                                Ok(response) => msg.respond(response),
                                Err(e) => msg.respond(e.to_string()),
                            },
                            IofsNetworkMessage::Put(msg) => match msg_sender.send_http_put(msg) {
                                Ok(response) => msg.respond(response),
                                Err(e) => msg.respond(e.to_string()),
                            },
                            IofsNetworkMessage::Patch(msg) => match msg_sender.send_http_patch(msg)
                            {
                                Ok(response) => msg.respond(response),
                                Err(e) => msg.respond(e.to_string()),
                            },
                            IofsNetworkMessage::Delete(msg) => {
                                match msg_sender.send_http_delete(msg) {
                                    Ok(response) => msg.respond(response),
                                    Err(e) => msg.respond(e.to_string()),
                                }
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

fn process_iofs_message<B: BlockStorage>(process: WasmProcess<B>, message: IofsNetworkMessage) {}

#[derive(Debug)]
struct RuntimeError {
    inner: Context<RuntimeErrorKind>,
}

impl RuntimeError {
    pub fn kind(&self) -> RuntimeErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for RuntimeError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
enum RuntimeErrorKind {
    #[fail(display = "Unable to start WASM program.")]
    ProgramInstantiation,
    #[fail(display = "Error invoking function in WASM.")]
    FunctionInvocation,
    #[fail(display = "Error invoking IOFS function in WASM.")]
    IofsInvocation,
    #[fail(display = "Insufficient permissions to execute function.")]
    IofsPermission,
}

impl From<RuntimeErrorKind> for RuntimeError {
    fn from(kind: RuntimeErrorKind) -> Self {
        RuntimeError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<RuntimeErrorKind>> for RuntimeError {
    fn from(inner: Context<RuntimeErrorKind>) -> Self {
        RuntimeError { inner: inner }
    }
}
