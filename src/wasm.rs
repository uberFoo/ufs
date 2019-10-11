//! Module for WASM Runtime
//!
//! We use wasmer as our WASM interpreter.
//!
mod callbacks;
pub(crate) mod manager;
pub(crate) mod message;

pub(crate) use {
    manager::{RuntimeManager, RuntimeManagerMsg, WasmProgram},
    message::{
        IofsDirMessage, IofsFileMessage, IofsMessage, IofsMessagePayload, IofsSystemMessage,
        WasmMessageSender,
    },
};

use {
    self::callbacks::*,
    crate::{
        block::BlockStorage,
        metadata::{DirectoryMetadata, File, FileHandle},
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
    sender: crossbeam_channel::Sender<IofsMessage>,
    /// we receive them using this.
    receiver: crossbeam_channel::Receiver<IofsMessage>,
    /// A list of IDs of the files or directories which were the subjects of the synchronous method
    /// invocations to the file system -- we can filter notifications with these.
    sync_func_ids: Vec<UfsUuid>,
    /// IOFS access
    iofs: Arc<Mutex<UberFileSystem<B>>>,
    /// Notification delivery registration tracking.
    pub notifications: HashMap<WasmMessage, bool>,
}

impl<B: BlockStorage> WasmProcess<B> {
    pub(in crate::wasm) fn new(
        path: PathBuf,
        program: Vec<u8>,
        iofs: Arc<Mutex<UberFileSystem<B>>>,
    ) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<IofsMessage>();

        let mut notifications = HashMap::new();
        notifications.insert(WasmMessage::Shutdown, false);
        notifications.insert(WasmMessage::Ping, false);
        notifications.insert(WasmMessage::FileCreate, false);
        notifications.insert(WasmMessage::DirCreate, false);
        notifications.insert(WasmMessage::FileDelete, false);
        notifications.insert(WasmMessage::DirDelete, false);
        notifications.insert(WasmMessage::DirDelete, false);
        notifications.insert(WasmMessage::FileClose, false);
        notifications.insert(WasmMessage::FileWrite, false);

        WasmProcess {
            path,
            program,
            sender,
            receiver,
            sync_func_ids: vec![],
            iofs,
            notifications,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.path.file_name().unwrap().to_str().unwrap()
    }

    pub(crate) fn path(&self) -> &str {
        self.path.to_str().unwrap()
    }

    pub(crate) fn get_sender(&self) -> crossbeam_channel::Sender<IofsMessage> {
        self.sender.clone()
    }

    pub(crate) fn does_handle_message(&mut self, msg: WasmMessage) -> bool {
        // If the entry does not exist, insert the default bool value (false).
        *self.notifications.entry(msg).or_default()
    }

    pub(crate) fn set_handles_message(&mut self, msg: WasmMessage) {
        self.notifications.insert(msg, true);
    }

    pub(crate) fn unset_handles_message(&mut self, msg: WasmMessage) {
        self.notifications.entry(msg).and_modify(|e| *e = true);
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

        self.sync_func_ids.push(id);

        guard.open_file(id, mode)
    }

    pub(crate) fn close_file(&mut self, id: UfsUuid, handle: FileHandle) {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        self.sync_func_ids.push(id);

        guard.close_file(handle);
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

        self.sync_func_ids.push(id);

        guard.read_file(handle, offset, size)
    }

    pub(crate) fn write_file<T: AsRef<[u8]>>(
        &mut self,
        id: UfsUuid,
        handle: FileHandle,
        bytes: T,
        offset: u64,
    ) -> Result<usize, failure::Error> {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        self.sync_func_ids.push(id);

        guard.write_file(handle, bytes.as_ref(), offset)
    }

    pub(crate) fn create_file(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<(FileHandle, File), failure::Error> {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        self.sync_func_ids.push(dir_id);

        guard.create_file(dir_id, name)
    }

    pub(crate) fn create_directory(
        &mut self,
        dir_id: UfsUuid,
        name: &str,
    ) -> Result<DirectoryMetadata, failure::Error> {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        self.sync_func_ids.push(dir_id);

        guard.create_directory(dir_id, name)
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
                let message = process.receiver.recv()?;
                debug!("WasmProcess dispatching message {:#?}", message);
                match &message {
                    IofsMessage::SystemMessage(m) => match m {
                        IofsSystemMessage::Shutdown => {
                            if process.does_handle_message(WasmMessage::Shutdown) {
                                msg_sender.send_shutdown()?;
                            }
                        }
                        IofsSystemMessage::Ping => {
                            if process.does_handle_message(WasmMessage::Ping) {
                                msg_sender.send_ping()?;
                            }
                        }
                    },
                    IofsMessage::FileMessage(m) => match m {
                        IofsFileMessage::Create(payload) => {
                            if process.does_handle_message(WasmMessage::FileCreate)
                                && process.should_send_notification(&payload.parent_id)
                            {
                                msg_sender.send_file_create(
                                    &payload.target_path,
                                    &payload.target_id,
                                    &payload.parent_id,
                                )?;
                            }
                        }
                        IofsFileMessage::Delete(payload) => {
                            if process.does_handle_message(WasmMessage::FileDelete)
                                && process.should_send_notification(&payload.target_id)
                            {
                                msg_sender
                                    .send_file_delete(&payload.target_path, &payload.target_id)?;
                            }
                        }
                        IofsFileMessage::Open(payload) => {
                            if process.does_handle_message(WasmMessage::FileClose)
                                && process.should_send_notification(&payload.target_id)
                            {
                                msg_sender
                                    .send_file_open(&payload.target_path, &payload.target_id)?;
                            }
                        }
                        IofsFileMessage::Close(payload) => {
                            if process.does_handle_message(WasmMessage::FileClose)
                                && process.should_send_notification(&payload.target_id)
                            {
                                msg_sender
                                    .send_file_close(&payload.target_path, &payload.target_id)?;
                            }
                        }
                        IofsFileMessage::Write(payload) => {
                            if process.does_handle_message(WasmMessage::FileWrite)
                                && process.should_send_notification(&payload.target_id)
                            {
                                msg_sender
                                    .send_file_write(&payload.target_path, &payload.target_id)?;
                            }
                        }
                        _ => unimplemented!(),
                    },
                    IofsMessage::DirMessage(m) => match m {
                        IofsDirMessage::Create(payload) => {
                            if process.does_handle_message(WasmMessage::DirCreate)
                                && process.should_send_notification(&payload.parent_id)
                            {
                                msg_sender
                                    .send_dir_create(&payload.target_path, &payload.target_id)?;
                            }
                        }
                        IofsDirMessage::Delete(payload) => {
                            if process.does_handle_message(WasmMessage::DirDelete)
                                && process.should_send_notification(&payload.target_id)
                            {
                                msg_sender
                                    .send_dir_delete(&payload.target_path, &payload.target_id)?
                            }
                        }
                    },
                };
                if let IofsMessage::SystemMessage(IofsSystemMessage::Shutdown) = message {
                    info!("WASM program {} shutting down", process.name());
                    break;
                }
            }

            Ok(())
        })
    }
}

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
    IOFSInvocation,
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
