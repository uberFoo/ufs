//! Module for WASM Runtime
//!
//! We use wasmer as our WASM interpreter.
//!
mod callbacks;
pub(crate) mod manager;
pub(crate) mod message;

pub(crate) use {
    manager::{RuntimeManager, RuntimeManagerMsg, WasmProgram},
    message::{IofsDirMessage, IofsFileMessage, IofsMessage, IofsSystemMessage, WasmMessageSender},
};

use {
    self::callbacks::*,
    crate::{block::BlockStorage, UberFileSystem},
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

/// Communication between WASM and Rust
///
/// This is the means whereby Rust and WASM may communicate. It's a shared memory context that
/// contains a handle to the file system that WASM programs may use to invoke file system functions.
/// It also contains a list of handlers that the WASM program has registered to be called when file
/// system events occur.
pub(in crate::wasm) struct WasmContext<B: BlockStorage> {
    /// A non-unique identifier for the WASM program.  Uniqueness may be virtuous.
    pub(in crate::wasm) path: PathBuf,
    /// The bytes that comprise the program.
    pub(in crate::wasm) program: Vec<u8>,
    /// Notification delivery registration tracking.
    pub notifications: HashMap<WasmMessage, bool>,
    /// IOFS access
    pub iofs: Arc<Mutex<UberFileSystem<B>>>,
}

impl<B: BlockStorage> WasmContext<B> {
    pub(crate) fn new(
        path: PathBuf,
        program: Vec<u8>,
        iofs: Arc<Mutex<UberFileSystem<B>>>,
    ) -> Self {
        let mut handlers = HashMap::new();
        handlers.insert(WasmMessage::Shutdown, false);
        handlers.insert(WasmMessage::Ping, false);
        handlers.insert(WasmMessage::FileCreate, false);
        handlers.insert(WasmMessage::DirCreate, false);
        handlers.insert(WasmMessage::FileDelete, false);
        handlers.insert(WasmMessage::DirDelete, false);
        handlers.insert(WasmMessage::DirDelete, false);
        handlers.insert(WasmMessage::FileClose, false);
        handlers.insert(WasmMessage::FileWrite, false);

        WasmContext {
            path,
            program,
            notifications: handlers,
            iofs,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.path.file_name().unwrap().to_str().unwrap()
    }

    pub(crate) fn path(&self) -> &str {
        self.path.to_str().unwrap()
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
}

/// The main interface between the file system and WASM
///
/// One of these is created when the file system loads a new WASM program. This struct maintains a
/// channel which the file system uses to send file system events to the WASM program. The WASM
/// program itself is started in the `start` associated function. Messages are received there and
/// forwarded to the executing WASM program.
pub(crate) struct WasmProcess<B: BlockStorage + 'static> {
    sender: crossbeam_channel::Sender<IofsMessage>,
    receiver: crossbeam_channel::Receiver<IofsMessage>,
    wasm_context: WasmContext<B>,
}

impl<B: BlockStorage> WasmProcess<B> {
    pub(in crate::wasm) fn new(ctx: WasmContext<B>) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<IofsMessage>();
        WasmProcess {
            sender,
            receiver,
            wasm_context: ctx,
        }
    }

    pub(crate) fn get_sender(&self) -> crossbeam_channel::Sender<IofsMessage> {
        self.sender.clone()
    }
}

impl<B: BlockStorage> WasmProcess<B> {
    pub(crate) fn start(mut process: WasmProcess<B>) -> JoinHandle<Result<(), failure::Error>> {
        debug!("--------");
        debug!("start {:?}", process.wasm_context.path());
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

            let mut instance =
                match instantiate(process.wasm_context.program.as_slice(), &import_object) {
                    Ok(i) => {
                        info!("Instantiated WASM program {}", process.wasm_context.name());
                        i
                    }
                    Err(e) => {
                        error!(
                            "Error {} -- unable to instantiate WASM program: {}",
                            e,
                            process.wasm_context.path()
                        );
                        return Err(RuntimeErrorKind::ProgramInstantiation.into());
                    }
                };

            // Clear the program buffer, and save a little memory?
            process.wasm_context.program = vec![];

            instance.context_mut().data = &mut process.wasm_context as *mut _ as *mut c_void;

            let root_id;
            {
                let guard = process.wasm_context.iofs.clone();
                let guard = guard.lock().expect("poisoned iofs lock");
                root_id = guard.get_root_directory_id();
            }

            let mut msg_sender = WasmMessageSender::new(&mut instance, root_id);

            loop {
                let message = process.receiver.recv()?;
                match &message {
                    IofsMessage::SystemMessage(m) => match m {
                        IofsSystemMessage::Shutdown => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::Shutdown)
                            {
                                msg_sender.send_shutdown()?;
                            }
                        }
                        IofsSystemMessage::Ping => {
                            if process.wasm_context.does_handle_message(WasmMessage::Ping) {
                                msg_sender.send_ping()?;
                            }
                        }
                    },
                    IofsMessage::FileMessage(m) => match m {
                        IofsFileMessage::Create(path, id, parent_id) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::FileCreate)
                            {
                                msg_sender.send_file_create(path, id, parent_id)?;
                            }
                        }
                        IofsFileMessage::Delete(path, id) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::FileDelete)
                            {
                                msg_sender.send_file_delete(path, id)?;
                            }
                        }
                        IofsFileMessage::Open(path, id) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::FileClose)
                            {
                                msg_sender.send_file_open(path, id)?;
                            }
                        }
                        IofsFileMessage::Close(path, id) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::FileClose)
                            {
                                msg_sender.send_file_close(path, id)?;
                            }
                        }
                        IofsFileMessage::Write(path, id) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::FileWrite)
                            {
                                msg_sender.send_file_write(path, id)?;
                            }
                        }
                        _ => unimplemented!(),
                    },
                    IofsMessage::DirMessage(m) => match m {
                        IofsDirMessage::Create(path, id) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::DirCreate)
                            {
                                msg_sender.send_dir_create(path, id)?;
                            }
                        }
                        IofsDirMessage::Delete(path, id) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::DirDelete)
                            {
                                msg_sender.send_dir_delete(path, id)?
                            }
                        }
                    },
                };
                if let IofsMessage::SystemMessage(IofsSystemMessage::Shutdown) = message {
                    info!("WASM program {} shutting down", process.wasm_context.name());
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
