mod callbacks;
pub(crate) mod manager;
pub(crate) mod message;

pub(crate) use {
    manager::{RuntimeManager, RuntimeManagerMsg, WasmProgram},
    message::{IofsDirMessage, IofsFileMessage, IofsMessage, IofsSystemMessage, WasmMessageSender},
};

use {
    self::callbacks::{__print, __register_for_callback, pong},
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
        handlers.insert(WasmMessage::NewFile, false);
        handlers.insert(WasmMessage::FileChanged, false);
        handlers.insert(WasmMessage::FileWritten, false);
        handlers.insert(WasmMessage::FileRead, false);
        handlers.insert(WasmMessage::NewDir, false);
        handlers.insert(WasmMessage::DirChanged, false);

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
        *self.notifications.entry(msg).or_default()
    }

    pub(crate) fn handle_message(&mut self, msg: WasmMessage) {
        self.notifications.insert(msg, true);
    }
}

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
            let import_object = imports! {
                "env" => {
                    "__register_for_callback" => func!(__register_for_callback<B>),
                    "__print" => func!(__print<B>),
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
                        return Err(RuntimeErrorKind::ProgramInstantiationFailure.into());
                    }
                };

            // Clear the program buffer, and save a little memory?
            process.wasm_context.program = vec![];

            instance.context_mut().data = &mut process.wasm_context as *mut _ as *mut c_void;

            let mut msg_sender = WasmMessageSender::new(&mut instance);

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
                        IofsFileMessage::NewFile(f) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::NewFile)
                            {
                                msg_sender.send_new_file(f)?;
                            }
                        }
                        IofsFileMessage::FileDeleted(f) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::FileDeleted)
                            {
                                msg_sender.send_file_deleted(f)?;
                            }
                        }
                        _ => unimplemented!(),
                    },
                    IofsMessage::DirMessage(m) => match m {
                        IofsDirMessage::NewDir(d) => {
                            if process
                                .wasm_context
                                .does_handle_message(WasmMessage::NewDir)
                            {
                                msg_sender.send_new_dir(d)?;
                            }
                        }
                        _ => unimplemented!(),
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
    fn cause(&self) -> Option<&Fail> {
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
    ProgramInstantiationFailure,
    #[fail(display = "Error invoking function in WASM.")]
    FunctionInvocationFailure,
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

#[cfg(test)]
mod tests {
    use {super::*, crate::block::BlockSize, env_logger, std::fs};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn runit() {
        init();

        let mut ufs = Arc::new(Mutex::new(UberFileSystem::new_memory(
            "test",
            "foobar",
            "test",
            BlockSize::TwentyFortyEight,
            100,
        )));

        let path = PathBuf::from("tank_test/target/wasm32-unknown-unknown/release/tank_test.wasm");
        let pgm = fs::read(&path).unwrap();

        let process = WasmProcess::new(WasmContext::new(path, pgm, ufs.clone()));
        let sender = process.get_sender();
        let h = WasmProcess::start(process);

        sender
            .send(IofsMessage::SystemMessage(IofsSystemMessage::Ping))
            .unwrap();
        sender
            .send(IofsMessage::FileMessage(IofsFileMessage::NewFile(
                "/etc/passwd".to_string(),
            )))
            .unwrap();
        sender
            .send(IofsMessage::SystemMessage(IofsSystemMessage::Shutdown))
            .unwrap();
        h.join()
            .expect("unable to join")
            .expect("error during execution");
    }
}
