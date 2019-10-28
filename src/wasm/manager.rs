//! Main interface between the IOFS and WASM
//!
use {
    crate::{
        block::BlockStorage,
        server::IofsNetworkMessage,
        wasm::{IofsDirMessage, IofsFileMessage, IofsMessage, IofsSystemMessage, WasmProcess},
        UberFileSystem,
    },
    crossbeam::{crossbeam_channel, RecvError, Select},
    log::{error, info},
    std::{
        collections::{HashMap, HashSet},
        path::PathBuf,
        sync::{Arc, Mutex},
        thread::{spawn, JoinHandle},
    },
    wasm_exports::WasmMessage,
};

/// Runtime Manager Messages
///
/// These are used to communicate messages to the Runtime Manager from the file system
/// implementation.
pub(crate) enum RuntimeManagerMsg {
    /// Shutdown WASM Runtime
    ///
    /// The file system is shutting down, and this allows the WASM programs the same opportunity.
    Shutdown,
    /// Add a new WASM Program
    ///
    /// The file system contains a WASM program, and wishes it to be loaded and run.
    Start(ProtoWasmProgram),
    /// Stop a running WASM Program
    ///
    /// There is a running program that must needs be stopped.
    Stop(PathBuf),
    /// Send a message to running WASM programs
    ///
    IofsMessage(IofsMessage),
}

/// Information necessary to start running a WASM program
///
/// This is the contents of the RuntimeManagerMsg::Start message.
pub(crate) struct ProtoWasmProgram {
    /// A unique identifier for the WASM program -- it's the path, and there can be only one.
    pub(in crate::wasm) name: PathBuf,
    /// The bytes that comprise the program.
    pub(in crate::wasm) program: Vec<u8>,
}

impl ProtoWasmProgram {
    pub(crate) fn new(name: PathBuf, program: Vec<u8>) -> Self {
        ProtoWasmProgram { name, program }
    }
}

#[derive(Debug)]
pub(crate) enum MessageRegistration {
    Register(WasmMessage),
    UnRegister(WasmMessage),
}

struct RuntimeProcess {
    channel: crossbeam_channel::Sender<IofsMessage>,
    handle: JoinHandle<Result<(), failure::Error>>,
    handled_messages: HashSet<WasmMessage>,
    receiver: crossbeam_channel::Receiver<MessageRegistration>,
}

impl RuntimeProcess {
    fn new<B: BlockStorage>(
        process: WasmProcess<B>,
        receiver: crossbeam_channel::Receiver<MessageRegistration>,
    ) -> Self {
        RuntimeProcess {
            channel: process.get_sender(),
            handle: WasmProcess::start(process),
            handled_messages: HashSet::new(),
            receiver,
        }
    }

    fn does_handle_message(&self, iofs_msg: &IofsMessage) -> bool {
        let msg = match iofs_msg {
            IofsMessage::SystemMessage(IofsSystemMessage::Shutdown) => WasmMessage::Shutdown,
            IofsMessage::SystemMessage(IofsSystemMessage::Ping) => WasmMessage::Ping,
            IofsMessage::FileMessage(IofsFileMessage::Create(_)) => WasmMessage::FileCreate,
            IofsMessage::FileMessage(IofsFileMessage::Delete(_)) => WasmMessage::FileDelete,
            IofsMessage::FileMessage(IofsFileMessage::Open(_)) => WasmMessage::FileOpen,
            IofsMessage::FileMessage(IofsFileMessage::Close(_)) => WasmMessage::FileClose,
            IofsMessage::FileMessage(IofsFileMessage::Write(_)) => WasmMessage::FileWrite,
            IofsMessage::FileMessage(IofsFileMessage::Read(_)) => WasmMessage::FileRead,
            IofsMessage::DirMessage(IofsDirMessage::Create(_)) => WasmMessage::FileCreate,
            IofsMessage::DirMessage(IofsDirMessage::Delete(_)) => WasmMessage::DirDelete,
        };
        self.handled_messages.contains(&msg)
    }

    fn handle_registration(&mut self, msg: MessageRegistration) {
        match msg {
            MessageRegistration::Register(m) => self.handled_messages.insert(m),
            MessageRegistration::UnRegister(m) => self.handled_messages.remove(&m),
        };
    }
}

/// WASM Thread Management
///
/// This struct is the interface between the `IOFileSystem` and Wasm programs running inside of the
/// file system. There is a channel that the IOFS uses to start and stop Wasm programs, as well as
/// sending file system messages to the programs.
///
/// The `UfsMounter` will also send a shutdown message, on the same channel, when the file system is
/// going away. Here, we use that message to nicely stop the WASM programs before exiting.
pub(crate) struct RuntimeManager<B: BlockStorage + 'static> {
    ufs: Arc<Mutex<UberFileSystem<B>>>,
    http_receiver: Option<crossbeam_channel::Receiver<IofsNetworkMessage>>,
    receiver: crossbeam_channel::Receiver<RuntimeManagerMsg>,
    threads_table: HashMap<PathBuf, usize>,
    threads: Vec<RuntimeProcess>,
}

impl<B: BlockStorage> RuntimeManager<B> {
    pub(crate) fn new(
        ufs: Arc<Mutex<UberFileSystem<B>>>,
        receiver: crossbeam_channel::Receiver<RuntimeManagerMsg>,
    ) -> Self {
        RuntimeManager {
            ufs,
            http_receiver: None,
            receiver,
            threads_table: HashMap::new(),
            threads: Vec::new(),
        }
    }

    pub(crate) fn set_http_receiver(
        &mut self,
        sender: crossbeam_channel::Receiver<IofsNetworkMessage>,
    ) {
        self.http_receiver.replace(sender);
    }

    fn notify_listeners(&mut self, msg: IofsMessage) {
        let mut dead_programs = vec![];
        for (id, idx) in &self.threads_table {
            let listener = &self.threads[*idx];
            if listener.does_handle_message(&msg) {
                match listener.channel.send(msg.clone()) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("unable to send on channel {}", e);
                        dead_programs.push(id.clone());
                    }
                }
            }
        }

        for id in dead_programs {
            let idx = self.threads_table.remove(&id).unwrap();
            self.threads.remove(idx);
        }
    }

    /// Start the RuntimeManager
    ///
    /// Note that this does not take `self`, but has access via `runtime`.
    pub(crate) fn start(mut runtime: RuntimeManager<B>) -> JoinHandle<Result<(), failure::Error>> {
        spawn(move || {
            info!("RuntimeManager Starting");
            loop {
                let msg = receive_message(&runtime).unwrap();
                match msg {
                    RuntimeMessage::Runtime(msg) => match msg {
                        // Shutdown comes from the UfsMounter, thus we need to shutdown the running
                        // programs (via our UberFileSystem reference) before joining the threads,
                        // see below.
                        RuntimeManagerMsg::Shutdown => break,
                        // Forward an IofsMessage to listeners
                        RuntimeManagerMsg::IofsMessage(msg) => runtime.notify_listeners(msg),
                        // Stop the WASM program and remove it from the listeners map.
                        RuntimeManagerMsg::Stop(name) => {
                            info!("Stopping WASM program {:?}", name);
                            if let Some(thread_idx) = runtime.threads_table.remove(&name) {
                                let thread = runtime.threads.remove(thread_idx);
                                thread
                                    .channel
                                    .send(IofsMessage::SystemMessage(IofsSystemMessage::Shutdown))
                                    .expect(&format!(
                                        "unable to send shutdown to Wasm program {:?}",
                                        name
                                    ));
                                thread
                                    .handle
                                    .join()
                                    .expect("unable to join WasmProcess")
                                    .expect("error during WasmProcess execution");
                            }
                        }
                        // Start the WASM program and add it to the listeners map.
                        RuntimeManagerMsg::Start(wasm) => {
                            info!("Starting WASM program {:?}", wasm.name);
                            let (sender, receiver) =
                                crossbeam_channel::unbounded::<MessageRegistration>();
                            let process = WasmProcess::new(
                                wasm.name.clone(),
                                wasm.program,
                                runtime.http_receiver.clone(),
                                sender,
                                runtime.ufs.clone(),
                            );
                            runtime
                                .threads_table
                                .insert(wasm.name, runtime.threads.len());
                            runtime.threads.push(RuntimeProcess::new(process, receiver));
                        }
                    },
                    RuntimeMessage::Registration((index, msg)) => {
                        println!("handling message {:?}", msg);
                        runtime.threads[index].handle_registration(msg);
                    }
                }
            }

            info!("Shutting down WASM programs");
            runtime.notify_listeners(IofsMessage::SystemMessage(IofsSystemMessage::Shutdown));

            for thread in runtime.threads {
                thread
                    .handle
                    .join()
                    .expect("unable to join WasmProcess")
                    .expect("error during WasmProcess execution");
            }

            Ok(())
        })
    }
}

enum RuntimeMessage {
    Runtime(RuntimeManagerMsg),
    Registration((usize, MessageRegistration)),
}

fn receive_message<B: BlockStorage>(
    runtime: &RuntimeManager<B>,
) -> Result<RuntimeMessage, RecvError> {
    let mut select = Select::new();
    select.recv(&runtime.receiver);

    for t in &runtime.threads {
        select.recv(&t.receiver);
    }

    loop {
        let index = select.ready();
        if index == 0 {
            let msg = runtime.receiver.try_recv();
            if let Err(e) = msg {
                if e.is_empty() {
                    continue;
                }
            }

            return msg
                .map(|m| RuntimeMessage::Runtime(m))
                .map_err(|_| RecvError);
        } else {
            let msg = runtime.threads[index - 1].receiver.try_recv();
            if let Err(e) = msg {
                if e.is_empty() {
                    continue;
                }
            }

            return msg
                .map(|m| RuntimeMessage::Registration((index - 1, m)))
                .map_err(|_| RecvError);
        }
    }
}
