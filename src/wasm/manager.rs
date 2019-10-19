//! Main interface between the IOFS and WASM
//!
use {
    crate::{
        block::BlockStorage,
        wasm::{IofsMessage, IofsSystemMessage, WasmProcess},
        UberFileSystem,
    },
    crossbeam::crossbeam_channel,
    log::{error, info},
    std::{
        collections::HashMap,
        path::PathBuf,
        sync::{Arc, Mutex},
        thread::{spawn, JoinHandle},
    },
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
    Start(WasmProgram),
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
pub(crate) struct WasmProgram {
    /// A unique identifier for the WASM program -- it's the path, and there can be only one.
    pub(in crate::wasm) name: PathBuf,
    /// The bytes that comprise the program.
    pub(in crate::wasm) program: Vec<u8>,
}

impl WasmProgram {
    pub(crate) fn new(name: PathBuf, program: Vec<u8>) -> Self {
        WasmProgram { name, program }
    }
}

struct RuntimeProcess {
    channel: crossbeam_channel::Sender<IofsMessage>,
    handle: JoinHandle<Result<(), failure::Error>>,
}

impl RuntimeProcess {
    fn new<B: BlockStorage>(process: WasmProcess<B>) -> Self {
        RuntimeProcess {
            channel: process.get_sender(),
            handle: WasmProcess::start(process),
        }
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
    receiver: crossbeam_channel::Receiver<RuntimeManagerMsg>,
    threads: HashMap<PathBuf, RuntimeProcess>,
}

impl<B: BlockStorage> RuntimeManager<B> {
    pub(crate) fn new(
        ufs: Arc<Mutex<UberFileSystem<B>>>,
        receiver: crossbeam_channel::Receiver<RuntimeManagerMsg>,
    ) -> Self {
        RuntimeManager {
            ufs,
            receiver,
            threads: HashMap::new(),
        }
    }

    fn notify_listeners(&mut self, msg: IofsMessage) {
        let mut dead_programs = vec![];
        for (id, listener) in &self.threads {
            match listener.channel.send(msg.clone()) {
                Ok(_) => (),
                Err(e) => {
                    error!("unable to send on channel {}", e);
                    dead_programs.push(id.clone());
                }
            }
        }

        for id in dead_programs {
            self.threads.remove(&id);
        }
    }

    /// Start the RuntimeManager
    ///
    /// Note that this does not take `self`, but has access via `runtime`.
    pub(crate) fn start(mut runtime: RuntimeManager<B>) -> JoinHandle<Result<(), failure::Error>> {
        spawn(move || {
            loop {
                let msg = runtime.receiver.recv().unwrap();
                match msg {
                    // Shutdown comes from the UfsMounter, thus we need to shutdown the running
                    // programs (via our UberFileSystem reference) before joining the threads,
                    // see below.
                    RuntimeManagerMsg::Shutdown => break,
                    // Forward an IofsMessage to listeners
                    RuntimeManagerMsg::IofsMessage(msg) => runtime.notify_listeners(msg),
                    // Stop the WASM program and remove it from the listeners map.
                    RuntimeManagerMsg::Stop(name) => {
                        info!("Stopping WASM program {:?}", name);
                        if let Some(thread) = runtime.threads.remove(&name) {
                            thread
                                .channel
                                .send(IofsMessage::SystemMessage(IofsSystemMessage::Shutdown));
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
                        let process =
                            WasmProcess::new(wasm.name.clone(), wasm.program, runtime.ufs.clone());
                        runtime
                            .threads
                            .insert(wasm.name, RuntimeProcess::new(process));
                    }
                }
            }

            info!("Shutting down WASM programs");
            runtime.notify_listeners(IofsMessage::SystemMessage(IofsSystemMessage::Shutdown));

            for (_, thread) in runtime.threads {
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
