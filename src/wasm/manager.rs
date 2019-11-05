//! Main interface between the IOFS and WASM
//!
use {
    crate::{
        block::BlockStorage,
        metadata::{Grant, GrantType},
        server::IofsNetworkMessage,
        wasm::{
            IofsDirMessage, IofsFileMessage, IofsMessage, IofsSystemMessage, WasmProcess,
            WasmProcessMessage,
        },
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

struct RuntimeProcess<B: BlockStorage> {
    path: PathBuf,
    iofs: Arc<Mutex<UberFileSystem<B>>>,
    sender: crossbeam_channel::Sender<WasmProcessMessage>,
    handle: JoinHandle<Result<(), failure::Error>>,
    handled_messages: HashSet<WasmMessage>,
    receiver: crossbeam_channel::Receiver<IofsEventRegistration>,
}

impl<B: BlockStorage> RuntimeProcess<B> {
    fn new(
        path: PathBuf,
        iofs: Arc<Mutex<UberFileSystem<B>>>,
        process: WasmProcess<B>,
        receiver: crossbeam_channel::Receiver<IofsEventRegistration>,
    ) -> Self {
        RuntimeProcess {
            path,
            iofs,
            sender: process.get_sender(),
            handle: WasmProcess::start(process),
            handled_messages: HashSet::new(),
            receiver,
        }
    }

    fn does_handle_message(&self, iofs_msg: &IofsMessage) -> bool {
        let guard = self.iofs.clone();
        let mut guard = guard.lock().expect("poisoned iofs lock");

        let (can_send, msg) = match iofs_msg {
            IofsMessage::SystemMessage(IofsSystemMessage::Shutdown) => {
                (true, WasmMessage::Shutdown)
            }
            IofsMessage::SystemMessage(IofsSystemMessage::Ping) => (true, WasmMessage::Ping),
            IofsMessage::FileMessage(IofsFileMessage::Create(_)) => {
                let can_send = match guard
                    .block_manager_mut()
                    .metadata_mut()
                    .check_wasm_program_grant(&self.path, GrantType::FileCreateEvent)
                {
                    Some(Grant::Allow) => true,
                    _ => false,
                };
                (can_send, WasmMessage::FileCreate)
            }
            IofsMessage::FileMessage(IofsFileMessage::Delete(_)) => {
                let can_send = match guard
                    .block_manager_mut()
                    .metadata_mut()
                    .check_wasm_program_grant(&self.path, GrantType::FileDeleteEvent)
                {
                    Some(Grant::Allow) => true,
                    _ => false,
                };
                (can_send, WasmMessage::FileDelete)
            }
            IofsMessage::FileMessage(IofsFileMessage::Open(_)) => {
                let can_send = match guard
                    .block_manager_mut()
                    .metadata_mut()
                    .check_wasm_program_grant(&self.path, GrantType::FileOpenEvent)
                {
                    Some(Grant::Allow) => true,
                    _ => false,
                };
                (can_send, WasmMessage::FileOpen)
            }
            IofsMessage::FileMessage(IofsFileMessage::Close(_)) => {
                let can_send = match guard
                    .block_manager_mut()
                    .metadata_mut()
                    .check_wasm_program_grant(&self.path, GrantType::FileCloseEvent)
                {
                    Some(Grant::Allow) => true,
                    _ => false,
                };
                (can_send, WasmMessage::FileClose)
            }
            IofsMessage::FileMessage(IofsFileMessage::Write(_)) => {
                let can_send = match guard
                    .block_manager_mut()
                    .metadata_mut()
                    .check_wasm_program_grant(&self.path, GrantType::FileWriteEvent)
                {
                    Some(Grant::Allow) => true,
                    _ => false,
                };
                (can_send, WasmMessage::FileWrite)
            }
            IofsMessage::FileMessage(IofsFileMessage::Read(_)) => {
                let can_send = match guard
                    .block_manager_mut()
                    .metadata_mut()
                    .check_wasm_program_grant(&self.path, GrantType::FileReadEvent)
                {
                    Some(Grant::Allow) => true,
                    _ => false,
                };
                (can_send, WasmMessage::FileRead)
            }
            IofsMessage::DirMessage(IofsDirMessage::Create(_)) => {
                let can_send = match guard
                    .block_manager_mut()
                    .metadata_mut()
                    .check_wasm_program_grant(&self.path, GrantType::DirCreateEvent)
                {
                    Some(Grant::Allow) => true,
                    _ => false,
                };
                (can_send, WasmMessage::FileCreate)
            }
            IofsMessage::DirMessage(IofsDirMessage::Delete(_)) => {
                let can_send = match guard
                    .block_manager_mut()
                    .metadata_mut()
                    .check_wasm_program_grant(&self.path, GrantType::DirDeleteEvent)
                {
                    Some(Grant::Allow) => true,
                    _ => false,
                };
                (can_send, WasmMessage::DirDelete)
            }
        };

        can_send && self.handled_messages.contains(&msg)
    }

    fn register_for_event(&mut self, event: WasmMessage) {
        self.handled_messages.insert(event);
    }

    fn unregister_for_event(&mut self, event: WasmMessage) {
        self.handled_messages.remove(&event);
    }
}

#[derive(Debug)]
pub(crate) enum IofsEventRegistration {
    Register(WasmMessage),
    UnRegister(WasmMessage),
    RegisterHttpGet(String),
    RegisterHttpPost(String),
    RegisterHttpPut(String),
    RegisterHttpPatch(String),
    RegisterHttpDelete(String),
}

#[derive(Debug, Eq, Hash, PartialEq)]
enum HttpEndPoint {
    GET(String),
    POST(String),
    PUT(String),
    PATCH(String),
    DELETE(String),
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
    http_endpoints: HashMap<HttpEndPoint, usize>,
    threads_table: HashMap<PathBuf, usize>,
    threads: Vec<RuntimeProcess<B>>,
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
            http_endpoints: HashMap::new(),
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
                match listener
                    .sender
                    .send(WasmProcessMessage::IofsEvent(msg.clone()))
                {
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
                                    .sender
                                    .send(WasmProcessMessage::IofsEvent(
                                        IofsMessage::SystemMessage(IofsSystemMessage::Shutdown),
                                    ))
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
                                crossbeam_channel::unbounded::<IofsEventRegistration>();
                            let process = WasmProcess::new(
                                wasm.name.clone(),
                                wasm.program,
                                sender,
                                runtime.ufs.clone(),
                            );
                            runtime
                                .threads_table
                                .insert(wasm.name.clone(), runtime.threads.len());
                            runtime.threads.push(RuntimeProcess::new(
                                wasm.name,
                                runtime.ufs.clone(),
                                process,
                                receiver,
                            ));
                        }
                    },
                    RuntimeMessage::Registration((index, msg)) => {
                        match msg {
                            IofsEventRegistration::Register(m) => {
                                runtime.threads[index].register_for_event(m)
                            }
                            IofsEventRegistration::UnRegister(m) => {
                                runtime.threads[index].unregister_for_event(m)
                            }
                            IofsEventRegistration::RegisterHttpGet(r) => {
                                runtime
                                    .http_endpoints
                                    .entry(HttpEndPoint::GET(r))
                                    .or_insert(index);
                            }
                            IofsEventRegistration::RegisterHttpPost(r) => {
                                runtime
                                    .http_endpoints
                                    .entry(HttpEndPoint::POST(r))
                                    .or_insert(index);
                            }
                            IofsEventRegistration::RegisterHttpPut(r) => {
                                runtime
                                    .http_endpoints
                                    .entry(HttpEndPoint::PUT(r))
                                    .or_insert(index);
                            }
                            IofsEventRegistration::RegisterHttpPatch(r) => {
                                runtime
                                    .http_endpoints
                                    .entry(HttpEndPoint::PATCH(r))
                                    .or_insert(index);
                            }
                            IofsEventRegistration::RegisterHttpDelete(r) => {
                                runtime
                                    .http_endpoints
                                    .entry(HttpEndPoint::DELETE(r))
                                    .or_insert(index);
                            }
                        };
                    }
                    RuntimeMessage::Network(msg) => {
                        let guard = runtime.ufs.clone();
                        let mut guard = guard.lock().expect("poisoned iofs lock");

                        if let Ok(()) = guard.validate_token(msg.token().to_owned()) {
                            match msg {
                                get @ IofsNetworkMessage::Get(_) => {
                                    let route = get.route();
                                    if let Some(endpoint) = runtime
                                        .http_endpoints
                                        .get(&HttpEndPoint::GET(route.to_string()))
                                    {
                                        let path = &runtime.threads[*endpoint].path;
                                        if let Some(Grant::Allow) = guard
                                            .block_manager_mut()
                                            .metadata_mut()
                                            .check_wasm_program_http_grant(
                                                path,
                                                GrantType::HttpGetEvent,
                                                route,
                                            )
                                        {
                                            runtime.threads[*endpoint]
                                                .sender
                                                .send(WasmProcessMessage::NetworkEvent(get))
                                                .unwrap();
                                        } else {
                                            get.not_allowed();
                                        }
                                    } else {
                                        get.no_such_endpoint();
                                    }
                                }
                                post @ IofsNetworkMessage::Post(_) => {
                                    let route = post.route();
                                    if let Some(endpoint) = runtime
                                        .http_endpoints
                                        .get(&HttpEndPoint::POST(route.to_string()))
                                    {
                                        let path = &runtime.threads[*endpoint].path;
                                        if let Some(Grant::Allow) = guard
                                            .block_manager_mut()
                                            .metadata_mut()
                                            .check_wasm_program_http_grant(
                                                path,
                                                GrantType::HttpPostEvent,
                                                route,
                                            )
                                        {
                                            runtime.threads[*endpoint]
                                                .sender
                                                .send(WasmProcessMessage::NetworkEvent(post))
                                                .unwrap();
                                        } else {
                                            post.not_allowed();
                                        }
                                    } else {
                                        post.no_such_endpoint();
                                    }
                                }
                                put @ IofsNetworkMessage::Put(_) => {
                                    let route = put.route();
                                    if let Some(endpoint) = runtime
                                        .http_endpoints
                                        .get(&HttpEndPoint::PUT(route.to_string()))
                                    {
                                        let path = &runtime.threads[*endpoint].path;
                                        if let Some(Grant::Allow) = guard
                                            .block_manager_mut()
                                            .metadata_mut()
                                            .check_wasm_program_http_grant(
                                                path,
                                                GrantType::HttpPutEvent,
                                                route,
                                            )
                                        {
                                            runtime.threads[*endpoint]
                                                .sender
                                                .send(WasmProcessMessage::NetworkEvent(put))
                                                .unwrap();
                                        } else {
                                            put.not_allowed();
                                        }
                                    } else {
                                        put.no_such_endpoint();
                                    }
                                }
                                patch @ IofsNetworkMessage::Patch(_) => {
                                    let route = patch.route();
                                    if let Some(endpoint) = runtime
                                        .http_endpoints
                                        .get(&HttpEndPoint::PATCH(route.to_string()))
                                    {
                                        let path = &runtime.threads[*endpoint].path;
                                        if let Some(Grant::Allow) = guard
                                            .block_manager_mut()
                                            .metadata_mut()
                                            .check_wasm_program_http_grant(
                                                path,
                                                GrantType::HttpPatchEvent,
                                                route,
                                            )
                                        {
                                            runtime.threads[*endpoint]
                                                .sender
                                                .send(WasmProcessMessage::NetworkEvent(patch))
                                                .unwrap();
                                        } else {
                                            patch.not_allowed();
                                        }
                                    } else {
                                        patch.no_such_endpoint();
                                    }
                                }
                                delete @ IofsNetworkMessage::Delete(_) => {
                                    let route = delete.route();
                                    if let Some(endpoint) = runtime
                                        .http_endpoints
                                        .get(&HttpEndPoint::DELETE(route.to_string()))
                                    {
                                        let path = &runtime.threads[*endpoint].path;
                                        if let Some(Grant::Allow) = guard
                                            .block_manager_mut()
                                            .metadata_mut()
                                            .check_wasm_program_http_grant(
                                                path,
                                                GrantType::HttpDeleteEvent,
                                                route,
                                            )
                                        {
                                            runtime.threads[*endpoint]
                                                .sender
                                                .send(WasmProcessMessage::NetworkEvent(delete))
                                                .unwrap();
                                        } else {
                                            delete.not_allowed();
                                        }
                                    } else {
                                        delete.no_such_endpoint();
                                    }
                                }
                            };
                        } else {
                            msg.unauthorized();
                        }
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
    Network(IofsNetworkMessage),
    Registration((usize, IofsEventRegistration)),
}

fn receive_message<B: BlockStorage>(
    runtime: &RuntimeManager<B>,
) -> Result<RuntimeMessage, RecvError> {
    let mut select = Select::new();

    select.recv(&runtime.receiver);

    let thread_offset = if let Some(http_receiver) = &runtime.http_receiver {
        select.recv(http_receiver);
        2
    } else {
        1
    };

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
        } else if index == 1 && thread_offset == 2 {
            let msg = runtime.http_receiver.as_ref().unwrap().try_recv();
            if let Err(e) = msg {
                if e.is_empty() {
                    continue;
                }
            }

            return msg
                .map(|m| RuntimeMessage::Network(m))
                .map_err(|_| RecvError);
        } else {
            let msg = runtime.threads[index - thread_offset].receiver.try_recv();
            if let Err(e) = msg {
                if e.is_empty() {
                    continue;
                }
            }

            return msg
                .map(|m| RuntimeMessage::Registration((thread_offset - 2, m)))
                .map_err(|_| RecvError);
        }
    }
}
