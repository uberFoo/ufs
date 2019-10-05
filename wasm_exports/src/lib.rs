use {
    lazy_static::lazy_static,
    mut_static::MutStatic,
    std::{collections::HashMap, slice, str},
};

lazy_static! {
    pub static ref LOOKUP: MutStatic<MessageHandler> = { MutStatic::from(MessageHandler::new()) };
}

/// These are exports that are available to be called by the WASM program.
extern "C" {
    pub fn pong();
}

/// These are imports used by the functions here, and resolved in Rust.
extern "C" {
    pub fn __register_for_callback(message: u32);
    pub fn __print(ptr: u32);
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[repr(C)]
pub enum WasmMessage {
    Shutdown,
    Ping,
    NewFile,
    FileChanged,
    FileWritten,
    FileRead,
    FileDeleted,
    NewDir,
    DirChanged,
}

#[no_mangle]
pub extern "C" fn print(msg: &str) {
    let msg = Box::into_raw(Box::new(msg));
    unsafe { __print(msg as u32) };
}

#[no_mangle]
pub extern "C" fn __handle_shutdown() {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::Shutdown) {
        func(None);
    }
}

#[no_mangle]
pub extern "C" fn __handle_ping() {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::Ping) {
        func(None);
    }
}

#[no_mangle]
pub extern "C" fn __handle_new_file(ptr: i32, len: i32) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::NewFile) {
        let path_from_host = RefStr { ptr, len };
        func(Some(MessagePayload::String(path_from_host)));
    }
}

#[no_mangle]
pub extern "C" fn __handle_file_delete(ptr: i32, len: i32) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileDeleted) {
        let path_from_host = RefStr { ptr, len };
        func(Some(MessagePayload::String(path_from_host)));
    }
}

#[no_mangle]
pub extern "C" fn __handle_new_dir(ptr: i32, len: i32) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::NewDir) {
        let path_from_host = RefStr { ptr, len };
        func(Some(MessagePayload::String(path_from_host)));
    }
}

#[derive(Debug)]
pub struct RefStr {
    ptr: i32,
    len: i32,
}

impl RefStr {
    pub fn get_str(&self) -> &str {
        let slice = unsafe { slice::from_raw_parts(self.ptr as _, self.len as _) };
        str::from_utf8(&slice).unwrap()
    }
}

#[derive(Debug)]
pub enum MessagePayload {
    String(RefStr),
}

pub struct MessageHandler {
    callbacks: HashMap<WasmMessage, extern "C" fn(Option<MessagePayload>)>,
}

impl MessageHandler {
    fn new() -> Self {
        MessageHandler {
            callbacks: HashMap::new(),
        }
    }

    fn lookup(&self, msg: &WasmMessage) -> Option<&extern "C" fn(Option<MessagePayload>)> {
        self.callbacks.get(msg)
    }
}

pub fn register_callback(msg: WasmMessage, func: extern "C" fn(Option<MessagePayload>)) {
    let mut lookup = LOOKUP.write().unwrap();
    lookup.callbacks.entry(msg.clone()).or_insert(func);

    let msg = Box::into_raw(Box::new(msg));
    unsafe { __register_for_callback(msg as u32) };
}
