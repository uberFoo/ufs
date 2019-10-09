//! Functions exported to the WASM environment
//!
//! These are functions needed to interface with the IOFS, and are linked into the user's WASM
//! program when it's built.
//!
use {
    lazy_static::lazy_static,
    mut_static::MutStatic,
    std::{collections::HashMap, convert::TryInto, slice, str},
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
    pub fn __open_file(id_ptr: u32) -> u64;
    pub fn __close_file(id_ptr: u32, handle: u64);
    pub fn __read_file(id_ptr: u32, handle: u64, offset: u32, data_ptr: u32, data_len: u32) -> u32;
    pub fn __write_file(id_ptr: u32, handle: u64, offset: u32, data_ptr: u32, data_len: u32)
        -> u32;
    pub fn __create_file(id_ptr: u32, name_ptr: u32) -> i32;
    pub fn __create_directory(id_ptr: u32, name_ptr: u32) -> i32;
    pub fn __open_directory(id_ptr: u32, name_ptr: u32) -> i32;
}

/// This is the sole function expected to exist in the user's WASM program
extern "C" {
    pub fn init(root_id: RefStr);
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[repr(C)]
pub enum WasmMessage {
    Shutdown,
    Ping,
    FileCreate,
    DirCreate,
    FileDelete,
    DirDelete,
    FileOpen,
    FileClose,
    FileRead,
    FileWrite,
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

#[derive(Debug)]
pub struct FileHandle {
    pub handle: u64,
    pub id: String,
}

pub enum MessagePayload {
    Path(RefStr),
    PathAndId(RefStr, RefStr),
    PathAndIdAndPid(RefStr, RefStr, RefStr),
    FileCreate(__FileCreate),
}

pub struct __FileCreate {
    path: RefStr,
    id: RefStr,
    dir_id: RefStr,
}

pub struct FileCreate {
    pub path: String,
    pub id: String,
    pub dir_id: String,
}

impl __FileCreate {
    pub fn unpack(self) -> FileCreate {
        FileCreate {
            path: self.path.get_str().to_owned(),
            id: self.id.get_str().to_owned(),
            dir_id: self.dir_id.get_str().to_owned(),
        }
    }
}

#[repr(C)]
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

//
// The following functions are called from WASM
//
pub fn print(msg: &str) {
    let msg = Box::into_raw(Box::new(msg));
    unsafe { __print(msg as u32) };
}

pub fn register_callback(msg: WasmMessage, func: extern "C" fn(Option<MessagePayload>)) {
    let mut lookup = LOOKUP.write().unwrap();
    lookup.callbacks.entry(msg.clone()).or_insert(func);

    let msg = Box::into_raw(Box::new(msg));
    unsafe { __register_for_callback(msg as u32) };
}

pub fn open_file(id: &str) -> u64 {
    let id = Box::into_raw(Box::new(id));
    unsafe { __open_file(id as u32) }
}

pub fn close_file(id: &str, handle: u64) {
    let id = Box::into_raw(Box::new(id));
    unsafe { __close_file(id as u32, handle) }
}

pub fn read_file(id: &str, handle: u64, offset: u32, data: &[u8]) -> u32 {
    let id = Box::into_raw(Box::new(id));
    let ptr = data.as_ptr();
    let len = data.len();
    unsafe { __read_file(id as u32, handle, offset, ptr as _, len as _) }
}

pub fn write_file(id: &str, handle: u64, offset: u32, data: &[u8]) -> u32 {
    let id = Box::into_raw(Box::new(id));

    let ptr = data.as_ptr();
    let len = data.len();
    unsafe { __write_file(id as u32, handle, offset, ptr as _, len as _) }
}

pub fn create_file(parent_id: &str, name: &str) -> Option<FileHandle> {
    let parent_id = Box::into_raw(Box::new(parent_id));
    let name = Box::into_raw(Box::new(name));
    let file_id_ptr = unsafe { __create_file(parent_id as u32, name as u32) };

    if file_id_ptr != -1 {
        let handle_buf = unsafe { slice::from_raw_parts(file_id_ptr as *const u8, 8) };
        let handle = u64::from_le_bytes(handle_buf.try_into().expect("unable to read file handle"));
        let slice = unsafe {
            slice::from_raw_parts((file_id_ptr + 8) as *const u8, file_id_ptr as usize + 36)
        };
        let file_id_str = str::from_utf8(&slice).expect("unable to create file_id str");

        Some(FileHandle {
            handle,
            id: file_id_str.to_string(),
        })
    } else {
        None
    }
}

pub fn create_directory(parent_id: &str, name: &str) -> Option<String> {
    let parent_id = Box::into_raw(Box::new(parent_id));
    let name = Box::into_raw(Box::new(name));
    let dir_id_ptr = unsafe { __create_directory(parent_id as u32, name as u32) };
    if dir_id_ptr != -1 {
        let slice = unsafe { slice::from_raw_parts(dir_id_ptr as *const u8, 36) };
        let dir_id_str = str::from_utf8(&slice).expect("unable to create dir_id str");

        Some(dir_id_str.to_string())
    } else {
        None
    }
}

pub fn open_directory(parent_id: &str, name: &str) -> Option<String> {
    let parent_id = Box::into_raw(Box::new(parent_id));
    let name = Box::into_raw(Box::new(name));
    let dir_id_ptr = unsafe { __open_directory(parent_id as u32, name as u32) };
    if dir_id_ptr != -1 {
        let slice = unsafe { slice::from_raw_parts(dir_id_ptr as *const u8, 36) };
        let dir_id_str = str::from_utf8(&slice).expect("unable to create dir_id str");

        Some(dir_id_str.to_string())
    } else {
        None
    }
}

//
// The following functions are called from Rust. They manipulate data coming across the WASM
// boundary, and make things nicer for the person writing a WASM program.
//
#[no_mangle]
pub extern "C" fn __init(ptr: i32, len: i32) {
    unsafe { init(RefStr { ptr, len }) };
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
pub extern "C" fn __handle_file_create(
    path_ptr: i32,
    path_len: i32,
    id_ptr: i32,
    id_len: i32,
    parent_id_ptr: i32,
    parent_id_len: i32,
) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileCreate) {
        let path_from_host = RefStr {
            ptr: path_ptr,
            len: path_len,
        };
        let id_from_host = RefStr {
            ptr: id_ptr,
            len: id_len,
        };
        let parent_id_from_host = RefStr {
            ptr: parent_id_ptr,
            len: parent_id_len,
        };
        func(Some(MessagePayload::FileCreate(__FileCreate {
            path: path_from_host,
            id: id_from_host,
            dir_id: parent_id_from_host,
        })));
    }
}

#[no_mangle]
pub extern "C" fn __handle_dir_create(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::DirCreate) {
        let path_from_host = RefStr {
            ptr: path_ptr,
            len: path_len,
        };
        let id_from_host = RefStr {
            ptr: id_ptr,
            len: id_len,
        };
        func(Some(MessagePayload::PathAndId(
            path_from_host,
            id_from_host,
        )));
    }
}

#[no_mangle]
pub extern "C" fn __handle_file_delete(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileDelete) {
        let path_from_host = RefStr {
            ptr: path_ptr,
            len: path_len,
        };
        let id_from_host = RefStr {
            ptr: id_ptr,
            len: id_len,
        };
        func(Some(MessagePayload::PathAndId(
            path_from_host,
            id_from_host,
        )));
    }
}

#[no_mangle]
pub extern "C" fn __handle_dir_delete(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::DirDelete) {
        let path_from_host = RefStr {
            ptr: path_ptr,
            len: path_len,
        };
        let id_from_host = RefStr {
            ptr: id_ptr,
            len: id_len,
        };
        func(Some(MessagePayload::PathAndId(
            path_from_host,
            id_from_host,
        )));
    }
}

#[no_mangle]
pub extern "C" fn __handle_file_open(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileOpen) {
        let path_from_host = RefStr {
            ptr: path_ptr,
            len: path_len,
        };
        let id_from_host = RefStr {
            ptr: id_ptr,
            len: id_len,
        };
        func(Some(MessagePayload::PathAndId(
            path_from_host,
            id_from_host,
        )));
    }
}

#[no_mangle]
pub extern "C" fn __handle_file_close(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileClose) {
        let path_from_host = RefStr {
            ptr: path_ptr,
            len: path_len,
        };
        let id_from_host = RefStr {
            ptr: id_ptr,
            len: id_len,
        };
        func(Some(MessagePayload::PathAndId(
            path_from_host,
            id_from_host,
        )));
    }
}

#[no_mangle]
pub extern "C" fn __handle_file_write(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = LOOKUP.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileWrite) {
        let path_from_host = RefStr {
            ptr: path_ptr,
            len: path_len,
        };
        let id_from_host = RefStr {
            ptr: id_ptr,
            len: id_len,
        };
        func(Some(MessagePayload::PathAndId(
            path_from_host,
            id_from_host,
        )));
    }
}
