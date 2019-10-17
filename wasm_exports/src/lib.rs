#![warn(missing_docs)]
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
    #[doc(hidden)]
    static ref CALLBACK_HANDLERS: MutStatic<MessageHandlers> =
        { MutStatic::from(MessageHandlers::new()) };
    #[doc(hidden)]
    static ref POST_HANDLERS: MutStatic<PostCallbacks> = { MutStatic::from(PostCallbacks::new()) };
}

/// These are exports that are available to be called by the WASM program.
extern "C" {
    #[doc(hidden)]
    pub fn pong();
}

/// These are imports used by the functions here, and resolved in Rust.
extern "C" {
    #[doc(hidden)]
    pub fn __register_for_callback(message: u32);
    #[doc(hidden)]
    pub fn __register_post_handler(route: u32);
    #[doc(hidden)]
    pub fn __print(ptr: u32);
    #[doc(hidden)]
    pub fn __open_file(id_ptr: u32) -> u64;
    #[doc(hidden)]
    pub fn __close_file(id_ptr: u32, handle: u64);
    #[doc(hidden)]
    pub fn __read_file(id_ptr: u32, handle: u64, offset: u32, data_ptr: u32, data_len: u32) -> u32;
    #[doc(hidden)]
    pub fn __write_file(id_ptr: u32, handle: u64, data_ptr: u32, data_len: u32) -> u32;
    #[doc(hidden)]
    pub fn __create_file(id_ptr: u32, name_ptr: u32) -> i32;
    #[doc(hidden)]
    pub fn __create_directory(id_ptr: u32, name_ptr: u32) -> i32;
    #[doc(hidden)]
    pub fn __open_directory(id_ptr: u32, name_ptr: u32) -> i32;
}

/// This is the sole function expected to exist in the user's WASM program
extern "C" {
    #[doc(hidden)]
    pub fn init(root_id: String);
}

/// Messages sent from the file system that may be acted upon by the user's program
///
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[repr(C)]
pub enum WasmMessage {
    /// The file system is being unmounted.
    ///
    /// This is the opportunity to shutdown; perform cleanup, etc.
    Shutdown,
    Ping,
    /// A new file has been created.
    FileCreate,
    /// A new directory has been created.
    DirCreate,
    /// A file has been deleted.
    FileDelete,
    /// A directory has been deleted.
    DirDelete,
    /// A file has been opened.
    FileOpen,
    /// A file has been closed.
    FileClose,
    /// A file is being read from.
    FileRead,
    /// A file is being written to.
    FileWrite,
}

/// Local storage for mapping file system events to message handlers.
///
#[doc(hidden)]
struct MessageHandlers {
    callbacks: HashMap<WasmMessage, extern "C" fn(Option<MessagePayload>)>,
}

impl MessageHandlers {
    fn new() -> Self {
        MessageHandlers {
            callbacks: HashMap::new(),
        }
    }

    fn lookup(&self, msg: &WasmMessage) -> Option<&extern "C" fn(Option<MessagePayload>)> {
        self.callbacks.get(msg)
    }
}

/// Local storage for mapping HTTP POST routes to callbacks.
///
#[doc(hidden)]
struct PostCallbacks {
    callbacks: HashMap<String, extern "C" fn(serde_json::Value)>,
}

impl PostCallbacks {
    fn new() -> Self {
        PostCallbacks {
            callbacks: HashMap::new(),
        }
    }

    fn lookup(&self, route: &String) -> Option<&extern "C" fn(serde_json::Value)> {
        self.callbacks.get(route)
    }
}

/// Returned from the `create_file` function
///
/// This structure must be used in subsequent file operations on the opened file.
#[derive(Debug)]
pub struct FileHandle {
    pub handle: u64,
    pub id: String,
}

/// File System Function Call Return Type
///
/// We wrap the return types in a MessagePayload to simplify handler callback registration.
pub enum MessagePayload {
    PathAndId(String, String),
    FileCreate(FileCreate),
}

pub struct FileCreate {
    pub path: String,
    pub id: String,
    pub dir_id: String,
}

//
// The following functions are called from WASM
//

/// Print a string to the IOFS output console
///
pub fn print(msg: &str) {
    let msg = Box::into_raw(Box::new(msg));
    unsafe { __print(msg as u32) };
}

/// Register a file system message callback
///
pub fn register_callback(msg: WasmMessage, func: extern "C" fn(Option<MessagePayload>)) {
    let mut lookup = CALLBACK_HANDLERS.write().unwrap();
    lookup.callbacks.entry(msg.clone()).or_insert(func);

    let msg = Box::into_raw(Box::new(msg));
    unsafe { __register_for_callback(msg as u32) };
}

/// Register an HTTP POST route
///
/// HTTP POST requests sent to http://hostname/wasm/<route> will be routed to this function. The
/// <route> is a single string, and not a path.
pub fn register_post_route<S: AsRef<str>>(route: S, func: extern "C" fn(serde_json::Value)) {
    let mut lookup = POST_HANDLERS.write().unwrap();
    lookup
        .callbacks
        .entry(route.as_ref().to_owned())
        .or_insert(func);

    let route = Box::into_raw(Box::new(route.as_ref()));
    unsafe { __register_post_handler(route as u32) };
}

/// Open a file
///
/// This function opens a file identified by a `UfsUuid`, and returns a `Option<FileHandle>`.
pub fn open_file(id: &str) -> Option<FileHandle> {
    let id_box = Box::into_raw(Box::new(id));
    let handle = unsafe { __open_file(id_box as u32) };
    if handle == 0 {
        None
    } else {
        Some(FileHandle {
            handle,
            id: id.to_string(),
        })
    }
}

/// Close an open file
///
/// This function takes a FileHandle, returned by a previous call to open_file.
pub fn close_file(handle: &FileHandle) {
    let id = Box::into_raw(Box::new(handle.id.as_str()));
    unsafe { __close_file(id as u32, handle.handle) }
}

/// Read bytes from a file
///
/// This function takes a FileHandle, returned by a previous call to open_file, an offset and a
/// `&[u8]` buffer. The offset is the location in the file being read at which the read should
/// begin. The bytes are returned in the `&[u8]` buffer.
pub fn read_file(handle: &FileHandle, offset: u32, data: &[u8]) -> u32 {
    let id = Box::into_raw(Box::new(handle.id.as_str()));
    let ptr = data.as_ptr();
    let len = data.len();
    unsafe { __read_file(id as u32, handle.handle, offset, ptr as _, len as _) }
}

/// Write bytes to a file
///
/// This function takes a FileHandle, returned by a previous call to open_file, or create_file, and
/// a `&[u8]` buffer of bytes.
pub fn write_file(handle: &FileHandle, data: &[u8]) -> u32 {
    let id = Box::into_raw(Box::new(handle.id.as_str()));

    let ptr = data.as_ptr();
    let len = data.len();
    unsafe { __write_file(id as u32, handle.handle, ptr as _, len as _) }
}

/// Create a new file
///
/// This function takes the `UfsUuid` of a directory, and a name. A new file will be created with
/// `name`, under the directory identified by the ID. An `Option<FileHandle>` is returned.
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

/// Create a new directory
///
/// This function takes the `UfsUuid` of a directory, and a name. A new directory will be created
/// with `name`, under the directory identified by the ID. An `Option<String>` is returned
/// containing the ID of the new directory.
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

/// Open a directory
///
/// This function takes the `UfsUuid` of a parent directory (possibly the root directory) and the
/// `name` of a subdirectory to open. The returned `Option<String>` is the id belonging to the
/// `name`d directory.
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
// Helpers
//
#[doc(hidden)]
fn unbox_string(ptr: i32, len: i32) -> String {
    let slice = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };
    str::from_utf8(&slice)
        .expect("unable to unbox string")
        .to_owned()
}

//
// The following functions are called from Rust. They manipulate data coming across the WASM
// boundary, and make things nicer for the person writing a WASM program.
//
#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __init(ptr: i32, len: i32) {
    unsafe { init(unbox_string(ptr, len)) };
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_shutdown() {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::Shutdown) {
        func(None);
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_ping() {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::Ping) {
        func(None);
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_create(
    path_ptr: i32,
    path_len: i32,
    id_ptr: i32,
    id_len: i32,
    parent_id_ptr: i32,
    parent_id_len: i32,
) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileCreate) {
        func(Some(MessagePayload::FileCreate(FileCreate {
            path: unbox_string(path_ptr, path_len),
            id: unbox_string(id_ptr, id_len),
            dir_id: unbox_string(parent_id_ptr, parent_id_len),
        })));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_dir_create(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::DirCreate) {
        func(Some(MessagePayload::PathAndId(
            unbox_string(path_ptr, path_len),
            unbox_string(id_ptr, id_len),
        )));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_delete(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileDelete) {
        func(Some(MessagePayload::PathAndId(
            unbox_string(path_ptr, path_len),
            unbox_string(id_ptr, id_len),
        )));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_dir_delete(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::DirDelete) {
        func(Some(MessagePayload::PathAndId(
            unbox_string(path_ptr, path_len),
            unbox_string(id_ptr, id_len),
        )));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_open(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileOpen) {
        func(Some(MessagePayload::PathAndId(
            unbox_string(path_ptr, path_len),
            unbox_string(id_ptr, id_len),
        )));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_close(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileClose) {
        func(Some(MessagePayload::PathAndId(
            unbox_string(path_ptr, path_len),
            unbox_string(id_ptr, id_len),
        )));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_write(path_ptr: i32, path_len: i32, id_ptr: i32, id_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileWrite) {
        func(Some(MessagePayload::PathAndId(
            unbox_string(path_ptr, path_len),
            unbox_string(id_ptr, id_len),
        )));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_http_post(route_ptr: i32, route_len: i32, json_ptr: i32, json_len: i32) {
    let slice = unsafe { slice::from_raw_parts(route_ptr as *const u8, route_len as usize) };
    let route = str::from_utf8(&slice).unwrap();

    let lookup = POST_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&route.to_owned()) {
        let slice = unsafe { slice::from_raw_parts(json_ptr as *const u8, json_len as usize) };
        let json: serde_json::Value =
            serde_json::from_slice(&slice).expect("unable to serialize JSON in __handle_http_post");
        func(json);
    }
}
