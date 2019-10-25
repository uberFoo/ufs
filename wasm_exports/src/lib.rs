#![warn(missing_docs)]
//! Functions exported to the WASM environment
//!
//! These are functions needed to interface with the IOFS, and are linked into the user's WASM
//! program when it's built.
//!
use {
    lazy_static::lazy_static,
    mut_static::MutStatic,
    serde_derive::{Deserialize, Serialize},
    std::{
        path::PathBuf,
        {collections::HashMap, convert::TryInto, slice, str},
    },
    uuid::Uuid,
};

lazy_static! {
    #[doc(hidden)]
    static ref CALLBACK_HANDLERS: MutStatic<MessageHandlers> =
        { MutStatic::from(MessageHandlers::new()) };
    #[doc(hidden)]
    static ref GET_HANDLERS: MutStatic<GetCallbacks> = { MutStatic::from(GetCallbacks::new()) };
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

/// Wasm Program init function declaration
///
/// This is the sole function expected to exist in the user's WASM program
///
/// The warning about `Uuid` not being FFI-safe may be ignored, as this function is called from
/// within the Wasm interpreter, and not used across the FFI boundary.
extern "C" {
    #[doc(hidden)]
    pub fn init(root_id: Uuid);
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

/// Local storage for mapping HTTP GET routes to callbacks.
///
#[doc(hidden)]
struct GetCallbacks {
    callbacks: HashMap<String, extern "C" fn(&str)>,
}

impl GetCallbacks {
    fn new() -> Self {
        GetCallbacks {
            callbacks: HashMap::new(),
        }
    }

    fn lookup(&self, route: &String) -> Option<&extern "C" fn(&str)> {
        self.callbacks.get(route)
    }
}

/// Local storage for mapping HTTP POST routes to callbacks.
///
#[doc(hidden)]
struct PostCallbacks {
    callbacks: HashMap<String, extern "C" fn(&str)>,
}

impl PostCallbacks {
    fn new() -> Self {
        PostCallbacks {
            callbacks: HashMap::new(),
        }
    }

    fn lookup(&self, route: &String) -> Option<&extern "C" fn(&str)> {
        self.callbacks.get(route)
    }
}

/// Returned from the `create_file` function
///
/// This structure must be used in subsequent file operations on the opened file.
#[derive(Debug, Deserialize, Serialize)]
pub struct FileHandle {
    pub handle: u64,
    pub id: Uuid,
}

/// File System Function Call Return Type
///
/// We wrap the return types in a MessagePayload to simplify handler callback registration.
#[derive(Debug, Deserialize, Serialize)]
pub struct MessagePayload {
    pub path: PathBuf,
    pub id: Uuid,
    pub parent_id: Uuid,
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
pub fn register_post_route<S: AsRef<str>>(route: S, func: extern "C" fn(&str)) {
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
pub fn open_file(id: &Uuid) -> Option<FileHandle> {
    let json_str = serde_json::to_string(&id).unwrap();
    let json_box = Box::into_raw(Box::new(json_str.as_str()));
    let handle = unsafe { __open_file(json_box as u32) };
    if handle == 0 {
        None
    } else {
        Some(FileHandle {
            handle,
            id: id.clone(),
        })
    }
}

/// Close an open file
///
/// This function takes a FileHandle, returned by a previous call to open_file.
pub fn close_file(handle: &FileHandle) {
    let json_str = serde_json::to_string(&handle.id).unwrap();
    let json_box = Box::into_raw(Box::new(json_str.as_str()));
    unsafe { __close_file(json_box as u32, handle.handle) }
}

/// Read bytes from a file
///
/// This function takes a FileHandle, returned by a previous call to open_file, an offset and a
/// `&[u8]` buffer. The offset is the location in the file being read at which the read should
/// begin. The bytes are returned in the `&[u8]` buffer.
pub fn read_file(handle: &FileHandle, offset: u32, data: &[u8]) -> u32 {
    let json_str = serde_json::to_string(&handle.id).unwrap();
    let json_box = Box::into_raw(Box::new(json_str.as_str()));
    let ptr = data.as_ptr();
    let len = data.len();
    unsafe { __read_file(json_box as u32, handle.handle, offset, ptr as _, len as _) }
}

/// Write bytes to a file
///
/// This function takes a FileHandle, returned by a previous call to open_file, or create_file, and
/// a `&[u8]` buffer of bytes.
pub fn write_file(handle: &FileHandle, data: &[u8]) -> u32 {
    let json_str = serde_json::to_string(&handle.id).unwrap();
    let json_box = Box::into_raw(Box::new(json_str.as_str()));
    let ptr = data.as_ptr();
    let len = data.len();
    unsafe { __write_file(json_box as u32, handle.handle, ptr as _, len as _) }
}
/// Create a new file
///
/// This function takes the `UfsUuid` of a directory, and a name. A new file will be created with
/// `name`, under the directory identified by the ID. An `Option<FileHandle>` is returned.
pub fn create_file(parent_id: &Uuid, name: &str) -> Option<FileHandle> {
    let json_str = serde_json::to_string(parent_id).unwrap();
    let json_box = Box::into_raw(Box::new(json_str.as_str()));

    let name = Box::into_raw(Box::new(name));
    let file_handle_ptr = unsafe { __create_file(json_box as u32, name as u32) };

    if file_handle_ptr != -1 {
        // The JSON string is returned as a length at memory location 0, and the string's bytes
        // located at memory location 8.
        let len_buf = unsafe { slice::from_raw_parts(file_handle_ptr as *const u8, 8) };
        let len = u64::from_le_bytes(len_buf.try_into().unwrap());

        let json_str = unbox_slice(file_handle_ptr + 8, len as _);
        let payload: FileHandle = serde_json::from_slice(json_str).unwrap();

        Some(payload)
    } else {
        None
    }
}

/// Create a new directory
///
/// This function takes the `UfsUuid` of a directory, and a name. A new directory will be created
/// with `name`, under the directory identified by the ID. An `Option<String>` is returned
/// containing the ID of the new directory.
pub fn create_directory(parent_id: &Uuid, name: &str) -> Option<Uuid> {
    let json_str = serde_json::to_string(parent_id).unwrap();
    let json_box = Box::into_raw(Box::new(json_str.as_str()));

    let name = Box::into_raw(Box::new(name));
    let dir_id_ptr = unsafe { __create_directory(json_box as u32, name as u32) };

    if dir_id_ptr != -1 {
        // The JSON string is returned as a length at memory location 0, and the string's bytes
        // located at memory location 8.
        let len_buf = unsafe { slice::from_raw_parts(dir_id_ptr as *const u8, 8) };
        let len = u64::from_le_bytes(len_buf.try_into().unwrap());

        let json_str = unbox_slice(dir_id_ptr + 8, len as _);
        let dir_id: Uuid = serde_json::from_slice(json_str).unwrap();

        Some(dir_id)
    } else {
        None
    }
}

/// Open a directory
///
/// This function takes the `UfsUuid` of a parent directory (possibly the root directory) and the
/// `name` of a subdirectory to open. The returned `Option<String>` is the id belonging to the
/// `name`d directory.
pub fn open_directory(parent_id: &Uuid, name: &str) -> Option<Uuid> {
    let json_str = serde_json::to_string(parent_id).unwrap();
    let json_box = Box::into_raw(Box::new(json_str.as_str()));

    let name = Box::into_raw(Box::new(name));
    let dir_id_ptr = unsafe { __open_directory(json_box as u32, name as u32) };

    if dir_id_ptr != -1 {
        // The JSON string is returned as a length at memory location 0, and the string's bytes
        // located at memory location 8.
        let len_buf = unsafe { slice::from_raw_parts(dir_id_ptr as *const u8, 8) };
        let len = u64::from_le_bytes(len_buf.try_into().unwrap());

        let json_str = unbox_slice(dir_id_ptr + 8, len as _);
        let dir_id: Uuid = serde_json::from_slice(json_str).unwrap();

        Some(dir_id)
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

fn unbox_str<'a>(ptr: i32, len: i32) -> &'a str {
    let slice = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };
    str::from_utf8(&slice).expect("unable to unbox string")
}

fn unbox_slice<'a>(ptr: i32, len: i32) -> &'a [u8] {
    unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) }
}

//
// The following functions are called from Rust. They manipulate data coming across the WASM
// boundary, and make things nicer for the person writing a WASM program.
//
#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __init(ptr: i32, len: i32) {
    let json_str = unbox_slice(ptr, len);
    let root_id: Uuid = serde_json::from_slice(json_str).unwrap();
    unsafe { init(root_id) };
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
pub extern "C" fn __handle_file_create(payload_ptr: i32, payload_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileCreate) {
        let json_str = unbox_slice(payload_ptr, payload_len);
        let payload: MessagePayload = serde_json::from_slice(json_str).unwrap();
        func(Some(payload));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_dir_create(payload_ptr: i32, payload_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::DirCreate) {
        let json_str = unbox_slice(payload_ptr, payload_len);
        let payload: MessagePayload = serde_json::from_slice(json_str).unwrap();
        func(Some(payload));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_delete(payload_ptr: i32, payload_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileDelete) {
        let json_str = unbox_slice(payload_ptr, payload_len);
        let payload: MessagePayload = serde_json::from_slice(json_str).unwrap();
        func(Some(payload));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_dir_delete(payload_ptr: i32, payload_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::DirDelete) {
        let json_str = unbox_slice(payload_ptr, payload_len);
        let payload: MessagePayload = serde_json::from_slice(json_str).unwrap();
        func(Some(payload));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_open(payload_ptr: i32, payload_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileOpen) {
        let json_str = unbox_slice(payload_ptr, payload_len);
        let payload: MessagePayload = serde_json::from_slice(json_str).unwrap();
        func(Some(payload));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_close(payload_ptr: i32, payload_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileClose) {
        let json_str = unbox_slice(payload_ptr, payload_len);
        let payload: MessagePayload = serde_json::from_slice(json_str).unwrap();
        func(Some(payload));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_write(payload_ptr: i32, payload_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileWrite) {
        let json_str = unbox_slice(payload_ptr, payload_len);
        let payload: MessagePayload = serde_json::from_slice(json_str).unwrap();
        func(Some(payload));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_file_read(payload_ptr: i32, payload_len: i32) {
    let lookup = CALLBACK_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&WasmMessage::FileRead) {
        let json_str = unbox_slice(payload_ptr, payload_len);
        let payload: MessagePayload = serde_json::from_slice(json_str).unwrap();
        func(Some(payload));
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_http_get(route_ptr: i32, route_len: i32, json_ptr: i32, json_len: i32) {
    let route = unbox_string(route_ptr, route_len);

    let lookup = POST_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&route) {
        let slice = unbox_str(json_ptr, json_len);
        func(slice);
    }
}

#[doc(hidden)]
#[no_mangle]
pub extern "C" fn __handle_http_post(route_ptr: i32, route_len: i32, json_ptr: i32, json_len: i32) {
    let route = unbox_string(route_ptr, route_len);

    let lookup = POST_HANDLERS.read().unwrap();
    if let Some(func) = lookup.lookup(&route) {
        let slice = unbox_str(json_ptr, json_len);
        func(slice);
    }
}
