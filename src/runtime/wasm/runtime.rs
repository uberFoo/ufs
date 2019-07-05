//! Runtime in WASM-land
//!
//! The code here runs in WASM-land, i.e., these functions are available to WASM code. Some of the
//! functions are utility. Some are called by WASM code to perform operations on the file system.
//! Yet others are meant to unmarshal WASM data types back into Rust data types, from message
//! handlers invoked on the Rust side of things.
//!
//! These all call functions exposed to the WASM runtime. Some are defined by the user, and others
//! are defined in wasm.rs.
//!
use std::{
    alloc::{alloc, dealloc, Layout},
    mem,
};

pub use crate::metadata::FileHandle;

/// Rust methods, ultimately called by the WASM program.
extern "C" {
    pub fn __rust_print(string: u32);
    //     pub fn list_files(path: &str) -> Vec<(String, FileSize, Timespec)>;
    pub fn __rust_create_file(path: u32) -> u32;
    //     pub fn open_file(path: &str) -> Option<FileHandle>;
    pub fn __rust_close_file(handle: FileHandle);
    pub fn __rust_write_file(handle: FileHandle, data: u32);
    //     pub fn read_file(handle: FileHandle, offset: i32, size: u32)
    //         -> Result<Vec<u8>, failure::Error>;
    pub fn __rust_create_dir(path: u32);
}

/// Methods in the WASM program, ultimately called from Rust.
extern "C" {
    pub fn handle_file_create(path: &str);
    pub fn handle_file_remove(path: &str);
    pub fn handle_file_open(path: &str);
    pub fn handle_file_close(path: &str);
    pub fn handle_file_read(path: &str, data: &[u8]);
    pub fn handle_file_write(path: &str, data: &[u8]);
    pub fn handle_dir_create(path: &str);
    pub fn handle_dir_remove(path: &str);
}

#[allow(dead_code)]
pub fn print(msg: &str) {
    let msg = Box::into_raw(Box::new(msg));
    unsafe { __rust_print(msg as u32) };
}

/// This method is invoked by the user program to create a file in the file system. It converts the
/// string into a pointer and length, which are passed across the WASM boundary.
#[allow(dead_code)]
pub fn create_file(path: &str) -> Option<FileHandle> {
    let ptr = Box::into_raw(Box::new(path));
    let maybe_handle = unsafe { __rust_create_file(ptr as u32) };
    // FIXME: the handle _could_ be 0, and it can also overflow a u32
    if maybe_handle == 0 {
        None
    } else {
        Some(maybe_handle as FileHandle)
    }
}

#[allow(dead_code)]
pub fn close_file(handle: FileHandle) {
    print(&format!("calling `__rust_close_file({})`", handle));
    unsafe { __rust_close_file(handle) };
}

// FIXME: not returning Result<usize, failure::Error>, or similar.
#[allow(dead_code)]
pub fn write_file(handle: FileHandle, data: &[u8]) {
    let ptr = Box::into_raw(Box::new(data));
    unsafe { __rust_write_file(handle, ptr as u32) };
}

#[allow(dead_code)]
pub fn create_dir(path: &str) {
    let ptr = Box::into_raw(Box::new(path));
    unsafe { __rust_create_dir(ptr as u32) };
}

// The functions below run in WASM-land. They are invoked by functions in handler.rs. They serve to
// reconstruct values from WASM ints into Rust values, and then they invoke the user's Rust
// functions.
#[export_name = "file_create"]
pub unsafe extern "C" fn file_create(path_ptr: *const u8, path_len: usize) {
    let path = {
        let slice = ::std::slice::from_raw_parts(path_ptr, path_len);
        ::std::str::from_utf8_unchecked(slice)
    };
    handle_file_create(path);
}

#[export_name = "file_remove"]
pub unsafe extern "C" fn file_remove(path_str: *const u8, path_len: usize) {
    let path = {
        let slice = ::std::slice::from_raw_parts(path_str, path_len);
        ::std::str::from_utf8_unchecked(slice)
    };
    handle_file_remove(path);
}

#[export_name = "file_open"]
pub unsafe extern "C" fn file_open(path_str: *const u8, path_len: usize) {
    let path = {
        let slice = ::std::slice::from_raw_parts(path_str, path_len);
        ::std::str::from_utf8_unchecked(slice)
    };
    handle_file_open(path);
}

#[export_name = "file_close"]
pub unsafe extern "C" fn file_close(path_ptr: *const u8, path_len: usize) {
    let path = {
        let slice = ::std::slice::from_raw_parts(path_ptr, path_len);
        ::std::str::from_utf8_unchecked(slice)
    };
    handle_file_close(path);
}

#[export_name = "file_read"]
pub unsafe extern "C" fn file_read(
    path_ptr: *const u8,
    path_len: usize,
    data_ptr: *const u8,
    data_len: usize,
) {
    let path = {
        let slice = ::std::slice::from_raw_parts(path_ptr, path_len);
        ::std::str::from_utf8_unchecked(slice)
    };

    let data = ::std::slice::from_raw_parts(data_ptr, data_len);

    handle_file_read(path, data);
}

#[export_name = "file_write"]
pub unsafe extern "C" fn file_write(
    path_ptr: *const u8,
    path_len: usize,
    data_ptr: *const u8,
    data_len: usize,
) {
    let path = {
        let slice = ::std::slice::from_raw_parts(path_ptr, path_len);
        ::std::str::from_utf8_unchecked(slice)
    };

    let data = ::std::slice::from_raw_parts(data_ptr, data_len);

    handle_file_write(path, data);
}

#[export_name = "dir_create"]
pub unsafe extern "C" fn dir_create(path_ptr: *const u8, path_len: usize) {
    let path = {
        let slice = ::std::slice::from_raw_parts(path_ptr, path_len);
        ::std::str::from_utf8_unchecked(slice)
    };
    handle_dir_create(path);
}

#[export_name = "dir_remove"]
pub unsafe extern "C" fn dir_remove(path_str: *const u8, path_len: usize) {
    let path = {
        let slice = ::std::slice::from_raw_parts(path_str, path_len);
        ::std::str::from_utf8_unchecked(slice)
    };
    handle_dir_remove(path);
}

#[no_mangle]
pub extern "C" fn __wbindgen_malloc(size: usize) -> *mut u8 {
    let align = mem::align_of::<usize>();
    if let Ok(layout) = Layout::from_size_align(size, align) {
        unsafe {
            if layout.size() > 0 {
                let ptr = alloc(layout);
                if !ptr.is_null() {
                    return ptr;
                }
            } else {
                return align as *mut u8;
            }
        }
    }

    malloc_failure();
}

#[no_mangle]
pub unsafe extern "C" fn __wbindgen_free(ptr: *mut u8, size: usize) {
    // This happens for zero-length slices, and in that case `ptr` is
    // likely bogus so don't actually send this to the system allocator
    if size == 0 {
        return;
    }
    let align = mem::align_of::<usize>();
    let layout = Layout::from_size_align_unchecked(size, align);
    dealloc(ptr, layout);
}

#[cold]
#[no_mangle]
pub extern "C" fn malloc_failure() -> ! {
    std::process::abort();
}
