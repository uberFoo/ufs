//! The main WASM Runtime implementation
//!
//! Besides heading the module, the functions in here are closely related to what one finds in
//! exports.rs. In that file, functions are exported to WASM. Those functions are defined in this
//! file. They must unmarshal the WASM data types to Rust data types, and then do whatever it is
//! that they are meant to do.
//!
//! Some of the functions are utility, like `print`, and yet others are meant to effect changes to
//! the file system, via the code in fsops.rs.
//!
//! Overall, when calling into WASM with file system messages, the code in handler.rs is first
//! invoked.  That code marshals Rust data types to WASM data types, and then invokes code in WASM.
//! The code it invokes is located in runtime.rs, where it is unmarshaled back into Rust data types,
//! and the user-defined _handler_ functions are called.
//!
//! When calling from WASM into Rust, the wasmi runtime checks the export table in exports.rs. The
//! functions declared there are defined in runtime.rs. Those functions will eventually be resolved
//! into something that is defined in this file.
//!
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod exports;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod handler;

pub mod runtime;

use std::path::PathBuf;

use colored::*;
use log::info;
use wasmi::{Externals, LittleEndianConvert, MemoryRef, RuntimeArgs, RuntimeValue, Trap};

#[cfg(not(target_arch = "wasm32"))]
use crate::{
    fsops::FileSystemOps,
    metadata::FileHandle,
    runtime::wasm::exports::{
        CLOSE_FILE_INDEX, CREATE_DIR_INDEX, CREATE_FILE_INDEX, PRINT_INDEX, WRITE_FILE_INDEX,
    },
};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct WasmRuntime {
    name: String,
    memory: MemoryRef,
    file_system: Box<dyn FileSystemOps>,
}

#[cfg(not(target_arch = "wasm32"))]
impl WasmRuntime {
    pub fn new<S: AsRef<str>>(
        name: S,
        memory: MemoryRef,
        file_system: Box<dyn FileSystemOps>,
    ) -> Self {
        WasmRuntime {
            name: name.as_ref().to_owned(),
            memory,
            file_system,
        }
    }

    fn print(&self, message: u32) -> Result<Option<RuntimeValue>, Trap> {
        // Read the raw (ptr, len) tuple from memory
        let str_ptr = self.memory.get(message as u32, 8).unwrap();
        // Extract the pointer
        let ptr = u32::from_little_endian(&str_ptr).unwrap();
        // Extract the string length
        let len = u32::from_little_endian(&str_ptr[4..]).unwrap();
        // Dereference the pointer, and read `len` bytes.
        let payload = self.memory.get(ptr, len as usize).unwrap();
        // Tada!
        println!(
            " {}  {} 🔎  {}",
            "WASM".yellow(),
            self.name.cyan().underline(),
            String::from_utf8_lossy(payload.as_slice())
        );
        Ok(None)
    }

    fn create_file(&mut self, path: u32) -> Result<Option<RuntimeValue>, Trap> {
        // Read the raw (ptr, len) tuple from memory
        let str_ptr = self.memory.get(path as u32, 8).unwrap();
        // Extract the pointer
        let ptr = u32::from_little_endian(&str_ptr).unwrap();
        // Extract the string length
        let len = u32::from_little_endian(&str_ptr[4..]).unwrap();
        // Dereference the pointer, and read `len` bytes.
        let payload = self.memory.get(ptr, len as usize).unwrap();

        if let Ok(s) = String::from_utf8(payload) {
            let path = PathBuf::from(s);
            info!("`create_file` {:?}", path);
            if let Ok((handle, _)) = self.file_system.create_file(&path) {
                Ok(Some(RuntimeValue::I32(handle as i32)))
            } else {
                // FIXME: Should we trap here?
                Ok(None)
            }
        } else {
            // FIXME: should trap here
            Ok(None)
        }
    }

    fn close_file(&mut self, handle: FileHandle) -> Result<Option<RuntimeValue>, Trap> {
        info!("`close_file` handle {}", handle);
        self.file_system.close_file(handle);
        Ok(None)
    }

    fn write_file(&mut self, handle: FileHandle, data: u32) -> Result<Option<RuntimeValue>, Trap> {
        let data_ptr = self.memory.get(data as u32, 8).unwrap();
        let ptr = u32::from_little_endian(&data_ptr).unwrap();
        let len = u32::from_little_endian(&data_ptr[4..]).unwrap();
        let data = self.memory.get(ptr, len as usize).unwrap();

        info!("`write_file` handle {}, bytes {:?}", handle, data);
        if let Ok(_bytes) = self.file_system.write_file(handle, data.as_slice()) {
            // Ok(Some(RuntimeValue::I32(bytes as i32)))
            Ok(None)
        } else {
            // FIXME: should trap here
            Ok(None)
        }
    }

    fn create_dir(&mut self, path: u32) -> Result<Option<RuntimeValue>, Trap> {
        // Read the raw (ptr, len) tuple from memory
        let str_ptr = self.memory.get(path as u32, 8).unwrap();
        // Extract the pointer
        let ptr = u32::from_little_endian(&str_ptr).unwrap();
        // Extract the string length
        let len = u32::from_little_endian(&str_ptr[4..]).unwrap();
        // Dereference the pointer, and read `len` bytes.
        let payload = self.memory.get(ptr, len as usize).unwrap();

        if let Ok(s) = String::from_utf8(payload) {
            let path = PathBuf::from(s);
            info!("`create_dir`: {:?}", path);
            if let Ok(_) = self.file_system.create_dir(&path) {
                Ok(None)
            } else {
                // FIXME: should trap here?
                Ok(None)
            }
        } else {
            // FIXME: should trap here
            Ok(None)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Externals for WasmRuntime {
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        match index {
            PRINT_INDEX => {
                let str_ptr: u32 = args.nth(0);
                self.print(str_ptr)
            }
            CREATE_FILE_INDEX => {
                let path_ptr: u32 = args.nth(0);
                self.create_file(path_ptr)
            }
            CLOSE_FILE_INDEX => {
                let handle: FileHandle = args.nth(0);
                self.close_file(handle)
            }
            WRITE_FILE_INDEX => {
                let handle: FileHandle = args.nth(0);
                let data_ptr: u32 = args.nth(1);
                self.write_file(handle, data_ptr)
            }
            CREATE_DIR_INDEX => {
                let path_ptr: u32 = args.nth(0);
                self.create_dir(path_ptr)
            }
            _ => panic!("unknown export"),
        }
    }
}
