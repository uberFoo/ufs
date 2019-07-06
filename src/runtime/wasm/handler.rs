//! `UfsMessage` handling in WASM
//!
//! Herein lies the first step in getting file system messages into WASM-land.
//!
//! The `WasmMessageHandler` struct maintains a handle to the wasm runtime. The impl block contains
//! functions that are invoked _by Rust_ to transform Rust data types into WASM data types, and then
//! invokes the WASM functions in WASM-land. The WASM-side functions are defined in runtime.rs.
//!
use log::{error, info};
use wasmi::{MemoryRef, ModuleRef, RuntimeValue};

use crate::runtime::{fsops::FileSystemOps, message::UfsMessageHandler, wasm::WasmRuntime};

pub(crate) struct WasmMessageHandler {
    instance: ModuleRef,
    memory: MemoryRef,
    runtime: WasmRuntime,
}

impl WasmMessageHandler {
    pub fn new(instance: ModuleRef, fs: Box<dyn FileSystemOps>) -> Self {
        let memory = instance
            .export_by_name("memory")
            .expect("`memory` export not found")
            .as_memory()
            .expect("export name `memory` is not of memory type")
            .clone();

        let runtime = WasmRuntime::new(memory.clone(), fs);

        WasmMessageHandler {
            instance,
            memory,
            runtime,
        }
    }
}

impl UfsMessageHandler for WasmMessageHandler {
    fn file_create(&mut self, path: &str) {
        // Allocate memory for the path string
        let mut args = Vec::<RuntimeValue>::new();
        args.push(RuntimeValue::from(path.len() as i32));
        if let Ok(Some(RuntimeValue::I32(mem_ptr))) =
            self.instance
                .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
        {
            self.memory.set(mem_ptr as u32, path.as_bytes()).unwrap();

            // Invoke the function in WASM-land
            let mut args = Vec::<RuntimeValue>::new();
            args.push(RuntimeValue::from(mem_ptr));
            args.push(RuntimeValue::from(path.len() as i32));
            info!("handle `file_create` {}", path);
            match self
                .instance
                .invoke_export("file_create", &args, &mut self.runtime)
            {
                Ok(_) => info!("`file_create` success"),
                Err(e) => error!("error invoking `file_create` in WASM: {}", e),
            }
        }
    }

    fn file_remove(&mut self, path: &str) {
        // Allocate memory for the path string
        let mut args = Vec::<RuntimeValue>::new();
        args.push(RuntimeValue::from(path.len() as i32));
        if let Ok(Some(RuntimeValue::I32(mem_ptr))) =
            self.instance
                .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
        {
            self.memory.set(mem_ptr as u32, path.as_bytes()).unwrap();

            // Invoke the function in WASM-land
            let mut args = Vec::<RuntimeValue>::new();
            args.push(RuntimeValue::from(mem_ptr));
            args.push(RuntimeValue::from(path.len() as i32));
            info!("handle `file_remove` {}", path);
            match self
                .instance
                .invoke_export("file_remove", &args, &mut self.runtime)
            {
                Ok(_) => info!("`file_remove` success"),
                Err(e) => error!("error invoking `file_remove` in WASM: {}", e),
            }
        }
    }

    fn file_open(&mut self, path: &str) {
        // Allocate memory for the path string
        let mut args = Vec::<RuntimeValue>::new();
        args.push(RuntimeValue::from(path.len() as i32));
        if let Ok(Some(RuntimeValue::I32(mem_ptr))) =
            self.instance
                .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
        {
            self.memory.set(mem_ptr as u32, path.as_bytes()).unwrap();

            // Invoke the function in WASM-land
            let mut args = Vec::<RuntimeValue>::new();
            args.push(RuntimeValue::from(mem_ptr));
            args.push(RuntimeValue::from(path.len() as i32));
            info!("handle `file_open` {}", path);
            match self
                .instance
                .invoke_export("file_open", &args, &mut self.runtime)
            {
                Ok(_) => info!("`file_open` success"),
                Err(e) => error!("error invoking `file_open` in WASM: {}", e),
            }
        }
    }

    fn file_close(&mut self, path: &str) {
        // Allocate memory for the path string
        let mut args = Vec::<RuntimeValue>::new();
        args.push(RuntimeValue::from(path.len() as i32));
        if let Ok(Some(RuntimeValue::I32(mem_ptr))) =
            self.instance
                .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
        {
            self.memory.set(mem_ptr as u32, path.as_bytes()).unwrap();

            // Invoke the function in WASM-land
            let mut args = Vec::<RuntimeValue>::new();
            args.push(RuntimeValue::from(mem_ptr));
            args.push(RuntimeValue::from(path.len() as i32));
            info!("handle `file_close` {}", path);
            match self
                .instance
                .invoke_export("file_close", &args, &mut self.runtime)
            {
                Ok(_) => info!("`file_close` success"),
                Err(e) => error!("error invoking `file_close` in WASM: {}", e),
            }
        }
    }

    fn file_read(&mut self, path: &str, data: &[u8]) {
        // Allocate memory for the path string
        let mut args = Vec::<RuntimeValue>::new();
        args.push(RuntimeValue::from(path.len() as i32));
        if let Ok(Some(RuntimeValue::I32(path_ptr))) =
            self.instance
                .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
        {
            self.memory.set(path_ptr as u32, path.as_bytes()).unwrap();

            // Allocate memory for the data
            let mut args = Vec::<RuntimeValue>::new();
            args.push(RuntimeValue::from(data.len() as i32));
            if let Ok(Some(RuntimeValue::I32(data_ptr))) =
                self.instance
                    .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
            {
                self.memory.set(data_ptr as u32, data).unwrap();

                // Invoke the function in WASM-land
                let mut args = Vec::<RuntimeValue>::new();
                args.push(RuntimeValue::from(path_ptr));
                args.push(RuntimeValue::from(path.len() as i32));
                args.push(RuntimeValue::from(data_ptr));
                args.push(RuntimeValue::from(data.len() as i32));
                info!("handle `file_read` {}, data len {}", path, data.len());
                match self
                    .instance
                    .invoke_export("file_read", &args, &mut self.runtime)
                {
                    Ok(_) => info!("`file_read` success"),
                    Err(e) => error!("error invoking `file_read` in WASM: {}", e),
                }
            }
        }
    }

    fn file_write(&mut self, path: &str, data: &[u8]) {
        // Allocate memory for the path string
        let mut args = Vec::<RuntimeValue>::new();
        args.push(RuntimeValue::from(path.len() as i32));
        if let Ok(Some(RuntimeValue::I32(path_ptr))) =
            self.instance
                .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
        {
            self.memory.set(path_ptr as u32, path.as_bytes()).unwrap();

            // Allocate memory for the data
            let mut args = Vec::<RuntimeValue>::new();
            args.push(RuntimeValue::from(data.len() as i32));
            if let Ok(Some(RuntimeValue::I32(data_ptr))) =
                self.instance
                    .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
            {
                self.memory.set(data_ptr as u32, data).unwrap();

                // Invoke the function in WASM-land
                let mut args = Vec::<RuntimeValue>::new();
                args.push(RuntimeValue::from(path_ptr));
                args.push(RuntimeValue::from(path.len() as i32));
                args.push(RuntimeValue::from(data_ptr));
                args.push(RuntimeValue::from(data.len() as i32));
                info!("handle `file_write` {}, data len {}", path, data.len());
                match self
                    .instance
                    .invoke_export("file_write", &args, &mut self.runtime)
                {
                    Ok(_) => info!("`file_write` success"),
                    Err(e) => error!("error invoking `file_write` in WASM: {}", e),
                }
            }
        }
    }

    fn dir_create(&mut self, path: &str) {
        // Allocate memory for the path string
        let mut args = Vec::<RuntimeValue>::new();
        args.push(RuntimeValue::from(path.len() as i32));
        if let Ok(Some(RuntimeValue::I32(mem_ptr))) =
            self.instance
                .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
        {
            self.memory.set(mem_ptr as u32, path.as_bytes()).unwrap();

            // Invoke the function in WASM-land
            let mut args = Vec::<RuntimeValue>::new();
            args.push(RuntimeValue::from(mem_ptr));
            args.push(RuntimeValue::from(path.len() as i32));
            info!("handle `dir_create` {}", path);
            match self
                .instance
                .invoke_export("dir_create", &args, &mut self.runtime)
            {
                Ok(_) => info!("`dir_create` success"),
                Err(e) => error!("error invoking `dir_create` in WASM: {}", e),
            }
        }
    }

    fn dir_remove(&mut self, path: &str) {
        // Allocate memory for the path string
        let mut args = Vec::<RuntimeValue>::new();
        args.push(RuntimeValue::from(path.len() as i32));
        if let Ok(Some(RuntimeValue::I32(mem_ptr))) =
            self.instance
                .invoke_export("__wbindgen_malloc", &args, &mut self.runtime)
        {
            self.memory.set(mem_ptr as u32, path.as_bytes()).unwrap();

            // Invoke the function in WASM-land
            let mut args = Vec::<RuntimeValue>::new();
            args.push(RuntimeValue::from(mem_ptr));
            args.push(RuntimeValue::from(path.len() as i32));
            info!("handle `dir_remove` {}", path);
            match self
                .instance
                .invoke_export("dir_remove", &args, &mut self.runtime)
            {
                Ok(_) => info!("`dir_remove` success"),
                Err(e) => error!("error invoking `dir_remove` in WASM: {}", e),
            }
        }
    }
}
