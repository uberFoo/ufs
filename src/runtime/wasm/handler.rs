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
                Ok(v) => info!("`file_create` success"),
                Err(e) => error!("error invoking `file_create` in WASM: {:?}", e),
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
                Ok(v) => info!("`file_close` success"),
                Err(e) => error!("error invoking `file_close` in WASM: {:?}", e),
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
                    Ok(v) => info!("`file_write` success"),
                    Err(e) => error!("error invoking `file_write` in WASM: {:?}", e),
                }
            }
        }
    }
}
