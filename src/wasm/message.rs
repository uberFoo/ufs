use {
    crate::wasm::RuntimeErrorKind,
    failure,
    log::error,
    wasmer_runtime::{Instance, Value},
};

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum IofsMessage {
    SystemMessage(IofsSystemMessage),
    FileMessage(IofsFileMessage),
    DirMessage(IofsDirMessage),
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum IofsSystemMessage {
    Shutdown,
    Ping,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum IofsFileMessage {
    NewFile(String),
    FileChanged,
    FileWritten,
    FileRead,
    FileDeleted(String),
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum IofsDirMessage {
    NewDir(String),
    DirChanged,
}

pub(crate) struct WasmMessageSender<'a> {
    instance: &'a mut Instance,
}

impl<'a> WasmMessageSender<'a> {
    pub(crate) fn new(instance: &'a mut Instance) -> Self {
        let mut wms = WasmMessageSender { instance };
        wms.call_wasm_func("init", None)
            .expect("Unable to call init function");
        wms
    }

    pub(crate) fn call_wasm_func(
        &mut self,
        name: &str,
        args: Option<&[Value]>,
    ) -> Result<(), failure::Error> {
        let args = match args {
            Some(a) => a,
            None => &[],
        };

        match self.instance.call(name, args) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Error invoking wasm function {}", e);
                Err(RuntimeErrorKind::FunctionInvocationFailure.into())
            }
        }
    }

    pub(crate) fn write_wasm_memory(&mut self, s: &str) {
        let memory = self.instance.context_mut().memory(0);

        for (byte, cell) in s
            .bytes()
            .zip(memory.view()[0 as usize..(s.len()) as usize].iter())
        {
            cell.set(byte);
        }
    }

    pub(crate) fn send_shutdown(&mut self) -> Result<(), failure::Error> {
        self.call_wasm_func("__handle_shutdown", None)
    }

    pub(crate) fn send_ping(&mut self) -> Result<(), failure::Error> {
        self.call_wasm_func("__handle_ping", None)
    }

    pub(crate) fn send_new_file(&mut self, path: &str) -> Result<(), failure::Error> {
        self.write_wasm_memory(path);
        self.call_wasm_func(
            "__handle_new_file",
            Some(&[Value::I32(0), Value::I32(path.len() as i32)]),
        )
    }

    pub(crate) fn send_file_deleted(&mut self, path: &str) -> Result<(), failure::Error> {
        self.write_wasm_memory(path);
        self.call_wasm_func(
            "__handle_file_delete",
            Some(&[Value::I32(0), Value::I32(path.len() as i32)]),
        )
    }

    pub(crate) fn send_new_dir(&mut self, path: &str) -> Result<(), failure::Error> {
        self.write_wasm_memory(path);
        self.call_wasm_func(
            "__handle_new_dir",
            Some(&[Value::I32(0), Value::I32(path.len() as i32)]),
        )
    }
}
