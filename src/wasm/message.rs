//! IOFS Messages sent to WASM programs
//!
//! These are file system events that are generated by the file system.
//!
use {
    crate::{uuid::UfsUuid, wasm::RuntimeErrorKind},
    failure,
    log::error,
    serde_derive::Serialize,
    serde_json,
    std::path::PathBuf,
    uuid::Uuid,
    wasm_exports::MessagePayload,
    wasmer_runtime::{Instance, Value},
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum IofsMessage {
    SystemMessage(IofsSystemMessage),
    FileMessage(IofsFileMessage),
    DirMessage(IofsDirMessage),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum IofsSystemMessage {
    Shutdown,
    Ping,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum IofsFileMessage {
    Create(IofsMessagePayload),
    Delete(IofsMessagePayload),
    Open(IofsMessagePayload),
    Close(IofsMessagePayload),
    Write(IofsMessagePayload),
    Read(IofsMessagePayload),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum IofsDirMessage {
    Create(IofsMessagePayload),
    Delete(IofsMessagePayload),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub(crate) struct IofsMessagePayload {
    pub(crate) target_id: UfsUuid,
    pub(crate) target_path: PathBuf,
    pub(crate) parent_id: UfsUuid,
}

impl From<&IofsMessagePayload> for MessagePayload {
    fn from(imp: &IofsMessagePayload) -> Self {
        MessagePayload {
            id: imp.target_id.into(),
            path: imp.target_path.clone(),
            parent_id: imp.parent_id.into(),
        }
    }
}

pub(crate) struct WasmMessageSender<'a> {
    instance: &'a mut Instance,
}

impl<'a> WasmMessageSender<'a> {
    pub(crate) fn new(instance: &'a mut Instance, root_id: UfsUuid) -> Self {
        let mut wms = WasmMessageSender { instance };

        let root_id: Uuid = root_id.into();
        let id_str = serde_json::to_string(&root_id).expect("unable to serialize JSON in new");
        wms.write_wasm_memory(0, &id_str);

        wms.call_wasm_func(
            "__init",
            Some(&[Value::I32(0), Value::I32(id_str.len() as _)]),
        )
        .expect("error calling init function");
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
                Err(RuntimeErrorKind::FunctionInvocation.into())
            }
        }
    }

    pub(crate) fn write_wasm_memory(&mut self, offset: usize, s: &str) {
        let memory = self.instance.context_mut().memory(0);

        for (byte, cell) in s
            .bytes()
            .zip(memory.view()[offset..(offset + s.len()) as usize].iter())
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

    pub(crate) fn send_file_create(
        &mut self,
        payload: &IofsMessagePayload,
    ) -> Result<(), failure::Error> {
        let payload: MessagePayload = payload.into();

        let json_str =
            serde_json::to_string(&payload).expect("unable to serialize JSON in send_file_create");

        self.write_wasm_memory(0, &json_str);

        self.call_wasm_func(
            "__handle_file_create",
            Some(&[Value::I32(0), Value::I32(json_str.len() as i32)]),
        )
    }

    pub(crate) fn send_dir_create(
        &mut self,
        payload: &IofsMessagePayload,
    ) -> Result<(), failure::Error> {
        let payload: MessagePayload = payload.into();
        let json_str =
            serde_json::to_string(&payload).expect("unable to serialize JSON in send_dir_create");

        self.write_wasm_memory(0, &json_str);

        self.call_wasm_func(
            "__handle_dir_create",
            Some(&[Value::I32(0), Value::I32(json_str.len() as i32)]),
        )
    }

    pub(crate) fn send_file_delete(
        &mut self,
        payload: &IofsMessagePayload,
    ) -> Result<(), failure::Error> {
        let payload: MessagePayload = payload.into();
        let json_str =
            serde_json::to_string(&payload).expect("unable to serialize JSON in send_file_delete");

        self.write_wasm_memory(0, &json_str);

        self.call_wasm_func(
            "__handle_file_delete",
            Some(&[Value::I32(0), Value::I32(json_str.len() as i32)]),
        )
    }

    pub(crate) fn send_dir_delete(
        &mut self,
        payload: &IofsMessagePayload,
    ) -> Result<(), failure::Error> {
        let payload: MessagePayload = payload.into();
        let json_str =
            serde_json::to_string(&payload).expect("unable to serialize JSON in send_dir_delete");

        self.write_wasm_memory(0, &json_str);

        self.call_wasm_func(
            "__handle_dir_delete",
            Some(&[Value::I32(0), Value::I32(json_str.len() as i32)]),
        )
    }

    pub(crate) fn send_file_open(
        &mut self,
        payload: &IofsMessagePayload,
    ) -> Result<(), failure::Error> {
        let payload: MessagePayload = payload.into();
        let json_str =
            serde_json::to_string(&payload).expect("unable to serialize JSON in send_file_open");

        self.write_wasm_memory(0, &json_str);

        self.call_wasm_func(
            "__handle_file_open",
            Some(&[Value::I32(0), Value::I32(json_str.len() as i32)]),
        )
    }

    pub(crate) fn send_file_close(
        &mut self,
        payload: &IofsMessagePayload,
    ) -> Result<(), failure::Error> {
        let payload: MessagePayload = payload.into();
        let json_str = serde_json::to_string(&payload)
            .expect("unable to serialize JSON in send_http_post send_file_close");

        self.write_wasm_memory(0, &json_str);

        self.call_wasm_func(
            "__handle_file_close",
            Some(&[Value::I32(0), Value::I32(json_str.len() as i32)]),
        )
    }

    pub(crate) fn send_file_write(
        &mut self,
        payload: &IofsMessagePayload,
    ) -> Result<(), failure::Error> {
        let payload: MessagePayload = payload.into();
        let json_str =
            serde_json::to_string(&payload).expect("unable to serialize JSON in send_file_write");

        self.write_wasm_memory(0, &json_str);

        self.call_wasm_func(
            "__handle_file_write",
            Some(&[Value::I32(0), Value::I32(json_str.len() as i32)]),
        )
    }

    pub(crate) fn send_file_read(
        &mut self,
        payload: &IofsMessagePayload,
    ) -> Result<(), failure::Error> {
        let payload: MessagePayload = payload.into();
        let json_str =
            serde_json::to_string(&payload).expect("unable to serialize JSON in send_file_read");

        self.write_wasm_memory(0, &json_str);

        self.call_wasm_func(
            "__handle_file_read",
            Some(&[Value::I32(0), Value::I32(json_str.len() as i32)]),
        )
    }

    // pub(crate) fn send_http_get(&mut self, msg: &IofsGetValue) -> Result<(), failure::Error> {
    //     let json_str =
    //         serde_json::to_string(msg.json()).expect("unable to serialize JSON in send_http_get");
    //     self.write_wasm_memory(0, &msg.route());
    //     self.write_wasm_memory(msg.route().len(), &json_str);
    //     self.call_wasm_func(
    //         "__handle_http_get",
    //         Some(&[
    //             Value::I32(0),
    //             Value::I32(msg.route().len() as i32),
    //             Value::I32(msg.route().len() as i32),
    //             Value::I32(json_str.len() as i32),
    //         ]),
    //     )
    // }

    // pub(crate) fn send_http_post(&mut self, msg: &IofsPostValue) -> Result<(), failure::Error> {
    //     let json_str =
    //         serde_json::to_string(msg.json()).expect("unable to serialize JSON in send_http_post");
    //     self.write_wasm_memory(0, &msg.route());
    //     self.write_wasm_memory(msg.route().len(), &json_str);
    //     self.call_wasm_func(
    //         "__handle_http_post",
    //         Some(&[
    //             Value::I32(0),
    //             Value::I32(msg.route().len() as i32),
    //             Value::I32(msg.route().len() as i32),
    //             Value::I32(json_str.len() as i32),
    //         ]),
    //     )
    // }
}
