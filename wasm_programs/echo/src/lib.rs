use {lazy_static::lazy_static, mut_static::MutStatic, wasm_exports::*};

lazy_static! {
    pub static ref PROGRAM: MutStatic<Echo> = { MutStatic::from(Echo::new()) };
}

pub struct Echo {}

impl Echo {
    fn new() -> Self {
        Echo {}
    }
}

#[no_mangle]
pub extern "C" fn init() {
    let mut _pgm = PROGRAM.write().unwrap();
    register_callback(WasmMessage::Ping, ping);
    register_callback(WasmMessage::Shutdown, shutdown);
    register_callback(WasmMessage::NewFile, handle_new_file);
    register_callback(WasmMessage::FileDeleted, handle_file_deleted);
    register_callback(WasmMessage::NewDir, handle_new_dir);
}

#[no_mangle]
pub extern "C" fn ping(_payload: Option<MessagePayload>) {
    unsafe {
        pong();
    }
}

#[no_mangle]
pub extern "C" fn shutdown(_payload: Option<MessagePayload>) {
    print("shutdown");
}

#[no_mangle]
pub extern "C" fn handle_new_file(payload: Option<MessagePayload>) {
    if let Some(MessagePayload::String(path)) = payload {
        print(&format!("handle new file: {:?}", path.get_str()));
    }
}

#[no_mangle]
pub extern "C" fn handle_file_deleted(payload: Option<MessagePayload>) {
    if let Some(MessagePayload::String(path)) = payload {
        print(&format!("handle file deleted: {:?}", path.get_str()));
    }
}

#[no_mangle]
pub extern "C" fn handle_new_dir(payload: Option<MessagePayload>) {
    if let Some(MessagePayload::String(path)) = payload {
        print(&format!("handle new dir: {:?}", path.get_str()));
    }
}
