use {
    lazy_static::lazy_static,
    mut_static::MutStatic,
    wasm_exports::{pong, print, register_callback, MessagePayload, WasmMessage},
};

lazy_static! {
    pub static ref PROGRAM: MutStatic<Test> = { MutStatic::from(Test::new()) };
}

pub struct Test {
    ping_count: usize,
}

impl Test {
    fn new() -> Self {
        Test { ping_count: 0 }
    }
}

#[no_mangle]
pub extern "C" fn init() {
    let mut _pgm = PROGRAM.write().unwrap();
    register_callback(WasmMessage::Ping, ping);
    register_callback(WasmMessage::Shutdown, shutdown);
    register_callback(WasmMessage::NewFile, handle_new_file);
}

#[no_mangle]
pub extern "C" fn ping(_payload: Option<MessagePayload>) {
    let mut pgm = PROGRAM.write().unwrap();
    pgm.ping_count += 1;
    print("test ping");
    unsafe {
        pong();
    }
}

#[no_mangle]
pub extern "C" fn shutdown(_payload: Option<MessagePayload>) {
    let pgm = PROGRAM.read().unwrap();

    print(&format!(
        "Hello uberFoo, I was pinged {} time(s).",
        pgm.ping_count
    ));
}

#[no_mangle]
pub extern "C" fn handle_new_file(payload: Option<MessagePayload>) {
    if let Some(MessagePayload::String(path)) = payload {
        print(&format!("handle new file: {:?}", path.get_str()));
    }
}
