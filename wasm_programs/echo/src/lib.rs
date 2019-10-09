use {lazy_static::lazy_static, mut_static::MutStatic, wasm_exports::*};

lazy_static! {
    pub static ref PROGRAM: MutStatic<Echo> = { MutStatic::from(Echo::new()) };
}

pub struct Echo {
    root_id: Option<String>,
}

impl Echo {
    fn new() -> Self {
        Echo { root_id: None }
    }
}

#[no_mangle]
pub extern "C" fn init(root_id: RefStr) {
    // Initialize our main struct
    let mut pgm = PROGRAM.write().unwrap();
    // Store the root id
    let root_id = root_id.get_str();
    pgm.root_id = Some(root_id.to_string());

    print(&format!("Starting at root directory {:?}.", pgm.root_id));

    // Register our callback functions
    register_callback(WasmMessage::Ping, ping);
    register_callback(WasmMessage::Shutdown, shutdown);
    register_callback(WasmMessage::FileCreate, handle_new_file);
    register_callback(WasmMessage::DirCreate, handle_new_dir);
    register_callback(WasmMessage::FileDelete, handle_file_deleted);
    register_callback(WasmMessage::FileOpen, handle_file_opened);
    register_callback(WasmMessage::FileClose, handle_file_closed);
    register_callback(WasmMessage::FileWrite, handle_file_write);
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
    if let Some(MessagePayload::FileCreate(file)) = payload {
        let file = file.unpack();
        print(&format!(
            "handle new file: {:?} ({}) under directory {}",
            file.path, file.id, file.dir_id
        ));
    }
}

#[no_mangle]
pub extern "C" fn handle_new_dir(payload: Option<MessagePayload>) {
    if let Some(MessagePayload::PathAndId(path, id)) = payload {
        print(&format!(
            "handle new dir: {:?} ({})",
            path.get_str(),
            id.get_str()
        ));
    }
}

#[no_mangle]
pub extern "C" fn handle_file_deleted(payload: Option<MessagePayload>) {
    if let Some(MessagePayload::PathAndId(path, id)) = payload {
        print(&format!(
            "handle file deleted: {:?} ({})",
            path.get_str(),
            id.get_str()
        ));
    }
}

#[no_mangle]
pub extern "C" fn handle_file_opened(payload: Option<MessagePayload>) {
    if let Some(MessagePayload::PathAndId(path, id)) = payload {
        print(&format!(
            "handle file opened: {:?} ({})",
            path.get_str(),
            id.get_str()
        ));
    }
}

#[no_mangle]
pub extern "C" fn handle_file_closed(payload: Option<MessagePayload>) {
    if let Some(MessagePayload::PathAndId(path, id)) = payload {
        let pgm = PROGRAM.read().unwrap();

        print(&format!(
            "handle file closed: {:?} ({})",
            path.get_str(),
            id.get_str()
        ));

        // Check for the "fubar" directory
        let dir_id = if let Some(dir_id) = open_directory(pgm.root_id.as_ref().unwrap(), "fubar") {
            print(&format!("found dir id: {:?}", dir_id));
            Some(dir_id)
        } else {
            if let Some(dir_id) = create_directory(pgm.root_id.as_ref().unwrap(), "fubar") {
                print(&format!("created dir id: {:?}", dir_id));
                Some(dir_id)
            } else {
                None
            }
        };

        // Try creating a file in the directory.
        if let Some(dir_id) = dir_id {
            if let Some(file_handle) = create_file(&dir_id, "baz") {
                let id = &file_handle.id;
                print(&format!("File id: {:?}", file_handle));
                write_file(id, file_handle.handle, 0, "Hello World!\n".as_bytes());
                close_file(id, file_handle.handle);
            } else {
                print("file create unsuccessful");
            }
        }

        // let handle = open_file(id);
        // print(&format!("open handle: {}", handle));
        // let mut bytes: [u8; 256] = [0; 256];
        // let mut offset = 0;
        // let mut read_len = read_file(handle, offset, &mut bytes);
        // while read_len > 0 {
        //     offset += read_len;
        //     let str = String::from_utf8_lossy(&bytes);
        //     print(&format!("read len: {}\n data: {}", read_len, str));
        //     read_len = read_file(handle, offset, &mut bytes);
        // }
    }
}

#[no_mangle]
pub extern "C" fn handle_file_write(payload: Option<MessagePayload>) {
    if let Some(MessagePayload::PathAndId(path, id)) = payload {
        print(&format!(
            "handle file write: {:?} ({})",
            path.get_str(),
            id.get_str()
        ));
    }
}
