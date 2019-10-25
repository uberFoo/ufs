use {lazy_static::lazy_static, mut_static::MutStatic, uuid::Uuid, wasm_exports::*};

lazy_static! {
    pub static ref PROGRAM: MutStatic<Echo> = { MutStatic::from(Echo::new()) };
}

pub struct Echo {
    root_id: Option<Uuid>,
}

impl Echo {
    fn new() -> Self {
        Echo { root_id: None }
    }
}

#[no_mangle]
pub extern "C" fn init(root_id: Uuid) {
    // Initialize our main struct
    let mut pgm = PROGRAM.write().unwrap();
    // Store the root id
    pgm.root_id = Some(root_id);

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
    register_callback(WasmMessage::FileRead, handle_file_read);

    register_post_route("foo", post);
    // register_get_route("foo");
}

#[no_mangle]
pub extern "C" fn post(json: &str) {
    print(&format!("post called with {:#?}", json));
}

#[no_mangle]
pub extern "C" fn get(route: &str) {
    print(&format!("get called with {:#?}", route));
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
    if let Some(file) = payload {
        print(&format!("handle new file: {:#?}", file));
    }
}

#[no_mangle]
pub extern "C" fn handle_new_dir(payload: Option<MessagePayload>) {
    if let Some(dir) = payload {
        print(&format!("handle new dir: {:#?}", dir));
    }
}

#[no_mangle]
pub extern "C" fn handle_file_deleted(payload: Option<MessagePayload>) {
    if let Some(file) = payload {
        print(&format!("handle file deleted: {:#?}", file));
    }
}

#[no_mangle]
pub extern "C" fn handle_file_opened(payload: Option<MessagePayload>) {
    if let Some(file) = payload {
        print(&format!("handle file opened: {:#?}", file));
    }
}

#[no_mangle]
pub extern "C" fn handle_file_closed(payload: Option<MessagePayload>) {
    if let Some(file) = payload {
        let pgm = PROGRAM.read().unwrap();

        print(&format!("handle file closed: {:#?}", file));

        let handle = open_file(&file.id).unwrap();
        // print(&format!("open handle: {}", handle));
        let mut bytes: [u8; 256] = [0; 256];
        let mut offset = 0;
        let mut read_len = read_file(&handle, offset, &mut bytes);
        let str = String::from_utf8_lossy(&bytes);
        print(&format!("read len: {}\n data: {}", read_len, str));
        close_file(&handle);

        // Try creating a file in the directory.
        if let Some(file_handle) = create_file(pgm.root_id.as_ref().unwrap(), "baz") {
            print(&format!("File id: {:?}", file_handle));
            let len = write_file(&file_handle, "Hello World!\n".as_bytes());
            print(&format!("good write {}", len));
            close_file(&file_handle);
        } else {
            print("file create unsuccessful");
        }

        // Check for the "fubar" directory
        if let Some(dir_id) = open_directory(pgm.root_id.as_ref().unwrap(), "fubar") {
            print(&format!("found dir id: {:?}", dir_id));
        } else {
            if let Some(dir_id) = create_directory(pgm.root_id.as_ref().unwrap(), "fubar") {
                print(&format!("created dir id: {:?}", dir_id));
            }
        }

        // let dir_id = if let Some(dir_id) = open_directory(pgm.root_id.as_ref().unwrap(), "fubar") {
        //     print(&format!("found dir id: {:?}", dir_id));
        //     Some(dir_id)
        // } else {
        //     if let Some(dir_id) = create_directory(pgm.root_id.as_ref().unwrap(), "fubar") {
        //         print(&format!("created dir id: {:?}", dir_id));
        //         Some(dir_id)
        //     } else {
        //         None
        //     }
        // };

        // // Try creating a file in the directory.
        // if let Some(dir_id) = dir_id {
        //     if let Some(file_handle) = create_file(&dir_id, "baz") {
        //         let id = &file_handle.id;
        //         print(&format!("File id: {:?}", file_handle));
        //         write_file(id, file_handle.handle, 0, "Hello World!\n".as_bytes());
        //         close_file(id, file_handle.handle);
        //     } else {
        //         print("file create unsuccessful");
        //     }
        // }

        print("done!");
    }
}

#[no_mangle]
pub extern "C" fn handle_file_write(payload: Option<MessagePayload>) {
    if let Some(file) = payload {
        print(&format!("handle file write: {:#?}", file));
    }
}

#[no_mangle]
pub extern "C" fn handle_file_read(payload: Option<MessagePayload>) {
    if let Some(file) = payload {
        print(&format!("handle file read: {:#?}", file));
    }
}
