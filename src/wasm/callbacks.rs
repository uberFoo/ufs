//! Extern WASM function implementations
//!
//! Functions that are declared in the WASM program as `extern` are resolved here.
//!
use {
    crate::{block::BlockStorage, wasm::WasmProcess, OpenFileMode},
    colored::*,
    log::{debug, error, info},
    std::{convert::TryInto, str},
    uuid::Uuid,
    wasm_exports::{FileHandle, WasmMessage},
    wasmer_runtime::Ctx,
};

pub(crate) fn __register_for_callback<B>(ctx: &mut Ctx, message_ptr: u32)
where
    B: BlockStorage + 'static,
{
    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let message = unbox_message(ctx, message_ptr);
    info!("register notification {:?}", message);
    wc.set_handles_message(message);
}

pub(crate) fn __register_get_handler<B>(ctx: &mut Ctx, get_route_ptr: u32)
where
    B: BlockStorage + 'static,
{
    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let route = unbox_str(ctx, get_route_ptr);
    info!("register GET handler {:?}", route);
    wc.register_get_callback(route);
}

pub(crate) fn __register_post_handler<B>(ctx: &mut Ctx, post_route_ptr: u32)
where
    B: BlockStorage + 'static,
{
    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let route = unbox_str(ctx, post_route_ptr);
    info!("register POST handler {:?}", route);
    wc.register_post_callback(route);
}

pub(crate) fn pong(_ctx: &mut Ctx) {
    debug!("pong");
}

pub(crate) fn __print<B>(ctx: &mut Ctx, str_ptr: u32)
where
    B: BlockStorage + 'static,
{
    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let payload = unbox_str(ctx, str_ptr);
    println!(
        " {}  {} 🔎  {}",
        "WASM".yellow(),
        wc.name().cyan().underline(),
        payload
    );
}

pub(crate) fn __open_file<B>(ctx: &mut Ctx, id_ptr: u32) -> u64
where
    B: BlockStorage + 'static,
{
    debug!("--------");
    debug!("__open_file: id_ptr: {}", id_ptr);

    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let id_json = unbox_str(ctx, id_ptr);
    let id: Uuid = serde_json::from_str(&id_json).expect("unable to deserialize id in __open_file");
    debug!("\tid: {}", id);

    let file = wc.open_file(id.into(), OpenFileMode::Read);

    match file {
        Ok(handle) => handle,
        Err(e) => {
            error!("Unable to open file: {}", e);
            0
        }
    }
}

pub(crate) fn __close_file<B>(ctx: &mut Ctx, id_ptr: u32, handle: u64)
where
    B: BlockStorage + 'static,
{
    debug!("--------");
    debug!("__close_file: id_ptr: {}, handle: {}", id_ptr, handle);

    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let id_json = unbox_str(ctx, id_ptr);
    let id: Uuid = serde_json::from_str(&id_json).expect("unable to deserialize id in __open_file");
    debug!("\tid: {}", id);

    wc.close_file(id.into(), handle);
}

pub(crate) fn __read_file<B>(
    ctx: &mut Ctx,
    id_ptr: u32,
    handle: u64,
    offset: u32,
    data_ptr: u32,
    data_len: u32,
) -> u32
where
    B: BlockStorage + 'static,
{
    debug!("--------");
    debug!(
        "__read_file: handle: {}, data_ptr: {}, data_len: {}",
        handle, data_ptr, data_len
    );

    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let id_json = unbox_str(ctx, id_ptr);
    let id: Uuid = serde_json::from_str(&id_json).expect("unable to deserialize id in __open_file");
    debug!("\tid: {}", id);

    let file_size = {
        let guard = wc.iofs.clone();
        let guard = guard.lock().expect("poisoned iofs lock");
        guard
            .get_file_size(handle)
            .expect("tried to read invalid file handle")
    };
    let read_len = std::cmp::min(data_len as u64, file_size - offset as u64);
    let bytes = wc.read_file(id.into(), handle, offset as _, read_len as _);

    match bytes {
        Ok(bytes) => {
            let memory = ctx.memory(0);
            for (i, cell) in memory.view()[data_ptr as _..data_ptr as usize + bytes.len()]
                .iter()
                .enumerate()
            {
                cell.set(bytes[i]);
            }
            bytes.len() as _
        }
        Err(_) => 0,
    }
}

pub(crate) fn __write_file<B>(
    ctx: &mut Ctx,
    id_ptr: u32,
    handle: u64,
    data_ptr: u32,
    data_len: u32,
) -> u32
where
    B: BlockStorage + 'static,
{
    debug!("--------");
    debug!(
        "__write_file: handle: {}, data_ptr: {}, data_len: {}",
        handle, data_ptr, data_len
    );

    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let id_json = unbox_str(ctx, id_ptr);
    let id: Uuid = serde_json::from_str(&id_json).expect("unable to deserialize id in __open_file");
    debug!("\tid: {}", id);

    let memory = ctx.memory(0);
    let bytes: Vec<u8> = memory.view()[data_ptr as usize..(data_ptr + data_len) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();

    let bytes_written = wc.write_file(id.into(), handle, &bytes);
    debug!("\twrote {:?} bytes", bytes_written);

    match bytes_written {
        Ok(i) => i as u32,
        Err(_) => 0,
    }
}

pub(crate) fn __create_file<B>(ctx: &mut Ctx, parent_id_ptr: u32, name_ptr: u32) -> i32
where
    B: BlockStorage + 'static,
{
    debug!("--------");
    debug!("__create_file: name_ptr: {}", name_ptr);

    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let id_json = unbox_str(ctx, parent_id_ptr);
    let parent_id: Uuid =
        serde_json::from_str(&id_json).expect("unable to deserialize id in __open_file");
    debug!("\tparent_id: {}", parent_id);

    let name = unbox_str(ctx, name_ptr);
    let file = wc.create_file(parent_id.into(), &name);

    match file {
        Ok((handle, file)) => {
            debug!(
                "created file {:?}, handle: {}, id: {}",
                name, handle, file.file_id
            );

            // Pass the JSON result to Wasm-land by putting the string length at memory address 0,
            // and then putting the string itself at memory address 8.
            let json_str = serde_json::to_string(&FileHandle {
                handle,
                id: file.file_id.into(),
            })
            .expect("unable to serialize JSON in __create_file");

            let memory = ctx.memory(0);
            let len = (json_str.len() as u64).to_le_bytes();
            for (i, cell) in memory.view()[0..len.len()].iter().enumerate() {
                cell.set(len[i]);
            }

            for (byte, cell) in json_str
                .bytes()
                .zip(memory.view()[8..8 + json_str.len()].iter())
            {
                cell.set(byte);
            }

            0
        }
        Err(e) => {
            error!("unable to create file {}", e);
            -1
        }
    }
}

pub(crate) fn __create_directory<B>(ctx: &mut Ctx, parent_id_ptr: u32, name_ptr: u32) -> i32
where
    B: BlockStorage + 'static,
{
    debug!("--------");
    debug!(
        "__create_directory: parent_id_ptr: {}, name_ptr: {}",
        parent_id_ptr, name_ptr
    );

    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let id_json = unbox_str(ctx, parent_id_ptr);
    let parent_id: Uuid =
        serde_json::from_str(&id_json).expect("unable to deserialize id in __open_file");
    debug!("\tparent_id: {}", parent_id);

    let name = unbox_str(ctx, name_ptr);
    debug!("\tid: {}", parent_id);

    let dir = wc.create_directory(parent_id.into(), &name);

    match dir {
        Ok(dir) => {
            debug!("created directory {:?} with id {}", name, dir.id());

            // Pass the JSON result to Wasm-land by putting the string length at memory address 0,
            // and then putting the string itself at memory address 8.
            let json_str = serde_json::to_string(&Uuid::from(dir.id()))
                .expect("unable to serialize JSON in __create_directory");

            let memory = ctx.memory(0);
            let len = (json_str.len() as u64).to_le_bytes();
            for (i, cell) in memory.view()[0..len.len()].iter().enumerate() {
                cell.set(len[i]);
            }

            for (byte, cell) in json_str
                .bytes()
                .zip(memory.view()[8..8 + json_str.len()].iter())
            {
                cell.set(byte);
            }
            0
        }
        Err(_) => -1,
    }
}

/// "Open" a directory
///
/// Perhaps this should be called __find_directory? What it does is search a parent directory for
/// a named subdirectory. So it's very much like open_file, which may explain it's name. If the
/// subdirectory is found, it's ID is returned.
///
/// There is no analog when working with the FUSE interface, and thus there is no notification send
/// to the Wasm programs. Therefore, there's no need to route this through WasmProgram.
pub(crate) fn __open_directory<B>(ctx: &mut Ctx, parent_id_ptr: u32, name_ptr: u32) -> i32
where
    B: BlockStorage + 'static,
{
    debug!("--------");
    debug!(
        "__open_directory: parent_id_ptr: {}, name_ptr: {}",
        parent_id_ptr, name_ptr
    );

    let wc: &mut WasmProcess<B> = unsafe { &mut *(ctx.data as *mut WasmProcess<B>) };
    let id_json = unbox_str(ctx, parent_id_ptr);
    let parent_id: Uuid =
        serde_json::from_str(&id_json).expect("unable to deserialize id in __open_file");
    debug!("\tparent_id: {}", parent_id);

    let name = unbox_str(ctx, name_ptr);

    debug!("\tid: {}", parent_id);

    let dir = wc.open_directory(parent_id.into(), &name);

    match dir {
        Ok(dir) => {
            debug!("found directory {:?} with id {}", name, dir);

            // Pass the JSON result to Wasm-land by putting the string length at memory address 0,
            // and then putting the string itself at memory address 8.
            let json_str = serde_json::to_string(&Uuid::from(dir))
                .expect("unable to serialize JSON in __create_directory");

            let memory = ctx.memory(0);
            let len = (json_str.len() as u64).to_le_bytes();
            for (i, cell) in memory.view()[0..len.len()].iter().enumerate() {
                cell.set(len[i]);
            }

            for (byte, cell) in json_str
                .bytes()
                .zip(memory.view()[8..8 + json_str.len()].iter())
            {
                cell.set(byte);
            }
            0
        }
        Err(_) => -1,
    }
}

fn unbox_message(ctx: &Ctx, msg_ptr: u32) -> WasmMessage {
    let memory = ctx.memory(0);
    let ptr_vec: Vec<_> = memory.view()[msg_ptr as usize..(msg_ptr + 4) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();
    let ptr = u32::from_le_bytes(
        ptr_vec
            .as_slice()
            .try_into()
            .expect("error unboxing message"),
    );

    match ptr {
        0 => WasmMessage::Shutdown,
        1 => WasmMessage::Ping,
        2 => WasmMessage::FileCreate,
        3 => WasmMessage::DirCreate,
        4 => WasmMessage::FileDelete,
        5 => WasmMessage::DirDelete,
        6 => WasmMessage::FileOpen,
        7 => WasmMessage::FileClose,
        8 => WasmMessage::FileRead,
        9 => WasmMessage::FileWrite,
        _ => panic!("Invalid value decoding WasmMessage"),
    }
}

fn unbox_str(ctx: &Ctx, str_ptr: u32) -> String {
    debug!("unbox_str: str_ptr {}", str_ptr);
    let memory = ctx.memory(0);

    // The &str is stored as a u32 pointer, followed by a length. We first extract the pointer
    // from memory.
    let ptr_vec: Vec<_> = memory.view()[str_ptr as usize..(str_ptr + 4) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();
    let ptr = u32::from_le_bytes(ptr_vec.as_slice().try_into().unwrap());
    debug!("\tptr: {}", ptr);

    // And then we extract the length.
    let len_vec: Vec<_> = memory.view()[(str_ptr + 4) as usize..(str_ptr + 8) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();
    let len = u32::from_le_bytes(len_vec.as_slice().try_into().unwrap());
    debug!("\tlen: {}", len);

    // Now we dereference the pointer, and read len bytes.
    let bytes: Vec<_> = memory.view()[ptr as usize..(ptr + len) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();

    // And finally turn it into a String.
    str::from_utf8(&bytes).unwrap().to_owned()
}
