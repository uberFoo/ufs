//! Extern WASM function implementations
//!
//! Functions that are declared in the WASM program as `extern` are resolved here.
//!
use {
    crate::{block::BlockStorage, wasm::WasmContext, IOFSError, IOFSErrorKind, OpenFileMode},
    colored::*,
    log::{debug, info},
    std::{cell::Cell, convert::TryInto, str},
    wasm_exports::{RefStr, WasmMessage},
    wasmer_runtime::{memory::MemoryView, Ctx},
};

pub(crate) fn __register_for_callback<B>(ctx: &mut Ctx, message_ptr: u32)
where
    B: BlockStorage,
{
    let wc: &mut WasmContext<B> = unsafe { &mut *(ctx.data as *mut WasmContext<B>) };
    let message = unbox_message(ctx, message_ptr);
    info!("register notification {:?}", message);
    wc.set_handles_message(message);
}

pub(crate) fn pong(_ctx: &mut Ctx) {
    debug!("pong");
}

pub(crate) fn __print<B>(ctx: &mut Ctx, str_ptr: u32)
where
    B: BlockStorage,
{
    let wc: &mut WasmContext<B> = unsafe { &mut *(ctx.data as *mut WasmContext<B>) };
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
    B: BlockStorage,
{
    debug!("--------");
    debug!("__open_file: id_ptr: {}", id_ptr);

    let wc: &mut WasmContext<B> = unsafe { &mut *(ctx.data as *mut WasmContext<B>) };
    let id = unbox_str(ctx, id_ptr);
    let guard = wc.iofs.clone();
    let mut guard = guard.lock().expect("poisoned iofs lock");

    // Disable notification for this open
    let has_handler = wc.does_handle_message(WasmMessage::FileOpen);

    if has_handler {
        wc.unset_handles_message(WasmMessage::FileOpen)
    }

    let file = guard.open_file(id.into(), OpenFileMode::Read);

    if has_handler {
        wc.set_handles_message(WasmMessage::FileOpen)
    }

    match file {
        Ok(handle) => handle,
        Err(e) => 0,
    }
}

pub(crate) fn __close_file<B>(ctx: &mut Ctx, handle: u64)
where
    B: BlockStorage,
{
    debug!("--------");
    debug!("__close_file: handle: {}", handle);

    let wc: &mut WasmContext<B> = unsafe { &mut *(ctx.data as *mut WasmContext<B>) };
    let guard = wc.iofs.clone();
    let mut guard = guard.lock().expect("poisoned iofs lock");

    // Disable notification for this message
    let has_handler = wc.does_handle_message(WasmMessage::FileClose);

    if has_handler {
        wc.unset_handles_message(WasmMessage::FileClose)
    }

    let file = guard.close_file(handle);

    if has_handler {
        wc.set_handles_message(WasmMessage::FileClose)
    }
}

pub(crate) fn __read_file<B>(
    ctx: &mut Ctx,
    handle: u64,
    offset: u32,
    data_ptr: u32,
    data_len: u32,
) -> u32
where
    B: BlockStorage,
{
    debug!("--------");
    debug!(
        "__read_file: handle: {}, data_ptr: {}, data_len: {}",
        handle, data_ptr, data_len
    );

    let wc: &mut WasmContext<B> = unsafe { &mut *(ctx.data as *mut WasmContext<B>) };
    let guard = wc.iofs.clone();
    let mut guard = guard.lock().expect("poisoned iofs lock");

    // Disable notification for this open
    let has_handler = wc.does_handle_message(WasmMessage::FileRead);

    if has_handler {
        wc.unset_handles_message(WasmMessage::FileRead)
    }

    let file_size = guard
        .get_file_size(handle)
        .expect("tried to read invalid file handle");
    let read_len = std::cmp::min(data_len as u64, file_size - offset as u64);
    let bytes = guard.read_file(handle, offset as _, read_len as _);

    if has_handler {
        wc.set_handles_message(WasmMessage::FileRead)
    }

    match bytes {
        Ok(bytes) => {
            let mut memory = ctx.memory(0);
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
    handle: u64,
    offset: u32,
    data_ptr: u32,
    data_len: u32,
) -> u32
where
    B: BlockStorage,
{
    debug!("--------");
    debug!(
        "__write_file: handle: {}, data_ptr: {}, data_len: {}",
        handle, data_ptr, data_len
    );

    let wc: &mut WasmContext<B> = unsafe { &mut *(ctx.data as *mut WasmContext<B>) };
    let guard = wc.iofs.clone();
    let mut guard = guard.lock().expect("poisoned iofs lock");

    // Disable notification for this open
    let has_handler = wc.does_handle_message(WasmMessage::FileWrite);

    if has_handler {
        wc.unset_handles_message(WasmMessage::FileWrite)
    }

    let mut memory = ctx.memory(0);
    let bytes: Vec<_> = memory.view()[data_ptr as usize..(data_ptr + data_len) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();

    let bytes_written = guard.write_file(handle, &bytes, offset as _);
    debug!("\twrote {:?} bytes", bytes_written);

    if has_handler {
        wc.set_handles_message(WasmMessage::FileWrite)
    }

    match bytes_written {
        Ok(i) => i as u32,
        Err(_) => 0,
    }
}

pub(crate) fn __create_file<B>(ctx: &mut Ctx, parent_id_ptr: u32, name_ptr: u32) -> i32
where
    B: BlockStorage,
{
    debug!("--------");
    debug!("__create_file: name_ptr: {}", name_ptr);

    let wc: &mut WasmContext<B> = unsafe { &mut *(ctx.data as *mut WasmContext<B>) };
    let parent_id = unbox_str(ctx, parent_id_ptr);
    let name = unbox_str(ctx, name_ptr);
    let guard = wc.iofs.clone();
    let mut guard = guard.lock().expect("poisoned iofs lock");

    // Disable notification for this
    let has_handler = wc.does_handle_message(WasmMessage::FileCreate);

    if has_handler {
        wc.unset_handles_message(WasmMessage::FileCreate)
    }

    let file = guard.create_file(parent_id.into(), &name);

    if has_handler {
        wc.set_handles_message(WasmMessage::FileCreate)
    }

    match file {
        Ok((handle, file)) => {
            debug!(
                "created file {:?}, handle: {}, id: {}",
                name, handle, file.file_id
            );
            let mut memory = ctx.memory(0);
            let ptr = handle.to_le_bytes();
            for (i, cell) in memory.view()[0..ptr.len()].iter().enumerate() {
                cell.set(ptr[i]);
            }

            let file_id_str = &format!("{}", file.file_id);
            for (byte, cell) in file_id_str
                .bytes()
                .zip(memory.view()[ptr.len()..ptr.len() + file_id_str.len()].iter())
            {
                cell.set(byte);
            }
            0
        }
        Err(_) => -1,
    }
}

pub(crate) fn __create_directory<B>(ctx: &mut Ctx, parent_id_ptr: u32, name_ptr: u32) -> i32
where
    B: BlockStorage,
{
    debug!("--------");
    debug!(
        "__create_directory: parent_id_ptr: {}, name_ptr: {}",
        parent_id_ptr, name_ptr
    );

    let wc: &mut WasmContext<B> = unsafe { &mut *(ctx.data as *mut WasmContext<B>) };
    let parent_id = unbox_str(ctx, parent_id_ptr);
    let name = unbox_str(ctx, name_ptr);
    let guard = wc.iofs.clone();
    let mut guard = guard.lock().expect("poisoned iofs lock");

    // Disable notification for this
    let has_handler = wc.does_handle_message(WasmMessage::DirCreate);

    if has_handler {
        wc.unset_handles_message(WasmMessage::DirCreate)
    }

    let dir = guard.create_directory(parent_id.into(), &name);

    if has_handler {
        wc.set_handles_message(WasmMessage::DirCreate)
    }

    match dir {
        Ok(dir) => {
            debug!("created directory {:?} with id {}", name, dir.id());
            let mut memory = ctx.memory(0);
            let dir_id_str = &format!("{}", dir.id());
            for (byte, cell) in dir_id_str
                .bytes()
                .zip(memory.view()[0..dir_id_str.len()].iter())
            {
                cell.set(byte);
            }
            0
        }
        Err(_) => -1,
    }
}

pub(crate) fn __open_directory<B>(ctx: &mut Ctx, parent_id_ptr: u32, name_ptr: u32) -> i32
where
    B: BlockStorage,
{
    debug!("--------");
    debug!(
        "__open_directory: parent_id_ptr: {}, name_ptr: {}",
        parent_id_ptr, name_ptr
    );

    let wc: &mut WasmContext<B> = unsafe { &mut *(ctx.data as *mut WasmContext<B>) };
    let parent_id = unbox_str(ctx, parent_id_ptr);
    let name = unbox_str(ctx, name_ptr);
    let guard = wc.iofs.clone();
    let mut guard = guard.lock().expect("poisoned iofs lock");

    let dir = guard.open_sub_directory(parent_id.into(), &name);

    match dir {
        Ok(dir) => {
            debug!("found directory {:?} with id {}", name, dir);
            let mut memory = ctx.memory(0);
            let dir_id_str = &format!("{}", dir);
            for (byte, cell) in dir_id_str
                .bytes()
                .zip(memory.view()[0..dir_id_str.len()].iter())
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
    let memory = ctx.memory(0);

    // The string is stored as a u32 pointer, followed by a length. We first extract the pointer
    // from memory.
    let ptr_vec: Vec<_> = memory.view()[str_ptr as usize..(str_ptr + 4) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();
    let ptr = u32::from_le_bytes(ptr_vec.as_slice().try_into().unwrap());

    // And then we extract the length.
    let len_vec: Vec<_> = memory.view()[(str_ptr + 4) as usize..(str_ptr + 8) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();
    let len = u32::from_le_bytes(len_vec.as_slice().try_into().unwrap());

    // Now we dereference the pointer, and read len bytes.
    let bytes: Vec<_> = memory.view()[ptr as usize..(ptr + len) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();

    // And finally turn it into a String.
    str::from_utf8(&bytes).unwrap().to_string()
}
