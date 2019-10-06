//! Extern WASM function implementations
//!
//! Functions that are declared in the WASM program as `extern` are resolved here.
//!
use {
    crate::{block::BlockStorage, wasm::WasmContext, OpenFileMode},
    colored::*,
    log::{debug, info},
    std::{convert::TryInto, str},
    wasm_exports::WasmMessage,
    wasmer_runtime::Ctx,
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
    let has_handler = wc.does_handle_message(WasmMessage::FileOpen);

    if has_handler {
        wc.unset_handles_message(WasmMessage::FileOpen)
    }

    let file_size = guard
        .get_file_size(handle)
        .expect("tried to read invalid file handle");
    let read_len = std::cmp::min(data_len as u64, file_size - offset as u64);
    let bytes = guard.read_file(handle, offset as _, read_len as _);

    if has_handler {
        wc.set_handles_message(WasmMessage::FileOpen)
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
        Err(e) => 0,
    }
}

fn unbox_message(ctx: &Ctx, msg_ptr: u32) -> WasmMessage {
    let memory = ctx.memory(0);

    let ptr_vec: Vec<_> = memory.view()[msg_ptr as usize..(msg_ptr + 4) as usize]
        .iter()
        .map(|cell| cell.get())
        .collect();
    let ptr = u32::from_le_bytes(ptr_vec.as_slice().try_into().unwrap());

    match ptr {
        0 => WasmMessage::Shutdown,
        1 => WasmMessage::Ping,
        2 => WasmMessage::NewFile,
        3 => WasmMessage::NewDir,
        4 => WasmMessage::FileDelete,
        5 => WasmMessage::DirDelete,
        6 => WasmMessage::FileOpen,
        7 => WasmMessage::FileClose,
        8 => WasmMessage::FileWrite,
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
