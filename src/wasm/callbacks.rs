use {
    crate::{block::BlockStorage, wasm::WasmContext},
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
    wc.handle_message(message);
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
        3 => WasmMessage::FileChanged,
        4 => WasmMessage::FileWritten,
        5 => WasmMessage::FileRead,
        6 => WasmMessage::FileDeleted,
        7 => WasmMessage::NewDir,
        8 => WasmMessage::DirChanged,
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
