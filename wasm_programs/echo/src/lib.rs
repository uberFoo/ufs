use std::path::PathBuf;

use lazy_static::lazy_static;
use mut_static::MutStatic;
use ufs::*;

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
pub extern "C" fn handle_file_create(path: &str) {
    print(&format!("handle_file_create: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_file_remove(path: &str) {
    print(&format!("handle_file_remove: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_file_open(path: &str) {
    print(&format!("handle_file_open: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_file_close(path: &str) {
    print(&format!("handle_file_close: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_file_read(path: &str, data: &[u8]) {
    print(&format!("handle_file_read: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_file_write(path: &str, data: &[u8]) {
    print(&format!("handle_file_write: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_dir_create(path: &str) {
    print(&format!("handle_dir_create: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_dir_remove(path: &str) {
    print(&format!("handle_dir_remove: {}", path));
}
