//! Handling WASM calls to Rust
//!
//! This is the Rust implementation of the WASM callable file system operations. The wasmi runtime
//! maintains a list of functions that are exported to WASM (see exports.rs). Those exported
//! functions are invoked by WASM code, and handled on the Rust side by the code in wasm.rs. The
//! code in wasm.rs invokes these functions, which ultimately invoke methods in the file system.
//!
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

use failure::format_err;

use crate::{
    block::BlockStorage,
    fsops::FileSystemOps,
    metadata::{DirectoryEntry, File, FileHandle},
    OpenFileMode, UberFileSystem,
};

pub(crate) struct FileSystemOperator<B: BlockStorage> {
    inner: Arc<Mutex<UberFileSystem<B>>>,
}

impl<'a, B: BlockStorage> FileSystemOperator<B> {
    pub(crate) fn new(ufs: Arc<Mutex<UberFileSystem<B>>>) -> Self {
        FileSystemOperator { inner: ufs }
    }
}

impl<'a, B: BlockStorage> FileSystemOps for FileSystemOperator<B> {
    fn list_files(&self, handle: FileHandle) -> HashMap<String, DirectoryEntry> {
        let guard = self.inner.lock().expect("poisoned ufs lock");
        guard.list_files(handle).unwrap().clone()
    }

    fn create_file(&mut self, path: &Path) -> Result<(FileHandle, File), failure::Error> {
        let mut guard = self.inner.lock().expect("poisoned ufs lock");
        let metadata = guard.block_manager().metadata();
        let dir = path.parent().unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();
        match metadata.id_from_path(dir) {
            Some(dir_id) => guard.create_file(dir_id, name),
            None => Err(format_err!("unable to find directory {:?}", dir)),
        }
    }

    fn open_file(&mut self, path: &Path, mode: OpenFileMode) -> Result<FileHandle, failure::Error> {
        let mut guard = self.inner.lock().expect("poisoned ufs lock");
        let metadata = guard.block_manager().metadata();
        match metadata.id_from_path(path) {
            Some(id) => guard.open_file(id, mode),
            None => Err(format_err!("unable to find file for path: {:?}", path)),
        }
    }

    fn close_file(&mut self, handle: FileHandle) {
        let mut guard = self.inner.lock().expect("poisoned ufs lock");
        guard.close_file(handle)
    }

    fn read_file(
        &mut self,
        handle: FileHandle,
        offset: i64,
        size: usize,
    ) -> Result<Vec<u8>, failure::Error> {
        let guard = self.inner.lock().expect("poisoned ufs lock");
        guard.read_file(handle, offset, size)
    }

    fn write_file(&mut self, handle: FileHandle, bytes: &[u8]) -> Result<usize, failure::Error> {
        let mut guard = self.inner.lock().expect("poisoned ufs lock");
        guard.write_file(handle, bytes)
    }

    fn create_dir(&mut self, path: &Path) -> Result<(), failure::Error> {
        let mut guard = self.inner.lock().expect("poisoned ufs lock");
        let metadata = guard.block_manager().metadata();
        let dir = path.parent().unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();
        match metadata.id_from_path(dir) {
            Some(dir_id) => {
                guard.create_directory(dir_id, name)?;
                Ok(())
            }
            None => Err(format_err!("unable to find directory {:?}", dir)),
        }
    }
}
