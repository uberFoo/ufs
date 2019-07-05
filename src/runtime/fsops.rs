use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

use failure::format_err;

use crate::{
    block::BlockStorage,
    metadata::{DirectoryEntry, File, FileHandle},
    OpenFileMode, UberFileSystem,
};

pub trait FileSystemOps: Send {
    fn list_files(&self, handle: FileHandle) -> HashMap<String, DirectoryEntry>;
    fn create_file(&mut self, path: &Path) -> Result<(FileHandle, File), failure::Error>;
    fn open_file(&mut self, path: &Path, mode: OpenFileMode) -> Result<FileHandle, failure::Error>;
    fn close_file(&mut self, handle: FileHandle);
    fn write_file(&mut self, handle: FileHandle, bytes: &[u8]) -> Result<usize, failure::Error>;
    fn read_file(
        &mut self,
        handle: FileHandle,
        offset: i64,
        size: usize,
    ) -> Result<Vec<u8>, failure::Error>;
}

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

    fn write_file(&mut self, handle: FileHandle, bytes: &[u8]) -> Result<usize, failure::Error> {
        let mut guard = self.inner.lock().expect("poisoned ufs lock");
        guard.write_file(handle, bytes)
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
}
