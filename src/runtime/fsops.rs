use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use ::time::Timespec;

use crate::{
    block::BlockStorage,
    metadata::{FileHandle, FileSize},
    OpenFileMode, UberFileSystem,
};

pub(crate) trait FileSystemOps: Send {
    fn list_files(&self, path: &Path) -> Vec<(String, FileSize, Timespec)>;
    fn create_file(&mut self, path: &Path) -> Option<(FileHandle, Timespec)>;
    fn open_file(&mut self, path: &Path, mode: OpenFileMode) -> Option<FileHandle>;
    fn close_file(&mut self, handle: FileHandle);
    fn write_file(&mut self, handle: FileHandle, bytes: &[u8]) -> Result<usize, failure::Error>;
    fn read_file(
        &mut self,
        handle: FileHandle,
        offset: i64,
        size: usize,
    ) -> Result<Vec<u8>, failure::Error>;
}

pub(crate) struct FileSystemOperator<B: BlockStorage + 'static> {
    inner: Arc<Mutex<UberFileSystem<B>>>,
}

impl<B: BlockStorage> FileSystemOperator<B> {
    pub(crate) fn new(ufs: Arc<Mutex<UberFileSystem<B>>>) -> Self {
        FileSystemOperator { inner: ufs }
    }
}

impl<B: BlockStorage> FileSystemOps for FileSystemOperator<B> {
    fn list_files(&self, path: &Path) -> Vec<(String, FileSize, Timespec)> {
        let guard = self.inner.lock().expect("poisoned ufs lock");
        guard.list_files(path)
    }

    fn create_file(&mut self, path: &Path) -> Option<(FileHandle, Timespec)> {
        let mut guard = self.inner.lock().expect("poisoned ufs lock");
        guard.create_file(path)
    }

    fn open_file(&mut self, path: &Path, mode: OpenFileMode) -> Option<FileHandle> {
        let mut guard = self.inner.lock().expect("poisoned ufs lock");
        guard.open_file(path, mode)
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
        let mut guard = self.inner.lock().expect("poisoned ufs lock");
        guard.read_file(handle, offset, size)
    }
}
