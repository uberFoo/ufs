//! Abstraction for File System Operations
//!
//! High level file system operations are abstracted here for use by both WASM programs, and for
//! implementing remote file system connections. The operations are contained in a trait that is
//! implemented in both places.
//!
use std::collections::HashMap;
use std::path::Path;

use crate::metadata::{DirectoryEntry, File, FileHandle};
use crate::OpenFileMode;

pub(crate) trait FileSystemOps: Send {
    fn list_files(&self, handle: FileHandle) -> HashMap<String, DirectoryEntry>;
    fn create_file(&mut self, path: &Path) -> Result<(FileHandle, File), failure::Error>;
    // fn remove_file(&mut self, path: &Path);
    fn open_file(&mut self, path: &Path, mode: OpenFileMode) -> Result<FileHandle, failure::Error>;
    fn close_file(&mut self, handle: FileHandle);
    fn read_file(
        &mut self,
        handle: FileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, failure::Error>;
    fn write_file(&mut self, handle: FileHandle, bytes: &[u8]) -> Result<usize, failure::Error>;
    fn create_dir(&mut self, path: &Path) -> Result<(), failure::Error>;
    // fn remove_dir(&mut self, path: &Path);
}
