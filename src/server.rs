//! Embedded UFS Block Server
//!
//! A mounted UFS may also act as a block server for remote connections. That is implemented herein.
//!
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{spawn, JoinHandle};

use failure::format_err;
use log::{debug, error, info};

use crate::fsops::FileSystemOps;
use crate::metadata::{DirectoryEntry, File, FileHandle};
use crate::{BlockStorage, OpenFileMode, UberFileSystem};

pub(crate) enum UfsRemoteServerMessage {
    ListFiles,
    CreateFile(PathBuf),
    OpenFile(PathBuf),
    Shutdown,
}

pub(crate) struct UfsRemoteServer<B: BlockStorage + 'static> {
    ufs: Arc<Mutex<UberFileSystem<B>>>,
    listener: TcpListener,
}

impl<B: BlockStorage> UfsRemoteServer<B> {
    pub(crate) fn new(
        ufs: Arc<Mutex<UberFileSystem<B>>>,
        port: u16,
    ) -> Result<(Self), failure::Error> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
        let listener = TcpListener::bind(addr)?;
        info!("Lisnening for remote ufs connections on {:?}", addr);
        Ok(UfsRemoteServer { ufs, listener })
    }

    pub(crate) fn start(mut server: UfsRemoteServer<B>) -> JoinHandle<Result<(), failure::Error>> {
        spawn(move || {
            for stream in server.listener.incoming() {
                let stream = stream?;
                info!("Got a connection from {:?}", stream.peer_addr().unwrap());
            }
            info!("Shutting down UfsRemoteServer");
            Ok(())
        })
    }
}

impl<B: BlockStorage> FileSystemOps for UfsRemoteServer<B> {
    fn list_files(&self, handle: FileHandle) -> HashMap<String, DirectoryEntry> {
        let guard = self.ufs.lock().expect("poisoned ufs lock");
        guard.list_files(handle).unwrap().clone()
    }

    fn create_file(&mut self, path: &Path) -> Result<(FileHandle, File), failure::Error> {
        let mut guard = self.ufs.lock().expect("poisoned ufs lock");
        let metadata = guard.block_manager().metadata();
        let dir = path.parent().unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();
        match metadata.id_from_path(dir) {
            Some(dir_id) => guard.create_file(dir_id, name),
            None => Err(format_err!("unable to find directory {:?}", dir)),
        }
    }

    fn open_file(&mut self, path: &Path, mode: OpenFileMode) -> Result<FileHandle, failure::Error> {
        let mut guard = self.ufs.lock().expect("poisoned ufs lock");
        let metadata = guard.block_manager().metadata();
        match metadata.id_from_path(path) {
            Some(id) => guard.open_file(id, mode),
            None => Err(format_err!("unable to find file for path: {:?}", path)),
        }
    }

    fn close_file(&mut self, handle: FileHandle) {
        let mut guard = self.ufs.lock().expect("poisoned ufs lock");
        guard.close_file(handle)
    }

    fn read_file(
        &mut self,
        handle: FileHandle,
        offset: i64,
        size: usize,
    ) -> Result<Vec<u8>, failure::Error> {
        let guard = self.ufs.lock().expect("poisoned ufs lock");
        guard.read_file(handle, offset, size)
    }

    fn write_file(&mut self, handle: FileHandle, bytes: &[u8]) -> Result<usize, failure::Error> {
        let mut guard = self.ufs.lock().expect("poisoned ufs lock");
        guard.write_file(handle, bytes)
    }

    fn create_dir(&mut self, path: &Path) -> Result<(), failure::Error> {
        let mut guard = self.ufs.lock().expect("poisoned ufs lock");
        let metadata = guard.block_manager().metadata();
        let dir = path.parent().unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();
        match metadata.id_from_path(dir) {
            Some(dir_id) => {
                guard.create_directory(dir_id, name);
                Ok(())
            }
            None => Err(format_err!("unable to find directory {:?}", dir)),
        }
    }
}