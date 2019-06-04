//! WASM Runtime
//!
//! Support for running WASM code *inside* the file system.
//!
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    thread::{spawn, JoinHandle},
};

use crossbeam::crossbeam_channel;

use failure;
use log::{debug, info, trace, warn};

// mod imports;

use crate::{
    block::{storage::BlockStorage, BlockNumber},
    metadata::{File, FileHandle},
    UberFileSystem,
};

#[derive(Clone, Debug)]
pub(crate) enum UfsMessage {
    FileCreate(PathBuf),
    FileRemove(PathBuf),
    FileOpen(PathBuf),
    FileClose(PathBuf),
    FileRead(Vec<u8>),
    FileWrite(Vec<u8>),
    DirCreate(PathBuf),
    DirRemove(PathBuf),
    Shutdown,
}

pub(crate) fn init_runtime() -> Result<Vec<Process>, failure::Error> {
    Ok(vec![Process::new()])
}

pub(crate) struct Process {
    sender: crossbeam_channel::Sender<UfsMessage>,
    receiver: crossbeam_channel::Receiver<UfsMessage>,
}

impl Process {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<UfsMessage>();
        Process { sender, receiver }
    }

    pub(crate) fn start<B: BlockStorage>(
        &mut self,
        ufs: Arc<Mutex<UberFileSystem<B>>>,
    ) -> JoinHandle<Result<(), failure::Error>> {
        debug!("-------");
        debug!("`start`");
        let receiver = self.receiver.clone();

        spawn(move || {
            let mut handle = None;
            for message in receiver {
                match message {
                    UfsMessage::Shutdown => break,
                    UfsMessage::FileCreate(path) => info!("`runtime`: FileCreate {:?}", path),
                    UfsMessage::FileRemove(path) => info!("`runtime`: FileRemove {:?}", path),
                    UfsMessage::FileOpen(path) => {
                        info!("`runtime`: FileOpen {:?}", path);
                        let mut guard = ufs.lock().expect("poisoned ufs lock");
                        if let Some((h, _)) = guard.create_file("uberfoo") {
                            handle = Some(h);
                        }
                    }
                    UfsMessage::FileClose(path) => {
                        info!("`runtime`: FileClose {:?}", path);
                        let mut guard = ufs.lock().expect("poisoned ufs lock");
                        if let Some(h) = handle {
                            guard.close_file(h);
                            handle = None;
                        }
                    }
                    UfsMessage::FileRead(bytes) => {
                        info!("`runtime`: FileRead");
                        trace!("\n{}", String::from_utf8_lossy(bytes.as_slice()));
                    }
                    UfsMessage::FileWrite(bytes) => {
                        info!("`runtime`: FileWrite");
                        trace!("{}", String::from_utf8_lossy(bytes.as_slice()));
                    }
                    _ => (),
                }
            }
            Ok(())
        })
    }

    pub(crate) fn send_message(&self, msg: UfsMessage) {
        self.sender.send(msg).unwrap()
    }
}
