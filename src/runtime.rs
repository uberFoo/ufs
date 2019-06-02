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
use log::{debug, info, warn};

// mod imports;

use crate::{
    block::BlockNumber,
    metadata::{File, FileHandle},
};

#[derive(Clone, Debug)]
pub(crate) enum UFSMessage {
    FileCreate(FileHandle),
    FileRemove(FileHandle),
    FileOpen(FileHandle),
    FileClose(FileHandle),
    FileRead((BlockNumber, usize, usize)),
    FileWrite(BlockNumber),
    DirCreate(PathBuf),
    DirRemove(PathBuf),
}

pub(crate) fn init_runtime(

) -> Result<Vec<Process>, failure::Error> {
    Ok(vec![Process::new()])
}

pub(crate) struct Process {
    handle: Option<JoinHandle<Result<(), failure::Error>>>,
    sender: crossbeam_channel::Sender<UFSMessage>,
    receiver: crossbeam_channel::Receiver<UFSMessage>,
}

impl Process {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<UFSMessage>();
        Process {
            handle: None,
            sender,
            receiver,
        }
    }

    pub(crate) fn handle(&self) -> Option<&JoinHandle<Result<(), failure::Error>>> {
        self.handle.as_ref()
    }

    pub(crate) fn start(&mut self, file_map: Arc<RwLock<HashMap<FileHandle, File>>>) {
        debug!("-------");
        debug!("`start`");
        let receiver = self.receiver.clone();

        let handle = spawn(move || {
            for message in receiver {
                info!("`runtime`: {:?}", message);
                match message {
                    UFSMessage::FileCreate(h)
                    | UFSMessage::FileRemove(h)
                    | UFSMessage::FileOpen(h)
                    | UFSMessage::FileClose(h) => {
                        let map = file_map.read().expect("poisoned RwLock");
                        match map.get(&h) {
                            Some(file) =>
                        info!("file path {:?}", file.path),
                        None => warn!("expected to find a file for handle {}", h)
                        }
                    }
                    _ => (),
                }
            }
            Ok(())
        });

        self.handle = Some(handle);
    }

    pub(crate) fn send_message(&self, msg: UFSMessage) {
        self.sender.send(msg).unwrap()
    }
}
