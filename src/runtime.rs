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
pub(crate) enum UfsMessage {
    FileCreate(PathBuf),
    FileRemove(PathBuf),
    FileOpen(PathBuf),
    FileClose(PathBuf),
    FileRead(Vec<u8>),
    FileWrite(Vec<u8>),
    DirCreate(PathBuf),
    DirRemove(PathBuf),
}

pub(crate) fn init_runtime() -> Result<Vec<Process>, failure::Error> {
    Ok(vec![Process::new()])
}

pub(crate) struct Process {
    handle: Option<JoinHandle<Result<(), failure::Error>>>,
    sender: crossbeam_channel::Sender<UfsMessage>,
    receiver: crossbeam_channel::Receiver<UfsMessage>,
}

impl Process {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<UfsMessage>();
        Process {
            handle: None,
            sender,
            receiver,
        }
    }

    pub(crate) fn handle(&self) -> Option<&JoinHandle<Result<(), failure::Error>>> {
        self.handle.as_ref()
    }

    pub(crate) fn start(&mut self) {
        debug!("-------");
        debug!("`start`");
        let receiver = self.receiver.clone();

        let handle = spawn(move || {
            for message in receiver {
                // info!("`runtime`: {:?}", message);
                match message {
                    UfsMessage::FileCreate(path) => info!("`runtime`: FileCreate {:?}", path),
                    UfsMessage::FileRemove(path) => info!("`runtime`: FileRemove {:?}", path),
                    UfsMessage::FileOpen(path) => info!("`runtime`: FileOpen {:?}", path),
                    UfsMessage::FileClose(path) => info!("`runtime`: FileClose {:?}", path),
                    UfsMessage::FileRead(bytes) => info!(
                        "`runtime`: FileRead\n{}",
                        String::from_utf8_lossy(bytes.as_slice())
                    ),
                    UfsMessage::FileWrite(bytes) => info!(
                        "`runtime`: FileWrite\n{}",
                        String::from_utf8_lossy(bytes.as_slice())
                    ),
                    // {
                    //     let map = file_map.read().expect("poisoned RwLock");
                    //     match map.get(&h) {
                    //         Some(file) =>
                    //     info!("file path {:?}", file.path),
                    //     None => warn!("expected to find a file for handle {}", h)
                    //     }
                    // }
                    _ => (),
                }
            }
            Ok(())
        });

        self.handle = Some(handle);
    }

    pub(crate) fn send_message(&self, msg: UfsMessage) {
        self.sender.send(msg).unwrap()
    }
}
