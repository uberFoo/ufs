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
    FileRead(PathBuf, Vec<u8>),
    FileWrite(PathBuf, Vec<u8>),
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
            let mut handles = HashMap::<PathBuf, FileHandle>::new();
            let mut word_hash = HashMap::<PathBuf, usize>::new();
            for message in receiver {
                match message {
                    UfsMessage::Shutdown => break,
                    UfsMessage::FileCreate(path) => {
                        info!("`runtime`: FileCreate {:?}", path);

                        if !handles.contains_key(&path) {
                            let mut file_path = PathBuf::new();
                            file_path.push("/");
                            file_path.push(path);

                            let mut guard = ufs.lock().expect("poisoned ufs lock");

                            let words_path = file_path.with_extension("words");
                            if let Some((h, _)) = guard.create_file(words_path.clone()) {
                                info!("created file {:?} with handle {}", words_path, h);
                                handles.insert(words_path, h);
                                let mut word_path = PathBuf::new();
                                word_hash.insert(file_path, 0);
                            }

                            // if let Some((h, _)) = guard.create_file("foo") {
                            //     handles.insert(path, h));
                            // }
                        }
                    }
                    UfsMessage::FileRemove(path) => info!("`runtime`: FileRemove {:?}", path),
                    UfsMessage::FileOpen(path) => {
                        info!("`runtime`: FileOpen {:?}", path);

                        // if !handles.contains_key(&path) {
                        //     let mut guard = ufs.lock().expect("poisoned ufs lock");
                        //     if let Some((h, _)) = guard.create_file("uberfoo") {
                        //         handles.insert(path, (h, 0));
                        //     }
                        // }
                    }
                    UfsMessage::FileClose(path) => {
                        info!("`runtime`: FileClose {:?}", path);

                        let words_path = path.with_extension("words");

                        if let Some(h) = handles.remove(&path) {
                            info!("removing words from hash");
                            word_hash.remove(&words_path);
                        }

                        if let Some(h) = handles.get_mut(&words_path) {
                            let mut guard = ufs.lock().expect("poisoned ufs lock");

                            if let Some(words) = word_hash.get_mut(&path) {
                                let mut contents = words.to_string();
                                contents.push('\t');
                                contents.push_str(words_path.to_str().unwrap());
                                contents.push('\n');

                                info!("writing {} to {}", contents, *h);
                                guard.write_file(*h, contents.as_bytes());
                                guard.close_file(*h);
                            }
                            guard.close_file(*h);
                        }
                    }
                    UfsMessage::FileRead(path, bytes) => {
                        info!("`runtime`: FileRead");
                        trace!("\n{}", String::from_utf8_lossy(bytes.as_slice()));
                    }
                    UfsMessage::FileWrite(path, bytes) => {
                        info!("`runtime`: FileWrite {:?}", path);
                        trace!("{}", String::from_utf8_lossy(bytes.as_slice()));

                        if let Some(words) = word_hash.get_mut(&path) {
                            let count = String::from_utf8_lossy(bytes.as_slice())
                                .split_whitespace()
                                .fold(0, |n, _| n + 1);

                            info!("counted {} words in file {:?}", count, path);
                            *words = count;
                        }
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
