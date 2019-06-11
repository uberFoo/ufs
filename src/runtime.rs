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
pub(crate) mod fsops;
pub(crate) mod message;

pub(crate) use self::fsops::FileSystemOperator;
pub(crate) use self::message::UfsMessage;

use self::{fsops::FileSystemOps, message::UfsMessageHandler};

use crate::metadata::{File, FileHandle};

pub(crate) fn init_runtime(
    ufs: Box<dyn FileSystemOps + Send>,
) -> Result<Vec<Process>, failure::Error> {
    let wc = Box::new(WordCounter::new(ufs));
    Ok(vec![Process::new(wc)])
}

pub(crate) struct Process {
    sender: crossbeam_channel::Sender<UfsMessage>,
    receiver: crossbeam_channel::Receiver<UfsMessage>,
    handler: Box<dyn UfsMessageHandler>,
}

impl Process {
    pub(crate) fn new(handler: Box<dyn UfsMessageHandler>) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<UfsMessage>();
        Process {
            sender,
            receiver,
            handler,
        }
    }

    pub(crate) fn start(mut p: Process) -> JoinHandle<Result<(), failure::Error>> {
        debug!("-------");
        debug!("`start`");
        spawn(move || {
            loop {
                let message = p.receiver.recv().unwrap();
                p.handler.dispatch_message((message.clone()));
                if let UfsMessage::Shutdown = message {
                    break;
                }
            }

            Ok(())
        })
    }

    pub(crate) fn get_sender(&self) -> crossbeam_channel::Sender<UfsMessage> {
        self.sender.clone()
    }
}

struct WordCounter {
    fs_ops: Box<dyn FileSystemOps>,
    handles: HashMap<String, FileHandle>,
    word_hash: HashMap<String, usize>,
}

impl WordCounter {
    fn new(fs_ops: Box<dyn FileSystemOps>) -> Self {
        WordCounter {
            fs_ops,
            handles: HashMap::new(),
            word_hash: HashMap::new(),
        }
    }
}

impl UfsMessageHandler for WordCounter {
    fn file_create(&mut self, path: &PathBuf) {
        let path = path.to_str().unwrap().to_string();

        if !self.handles.contains_key(&path) {
            let mut file_path = PathBuf::new();
            file_path.push("/");
            file_path.push(path);

            let words_path = file_path.with_extension("words");
            if let Some((h, _)) = self.fs_ops.create_file(words_path.as_ref()) {
                info!("created file {:?} with handle {}", words_path, h);
                self.handles
                    .insert(words_path.to_str().unwrap().to_string(), h);
                self.word_hash
                    .insert(file_path.to_str().unwrap().to_string(), 0);
            }
        }
    }

    fn file_close(&mut self, path: &PathBuf) {
        let words_path = path.with_extension("words");

        if let Some(h) = self.handles.remove(&path.to_str().unwrap().to_string()) {
            info!("removing words from hash");
            self.word_hash
                .remove(&words_path.to_str().unwrap().to_string());
        }

        if let Some(h) = self
            .handles
            .get_mut(&words_path.to_str().unwrap().to_string())
        {
            if let Some(words) = self.word_hash.get_mut(&path.to_str().unwrap().to_string()) {
                let mut contents = words.to_string();
                contents.push('\t');
                contents.push_str(words_path.to_str().unwrap());
                contents.push('\n');

                info!("writing {} to {}", contents, *h);
                self.fs_ops.write_file(*h, contents.as_bytes());
                self.fs_ops.close_file(*h);
            }
            self.fs_ops.close_file(*h);
        }
    }

    fn file_write(&mut self, path: &PathBuf, data: &[u8]) {
        if let Some(words) = self.word_hash.get_mut(&path.to_str().unwrap().to_string()) {
            let count = String::from_utf8_lossy(data)
                .split_whitespace()
                .fold(0, |n, _| n + 1);

            info!("counted {} words in file {:?}", count, path);
            *words = count;
        }
    }
}
