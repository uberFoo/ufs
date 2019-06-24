//! WASM Runtime
//!
//! Support for running WASM code *inside* the file system.
//!
use std::{
    fs,
    io::Read,
    thread::{spawn, JoinHandle},
};

use crossbeam::crossbeam_channel;
use failure;
use log::{debug, info};
use wasmi::{ImportsBuilder, ModuleInstance};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod fsops;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod message;
pub mod wasm;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use self::{
    fsops::FileSystemOperator, wasm::exports::RuntimeModuleImportResolver,
    wasm::handler::WasmMessageHandler,
};

#[cfg(not(target_arch = "wasm32"))]
pub use self::fsops::FileSystemOps;
#[cfg(not(target_arch = "wasm32"))]
pub use self::message::{UfsMessage, UfsMessageHandler};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn init_runtime() -> Result<Vec<Process>, failure::Error> {
    Ok(vec![Process::new()])
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct Process {
    sender: crossbeam_channel::Sender<UfsMessage>,
    receiver: crossbeam_channel::Receiver<UfsMessage>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Process {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<UfsMessage>();
        Process { sender, receiver }
    }

    pub(crate) fn start(
        mut p: Process,
        fs_ops: Box<dyn FileSystemOps>,
    ) -> JoinHandle<Result<(), failure::Error>> {
        debug!("-------");
        debug!("`start`");
        spawn(move || {
            let mut buffer = Vec::new();
            let mut f = fs::File::open("wasm_programs/word_count.wasm")?;
            f.read_to_end(&mut buffer)?;
            let module = wasmi::Module::from_buffer(buffer)?;

            let resolver = RuntimeModuleImportResolver::new();
            let mut builder = ImportsBuilder::new();
            builder.push_resolver("env", &resolver);

            let instance = ModuleInstance::new(&module, &builder)
                .expect("failed to instantiate WASM module")
                .assert_no_start();

            let mut handler = WasmMessageHandler::new(instance, fs_ops);

            loop {
                let message = p.receiver.recv().unwrap();
                p.dispatch_message(&mut handler, message.clone());
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

    fn dispatch_message(&mut self, handler: &mut dyn UfsMessageHandler, message: UfsMessage) {
        match message {
            UfsMessage::FileCreate(p) => {
                info!("dispatch FileCreate {:?}", p);
                handler.file_create(p.to_str().unwrap());
                info!("FileCreate complete");
            }
            UfsMessage::FileRemove(p) => {
                info!("dispatch FileRemove {:?}", p);
                handler.file_remove(p.to_str().unwrap());
                info!("FileRemove complete");
            }
            UfsMessage::FileOpen(p) => {
                info!("dispatch FileOpen {:?}", p);
                handler.file_open(p.to_str().unwrap());
                info!("FileOpen complete");
            }
            UfsMessage::FileClose(p) => {
                info!("dispatch FileClose {:?}", p);
                handler.file_close(p.to_str().unwrap());
                info!("FileClose complete");
            }
            UfsMessage::FileRead(p, d) => {
                info!("dispatch FileRead {:?}", p);
                handler.file_read(p.to_str().unwrap(), d.as_slice());
                info!("FileRead complete");
            }
            UfsMessage::FileWrite(p, d) => {
                info!("dispatch FileWrite {:?}", p);
                handler.file_write(p.to_str().unwrap(), d.as_slice());
                info!("FileWrite complete");
            }
            UfsMessage::DirCreate(p) => handler.dir_create(p.to_str().unwrap()),
            UfsMessage::DirRemove(p) => handler.dir_remove(p.to_str().unwrap()),
            UfsMessage::Shutdown => {
                info!("dispatch Shutdown");
                handler.shutdown();
                info!("Shutdown complete");
            }
        }
    }
}

// struct WordCounter {
//     fs_ops: Box<dyn FileSystemOps>,
//     handles: HashMap<String, FileHandle>,
//     word_hash: HashMap<String, usize>,
// }

// impl WordCounter {
//     fn new(fs_ops: Box<dyn FileSystemOps>) -> Self {
//         WordCounter {
//             fs_ops,
//             handles: HashMap::new(),
//             word_hash: HashMap::new(),
//         }
//     }
// }

// impl UfsMessageHandler for WordCounter {
//     fn file_create(&mut self, path: &str) {
//         let path = path.to_string();

//         if !self.handles.contains_key(&path) {
//             let mut file_path = PathBuf::new();
//             file_path.push("/");
//             file_path.push(path);

//             let words_path = file_path.with_extension("words");
//             if let Some((h, _)) = self.fs_ops.create_file(words_path.as_ref()) {
//                 info!("created file {:?} with handle {}", words_path, h);
//                 self.handles
//                     .insert(words_path.to_str().unwrap().to_string(), h);
//                 self.word_hash
//                     .insert(file_path.to_str().unwrap().to_string(), 0);
//             }
//         }
//     }

//     fn file_close(&mut self, path: &str) {
//         let words_path = PathBuf::from(path).with_extension("words");

//         if let Some(h) = self.handles.remove(&path.to_string()) {
//             info!("removing words from hash");
//             self.word_hash
//                 .remove(&words_path.to_str().unwrap().to_string());
//         }

//         if let Some(h) = self
//             .handles
//             .get_mut(&words_path.to_str().unwrap().to_string())
//         {
//             if let Some(words) = self.word_hash.get_mut(&path.to_string()) {
//                 let mut contents = words.to_string();
//                 contents.push('\t');
//                 contents.push_str(words_path.to_str().unwrap());
//                 contents.push('\n');

//                 info!("writing {} to {}", contents, *h);
//                 self.fs_ops.write_file(*h, contents.as_bytes());
//                 self.fs_ops.close_file(*h);
//             }
//             self.fs_ops.close_file(*h);
//         }
//     }

//     fn file_write(&mut self, path: &str, data: &[u8]) {
//         if let Some(words) = self.word_hash.get_mut(&path.to_string()) {
//             let count = String::from_utf8_lossy(data)
//                 .split_whitespace()
//                 .fold(0, |n, _| n + 1);

//             info!("counted {} words in file {:?}", count, path);
//             *words = count;
//         }
//     }
// }
