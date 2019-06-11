use std::path::PathBuf;

use log::debug;

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

pub(crate) trait UfsMessageHandler: Send {
    fn file_create(&mut self, _path: &PathBuf) {
        debug!("`runtime`: file_create {:?}", _path);
    }
    fn file_remove(&mut self, _path: &PathBuf) {}
    fn file_open(&mut self, _path: &PathBuf) {
        debug!("`runtime`: file_open {:?}", _path);
    }
    fn file_close(&mut self, _path: &PathBuf) {}
    fn file_read(&mut self, _path: &PathBuf, _data: &[u8]) {}
    fn file_write(&mut self, _path: &PathBuf, _data: &[u8]) {}
    fn dir_create(&mut self, _path: &PathBuf) {}
    fn dir_remove(&mut self, _path: &PathBuf) {}
    fn shutdown(&mut self) {}

    fn dispatch_message(&mut self, message: UfsMessage) {
        match message {
            UfsMessage::FileCreate(p) => self.file_create(&p),
            UfsMessage::FileRemove(p) => self.file_remove(&p),
            UfsMessage::FileOpen(p) => self.file_open(&p),
            UfsMessage::FileClose(p) => self.file_close(&p),
            UfsMessage::FileRead(p, d) => self.file_read(&p, d.as_slice()),
            UfsMessage::FileWrite(p, d) => self.file_write(&p, d.as_slice()),
            UfsMessage::DirCreate(p) => self.dir_create(&p),
            UfsMessage::DirRemove(p) => self.dir_remove(&p),
            UfsMessage::Shutdown => self.shutdown(),
        }
    }
}
