use std::path::PathBuf;

#[derive(Clone, Debug)]
pub enum UfsMessage {
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

pub trait UfsMessageHandler {
    fn file_create(&mut self, _path: &str) {}
    fn file_remove(&mut self, _path: &str) {}
    fn file_open(&mut self, _path: &str) {}
    fn file_close(&mut self, _path: &str) {}
    fn file_read(&mut self, _path: &str, _data: &[u8]) {}
    fn file_write(&mut self, _path: &str, _data: &[u8]) {}
    fn dir_create(&mut self, _path: &str) {}
    fn dir_remove(&mut self, _path: &str) {}
    fn shutdown(&mut self) {}
}
