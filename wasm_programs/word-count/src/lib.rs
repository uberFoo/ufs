use std::{collections::HashMap, path::PathBuf};

use lazy_static::lazy_static;
use mut_static::MutStatic;
use ufs::*;
lazy_static! {
    pub static ref PROGRAM: MutStatic<WordCounter> = { MutStatic::from(WordCounter::new()) };
}

pub struct WordCounter {
    handles: HashMap<String, FileHandle>,
    word_hash: HashMap<String, usize>,
}

impl WordCounter {
    fn new() -> Self {
        WordCounter {
            handles: HashMap::new(),
            word_hash: HashMap::new(),
        }
    }
}

#[no_mangle]
pub extern "C" fn handle_file_create(path: &str) {
    print(&format!("handle_file_create: {}", path));
    let mut wc = PROGRAM.write().unwrap();

    let path = path.to_string();

    if !wc.handles.contains_key(&path) {
        let file_path = PathBuf::from(path);
        let mut file_name = file_path.file_name().unwrap();

        // Create a directory to hold our word count files.
        let mut dir = file_path.parent().unwrap().to_path_buf();
        dir.push("words");
        create_dir(dir.to_str().unwrap());

        // Create a file to hold our word counts.  Being sure to add it to our handles hash so that
        // we don't create a file to count the words in the file we created to count the words...
        // That is, we won't entre this code section when we're notified about the file we just
        // created.
        // file_name = file_name.with_extension("word_count");
        dir.push(file_name);
        let dir = dir.with_extension("word_count");
        let dir_str = dir.to_str().unwrap();
        // let words_path = file_path.with_extension("words");
        // if let Some(h) = create_file(words_path.to_str().unwrap()) {
        if let Some(h) = create_file(dir_str) {
            // print(&format!("created file {:?}", words_path));
            print(&format!("created file {:?}", dir));
            wc.handles.insert(dir_str.to_string(), h);
            wc.word_hash
                .insert(file_path.to_str().unwrap().to_string(), 0);
        }
    }
}

#[no_mangle]
pub extern "C" fn handle_file_remove(path: &str) {
    print(&format!("handle_file_remove: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_file_open(path: &str) {
    print(&format!("handle_file_open: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_file_close(path: &str) {
    print(&format!("handle_file_close: {}", path));
    let mut wc = PROGRAM.write().unwrap();

    // let words_path = PathBuf::from(path).with_extension("words");
    let file_path = PathBuf::from(path);
    let mut dir = file_path.parent().unwrap().to_path_buf();
    let file_name = file_path.file_name().unwrap();
    dir.push("words");
    dir.push(file_name);
    let words_path = dir.with_extension("word_count");

    // Remove the file from the handles hash, and if it's the one we created, remove it from the
    // word counting hash as well.
    if let Some(_) = wc.handles.remove(&path.to_string()) {
        wc.word_hash
            .remove(&words_path.to_str().unwrap().to_string());
    }

    // Grab the file handle for the file we want to writ.
    if let Some(h) = wc.handles.get(&words_path.to_str().unwrap().to_string()) {
        // Get the word counts, and format the file's contents.
        if let Some(words) = wc.word_hash.get(&path.to_string()) {
            let mut contents = words.to_string();
            contents.push('\t');
            contents.push_str(words_path.to_str().unwrap());
            contents.push('\n');

            print(&format!("writing {:?}", contents));
            write_file(*h, contents.as_bytes());
            print(&format!("wrote {:?}", words_path));
            close_file(*h);
        }
    }
}

#[no_mangle]
pub extern "C" fn handle_file_read(path: &str, data: &[u8]) {
    print(&format!("handle_file_read: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_file_write(path: &str, data: &[u8]) {
    print(&format!("handle_file_write: {}", path));
    let mut wc = PROGRAM.write().unwrap();

    if let Some(words) = wc.word_hash.get_mut(&path.to_string()) {
        let count = String::from_utf8_lossy(data)
            .split_whitespace()
            .fold(0, |n, _| n + 1);

        *words = count;
    }
}

#[no_mangle]
pub extern "C" fn handle_dir_create(path: &str) {
    print(&format!("handle_dir_create: {}", path));
}

#[no_mangle]
pub extern "C" fn handle_dir_remove(path: &str) {
    print(&format!("handle_dir_remove: {}", path));
}
