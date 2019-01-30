#![warn(missing_docs)]
//! Another file system: UberFS
//!

mod block;

pub use crate::block::{FileStore, MemoryStore};
