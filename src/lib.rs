#![warn(missing_docs)]
#![feature(const_fn, crate_visibility_modifier, try_from)]
//! Another file system: UberFS
//!

mod block;

pub use crate::block::{FileStore, MemoryStore};
