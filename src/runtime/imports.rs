//! WASM module import resolution
//!
use wasmi::{
    Error as InterpreterError, FuncInstance, FuncRef, ModuleImportResolver, Signature, ValueType,
};

/// Exported Functionality
///
/// These are the function that we expose to WASM modules.  There needs to be a permissions system
/// built atop these.
///
/// List files in a directory
pub const LIST_FILES_INDEX: usize = 0;
/// Open a file
pub const OPEN_FILE_INDEX: usize = 1;
/// Read from a file
pub const READ_FILE_INDEX: usize = 2;
/// Write to a file
pub const WRITE_FILE_INDEX: usize = 3;
