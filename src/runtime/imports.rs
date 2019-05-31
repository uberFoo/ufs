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
const LIST_FILES_NAME: &'static str = "list";
const LIST_FILES_INDEX: usize = 0;
/// Open a file
const OPEN_FILE_NAME: &'static str = "open";
const OPEN_FILE_INDEX: usize = 1;
/// Read from a file
const READ_FILE_NAME: &'static str = "read";
const READ_FILE_INDEX: usize = 2;
/// Write to a file
const WRITE_FILE_NAME: &'static str = "write";
const WRITE_FILE_INDEX: usize = 3;

pub struct RuntimeModuleImportResolver;

impl RuntimeModuleImportResolver {
    pub fn new() -> RuntimeModuleImportResolver {
        RuntimeModuleImportResolver {}
    }
}

impl<'a> ModuleImportResolver for RuntimeModuleImportResolver {
    fn resolve_func(
        &self,
        field_name: &str,
        _signature: &Signature,
    ) -> Result<FuncRef, InterpreterError> {
        let func_ref = match field_name {
            LIST_FILES_NAME => {
                FuncInstance::alloc_host(Signature::new(&[][..], None), LIST_FILES_INDEX)
            }
            OPEN_FILE_NAME => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                OPEN_FILE_INDEX,
            ),
            _ => {
                return Err(InterpreterError::Function(format!(
                    "no function exported with name {}",
                    field_name
                )))
            }
        };
        Ok(func_ref)
    }
}
