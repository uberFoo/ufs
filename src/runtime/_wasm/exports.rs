//! WASM module import resolution
//!
//! These functions are exported to WASM-land, and invoked by programs running there. They are
//! resolved here, and invoke code defined in runtime.rs.
//!
use wasmi::{
    Error as InterpreterError, FuncInstance, FuncRef, ModuleImportResolver, Signature, ValueType,
};

/// Exported Functionality
///
/// These are the function that we expose to WASM modules.  There needs to be a permissions system
/// built atop these.
///
/// Debug
pub(in crate::runtime) const PRINT_NAME: &'static str = "__rust_print";
pub(in crate::runtime) const PRINT_INDEX: usize = 0;
/// List files in a directory
pub(in crate::runtime) const LIST_FILES_NAME: &'static str = "__rust_list_files";
pub(in crate::runtime) const LIST_FILES_INDEX: usize = 1;
/// Create a file
pub(in crate::runtime) const CREATE_FILE_NAME: &'static str = "__rust_create_file";
pub(in crate::runtime) const CREATE_FILE_INDEX: usize = 2;
/// Remove a file
pub(in crate::runtime) const REMOVE_FILE_NAME: &'static str = "__rust_remove_file";
pub(in crate::runtime) const REMOVE_FILE_INDEX: usize = 3;
/// Open a file
pub(in crate::runtime) const OPEN_FILE_NAME: &'static str = "__rust_open_file";
pub(in crate::runtime) const OPEN_FILE_INDEX: usize = 4;
/// Close from a file
pub(in crate::runtime) const CLOSE_FILE_NAME: &'static str = "__rust_close_file";
pub(in crate::runtime) const CLOSE_FILE_INDEX: usize = 5;
/// Read from a file
pub(in crate::runtime) const READ_FILE_NAME: &'static str = "__rust_read_file";
pub(in crate::runtime) const READ_FILE_INDEX: usize = 6;
/// Write to a file
pub(in crate::runtime) const WRITE_FILE_NAME: &'static str = "__rust_write_file";
pub(in crate::runtime) const WRITE_FILE_INDEX: usize = 7;
/// Create a directory
pub(in crate::runtime) const CREATE_DIR_NAME: &'static str = "__rust_create_dir";
pub(in crate::runtime) const CREATE_DIR_INDEX: usize = 8;
/// Remove a directory
pub(in crate::runtime) const REMOVE_DIR_NAME: &'static str = "__rust_remove_dir";
pub(in crate::runtime) const REMOVE_DIR_INDEX: usize = 9;

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
            PRINT_NAME => {
                FuncInstance::alloc_host(Signature::new(&[ValueType::I32][..], None), PRINT_INDEX)
            }
            // LIST_FILES_NAME => FuncInstance::alloc_host(
            //     Signature::new(&[ValueType::I32][..], None),
            //     LIST_FILES_INDEX,
            // ),
            CREATE_FILE_NAME => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                CREATE_FILE_INDEX,
            ),
            // OPEN_FILE_NAME => FuncInstance::alloc_host(
            //     Signature::new(&[ValueType::I32, ValueType::I32][..], None),
            //     OPEN_FILE_INDEX,
            // ),
            CLOSE_FILE_NAME => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I64][..], None),
                CLOSE_FILE_INDEX,
            ),
            WRITE_FILE_NAME => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I64, ValueType::I32][..], None),
                WRITE_FILE_INDEX,
            ),
            // READ_FILE_NAME => FuncInstance::alloc_host(
            //     Signature::new(&[ValueType::I32, ValueType::I32, ValueType::I32][..], None),
            //     READ_FILE_INDEX,
            // ),
            CREATE_DIR_NAME => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                CREATE_DIR_INDEX,
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
