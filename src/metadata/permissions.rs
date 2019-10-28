//! Wasm Program Permissions
//!
//! The user's Wasm programs operate by receiving events from the file system and the network.
//! Before receiving these events, the user must first grant authorization for each different event.
//! Additionally, there are file system callbacks that Wasm programs may use to modify the file
//! system. These must also be authorized by the user prior to their being allowed.
//!
//! The user is prompted to allow or deny each at the time the event is registered, or the first
//! time the function is invoked.
//!
//! Permissions are stored in the file system metadata.
use {
    crate::metadata,
    serde_derive::{Deserialize, Serialize},
    std::{
        collections::HashMap,
        io::{self, Read, Write},
        path::PathBuf,
    },
};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub(crate) enum Grant {
    Unknown,
    Allow,
    Deny,
}

#[derive(Clone, Copy)]
pub(crate) enum GrantType {
    file_create_event,
    dir_create_event,
    file_delete_event,
    dir_delete_event,
    file_open_event,
    file_close_event,
    file_read_event,
    file_write_event,
    http_get_event,
    http_post_event,
    open_file,
    close_file,
    read_file,
    write_file,
    create_file,
    create_directory,
    open_directory,
}

impl GrantType {
    pub(crate) fn grant_string(&self) -> &'static str {
        match self {
            GrantType::file_create_event => "receive file create events",
            GrantType::dir_create_event => "receive directory create events",
            GrantType::file_delete_event => "receive file delete events",
            GrantType::dir_delete_event => "receive directory delete events",
            GrantType::file_open_event => "receive file open events",
            GrantType::file_close_event => "receive file close events",
            GrantType::file_read_event => "receive file read events",
            GrantType::file_write_event => "receive file write events",
            GrantType::http_get_event => "receive HTTP GET to",
            GrantType::http_post_event => "receive HTTP POST to",
            GrantType::open_file => "open files",
            GrantType::close_file => "close files",
            GrantType::read_file => "read files",
            GrantType::write_file => "write files",
            GrantType::create_file => "create files",
            GrantType::create_directory => "create directories",
            GrantType::open_directory => "open directories",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub(crate) struct ProgramPermissions {
    // File System Events
    file_create_event: Grant,
    dir_create_event: Grant,
    file_delete_event: Grant,
    dir_delete_event: Grant,
    file_open_event: Grant,
    file_close_event: Grant,
    file_read_event: Grant,
    file_write_event: Grant,
    // HTTP Events
    http_get_event: Grant,
    http_post_event: Grant,
    // Synchronous function calls
    open_file: Grant,
    close_file: Grant,
    read_file: Grant,
    write_file: Grant,
    create_file: Grant,
    create_directory: Grant,
    open_directory: Grant,
}

impl ProgramPermissions {
    pub(crate) fn new() -> Self {
        ProgramPermissions {
            file_create_event: Grant::Unknown,
            dir_create_event: Grant::Unknown,
            file_delete_event: Grant::Unknown,
            dir_delete_event: Grant::Unknown,
            file_open_event: Grant::Unknown,
            file_close_event: Grant::Unknown,
            file_read_event: Grant::Unknown,
            file_write_event: Grant::Unknown,
            http_get_event: Grant::Unknown,
            http_post_event: Grant::Unknown,
            open_file: Grant::Unknown,
            close_file: Grant::Unknown,
            read_file: Grant::Unknown,
            write_file: Grant::Unknown,
            create_file: Grant::Unknown,
            create_directory: Grant::Unknown,
            open_directory: Grant::Unknown,
        }
    }

    fn get_grant(&self, grant_type: GrantType) -> Grant {
        match grant_type {
            GrantType::file_create_event => self.file_create_event,
            GrantType::dir_create_event => self.dir_create_event,
            GrantType::file_delete_event => self.file_delete_event,
            GrantType::dir_delete_event => self.dir_delete_event,
            GrantType::file_open_event => self.file_open_event,
            GrantType::file_close_event => self.file_close_event,
            GrantType::file_read_event => self.file_read_event,
            GrantType::file_write_event => self.file_write_event,
            GrantType::http_get_event => self.http_get_event,
            GrantType::http_post_event => self.http_post_event,
            GrantType::open_file => self.open_file,
            GrantType::close_file => self.close_file,
            GrantType::read_file => self.read_file,
            GrantType::write_file => self.write_file,
            GrantType::create_file => self.create_file,
            GrantType::create_directory => self.create_directory,
            GrantType::open_directory => self.open_directory,
        }
    }

    fn set_grant(&mut self, grant_type: GrantType, grant: Grant) -> Grant {
        match grant_type {
            GrantType::file_create_event => {
                self.file_create_event = grant;
                grant
            }
            GrantType::dir_create_event => {
                self.dir_create_event = grant;
                grant
            }
            GrantType::file_delete_event => {
                self.file_delete_event = grant;
                grant
            }
            GrantType::dir_delete_event => {
                self.dir_delete_event = grant;
                grant
            }
            GrantType::file_open_event => {
                self.file_open_event = grant;
                grant
            }
            GrantType::file_close_event => {
                self.file_close_event = grant;
                grant
            }
            GrantType::file_read_event => {
                self.file_read_event = grant;
                grant
            }
            GrantType::file_write_event => {
                self.file_write_event = grant;
                grant
            }
            GrantType::http_get_event => {
                self.http_get_event = grant;
                grant
            }
            GrantType::http_post_event => {
                self.http_post_event = grant;
                grant
            }
            GrantType::open_file => {
                self.open_file = grant;
                grant
            }
            GrantType::close_file => {
                self.close_file = grant;
                grant
            }
            GrantType::read_file => {
                self.read_file = grant;
                grant
            }
            GrantType::write_file => {
                self.write_file = grant;
                grant
            }
            GrantType::create_file => {
                self.create_file = grant;
                grant
            }
            GrantType::create_directory => {
                self.create_directory = grant;
                grant
            }
            GrantType::open_directory => {
                self.open_directory = grant;
                grant
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub(crate) struct WasmPermissions {
    inner: HashMap<PathBuf, ProgramPermissions>,
}

impl WasmPermissions {
    pub(crate) fn new() -> Self {
        WasmPermissions {
            inner: HashMap::new(),
        }
    }

    pub(crate) fn add_program(&mut self, program: PathBuf) {
        self.inner
            .entry(program)
            .or_insert(ProgramPermissions::new());
    }

    pub(crate) fn remove_program(&mut self, program: &PathBuf) {
        self.inner.remove(program);
    }

    pub(crate) fn check_grant(
        &mut self,
        program: &PathBuf,
        grant_type: GrantType,
    ) -> Option<Grant> {
        match self.inner.get_mut(program) {
            Some(mut p) => Some(check_grant_and_get_auth(&mut p, program, grant_type)),
            None => None,
        }
    }
}

fn check_grant_and_get_auth(
    inner: &mut ProgramPermissions,
    program: &PathBuf,
    grant_type: GrantType,
) -> Grant {
    match inner.get_grant(grant_type) {
        Grant::Unknown => inner.set_grant(
            grant_type,
            get_authorization(&program, grant_type.grant_string()),
        ),
        grant => grant,
    }
}

fn get_authorization(program: &PathBuf, grant_name: &str) -> Grant {
    let mut buffer = String::new();
    print!(
        "Allow {} to {}? (y/N): ",
        program.to_str().unwrap(),
        grant_name
    );
    io::stdout().flush().unwrap();
    io::stdin().read_line(&mut buffer).unwrap();
    if buffer == "y\n" || buffer == "Y\n" {
        Grant::Allow
    } else {
        Grant::Deny
    }
}
