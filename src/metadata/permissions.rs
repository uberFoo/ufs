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
    serde_derive::{Deserialize, Serialize},
    std::{
        collections::HashMap,
        io::{self, Write},
        path::PathBuf,
    },
};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub(crate) enum Grant {
    Unknown,
    Allow,
    Deny,
}

// #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
// pub(crate) enum HttpGrant {
//     Unknown,
//     Allow(String),
//     Deny,
// }

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct HttpGrant {
    inner: HashMap<String, Grant>,
}

impl HttpGrant {
    fn new() -> Self {
        HttpGrant {
            inner: HashMap::new(),
        }
    }

    fn check(&mut self, route: String) -> Grant {
        *self.inner.entry(route).or_insert(Grant::Unknown)
    }

    fn set(&mut self, route: String, grant: Grant) -> Grant {
        *self
            .inner
            .entry(route)
            .and_modify(|g| *g = grant)
            .or_insert(grant)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum GrantType {
    FileCreateEvent,
    DirCreateEvent,
    FileDeleteEvent,
    DirDeleteEvent,
    FileOpenEvent,
    FileCloseEvent,
    FileReadEvent,
    FileWriteEvent,
    HttpGetEvent,
    HttpPostEvent,
    HttpPutEvent,
    HttpPatchEvent,
    HttpDeleteEvent,
    OpenFileInvocation,
    CloseFileInvocation,
    ReadFileInvocation,
    WriteFileInvocation,
    CreateFileInvocation,
    CreateDirectoryInvocation,
    OpenDirectoryInvocation,
}

impl GrantType {
    pub(crate) fn grant_string(&self) -> &'static str {
        match self {
            GrantType::FileCreateEvent => "receive file create events",
            GrantType::DirCreateEvent => "receive directory create events",
            GrantType::FileDeleteEvent => "receive file delete events",
            GrantType::DirDeleteEvent => "receive directory delete events",
            GrantType::FileOpenEvent => "receive file open events",
            GrantType::FileCloseEvent => "receive file close events",
            GrantType::FileReadEvent => "receive file read events",
            GrantType::FileWriteEvent => "receive file write events",
            GrantType::HttpGetEvent => "receive HTTP GET to",
            GrantType::HttpPostEvent => "receive HTTP POST to",
            GrantType::HttpPutEvent => "receive HTTP PUT to",
            GrantType::HttpPatchEvent => "receive HTTP PATCH to",
            GrantType::HttpDeleteEvent => "receive HTTP DELETE to",
            GrantType::OpenFileInvocation => "open files",
            GrantType::CloseFileInvocation => "close files",
            GrantType::ReadFileInvocation => "read files",
            GrantType::WriteFileInvocation => "write files",
            GrantType::CreateFileInvocation => "create files",
            GrantType::CreateDirectoryInvocation => "create directories",
            GrantType::OpenDirectoryInvocation => "open directories",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub(crate) struct ProgramPermissions {
    // File System Events
    file_create: Grant,
    dir_create: Grant,
    file_delete: Grant,
    dir_delete: Grant,
    file_open: Grant,
    file_close: Grant,
    file_read: Grant,
    file_write: Grant,
    // HTTP Events
    http_get: HttpGrant,
    http_post: HttpGrant,
    http_put: HttpGrant,
    http_patch: HttpGrant,
    http_delete: HttpGrant,
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
            file_create: Grant::Unknown,
            dir_create: Grant::Unknown,
            file_delete: Grant::Unknown,
            dir_delete: Grant::Unknown,
            file_open: Grant::Unknown,
            file_close: Grant::Unknown,
            file_read: Grant::Unknown,
            file_write: Grant::Unknown,
            http_get: HttpGrant::new(),
            http_post: HttpGrant::new(),
            http_put: HttpGrant::new(),
            http_patch: HttpGrant::new(),
            http_delete: HttpGrant::new(),
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
            GrantType::FileCreateEvent => self.file_create,
            GrantType::DirCreateEvent => self.dir_create,
            GrantType::FileDeleteEvent => self.file_delete,
            GrantType::DirDeleteEvent => self.dir_delete,
            GrantType::FileOpenEvent => self.file_open,
            GrantType::FileCloseEvent => self.file_close,
            GrantType::FileReadEvent => self.file_read,
            GrantType::FileWriteEvent => self.file_write,
            GrantType::OpenFileInvocation => self.open_file,
            GrantType::CloseFileInvocation => self.close_file,
            GrantType::ReadFileInvocation => self.read_file,
            GrantType::WriteFileInvocation => self.write_file,
            GrantType::CreateFileInvocation => self.create_file,
            GrantType::CreateDirectoryInvocation => self.create_directory,
            GrantType::OpenDirectoryInvocation => self.open_directory,
            _ => panic!("called get_grant with HTTP grant-type"),
        }
    }

    fn get_http_grant(&mut self, grant_type: GrantType, route: String) -> Grant {
        match grant_type {
            GrantType::HttpGetEvent => self.http_get.check(route),
            GrantType::HttpPostEvent => self.http_post.check(route),
            GrantType::HttpPutEvent => self.http_put.check(route),
            GrantType::HttpPatchEvent => self.http_patch.check(route),
            GrantType::HttpDeleteEvent => self.http_delete.check(route),
            _ => panic!("called get_http_grant with non-HTTP grant-type"),
        }
    }

    fn set_grant(&mut self, grant_type: GrantType, grant: Grant) -> Grant {
        match grant_type {
            GrantType::FileCreateEvent => {
                self.file_create = grant;
                grant
            }
            GrantType::DirCreateEvent => {
                self.dir_create = grant;
                grant
            }
            GrantType::FileDeleteEvent => {
                self.file_delete = grant;
                grant
            }
            GrantType::DirDeleteEvent => {
                self.dir_delete = grant;
                grant
            }
            GrantType::FileOpenEvent => {
                self.file_open = grant;
                grant
            }
            GrantType::FileCloseEvent => {
                self.file_close = grant;
                grant
            }
            GrantType::FileReadEvent => {
                self.file_read = grant;
                grant
            }
            GrantType::FileWriteEvent => {
                self.file_write = grant;
                grant
            }
            GrantType::OpenFileInvocation => {
                self.open_file = grant;
                grant
            }
            GrantType::CloseFileInvocation => {
                self.close_file = grant;
                grant
            }
            GrantType::ReadFileInvocation => {
                self.read_file = grant;
                grant
            }
            GrantType::WriteFileInvocation => {
                self.write_file = grant;
                grant
            }
            GrantType::CreateFileInvocation => {
                self.create_file = grant;
                grant
            }
            GrantType::CreateDirectoryInvocation => {
                self.create_directory = grant;
                grant
            }
            GrantType::OpenDirectoryInvocation => {
                self.open_directory = grant;
                grant
            }
            _ => panic!("called set_grant with HTTP grant-type"),
        }
    }

    fn set_http_grant(&mut self, grant_type: GrantType, route: String, grant: Grant) -> Grant {
        match grant_type {
            GrantType::HttpGetEvent => self.http_get.set(route, grant),
            GrantType::HttpPostEvent => self.http_post.set(route, grant),
            GrantType::HttpPutEvent => self.http_put.set(route, grant),
            GrantType::HttpPatchEvent => self.http_patch.set(route, grant),
            GrantType::HttpDeleteEvent => self.http_delete.set(route, grant),
            _ => panic!("called set_http_grant with non-HTTP grant-type"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub(crate) struct WasmPermissions {
    dirty: bool,
    inner: HashMap<PathBuf, ProgramPermissions>,
}

impl WasmPermissions {
    pub(crate) fn new() -> Self {
        WasmPermissions {
            dirty: true,
            inner: HashMap::new(),
        }
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub(crate) fn add_program(&mut self, program: PathBuf) {
        self.dirty = true;
        self.inner
            .entry(program)
            .or_insert(ProgramPermissions::new());
    }

    pub(crate) fn remove_program(&mut self, program: &PathBuf) {
        self.dirty = true;
        self.inner.remove(program);
    }

    pub(crate) fn check_grant(
        &mut self,
        program: &PathBuf,
        grant_type: GrantType,
    ) -> Option<Grant> {
        match self.inner.get_mut(program) {
            Some(mut p) => {
                let (changed, grant) = check_grant_and_get_auth(&mut p, program, grant_type);
                self.dirty = changed;
                Some(grant)
            }
            None => None,
        }
    }

    pub(crate) fn check_http_grant(
        &mut self,
        program: &PathBuf,
        grant_type: GrantType,
        route: &str,
    ) -> Option<Grant> {
        match self.inner.get_mut(program) {
            Some(mut p) => {
                let (changed, grant) =
                    check_http_grant_and_get_auth(&mut p, program, grant_type, route);
                self.dirty = changed;
                Some(grant)
            }
            None => None,
        }
    }
}

fn query_user(prompt: String) -> bool {
    let mut buffer = String::new();
    print!("{}", prompt);

    io::stdout().flush().unwrap();
    io::stdin().read_line(&mut buffer).unwrap();

    if buffer == "y\n" || buffer == "Y\n" {
        true
    } else if buffer == "\n" {
        false
    } else {
        query_user(prompt)
    }
}

fn check_grant_and_get_auth(
    inner: &mut ProgramPermissions,
    program: &PathBuf,
    grant_type: GrantType,
) -> (bool, Grant) {
    match inner.get_grant(grant_type) {
        Grant::Unknown => (
            true,
            inner.set_grant(
                grant_type,
                get_authorization(&program, grant_type.grant_string()),
            ),
        ),
        grant => (false, grant),
    }
}

fn get_authorization(program: &PathBuf, grant_desc: &str) -> Grant {
    if query_user(format!(
        "\nAllow {} to {}? (y/N): ",
        program.to_str().unwrap(),
        grant_desc
    )) {
        Grant::Allow
    } else {
        Grant::Deny
    }
}

fn check_http_grant_and_get_auth(
    inner: &mut ProgramPermissions,
    program: &PathBuf,
    grant_type: GrantType,
    route: &str,
) -> (bool, Grant) {
    match inner.get_http_grant(grant_type, route.to_string()) {
        Grant::Unknown => (
            true,
            inner.set_http_grant(
                grant_type,
                route.to_string(),
                get_http_authorization(&program, grant_type.grant_string(), route),
            ),
        ),
        grant => (false, grant),
    }
}

fn get_http_authorization(program: &PathBuf, grant_desc: &str, route: &str) -> Grant {
    if query_user(format!(
        "\nAllow {} to {} /wasm/{}? (y/N): ",
        program.to_str().unwrap(),
        grant_desc,
        route
    )) {
        Grant::Allow
    } else {
        Grant::Deny
    }
}
