#![warn(missing_docs)]
//! A different sort of file system: *IOFS*
//!
//! # File System Structure
//!
//! ## Blocks
//!
//! The blocks are fixed-size, and uniform for a given file system.  Blocks contain nothing more
//! than the raw data that is written to them.  Put another way, there is no metadata _about_ the
//! block stored in the block _itself_.  Rather, metadata blocks contain information about blocks
//! that make up files and directories.
//!
//! ### Working with Blocks
//!
//! Blocks storage is abstracted by the [BlockStorage] trait.  There are implementations that
//! support storing blocks as files in on a host file system, in memory, and over the network.
//!
//! The BlockStorage trait has methods for reading (`read_block`) and, writing (`write_block`)
//! blocks to implemented media.
//!
//! ### Block 0
//!
//! The first block is the file system is special.  It contains the `BlockMap`, described below.
//!
//! ## `BlockMap`
//!
//! The block map maintains a mapping from a block's number to a block's type. It also contains
//! metadata about the blocks themselves. It's stored starting at block 0, and is the first thing
//! loaded when a file system is initialized, by the `BlockStorage` implementation. Additionally
//! the block map stores the block size, free block list, and something called the "root block".
//! This is a pointer to the block that contains the file system metadata, described in the next
//! section.
//!
//! The block map itself is stored as as a `bincode` byte-stream, spread across blocks in the file
//! system. This is accomplished by chunking the serialized bytes and storing them in a
//! `BlockMapWrapper` structure. This struct is sized so that it fit's into a single disk block, and
//! in addition to some piece of the `BlockMap`'s data, it contains a hash of the data, and a
//! pointer to the block number that contains the next chunk of serialized data.
//!
//! Thus when deserializing the `BlockMap`, we begin at block 0, which is a `BlockMapWrapper`, read
//! it's data, and follow it's pointer to our next chunk of data. Reading data, validating it's
//! hash, and appending it to a buffer continues until all of the  data bits are read, and then the
//! `BlockMap` is deserialized with `bincode`.
//!
//! ## Metadata
//!
//! The file system metadata is similar to the block map, but instead of keeping track of file
//! system blocks, it knows about files and directories. Once the block map is constructed in
//! memory, it is possible to do the same for the metadata. Like the block map, the metadata is
//! spread across the filesystem using wrapper blocks.
//!
//! ## Addressing
//!
//! Each file system has a unique ID, as discussed elsewhere. This ID forms a namespace for block
//! numbers. Each block has a number, assigned by the file system starting at 0, and increasing
//! until the file system is full.  Note that this does not preclude growing the file system.
//!
//! ## Block Lists
//!
//! A block list is how data (files, directories, metadata, etc.) is stitched together from
//! disparate blocks. Below the discussion degrades to talking about files, but it is equally
//! applicable to other entities stored on the file system.
//!
//! As files change, the history of it's contents is preserved via a block list. Blocks may be
//! inserted into, and deleted from a block list, and a history is maintained. It is imagined that a
//! user may intentionally delete an entire file, or some portion of it's history, but that is not
//! the default use case. To this end blocks are for the most part write-once.
//!
//! Blocks are encrypted. Period. One of the main intentions of *uberFS* is privacy, and that's
//! simply not possible without encryption. Additionally, the data must be preserved, and secured
//! from bit-rot, intentional tampering, etc.
//!
//! Blocks are encrypted using an AEAD algorithm, and it's MAC is stored, along with the block
//! number in a block list.  Prior to encryption each block is hashed with SHA256, and it's hash
//! is also stored in the block list.  While this may seem a bit belt-and-suspenders, it allows
//! validation of files based on either encrypted or clear-text blocks, and ensures against
//! man-in-the-middle attacks on remote blocks.
//!
//! A block list is thus an array of tuples that include an *operation* (insert, or delete), a
//! *block address* (note that this allows for files comprised of blocks distributed across file
//! systems), a plain-text *hash*, and an encryption *MAC*.
//!
//! # API
//!
//! There are a number of services that need to be built on top of this library, and likely as not
//! quite a few that I have not yet conceived. Therefore the API needs flexibility and generality.
//!
//! The services that I have in mind include:
//!
//!  * A remote *Block* service with a RESTful API. The purpose of this service is to provide an
//! online block storage, with read/write capability. Encrypted blocks may be read, and written by
//! remote file systems.
//!
//!  * A remote *execution* service that, with appropriate authentication, will execute WASM code
//! against files, returning possibly transformed information based on the underlying data. This is
//! distinct from the block service, but may be integrated into the same.
//!
//! * A FUSE-based file system adaptor to mount an *uberFS* as a native file system.
//!
//! * A web-based view for *uberFS*.
//!
//! ## Block Server
//!
//! The block service's main contribution is distributing blocks. It may or may not decrypt blocks,
//! depending on whether proper authentication is provided. It is however intended to utilize TLS
//! in order to preserve encryption in the event that it is returning decrypted blocks.

//! ### Required End-Points
//!
//! * `read_block(number)`
//! * `write_block(number)`
//!

mod block;
mod crypto;
mod fsimpl;
mod fuse;
mod jwt;
mod metadata;
mod server;
mod time;
mod uuid;
mod wasm;

use {
    failure::{Backtrace, Context, Fail},
    std::fmt::{self, Display},
};

pub use {
    crate::{crypto::make_fs_key, fuse::UberFSFuse, uuid::UfsUuid},
    block::{
        manager::BlockManager, map::BlockMap, BlockAddress, BlockCardinality, BlockNumber,
        BlockReader, BlockSize, BlockStorage, BlockWriter, FileStore,
    },
    fsimpl::{OpenFileMode, UberFileSystem, UfsMounter},
};

#[derive(Debug)]
pub(crate) struct IOFSError {
    ctx: Context<IOFSErrorKind>,
}

impl IOFSError {
    pub fn kind(&self) -> &IOFSErrorKind {
        self.ctx.get_context()
    }
}

impl Fail for IOFSError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.ctx.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.ctx.backtrace()
    }
}

impl Display for IOFSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.ctx, f)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
enum IOFSErrorKind {
    #[fail(display = "Directory already exists")]
    DirectoryExists,
    #[fail(display = "Expired token")]
    TokenExpired,
    #[fail(display = "Unknown token")]
    UnknownToken,
    #[fail(display = "Invalid JWT token")]
    InvalidToken,
    #[fail(display = "Invalid JWT Signature")]
    InvalidSignature,
    #[fail(display = "Unknown token error")]
    TokenError,
}

impl From<IOFSErrorKind> for IOFSError {
    fn from(kind: IOFSErrorKind) -> Self {
        IOFSError::from(Context::new(kind))
    }
}

impl From<Context<IOFSErrorKind>> for IOFSError {
    fn from(ctx: Context<IOFSErrorKind>) -> Self {
        IOFSError { ctx }
    }
}
