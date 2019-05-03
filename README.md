# ufs

A different sort of file system: *uberFS*

## File System Structure

Like every other file system, with some possible few exceptions, fixed-size blocks are the
foundation of *uberFS*. What's different across all file systems is how they are utilized.
Particularly, in how the file system metadata is represented.  We'll touch on blocks, and other
miscellany, but the primary focus of this section will be metadata representation.

### Metadata

All metadata is stored in a dictionary. The dictionary is serialized with to a `Vec<u8>` with
`Serde` and `Bincode`, and written to blocks. If the serialized dictionary does not fit within a
single block, it will contain a pointer to the next block.  The pointer is stored under the key
`@next_block`. Additionally, eac block is identified by a `@type` key, where the value may be
something like `directory`, `fs-metadata`, etc.  Additional metadata for each specific block
type may exist under the `@metadata` key.  Data specific to the block type lives under the
`@data` key. Finally, each dictionary contains a `@checksum` key that contains the checksum
for the entire dictionary.

### Blocks

The blocks are fixed-size, and uniform for a given file system.  Blocks contain nothing more
than the raw data that is written to them.  Put another way, there is no metadata _about_ the
block stored in the block _itself_.  Rather, metadata blocks contain information about blocks
that make up files and directories.

#### Working with Blocks

Blocks storage is abstracted by the [BlockStorage] trait.  There are implementations that
support storing blocks as files in on a host file system, in memory, and over the network.

The BlockStorage trait has methods for reading (`read_block`) and, writing (`write_block`)
blocks to implemented media.

#### Block 0

The first block is the file system is special.  It contains information about the file system
itself, such as the number of blocks, the block size, a free block list, etc.

The following is currently how block 0 is organized; it's serialized to a `Vec<u8>` using
`Serde` and `Bincode`:

```rust
pub(crate) struct BlockMetadata {
   pub size: BlockSize,
   pub count: BlockCardinality,
   pub next_free_block: Option<BlockCardinality>,
   pub directory: HashMap<String, Block>,
}
```

Note that the above flies in the face of what was described above -- this is clearly not a
dictionary.  Instead, it's legacy code that needs to be updated.

### Addressing

Each file system has a unique ID, as discussed elsewhere. This ID forms a namespace for block
numbers. Each block has a number, assigned by the file system starting at 0, and increasing
until the file system is full.  Note that this does not preclude growing the file system.

### Block Lists

A block list is how data (files, directories, metadata, etc.) is stitched together from
disparate blocks. Below the discussion degrades to talking about files, but it is equally
applicable to other entities stored on the file system.

As files change, the history of it's contents is preserved via a block list. Blocks may be
inserted into, and deleted from a block list, and a history is maintained. It is imagined that a
user may intentionally delete an entire file, or some portion of it's history, but that is not
the default use case. To this end blocks are for the most part write-once.

Blocks are encrypted. Period. One of the main intentions of *uberFS* is privacy, and that's
simply not possible without encryption. Additionally, the data must be preserved, and secured
from bit-rot, intentional tampering, etc.

Blocks are encrypted using an AEAD algorithm, and it's MAC is stored, along with the block
number in a block list.  Prior to encryption each block is hashed with SHA256, and it's hash
is also stored in the block list.  While this may seem a bit belt-and-suspenders, it allows
validation of files based on either encrypted or clear-text blocks, and ensures against
man-in-the-middle attacks on remote blocks.

A block list is thus an array of tuples that include an *operation* (insert, or delete), a
*block address* (note that this allows for files comprised of blocks distributed across file
systems), a plain-text *hash*, and an encryption *MAC*.

## API

There are a number of services that need to be built on top of this library, and likely as not
quite a few that I have not yet conceived. Therefore the API needs flexibility and generality.

The services that I have in mind include:

 * A remote *Block* service with a RESTful API. The purpose of this service is to provide an
online block storage, with read/write capability. Encrypted blocks may be read, and written by
remote file systems.

 * A remote *execution* service that, with appropriate authentication, will execute WASM code
against files, returning possibly transformed information based on the underlying data. This is
distinct from the block service, but may be integrated into the same.

* A FUSE-based file system adaptor to mount an *uberFS* as a native file system.

* A web-based view for *uberFS*.

### Block Server

The block service's main contribution is distributing blocks. It may or may not decrypt blocks,
depending on whether proper authentication is provided. It is however intended to utilize TLS
in order to preserve encryption in the event that it is returning decrypted blocks.
#### Required End-Points

* `read_block(number)`
* `write_block(number)`

