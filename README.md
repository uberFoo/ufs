# ufs

A different sort of file system: UberFS

## FIXME

I need to touch on the following topics:

-   Distributed block storage
-   Block level encryption
-   Merkle Tree usage
-   WASM support

## File System Structure

Like every other file system, with some possible few exceptions, fixed-size blocks are the
foundation of UberFS. What's different across all file systems is how they are utilized.
Particularly, in how the file system metadata is represented. We'll touch on blocks, and other
miscellany, but the primary focus of this section will be metadata representation.

### Metadata

All metadata is stored in a dictionary. The dictionary is serialized with to a `Vec<u8>` with
`Serde` and `Bincode`, and written to blocks. If the serialized dictionary does not fit within a
single block, it will contain a pointer to the next block. The pointer is stored under the key
`@next_block`. Additionally, eac block is identified by a `@type` key, where the value may be
something like `directory`, `fs-metadata`, etc. Additional metadata for each specific block
type may exist under the `@metadata` key. Data specific to the block type lives under the
`@data` key. Finally, each dictionary contains a `@checksum` key that contains the checksum
for the entire dictionary.

#### Block Trees

### Blocks

The blocks are fixed-size, and uniform for a given file system. Blocks contain nothing more
than the raw data that is written to them. Put another way, there is no metadata _about_ the
block stored in the block _itself_. Rather, metadata blocks contain information about blocks
that make up files and directories.

#### Block 0

The first block is the file system is special. It contains information about the file system
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
dictionary. Instead, it's legacy code that needs to be updated.
