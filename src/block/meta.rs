use std::collections::HashMap;

use serde_derive::{Deserialize, Serialize};

use crate::block::{BlockCardinality, BlockList, BlockSize};

/// Block-level File System Metadata
///
/// This is the information necessary to bootstrap the block management subsystem from storage.
/// I plan on elaborating this quite extensively in the future. For now however, I'm applying
/// Occam's Razor.  Minimally, we need to know how many blocks there are in the file system, and we
/// need to know the block size.
///
/// Additionally, for now, I think it easiest to allocate the total number of blocks necessary to
/// store the metadata at file system creation.  Eventually UberBlocks may allow us to bypass this
/// need.
///
/// Then there is the issue of free blocks, or conversely used blocks.  Closely related to these
/// is the number of free blocks.  As an aside, I don't think that the free block count is not
/// strictly necessary, but makes things nicer when allocating blocks.  As such, I'm on the fence
/// about keeping it.
///
/// Anyway, one assumption of this nascent file system is that blocks are write once.  This has
/// nice side effects, like versioning, cloning, snapshots, etc.  None of which will I be
/// considering at the moment!  Instead, the thought is that a free block list is sort of redundant
/// when the next available block is a monotonically increasing integer.  So I think it best,
/// keeping things simple for now, to store the next available block number.  A nice side effect is
/// that the number of free blocks is easily calculated from that and the block count.
///
/// Another issue is that of Merkle Trees.  With my UberBlock idea, the block trees grew as
/// necessary to accommodate used blocks.  In this case, I'd need to figure out the total size of
/// the tree a-priori to allocate enough blocks when the file system is created.  While this isn't
/// hard (n^{log_2(n) + 1} - 1, where n = |blocks|), I can't say how important this feature is
/// just now.
///
/// Bootstrapping is an interesting problem in that it's strictly necessary that we are able to read
/// block 0.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct BlockMetadata {
    pub size: BlockSize,
    pub count: BlockCardinality,
    pub next_free_block: Option<BlockCardinality>,
    pub block_list_map: HashMap<String, Option<BlockList>>,
}

impl BlockMetadata {
    /// Load the [BlockMetadata]
    ///
    /// This method retrieves the metadata from block 0 of the [BlockStorage].
    ///
    /// FIXME: If this fails, then what?
    pub(crate) fn deserialize<T>(bytes: T) -> bincode::Result<Self>
    where
        T: AsRef<[u8]>,
    {
        bincode::deserialize(bytes.as_ref())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_metadata() {
        unimplemented!();
    }
}
