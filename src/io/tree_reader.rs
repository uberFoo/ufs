//! Buffered Reader for BlockTrees
//!
use std::{
    cmp,
    io::{self, prelude::*},
    ptr,
};

use crate::block::{
    manager::BlockManager, storage::BlockStorage, tree::BlockTree, BlockCardinality,
};

/// True NFB
/// Note that it _does_ need to be at least as large as the largest available block size.  It should
/// also probably be some multiple of that block size.
const DEFAULT_BUF_SIZE: usize = 512; // 8192;

pub(crate) struct BlockTreeReader<'a, S>
where
    S: BlockStorage,
{
    list: &'a BlockTree,
    manager: &'a BlockManager<S>,
    // A cursor that points to the current read position, as a function of the total size of all of
    // the blocks combined.
    pos: usize,
    // The next block that would be read into our internal buffer.
    next_block: BlockCardinality,
    // Our internal buffer.
    buf: Box<[u8]>,
    // The offset of the beginning of the buffer, with respect to the total size of all blocks.
    buf_offset: usize,
    // The number of bytes (from reading blocks) that the buffer contains.
    buf_extent: usize,
}

impl<'a, S> BlockTreeReader<'a, S>
where
    S: BlockStorage,
{
    pub(crate) fn new(list: &'a BlockTree, manager: &'a BlockManager<S>) -> Self {
        unsafe {
            let mut buf = Vec::with_capacity(DEFAULT_BUF_SIZE);
            // This is so that into_boxed_slice does not shrink our buffer to 0 len.
            buf.set_len(DEFAULT_BUF_SIZE);
            // This initializes the buffer to zeroes...and I don't think that it's actually
            // necessary.
            // ptr::write_bytes(buf.as_mut_ptr(), 0, DEFAULT_BUF_SIZE);
            BlockTreeReader {
                list,
                manager,
                pos: 0,
                next_block: 0,
                buf: buf.into_boxed_slice(),
                buf_offset: 0,
                buf_extent: 0,
            }
        }
    }

    fn read_block(&mut self) {
        if let Some(block) = self.list.get(self.next_block) {
            if let Ok(data) = self.manager.read(&block) {
                let len = data.len();
                self.next_block += 1;
                self.buf_extent += len;
                let read = data.as_slice().read(&mut self.buf);
            }
        }
    }
}

impl<'a, S> Read for BlockTreeReader<'a, S>
where
    S: BlockStorage,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n_read = {
            let mut int_buf = self.fill_buf()?;
            int_buf.read(buf)?
        };

        self.consume(n_read);

        Ok(n_read)
    }
}

impl<'a, S> BufRead for BlockTreeReader<'a, S>
where
    S: BlockStorage,
{
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        // let amt = cmp::min(self.pos, self.list.size() as usize);
        if self.pos >= self.buf_extent {
            let offset = self.buf_extent;
            self.read_block();
            self.buf_offset = offset;
        }

        Ok(&self.buf[self.pos - self.buf_offset..self.buf_extent - self.buf_offset])
    }

    fn consume(&mut self, amt: usize) {
        self.pos += amt;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::block::{storage::memory::MemoryStore, tree::BlockTree, BlockSize};

    #[test]
    fn one_block() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 10));
        let block = bm.write(b"abc").unwrap();
        let tree = BlockTree::new(&vec![block]);

        let mut btr = BlockTreeReader::new(&tree, &bm);

        let mut buffer = [0_u8; 0x10];
        let n_read = btr.read(&mut buffer).unwrap();
        println!("test read {:?}", buffer);
        assert_eq!(n_read, b"abc".len());
        assert_eq!(buffer[..n_read], b"abc"[..]);
    }

    #[test]
    fn two_blocks() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 10));
        let tree = BlockTree::new(&vec![
            bm.write(b"abcdefg").unwrap(),
            bm.write(b"hi").unwrap(),
        ]);

        let mut btr = BlockTreeReader::new(&tree, &bm);

        let mut buffer = [0_u8; 0x10];
        let n_read = btr.read(&mut buffer).unwrap();
        println!("test read {:?}", buffer);
        assert_eq!(n_read, b"abcdefg".len());
        assert_eq!(buffer[..n_read], b"abcdefg"[..]);

        let n_read = btr.read(&mut buffer).unwrap();
        println!("test read {:?}", buffer);
        assert_eq!(n_read, b"hi".len());
        assert_eq!(buffer[..n_read], b"hi"[..]);
    }

    #[test]
    fn two_blocks_tiny_read() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 10));
        let tree = BlockTree::new(&vec![
            bm.write(b"abcdefg").unwrap(),
            bm.write(b"hi").unwrap(),
        ]);

        let mut btr = BlockTreeReader::new(&tree, &bm);

        let mut buffer = [0_u8; 0x3];
        let n_read = btr.read(&mut buffer).unwrap();
        println!("test read {:?}", buffer);
        assert_eq!(n_read, b"abc".len());
        assert_eq!(buffer[..n_read], b"abc"[..]);

        let n_read = btr.read(&mut buffer).unwrap();
        println!("test read {:?}", buffer);
        assert_eq!(n_read, b"def".len());
        assert_eq!(buffer[..n_read], b"def"[..]);

        let n_read = btr.read(&mut buffer).unwrap();
        println!("test read {:?}", buffer);
        assert_eq!(n_read, b"g".len());
        assert_eq!(buffer[..n_read], b"g"[..]);

        let n_read = btr.read(&mut buffer).unwrap();
        println!("read {:?}", buffer);
        assert_eq!(n_read, b"hi".len());
        assert_eq!(buffer[..n_read], b"hi"[..]);
    }

}
