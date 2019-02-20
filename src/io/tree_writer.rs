//! Buffered Writer for BlockTrees
//!
//! This is an initial, naive implementation that should be cleaned up eventually.
//! Specifically, I don't completely trust that I'm correctly using the VecDeque correctly/as best
//! it could be done.

use std::{
    cmp,
    collections::VecDeque,
    fmt,
    io::{self, prelude::*},
    ptr,
};

use crate::block::{manager::BlockManager, storage::BlockStorage, tree::BlockTree, Block};

/// Our buffer is block_size multiplied by this constant.  I have no idea what a good value is, but
/// it did occur to me that a nifty adaptive algorithm could be devised to update this value
/// depending on usage patterns.
const BLOCK_BUF_MULTIPLIER: usize = 3;

struct BlockTreeBlockWriter<'writer, S>
where
    S: BlockStorage,
{
    list: &'writer mut Vec<Block>,
    manager: &'writer mut BlockManager<S>,
}

impl<'writer, S> BlockTreeBlockWriter<'writer, S>
where
    S: BlockStorage,
{
    fn write_block(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self.manager.write(bytes) {
            Ok(block) => {
                let size = block.size();
                self.list.push(block);
                Ok(size)
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.compat())),
        }
    }
}

pub(crate) struct BlockTreeWriter<'a, S>
where
    S: BlockStorage,
{
    list: Vec<Block>,
    manager: &'a mut BlockManager<S>,
    buf: VecDeque<u8>,
}

impl<'a, S> BlockTreeWriter<'a, S>
where
    S: BlockStorage,
{
    pub(crate) fn new(manager: &'a mut BlockManager<S>) -> Self {
        let block_size = manager.block_size() as usize;
        BlockTreeWriter {
            list: Vec::new(),
            manager,
            buf: VecDeque::with_capacity(block_size * BLOCK_BUF_MULTIPLIER),
        }
    }

    pub(crate) fn freeze(mut self) -> BlockTree {
        self.flush_buf();
        BlockTree::new(&self.list)
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        let mut written = 0;
        let bs = self.manager.block_size() as usize;
        let (front, _back) = self.buf.as_slices();

        // let mut start = 0;
        while written < front.len() {
            let end = cmp::min(written + bs, front.len());
            if let Ok(n) = (BlockTreeBlockWriter {
                list: &mut self.list,
                manager: &mut self.manager,
            })
            .write_block(&front[written..end])
            {
                // start += bs;
                written += n;
            }
        }

        println!(
            "flushed: buf size: {}, front size: {}, written: {}",
            self.buf.len(),
            front.len(),
            written
        );

        if written > 0 {
            self.buf.drain(..written);
        }

        Ok(())
    }
}

impl<'a, S> Write for BlockTreeWriter<'a, S>
where
    S: BlockStorage,
{
    /// Create blocks from incoming bytes.
    ///
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Flush our buffer if there's not enough room for the incoming bytes.
        if self.buf.len() + buf.len() > self.buf.capacity() {
            println!("not enough room in buffer -- flushing");
            self.flush_buf()?;
        }

        // If our buffer is too small, just write the buffer directly as blocks -- keeping any left-
        // over and stuffing it into our buffer.
        let written = if buf.len() >= self.buf.capacity() {
            println!("buffer too small for data");
            let bs = self.manager.block_size() as usize;
            let mut written = 0;
            while written < buf.len() {
                let end = cmp::min(written + bs, buf.len());

                // Write the block if we have a full one.
                if end - written == bs {
                    if let Ok(n) = (BlockTreeBlockWriter {
                        list: &mut self.list,
                        manager: &mut self.manager,
                    })
                    .write_block(&buf[written..end])
                    {
                        written += n;
                    }
                } else {
                    println!("extra bytes: {}", buf.len());
                    self.buf.append(&mut (Vec::from(&buf[written..end])).into());
                    written += end - written;
                }
            }

            written

        // buf.chunks_exact(512).fold(0, |written, bytes| {
        //     let n = match (BlockTreeBlockWriter {
        //         list: &mut self.list,
        //         manager: &mut self.manager,
        //     })
        //     .write_block(bytes)
        //     {
        //         Ok(n) => n,
        //         Err(_) => panic!("How the fuck to I make this work!?"),
        //         // Err(e) => {
        //         //     return Err(io::Error::new(io::ErrorKind::Other, e.compat()));
        //         // }
        //     };

        //     written + n
        // })
        } else {
            // Otherwise, add the bytes to our buffer.
            println!("buffering {} bytes", buf.len());
            self.buf.append(&mut (Vec::from(buf)).into());
            buf.len()
        };

        println!("buffer: len: {}\ncontents: {:?}", self.buf.len(), self.buf);

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buf()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::{
        block::{storage::memory::MemoryStore, tree::BlockTree, BlockSize},
        io::tree_reader::BlockTreeReader,
    };

    #[test]
    fn one_test() {
        let mut bm = BlockManager::new(MemoryStore::new(BlockSize::FiveTwelve, 0x10));
        let mut btw = BlockTreeWriter::new(&mut bm);

        btw.write(b"abc");
        btw.write(b"def");
        btw.write(&[103; 512 - 6]);
        btw.write(&[104; 2222]);
        btw.write(&[105; 1024]);
        let tree = btw.freeze();
        println!("tree {:#?}", tree);

        assert_eq!(tree.size(), 512 + 2222 + 1024);

        assert_eq!(tree.block_count(), 8);

        let mut b0 = Vec::<u8>::with_capacity(512);
        b0.extend_from_slice(b"abcdef");
        b0.extend_from_slice(&[103; 512 - 6][..]);
        assert_eq!(bm.read(&tree.get(0).unwrap()).unwrap(), b0);
        assert_eq!(bm.read(&tree.get(1).unwrap()).unwrap(), vec![104; 512]);
        assert_eq!(bm.read(&tree.get(2).unwrap()).unwrap(), vec![104; 512]);
        assert_eq!(bm.read(&tree.get(3).unwrap()).unwrap(), vec![104; 512]);
        assert_eq!(bm.read(&tree.get(4).unwrap()).unwrap(), vec![104; 512]);
        let mut b5 = vec![104; 2222 - 2048];
        b5.append(&mut vec![105; 512 - (2222 - 2048)]);
        assert_eq!(bm.read(&tree.get(6).unwrap()).unwrap(), vec![105; 512]);
        assert_eq!(
            bm.read(&tree.get(7).unwrap()).unwrap(),
            vec![105; 512 - (512 - (2222 - 2048))]
        );
    }
}
