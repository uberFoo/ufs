//! Buffered Writer for BlockTrees
//!
//! This is an initial, naive implementation that should be cleaned up eventually.
//! Specifically, I don't completely trust that I'm correctly using the VecDeque correctly/as best
//! it could be done.

use std::{
    cell::RefCell,
    cmp,
    collections::VecDeque,
    io::{self, prelude::*},
    rc::Rc,
};

use log::debug;

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

/// Turn bytes into Blocks
///
/// This struct implements [io::Read] and [io::Write], but not in the usually trivial manner.  In
/// our case, we need to chunk the input into properly sized [Block]s, and pass those blocks to
/// a [BlockManager] to persist them.
pub struct BlockTreeWriter<S>
where
    S: BlockStorage,
{
    /// List of written blocks.
    list: Vec<Block>,
    /// A handle to the Block Manager to use.
    /// TODO: I'm really hating the way this is getting passed around everyplace from the root ufs.
    /// I think there must be a better way that Re<RefCell<_>>.
    manager: Rc<RefCell<BlockManager<S>>>,
    /// A place to store incoming bytes before they are turned into Blocks.
    buf: VecDeque<u8>,
}

impl<S> BlockTreeWriter<S>
where
    S: BlockStorage,
{
    pub(crate) fn new(manager: Rc<RefCell<BlockManager<S>>>) -> Self {
        let block_size = manager.borrow().block_size() as usize;
        BlockTreeWriter {
            list: Vec::new(),
            manager,
            buf: VecDeque::with_capacity(block_size * BLOCK_BUF_MULTIPLIER),
        }
    }

    // pub(crate) fn new(manager: &'a mut BlockManager<S>) -> Self {
    //     let block_size = manager.block_size() as usize;
    //     BlockTreeWriter {
    //         list: Vec::new(),
    //         manager,
    //         buf: VecDeque::with_capacity(block_size * BLOCK_BUF_MULTIPLIER),
    //     }
    // }

    pub(crate) fn into_block_tree(mut self) -> io::Result<BlockTree> {
        self.flush_buf()
            .and_then(|()| Ok(BlockTree::new(&self.list)))
    }

    // I don't like that using an Rc<RefCell<_>> is forcing me into this borrowed version.  I much
    // prefer the one above that consumes the writer.  I don't think it's wise to have both the
    // tree and the writer around.  Once you have the tree, the writer should be cast away.
    pub(crate) fn get_tree(&mut self) -> io::Result<BlockTree> {
        self.flush_buf()
            .and_then(|()| Ok(BlockTree::new(&self.list)))
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        let mut written = 0;
        let bs = self.manager.borrow().block_size() as usize;
        let (front, _back) = self.buf.as_slices();

        while written < front.len() {
            let end = cmp::min(written + bs, front.len());
            if let Ok(n) = (BlockTreeBlockWriter {
                list: &mut self.list,
                manager: &mut self.manager.borrow_mut(),
            })
            .write_block(&front[written..end])
            {
                written += n;
            }
        }

        debug!(
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

impl<S> Write for BlockTreeWriter<S>
where
    S: BlockStorage,
{
    /// Create blocks from incoming bytes.
    ///
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Flush our buffer if there's not enough room for the incoming bytes.
        if self.buf.len() + buf.len() > self.buf.capacity() {
            debug!("not enough room in buffer -- flushing");
            self.flush_buf()?;
        }

        // If our buffer is too small, just write the buffer directly as blocks -- keeping any left-
        // over and stuffing it into our buffer.
        let written = if buf.len() >= self.buf.capacity() {
            debug!("buffer too small for data");
            let bs = self.manager.borrow().block_size() as usize;
            let mut written = 0;
            while written < buf.len() {
                let end = cmp::min(written + bs, buf.len());

                // Write the block if we have a full one.
                if end - written == bs {
                    if let Ok(n) = (BlockTreeBlockWriter {
                        list: &mut self.list,
                        manager: &mut self.manager.borrow_mut(),
                    })
                    .write_block(&buf[written..end])
                    {
                        written += n;
                    }
                } else {
                    debug!("extra bytes: {}", buf.len());
                    self.buf.append(&mut (Vec::from(&buf[written..end])).into());
                    written += end - written;
                }
            }

            written
        } else {
            // Otherwise, add the bytes to our buffer.
            debug!("buffering {} bytes", buf.len());
            self.buf.append(&mut (Vec::from(buf)).into());
            buf.len()
        };

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
        let mut btw = BlockTreeWriter::new(Rc::new(RefCell::new(bm)));

        btw.write_all(b"abc").unwrap();
        btw.write_all(b"def").unwrap();
        btw.write_all(&[103; 512 - 6]).unwrap();
        btw.write_all(&[104; 2222]).unwrap();
        btw.write_all(&[105; 1024]).unwrap();
        let tree = btw.into_block_tree()();
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
