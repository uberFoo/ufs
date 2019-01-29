//! The Place that Blocks Live
//!
//! Merkle Tree Implementation for Block Validation
//!
use std::collections::VecDeque;

use ring::{self, digest};

use crate::block::{Block, BlockChecksum};

#[derive(Debug, PartialEq)]
crate enum BlockTree {
    Empty,
    File(Box<InnerNode>),
    // Directory(Box<InnerNode>),
    Inner(Box<InnerNode>),
    Leaf(Box<LeafNode>),
}

impl BlockTree {
    fn checksum(&self) -> Option<&BlockChecksum> {
        match self {
            BlockTree::File(n) | BlockTree::Inner(n) => Some(&n.checksum),
            BlockTree::Leaf(n) => Some(&n.block.checksum),
            BlockTree::Empty => None,
        }
    }

    /// FIXME:
    ///  * don't unwrap() checksums below
    ///  * maybe use a slice of Blocks?
    ///  * should be hardened to second pre-image attack
    ///  * We are having to clone the blocks because of the way that I'm chunking the blocks.  I
    ///    can either revert to using block_list.into_iter, or just live with it.  In fact, at this
    ///    point, I'm not sure which way it should be.
    pub fn new_file(block_list: Vec<Block>) -> Self {
        let mut inner_nodes = VecDeque::<BlockTree>::new();

        // Iterate over the Vec of blocks, pair-wise, in order to build inner nodes.  Those nodes
        // are added to a deque which will then be used to construct the rest of the tree.
        for pair in block_list.chunks(2) {
            let inner_block = match pair {
                [left, right] => {
                    let mut ctx = digest::Context::new(&digest::SHA256);
                    ctx.update(left.checksum.as_ref());
                    ctx.update(right.checksum.as_ref());

                    BlockTree::Inner(Box::new(InnerNode {
                        left: BlockTree::Leaf(Box::new(LeafNode {
                            block: left.clone(),
                        })),
                        right: BlockTree::Leaf(Box::new(LeafNode {
                            block: right.clone(),
                        })),
                        checksum: BlockChecksum::from(ctx.finish().as_ref()),
                    }))
                }
                [left] => {
                    let mut ctx = digest::Context::new(&digest::SHA256);
                    ctx.update(left.checksum.as_ref());
                    ctx.update(left.checksum.as_ref());

                    BlockTree::Inner(Box::new(InnerNode {
                        left: BlockTree::Leaf(Box::new(LeafNode {
                            block: left.clone(),
                        })),
                        right: BlockTree::Empty,
                        checksum: BlockChecksum::from(ctx.finish().as_ref()),
                    }))
                }
                _ => unreachable!(),
            };
            inner_nodes.push_back(inner_block);
        }

        // We stop when there is one node left, because it is the root.
        while inner_nodes.len() > 1 {
            let inner_node = match (inner_nodes.pop_front(), inner_nodes.pop_front()) {
                (Some(left), Some(right)) => {
                    let mut ctx = digest::Context::new(&digest::SHA256);
                    ctx.update(left.checksum().unwrap().as_ref());
                    ctx.update(right.checksum().unwrap().as_ref());

                    BlockTree::Inner(Box::new(InnerNode {
                        left: left,
                        right: right,
                        checksum: BlockChecksum::from(ctx.finish().as_ref()),
                    }))
                }
                (Some(left), None) => {
                    let mut ctx = digest::Context::new(&digest::SHA256);
                    ctx.update(left.checksum().unwrap().as_ref());
                    ctx.update(left.checksum().unwrap().as_ref());

                    BlockTree::Inner(Box::new(InnerNode {
                        left: left,
                        right: BlockTree::Empty,
                        checksum: BlockChecksum::from(ctx.finish().as_ref()),
                    }))
                }
                _ => unreachable!(),
            };

            // Note that the deque is a stack at this point, otherwise the nodes come out
            // backwards.
            inner_nodes.push_front(inner_node);
        }

        inner_nodes
            .pop_front()
            .map_or(BlockTree::Empty, |i| match i {
                BlockTree::Inner(i) => BlockTree::File(Box::new(*i)),
                _ => unreachable!(),
            })
    }
}

#[derive(Debug, PartialEq)]
crate struct InnerNode {
    left: BlockTree,
    right: BlockTree,
    checksum: BlockChecksum,
}

#[derive(Debug, PartialEq)]
crate struct LeafNode {
    block: Block,
}

#[cfg(test)]
mod test {
    use assert_matches::assert_matches;

    use super::*;

    #[test]
    fn one_block_file() {
        let b0 = Block {
            number: 0,
            checksum: BlockChecksum::new(b"block0"),
        };
        let mut block_list = Vec::new();
        block_list.push(b0.clone());

        let tree = BlockTree::new_file(block_list);
        println!("tree: {:#?}", tree);

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b0.checksum.as_ref());
        ctx.update(b0.checksum.as_ref());

        assert_matches!(
            tree,
            BlockTree::File(node) => {
                assert_matches!(
                    node.left,
                    BlockTree::Leaf(node) => {
                        assert_eq!(node.block, b0);
                    }
                );
                assert_eq!(node.right, BlockTree::Empty);
                assert_eq!(node.checksum.as_ref(), ctx.finish().as_ref());
            }
        )
    }

    #[test]
    fn two_block_file() {
        let b0 = Block {
            number: 0,
            checksum: BlockChecksum::new(b"block0"),
        };
        let b1 = Block {
            number: 1,
            checksum: BlockChecksum::new(b"block1"),
        };
        let mut block_list = Vec::new();
        block_list.push(b0.clone());
        block_list.push(b1.clone());

        let tree = BlockTree::new_file(block_list);
        println!("tree: {:#?}", tree);

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b0.checksum.as_ref());
        ctx.update(b1.checksum.as_ref());

        assert_matches!(
            tree,
            BlockTree::File(node) => {
                assert_matches!(
                    node.left,
                    BlockTree::Leaf(node) => {
                        assert_eq!(node.block, b0);
                    }
                );
                assert_matches!(
                    node.right,
                    BlockTree::Leaf(node) => {
                        assert_eq!(node.block, b1);
                    }
                );
                assert_eq!(node.checksum.as_ref(), ctx.finish().as_ref());
            }
        )
    }

    #[test]
    #[allow(clippy::cyclomatic_complexity)]
    fn five_block_file() {
        let b0 = Block {
            number: 0,
            checksum: BlockChecksum::new(b"block0"),
        };
        let b1 = Block {
            number: 1,
            checksum: BlockChecksum::new(b"block1"),
        };
        let b2 = Block {
            number: 2,
            checksum: BlockChecksum::new(b"block2"),
        };
        let b3 = Block {
            number: 3,
            checksum: BlockChecksum::new(b"block3"),
        };
        let b4 = Block {
            number: 4,
            checksum: BlockChecksum::new(b"block4"),
        };

        let mut block_list = Vec::new();
        block_list.push(b0.clone());
        block_list.push(b1.clone());
        block_list.push(b2.clone());
        block_list.push(b3.clone());
        block_list.push(b4.clone());

        let tree = BlockTree::new_file(block_list);
        println!("tree: {:#?}", tree);

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b0.checksum.as_ref());
        ctx.update(b1.checksum.as_ref());
        let b01 = ctx.finish();

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b2.checksum.as_ref());
        ctx.update(b3.checksum.as_ref());
        let b23 = ctx.finish();

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b4.checksum.as_ref());
        ctx.update(b4.checksum.as_ref());
        let b44 = ctx.finish();

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b01.as_ref());
        ctx.update(b23.as_ref());
        let b0123 = ctx.finish();

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b0123.as_ref());
        ctx.update(b44.as_ref());
        let b012344 = ctx.finish();

        assert_matches!(
            tree,
            BlockTree::File(node) => {
                assert_matches!(
                    node.left,
                    BlockTree::Inner(node) => {
                        assert_matches!(
                            node.left,
                            BlockTree::Inner(node) => {
                                assert_matches!(
                                    node.left,
                                    BlockTree::Leaf(node) => {
                                        assert_eq!(node.block, b0);
                                    }
                                );
                                assert_matches!(
                                    node.right,
                                    BlockTree::Leaf(node) => {
                                        assert_eq!(node.block, b1);
                                    }
                                );
                                assert_eq!(node.checksum.as_ref(), b01.as_ref());
                            }
                        );
                        assert_matches!(
                            node.right,
                            BlockTree::Inner(node) => {
                                assert_matches!(
                                    node.left,
                                    BlockTree::Leaf(node) => {
                                        assert_eq!(node.block, b2);
                                    }
                                );
                                assert_matches!(
                                    node.right,
                                    BlockTree::Leaf(node) => {
                                        assert_eq!(node.block, b3);
                                    }
                                );
                                assert_eq!(node.checksum.as_ref(), b23.as_ref());
                            }
                        );
                        assert_eq!(node.checksum.as_ref(), b0123.as_ref());
                    }
                );
                assert_matches!(
                    node.right,
                    BlockTree::Inner(node) => {
                        assert_eq!(node.checksum.as_ref(), b44.as_ref());
                        assert_matches!(
                            node.left,
                            BlockTree::Leaf(node) => {
                                assert_eq!(node.block, b4);
                            }
                        );
                        assert_matches!(
                            node.right,
                            BlockTree::Empty
                        );
                    }
                );
                assert_eq!(node.checksum.as_ref(), b012344.as_ref());
            }
        )
    }
}
