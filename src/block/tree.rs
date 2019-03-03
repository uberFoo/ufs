//! The Place that Blocks Live
//!
//! Merkle Tree Implementation for Block Validation
//!
//! I'm implementing Read and Write on this for now.  I don't think it'll live here long term.
use std::collections::VecDeque;

use bincode;
use ring::{self, digest};
use serde_derive::{Deserialize, Serialize};

use crate::block::{Block, BlockCardinality, BlockHash, BlockSizeType};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct BlockTree {
    byte_count: u64,
    //FIXME: This is a huge waste of space.  We should instead build a Vec<&LeafNode> when we lead
    // the tree.  Or build something that's useful when we validate block checksums, or something...
    block_list: Vec<LeafNode>,
    inner: BlockTreeNode,
}

impl BlockTree {
    pub(crate) fn deserialize_using<R>(reader: R) -> bincode::Result<Self>
    where
        R: std::io::Read,
    {
        bincode::deserialize_from(reader)
    }

    pub(crate) fn deserialize<T>(bytes: T) -> bincode::Result<Self>
    where
        T: AsRef<[u8]>,
    {
        bincode::deserialize_from(bytes.as_ref())
    }

    /// FIXME:
    ///  * should be hardened to second pre-image attack
    pub(crate) fn new(blocks: &Vec<Block>) -> Self {
        let mut inner_nodes = VecDeque::<BlockTreeNode>::new();
        let mut block_list = Vec::<LeafNode>::with_capacity(blocks.len());
        let mut byte_count = 0_u64;

        // Iterate over the Vec of blocks, pair-wise, in order to build inner nodes.  Those nodes
        // are added to a deque which will then be used to construct the rest of the tree.
        for pair in blocks.chunks(2) {
            let inner_block = match pair {
                [left, right] => {
                    let mut ctx = digest::Context::new(&digest::SHA256);
                    ctx.update(left.hash.unwrap().as_ref());
                    ctx.update(right.hash.unwrap().as_ref());

                    let left = LeafNode {
                        byte_count: left.byte_count,
                        block_number: left.number.unwrap(),
                        hash: left.hash.unwrap(),
                    };
                    let right = LeafNode {
                        byte_count: right.byte_count,
                        block_number: right.number.unwrap(),
                        hash: right.hash.unwrap(),
                    };

                    byte_count = byte_count + left.byte_count as u64 + right.byte_count as u64;

                    block_list.push(left.clone());
                    block_list.push(right.clone());

                    BlockTreeNode::Inner(Box::new(InnerNode {
                        child_count: 2,
                        left: BlockTreeNode::Leaf(Box::new(left)),
                        right: BlockTreeNode::Leaf(Box::new(right)),
                        hash: BlockHash::from(ctx.finish().as_ref()),
                    }))
                }
                [left] => {
                    let mut ctx = digest::Context::new(&digest::SHA256);
                    ctx.update(left.hash.unwrap().as_ref());
                    ctx.update(left.hash.unwrap().as_ref());

                    let left = LeafNode {
                        byte_count: left.byte_count,
                        block_number: left.number.unwrap(),
                        hash: left.hash.unwrap(),
                    };

                    byte_count += left.byte_count as u64;

                    block_list.push(left.clone());

                    BlockTreeNode::Inner(Box::new(InnerNode {
                        child_count: 1,
                        left: BlockTreeNode::Leaf(Box::new(left)),
                        right: BlockTreeNode::Empty,
                        hash: BlockHash::from(ctx.finish().as_ref()),
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
                    ctx.update(left.hash().unwrap().as_ref());
                    ctx.update(right.hash().unwrap().as_ref());

                    BlockTreeNode::Inner(Box::new(InnerNode {
                        child_count: left.child_count() + right.child_count(),
                        left,
                        right,
                        hash: BlockHash::from(ctx.finish().as_ref()),
                    }))
                }
                (Some(left), None) => {
                    let mut ctx = digest::Context::new(&digest::SHA256);
                    ctx.update(left.hash().unwrap().as_ref());
                    ctx.update(left.hash().unwrap().as_ref());

                    BlockTreeNode::Inner(Box::new(InnerNode {
                        child_count: left.child_count(),
                        left,
                        right: BlockTreeNode::Empty,
                        hash: BlockHash::from(ctx.finish().as_ref()),
                    }))
                }
                _ => unreachable!(),
            };

            // Note that the deque is a stack at this point, otherwise the nodes come out
            // backwards.  IOW, the leaves need to be in the same order as the Vec they arrived in,
            // which is their allocation order.  To maintain that constraint, the inner nodes, which
            // contain the leaf nodes, need to remain on the "left".
            inner_nodes.push_front(inner_node);
        }

        BlockTree {
            byte_count,
            block_list,
            inner: inner_nodes.pop_front().map_or(BlockTreeNode::Empty, |i| i),
        }
    }

    pub(crate) fn get(&self, n: BlockCardinality) -> Option<Block> {
        if let Some(leaf) = self.block_list.get(n as usize) {
            let block = Block::nasty_hack(leaf.block_number, leaf.byte_count, leaf.hash);
            Some(block)
        } else {
            None
        }
    }

    pub(crate) fn block_count(&self) -> BlockCardinality {
        self.inner.child_count()
    }

    pub(crate) fn size(&self) -> u64 {
        self.byte_count
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) enum BlockTreeNode {
    Empty,
    Inner(Box<InnerNode>),
    Leaf(Box<LeafNode>),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct InnerNode {
    child_count: BlockCardinality,
    left: BlockTreeNode,
    right: BlockTreeNode,
    hash: BlockHash,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct LeafNode {
    byte_count: BlockSizeType,
    block_number: BlockCardinality,
    hash: BlockHash,
}

impl BlockTreeNode {
    pub(crate) fn child_count(&self) -> BlockCardinality {
        match self {
            BlockTreeNode::Inner(n) => n.child_count,
            _ => 0,
        }
    }

    pub(crate) fn hash(&self) -> Option<&BlockHash> {
        match self {
            BlockTreeNode::Inner(n) => Some(&n.hash),
            BlockTreeNode::Leaf(n) => Some(&n.hash),
            BlockTreeNode::Empty => None,
        }
    }
}

#[cfg(test)]
mod test {
    use assert_matches::assert_matches;

    use super::*;

    #[test]
    fn one_block_tree() {
        let b0 = Block::new(0, Some(b"one block 0"));

        let block_list = vec![b0.clone()];

        let tree = BlockTree::new(&block_list);
        println!("one block tree: {:#?}", tree);

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b0.hash.unwrap().as_ref());
        ctx.update(b0.hash.unwrap().as_ref());

        assert_eq!(tree.size(), 11);
        assert_eq!(tree.block_count(), 1);

        let block = tree.get(0).unwrap();

        assert_eq!(block.number(), b0.number());
        assert_eq!(block.size(), b0.size());
        assert_eq!(block.hash(), b0.hash());

        assert_matches!(
            tree.inner,
            BlockTreeNode::Inner(node) => {
                assert_eq!(node.child_count, 1, "child count check");
                assert_matches!(
                    node.left,
                    BlockTreeNode::Leaf(node) => {
                        assert_eq!(node.block_number, 0, "block number check");
                        assert_eq!(node.byte_count, 11, "byte count check");
                        assert_eq!(node.hash, b0.hash.unwrap());
                    }
                );
                assert_eq!(node.right, BlockTreeNode::Empty);
                assert_eq!(node.hash.as_ref(), ctx.finish().as_ref());
            }
        );
    }

    #[test]
    fn two_block_tree() {
        let b0 = Block::new(20, Some(b"two block 0"));
        let b1 = Block::new(21, Some(b"two block 1*"));

        let block_list = vec![b0.clone(), b1.clone()];

        let tree = BlockTree::new(&block_list);
        println!("two block tree: {:#?}", tree);

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b0.hash.unwrap().as_ref());
        ctx.update(b1.hash.unwrap().as_ref());

        assert_eq!(tree.size(), 23);
        assert_eq!(tree.block_count(), 2);

        // quick-ish way to test each individual block
        block_list
            .iter()
            .enumerate()
            .map(|(i, b)| {
                let block = tree.get(i as u64).unwrap();
                assert_eq!(block.number(), b.number());
                assert_eq!(block.size(), b.size());
                assert_eq!(block.hash(), b.hash());
            })
            .for_each(|()| ());

        assert_matches!(
            tree.inner,
            BlockTreeNode::Inner(node) => {

                assert_eq!(node.child_count, 2, "child count check");
                assert_eq!(node.hash.as_ref(), ctx.finish().as_ref());
                assert_matches!(
                    node.left,
                    BlockTreeNode::Leaf(node) => {
                        assert_eq!(node.block_number, 20, "block number check");
                        assert_eq!(node.byte_count, 11, "byte count check");
                        assert_eq!(node.hash, b0.hash.unwrap());
                    }
                );
                assert_matches!(
                    node.right,
                    BlockTreeNode::Leaf(node) => {
                        assert_eq!(node.block_number, 21, "block number check");
                        assert_eq!(node.byte_count, 12, "byte count check");
                        assert_eq!(node.hash, b1.hash.unwrap());
                    }
                );
            }
        )
    }

    #[test]
    #[allow(clippy::cyclomatic_complexity)]
    fn five_block_tree() {
        // Weird block numbers to differentiate logical blocks from sequential blocks in the tree.
        let b0 = Block::new(0, Some(b"five block 0"));
        let b1 = Block::new(10, Some(b"five block 10"));
        let b2 = Block::new(200, Some(b"five block 200"));
        let b3 = Block::new(3000, Some(b"five block 3000"));
        let b4 = Block::new(40000, Some(b"five block 40000"));

        let block_list = vec![b0.clone(), b1.clone(), b2.clone(), b3.clone(), b4.clone()];

        let tree = BlockTree::new(&block_list);
        println!("five block tree: {:#?}", tree);

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b0.hash.unwrap().as_ref());
        ctx.update(b1.hash.unwrap().as_ref());
        let b01 = ctx.finish();

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b2.hash.unwrap().as_ref());
        ctx.update(b3.hash.unwrap().as_ref());
        let b23 = ctx.finish();

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b4.hash.unwrap().as_ref());
        ctx.update(b4.hash.unwrap().as_ref());
        let b44 = ctx.finish();

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b01.as_ref());
        ctx.update(b23.as_ref());
        let b0123 = ctx.finish();

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(b0123.as_ref());
        ctx.update(b44.as_ref());
        let b012344 = ctx.finish();

        assert_eq!(tree.size(), 70);
        assert_eq!(tree.block_count(), 5);

        // quick-ish way to test each individual block
        block_list
            .iter()
            .enumerate()
            .map(|(i, b)| {
                let block = tree.get(i as u64).unwrap();
                assert_eq!(block.number(), b.number());
                assert_eq!(block.size(), b.size());
                assert_eq!(block.hash(), b.hash());
            })
            .for_each(|()| ());

        assert_matches!(
            tree.inner,
            BlockTreeNode::Inner(node) => {

                assert_eq!(node.child_count, 5, "child count check");
                assert_eq!(node.hash.as_ref(), b012344.as_ref());

                assert_matches!(
                    node.left,
                    BlockTreeNode::Inner(node) => {
                        assert_matches!(
                            node.left,
                            BlockTreeNode::Inner(node) => {
                                assert_matches!(
                                    node.left,
                                    BlockTreeNode::Leaf(node) => {
                                        assert_eq!(node.block_number, 0, "block number check");
                                        assert_eq!(node.byte_count, 12, "byte count check");
                                        assert_eq!(node.hash, b0.hash.unwrap());
                                    }
                                );
                                assert_matches!(
                                    node.right,
                                    BlockTreeNode::Leaf(node) => {
                                        assert_eq!(node.block_number, 10, "block number check");
                                        assert_eq!(node.byte_count, 13, "byte count check");
                                        assert_eq!(node.hash, b1.hash.unwrap());
                                    }
                                );
                                assert_eq!(node.hash.as_ref(), b01.as_ref());
                            }
                        );
                        assert_matches!(
                            node.right,
                            BlockTreeNode::Inner(node) => {
                                assert_matches!(
                                    node.left,
                                    BlockTreeNode::Leaf(node) => {
                                        assert_eq!(node.byte_count, 14, "byte count check");
                                        assert_eq!(node.block_number, 200, "block number check");
                                        assert_eq!(node.hash, b2.hash.unwrap());
                                    }
                                );
                                assert_matches!(
                                    node.right,
                                    BlockTreeNode::Leaf(node) => {
                                        assert_eq!(node.byte_count, 15, "byte count check");
                                        assert_eq!(node.block_number, 3000, "block number check");
                                        assert_eq!(node.hash, b3.hash.unwrap());
                                    }
                                );
                                assert_eq!(node.hash.as_ref(), b23.as_ref());
                            }
                        );
                        assert_eq!(node.hash.as_ref(), b0123.as_ref());
                    }
                );
                assert_matches!(
                    node.right,
                    BlockTreeNode::Inner(node) => {
                        assert_eq!(node.hash.as_ref(), b44.as_ref());
                        assert_matches!(
                            node.left,
                            BlockTreeNode::Leaf(node) => {
                                assert_eq!(node.byte_count, 16, "byte count check");
                                assert_eq!(node.block_number, 40000, "block number check");
                                assert_eq!(node.hash, b4.hash.unwrap());
                            }
                        );
                        assert_matches!(
                            node.right,
                            BlockTreeNode::Empty
                        );
                    }
                );
            }
        )
    }
}
