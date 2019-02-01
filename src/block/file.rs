//! File-based Block Storage
//!
//! Blocks are stored as regular files.  The files are nested in directories as the following
//! example: block `f03da2` would be stored as `root_dir/2/a/d/3/0/f.ufsb`.
//!
//! Currently all blocks are created, and filled with zeros when the store is initialized.
//! Additionally each partial block is padded with zeros before being written.  I was initially
//! thinking that the inode, knowing the file size, could trim the last block.  At this moment in
//! time, it seems like a bad idea...
//!
//! ## FIXME
//! * It might be better to build a more shallow directory tree: `root_dir/a2/3d/f0.ufsb`.
//! * Option for sparse initialization?
//! * Option for sparse blocks?
use failure::{format_err, Error};

use crate::block::{Block, BlockCardinality, BlockHash, BlockManager, BlockSize, BlockStorage};

use std::{
    collections::VecDeque,
    fmt, fs, io,
    path::{Path, PathBuf},
};

const BLOCK_EXT: &str = "ufsb";

/// File-based Block Storage
///
#[derive(Debug)]
pub struct FileStore {
    block_size: BlockSize,
    block_count: BlockCardinality,
    free_blocks: VecDeque<BlockCardinality>,
    root_path: PathBuf,
}

impl FileStore {
    /// FileStore Constructor
    ///
    pub fn new<P: AsRef<Path>>(
        path: P,
        size: BlockSize,
        count: BlockCardinality,
    ) -> Result<Self, Error> {
        // let p = path.as_ref();
        let root_path: PathBuf = path.as_ref().into();
        FileStore::init(&root_path, size, count)?;

        Ok(FileStore {
            block_size: size,
            block_count: count,
            free_blocks: (0..count).collect(),
            root_path,
        })
    }

    fn init(path: &PathBuf, size: BlockSize, count: BlockCardinality) -> Result<(), Error> {
        /// Little function that calls itself to create the directories in which we store our
        /// blocks.  Note that it currently makes more directories than strictly necessary.  I just
        /// don't feel like adding (figuring out really) the additional logic to minimize things.
        fn make_dirs(root: &PathBuf, count: BlockCardinality) -> io::Result<()> {
            if count > 0 {
                let count = count - 1;
                for i in 0x0..0x10 {
                    let mut path = root.clone();
                    path.push(fmt::format(format_args!("{:x?}", i)));
                    make_dirs(&path, count)?;
                }
            } else {
                fs::DirBuilder::new().recursive(true).create(&root)?;
            }
            Ok(())
        }

        // The trick here is to count how many times the number of blocks needs to be shifted right
        // to equal one, and then divide that number by four.  This tells us how many nibbles there
        // are in the number, which is exactly the depth of our directory hierarchy.
        let mut n = 0;
        let mut b = count - 1;
        while b > 1 {
            b >>= 1;
            n += 1;
        }

        let depth = n >> 2;
        make_dirs(&path, depth).unwrap();

        // Now allocate the blocks.
        // FIXME: We _could_ be lazy...  Maybe an option?
        let block_data = vec![0u8; size as usize];
        for block in 0..count {
            let path = FileStore::path_for_block(&path, block);
            fs::write(path, &block_data)?;
        }

        Ok(())
    }

    /// It'd be cool to impl From<BlockCardinality> for PathBuf
    fn path_for_block(root: &PathBuf, mut block: BlockCardinality) -> PathBuf {
        let mut path = root.clone();
        while block > 0xf {
            let nibble = block & 0xf;
            path.push(fmt::format(format_args!("{:x?}", nibble)));
            block >>= 4;
        }
        // Pulling this out of the loop avoids an issue with the `0` block.
        path.push(fmt::format(format_args!("{:x?}", block)));
        path.set_extension(BLOCK_EXT);
        path
    }
}

impl BlockStorage for FileStore {
    fn block_count(&self) -> BlockCardinality {
        self.block_count
    }

    fn block_size(&self) -> BlockSize {
        self.block_size
    }

    fn write_block(&mut self, bn: BlockCardinality, data: &[u8]) -> Result<Block, Error> {
        let mut zeroes = [0u8; BlockSize::TwentyFortyEight as usize];
        if bn > self.block_count {
            Err(format_err!("request for bogus block {}", bn))
        } else {
            if data.len() > self.block_size as usize {
                return Err(format_err!("data is larger than block size"));
            }

            let buffer = &mut zeroes[0..self.block_size as usize];

            buffer[..data.len()].copy_from_slice(&data[..]);

            let path = FileStore::path_for_block(&self.root_path, bn);
            fs::write(path, &buffer)?;

            Ok(Block {
                number: bn,
                hash: BlockHash::new(buffer),
            })
        }
    }

    fn read_block(&self, block: &Block) -> Result<Vec<u8>, Error> {
        if block.number > self.block_count {
            Err(format_err!("request for bogus block {}", block.number))
        } else {
            let path = FileStore::path_for_block(&self.root_path, block.number);
            let data = fs::read(path)?;
            let hash = BlockHash::new(&data);
            if block.hash == hash {
                Ok(data)
            } else {
                Err(format_err!(
                    "hash mismatch: expected {:?}, but calculated {:?}",
                    block.hash,
                    hash
                ))
            }
        }
    }
}

impl BlockManager for FileStore {
    fn free_block_count(&self) -> BlockCardinality {
        self.free_blocks.len() as BlockCardinality
    }

    fn get_free_block(&mut self) -> Option<BlockCardinality> {
        self.free_blocks.pop_front()
    }

    fn recycle_block(&mut self, block: BlockCardinality) {
        self.free_blocks.push_back(block);
    }
}

#[cfg(test)]
mod test {
    use hex_literal::{hex, hex_impl};

    use super::*;

    const TEST_ROOT: &str = "/tmp/ufs_test/";

    #[test]
    fn bad_block_number() {
        let test_dir = [TEST_ROOT, "bad_block_number"].concat();
        let data = [0x0; BlockSize::FiveTwelve as usize];
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut fs = FileStore::new(&test_dir, BlockSize::FiveTwelve, 3).unwrap();

        let block = Block {
            number: 7,
            hash: BlockHash::from(
                &hex!("62c2eacaf26c12f80eeb0b5b849c8805e0295db339dd793620190680799bec95")[..],
            ),
        };

        assert_eq!(
            fs.read_block(&block).is_err(),
            true,
            "read should fail with block number out of range"
        );
        assert_eq!(
            fs.write_block(7, &data).is_err(),
            true,
            "write should fail with block number out of range"
        );
    }

    #[test]
    fn block_too_bukoo() {
        let test_dir = [TEST_ROOT, "block_too_bukoo"].concat();
        let data = [0x42; BlockSize::TenTwentyFour as usize + 1];
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut fs = FileStore::new(&test_dir, BlockSize::TenTwentyFour, 0x10).unwrap();
        assert_eq!(fs.write_block(1, &data).is_err(), true);
    }

    #[test]
    fn write_block() {
        let test_dir = [TEST_ROOT, "write_block"].concat();
        let data = hex!(
            "451101250ec6f26652249d59dc974b7361d571a8101cdfd36aba3b5854d3ae086b5fdd4597721b66e3c0dc5
            d8c606d9657d0e323283a5217d1f53f2f284f57b85c8a61ac8924711f895c5ed90ef17745ed2d728abd22a5f
            7a13479a462d71b56c19a74a40b655c58edfe0a188ad2cf46cbf30524f65d423c837dd1ff2bf462ac4198007
            345bb44dbb7b1c861298cdf61982a833afc728fae1eda2f87aa2c9480858bec"
        );

        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut fs = FileStore::new(&test_dir, BlockSize::FiveTwelve, 0x10).unwrap();

        let mut data_block = vec![0x0; BlockSize::FiveTwelve as usize];
        data_block[..data.len()].copy_from_slice(&data[..]);

        let block = fs.write_block(7, &data[..]).unwrap();

        assert_eq!(block.number, 7);
        assert_eq!(
            block.hash.as_ref(),
            hex!("62c2eacaf26c12f80eeb0b5b849c8805e0295db339dd793620190680799bec95"),
            "validate hash"
        );

        let mut path = PathBuf::from(&test_dir);
        path.push("7");
        path.set_extension(BLOCK_EXT);
        assert_eq!(
            fs::read(path).unwrap(),
            data_block,
            "API write to block, and compare directly"
        );
    }

    #[test]
    fn read_block() {
        let test_dir = [TEST_ROOT, "read_block"].concat();
        let data = hex!(
            "451101250ec6f26652249d59dc974b7361d571a8101cdfd36aba3b5854d3ae086b5fdd4597721b66e3c0dc5
            d8c606d9657d0e323283a5217d1f53f2f284f57b85c8a61ac8924711f895c5ed90ef17745ed2d728abd22a5f
            7a13479a462d71b56c19a74a40b655c58edfe0a188ad2cf46cbf30524f65d423c837dd1ff2bf462ac4198007
            345bb44dbb7b1c861298cdf61982a833afc728fae1eda2f87aa2c9480858bec"
        );

        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let fs = FileStore::new(&test_dir, BlockSize::FiveTwelve, 0x10).unwrap();

        let mut expected_block = vec![0x0; BlockSize::FiveTwelve as usize];
        expected_block[..data.len()].copy_from_slice(&data[..]);

        let mut path = PathBuf::from(&test_dir);
        path.push("0");
        path.set_extension(BLOCK_EXT);
        fs::write(path, &expected_block).unwrap();

        let block = Block {
            number: 0,
            hash: BlockHash::from(
                &hex!("62c2eacaf26c12f80eeb0b5b849c8805e0295db339dd793620190680799bec95")[..],
            ),
        };

        assert_eq!(
            fs.read_block(&block).unwrap(),
            expected_block,
            "write directly to block, and compare via the API"
        );
    }

    #[test]
    fn read_block_bad_hash() {
        let test_dir = [TEST_ROOT, "read_block_bad_hash"].concat();
        let data = hex!(
            "451101250ec6f26652249d59dc974b7361d571a8101cdfd36aba3b5854d3ae086b5fdd4597721b66e3c0dc5
            d8c606d9657d0e323283a5217d1f53f2f284f57b85c8a61ac8924711f895c5ed90ef17745ed2d728abd22a5f
            7a13479a462d71b56c19a74a40b655c58edfe0a188ad2cf46cbf30524f65d423c837dd1ff2bf462ac4198007
            345bb44dbb7b1c861298cdf61982a833afc728fae1eda2f87aa2c9480858bec"
        );

        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let fs = FileStore::new(&test_dir, BlockSize::FiveTwelve, 0x10).unwrap();

        let mut expected_block = vec![0x0; BlockSize::FiveTwelve as usize];
        expected_block[..data.len()].copy_from_slice(&data[..]);

        // Corrupt the block data
        expected_block[0] = 0;

        let mut path = PathBuf::from(&test_dir);
        path.push("0");
        path.set_extension(BLOCK_EXT);
        fs::write(path, &expected_block).unwrap();

        let block = Block {
            number: 0,
            hash: BlockHash::new(&hex!(
                "62c2eacaf26c12f80eeb0b5b849c8805e0295db339dd793620190680799bec95"
            )),
        };

        assert_eq!(
            fs.read_block(&block).is_err(),
            true,
            "detect a hash mismatch"
        );
    }

    #[test]
    fn construction_sanity() {
        let test_dir = [TEST_ROOT, "construction_sanity"].concat();
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let fs = FileStore::new(&test_dir, BlockSize::FiveTwelve, 3).unwrap();
        assert_eq!(
            fs.free_block_count(),
            3,
            "verify that there are three free blocks"
        );
        assert_eq!(
            fs.block_size() as usize,
            512,
            "verify block size as 512 bytes"
        );
        assert_eq!(
            fs.block_count(),
            3,
            "verify that there are three blocks total"
        );
    }

    #[test]
    fn not_enough_free_blocks_error() {
        let test_dir = [TEST_ROOT, "not_enough_free_blocks_error"].concat();
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut bm = FileStore::new(&test_dir, BlockSize::FiveTwelve, 1).unwrap();
        let blocks = bm.write(&vec![0x0; 513][..]);
        assert_eq!(
            blocks.is_err(),
            true,
            "verify that more blocks are needed for write"
        );
    }

    #[test]
    fn tiny_test() {
        let test_dir = [TEST_ROOT, "tiny_test"].concat();
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut bm = FileStore::new(&test_dir, BlockSize::FiveTwelve, 1).unwrap();

        let blocks = bm.write(b"abc").unwrap();
        println!("{:#?}", blocks);

        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 1);
        assert_eq!(
            blocks[0].hash.as_ref(),
            hex!("b064446561934ed673ed230b6c0e68ebde7d574bf81288b00ac88ff6e518ade4"),
            "validate hash"
        );

        let mut expected = vec![0x0; 512];
        expected[0] = 97;
        expected[1] = 98;
        expected[2] = 99;
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &expected[..],
            "compare stored data with expected values"
        );
    }

    #[test]
    fn write_data_smaller_than_blocksize() {
        let test_dir = [TEST_ROOT, "write_data_smaller_than_blocksize"].concat();
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut bm = FileStore::new(&test_dir, BlockSize::FiveTwelve, 1).unwrap();

        let blocks = bm.write(&vec![0x38; 511][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 1);
        let mut expected = vec![0x38; 511];
        expected.push(0x0);
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &expected[..],
            "compare stored data with expected values"
        );
        println!("{:#?}", blocks);
        println!("{:?}", bm.read(&blocks));
    }

    #[test]
    fn write_data_larger_than_blocksize() {
        let test_dir = [TEST_ROOT, "write_data_larger_than_blocksize"].concat();
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut bm = FileStore::new(&test_dir, BlockSize::FiveTwelve, 2).unwrap();

        let blocks = bm.write(&vec![0x38; 513][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 2);
        let mut expected = vec![0x0; 1024];
        expected[..513].copy_from_slice(&vec![0x38; 513][..]);
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &expected[..],
            "compare stored data with expected values"
        );
        println!("{:#?}", blocks);
    }

    #[test]
    fn write_data_multiple_of_blocksize() {
        let test_dir = [TEST_ROOT, "write_data_multiple_of_blocksize"].concat();
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut bm = FileStore::new(&test_dir, BlockSize::FiveTwelve, 2).unwrap();

        let blocks = bm.write(&vec![0x38; 1024][..]).unwrap();
        assert_eq!(bm.free_block_count(), 0);
        assert_eq!(blocks.len(), 2);
        // let mut expected = vec![0x0; 1024];
        // expected[..513].copy_from_slice(&vec![0x38; 513][..]);
        assert_eq!(
            bm.read(&blocks).unwrap(),
            &vec![0x38; 1024][..],
            "compare stored data with expected values"
        );
        println!("{:#?}", blocks);
    }
}
