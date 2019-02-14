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

use crate::block::{
    meta::BlockMetadata, storage::BlockStorage, BlockCardinality, BlockSize, BlockSizeType,
};

use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
};

const BLOCK_EXT: &str = "ufsb";

/// File-based Block Storage
///
#[derive(Debug, PartialEq)]
pub struct FileStore {
    block_size: BlockSize,
    block_count: BlockCardinality,
    root_path: PathBuf,
}

impl FileStore {
    /// FileStore Constructor
    ///
    /// Note that block 0 is reserved to store block-level metadata.
    pub fn new<P, BS>(path: P, size: BS, count: BlockCardinality) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        BS: Into<BlockSize>,
    {
        let root_path: PathBuf = path.as_ref().into();
        let size = size.into();
        FileStore::init(&root_path, size, count)?;

        Ok(FileStore {
            block_size: size,
            block_count: count,
            root_path,
        })
    }

    pub(crate) fn load<P: AsRef<Path>>(path: P) -> Result<(Self, BlockMetadata), Error> {
        let root_path: PathBuf = path.as_ref().into();
        let path = FileStore::path_for_block(&root_path, 0);

        let metadata = BlockMetadata::deserialize(fs::read(path)?)?;

        Ok((
            FileStore {
                block_size: metadata.size,
                block_count: metadata.count,
                root_path,
            },
            metadata,
        ))
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
        let block_data = vec![0u8];
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

    fn write_block<T>(&mut self, bn: BlockCardinality, data: T) -> Result<BlockSizeType, Error>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();

        // let mut zeroes = [0u8; BlockSize::TwentyFortyEight as usize];
        if bn > self.block_count {
            Err(format_err!("request for bogus block {}", bn))
        } else {
            if data.len() > self.block_size as usize {
                return Err(format_err!("data is larger than block size"));
            }

            let path = FileStore::path_for_block(&self.root_path, bn);
            fs::write(path, data);

            Ok(data.len() as BlockSizeType)
        }
    }

    fn read_block(&self, bn: BlockCardinality) -> Result<Vec<u8>, Error> {
        if bn > self.block_count {
            Err(format_err!("request for bogus block {}", bn))
        } else {
            let path = FileStore::path_for_block(&self.root_path, bn);
            let data = fs::read(path)?;
            Ok(data)
        }
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

        assert_eq!(
            fs.read_block(7).is_err(),
            true,
            "read should fail with block number out of range"
        );
        assert_eq!(
            fs.write_block(7, &data[..]).is_err(),
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
        assert_eq!(fs.write_block(1, &data[..]).is_err(), true);
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

        let block = fs.write_block(7, &data[..]).unwrap();

        let mut path = PathBuf::from(&test_dir);
        path.push("7");
        path.set_extension(BLOCK_EXT);
        assert_eq!(
            fs::read(path).unwrap(),
            &data[..],
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

        assert_eq!(
            fs.read_block(0).unwrap(),
            expected_block,
            "write directly to block, and compare via the API"
        );
    }

    #[test]
    fn construction_sanity() {
        let test_dir = [TEST_ROOT, "construction_sanity"].concat();
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let fs = FileStore::new(&test_dir, BlockSize::FiveTwelve, 4).unwrap();
        assert_eq!(
            fs.block_size() as usize,
            512,
            "verify block size as 512 bytes"
        );
        assert_eq!(
            fs.block_count(),
            4,
            "verify that there are four blocks total"
        );
    }
}
