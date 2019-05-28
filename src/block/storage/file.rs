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
//! * Optionally don't create files for every block.
use std::cell::RefCell;

use failure::{format_err, Error};
use log::{debug, trace};
use serde_derive::{Deserialize, Serialize};

use crate::block::{
    map::BlockMap,
    storage::{BlockReader, BlockStorage, BlockWriter},
    BlockCardinality, BlockNumber, BlockSize, BlockSizeType,
};

use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
};

const BLOCK_EXT: &str = "ufsb";

/// Internal-only block writing implementation.
///
struct FileWriter {
    block_size: BlockSize,
    block_count: BlockCardinality,
    root_path: PathBuf,
}

impl BlockWriter for FileWriter {
    /// This exists because we need a means of bootstrapping the creation of metadata on a file-
    /// based block storage.
    fn write_block<T>(&mut self, bn: BlockNumber, data: T) -> Result<BlockSizeType, Error>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();

        if bn > self.block_count {
            Err(format_err!("request for bogus block {}", bn))
        } else {
            if data.len() > self.block_size as usize {
                return Err(format_err!("data is larger than block size"));
            }

            let path = path_for_block(&self.root_path, bn);
            fs::write(path, data)?;

            debug!("wrote {} bytes to block 0x{:x?}", data.len(), bn);
            trace!("{:#?}", data);
            Ok(data.len() as BlockSizeType)
        }
    }
}

/// Internal-only block reading implementation.
///
struct FileReader {
    root_path: PathBuf,
}

impl BlockReader for FileReader {
    /// This exists because we need a means of loading metadata from a file-based block storage. We
    /// aren't doing any sanity checking on the block number, or block size, since we don't yet have
    ///  that information.
    fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, Error> {
        let path = path_for_block(&self.root_path, bn);
        debug!("reading block from {:?}", path);
        let data = fs::read(path)?;
        debug!("read {} bytes from block 0x{:x?}", data.len(), bn);
        trace!("{:#?}", data);

        Ok(data)
    }
}

/// It'd be cool to impl From<BlockNumber> for PathBuf
fn path_for_block(root: &PathBuf, block: BlockNumber) -> PathBuf {
    let mut path = root.clone();
    let mut stack = vec![];
    let mut blk = block;
    while blk > 0xf {
        let nibble = blk & 0xf;
        stack.push(nibble);
        blk >>= 4;
    }
    // Pulling this out of the loop avoids an issue with the `0` block.
    path.push(format!("{:x?}", blk));

    while stack.len() > 0 {
        path.push(format!("{:x?}", stack.pop().unwrap()));
    }
    path.set_extension(BLOCK_EXT);
    trace!("Path for block {:x?}: {:?}", block, path);
    path
}

/// File-based Block Storage
///
#[derive(Clone, Debug, PartialEq)]
pub struct FileStore {
    block_size: BlockSize,
    block_count: BlockCardinality,
    root_path: PathBuf,
    map: BlockMap,
}

impl FileStore {
    /// FileStore Constructor
    ///
    /// Note that block 0 is reserved to store block-level metadata.
    pub fn new<P>(path: P, mut map: BlockMap) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let root_path: PathBuf = path.as_ref().into();
        FileStore::init(&root_path, map.block_size(), map.block_count())?;

        let mut writer = FileWriter {
            block_size: map.block_size(),
            block_count: map.block_count(),
            root_path: root_path.clone(),
        };

        map.serialize(&mut writer)?;

        Ok(FileStore {
            block_size: map.block_size(),
            block_count: map.block_count(),
            root_path,
            map: map,
        })
    }

    /// Consistency Check
    ///
    /// FIXME: Actually check consistency?
    pub fn check<P>(path: P) -> Result<(), Error>
    where
        P: AsRef<Path>,
    {
        println!("Running consistency check on {:?}", path.as_ref());

        let fs = FileStore::load(path)?;
        println!("File-based Block Storage:");
        println!("\tblock count: {}", fs.block_count);
        println!("\tblock size: {}", fs.block_size);

        Ok(())
    }

    /// Construct Existing
    ///
    /// Load an existing file store from disk.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let root_path: PathBuf = path.as_ref().into();

        let reader = FileReader {
            root_path: root_path.clone(),
        };

        let metadata = BlockMap::deserialize(&reader)?;

        Ok(FileStore {
            block_size: metadata.block_size(),
            block_count: metadata.block_count(),
            root_path,
            map: metadata,
        })
    }

    fn init(path: &PathBuf, size: BlockSize, count: BlockCardinality) -> Result<(), Error> {
        debug!(
            "Creating new file-based storage at {:?} with {} blocks having block size {:?}.",
            path, count, size
        );
        /// Little function that calls itself to create the directories in which we store our
        /// blocks.  Note that it currently makes more directories than strictly necessary.  I just
        /// don't feel like adding (figuring out really) the additional logic to minimize things.
        fn make_dirs(root: &PathBuf, count: BlockCardinality) -> io::Result<()> {
            if count > 0 {
                let count = count - 1;
                for i in 0x0..0x10 {
                    let mut path = root.clone();
                    path.push(format!("{:x?}", i));
                    make_dirs(&path, count)?;
                }
            } else {
                trace!("creating directory {:?}", root);
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
        for block in 0..count {
            let path = path_for_block(&path, block);
            trace!("creating block file {:}", block);
            fs::File::create(path)
                .unwrap_or_else(|_| panic!("Unable to create file for block {}.", block));
        }

        Ok(())
    }
}

impl Drop for FileStore {
    fn drop(&mut self) {
        debug!("Writing Block Map");
        let mut writer = FileWriter {
            block_size: self.block_size,
            block_count: self.block_count,
            root_path: self.root_path.clone(),
        };

        debug!("Dropping FileStore");
        self.map.serialize(&mut writer);
    }
}

impl BlockStorage for FileStore {
    fn metadata(&self) -> &BlockMap {
        &self.map
    }

    fn metadata_mut(&mut self) -> &mut BlockMap {
        &mut self.map
    }

    fn block_count(&self) -> BlockCardinality {
        self.block_count
    }

    fn block_size(&self) -> BlockSize {
        self.block_size
    }
}

// impl<'a, T> BlockWriter for &'a mut T
// where
//     T: BlockWriter,
// {
//     fn write_block<D>(&mut self, bn: BlockNumber, data: D) -> Result<BlockSizeType, Error>
//     where
//         D: AsRef<[u8]>,
//     {
//         self.write_block(bn, data.as_ref())
//     }
// }

impl BlockWriter for FileStore {
    fn write_block<D>(&mut self, bn: BlockNumber, data: D) -> Result<BlockSizeType, Error>
    where
        D: AsRef<[u8]>,
    {
        let data = data.as_ref();

        // let mut zeroes = [0u8; BlockSize::TwentyFortyEight as usize];
        if bn > self.block_count {
            Err(format_err!("request for bogus block {}", bn))
        } else {
            if data.len() > self.block_size as usize {
                return Err(format_err!("data is larger than block size"));
            }

            let path = path_for_block(&self.root_path, bn);
            fs::write(path, data)?;

            debug!("wrote {} bytes to block 0x{:x?}", data.len(), bn);
            trace!("{:?}", data);
            Ok(data.len() as BlockSizeType)
        }
    }
}

impl BlockReader for FileStore {
    fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, Error> {
        if bn > self.block_count {
            Err(format_err!("request for bogus block {}", bn))
        } else {
            let path = path_for_block(&self.root_path, bn);
            debug!("reading block from {:?}", path);
            let data = fs::read(path)?;
            debug!("read {} bytes from block 0x{:x?}", data.len(), bn);
            trace!("{:?}", data);

            Ok(data)
        }
    }
}

#[cfg(test)]
mod test {
    use hex_literal::{hex, hex_impl};

    use crate::UfsUuid;

    use super::*;

    const TEST_ROOT: &str = "/tmp/ufs_test/";

    #[test]
    fn bad_block_number() {
        let test_dir = [TEST_ROOT, "bad_block_number"].concat();
        let data = [0x0; BlockSize::FiveTwelve as usize];
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut fs = FileStore::new(
            &test_dir,
            BlockMap::new(UfsUuid::new("test"), BlockSize::FiveTwelve, 3),
        )
        .unwrap();

        assert!(
            fs.read_block(7).is_err(),
            "read should fail with block number out of range"
        );
        assert!(
            fs.write_block(7, &data[..]).is_err(),
            "write should fail with block number out of range"
        );
    }

    #[test]
    fn block_too_bukoo() {
        let test_dir = [TEST_ROOT, "block_too_bukoo"].concat();
        let data = [0x42; BlockSize::TenTwentyFour as usize + 1];
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut fs = FileStore::new(
            &test_dir,
            BlockMap::new(UfsUuid::new("test"), BlockSize::FiveTwelve, 0x10),
        )
        .unwrap();
        assert!(fs.write_block(1, &data[..]).is_err());
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
        let mut fs = FileStore::new(
            &test_dir,
            BlockMap::new(UfsUuid::new("test"), BlockSize::FiveTwelve, 0x10),
        )
        .unwrap();

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
        let mut fs = FileStore::new(
            &test_dir,
            BlockMap::new(UfsUuid::new("test"), BlockSize::FiveTwelve, 0x10),
        )
        .unwrap();

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
        let mut fs = FileStore::new(
            &test_dir,
            BlockMap::new(UfsUuid::new("test"), BlockSize::FiveTwelve, 4),
        )
        .unwrap();

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
