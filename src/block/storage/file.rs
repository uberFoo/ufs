//! File-based Block Storage
//!
//! Blocks are stored as regular files.  The files are nested in directories as the following
//! example: block `f03da2` would be stored as `root_dir/f/0/3/d/a/2.ufsb`.
//!
//! ## FIXME
//! * It might be better to build a more shallow directory tree: `root_dir/f0/3d/a2.ufsb`?
//! * Optionally don't create files for every block.
use std::{
    fs, io,
    path::{Path, PathBuf},
};

use {
    failure::format_err,
    log::{debug, error, trace},
};

use crate::{
    block::{
        map::BlockMap, BlockCardinality, BlockNumber, BlockReader, BlockSize, BlockSizeType,
        BlockStorage, BlockWriter,
    },
    crypto::{decrypt, encrypt, make_fs_key},
    uuid::UfsUuid,
};

const BLOCK_EXT: &str = "ufsb";

/// Internal-only block writing implementation.
///
/// During bootstrapping we do metadata encryption at this level, rather than in the BlockManager.
/// This is primarily because we don't yet have a BlockManager!
///
struct FileWriter {
    key: [u8; 32],
    nonce: Vec<u8>,
    block_size: BlockSize,
    block_count: BlockCardinality,
    root_path: PathBuf,
}

impl BlockWriter for FileWriter {
    /// This exists because we need a means of bootstrapping the creation of metadata on a file-
    /// based block storage.
    fn write_block<T>(&mut self, bn: BlockNumber, data: T) -> Result<BlockSizeType, failure::Error>
    where
        T: AsRef<[u8]>,
    {
        let mut data = data.as_ref().to_vec();
        encrypt(
            &self.key,
            &self.nonce,
            bn * self.block_size as u64,
            &mut data,
        );

        if bn > self.block_count {
            Err(format_err!("request for bogus block {}", bn))
        } else {
            if data.len() > self.block_size as usize {
                return Err(format_err!("data is larger than block size"));
            }

            let path = path_for_block(&self.root_path, bn);
            fs::write(path, &data)?;

            debug!("wrote {} bytes to block 0x{:x?}", data.len(), bn);
            trace!("{:?}", data);
            Ok(data.len() as BlockSizeType)
        }
    }
}

/// Internal-only block reading implementation.
///
/// During bootstrapping we do metadata decryption at this level, rather than in the BlockManager.
/// This is primarily because we don't yet have a BlockManager!
///
struct FileReader {
    key: [u8; 32],
    nonce: Vec<u8>,
    block_size: BlockSize,
    root_path: PathBuf,
}

impl FileReader {
    pub(crate) fn new<P: AsRef<Path>>(key: [u8; 32], path: P) -> Self {
        let root_path: PathBuf = path.as_ref().into();

        // Note that the id of the file system is the last element in the path
        let id = UfsUuid::new_root_fs(root_path.file_name().unwrap().to_str().unwrap());
        let mut nonce = Vec::with_capacity(24);
        // FIXME: Is this nonce sufficient?
        nonce.extend_from_slice(&id.as_bytes()[..]);
        nonce.extend_from_slice(&id.as_bytes()[0..8]);

        // Infer the block size from the size of the 0-block file.
        let metadata = fs::metadata(path_for_block(&root_path, 0)).unwrap();

        FileReader {
            key,
            nonce,
            block_size: metadata.len().into(),
            root_path,
        }
    }
}

impl BlockReader for FileReader {
    /// This exists because we need a means of bootstrapping metadata from a file-based block
    /// storage. We aren't doing any sanity checking on the block number, or block size, since we
    /// don't yet have that information -- it's stored in the file system we are bootstrapping.
    fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, failure::Error> {
        let path = path_for_block(&self.root_path, bn);
        debug!("reading block from {:?}", path);
        let data = match fs::read(&path) {
            Ok(mut data) => {
                decrypt(
                    &self.key,
                    &self.nonce,
                    bn * self.block_size as u64,
                    &mut data,
                );
                data
            }
            Err(_) => {
                error!("error reading file {:?}", path);
                panic!();
            }
        };
        debug!("read {} bytes from block 0x{:x?}", data.len(), bn);
        trace!("{:?}", data);

        Ok(data)
    }
}

/// It'd be cool to impl From<BlockNumber> for PathBuf
fn path_for_block(root: &PathBuf, block: BlockNumber) -> PathBuf {
    let mut path = root.clone();
    let mut stack = vec![];
    let mut blk = block;
    if block < 0x10 {
        stack.push(block);
        stack.push(0);
    } else {
        while blk > 0x0 {
            let nibble = blk & 0xf;
            stack.push(nibble);
            blk >>= 4;
        }
    }

    while stack.len() > 0 {
        path.push(format!("{:x?}", stack.pop().unwrap()));
    }
    path.set_extension(BLOCK_EXT);
    trace!("path for block {:x?}: {:?}", block, path);
    path
}

/// File-based Block Storage
///
#[derive(Clone, Debug, PartialEq)]
pub struct FileStore {
    id: UfsUuid,
    key: [u8; 32],
    nonce: Vec<u8>,
    block_size: BlockSize,
    block_count: BlockCardinality,
    root_path: PathBuf,
    map: BlockMap,
}

impl FileStore {
    /// FileStore Constructor
    ///
    /// Note that block 0 is reserved to store block-level metadata.
    pub fn new<S, P>(password: S, path: P, mut map: BlockMap) -> Result<Self, failure::Error>
    where
        S: AsRef<str>,
        P: AsRef<Path>,
    {
        let root_path: PathBuf = path.as_ref().into();
        FileStore::init(&root_path, map.block_size(), map.block_count())?;

        let key = make_fs_key(password.as_ref(), &map.id());
        let mut nonce = Vec::with_capacity(24);
        // FIXME: Is this nonce sufficient?
        nonce.extend_from_slice(&map.id().as_bytes()[..]);
        nonce.extend_from_slice(&map.id().as_bytes()[0..8]);

        let mut writer = FileWriter {
            key,
            nonce,
            block_size: map.block_size(),
            block_count: map.block_count(),
            root_path: root_path.clone(),
        };

        map.serialize(&mut writer)?;

        Ok(FileStore {
            id: map.id().clone(),
            key,
            nonce: writer.nonce,
            block_size: map.block_size(),
            block_count: map.block_count(),
            root_path,
            map: map,
        })
    }

    /// Consistency Check
    ///
    /// FIXME: Actually check consistency?
    pub fn check<S, P>(password: S, path: P, show_map: bool) -> Result<(), failure::Error>
    where
        S: AsRef<str>,
        P: AsRef<Path>,
    {
        println!("Running consistency check on {:?}", path.as_ref());

        let key = make_fs_key(
            password.as_ref(),
            &UfsUuid::new_root_fs(
                path.as_ref()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .as_bytes(),
            ),
        );

        let fs = FileStore::load(key, path)?;

        println!("File-based Block Storage:");
        println!("\tID: {}", fs.id);
        println!("\tblock count: {}", fs.block_count);
        println!("\tblock size: {}", fs.block_size);
        println!("\tfree blocks: {}", fs.map.free_blocks().len());
        match fs.map.root_block() {
            Some(block) => println!("\troot block number: {}", block),
            None => (),
        };

        if show_map {
            println!("\nBlockMap Metadata:");
            println!("{:#?}", fs.map);
        }

        Ok(())
    }

    /// Construct Existing
    ///
    /// Load an existing file store from disk.
    pub fn load<P>(key: [u8; 32], path: P) -> Result<Self, failure::Error>
    where
        P: AsRef<Path>,
    {
        let root_path: PathBuf = path.as_ref().into();

        let reader = FileReader::new(key, &path);

        let map = match BlockMap::deserialize(&reader) {
            Ok(map) => map,
            Err(e) => {
                error!(
                    "Unable to load block map -- possibly incorrect master password?\nError: {}",
                    e
                );
                return Err(format_err!(
                    "Unable to load block map -- possibly incorrect master password?"
                ));
            }
        };

        Ok(FileStore {
            id: map.id().clone(),
            key: reader.key,
            nonce: reader.nonce,
            block_size: map.block_size(),
            block_count: map.block_count(),
            root_path,
            map,
        })
    }

    fn init(
        path: &PathBuf,
        size: BlockSize,
        count: BlockCardinality,
    ) -> Result<(), failure::Error> {
        debug!(
            "creating new file-based storage at {:?} with {} blocks having block size {:?}",
            path, count, size
        );
        /// Little function that calls itself to create the directories in which we store our
        /// blocks.  Note that it currently makes more directories than strictly necessary.  I just
        /// don't feel like adding (figuring out really) the additional logic to minimize things.
        fn make_dirs(root: &PathBuf, count: BlockCardinality) -> io::Result<()> {
            trace!("`make_dirs({:?}, {})", root, count);
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
        // to equal zero, and then divide that number by four.  This tells us how many nibbles there
        // are in the number, which is exactly the depth of our directory hierarchy.
        let mut n = 0;
        // count - 1 because we start counting at 0, i.e., the first block is block 0
        let mut b = count - 1;
        while b > 0 {
            b >>= 1;
            n += 1;
        }

        let depth = std::cmp::max(n >> 2, 1);
        make_dirs(&path, depth).unwrap();

        // Now allocate the blocks.
        for block in 0..count {
            let path = path_for_block(&path, block);
            trace!("creating block file {:x?}", block);
            fs::File::create(&path).unwrap_or_else(|e| {
                panic!(
                    "unable to create file {:?} for block {}: {}",
                    path, block, e
                )
            });
        }

        Ok(())
    }
}

impl BlockStorage for FileStore {
    fn id(&self) -> &UfsUuid {
        &self.id
    }

    fn commit_map(&mut self) {
        debug!("writing BlockMap");

        let mut writer = FileWriter {
            key: self.key,
            nonce: self.nonce.clone(),
            block_size: self.block_size,
            block_count: self.block_count,
            root_path: self.root_path.clone(),
        };

        debug!("dropping FileStore");
        match self.map.serialize(&mut writer) {
            Ok(_) => debug!("dropped FileStore"),
            Err(e) => error!("error dropping FileStore: {}", e),
        };
    }

    fn map(&self) -> &BlockMap {
        &self.map
    }

    fn map_mut(&mut self) -> &mut BlockMap {
        &mut self.map
    }

    fn block_count(&self) -> BlockCardinality {
        self.block_count
    }

    fn block_size(&self) -> BlockSize {
        self.block_size
    }
}

impl BlockWriter for FileStore {
    fn write_block<D>(&mut self, bn: BlockNumber, data: D) -> Result<BlockSizeType, failure::Error>
    where
        D: AsRef<[u8]>,
    {
        let mut data = data.as_ref().to_vec();
        encrypt(
            &self.key,
            &self.nonce,
            bn * self.block_size as u64,
            &mut data,
        );

        if bn > self.block_count {
            Err(format_err!("request for bogus block {}", bn))
        } else {
            if data.len() > self.block_size as usize {
                return Err(format_err!("data is larger than block size"));
            }

            let path = path_for_block(&self.root_path, bn);
            fs::write(path, &data)?;

            debug!("wrote {} bytes to block 0x{:x?}", data.len(), bn);
            trace!("{:?}", data);
            Ok(data.len() as BlockSizeType)
        }
    }
}

impl BlockReader for FileStore {
    fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, failure::Error> {
        if bn > self.block_count {
            Err(format_err!("request for bogus block {}", bn))
        } else {
            let path = path_for_block(&self.root_path, bn);
            debug!("reading block from {:?}", path);
            let data = match fs::read(&path) {
                Ok(mut data) => {
                    decrypt(
                        &self.key,
                        &self.nonce,
                        bn * self.block_size as u64,
                        &mut data,
                    );
                    data
                }
                Err(_) => {
                    error!("error reading file {:?}", path);
                    panic!();
                }
            };

            debug!("read {} bytes from block 0x{:x?}", data.len(), bn);
            trace!("{:?}", data);

            Ok(data)
        }
    }
}

#[cfg(test)]
mod test {
    use hex_literal::hex;

    use crate::UfsUuid;

    use super::*;

    const TEST_ROOT: &str = "/tmp/ufs_test/";

    #[test]
    fn bad_block_number() {
        let test_dir = [TEST_ROOT, "bad_block_number"].concat();
        let data = [0x0; BlockSize::FiveTwelve as usize];
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut fs = FileStore::new(
            "foobar",
            &test_dir,
            BlockMap::new(UfsUuid::new_root_fs("test"), BlockSize::FiveTwelve, 3),
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
            "foobar",
            &test_dir,
            BlockMap::new(UfsUuid::new_root_fs("test"), BlockSize::FiveTwelve, 0x10),
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
        let enciphered = hex!(
            "6a1a099ad6c2d71922c32c892fe694e47dd60dadde08e5f9393e41f1aa57534f39b72c0d5e08af1c3155564
            b247499a0327773baa0a4515ee18996b660c7d84b36aaf4d6b585cd0da20e1a383588d8e9040d8748746f121
            0a73c71107033efab4d23ebc841f3f738dfeaa1192d97ca2b8f7f49d100b8c785d3adb2c1a45d00c7b335c4c
            6d8296ca9550fe0c01254599bc499b1890cbd63462647bbc1075547011b3bf7"
        );

        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let mut fs = FileStore::new(
            "foobar",
            &test_dir,
            BlockMap::new(UfsUuid::new_root_fs("test"), BlockSize::FiveTwelve, 0x10),
        )
        .unwrap();

        let _ = fs.write_block(7, &data[..]).unwrap();

        let mut path = PathBuf::from(&test_dir);
        path.push("0");
        path.push("7");
        path.set_extension(BLOCK_EXT);
        assert_eq!(
            fs::read(path).unwrap(),
            &enciphered[..],
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
        let enciphered = hex!(
            "6a1a099ad6c2d71922c32c892fe694e47dd60dadde08e5f9393e41f1aa57534f39b72c0d5e08af1c3155564
            b247499a0327773baa0a4515ee18996b660c7d84b36aaf4d6b585cd0da20e1a383588d8e9040d8748746f121
            0a73c71107033efab4d23ebc841f3f738dfeaa1192d97ca2b8f7f49d100b8c785d3adb2c1a45d00c7b335c4c
            6d8296ca9550fe0c01254599bc499b1890cbd63462647bbc1075547011b3bf7"
        );

        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let fs = FileStore::new(
            "foobar",
            &test_dir,
            BlockMap::new(UfsUuid::new_root_fs("test"), BlockSize::FiveTwelve, 0x10),
        )
        .unwrap();

        // Manually write the block to the file system
        let mut path = PathBuf::from(&test_dir);
        path.push("0");
        path.push("7");
        path.set_extension(BLOCK_EXT);
        fs::write(path, &enciphered[..]).unwrap();

        assert_eq!(
            fs.read_block(7).unwrap(),
            &data[..],
            "write directly to block, and compare via the API"
        );
    }

    #[test]
    fn construction_sanity() {
        let test_dir = [TEST_ROOT, "construction_sanity"].concat();
        fs::remove_dir_all(&test_dir).unwrap_or_default();
        let fs = FileStore::new(
            "foobar",
            &test_dir,
            BlockMap::new(UfsUuid::new_root_fs("test"), BlockSize::FiveTwelve, 4),
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
