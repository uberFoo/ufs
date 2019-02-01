use std::fmt;

use ring::digest;

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct BlockHash {
    inner: [u8; 32],
}

impl BlockHash {
    pub(crate) fn new(data: &[u8]) -> Self {
        BlockHash::from(digest::digest(&digest::SHA256, &data[..]).as_ref())
    }
}

impl AsRef<[u8]> for BlockHash {
    fn as_ref(&self) -> &[u8] {
        &self.inner
    }
}

impl From<&[u8]> for BlockHash {
    fn from(data: &[u8]) -> Self {
        let mut hash: [u8; 32] = [0; 32];
        hash.copy_from_slice(data);
        BlockHash { inner: hash }
    }
}

impl fmt::Debug for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in &self.inner {
            write!(f, "{:02x}", i)?;
        }
        // write!(f, "{:?}", self.0);
        Ok(())
    }
}
