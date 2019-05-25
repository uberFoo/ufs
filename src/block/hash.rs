use std::fmt;

use ring::digest;
use serde_derive::{Deserialize, Serialize};

#[derive(Copy, Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct BlockHash {
    inner: [u8; 32],
}

impl BlockHash {
    pub(in crate::block) fn new<T>(data: T) -> Self
    where
        T: AsRef<[u8]>,
    {
        BlockHash::from(digest::digest(&digest::SHA256, data.as_ref()).as_ref())
    }

    /// Validate a hash against a buffer of bytes
    ///
    /// # Examples
    /// ```
    /// let hash = BlockHash::new(b"uberfoo");
    /// assert_eq!(true, hash.validate(b"uberfoo"));
    /// ```
    pub(in crate::block) fn validate<T>(&self, data: T) -> bool
    where
        T: AsRef<[u8]>,
    {
        self == &BlockHash::new(data.as_ref())
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn name() {
        let hash = BlockHash::new(b"uberfoo");
        assert_eq!(true, hash.validate(b"uberfoo"));
    }
}
