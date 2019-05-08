//! Network based Block Storage
//!
//! This is how we fetch blocks from the network.
//!
use failure::{format_err, Error};
use log::trace;
use reqwest::{get, header::CONTENT_TYPE, Client, IntoUrl, Url};

use crate::block::{
    storage::BlockStorage, BlockCardinality, BlockNumber, BlockSize, BlockSizeType,
};

struct NetworkStore {
    url: Url,
    client: Client,
}

impl NetworkStore {
    pub fn new<U: IntoUrl>(url: U) -> Result<Self, Error> {
        match url.into_url() {
            Ok(url) => {
                // FIXME: Actually enable gzip compression in te server...
                let client = Client::builder().gzip(true).build()?;
                Ok(NetworkStore { url, client })
            }
            Err(e) => Err(format_err!("Bad URL: {}", e)),
        }
    }
}

impl BlockStorage for NetworkStore {
    // FIXME:
    // This is clearly bogus. I'm wondering if this is something that is passed to the ctor, or if
    // it's something that is queried from the Block Server to which we are connecting?
    fn block_size(&self) -> BlockSize {
        BlockSize::TwentyFortyEight
    }

    // FIXME: See above.
    fn block_count(&self) -> BlockCardinality {
        100
    }

    fn write_block<T>(&mut self, bn: BlockNumber, data: T) -> Result<BlockSizeType, Error>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        trace!(
            "Writing {} bytes to block number {} at {}.",
            data.len(),
            bn,
            &self.url.as_str()
        );

        let mut url = self.url.clone();
        url.set_query(Some(&bn.to_string()));

        let mut resp = self
            .client
            .post(url.as_str())
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(data.to_vec())
            .send()?;

        match resp.text()?.parse::<BlockSizeType>() {
            Ok(bytes_written) => Ok(bytes_written),
            Err(e) => Err(format_err!("Could not parse result as BlockSize: {}", e)),
        }
    }

    fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, Error> {
        trace!("Reading block number {} from {}.", bn, &self.url.as_str());

        let mut url = self.url.clone();
        url.set_query(Some(&bn.to_string()));

        let mut resp = self.client.get(url.as_str()).send()?;
        let data = resp.text()?;
        Ok(data.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn read_and_write_block() {
        let mut bs = NetworkStore::new("http://localhost:8888/test").unwrap();
        let block_number = 88;
        let expected = r#"ion<BlockCardinality>,
   pub directory: HashMap<String, Block>,
}
```

Note that the above flies in the face of what was described above -- this is clearly not a
dictionary. Instead, it's legacy code that needs to be updated.
"#;

        let count = bs.write_block(block_number, expected).unwrap();
        assert_eq!(count, expected.len() as BlockSizeType);
        let data = bs.read_block(block_number).unwrap();
        assert_eq!(data, expected.as_bytes());
    }
}
