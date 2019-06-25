//! Network based Block Storage
//!
//! This is how we fetch blocks from the network.
//!
use failure::format_err;
use log::trace;
use reqwest::{header::CONTENT_TYPE, Client, IntoUrl, Url};

use crate::block::{
    map::BlockMap, BlockCardinality, BlockNumber, BlockReader, BlockSize, BlockSizeType,
    BlockStorage, BlockWriter,
};

struct NetworkReader {
    url: Url,
    client: Client,
}

impl BlockReader for NetworkReader {
    fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, failure::Error> {
        trace!("Reading block number {} from {}.", bn, &self.url.as_str());

        let mut url = self.url.clone();
        url.set_query(Some(&bn.to_string()));

        let mut resp = self.client.get(url.as_str()).send()?;
        let data = resp.text()?;
        println!("WTF: {:?}", data);
        let wtf: Vec<u8> = data.clone().into();
        println!("WTF: {:?}", wtf);
        Ok(data.into())
    }
}

/// Network-based Block Storage
///
pub struct NetworkStore {
    url: Url,
    client: Client,
    block_size: BlockSize,
    block_count: BlockCardinality,
    map: BlockMap,
}

impl NetworkStore {
    pub fn new<U: IntoUrl>(url: U) -> Result<Self, failure::Error> {
        match url.into_url() {
            Ok(url) => {
                let client = Client::builder().gzip(true).build()?;

                let reader = NetworkReader {
                    url: url.clone(),
                    client: client.clone(),
                };
                let metadata = BlockMap::deserialize(&reader)?;

                Ok(NetworkStore {
                    url,
                    client,
                    block_size: metadata.block_size(),
                    block_count: metadata.block_count(),
                    map: metadata,
                })
            }
            Err(e) => Err(format_err!("Bad URL: {}", e)),
        }
    }
}

impl BlockStorage for NetworkStore {
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

impl BlockWriter for NetworkStore {
    fn write_block<T>(&mut self, bn: BlockNumber, data: T) -> Result<BlockSizeType, failure::Error>
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
}

impl BlockReader for NetworkStore {
    fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, failure::Error> {
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
