//! Network based Block Storage
//!
//! This is how we fetch blocks from the network.
//!
use {
    c2_chacha::{
        stream_cipher::{NewStreamCipher, SyncStreamCipher, SyncStreamCipherSeek},
        XChaCha20,
    },
    failure::format_err,
    log::{debug, error, trace},
    reqwest::{header::CONTENT_TYPE, Client, IntoUrl, Url},
};

use crate::{
    block::{
        map::BlockMap, BlockCardinality, BlockNumber, BlockReader, BlockSize, BlockSizeType,
        BlockStorage, BlockWriter,
    },
    uuid::UfsUuid,
};

/// Network-based Block Storage
///
pub struct NetworkStore {
    id: UfsUuid,
    key: [u8; 32],
    nonce: Vec<u8>,
    url: Url,
    client: Client,
    block_size: BlockSize,
    block_count: BlockCardinality,
    map: BlockMap,
}

impl NetworkStore {
    pub fn new<S, U>(key: [u8; 32], name: S, url: U) -> Result<Self, failure::Error>
    where
        S: AsRef<str>,
        U: IntoUrl,
    {
        match url.into_url() {
            Ok(u) => {
                let url = u.join(name.as_ref())?;
                let client = Client::builder().gzip(true).build()?;

                // Note that the id of the file system is the last element in the path
                let id = UfsUuid::new_root(name.as_ref());
                let mut nonce = Vec::with_capacity(24);
                /// FIXME: Is this nonce sufficient?
                nonce.extend_from_slice(&id.as_bytes()[..]);
                nonce.extend_from_slice(&id.as_bytes()[0..8]);

                println!("key: {:?}\nnonce: {:?}", key, nonce);

                let mut reader = NetworkReader {
                    key,
                    nonce,
                    url: url.clone(),
                    client: client.clone(),
                };

                println!("foo");
                let metadata = BlockMap::deserialize(&mut reader)?;
                println!("bar");

                Ok(NetworkStore {
                    id: metadata.id().clone(),
                    key,
                    nonce: reader.nonce,
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
    fn id(&self) -> &UfsUuid {
        &self.id
    }

    fn commit_map(&mut self) {
        debug!("writing BlockMap");

        let mut writer = NetworkWriter {
            key: self.key,
            nonce: self.nonce.clone(),
            url: self.url.clone(),
            client: self.client.clone(),
        };

        debug!("dropping NetworkStore");
        match self.map.serialize(&mut writer) {
            Ok(_) => debug!("dropped NetworkStore"),
            Err(e) => error!("error dropping NetworkStore: {}", e),
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

impl BlockWriter for NetworkStore {
    fn write_block<T>(&mut self, bn: BlockNumber, data: T) -> Result<BlockSizeType, failure::Error>
    where
        T: AsRef<[u8]>,
    {
        let mut cipher = XChaCha20::new_var(&self.key, &self.nonce).unwrap();
        cipher.seek(bn * self.block_size as u64);

        let mut data = data.as_ref().to_vec();
        // cipher.apply_keystream(&mut data);

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

        // debug!("block: {}, bytes:\n{:?}", bn, data);

        match resp.text()?.parse::<BlockSizeType>() {
            Ok(bytes_written) => Ok(bytes_written),
            Err(e) => Err(format_err!("Could not parse result as BlockSize: {}", e)),
        }
    }
}

impl BlockReader for NetworkStore {
    fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, failure::Error> {
        trace!("Reading block number {} from {}.", bn, &self.url.as_str());

        println!("key: {:?}\nnonce: {:?}", self.key, self.nonce);

        let mut cipher = XChaCha20::new_var(&self.key, &self.nonce).unwrap();
        cipher.seek(bn * self.block_size as u64);

        let mut url = self.url.clone();
        url.set_query(Some(&bn.to_string()));

        let mut resp = self.client.get(url.as_str()).send()?;
        let mut data: Vec<u8> = vec![];
        resp.copy_to(&mut data)?;

        // cipher.apply_keystream(&mut data);

        Ok(data)
    }
}

struct NetworkWriter {
    key: [u8; 32],
    nonce: Vec<u8>,
    url: Url,
    client: Client,
}

impl BlockWriter for NetworkWriter {
    fn write_block<T>(&mut self, bn: BlockNumber, data: T) -> Result<BlockSizeType, failure::Error>
    where
        T: AsRef<[u8]>,
    {
        let mut cipher = XChaCha20::new_var(&self.key, &self.nonce).unwrap();
        /// FIXME: This should be pulled from the server, but I don't want to implement it, because
        /// I think the server needs to be reimplemented differently.
        cipher.seek(bn * 2048);

        let mut data = data.as_ref().to_vec();
        // cipher.apply_keystream(&mut data);

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

struct NetworkReader {
    key: [u8; 32],
    nonce: Vec<u8>,
    url: Url,
    client: Client,
}

impl BlockReader for NetworkReader {
    fn read_block(&self, bn: BlockNumber) -> Result<Vec<u8>, failure::Error> {
        trace!("Reading block number {} from {}.", bn, &self.url.as_str());

        println!("key: {:?}\nnonce: {:?}", self.key, self.nonce);

        let mut cipher = XChaCha20::new_var(&self.key, &self.nonce).unwrap();
        /// FIXME: This should be pulled from the server, but I don't want to implement it, because
        /// I think the server needs to be reimplemented differently.
        cipher.seek(bn * 2048);

        let mut url = self.url.clone();
        url.set_query(Some(&bn.to_string()));

        let mut resp = self.client.get(url.as_str()).send()?;
        let mut data: Vec<u8> = vec![];
        resp.copy_to(&mut data)?;
        // cipher.apply_keystream(&mut data);

        Ok(data)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::crypto::make_fs_key;

    #[test]
    fn read_and_write_block() {
        let key = make_fs_key("", &UfsUuid::new_root("test"));
        let mut bs = NetworkStore::new(key, "test", "http://localhost:8888").unwrap();
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
