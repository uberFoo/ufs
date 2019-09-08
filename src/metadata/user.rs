//! User Data Blocks
//!
use std::collections::HashMap;

use {
    hmac::Hmac,
    rand::prelude::*,
    serde_derive::{Deserialize, Serialize},
    sha2::Sha256,
};

use crate::{
    block::{Block, BlockHash, BlockNumber, BlockReader, BlockWriter},
    uuid::UfsUuid,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct User {
    id: UfsUuid,
    key: [u8; 32],
    nonce: Vec<u8>,
}

impl User {
    pub(crate) fn new(user_name: &String, password: String) -> Self {
        let mut nonce = Vec::with_capacity(16);
        rand::thread_rng().fill_bytes(&mut nonce);

        let mut key = [0; 32];
        pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_bytes(), &nonce, 271828, &mut key);

        let id = UfsUuid::new_user(user_name);

        User { id, key, nonce }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct UserMetadata {
    inner: HashMap<String, User>,
}

impl UserMetadata {
    pub(crate) fn new() -> Self {
        UserMetadata {
            inner: HashMap::new(),
        }
    }

    pub(crate) fn new_user(&mut self, id: String, password: String) {
        let user = User::new(&id, password);
        self.inner.entry(id).or_insert(user);
    }

    pub(crate) fn get_users(&self) -> Vec<String> {
        self.inner.keys().cloned().collect()
    }
}
