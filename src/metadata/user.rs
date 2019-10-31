//! User Data Blocks
//!

use {
    crate::{crypto::hash_password, uuid::UfsUuid},
    log::debug,
    rand::prelude::*,
    serde_derive::{Deserialize, Serialize},
    std::collections::HashMap,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(in crate::metadata) struct User {
    id: UfsUuid,
    nonce: [u8; 16],
}

impl User {
    pub(crate) fn new<S: AsRef<str>>(user_name: S) -> Self {
        let mut nonce: [u8; 16] = [0; 16];
        rand::thread_rng().fill_bytes(&mut nonce);

        let id = UfsUuid::new_user(user_name.as_ref());

        User { id, nonce }
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

    pub(crate) fn new_user(&mut self, id: String) {
        let user = User::new(&id);
        self.inner.entry(id).or_insert(user);
    }

    pub(crate) fn get_users(&self) -> Vec<String> {
        self.inner.keys().cloned().collect()
    }

    pub(crate) fn get_user<S: AsRef<str>>(
        &self,
        id: S,
        password: S,
    ) -> Option<(UfsUuid, [u8; 32])> {
        debug!("*******");
        debug!("validate_user");

        match self.inner.get(id.as_ref()) {
            Some(u) => Some((u.id, hash_password(password, &u.nonce))),
            None => None,
        }
    }
}
