//! User Data Blocks
//!
use std::collections::HashMap;

use {
    hmac::Hmac,
    log::{debug, error},
    rand::prelude::*,
    serde_derive::{Deserialize, Serialize},
    sha2::Sha256,
};

use crate::uuid::UfsUuid;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(in crate::metadata) struct User {
    id: UfsUuid,
    key: [u8; 32],
    nonce: Vec<u8>,
}

impl User {
    pub(crate) fn new<S: AsRef<str>>(user_name: S, password: S) -> Self {
        let mut nonce = Vec::with_capacity(16);
        rand::thread_rng().fill_bytes(&mut nonce);

        let mut key = [0; 32];
        pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_ref().as_bytes(), &nonce, 271828, &mut key);

        let id = UfsUuid::new_user(user_name.as_ref());

        User { id, key, nonce }
    }

    pub(crate) fn validate<S: AsRef<str>>(&self, password: S) -> Option<[u8; 32]> {
        debug!("*******");
        debug!("validate");
        let mut key = [0; 32];
        pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_ref().as_bytes(), &self.nonce, 271828, &mut key);

        if key == self.key {
            Some(self.key)
        } else {
            error!("Mismatched keys.");
            None
        }
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
        let user = User::new(&id, &password);
        self.inner.entry(id).or_insert(user);
    }

    pub(crate) fn get_users(&self) -> Vec<String> {
        self.inner.keys().cloned().collect()
    }

    pub(crate) fn validate_user<S: AsRef<str>>(
        &self,
        id: S,
        password: S,
    ) -> Option<(UfsUuid, [u8; 32])> {
        debug!("*******");
        debug!("validate_user");

        match self.inner.get(id.as_ref()) {
            Some(u) => match u.validate(password.as_ref()) {
                Some(key) => Some((u.id, key)),
                None => None,
            },
            None => None,
        }
    }
}
