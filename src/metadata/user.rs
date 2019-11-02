//! User Data Blocks
//!

use {
    crate::{
        crypto::{decrypt, encrypt, hash_password},
        uuid::UfsUuid,
    },
    log::debug,
    rand::prelude::*,
    serde_derive::{Deserialize, Serialize},
    std::collections::HashMap,
};

const VALIDATION_STRING: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const VALIDATION_NONCE: &[u8; 24] = b"abcdefghijklmnopqrstuvwx";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(in crate::metadata) struct User {
    id: UfsUuid,
    nonce: [u8; 16],
    validation: Vec<u8>,
}

impl User {
    pub(crate) fn new<S: AsRef<str>>(user_name: S, password: S) -> Self {
        let mut nonce: [u8; 16] = [0; 16];
        rand::thread_rng().fill_bytes(&mut nonce);

        let key = hash_password(password.as_ref(), &nonce);
        let mut validation = VALIDATION_STRING.to_owned().into_bytes();
        encrypt(&key, &VALIDATION_NONCE.to_vec(), 0, &mut validation);

        let id = UfsUuid::new_user(user_name.as_ref());

        User {
            id,
            nonce,
            validation: validation.to_vec(),
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

    pub(crate) fn get_user<S: AsRef<str>>(
        &self,
        id: S,
        password: S,
    ) -> Option<(UfsUuid, [u8; 32])> {
        debug!("*******");
        debug!("validate_user");

        match self.inner.get(id.as_ref()) {
            Some(u) => {
                let key = hash_password(password, &u.nonce);
                let mut validation = u.validation.clone();
                decrypt(&key, &VALIDATION_NONCE.to_vec(), 0, &mut validation);

                if validation == VALIDATION_STRING.as_bytes() {
                    Some((u.id, key))
                } else {
                    None
                }
            }
            None => None,
        }
    }
}
