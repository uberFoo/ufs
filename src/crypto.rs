///! Cryptographic Helpers, etc.
use {hmac::Hmac, sha2::Sha256};

use crate::uuid::UfsUuid;

pub fn make_fs_key(password: &str, id: &UfsUuid) -> [u8; 32] {
    let mut key = [0; 32];
    pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_bytes(), id.as_bytes(), 271828, &mut key);
    key
}
