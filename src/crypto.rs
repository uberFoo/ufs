///! Cryptographic Helpers, etc.
use {
    c2_chacha::{
        stream_cipher::{NewStreamCipher, SyncStreamCipher, SyncStreamCipherSeek},
        XChaCha20,
    },
    hmac::Hmac,
    sha2::Sha256,
};

use crate::uuid::UfsUuid;

/// Generate a file system key
///
/// Given a password, and a UUID generate a key using HMAC-SHA256.
pub fn make_fs_key(password: &str, id: &UfsUuid) -> [u8; 32] {
    let mut key = [0; 32];
    pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_bytes(), id.as_bytes(), 271828, &mut key);
    key
}

/// Encrypt a block of data
///
pub fn encrypt(key: &[u8], nonce: &Vec<u8>, offset: u64, mut data: &mut [u8]) {
    let mut cipher = XChaCha20::new_var(key, nonce).unwrap();
    cipher.seek(offset);
    cipher.apply_keystream(&mut data);
}

/// Encrypt a block of data
///
/// Note that this is exactly the same as encryption, but exists for symmetry.
pub fn decrypt(key: &[u8], nonce: &Vec<u8>, offset: u64, mut data: &mut [u8]) {
    let mut cipher = XChaCha20::new_var(key, nonce).unwrap();
    cipher.seek(offset);
    cipher.apply_keystream(&mut data);
}
