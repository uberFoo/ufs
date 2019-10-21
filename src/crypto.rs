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

pub(crate) const ITERATION_COUNT: usize = 271828;

/// Generate a file system key
///
/// Given a password, and a UUID generate a key using HMAC-SHA256.
pub fn make_fs_key<S: AsRef<str>>(password: S, id: &UfsUuid) -> [u8; 32] {
    hash_password(password, id.as_bytes())
}

/// Encrypt a block of data
///
pub(crate) fn encrypt(key: &[u8], nonce: &Vec<u8>, offset: u64, mut data: &mut [u8]) {
    let mut cipher = XChaCha20::new_var(key, nonce).unwrap();
    cipher.seek(offset);
    cipher.apply_keystream(&mut data);
}

/// Encrypt a block of data
///
/// Note that this is exactly the same as encryption, but exists for symmetry.
pub(crate) fn decrypt(key: &[u8], nonce: &Vec<u8>, offset: u64, mut data: &mut [u8]) {
    let mut cipher = XChaCha20::new_var(key, nonce).unwrap();
    cipher.seek(offset);
    cipher.apply_keystream(&mut data);
}

pub(crate) fn hash_password<S: AsRef<str>, V: AsRef<[u8]>>(password: S, nonce: V) -> [u8; 32] {
    let mut key = [0; 32];
    pbkdf2::pbkdf2::<Hmac<Sha256>>(
        password.as_ref().as_bytes(),
        nonce.as_ref(),
        ITERATION_COUNT,
        &mut key,
    );
    key
}
