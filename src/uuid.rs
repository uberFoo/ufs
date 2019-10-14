use std::fmt;

use lazy_static::lazy_static;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

lazy_static! {
/// The UUID to rule them all
///
/// This is the main V5 uuid namespace from which all UUIDs in ufs are derived.
static ref FS_ROOT_UUID: Uuid = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"uberfoo.com");
static ref USER_ROOT_UUID: Uuid = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"user.uberfoo.com");
}

/// uberFS unique ID
///
/// The ID is a version 5 UUID wit it's base namespace as "uberfoo.com". New ID's are derived from
/// that root.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct UfsUuid {
    inner: Uuid,
}

impl UfsUuid {
    /// Create a new file system UfsUuid
    ///
    /// The UUID is generated based on the UFS FS ROOT UUID, and the supplied name.
    pub fn new_root_fs<N>(name: N) -> Self
    where
        N: AsRef<[u8]>,
    {
        UfsUuid {
            inner: Uuid::new_v5(&FS_ROOT_UUID, name.as_ref()),
        }
    }

    /// Create a new user UfsUuid
    ///
    /// The UUID is generated based on the UFS USER ROOT UUID, and the supplied name.
    pub fn new_user<N>(name: N) -> Self
    where
        N: AsRef<[u8]>,
    {
        UfsUuid {
            inner: Uuid::new_v5(&USER_ROOT_UUID, name.as_ref()),
        }
    }

    /// Create a new UfsUuid based on this one
    pub fn new<N>(&self, name: N) -> Self
    where
        N: AsRef<[u8]>,
    {
        UfsUuid {
            inner: Uuid::new_v5(&self.inner, name.as_ref()),
        }
    }

    /// Pass through function to return UUID as an array of bytes
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.inner.as_bytes()
    }

    /// Create a new random UfsUuid, under a namespace
    pub fn random(&self) -> Self {
        let rando_calrissian: String = thread_rng().sample_iter(&Alphanumeric).take(20).collect();
        UfsUuid {
            inner: Uuid::new_v5(&self.inner, rando_calrissian.as_bytes()),
        }
    }
}

impl AsRef<Uuid> for UfsUuid {
    fn as_ref(&self) -> &Uuid {
        &self.inner
    }
}

impl fmt::Display for UfsUuid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl From<String> for UfsUuid {
    fn from(str: String) -> Self {
        UfsUuid {
            inner: Uuid::parse_str(&str).expect("unable to parse Uuid from String"),
        }
    }
}
