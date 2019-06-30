use lazy_static::lazy_static;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
lazy_static! {
/// The UUID to rule them all
///
/// This is the main V5 uuid namespace from which all UUIDs in ufs are derived.
static ref ROOT_UUID: Uuid = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"uberfoo.com");
}

/// uberFS unique ID
///
/// The ID is a version 5 UUID wit it's base namespace as "uberfoo.com". New ID's are derived from
/// that root.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct UfsUuid {
    inner: Uuid,
}

#[cfg(not(target_arch = "wasm32"))]
impl UfsUuid {
    /// Create a new UfsUuid
    ///
    /// The UUID is generated based on the UFS UUID ROOT, and the supplied name.
    pub fn new_root<N>(name: N) -> Self
    where
        N: AsRef<[u8]>,
    {
        UfsUuid {
            inner: Uuid::new_v5(&ROOT_UUID, name.as_ref()),
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

    /// Create a new random UfsUuid, under a namespace
    pub fn random(&self) -> Self {
        let rando_calrissian: String = thread_rng().sample_iter(&Alphanumeric).take(20).collect();
        UfsUuid {
            inner: Uuid::new_v5(&self.inner, rando_calrissian.as_bytes()),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<Uuid> for UfsUuid {
    fn as_ref(&self) -> &Uuid {
        &self.inner
    }
}
