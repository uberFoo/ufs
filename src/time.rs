#![cfg(not(target_arch = "wasm32"))]
use chrono::prelude::*;
use serde_derive::{Deserialize, Serialize};
use time::Timespec;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct UfsTime {
    inner: DateTime<Utc>,
}

impl UfsTime {
    pub fn now() -> Self {
        UfsTime { inner: Utc::now() }
    }
}

impl From<UfsTime> for Timespec {
    fn from(t: UfsTime) -> Self {
        Timespec {
            sec: t.inner.timestamp(),
            nsec: t.inner.timestamp_nanos() as i32,
        }
    }
}
