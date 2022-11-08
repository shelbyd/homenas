#![allow(dead_code)]

pub mod file_system;
pub use file_system::*;

pub mod memory;
pub use memory::*;

pub mod network;
pub use network::*;

pub mod typed;
pub use typed::*;

use crate::fs::{IoError, IoResult};

#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()>;
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>>;
    async fn clear(&self, key: &str) -> IoResult<()>;

    /// Currently does not support strong compare_exchange semantics.
    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<&[u8]>,
        new: &[u8],
    ) -> IoResult<bool>;
}

/// Update the value at the provided key. May retry until successful.
pub async fn update<R, F, O>(store: &O, key: &str, mut f: F) -> IoResult<R>
where
    O: ObjectStore,
    F: for<'v> FnMut(Option<&'v [u8]>) -> IoResult<(Vec<u8>, R)> + Send,
    R: Send,
{
    loop {
        let read = store.get(&key).await?;
        let read = read.as_ref().map(|vec| vec.as_slice());
        let (new, ret) = f(read)?;
        if store.compare_exchange(&key, read, &new).await? {
            return Ok(ret);
        }
    }
}

pub struct StorageOptions {
    replication: Replication,
    location: Option<Location>,
}

pub enum Replication {
    Saturate,
    None {
        banned_machines: Vec<MachineId>,
        banned_disks: Vec<DiskId>,
    },
}

pub struct MachineId(u64);
pub struct DiskId(u64);

pub struct Location {
    machine: MachineId,
    disk: DiskId,
}

#[async_trait::async_trait]
impl<'o, O> ObjectStore for &'o O
where
    O: ObjectStore,
{
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()> {
        (**self).set(key, value).await
    }
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        (**self).get(key).await
    }
    async fn clear(&self, key: &str) -> IoResult<()> {
        (**self).clear(key).await
    }

    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<&[u8]>,
        new: &[u8],
    ) -> IoResult<bool> {
        (**self).compare_exchange(key, current, new).await
    }
}

#[async_trait::async_trait]
impl<O> ObjectStore for std::sync::Arc<O>
where
    O: ObjectStore,
{
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()> {
        (**self).set(key, value).await
    }
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        (**self).get(key).await
    }
    async fn clear(&self, key: &str) -> IoResult<()> {
        (**self).clear(key).await
    }

    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<&[u8]>,
        new: &[u8],
    ) -> IoResult<bool> {
        (**self).compare_exchange(key, current, new).await
    }
}

#[async_trait::async_trait]
impl ObjectStore for Box<dyn ObjectStore> {
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()> {
        (**self).set(key, value).await
    }
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        (**self).get(key).await
    }
    async fn clear(&self, key: &str) -> IoResult<()> {
        (**self).clear(key).await
    }

    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<&[u8]>,
        new: &[u8],
    ) -> IoResult<bool> {
        (**self).compare_exchange(key, current, new).await
    }
}
