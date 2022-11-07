#![allow(dead_code)]

pub mod memory;
pub use memory::*;

pub mod network;
pub use network::*;

#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    async fn set(&self, key: String, value: Vec<u8>);
    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    async fn compare_exchange(&self, key: &str, current: Option<Vec<u8>>, new: Vec<u8>) -> bool;
}

/// Update the value at the provided key. May retry until successful.
pub async fn update<R, F, O>(store: &O, key: &str, mut f: F) -> (Vec<u8>, R)
where
    O: ObjectStore,
    F: for<'v> FnMut(Option<&'v [u8]>) -> (Vec<u8>, R) + Send,
    R: Send,
{
    loop {
        let read = store.get(&key).await;
        let (new, ret) = f(read.as_ref().map(|vec| vec.as_slice()));
        if store.compare_exchange(&key, read, new.clone()).await {
            return (new, ret);
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
impl<O> ObjectStore for std::sync::Arc<O>
where
    O: ObjectStore,
{
    async fn set(&self, key: String, value: Vec<u8>) {
        (**self).set(key, value).await
    }
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        (**self).get(key).await
    }

    /// Currently does not implement actual compare_exchange semantics for all ObjectStores.
    async fn compare_exchange(&self, key: &str, current: Option<Vec<u8>>, new: Vec<u8>) -> bool {
        (**self).compare_exchange(key, current, new).await
    }
}
