use serde::{Deserialize, Serialize};

pub mod typed;
pub use typed::*;

use crate::io::*;

#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()>;
    async fn get(&self, key: &str) -> IoResult<Vec<u8>>;
    async fn clear(&self, key: &str) -> IoResult<()>;

    /// Currently does not support strong compare_exchange semantics.
    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<&[u8]>,
        new: &[u8],
    ) -> IoResult<bool>;

    async fn locations(&self) -> IoResult<Vec<Location>>;
    async fn connect(&self, location: &Location) -> IoResult<Box<dyn ObjectStore + '_>>;
}

/// Update the value at the provided key. May retry until successful.
pub async fn update<R, F, O>(store: &O, key: &str, mut f: F) -> IoResult<R>
where
    O: ObjectStore,
    F: for<'v> FnMut(Option<&'v [u8]>) -> IoResult<(Vec<u8>, R)> + Send,
    R: Send,
{
    loop {
        let read = store.get(&key).await.into_found()?;
        let read = read.as_ref().map(|vec| vec.as_slice());
        let (new, ret) = f(read)?;
        if store.compare_exchange(&key, read, &new).await? {
            return Ok(ret);
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum Location {
    Memory(u64),
    Directory(u64),
}

pub trait ResultExt<T> {
    fn into_found(self) -> Result<Option<T>, IoError>;
}

impl<T> ResultExt<T> for Result<T, IoError> {
    fn into_found(self) -> Result<Option<T>, IoError> {
        match self {
            Ok(t) => Ok(Some(t)),
            Err(IoError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl<P> ObjectStore for P
where
    P: std::ops::Deref + Send + Sync,
    P::Target: ObjectStore,
{
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()> {
        (**self).set(key, value).await
    }
    async fn get(&self, key: &str) -> IoResult<Vec<u8>> {
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

    async fn locations(&self) -> IoResult<Vec<Location>> {
        (**self).locations().await
    }
    async fn connect(&self, location: &Location) -> IoResult<Box<dyn ObjectStore + '_>> {
        (**self).connect(location).await
    }
}
