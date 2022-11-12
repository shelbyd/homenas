use serde::{Deserialize, Serialize};

pub mod typed;
pub use typed::*;

use crate::io::*;

// #[deprecated]
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
}
