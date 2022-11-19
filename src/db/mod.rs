use crate::io::*;

mod memory;
pub use memory::*;

mod multi;
pub use multi::*;

mod sled;
pub use self::sled::*;

mod typed;
pub use self::typed::*;

#[async_trait::async_trait]
pub trait Tree: Send + Sync {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>>;

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()>;

    async fn compare_and_swap(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError>>;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CompareAndSwapError {
    current: Option<Vec<u8>>,
}

#[async_trait::async_trait]
impl<P> Tree for P
where
    P: core::ops::Deref + Send + Sync,
    P::Target: Tree,
{
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        (**self).get(key).await
    }
    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        (**self).set(key, value).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError>> {
        (**self).compare_and_swap(key, old, new).await
    }
}
