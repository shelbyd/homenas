use crate::io::*;

mod memory;
pub use memory::*;

mod multi;
pub use multi::*;

mod network;
pub use network::*;

mod sled;
pub use self::sled::*;

mod typed;
pub use self::typed::*;

#[async_trait::async_trait]
pub trait Tree: Send + Sync {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()>;

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>>;
}

#[allow(unused)]
pub struct CompareAndSwapError<'p> {
    current: Option<Vec<u8>>,
    proposed: Option<&'p [u8]>,
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

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>> {
        (**self).compare_and_swap(key, old, new).await
    }
}
