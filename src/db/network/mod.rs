use super::*;

use std::net::SocketAddr;

pub struct NetworkTree<T> {
    backing: T,
}

impl<T: Tree> NetworkTree<T> {
    pub fn create(backing: T, _listen_on: u16, _peers: &[SocketAddr]) -> anyhow::Result<Self> {
        Ok(NetworkTree { backing })
    }
}

#[async_trait::async_trait]
impl<T: Tree> Tree for NetworkTree<T> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        self.backing.get(key).await
    }

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        self.backing.set(key, value).await
    }

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>> {
        self.backing.compare_and_swap(key, old, new).await
    }
}
