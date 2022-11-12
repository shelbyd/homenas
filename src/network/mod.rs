use crate::{chunk_store::*, db::*, io::*};

use std::{net::SocketAddr, path::Path};

pub struct NetworkStore<T, C> {
    backing_tree: T,
    backing_chunks: C,
}

impl<T: Tree, C: ChunkStore> NetworkStore<T, C> {
    pub fn create(
        backing_tree: T,
        backing_chunks: C,
        _listen_on: u16,
        _peers: &[SocketAddr],
        _state_dir: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        Ok(NetworkStore {
            backing_tree,
            backing_chunks,
        })
    }
}

#[async_trait::async_trait]
impl<T: Tree, C: ChunkStore> Tree for NetworkStore<T, C> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        log::info!("get: {:?}", key);
        self.backing_tree.get(key).await
    }

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        log::info!("set: {:?}", key);
        self.backing_tree.set(key, value).await
    }

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>> {
        log::info!("compare_and_swap: {:?}", key);
        self.backing_tree.compare_and_swap(key, old, new).await
    }
}

#[async_trait::async_trait]
impl<T: Tree, C: ChunkStore> ChunkStore for NetworkStore<T, C> {
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        self.backing_chunks.read(id).await
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        self.backing_chunks.store(chunk).await
    }

    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        self.backing_chunks.store_at(chunk, location).await
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        self.backing_chunks.drop(id).await
    }

    async fn locations(&self) -> IoResult<HashSet<Location>> {
        self.backing_chunks.locations().await
    }
}
