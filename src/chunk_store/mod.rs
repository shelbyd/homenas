use std::{collections::HashSet, ops::Deref};

use crate::{io::*, object_store::*};

#[cfg(test)]
use crate::stores::*;

mod file_system;
pub use file_system::*;

mod memory;
pub use memory::*;

mod multi;
pub use multi::{Multi, *};

mod ref_count;
pub use ref_count::*;

mod striping;
pub use striping::*;

pub fn id_for(chunk: &[u8]) -> String {
    hex::encode(blake3::hash(chunk).as_bytes())
}

#[async_trait::async_trait]
pub trait ChunkStore: Send + Sync {
    async fn read(&self, id: &str) -> IoResult<Vec<u8>>;

    // TODO(shelbyd): Provide id alongside chunk.
    async fn store(&self, chunk: &[u8]) -> IoResult<String>;
    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String>;

    async fn drop(&self, id: &str) -> IoResult<()>;

    async fn locations(&self) -> IoResult<HashSet<Location>>;
}

#[async_trait::async_trait]
impl<P> ChunkStore for P
where
    P: Deref + Send + Sync,
    P::Target: ChunkStore,
{
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        (**self).read(id).await
    }
    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        (**self).store(chunk).await
    }
    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        (**self).store_at(chunk, location).await
    }
    async fn drop(&self, id: &str) -> IoResult<()> {
        (**self).drop(id).await
    }
    async fn locations(&self) -> IoResult<HashSet<Location>> {
        (**self).locations().await
    }
}

#[cfg(test)]
pub fn memory_chunk_store() -> MemChunkStore {
    MemChunkStore::default()
}
