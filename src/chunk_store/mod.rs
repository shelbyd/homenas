use std::{collections::HashSet, ops::Deref};

use crate::{io::*, object_store::*};

#[cfg(test)]
use crate::stores::*;

mod ref_count;
pub use ref_count::*;

mod striping;
pub use striping::*;

#[cfg(test)]
// TODO(shelbyd): Remove.
pub fn chunk_storage_key(id: &str) -> String {
    let location = if id.len() >= 5 {
        format!("{}/{}", &id[..4], &id[4..])
    } else {
        id.to_string()
    };
    format!("chunks/{}", location)
}

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
    P::Target: ChunkStore + Sized,
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

// #[deprecated]
#[derive(Debug)]
pub struct Direct<O> {
    // TODO(shelbyd): Not pub.
    pub backing: O,
    prefix: String,
}

impl<O> Direct<O> {
    pub fn new(backing: O, prefix: &str) -> Self {
        Direct {
            backing,
            prefix: prefix.trim_end_matches("/").to_string(),
        }
    }

    fn storage_key(&self, id: &str) -> String {
        let location = if id.len() >= 5 {
            format!("{}/{}", &id[..4], &id[4..])
        } else {
            id.to_string()
        };
        format!("{}/{}", &self.prefix, location)
    }
}

#[async_trait::async_trait]
impl<O> ChunkStore for Direct<O>
where
    O: ObjectStore,
{
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        log::debug!("{}: Reading", &id[..6]);
        let ok = self.backing.get(&self.storage_key(id)).await?;
        log::debug!("{}: Found", &id[..6]);
        Ok(ok)
    }
    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        let id = id_for(chunk);
        self.backing.set(&self.storage_key(&id), chunk).await?;
        Ok(id)
    }
    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        let id = id_for(chunk);
        self.backing
            .connect(location)
            .await?
            .set(&self.storage_key(&id), chunk)
            .await?;
        Ok(id)
    }
    async fn drop(&self, id: &str) -> IoResult<()> {
        self.backing.clear(&self.storage_key(id)).await
    }

    async fn locations(&self) -> IoResult<HashSet<Location>> {
        Ok(self.backing.locations().await?.into_iter().collect())
    }
}

#[cfg(test)]
pub fn memory_chunk_store() -> Direct<Memory> {
    Direct::new(Memory::default(), "chunks")
}
