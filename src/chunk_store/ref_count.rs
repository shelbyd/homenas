use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::*;
use crate::object_store::*;

pub struct RefCount<O> {
    backing: O,
    meta_key: String,
}

#[derive(Serialize, Deserialize, Default)]
struct ChunkMeta {
    counts: HashMap<String, u64>,
}

pub fn chunk_storage_key(id: &str) -> String {
    let location = if id.len() >= 5 {
        format!("{}/{}", &id[..4], &id[4..])
    } else {
        id.to_string()
    };
    format!("chunks/{}", location)
}

impl<O> RefCount<O> {
    pub fn new(backing: O, meta_key: &str) -> Self {
        Self {
            backing,
            meta_key: meta_key.to_string(),
        }
    }
}

impl<O: ObjectStore> RefCount<O> {
    async fn update_meta<R: Send>(
        &self,
        _id: &str,
        mut f: impl FnMut(&mut ChunkMeta) -> R + Send,
    ) -> IoResult<R> {
        update_typed(&self.backing, &self.meta_key, |meta: Option<ChunkMeta>| {
            let mut meta = meta.unwrap_or_default();

            let r = f(&mut meta);

            Ok((meta, r))
        })
        .await
    }
}

#[async_trait::async_trait]
impl<O> ChunkStore for RefCount<O>
where
    O: ObjectStore,
{
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        self.backing.get(&chunk_storage_key(id)).await
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        let id = hex::encode(blake3::hash(chunk).as_bytes());

        let ref_count = self
            .update_meta(&id, |meta| {
                let entry = meta.counts.entry(id.to_string()).or_default();
                *entry += 1;
                *entry
            })
            .await?;

        let newly_created = ref_count == 1;
        if newly_created {
            log::info!("{}: Flushing chunk", id);
            self.backing.set(&chunk_storage_key(&id), chunk).await?;
        } else {
            log::info!("{}: Already stored, not setting", id);
        }

        Ok(id)
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        let ref_count = self
            .update_meta(&id, |meta| {
                let entry = meta.counts.entry(id.to_string()).or_default();
                *entry -= 1;

                let new_count = *entry;
                if new_count == 0 {
                    meta.counts.remove(id);
                }

                new_count
            })
            .await?;

        if ref_count == 0 {
            log::info!("{}: Dropping chunk", id);
            self.backing.clear(&chunk_storage_key(id)).await?;
        } else {
            log::info!("{}: Still has ref-count {}", id, ref_count);
        }

        Ok(())
    }
}

impl<O> std::ops::Deref for RefCount<O> {
    type Target = O;

    fn deref(&self) -> &Self::Target {
        &self.backing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_store() {
        let mem = Memory::default();
        let store = RefCount::new(&mem, "meta");

        assert_eq!(store.read("foo").await, Err(IoError::NotFound));
    }

    #[tokio::test]
    async fn store_allows_read() {
        let mem = Memory::default();
        let store = RefCount::new(&mem, "meta");

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();

        assert_eq!(store.read(&id).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn store_to_new_creates() {
        let mem = Memory::default();
        let store = RefCount::new(&mem, "meta");

        store.store(&[0, 1, 2, 3]).await.unwrap();
        let after_one = mem.len();

        store.store(&[0, 1, 2, 3, 4, 5, 6, 7]).await.unwrap();
        assert_ne!(mem.len(), after_one);
    }

    #[tokio::test]
    async fn store_to_existing_does_not_create() {
        let mem = Memory::default();
        let store = RefCount::new(&mem, "meta");

        store.store(&[0, 1, 2, 3]).await.unwrap();
        let after_one = mem.len();

        store.store(&[0, 1, 2, 3]).await.unwrap();
        assert_eq!(mem.len(), after_one);
    }

    #[tokio::test]
    async fn drop_removes_from_backing() {
        let mem = Memory::default();
        let store = RefCount::new(&mem, "meta");

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();
        store.drop(&id).await.unwrap();

        assert!(!mem.values().contains(&vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn separate_stores_and_single_drop_keeps() {
        let mem = Memory::default();
        let store = RefCount::new(&mem, "meta");

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();
        let second_id = store.store(&[0, 1, 2, 3]).await.unwrap();
        assert_eq!(id, second_id);

        store.drop(&id).await.unwrap();

        assert_eq!(store.read(&id).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn double_store_does_not_set() {
        let mem = Memory::default();
        let store = RefCount::new(&mem, "meta");

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();
        mem.clear(&chunk_storage_key(&id)).await.unwrap();

        store.store(&[0, 1, 2, 3]).await.unwrap();
        assert!(!mem.values().contains(&vec![0, 1, 2, 3]));
    }
}
