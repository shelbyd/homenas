use std::collections::HashMap;

use super::*;
use crate::object_store::*;

pub struct ChunkStore<O> {
    backing: O,
    meta_key: String,
}

#[derive(Serialize, Deserialize, Default)]
struct ChunkMeta {
    counts: HashMap<String, u64>,
}

impl<O: ObjectStore> ChunkStore<O> {
    pub fn new(backing: O, meta_key: &str) -> Self {
        Self {
            backing,
            meta_key: meta_key.to_string(),
        }
    }

    pub async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        self.backing.get(&self.storage_key(id)).await
    }

    fn storage_key(&self, id: &str) -> String {
        format!("chunks/{}", id)
    }

    pub async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        let id = hex::encode(blake3::hash(chunk).as_bytes());
        self.backing.set(&self.storage_key(&id), chunk).await?;

        self.update_meta(&id, |meta| {
            *meta.counts.entry(id.to_string()).or_default() += 1;
        })
        .await?;

        Ok(id)
    }

    async fn update_meta<R: Send>(
        &self,
        id: &str,
        mut f: impl FnMut(&mut ChunkMeta) -> R + Send,
    ) -> IoResult<R> {
        update_typed(
            &self.backing,
            &self.meta_key,
            |mut meta: Option<ChunkMeta>| {
                let mut meta = meta.unwrap_or_default();

                let r = f(&mut meta);

                Ok((meta, r))
            },
        )
        .await
    }

    pub async fn drop(&self, id: &str) -> IoResult<()> {
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
            self.backing.clear(&self.storage_key(id)).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_store() {
        let mem = Memory::default();
        let store = ChunkStore::new(&mem, "meta");

        assert_eq!(store.read("foo").await, Err(IoError::NotFound));
    }

    #[tokio::test]
    async fn store_allows_read() {
        let mem = Memory::default();
        let store = ChunkStore::new(&mem, "meta");

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();

        assert_eq!(store.read(&id).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn store_to_new_creates() {
        let mem = Memory::default();
        let store = ChunkStore::new(&mem, "meta");

        store.store(&[0, 1, 2, 3]).await.unwrap();
        let after_one = mem.len();

        store.store(&[0, 1, 2, 3, 4, 5, 6, 7]).await.unwrap();
        assert_ne!(mem.len(), after_one);
    }

    #[tokio::test]
    async fn store_to_existing_does_not_create() {
        let mem = Memory::default();
        let store = ChunkStore::new(&mem, "meta");

        store.store(&[0, 1, 2, 3]).await.unwrap();
        let after_one = mem.len();

        store.store(&[0, 1, 2, 3]).await.unwrap();
        assert_eq!(mem.len(), after_one);
    }

    #[tokio::test]
    async fn drop_removes_from_backing() {
        let mem = Memory::default();
        let store = ChunkStore::new(&mem, "meta");

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();
        store.drop(&id).await.unwrap();

        assert!(!mem.values().contains(&vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn separate_stores_and_single_drop_keeps() {
        let mem = Memory::default();
        let store = ChunkStore::new(&mem, "meta");

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();
        let second_id = store.store(&[0, 1, 2, 3]).await.unwrap();
        assert_eq!(id, second_id);

        store.drop(&id).await.unwrap();

        assert_eq!(store.read(&id).await, Ok(vec![0, 1, 2, 3]));
    }
}
