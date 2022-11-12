use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::*;

pub struct RefCount<C, T> {
    backing: C,
    tree: T,
    meta_key: String,
}

#[derive(Serialize, Deserialize, Default)]
struct ChunkMeta {
    counts: HashMap<String, u64>,
}

impl<C, T> RefCount<C, T> {
    pub fn new(backing: C, meta_key: &str, tree: T) -> Self {
        Self {
            backing,
            tree,
            meta_key: meta_key.to_string(),
        }
    }
}

impl<C: ChunkStore, T: Tree> RefCount<C, T> {
    async fn update_meta<R: Send>(
        &self,
        _id: &str,
        mut f: impl FnMut(&mut ChunkMeta) -> R + Send,
    ) -> IoResult<R> {
        update_typed(
            &self.tree,
            &format!("{}/ref-counts", self.meta_key),
            |meta: Option<ChunkMeta>| {
                let mut meta = meta.unwrap_or_default();

                let r = f(&mut meta);

                Ok((Some(meta), r))
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl<C, T> ChunkStore for RefCount<C, T>
where
    C: ChunkStore,
    T: Tree,
{
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        self.backing.read(id).await
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        let id = id_for(chunk);

        let ref_count = self
            .update_meta(&id, |meta| {
                let entry = meta.counts.entry(id.to_string()).or_default();
                *entry += 1;
                *entry
            })
            .await?;

        let newly_created = ref_count == 1;
        if newly_created {
            self.backing.store(chunk).await?;
        } else {
            log::info!("{}: Already stored, not setting", id);
        }

        Ok(id)
    }

    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        self.backing.store_at(chunk, location).await
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        let ref_count = self
            .update_meta(id, |meta| {
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
            self.backing.drop(id).await?;
        } else {
            log::info!("{}: Still has ref-count {}", id, ref_count);
        }

        Ok(())
    }

    async fn locations(&self) -> IoResult<HashSet<Location>> {
        self.backing.locations().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_store() {
        let mem = memory_chunk_store();
        let store = RefCount::new(&mem, "meta", MemoryTree::default());

        assert_eq!(store.read("foobarbaz").await, Err(IoError::NotFound));
    }

    #[tokio::test]
    async fn store_allows_read() {
        let mem = memory_chunk_store();
        let store = RefCount::new(&mem, "meta", MemoryTree::default());

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();

        assert_eq!(store.read(&id).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn store_to_new_creates() {
        let mem = memory_chunk_store();
        let store = RefCount::new(&mem, "meta", MemoryTree::default());

        store.store(&[0, 1, 2, 3]).await.unwrap();
        let after_one = mem.len();

        store.store(&[0, 1, 2, 3, 4, 5, 6, 7]).await.unwrap();
        assert_ne!(mem.len(), after_one);
    }

    #[tokio::test]
    async fn store_to_existing_does_not_create() {
        let mem = memory_chunk_store();
        let store = RefCount::new(&mem, "meta", MemoryTree::default());

        store.store(&[0, 1, 2, 3]).await.unwrap();
        let after_one = mem.len();

        store.store(&[0, 1, 2, 3]).await.unwrap();
        assert_eq!(mem.len(), after_one);
    }

    #[tokio::test]
    async fn drop_removes_from_backing() {
        let mem = memory_chunk_store();
        let store = RefCount::new(&mem, "meta", MemoryTree::default());

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();
        store.drop(&id).await.unwrap();

        assert!(!mem.values().contains(&vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn separate_stores_and_single_drop_keeps() {
        let mem = memory_chunk_store();
        let store = RefCount::new(&mem, "meta", MemoryTree::default());

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();
        let second_id = store.store(&[0, 1, 2, 3]).await.unwrap();
        assert_eq!(id, second_id);

        store.drop(&id).await.unwrap();

        assert_eq!(store.read(&id).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn double_store_does_not_set() {
        let mem = memory_chunk_store();
        let store = RefCount::new(&mem, "meta", MemoryTree::default());

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();
        mem.drop(&id).await.unwrap();

        store.store(&[0, 1, 2, 3]).await.unwrap();
        assert!(!mem.values().contains(&vec![0, 1, 2, 3]));
    }
}
