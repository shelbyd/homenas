use std::{collections::BTreeMap, sync::RwLock};

use super::*;

#[derive(Debug)]
pub struct MemChunkStore {
    map: RwLock<BTreeMap<String, Vec<u8>>>,
    id: u64,
}

#[async_trait::async_trait]
impl ChunkStore for MemChunkStore {
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        self.map
            .read()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or(IoError::NotFound)
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        let id = id_for(chunk);
        self.map.write().unwrap().insert(id.clone(), chunk.to_vec());
        Ok(id)
    }

    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        if *location != Location::Memory(self.id) {
            return Err(IoError::NotFound);
        }

        self.store(chunk).await
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        self.map.write().unwrap().remove(id);
        Ok(())
    }

    async fn locations(&self) -> IoResult<HashSet<Location>> {
        Ok(maplit::hashset! { Location::Memory(self.id) })
    }
}

impl Default for MemChunkStore {
    fn default() -> MemChunkStore {
        MemChunkStore {
            map: Default::default(),
            id: rand::random(),
        }
    }
}

#[cfg(test)]
#[allow(unused)]
impl MemChunkStore {
    pub fn len(&self) -> usize {
        self.map.read().unwrap().len()
    }

    pub fn values(&self) -> Vec<Vec<u8>> {
        self.map
            .read()
            .unwrap()
            .values()
            .into_iter()
            .cloned()
            .collect()
    }

    pub fn keys(&self) -> Vec<String> {
        self.map
            .read()
            .unwrap()
            .keys()
            .into_iter()
            .cloned()
            .collect()
    }

    pub fn clear_all(&self) {
        self.map.write().unwrap().clear()
    }
}
