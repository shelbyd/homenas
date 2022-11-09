use std::{collections::BTreeMap, sync::RwLock};

use super::*;

#[derive(Debug)]
pub struct Memory {
    map: RwLock<BTreeMap<String, Vec<u8>>>,
    id: u64,
}

#[async_trait::async_trait]
impl ObjectStore for Memory {
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()> {
        self.map
            .write()
            .unwrap()
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn get(&self, key: &str) -> IoResult<Vec<u8>> {
        self.map
            .read()
            .unwrap()
            .get(key)
            .map(|k| {
                log::debug!("{}: Served from memory({})", key, self.id);
                k
            })
            .cloned()
            .ok_or(IoError::NotFound)
    }

    async fn clear(&self, key: &str) -> IoResult<()> {
        self.map.write().unwrap().remove(key);
        Ok(())
    }

    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<&[u8]>,
        new: &[u8],
    ) -> IoResult<bool> {
        let mut write = self.map.write().unwrap();
        let actual_current = write.get(key);
        if actual_current.map(Vec::as_slice) != current {
            return Ok(false);
        }

        write.insert(key.to_string(), new.to_vec());
        Ok(true)
    }

    async fn locations(&self) -> IoResult<Vec<Location>> {
        Ok(vec![Location::Memory(self.id)])
    }

    async fn connect(&self, location: &Location) -> IoResult<Box<dyn ObjectStore + '_>> {
        if *location != Location::Memory(self.id) {
            return Err(IoError::NotFound);
        }

        Ok(Box::new(self))
    }
}

impl Default for Memory {
    fn default() -> Memory {
        Memory {
            map: Default::default(),
            id: rand::random(),
        }
    }
}

#[cfg(test)]
#[allow(unused)]
impl Memory {
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
