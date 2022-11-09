use std::{collections::HashMap, sync::RwLock};

use super::*;

#[derive(Default, Debug)]
pub struct Memory {
    inner: RwLock<HashMap<String, Vec<u8>>>,
}

impl Memory {
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    #[cfg(test)]
    pub fn entries(&self) -> HashMap<String, Vec<u8>> {
        self.inner.read().unwrap().clone()
    }

    #[cfg(test)]
    pub fn values(&self) -> Vec<Vec<u8>> {
        self.inner
            .read()
            .unwrap()
            .values()
            .into_iter()
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl ObjectStore for Memory {
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()> {
        self.inner
            .write()
            .unwrap()
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn get(&self, key: &str) -> IoResult<Vec<u8>> {
        self.inner
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or(IoError::NotFound)
    }

    async fn clear(&self, key: &str) -> IoResult<()> {
        self.inner.write().unwrap().remove(key);
        Ok(())
    }

    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<&[u8]>,
        new: &[u8],
    ) -> IoResult<bool> {
        let mut write = self.inner.write().unwrap();
        let actual_current = write.get(key);
        if actual_current.map(Vec::as_slice) != current {
            return Ok(false);
        }

        write.insert(key.to_string(), new.to_vec());
        Ok(true)
    }
}
