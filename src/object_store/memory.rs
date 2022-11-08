use std::{collections::HashMap, sync::RwLock};

use super::*;

#[derive(Default, Debug)]
pub struct Memory {
    inner: RwLock<HashMap<String, Vec<u8>>>,
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

    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        Ok(self.inner.read().unwrap().get(key).cloned())
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
