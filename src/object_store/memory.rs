use std::{collections::HashMap, sync::RwLock};

use super::*;

#[derive(Default, Debug)]
pub struct Memory {
    inner: RwLock<HashMap<String, Vec<u8>>>,
}

#[async_trait::async_trait]
impl ObjectStore for Memory {
    async fn set(&self, key: String, value: Vec<u8>) {
        self.inner.write().unwrap().insert(key, value);
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.inner.read().unwrap().get(key).cloned()
    }

    async fn compare_exchange(&self, key: &str, current: Option<Vec<u8>>, new: Vec<u8>) -> bool {
        let mut write = self.inner.write().unwrap();
        let actual_current = write.get(key);
        if actual_current != current.as_ref() {
            return false;
        }

        write.insert(key.to_string(), new);
        true
    }
}
