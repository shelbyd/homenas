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

    async fn update<R, F>(&self, key: String, mut f: F) -> (Vec<u8>, R)
    where
        F: for<'v> FnMut(Option<&'v Vec<u8>>) -> (Vec<u8>, R) + Send,
    {
        let mut write = self.inner.write().unwrap();
        let ref_mut = write.get(&key);
        let (new_data, r) = f(ref_mut);
        write.insert(key, new_data.clone());
        (new_data, r)
    }
}
