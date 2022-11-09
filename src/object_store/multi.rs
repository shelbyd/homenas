use futures::future::*;

use super::*;

#[derive(Debug)]
pub struct Multi<O> {
    stores: Vec<O>,
}

impl<O> Multi<O> {
    pub fn new(stores: impl IntoIterator<Item = O>) -> Self {
        Self {
            stores: stores.into_iter().collect(),
        }
    }
}

#[async_trait::async_trait]
impl<O: ObjectStore> ObjectStore for Multi<O> {
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()> {
        let futs = self.stores.iter().map(|s| s.set(key, value));
        let results = join_all(futs).await;
        results.into_iter().collect()
    }

    async fn get(&self, key: &str) -> IoResult<Vec<u8>> {
        let futs = self.stores.iter().map(|s| s.get(key));
        Ok(select_ok(futs).await?.0)
    }

    async fn clear(&self, key: &str) -> IoResult<()> {
        let futs = self.stores.iter().map(|s| s.clear(key));
        let results = join_all(futs).await;
        results.into_iter().collect()
    }

    /// Currently does not support strong compare_exchange semantics.
    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<&[u8]>,
        new: &[u8],
    ) -> IoResult<bool> {
        let futs = self
            .stores
            .iter()
            .map(|s| s.compare_exchange(key, current, new));
        let results = join_all(futs).await;
        results.into_iter().fold(Ok(true), |acc, v| Ok(acc? && v?))
    }

    async fn locations(&self) -> IoResult<Vec<Location>> {
        Ok(try_join_all(self.stores.iter().map(|s| s.locations()))
            .await?
            .into_iter()
            .flat_map(|ls| ls)
            .collect())
    }

    async fn connect(&self, location: &Location) -> IoResult<Box<dyn ObjectStore + '_>> {
        Ok(select_ok(self.stores.iter().map(|s| s.connect(location)))
            .await?
            .0)
    }
}
