use futures::future::*;

use super::*;

pub struct Multi<C> {
    // TODO(shelbyd): NonEmpty.
    stores: Vec<C>,
}

impl<C> Multi<C> {
    pub fn new(stores: impl IntoIterator<Item = C>) -> Self {
        Self {
            stores: stores.into_iter().collect(),
        }
    }
}

#[async_trait::async_trait]
impl<C: ChunkStore> ChunkStore for Multi<C> {
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        let (found, _) = select_ok(self.stores.iter().map(|s| s.read(id))).await?;
        Ok(found)
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        try_join_all(self.stores.iter().map(|s| s.store(chunk))).await?;

        // TODO(shelbyd): Don't have everything use id_for.
        Ok(id_for(chunk))
    }

    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        let (stored, _) =
            select_ok(self.stores.iter().map(|s| s.store_at(chunk, location))).await?;

        // TODO(shelbyd): Don't have everything use id_for.
        Ok(stored)
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        let done = join_all(self.stores.iter().map(|s| s.drop(id))).await;
        done.into_iter().collect::<Result<(), _>>()
    }

    async fn locations(&self) -> IoResult<HashSet<Location>> {
        let locs = try_join_all(self.stores.iter().map(|s| s.locations())).await?;
        Ok(locs.into_iter().flatten().collect())
    }
}
