use crate::io::*;

mod ref_count;
pub use ref_count::*;

#[async_trait::async_trait]
pub trait ChunkStore {
    async fn read(&self, id: &str) -> IoResult<Vec<u8>>;
    async fn store(&self, chunk: &[u8]) -> IoResult<String>;
    async fn drop(&self, id: &str) -> IoResult<()>;
}
