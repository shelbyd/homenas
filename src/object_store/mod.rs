pub mod memory;
pub use memory::*;

pub mod network;
pub use network::*;

#[async_trait::async_trait]
pub trait ObjectStore {
    async fn set(&self, key: String, value: Vec<u8>);
    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Update the value at the provided key. May retry until successful.
    async fn update<R, F>(&self, key: String, mut f: F) -> (Vec<u8>, R)
    where
        F: for<'v> FnMut(Option<&'v Vec<u8>>) -> (Vec<u8>, R) + Send;
}
