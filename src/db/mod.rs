#![allow(unused)]

use crate::io::*;

mod memory;
pub use memory::*;

#[async_trait::async_trait]
pub trait Tree: Send + Sync {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>>;

    async fn compare_and_swap(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<Vec<u8>>,
    ) -> IoResult<Result<(), CompareAndSwapError>>;
}

pub struct CompareAndSwapError {
    current: Option<Vec<u8>>,
    proposed: Option<Vec<u8>>,
}
