#![allow(unused)]

use crate::io::*;

mod memory;
pub use memory::*;

mod multi;
pub use multi::*;

mod sled;
pub use self::sled::*;

#[async_trait::async_trait]
pub trait Tree: Send + Sync {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()>;

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>>;
}

pub struct CompareAndSwapError<'p> {
    current: Option<Vec<u8>>,
    proposed: Option<&'p [u8]>,
}
