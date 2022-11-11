use super::*;

use std::path::Path;

pub struct Sled {
    sled_tree: ::sled::Db,
}

impl Sled {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Sled> {
        let sled_tree = ::sled::open(path)?;
        Ok(Sled { sled_tree })
    }
}

#[async_trait::async_trait]
impl Tree for Sled {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        todo!();
    }

    async fn insert(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        todo!();
    }

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>> {
        todo!()
    }
}
