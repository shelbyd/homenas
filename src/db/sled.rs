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
        Ok(self.sled_tree.get(key)?.map(|iv| iv.to_vec()))
    }

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        match value {
            Some(value) => self.sled_tree.insert(key, value)?,
            None => self.sled_tree.remove(key)?,
        };

        Ok(())
    }

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>> {
        let result = self.sled_tree.compare_and_swap(key, old, new)?;

        Ok(result.map_err(|e| CompareAndSwapError {
            proposed: new,
            current: e.current.map(|iv| iv.to_vec()),
        }))
    }
}
