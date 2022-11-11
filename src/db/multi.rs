use super::*;
use crate::object_store::ResultExt;

use futures::future::*;

pub struct Multi<T> {
    leader: T,
    trees: Vec<T>,
}

impl<T> Multi<T> {
    pub fn new(leader: T, trees: impl IntoIterator<Item = T>) -> Multi<T> {
        Multi {
            leader,
            trees: trees.into_iter().collect(),
        }
    }

    fn all_trees(&self) -> impl Iterator<Item = &T> {
        [&self.leader].into_iter().chain(&self.trees)
    }
}

#[async_trait::async_trait]
impl<T: Tree> Tree for Multi<T> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        let futs = self.all_trees().map(|t| {
            t.get(key).map(|result| match result {
                Ok(Some(v)) => Ok(v),
                Ok(None) => Err(IoError::NotFound),
                Err(e) => Err(e),
            })
        });

        Ok(select_ok(futs).await.into_found()?.map(|(found, _)| found))
    }

    async fn insert(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        try_join_all(self.all_trees().map(|t| t.insert(key, value))).await?;
        Ok(())
    }

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>> {
        if let Err(e) = self.leader.compare_and_swap(key, old, new).await? {
            return Ok(Err(e));
        }

        try_join_all(self.trees.iter().map(|t| t.insert(key, new))).await?;

        Ok(Ok(()))
    }
}
