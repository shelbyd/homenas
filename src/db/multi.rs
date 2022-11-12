use super::*;
use crate::utils::*;

use futures::future::*;

pub struct Multi<T> {
    leader: T,
    followers: Vec<T>,
}

impl<T> Multi<T> {
    pub fn new(leader: T, followers: impl IntoIterator<Item = T>) -> Multi<T> {
        Multi {
            leader,
            followers: followers.into_iter().collect(),
        }
    }

    fn all_trees(&self) -> impl Iterator<Item = &T> {
        [&self.leader].into_iter().chain(&self.followers)
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

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        try_join_all(self.all_trees().map(|t| t.set(key, value))).await?;
        Ok(())
    }

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>> {
        // TODO(shelbyd): Maybe a more robust implementation?
        if let Err(e) = self.leader.compare_and_swap(key, old, new).await? {
            return Ok(Err(e));
        }

        try_join_all(self.followers.iter().map(|t| t.set(key, new))).await?;

        Ok(Ok(()))
    }
}
