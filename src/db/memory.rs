use super::*;

use dashmap::{mapref::entry::Entry, *};

pub struct MemoryTree {
    map: DashMap<String, Vec<u8>>,
}

#[async_trait::async_trait]
impl Tree for MemoryTree {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        Ok(self.map.get(key).map(|v| v.clone()))
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<Vec<u8>>,
    ) -> IoResult<Result<(), CompareAndSwapError>> {
        let entry = self.map.entry(key.to_string());

        match (&entry, old) {
            (Entry::Occupied(current), Some(old)) if *current.get() == old => {}
            (Entry::Vacant(_), None) => {}
            (current, old) => {
                let current = match current {
                    Entry::Occupied(c) => Some(c.get().clone()),
                    Entry::Vacant(_) => None,
                };
                return Ok(Err(CompareAndSwapError {
                    current,
                    proposed: new,
                }));
            }
        }

        match (entry, new) {
            (Entry::Occupied(mut c), Some(new)) => *c.get_mut() = new,
            (Entry::Vacant(v), Some(new)) => {
                v.insert(new);
            }
            (Entry::Vacant(_), None) => {}
            (Entry::Occupied(c), None) => {
                c.remove();
            }
        }

        Ok(Ok(()))
    }
}
