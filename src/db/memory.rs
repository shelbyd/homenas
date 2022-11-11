use super::*;

use dashmap::{mapref::entry::Entry, *};

#[derive(Default)]
pub struct MemoryTree {
    map: DashMap<String, Vec<u8>>,
}

#[async_trait::async_trait]
impl Tree for MemoryTree {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        Ok(self.map.get(key).map(|v| v.clone()))
    }

    async fn insert(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        match value {
            Some(v) => {
                self.map.insert(key.to_string(), v.to_vec());
            }
            None => {
                self.map.remove(key);
            }
        }

        Ok(())
    }

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>> {
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
            (Entry::Occupied(mut c), Some(new)) => *c.get_mut() = new.to_vec(),
            (Entry::Vacant(v), Some(new)) => {
                v.insert(new.to_vec());
            }
            (Entry::Vacant(_), None) => {}
            (Entry::Occupied(c), None) => {
                c.remove();
            }
        }

        Ok(Ok(()))
    }
}
