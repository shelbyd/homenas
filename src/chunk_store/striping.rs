#![allow(dead_code)]

use futures::future::*;
use nonempty::*;
use serde::*;
use std::collections::{HashMap, HashSet};

use super::*;

type ChunkId = String;
type StripeId = String;

pub struct Striping<C, O> {
    backing: C,
    object: O,
    meta_prefix: String,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
struct StripesMeta {
    unstriped: Stripe,
    in_progress: Stripe,
    in_progress_locations: HashSet<Location>,

    completed: HashMap<StripeId, Stripe>,

    chunk_stripe: HashMap<ChunkId, StripeId>,

    /// How many parity chunks the location has.
    parity_counts: HashMap<Location, u32>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct Stripe {
    parity: Vec<u8>,
    chunks: HashSet<ChunkId>,
}

impl<C: ChunkStore, O: ObjectStore> Striping<C, O> {
    pub fn new(backing: C, meta_prefix: &str, object: O) -> Self {
        Striping {
            backing,
            object,
            meta_prefix: meta_prefix.to_string(),
        }
    }

    fn stripe_key(&self) -> String {
        format!("{}/stripes", self.meta_prefix)
    }

    async fn update_meta<R: Send>(
        &self,
        _id: &str,
        mut f: impl FnMut(&mut StripesMeta) -> R + Send,
    ) -> IoResult<R> {
        update_typed(&self.object, &self.stripe_key(), |meta| {
            let mut meta = meta.unwrap_or_default();

            let r = f(&mut meta);

            Ok((meta, r))
        })
        .await
    }
}

#[async_trait::async_trait]
impl<C: ChunkStore, O: ObjectStore> ChunkStore for Striping<C, O> {
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        log::debug!("{}: Reading", &id[..4]);
        if let Some(r) = self.backing.read(id).await.into_found()? {
            return Ok(r);
        }

        log::warn!("{}: Lost, recovering from stripe", &id[..6]);

        let stripes = self
            .object
            .get_typed::<StripesMeta>(&self.stripe_key())
            .await?;

        let needed = stripes
            .chunks_needed(id)?
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let found: Vec<Vec<u8>> =
            try_join_all(needed.iter().map(|id| self.backing.read(id))).await?;

        let chunks = needed.iter().map(String::as_str).zip(found).collect();
        stripes.recover(id, chunks)
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        let id = id_for(chunk);

        let locations = self.locations().await?.into_iter().collect();
        let locations = NonEmpty::from_vec(locations).ok_or(IoError::Io)?;

        let (store_at, parity_chunk) = self
            .update_meta(&id, |stripes| stripes.set(&id, chunk, locations.clone()))
            .await?;

        self.store_at(chunk, &store_at).await?;

        if let Some((location, data)) = parity_chunk {
            self.store_at(&data, &location).await?;
        }

        Ok(id)
    }

    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        self.backing.store_at(chunk, location).await
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        let chunk = self.backing.read(id).await?;
        self.update_meta(id, |stripes| stripes.drop(id, &chunk))
            .await??;
        self.backing.drop(id).await?;

        Ok(())
    }

    async fn locations(&self) -> IoResult<HashSet<Location>> {
        self.backing.locations().await
    }
}

impl StripesMeta {
    fn set(
        &mut self,
        id: &str,
        chunk: &[u8],
        locations: NonEmpty<Location>,
    ) -> (Location, Option<(Location, Vec<u8>)>) {
        // TODO(shelbyd): Use stripes_mut.
        let update_parity = |p: &mut Vec<u8>| {
            p.resize(core::cmp::max(p.len(), chunk.len()), 0);
            *p = xor(&p, chunk);
        };

        if locations.tail.is_empty() {
            self.unstriped.chunks.insert(id.to_string());
            update_parity(&mut self.unstriped.parity);

            return (locations.head, None);
        }

        update_parity(&mut self.in_progress.parity);
        self.in_progress.chunks.insert(id.to_string());

        let mut valid = locations
            .into_iter()
            .filter(|l| !self.in_progress_locations.contains(l))
            .collect::<Vec<_>>();

        // Since we pop from the end of the list later, we reverse the list to break ties by the
        // first items in the locations list. Primarily for testing to be clearer.
        valid.reverse();
        valid.sort_by_key(|loc| *self.parity_counts.get(loc).unwrap_or(&0));

        let primary = valid.pop().unwrap();
        self.in_progress_locations.insert(primary.clone());

        let final_location = match (valid.pop(), valid.pop()) {
            (Some(_), Some(_)) => return (primary, None),
            (Some(last), None) => last,
            (None, _) => unreachable!(),
        };

        let to_finalize = std::mem::take(&mut self.in_progress);
        self.in_progress_locations.clear();

        let stripe_id = id_for(&to_finalize.parity);

        for chunk in to_finalize.chunks.iter() {
            self.chunk_stripe
                .insert(chunk.to_string(), stripe_id.clone());
        }

        let new_stripe = Stripe {
            parity: Vec::new(),
            chunks: to_finalize
                .chunks
                .into_iter()
                .chain([stripe_id.clone()])
                .collect(),
        };
        self.completed.insert(stripe_id, new_stripe);

        *self
            .parity_counts
            .entry(final_location.clone())
            .or_insert(0) += 1;

        (primary, Some((final_location, to_finalize.parity)))
    }

    fn chunks_needed(&self, recover: &str) -> IoResult<HashSet<&str>> {
        Ok(self
            .stripe(recover)?
            .chunks
            .iter()
            .map(String::as_ref)
            .filter(|&c| c != recover)
            .collect())
    }

    fn recover(self, id: &str, chunks: HashMap<&str, impl AsRef<[u8]>>) -> IoResult<Vec<u8>> {
        Ok(self.take_stripe(id)?.recover(chunks, id))
    }

    fn stripe(&self, id: &str) -> IoResult<&Stripe> {
        let stripes = [
            self.chunk_stripe.get(id).map(|stripe_id| {
                self.completed
                    .get(stripe_id)
                    .expect("broken link to stripe")
            }),
            Some(&self.unstriped),
            Some(&self.in_progress),
        ];

        for stripe in stripes {
            if let Some(stripe) = stripe {
                if stripe.chunks.contains(id) {
                    return Ok(stripe);
                }
            }
        }

        Err(IoError::NotFound)
    }

    fn stripe_mut(&mut self, id: &str) -> IoResult<&mut Stripe> {
        let stripes = [
            self.chunk_stripe.get(id).map(|stripe_id| {
                self.completed
                    .get_mut(stripe_id)
                    .expect("broken link to stripe")
            }),
            Some(&mut self.unstriped),
            Some(&mut self.in_progress),
        ];

        for stripe in stripes {
            if let Some(stripe) = stripe {
                if stripe.chunks.contains(id) {
                    return Ok(stripe);
                }
            }
        }

        Err(IoError::NotFound)
    }

    fn take_stripe(mut self, id: &str) -> IoResult<Stripe> {
        let stripes = [
            self.chunk_stripe.remove(id).map(|stripe_id| {
                self.completed
                    .remove(&stripe_id)
                    .expect("broken link to stripe")
            }),
            Some(self.unstriped),
            Some(self.in_progress),
        ];

        for stripe in stripes {
            if let Some(stripe) = stripe {
                if stripe.chunks.contains(id) {
                    return Ok(stripe);
                }
            }
        }

        Err(IoError::NotFound)
    }

    fn drop(&mut self, id: &str, chunk: &[u8]) -> IoResult<()> {
        self.stripe_mut(id)?.drop(id, chunk);

        // Not tested. Please add a test that requires this remove.
        self.chunk_stripe.remove(id);

        Ok(())
    }
}

impl Stripe {
    fn recover(self, mut chunks: HashMap<&str, impl AsRef<[u8]>>, skip: &str) -> Vec<u8> {
        let mut result = self.parity;
        for chunk_id in &self.chunks {
            if chunk_id == skip {
                continue;
            }

            let chunk = chunks
                .remove(chunk_id.as_str())
                .expect(&format!("missing required chunk {}", chunk_id));

            let chunk = chunk.as_ref();
            result.resize(core::cmp::max(result.len(), chunk.len()), 0);
            result = xor(&result, chunk);
        }
        result
    }

    fn drop(&mut self, id: &str, chunk: &[u8]) {
        assert!(self.chunks.remove(id), "stripe dropped missing id");
        self.parity = xor(&self.parity, &chunk);
    }
}

fn xor(a: &[u8], b: &[u8]) -> Vec<u8> {
    let total = core::cmp::max(a.len(), b.len());

    let extended_a = a.iter().chain(std::iter::repeat(&0));
    let extended_b = b.iter().chain(std::iter::repeat(&0));

    extended_a
        .zip(extended_b)
        .take(total)
        .map(|(a, b)| a ^ b)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::*;
    use test_log::test;

    #[test]
    fn xor_test() {
        assert_eq!(xor(&[0b00000000], &[0b11111111]), vec![0b11111111]);
        assert_eq!(xor(&[0b01010101], &[0b10101010]), vec![0b11111111]);
        assert_eq!(xor(&[0xff], &[0xff]), vec![0x00]);
        assert_eq!(xor(&[0xff, 0xff], &[0xff]), vec![0x00, 0xff]);
        assert_eq!(xor(&[0xff], &[0xff, 0xff]), vec![0x00, 0xff]);
    }

    #[tokio::test]
    async fn single_insert_inserts_to_backing() {
        let mem = memory_chunk_store();
        let store = Striping::new(&mem, "meta", Memory::default());

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();

        assert_eq!(mem.read(&id).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn recovers_first_write_after_underlying_failure() {
        let mem = memory_chunk_store();
        let store = Striping::new(&mem, "meta", Memory::default());

        let first = store.store(&[0, 1, 2, 3]).await.unwrap();
        store.store(&[4, 5, 6, 7]).await.unwrap();

        mem.drop(&first).await.unwrap();

        assert_eq!(store.read(&first).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn another_instance_recovers_first_write_after_underlying_failure() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let store = Striping::new(&mem, "meta", &mem_os);

        let first = store.store(&[0, 1, 2, 3]).await.unwrap();
        store.store(&[4, 5, 6, 7]).await.unwrap();
        mem.drop(&first).await.unwrap();

        assert_eq!(
            Striping::new(&mem, "meta", &mem_os).read(&first).await,
            Ok(vec![0, 1, 2, 3])
        );
    }

    #[tokio::test]
    async fn dropping_two_allows_recovering_another() {
        let backing_1 = MemChunkStore::default();
        let backing_2 = MemChunkStore::default();
        let backing_3 = MemChunkStore::default();
        let chunk_store = crate::chunk_store::Multi::new([&backing_1, &backing_2, &backing_3]);

        let store = Striping::new(&chunk_store, "meta", Memory::default());

        let first = store.store(&[1]).await.unwrap();
        let second = store.store(&[2]).await.unwrap();

        store.drop(&first).await.unwrap();
        chunk_store.drop(&second).await.unwrap();

        assert_eq!(store.read(&second).await, Ok(vec![2]));
    }

    #[cfg(test)]
    mod stripe_meta {
        use super::{test, *};

        use crate::object_store::Location::*;

        fn empty_map() -> HashMap<&'static str, Vec<u8>> {
            hashmap! {}
        }

        #[test]
        fn empty_cannot_help_with_failed_read() {
            let stripes = StripesMeta::default();

            assert_eq!(stripes.recover("foo", empty_map()), Err(IoError::NotFound));
        }

        #[test]
        fn single_chunk_is_value() {
            let mut stripes = StripesMeta::default();

            stripes.set("foo", &[0, 1, 2, 3], nonempty![Memory(42)]);

            // Parity of a single chunk is just the chunk.
            assert_eq!(stripes.chunks_needed("foo"), Ok(hashset! {}));
            assert_eq!(stripes.recover("foo", empty_map()), Ok(vec![0, 1, 2, 3]));
        }

        #[test]
        fn three_chunks_uses_parity_and_others() {
            let mut stripes = StripesMeta::default();

            stripes.set("foo", &[0, 1, 2, 3], nonempty![Memory(42)]);
            stripes.set("bar", &[4, 5, 6, 7], nonempty![Memory(42)]);
            stripes.set("baz", &[8, 9, 10, 11], nonempty![Memory(42)]);

            assert_eq!(stripes.chunks_needed("foo"), Ok(hashset! { "bar", "baz" }));

            assert_eq!(
                stripes.recover(
                    "foo",
                    hashmap! {
                        "bar" => &[4, 5, 6, 7],
                        "baz" => &[8, 9, 10, 11],
                    }
                ),
                Ok(vec![0, 1, 2, 3])
            );
        }

        #[test]
        fn smaller_second_insert_does_not_lose_data() {
            let mut stripes = StripesMeta::default();

            stripes.set("foo", &[0, 1, 2, 3], nonempty![Memory(42)]);
            stripes.set("bar", &[4, 5, 6], nonempty![Memory(42)]);

            assert_eq!(
                stripes.recover("foo", hashmap! { "bar" => &[4, 5, 6] }),
                Ok(vec![0, 1, 2, 3])
            );
        }

        #[cfg(test)]
        mod many_locations {
            use super::*;
            use test_log::test;

            #[test]
            fn stores_first_at_first_location() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42), Memory(43), Memory(44)];

                assert_eq!(
                    stripes.set("foo", &[0, 1, 2, 3], locations.clone()).0,
                    Memory(42),
                );
            }

            #[test]
            fn stores_second_at_another_location() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42), Memory(43), Memory(44)];

                let first = stripes.set("foo", &[0], locations.clone()).0;
                let second = stripes.set("bar", &[1], locations.clone()).0;

                assert_ne!(first, second);
            }

            #[test]
            fn two_stores_can_recover_first() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42), Memory(43), Memory(44), Memory(45)];

                stripes.set("foo", &[1], locations.clone());
                stripes.set("bar", &[2], locations.clone());

                assert_eq!(stripes.chunks_needed("foo"), Ok(hashset! { "bar" }));
                assert_eq!(
                    stripes.recover("foo", hashmap! { "bar" => &[2] }),
                    Ok(vec![1])
                );
            }

            #[test]
            fn stores_parity_when_full() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42), Memory(43), Memory(44)];

                stripes.set("foo", &[0, 1, 2, 3], locations.clone());
                assert_eq!(
                    stripes.set("bar", &[4, 5, 6, 7], locations.clone()),
                    (Memory(43), Some((Memory(44), vec![4, 4, 4, 4],))),
                );
            }

            #[test]
            fn starts_new_stripe_after_parity_flush() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42), Memory(43), Memory(44)];

                stripes.set("foo", &[0, 1, 2, 3], locations.clone());
                stripes.set("bar", &[4, 5, 6, 7], locations.clone());

                stripes.set("baz", &[8, 9, 10, 11], locations.clone());
                assert_eq!(stripes.chunks_needed("baz"), Ok(hashset! {}));
            }

            #[test]
            fn needs_parity_after_flush() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42), Memory(43), Memory(44)];

                stripes.set("foo", &[0, 1, 2, 3], locations.clone());
                stripes.set("bar", &[4, 5, 6, 7], locations.clone());

                let needed_for_foo = stripes.chunks_needed("foo").unwrap();

                assert!(needed_for_foo.contains("bar"));
                assert!(!needed_for_foo.contains("foo"));

                assert_eq!(needed_for_foo.len(), 2); // Contains the parity chunk.
            }

            #[test]
            fn recovers_after_flush() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42), Memory(43), Memory(44)];

                let foo = &[0, 1, 2, 3];
                let bar = &[4, 5, 6, 7];

                stripes.set("foo", foo, locations.clone());
                stripes.set("bar", bar, locations.clone());

                let parity = xor(foo, bar);
                let parity_id = id_for(&parity);

                assert_eq!(
                    stripes.clone().recover(
                        "foo",
                        hashmap! {
                            "bar" => bar.to_vec(),
                            &parity_id => parity,
                        }
                    ),
                    Ok(foo.to_vec())
                );
            }

            #[test]
            fn sends_parity_to_a_different_location() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42), Memory(43), Memory(44)];

                stripes.set("foo", &[1], locations.clone());
                let first_parity_loc = stripes.set("bar", &[2], locations.clone()).1.unwrap().0;

                stripes.set("baz", &[4], locations.clone());
                let second_parity_loc = stripes.set("qux", &[8], locations.clone()).1.unwrap().0;

                assert_ne!(first_parity_loc, second_parity_loc);
            }
        }

        #[cfg(test)]
        mod dropping {
            use super::{test, *};

            #[test]
            fn striped_allows_recover() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42), Memory(43), Memory(44)];

                let foo = &[0, 1, 2, 3];
                let bar = &[4, 5, 6, 7];

                stripes.set("foo", foo, locations.clone());
                stripes.set("bar", bar, locations.clone());

                assert_eq!(stripes.drop("foo", &[0, 1, 2, 3][..]), Ok(()));
            }

            #[test]
            fn unstriped_allows_recover() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![Memory(42)];

                stripes.set("foo", &[0, 1, 2, 3], locations.clone());
                stripes.set("bar", &[4, 5, 6, 7], locations.clone());
                stripes.drop("foo", &[0, 1, 2, 3][..]).unwrap();

                assert_eq!(stripes.recover("bar", empty_map()), Ok(vec![4, 5, 6, 7]));
            }
        }
    }

    // Robust against partial failures.
    // Unstriped will stripe when another is available.
    // Cleaning up stripes that are only parity.
}
