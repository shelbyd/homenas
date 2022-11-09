#![allow(dead_code)]

use maplit::*;
use nonempty::*;
use serde::*;
use std::collections::{HashMap, HashSet, VecDeque};

use super::*;

type ChunkId = String;
type StripeId = String;

pub struct Striping<C> {
    backing: C,
    meta_prefix: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct StripesMetaDepr {
    unstriped: StripeDepr<Vec<ChunkId>>,
    in_progress: StripeDepr<HashMap<Location, ChunkId>>,

    chunk_stripe: HashMap<ChunkId, StripeId>,
    stripe_chunks: HashMap<StripeId, Vec<ChunkId>>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct StripeDepr<S> {
    parity: Vec<u8>,
    stored: S,
}

impl<C: ChunkStore> Striping<C> {
    pub fn new(backing: C, meta_prefix: &str) -> Self {
        Striping {
            backing,
            meta_prefix: meta_prefix.to_string(),
        }
    }

    fn stripe_key(&self) -> String {
        format!("{}/stripes", self.meta_prefix)
    }

    async fn update_meta<R: Send>(
        &self,
        _id: &str,
        mut f: impl FnMut(&mut StripesMetaDepr) -> R + Send,
    ) -> IoResult<R> {
        update_typed(
            self.backing.object(),
            &self.stripe_key(),
            |meta: Option<StripesMetaDepr>| {
                let mut meta = meta.unwrap_or_default();

                let r = f(&mut meta);

                Ok((meta, r))
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl<C: ChunkStore> ChunkStore for Striping<C> {
    type Backing = C::Backing;

    fn object(&self) -> &Self::Backing {
        self.backing.object()
    }

    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        log::debug!("{}: Reading", &id[..4]);
        if let Some(r) = self.backing.read(id).await.into_found()? {
            return Ok(r);
        }

        log::warn!("{}: Lost, recovering from stripe", &id[..6]);

        let stripes = self
            .object()
            .get_typed::<StripesMetaDepr>(&self.stripe_key())
            .await?;

        if let Some(stripe_id) = stripes.chunk_stripe.get(id) {
            log::debug!("{}: Chunk in Stripe {}", &id[..6], &stripe_id[..6]);

            let chunks = stripes
                .stripe_chunks
                .get(stripe_id)
                .expect("broken StripesMetaDepr");

            let mut ret = self.backing.read(&stripe_id).await?;
            for other_id in chunks {
                if id == other_id {
                    continue;
                }
                let read = self.backing.read(&other_id).await?;
                ret = xor(&ret, &read);
            }
            return Ok(ret);
        }

        log::error!("Not implemented correctly here");

        let stripe = stripes.unstriped;

        let mut ret = stripe.parity;
        for other_id in stripe.stored {
            if id == other_id {
                continue;
            }
            let read = self.backing.read(&other_id).await?;
            ret = xor(&ret, &read);
        }

        Ok(ret)
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        let id = id_for(chunk);

        let location_options = self.object().locations().await?;

        let (store_at, flush_chunk) = self
            .update_meta(&id, |stripes| {
                stripes.incorporate_chunk_depr(chunk, &id, &location_options)
            })
            .await?;

        log::info!("{}: storing at {:?}", &id[0..4], store_at);
        self.store_at(chunk, store_at).await?;

        if let Some((at, chunk)) = flush_chunk {
            let parity_id = id_for(&chunk);
            log::info!("{}: parity storing at {:?}", &parity_id[..4], at);
            self.store_at(&chunk, at).await?;
        }

        Ok(id)
    }

    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        self.backing.store_at(chunk, location).await
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        self.backing.drop(id).await
    }
}

impl StripesMetaDepr {
    fn incorporate_chunk_depr<'l>(
        &mut self,
        chunk: &[u8],
        id: &str,
        locations: &'l [Location],
    ) -> (&'l Location, Option<(&'l Location, Vec<u8>)>) {
        match locations {
            [] => unreachable!(),
            [only] => {
                let stripe = &mut self.unstriped;

                if stripe.parity.len() == 0 {
                    stripe.parity.resize(chunk.len(), 0);
                }
                stripe.parity = xor(&stripe.parity, chunk);

                stripe.stored.push(id.to_string());

                (only, None)
            }
            [..] => {
                let stripe = &mut self.in_progress;

                let first = if stripe.parity.len() == 0 {
                    stripe.parity.resize(chunk.len(), 0);
                    true
                } else {
                    false
                };
                stripe.parity = xor(&stripe.parity, chunk);
                if !first {
                    assert_ne!(stripe.parity, chunk);
                }

                let mut options: Vec<_> = locations
                    .iter()
                    .filter(|l| !stripe.stored.contains_key(l))
                    .collect();
                log::debug!("{:?}", &options);

                let at = options.pop().unwrap();
                stripe.stored.insert(at.clone(), id.to_string());

                match options.as_slice() {
                    [_, _, ..] => (at, None),
                    [parity] => {
                        let full_stripe = std::mem::take(stripe);

                        let stripe_id = id_for(&full_stripe.parity);

                        let chunks: Vec<ChunkId> = full_stripe.stored.into_values().collect();
                        for chunk in &chunks {
                            self.chunk_stripe.insert(chunk.clone(), stripe_id.clone());
                        }
                        self.stripe_chunks.insert(stripe_id, chunks);

                        (at, Some((*parity, full_stripe.parity)))
                    }
                    [] => unreachable!(),
                }
            }
        }
    }
}

#[derive(Default, Clone, Debug)]
struct StripesMeta {
    unstriped: Stripe,
    in_progress: Stripe,
    in_progress_locations: HashSet<Location>,

    completed: HashMap<StripeId, Stripe>,

    chunk_stripe: HashMap<ChunkId, StripeId>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct Stripe {
    parity: Vec<u8>,
    chunks: HashSet<ChunkId>,
}

impl StripesMeta {
    fn set<'c, 'l>(
        &'c mut self,
        id: &str,
        chunk: &'c [u8],
        locations: NonEmpty<&'l Location>,
    ) -> (&'l Location, HashMap<&'l Location, Vec<u8>>) {
        // TODO(shelbyd): Use stripes_mut.
        let update_parity = |p: &mut Vec<u8>| {
            p.resize(core::cmp::max(p.len(), chunk.len()), 0);
            *p = xor(&p, chunk);
        };

        if locations.tail.is_empty() {
            self.unstriped.chunks.insert(id.to_string());
            update_parity(&mut self.unstriped.parity);

            return (*locations.first(), HashMap::new());
        }

        update_parity(&mut self.in_progress.parity);
        self.in_progress.chunks.insert(id.to_string());

        let mut valid = locations
            .iter()
            .filter(|l| !self.in_progress_locations.contains(l))
            .collect::<VecDeque<_>>();

        let primary = valid.pop_front().unwrap();
        self.in_progress_locations.insert(Location::clone(primary));

        let final_location = match (valid.pop_front(), valid.pop_front()) {
            (Some(_), Some(_)) => return (primary, HashMap::new()),
            (Some(&last), None) => last,
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

        (primary, hashmap! { final_location => to_finalize.parity })
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
}

fn xor(a: &[u8], b: &[u8]) -> Vec<u8> {
    assert!(a.len() >= b.len());
    let extended_b = b.iter().chain(std::iter::repeat(&0));

    a.iter().zip(extended_b).map(|(a, b)| a ^ b).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xor_test() {
        assert_eq!(xor(&[0b00000000], &[0b11111111]), vec![0b11111111]);
        assert_eq!(xor(&[0b01010101], &[0b10101010]), vec![0b11111111]);
        assert_eq!(xor(&[0xff], &[0xff]), vec![0x00]);
        assert_eq!(xor(&[0xff, 0xff], &[0xff]), vec![0x00, 0xff]);
    }

    #[tokio::test]
    async fn single_insert_inserts_to_backing() {
        let mem = memory_chunk_store();
        let store = Striping::new(&mem, "meta");

        let id = store.store(&[0, 1, 2, 3]).await.unwrap();

        assert_eq!(mem.read(&id).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn recovers_first_write_after_underlying_failure() {
        let mem = memory_chunk_store();
        let store = Striping::new(&mem, "meta");

        let first = store.store(&[0, 1, 2, 3]).await.unwrap();
        store.store(&[4, 5, 6, 7]).await.unwrap();

        mem.drop(&first).await.unwrap();

        assert_eq!(store.read(&first).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn another_instance_recovers_first_write_after_underlying_failure() {
        let mem = memory_chunk_store();
        let store = Striping::new(&mem, "meta");

        let first = store.store(&[0, 1, 2, 3]).await.unwrap();
        store.store(&[4, 5, 6, 7]).await.unwrap();
        mem.drop(&first).await.unwrap();

        assert_eq!(
            Striping::new(&mem, "meta").read(&first).await,
            Ok(vec![0, 1, 2, 3])
        );
    }

    #[tokio::test]
    async fn divides_among_underlying_storage() {
        let primary = Memory::default();
        let secondary = Memory::default();
        let tertiary = Memory::default();
        let multi = Multi::new([&primary, &secondary, &tertiary]);
        let chunk_store = Direct::new(&multi, "chunks");

        let store = Striping::new(&chunk_store, "meta");

        let chunks = vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]];

        store.store(&chunks[0]).await.unwrap();
        store.store(&chunks[1]).await.unwrap();

        let count_chunks = |p: &Memory| p.keys().iter().filter(|k| k.starts_with("chunks")).count();
        assert_eq!(count_chunks(&primary), 1);
        assert_eq!(count_chunks(&secondary), 1);
        assert_eq!(count_chunks(&tertiary), 1);
    }

    #[tokio::test]
    async fn recovers_after_entire_underlying_failure() {
        let primary = Memory::default();
        let secondary = Memory::default();
        let tertiary = Memory::default();
        let multi = Multi::new([&primary, &secondary, &tertiary]);
        dbg!(multi.locations().await.unwrap());

        let chunk_store = Direct::new(&multi, "chunks");

        let store = Striping::new(&chunk_store, "meta");

        let chunks = (0..)
            .take(9)
            .map(|i| (1u64 << i).to_be_bytes().to_vec())
            .collect::<Vec<_>>();

        let mut ids = Vec::new();
        for chunk in &chunks {
            ids.push(store.store(chunk).await.unwrap());
        }

        secondary.clear_all();

        for (id, chunk) in ids.iter().zip(chunks) {
            assert_eq!(
                Striping::new(&chunk_store, "meta").read(&id).await,
                Ok(chunk)
            );
        }
    }

    #[tokio::test]
    async fn non_full_write_at_end_of_file() {
        let mem = memory_chunk_store();
        let store = Striping::new(&mem, "meta");

        store.store(&[0, 1, 2, 3]).await.unwrap();
        let id = store.store(&[4, 5, 6]).await.unwrap();

        assert_eq!(mem.read(&id).await, Ok(vec![4, 5, 6]));
    }

    #[cfg(test)]
    mod stripe_meta {
        use super::*;

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

            stripes.set("foo", &[0, 1, 2, 3], nonempty![&Memory(42)]);

            // Parity of a single chunk is just the chunk.
            assert_eq!(stripes.chunks_needed("foo"), Ok(hashset! {}));
            assert_eq!(stripes.recover("foo", empty_map()), Ok(vec![0, 1, 2, 3]));
        }

        #[test]
        fn three_chunks_uses_parity_and_others() {
            let mut stripes = StripesMeta::default();

            stripes.set("foo", &[0, 1, 2, 3], nonempty![&Memory(42)]);
            stripes.set("bar", &[4, 5, 6, 7], nonempty![&Memory(42)]);
            stripes.set("baz", &[8, 9, 10, 11], nonempty![&Memory(42)]);

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

            stripes.set("foo", &[0, 1, 2, 3], nonempty![&Memory(42)]);
            stripes.set("bar", &[4, 5, 6], nonempty![&Memory(42)]);

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

                let locations = nonempty![&Memory(42), &Memory(43), &Memory(44)];

                assert_eq!(
                    stripes.set("foo", &[0, 1, 2, 3], locations.clone()).0,
                    &Memory(42),
                );
            }

            #[test]
            fn stores_second_at_another_location() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![&Memory(42), &Memory(43), &Memory(44)];

                let first = stripes.set("foo", &[0], locations.clone()).0;
                let second = stripes.set("bar", &[1], locations.clone()).0;

                assert_ne!(first, second);
            }

            #[test]
            fn two_stores_can_recover_first() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![&Memory(42), &Memory(43), &Memory(44), &Memory(45)];

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

                let locations = nonempty![&Memory(42), &Memory(43), &Memory(44)];

                stripes.set("foo", &[0, 1, 2, 3], locations.clone());
                assert_eq!(
                    stripes.set("bar", &[4, 5, 6, 7], locations.clone()),
                    (
                        &Memory(43),
                        hashmap! {
                            &Memory(44) => vec![4, 4, 4, 4],
                        }
                    ),
                );
            }

            #[test]
            fn starts_new_stripe_after_parity_flush() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![&Memory(42), &Memory(43), &Memory(44)];

                stripes.set("foo", &[0, 1, 2, 3], locations.clone());
                stripes.set("bar", &[4, 5, 6, 7], locations.clone());

                stripes.set("baz", &[8, 9, 10, 11], locations.clone());
                assert_eq!(stripes.chunks_needed("baz"), Ok(hashset! {}));
            }

            #[test]
            fn needs_parity_after_flush() {
                let mut stripes = StripesMeta::default();

                let locations = nonempty![&Memory(42), &Memory(43), &Memory(44)];

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

                let locations = nonempty![&Memory(42), &Memory(43), &Memory(44)];

                let foo = &[0, 1, 2, 3];
                let bar = &[4, 5, 6, 7];

                dbg!(&stripes);
                stripes.set("foo", foo, locations.clone());
                dbg!(&stripes);
                stripes.set("bar", bar, locations.clone());
                dbg!(&stripes);

                let parity = xor(foo, bar);
                let parity_id = id_for(&parity);

                dbg!(&parity);
                dbg!(&stripes);

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
        }
    }

    // Robust against partial failures.
    // Unstriped will stripe when another is available.
    // Distributes parity chunks.
    // Dropping chunks.
}
