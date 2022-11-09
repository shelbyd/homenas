#![allow(dead_code)]

use serde::*;
use std::collections::HashMap;

use super::*;

type ChunkId = String;
type StripeId = String;

pub struct Striping<C> {
    backing: C,
    meta_prefix: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct StripesMeta {
    unstriped: Stripe<Vec<ChunkId>>,
    in_progress: Stripe<HashMap<Location, ChunkId>>,

    chunk_stripe: HashMap<ChunkId, StripeId>,
    stripe_chunks: HashMap<StripeId, Vec<ChunkId>>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Stripe<S> {
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
        mut f: impl FnMut(&mut StripesMeta) -> R + Send,
    ) -> IoResult<R> {
        update_typed(
            self.backing.object(),
            &self.stripe_key(),
            |meta: Option<StripesMeta>| {
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
            .get_typed::<StripesMeta>(&self.stripe_key())
            .await?;

        if let Some(stripe_id) = stripes.chunk_stripe.get(id) {
            log::debug!("{}: Chunk in Stripe {}", &id[..6], &stripe_id[..6]);

            let chunks = stripes
                .stripe_chunks
                .get(stripe_id)
                .expect("broken StripesMeta");

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
                stripes.incorporate_chunk(chunk, &id, &location_options)
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

impl StripesMeta {
    fn incorporate_chunk<'l>(
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

fn xor(a: &[u8], b: &[u8]) -> Vec<u8> {
    assert_eq!(a.len(), b.len());

    a.iter().zip(b).map(|(a, b)| a ^ b).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn xor_test() {
        assert_eq!(xor(&[0b00000000], &[0b11111111]), vec![0b11111111]);
        assert_eq!(xor(&[0b01010101], &[0b10101010]), vec![0b11111111]);
        assert_eq!(xor(&[0xff], &[0xff]), vec![0x00]);
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
    #[ignore]
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

    // Robust against partial failures.
    // Unstriped will stripe when another is available.
    // Distributes parity chunks.
}
