use crate::{chunk_store::*, db::*, io::*, log_err, utils::*};

use serde::*;
use std::{net::SocketAddr, path::Path, sync::Arc};

mod cluster;
use cluster::{Event as ClusterEvent, *};

mod connections;
use connections::*;

mod consensus;

pub type NodeId = u64;

pub struct NetworkStore<T: Tree + 'static, C> {
    backing_tree: T,
    backing_chunks: C,
    cluster: Arc<Cluster<Event, Request>>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Event {
    Set(String, Option<Vec<u8>>),
    CompareAndSwap(String, Option<Vec<u8>>, Option<Vec<u8>>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get(String),
}

impl<T: Tree + Clone + 'static, C: ChunkStore + 'static> NetworkStore<T, C> {
    pub async fn create(
        backing_tree: T,
        backing_chunks: C,
        listen_on: u16,
        peers: &[SocketAddr],
        state_dir: impl AsRef<Path>,
    ) -> anyhow::Result<Arc<Self>> {
        let state_dir = state_dir.as_ref();
        crate::ensure_dir_exists(state_dir).await?;
        let sled = sled::open(state_dir)?;

        let node_id = sled
            .update_and_fetch("node_id", |existing| {
                Some(
                    opt_vec(existing)
                        .unwrap_or_else(|| crate::to_vec::<u64>(&rand::random()).unwrap()),
                )
            })?
            .unwrap();
        let node_id = crate::from_slice(&node_id)?;
        log::info!("Current raft id: {}", node_id);

        let cluster = Arc::new(Cluster::new(node_id, listen_on, peers).await?);

        let network_store = Arc::new(NetworkStore {
            backing_tree,
            backing_chunks,
            cluster,
        });

        let event_listener = Arc::clone(&network_store);
        tokio::task::spawn(async move { event_listener.handle_cluster_events().await });

        Ok(network_store)
    }
}

impl<C: ChunkStore + 'static, T: Tree> NetworkStore<T, C> {
    async fn handle_cluster_events(self: Arc<Self>) {
        loop {
            let event = self.cluster.next_event().await;
            let clone = Arc::clone(&self);
            tokio::task::spawn(async move {
                if let None = log_err!(clone.handle_cluster_event(event).await) {
                    std::process::exit(1);
                }
            });
        }
    }

    async fn handle_cluster_event(
        &self,
        event: ClusterEvent<Event, Request>,
    ) -> anyhow::Result<()> {
        let (peer_id, req_id, request) = match event {
            ClusterEvent::NewConnection(_) | ClusterEvent::Dropped(_) => {
                return Ok(());
            }
            ClusterEvent::Request(peer_id, req_id, request) => (peer_id, req_id, request),
            unhandled => {
                log::error!("Unhandled: {:?}", unhandled);
                std::process::exit(1);
            }
        };

        match request {
            Request::Get(key) => {
                self.cluster
                    .respond::<Option<Vec<u8>>>(peer_id, req_id, self.get(&key).await?)
                    .await?;
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: Tree, C: ChunkStore + 'static> Tree for NetworkStore<T, C> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        if let Some(here) = self.backing_tree.get(key).await? {
            return Ok(Some(here));
        }

        for peer in self.cluster.peers() {
            if let Ok(Some(v)) = self
                .cluster
                .request(peer, Request::Get(key.to_string()))
                .await
            {
                return Ok(Some(v));
            }
        }

        Ok(None)
    }

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        self.backing_tree.set(key, value).await?;

        log_err!(
            self.cluster
                .broadcast(Event::Set(key.to_string(), opt_vec(value)))
                .await
        )
        .ok_or(IoError::Internal)?;

        Ok(())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError>> {
        log::warn!("Unsafe implementation of compare_and_swap");

        if let Err(cas) = self.backing_tree.compare_and_swap(key, old, new).await? {
            return Ok(Err(cas));
        }

        log_err!(
            self.cluster
                .broadcast(Event::CompareAndSwap(
                    key.to_string(),
                    opt_vec(old),
                    opt_vec(new),
                ))
                .await
        )
        .ok_or(IoError::Internal)?;

        Ok(Ok(()))
    }
}

#[async_trait::async_trait]
impl<T: Tree, C: ChunkStore> ChunkStore for NetworkStore<T, C> {
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        self.backing_chunks.read(id).await
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        self.backing_chunks.store(chunk).await
    }

    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        self.backing_chunks.store_at(chunk, location).await
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        self.backing_chunks.drop(id).await
    }

    async fn locations(&self) -> IoResult<HashSet<Location>> {
        self.backing_chunks.locations().await
    }
}
