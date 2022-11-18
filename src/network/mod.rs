use crate::{chunk_store::*, db::*, io::*, log_err, utils::*};

use serde::{de::DeserializeOwned, *};
use std::{net::SocketAddr, path::Path, sync::Arc};

mod cluster;
use cluster::{Event as ClusterEvent, *};

mod connections;
use connections::*;

mod consensus;
use consensus::*;

pub type NodeId = u64;

pub struct NetworkStore<T: Tree + 'static, C> {
    backing_chunks: C,
    cluster: Arc<Cluster<Event, Request>>,
    consensus: Consensus<T, ConsensusTransport>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Event {
    Set(String, Option<Vec<u8>>),
    CompareAndSwap(String, Option<Vec<u8>>, Option<Vec<u8>>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get(String),
    Consensus(consensus::Msg),
}

struct ConsensusTransport {
    cluster: Arc<Cluster<Event, Request>>,
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
        log::info!("Current node id: {node_id}");

        let cluster = Arc::new(Cluster::new(node_id, listen_on, peers).await?);

        let consensus = Consensus::new(
            node_id,
            backing_tree,
            ConsensusTransport {
                cluster: Arc::clone(&cluster),
            },
        );
        let network_store = Arc::new(NetworkStore {
            backing_chunks,
            cluster,
            consensus,
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
                let members = self
                    .cluster
                    .peers()
                    .into_iter()
                    .chain([self.cluster.node_id])
                    .collect();
                self.consensus.set_members(members).await;
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
            Request::Consensus(msg) => {
                if let Some(response) = self.consensus.on_message(peer_id, msg).await? {
                    self.cluster.respond_raw(peer_id, req_id, response).await?;
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: Tree, C: ChunkStore + 'static> Tree for NetworkStore<T, C> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        self.consensus.get(key).await
    }

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        self.consensus.set(key, value).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError>> {
        self.consensus.compare_and_swap(key, old, new).await
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

#[async_trait::async_trait]
impl Transport for ConsensusTransport {
    async fn request<R: DeserializeOwned>(&self, target: NodeId, msg: Msg) -> IoResult<R> {
        log_err!(self.cluster.request(target, Request::Consensus(msg)).await)
            .ok_or(IoError::Internal)
    }
}
