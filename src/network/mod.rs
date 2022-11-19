use crate::{chunk_store::*, db::*, io::*, log_err, utils::*};

use futures::future::*;
use serde::{de::DeserializeOwned, *};
use std::{net::SocketAddr, sync::Arc};

mod cluster;
use cluster::{Event as ClusterEvent, *};

mod connections;
use connections::*;

mod consensus_tree;
use consensus_tree::*;

pub type NodeId = u64;

pub struct NetworkStore<T: Tree + 'static, C> {
    backing_chunks: C,
    cluster: Arc<Cluster<Event, Request>>,
    consensus: ConsensusTree<T, ConsensusTransport>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Event {}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Consensus(consensus_tree::Msg),

    Locations,
    ReadChunk(String),
    StoreChunkAt(Vec<u8>, Location),
    Drop(String),
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
    ) -> anyhow::Result<Arc<Self>> {
        let node_id = rand::random();
        log::info!("Current node id: {node_id}");

        let cluster = Arc::new(Cluster::new(node_id, listen_on, peers).await?);

        let consensus = ConsensusTree::new(
            node_id,
            backing_tree,
            ConsensusTransport {
                cluster: Arc::clone(&cluster),
            },
            consensus_tree::Config::default(),
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
                let members = self.cluster.peers();
                self.consensus.set_peers(members).await;
                return Ok(());
            }
            ClusterEvent::Request(peer_id, req_id, request) => (peer_id, req_id, request),
            ClusterEvent::Notification(_peer_id, event) => match event {},
        };

        // TODO(shelbyd): Pass errors back to requester.
        match request {
            Request::Consensus(msg) => {
                let response = self.consensus.on_message(peer_id, msg).await?;
                self.cluster.respond_raw(peer_id, req_id, response).await?;
            }

            Request::Locations => {
                self.cluster
                    .respond::<HashSet<Location>>(
                        peer_id,
                        req_id,
                        self.backing_chunks.locations().await?,
                    )
                    .await?;
            }

            Request::StoreChunkAt(chunk, location) => {
                self.cluster
                    .respond::<String>(
                        peer_id,
                        req_id,
                        self.backing_chunks.store_at(&chunk, &location).await?,
                    )
                    .await?;
            }
            Request::ReadChunk(id) => {
                self.cluster
                    .respond(peer_id, req_id, self.backing_chunks.read(&id).await.ok())
                    .await?;
            }
            Request::Drop(id) => {
                self.cluster
                    .respond::<()>(peer_id, req_id, self.backing_chunks.drop(&id).await?)
                    .await?;
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
        if let Ok(data) = self.backing_chunks.read(id).await {
            return Ok(data);
        }

        for peer in self.cluster.peers() {
            log::info!("{id}: Checking peer {peer}");
            if let Some(Some(data)) = log_err!(
                self.cluster
                    .request(peer, Request::ReadChunk(id.to_string()))
                    .await
            ) {
                log::info!("{id}: Found on peer {peer}");
                return Ok(data);
            }
        }

        log::info!("{id}: Not found");
        Err(IoError::NotFound)
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        self.backing_chunks.store(chunk).await
    }

    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        if self.backing_chunks.locations().await?.contains(location) {
            return self.backing_chunks.store_at(chunk, location).await;
        }

        for peer in self.cluster.peers() {
            let locations: HashSet<Location> =
                log_err!(self.cluster.request(peer, Request::Locations).await)
                    .ok_or(IoError::Internal)?;
            if locations.contains(location) {
                return log_err!(
                    self.cluster
                        .request::<String>(
                            peer,
                            Request::StoreChunkAt(chunk.to_vec(), location.clone())
                        )
                        .await
                )
                .ok_or(IoError::Internal);
            }
        }

        Err(IoError::NotFound)
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        // TODO(shelbyd): Drop as Event.
        self.backing_chunks.drop(id).await?;

        let futs = self.cluster.peers().into_iter().map(|peer| async move {
            log_err!(
                self.cluster
                    .request::<()>(peer, Request::Drop(id.to_string()))
                    .await
            )
            .ok_or(IoError::Internal)
        });
        join_all(futs).await.into_iter().collect()
    }

    async fn locations(&self) -> IoResult<HashSet<Location>> {
        let local = self.backing_chunks.locations().await?;

        let futs = self.cluster.peers().into_iter().map(|peer| async move {
            log_err!(
                self.cluster
                    .request::<HashSet<_>>(peer, Request::Locations)
                    .await
            )
            .ok_or(IoError::Internal)
        });
        Ok(try_join_all(futs)
            .await?
            .into_iter()
            .flat_map(|h| h)
            .chain(local)
            .collect())
    }
}

#[async_trait::async_trait]
impl Transport for ConsensusTransport {
    async fn request<R: DeserializeOwned>(&self, target: NodeId, msg: Msg) -> IoResult<R> {
        log_err!(self.cluster.request(target, Request::Consensus(msg)).await)
            .ok_or(IoError::Internal)
    }
}
