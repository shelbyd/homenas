use crate::{chunk_store::*, db::*, io::*, utils::*};

use async_raft::{error::*, raft::*, *};
use std::{net::SocketAddr, path::Path, sync::Arc};
use tokio::sync::{mpsc, oneshot};

mod raft;
use raft::*;

mod transport;
pub use transport::*;

pub struct NetworkStore<T: Tree + 'static, C> {
    backing_tree: T,
    backing_chunks: C,
    raft: HomeNasRaft<T>,
    transport: Arc<Transport>,
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
            .get("node_id")?
            .map(crate::from_slice)
            .unwrap_or(Ok(rand::random()))?;

        let (raft_request_tx, raft_request_rx) = mpsc::channel(32);

        let transport = Transport::create(node_id, listen_on, peers, raft_request_tx).await?;
        let members = transport.raft_members();

        let raft = Raft::new(
            node_id,
            Arc::new(Config::build("HomeNAS".to_string()).validate()?),
            Arc::clone(&transport),
            Arc::new(raft::Storage {
                node_id,
                sled,
                backing: backing_tree.clone(),
            }),
        );
        raft.initialize(members).await?;

        {
            let raft = raft.clone();
            tokio::spawn(async move {
                loop {
                    log::info!("Raft metrics: {:?}", raft.metrics().borrow());
                    while let Some(()) = raft.metrics().changed().await.log_err() {}
                }
            });
        }

        let network_store = Arc::new(NetworkStore {
            backing_tree,
            backing_chunks,
            raft,
            transport,
        });

        handle_app_requests(raft_request_rx, network_store.clone());

        Ok(network_store)
    }
}

fn handle_app_requests<T: Tree + 'static, C: ChunkStore + 'static>(
    mut receiver: mpsc::Receiver<(Request, oneshot::Sender<anyhow::Result<Response>>)>,
    this: Arc<NetworkStore<T, C>>,
) {
    tokio::task::spawn(async move {
        while let Some((req, response_tx)) = receiver.recv().await {
            log::debug!("Internal message: {:?}", req);
            let resp: anyhow::Result<_> = async {
                match req {
                    Request::Raft(RaftRequest::Vote(vote)) => Ok(Response::Raft(
                        RaftResponse::Vote(this.raft.vote(vote).await?),
                    )),
                    Request::Raft(RaftRequest::ChangeMembership(members)) => {
                        this.raft.change_membership(members).await?;
                        Ok(Response::Raft(RaftResponse::ChangeMembership))
                    }
                    Request::Raft(RaftRequest::AppendEntries(req)) => {
                        let resp = this.raft.append_entries(req).await?;
                        Ok(Response::Raft(RaftResponse::AppendEntries(resp)))
                    }
                    Request::Get(key) => Ok(Response::Get(this.get(&key).await?)),
                }
            }
            .await;
            log::debug!("Internal response: {:?}", resp);
            response_tx.send(resp).ok();
        }
    });
}

#[async_trait::async_trait]
impl<T: Tree, C: ChunkStore> Tree for NetworkStore<T, C> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        let mut maybe_leader = match self.raft.client_read().await {
            Ok(()) => return self.backing_tree.get(key).await,
            Err(ClientReadError::RaftError(e)) => return Err(e.into()),
            Err(ClientReadError::ForwardToLeader(leader)) => leader,
        };

        let leader = loop {
            if let Some(l) = maybe_leader {
                break l;
            }

            let mut metrics = self.raft.metrics();
            metrics.changed().await.map_err(|e| {
                log::error!("{}", e);
                IoError::Internal
            })?;
            maybe_leader = metrics.borrow().current_leader;
        };

        match self
            .transport
            .request(leader, Request::Get(key.to_string()))
            .await
            .log_err()
            .ok_or(IoError::Internal)?
        {
            Response::Get(data) => Ok(data),
            unexpected => {
                log::error!("Unexpected response: {:?}", unexpected);
                Err(IoError::Internal)
            }
        }
    }

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        let req =
            ClientWriteRequest::new(LogEntry::SetKV(key.to_string(), value.map(|v| v.to_vec())));

        let write = self.raft.client_write(req).await;
        let (_req, mut leader) = match write {
            Ok(_) => return Ok(()),
            Err(ClientWriteError::RaftError(e)) => return Err(e.into()),
            Err(ClientWriteError::ForwardToLeader(req, l)) => (req, l),
        };

        while let None = leader {
            let mut metrics = self.raft.metrics();
            metrics.changed().await.map_err(|e| {
                log::error!("{}", e);
                IoError::Internal
            })?;
            leader = metrics.borrow().current_leader;
        }

        log::info!("{:?}", leader);
        todo!("ForwardToLeader");
    }

    async fn compare_and_swap<'p>(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&'p [u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError<'p>>> {
        log::info!("compare_and_swap: {:?}", key);
        self.backing_tree.compare_and_swap(key, old, new).await
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
