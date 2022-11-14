use crate::{chunk_store::*, db::*, io::*, log_err, utils::*};

use openraft::{error::*, raft::*, *};
use std::{net::SocketAddr, path::Path, sync::Arc};
use tokio::sync::{mpsc, oneshot};

mod connections;

mod openraft_storage;
use openraft_storage::*;

mod transport;
pub use transport::*;

pub struct NetworkStore<T: Tree + 'static, C> {
    backing_tree: T,
    backing_chunks: C,
    raft: HomeNasRaft<T>,
    transport: Arc<Transport>,
}

enum StrongRead {
    Ok,
    ForwardTo(NodeId),
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

        let (raft_request_tx, raft_request_rx) = mpsc::channel(32);

        let transport = Transport::create(node_id, listen_on, peers, raft_request_tx).await?;
        let members = transport.raft_members();

        let raft = Raft::new(
            node_id,
            Arc::new(Config::build(&["HomeNAS"])?),
            Arc::clone(&transport),
            Arc::new(OpenRaftStore::new(sled, backing_tree.clone())?),
        );

        log::info!("Initializing raft with members: {:?}", members);
        match raft.initialize(members.clone()).await {
            Ok(()) => {}
            Err(InitializeError::NotAllowed) => {
                log::info!("Got not allowed, cluster should be running");
            }
            Err(InitializeError::Fatal(e)) => anyhow::bail!(e),
        }
        log::info!("Raft initialized");

        tokio::task::spawn(monitor_raft_metrics(raft.metrics()));

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

impl<C: ChunkStore, T: Tree> NetworkStore<T, C> {
    async fn strong_read(&self) -> IoResult<StrongRead> {
        let maybe_leader = match self.raft.client_read().await {
            Ok(()) => return Ok(StrongRead::Ok),
            Err(ClientReadError::Fatal(e)) => {
                log::error!("{}", e);
                return Err(IoError::Internal);
            }
            Err(ClientReadError::QuorumNotEnough(e)) => {
                log::error!("{:?}", e);
                return Err(IoError::Internal);
            }
            Err(ClientReadError::ForwardToLeader(leader)) => leader.leader_id,
        };

        Ok(StrongRead::ForwardTo(self.get_leader(maybe_leader).await?))
    }

    async fn get_leader(&self, mut maybe_leader: Option<NodeId>) -> IoResult<NodeId> {
        loop {
            if let Some(l) = maybe_leader {
                return Ok(l);
            }

            let mut metrics = self.raft.metrics();
            log_err!(metrics.changed().await).ok_or(IoError::Internal)?;
            maybe_leader = metrics.borrow().current_leader;
        }
    }

    async fn do_write(&self, write: LogEntry) -> IoResult<LogEntryResponse> {
        let maybe_leader = match self.raft.client_write(write.clone()).await {
            Err(ClientWriteError::Fatal(e)) => {
                log::error!("{}", e);
                return Err(IoError::Internal);
            }
            Err(ClientWriteError::ChangeMembershipError(e)) => {
                log::error!("{}", e);
                return Err(IoError::Internal);
            }
            Err(ClientWriteError::ForwardToLeader(leader)) => leader.leader_id,
            Ok(resp) => return Ok(resp.data),
        };

        let leader = self.get_leader(maybe_leader).await?;

        match log_err!(self.transport.request(leader, Request::Write(write)).await)
            .ok_or(IoError::Internal)?
        {
            Response::Write(r) => Ok(r),
            unexpected => {
                log::error!("Unexpected response: {:?}", unexpected);
                Err(IoError::Internal)
            }
        }
    }
}

fn handle_app_requests<T: Tree + 'static, C: ChunkStore + 'static>(
    mut receiver: mpsc::Receiver<(Request, oneshot::Sender<anyhow::Result<Response>>)>,
    this: Arc<NetworkStore<T, C>>,
) {
    tokio::task::spawn(async move {
        while let Some((req, response_tx)) = receiver.recv().await {
            let this = Arc::clone(&this);
            tokio::task::spawn(async move {
                let resp: anyhow::Result<_> = async {
                    match req {
                        Request::Raft(RaftRequest::Vote(vote)) => Ok(Response::Raft(
                            RaftResponse::Vote(this.raft.vote(vote).await?),
                        )),
                        Request::Raft(RaftRequest::ChangeMembership(members)) => {
                            log::info!("Changing membership: {:?}", members);
                            this.raft.change_membership(members, true).await?;
                            Ok(Response::Raft(RaftResponse::ChangeMembership))
                        }
                        Request::Raft(RaftRequest::AppendEntries(req)) => {
                            let resp = this.raft.append_entries(req).await?;
                            Ok(Response::Raft(RaftResponse::AppendEntries(resp)))
                        }
                        Request::Get(key) => Ok(Response::Get(this.get(&key).await?)),
                        Request::Write(req) => Ok(Response::Write(this.do_write(req).await?)),
                    }
                }
                .await;
                response_tx.send(resp).ok();
            });
        }
    });
}

#[async_trait::async_trait]
impl<T: Tree, C: ChunkStore> Tree for NetworkStore<T, C> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        match self.strong_read().await? {
            StrongRead::Ok => self.backing_tree.get(key).await,
            StrongRead::ForwardTo(other) => {
                match log_err!(
                    self.transport
                        .request(other, Request::Get(key.to_string()))
                        .await
                )
                .ok_or(IoError::Internal)?
                {
                    Response::Get(data) => Ok(data),
                    unexpected => {
                        log::error!("Unexpected response: {:?}", unexpected);
                        Err(IoError::Internal)
                    }
                }
            }
        }
    }

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        self.do_write(LogEntry::SetKV(key.to_string(), value.map(|v| v.to_vec())))
            .await?;

        Ok(())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError>> {
        let result = self
            .do_write(LogEntry::CompareAndSwap(
                key.to_string(),
                opt_vec(old),
                opt_vec(new),
            ))
            .await?;

        match result {
            LogEntryResponse::CompareAndSwap(r) => Ok(r),
            unexpected => {
                log::error!("Unexpected: {:?}", unexpected);
                Err(IoError::Internal)
            }
        }
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

async fn monitor_raft_metrics(
    mut metrics: tokio::sync::watch::Receiver<RaftMetrics>,
) -> anyhow::Result<()> {
    let mut prev = metrics.borrow().clone();

    log::info!("Initial metrics");
    log::info!("  current_leader: {:?}", prev.current_leader);
    log::info!(
        "  membership: {:?}",
        prev.membership_config.membership.all_nodes()
    );

    loop {
        metrics.changed().await?;
        let new = metrics.borrow().clone();

        if let Some((prev_leader, new_leader)) = diff(prev.current_leader, new.current_leader) {
            log::info!("Leader change: {:?} -> {:?}", prev_leader, new_leader);
            if new_leader == Some(new.id) {
                log::info!("This node is now the leader");
            }
        }

        if let Some((prev, new)) = diff(
            prev.membership_config.membership.all_nodes(),
            new.membership_config.membership.all_nodes(),
        ) {
            log::info!("Membership change: {:?} -> {:?}", prev, new);
        }

        prev = new;
    }
}
