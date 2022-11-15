use crate::{chunk_store::*, db::*, io::*, log_err, utils::*};

use openraft::{error::*, raft::*, *};
use serde::*;
use std::{collections::*, net::SocketAddr, path::Path, sync::Arc};

mod cluster;
use cluster::{Event as ClusterEvent, *};

mod connections;
use connections::*;

mod openraft_storage;
use openraft_storage::*;

type HomeNasRaft<T> = Raft<LogEntry, LogEntryResponse, RaftTransport, OpenRaftStore<T>>;

pub struct NetworkStore<T: Tree + 'static, C> {
    backing_tree: T,
    backing_chunks: C,
    raft: HomeNasRaft<T>,
    cluster: Arc<Cluster<Event, Request>>,
}

enum StrongRead {
    Ok,
    ForwardTo(NodeId),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Event {}

struct RaftTransport(Arc<Cluster<Event, Request>>);

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Raft(RaftRequest),
    Get(String),
    Write(openraft_storage::LogEntry),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftRequest {
    Vote(openraft::types::v070::VoteRequest),
    ChangeMembership(BTreeSet<NodeId>),
    AppendEntries(openraft::AppendEntriesRequest<openraft_storage::LogEntry>),
    InstallSnapshot(openraft::types::v070::InstallSnapshotRequest),
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
        let members = cluster.members();

        let raft = Raft::new(
            node_id,
            Arc::new(Config::build(&["HomeNAS"])?),
            Arc::new(RaftTransport(Arc::clone(&cluster))),
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

        let network_store = Arc::new(NetworkStore {
            backing_tree,
            backing_chunks,
            raft: raft.clone(),
            cluster,
        });

        tokio::task::spawn(monitor_raft_metrics(raft.metrics()));

        let event_listener = Arc::clone(&network_store);
        tokio::task::spawn(async move { event_listener.handle_cluster_events().await });

        Ok(network_store)
    }
}

impl<C: ChunkStore + 'static, T: Tree> NetworkStore<T, C> {
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

        log_err!(self.cluster.request(leader, Request::Write(write)).await)
            .ok_or(IoError::Internal)?
    }

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
                match self
                    .raft
                    .change_membership(self.cluster.members(), true)
                    .await
                {
                    Ok(_) => {}
                    Err(ClientWriteError::ForwardToLeader(_)) => {}
                    Err(e) => anyhow::bail!(e),
                };
                return Ok(());
            }
            ClusterEvent::Request(peer_id, req_id, request) => (peer_id, req_id, request),
            unhandled => {
                log::error!("Unhandled: {:?}", unhandled);
                std::process::exit(1);
            }
        };

        match request {
            Request::Raft(RaftRequest::Vote(rpc)) => {
                self.cluster
                    .respond::<VoteResponse>(peer_id, req_id, self.raft.vote(rpc).await?)
                    .await?;
            }
            Request::Raft(RaftRequest::AppendEntries(rpc)) => {
                self.cluster
                    .respond::<AppendEntriesResponse>(
                        peer_id,
                        req_id,
                        self.raft.append_entries(rpc).await?,
                    )
                    .await?;
            }
            Request::Get(key) => {
                self.cluster
                    .respond::<Option<Vec<u8>>>(peer_id, req_id, self.get(&key).await?)
                    .await?;
            }
            unhandled => {
                log::error!("Unhandled: {:?}", unhandled);
                std::process::exit(1);
            }
        }

        Ok(())
    }
}

// fn handle_cluster_events<T: Tree + 'static, C: ChunkStore + 'static>(
//     this: Arc<NetworkStore<T, C>>,
// ) {
// tokio::task::spawn(async move {
//     while let Some((req, response_tx)) = receiver.recv().await {
//         let this = Arc::clone(&this);
//         tokio::task::spawn(async move {
//             let resp: anyhow::Result<_> = async {
//                 match req {
//                     Request::Raft(RaftRequest::Vote(vote)) => Ok(Response::Raft(
//                         RaftResponse::Vote(this.raft.vote(vote).await?),
//                     )),
//                     Request::Raft(RaftRequest::ChangeMembership(members)) => {
//                         log::info!("Changing membership: {:?}", members);
//                         this.raft.change_membership(members, true).await?;
//                         Ok(Response::Raft(RaftResponse::ChangeMembership))
//                     }
//                     Request::Raft(RaftRequest::AppendEntries(req)) => {
//                         let resp = this.raft.append_entries(req).await?;
//                         Ok(Response::Raft(RaftResponse::AppendEntries(resp)))
//                     }
//                     Request::Raft(RaftRequest::InstallSnapshot(_)) => unimplemented!(),
//                     Request::Get(key) => Ok(Response::Get(this.get(&key).await?)),
//                     Request::Write(req) => Ok(Response::Write(this.do_write(req).await?)),
//                 }
//             }
//             .await;
//             response_tx.send(resp).ok();
//         });
//     }
// });
// }

#[async_trait::async_trait]
impl<T: Tree, C: ChunkStore + 'static> Tree for NetworkStore<T, C> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        match self.strong_read().await? {
            StrongRead::Ok => self.backing_tree.get(key).await,
            StrongRead::ForwardTo(other) => log_err!(
                self.cluster
                    .request::<Option<Vec<u8>>>(other, Request::Get(key.to_string()))
                    .await
            )
            .ok_or(IoError::Internal),
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

#[openraft::async_trait::async_trait]
impl RaftNetwork<LogEntry> for RaftTransport {
    async fn send_append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<LogEntry>,
    ) -> anyhow::Result<AppendEntriesResponse> {
        self.0
            .request(target, Request::Raft(RaftRequest::AppendEntries(rpc)))
            .await
            .map_err(|e| {
                log::error!("Error performing Raft AppendEntries: {}", e);
                e
            })
    }

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse> {
        self.0
            .request(target, Request::Raft(RaftRequest::InstallSnapshot(rpc)))
            .await
            .map_err(|e| {
                log::error!("Error performing Raft InstallSnapshot: {}", e);
                e
            })
    }

    async fn send_vote(&self, target: NodeId, rpc: VoteRequest) -> anyhow::Result<VoteResponse> {
        self.0
            .request(target, Request::Raft(RaftRequest::Vote(rpc)))
            .await
            .map_err(|e| {
                log::error!("Error performing Raft Vote: {}", e);
                e
            })
    }
}
