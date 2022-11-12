use crate::{chunk_store::*, db::*, io::*};

use async_raft::{error::*, raft::*, *};
use std::{net::SocketAddr, path::Path, sync::Arc};

mod raft;
use raft::*;

pub struct NetworkStore<T: Tree + 'static, C> {
    backing_tree: T,
    backing_chunks: C,
    raft: HomeNasRaft<T>,
}

impl<T: Tree + Clone + 'static, C: ChunkStore> NetworkStore<T, C> {
    pub async fn create(
        backing_tree: T,
        backing_chunks: C,
        _listen_on: u16,
        _peers: &[SocketAddr],
        state_dir: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        let state_dir = state_dir.as_ref();
        let sled = sled::open(state_dir)?;

        let node_id = sled
            .get("node_id")?
            .map(crate::from_slice)
            .unwrap_or(Ok(rand::random()))?;

        let mut network = raft::Network {};
        let mut members = network.discover().await?;
        members.insert(node_id);

        let raft = Raft::new(
            node_id,
            Arc::new(Config::build("HomeNAS".to_string()).validate()?),
            Arc::new(network),
            Arc::new(raft::Storage {
                node_id,
                sled,
                backing: backing_tree.clone(),
            }),
        );
        raft.initialize(members).await?;

        Ok(NetworkStore {
            backing_tree,
            backing_chunks,
            raft,
        })
    }
}

#[async_trait::async_trait]
impl<T: Tree, C: ChunkStore> Tree for NetworkStore<T, C> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        let mut leader = match self.raft.client_read().await {
            Ok(()) => return self.backing_tree.get(key).await,
            Err(ClientReadError::RaftError(e)) => return Err(e.into()),
            Err(ClientReadError::ForwardToLeader(leader)) => leader,
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
