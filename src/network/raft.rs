use super::*;

use anyhow::Result;
use async_raft::{async_trait::async_trait, raft::*, storage::*};
use serde::*;
use tokio::fs::*;

pub type HomeNasRaft = Raft<LogEntry, Response, Network, Storage>;

#[derive(Clone, Serialize, Deserialize)]
pub struct LogEntry {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Response {}

pub struct Network {}

pub struct Storage {
    pub node_id: u64,
    pub sled: sled::Db,
}

impl Network {
    pub async fn discover(&mut self) -> anyhow::Result<HashSet<NodeId>> {
        log::warn!("TODO(shelbyd): Implement network discovery, or at least use args");
        Ok(HashSet::default())
    }
}

impl AppData for LogEntry {}

impl AppDataResponse for Response {}

#[async_trait]
impl RaftNetwork<LogEntry> for Network {
    async fn append_entries(
        &self,
        _: u64,
        _: AppendEntriesRequest<LogEntry>,
    ) -> Result<AppendEntriesResponse> {
        log::error!("append_entries");
        Err(IoError::Unimplemented.into())
    }

    async fn install_snapshot(
        &self,
        _node_id: u64,
        _req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        log::error!("install_snapshot");
        Err(IoError::Unimplemented.into())
    }

    async fn vote(&self, _node_id: u64, _req: VoteRequest) -> Result<VoteResponse> {
        log::error!("vote");
        Err(IoError::Unimplemented.into())
    }
}

#[async_trait]
impl RaftStorage<LogEntry, Response> for Storage {
    type Snapshot = File;
    type ShutdownError = IoError;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        log::error!("get_membership_config");
        Err(IoError::Unimplemented.into())
    }

    async fn get_initial_state(&self) -> Result<InitialState> {
        log::warn!("TODO(shelbyd): Get all values");

        let mut base = InitialState::new_initial(self.node_id);

        if let Some(hard_state) = self.sled.get("hard_state")? {
            base.hard_state = crate::from_slice(hard_state)?;
        }

        Ok(InitialState::new_initial(self.node_id))
    }

    async fn save_hard_state(&self, state: &HardState) -> Result<()> {
        self.sled.insert("hard_state", crate::to_vec(state)?)?;
        Ok(())
    }

    async fn get_log_entries(&self, _start: u64, _end: u64) -> Result<Vec<Entry<LogEntry>>> {
        log::error!("get_log_entries");
        Err(IoError::Unimplemented.into())
    }

    async fn delete_logs_from(&self, _start: u64, _end: Option<u64>) -> Result<()> {
        log::error!("delete_logs_from");
        Err(IoError::Unimplemented.into())
    }

    async fn append_entry_to_log(&self, entry: &Entry<LogEntry>) -> Result<()> {
        self.sled
            .insert(format!("log/{}", entry.index), crate::to_vec(entry)?)?;
        Ok(())
    }

    async fn replicate_to_log(&self, _entries: &[Entry<LogEntry>]) -> Result<()> {
        log::error!("replicate_to_log");
        Err(IoError::Unimplemented.into())
    }

    async fn apply_entry_to_state_machine(
        &self,
        _index: &u64,
        _data: &LogEntry,
    ) -> Result<Response> {
        log::error!("apply_entry_to_state_machine");
        Err(IoError::Unimplemented.into())
    }

    async fn replicate_to_state_machine(&self, _entries: &[(&u64, &LogEntry)]) -> Result<()> {
        log::error!("replicate_to_state_machine");
        Err(IoError::Unimplemented.into())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<File>> {
        log::error!("do_log_compaction");
        Err(IoError::Unimplemented.into())
    }

    async fn create_snapshot(&self) -> Result<(String, Box<File>)> {
        log::error!("create_snapshot");
        Err(IoError::Unimplemented.into())
    }

    async fn finalize_snapshot_installation(
        &self,
        _index: u64,
        _term: u64,
        _delete_through: Option<u64>,
        _id: String,
        _snapshot: Box<File>,
    ) -> Result<()> {
        log::error!("finalize_snapshot_installation");
        Err(IoError::Unimplemented.into())
    }

    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<File>>> {
        log::warn!("TODO(shelbyd): Returning empty snapsot");
        Ok(None)
    }
}
