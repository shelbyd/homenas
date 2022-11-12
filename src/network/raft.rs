use super::*;

use anyhow::Result;
use async_raft::{async_trait::async_trait, storage::*};
use serde::*;
use tokio::fs::*;

pub type HomeNasRaft<T> = Raft<LogEntry, Response, Transport, Storage<T>>;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum LogEntry {
    SetKV(String, Option<Vec<u8>>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Response {
    SetKV,
}

pub struct Storage<T: Tree> {
    pub node_id: u64,
    pub sled: sled::Db,
    pub backing: T,
}

impl AppData for LogEntry {}

impl AppDataResponse for Response {}

#[async_trait]
impl<T: Tree + 'static> RaftStorage<LogEntry, Response> for Storage<T> {
    type Snapshot = File;
    type ShutdownError = IoError;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        log::error!("----- UNIMPLEMENTED -----: get_membership_config");
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
        log::error!("----- UNIMPLEMENTED -----: get_log_entries");
        Err(IoError::Unimplemented.into())
    }

    async fn delete_logs_from(&self, _start: u64, _end: Option<u64>) -> Result<()> {
        log::error!("----- UNIMPLEMENTED -----: delete_logs_from");
        Err(IoError::Unimplemented.into())
    }

    async fn append_entry_to_log(&self, entry: &Entry<LogEntry>) -> Result<()> {
        self.sled
            .insert(format!("log/{}", entry.index), crate::to_vec(entry)?)?;
        Ok(())
    }

    async fn replicate_to_log(&self, _entries: &[Entry<LogEntry>]) -> Result<()> {
        log::error!("----- UNIMPLEMENTED -----: replicate_to_log");
        Err(IoError::Unimplemented.into())
    }

    async fn apply_entry_to_state_machine(
        &self,
        _index: &u64,
        entry: &LogEntry,
    ) -> Result<Response> {
        match entry {
            LogEntry::SetKV(key, value) => {
                self.backing
                    .set(key, value.as_ref().map(Vec::as_slice))
                    .await?;
                Ok(Response::SetKV)
            }
        }
    }

    async fn replicate_to_state_machine(&self, _entries: &[(&u64, &LogEntry)]) -> Result<()> {
        log::error!("----- UNIMPLEMENTED -----: replicate_to_state_machine");
        Err(IoError::Unimplemented.into())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<File>> {
        log::error!("----- UNIMPLEMENTED -----: do_log_compaction");
        Err(IoError::Unimplemented.into())
    }

    async fn create_snapshot(&self) -> Result<(String, Box<File>)> {
        log::error!("----- UNIMPLEMENTED -----: create_snapshot");
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
        log::error!("----- UNIMPLEMENTED -----: finalize_snapshot_installation");
        Err(IoError::Unimplemented.into())
    }

    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<File>>> {
        log::warn!("TODO(shelbyd): Returning empty snapsot");
        Ok(None)
    }
}
