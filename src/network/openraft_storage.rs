use super::*;

use openraft::{ErrorSubject::*, ErrorVerb::*, HardState, Snapshot};
use serde::*;
use std::{ops::RangeBounds, option::Option::None};
use tokio::{fs::*, sync::Mutex};

type Result<T> = std::result::Result<T, StorageError>;
pub type HomeNasRaft<T> = Raft<LogEntry, LogEntryResponse, crate::Transport, OpenRaftStore<T>>;

pub struct OpenRaftStore<T> {
    sled: sled::Db,
    logs: sled::Tree,
    state: Mutex<State>,

    #[allow(unused)]
    backing: T,
}

#[derive(Default)]
struct State {
    last_applied_id: Option<LogId>,
    last_purged_log_id: Option<LogId>,
    membership: Option<EffectiveMembership>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum LogEntry {
    SetKV(String, Option<Vec<u8>>),
    CompareAndSwap(String, Option<Vec<u8>>, Option<Vec<u8>>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum LogEntryResponse {
    RaftOp,
    Error(String),

    SetKV,
    CompareAndSwap(std::result::Result<(), CompareAndSwapError>),
}

impl AppData for LogEntry {}
impl AppDataResponse for LogEntryResponse {}

impl<T: Tree> OpenRaftStore<T> {
    pub fn new(db: sled::Db, backing: T) -> anyhow::Result<Self> {
        Ok(Self {
            logs: db.open_tree("logs")?,
            sled: db,
            state: Mutex::new(State::default()),
            backing,
        })
    }

    fn log_key(&self, index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    async fn handle_request(&self, req: &LogEntry) -> anyhow::Result<LogEntryResponse> {
        match req {
            LogEntry::CompareAndSwap(key, old, new) => {
                let r = self
                    .backing
                    .compare_and_swap(key, opt_slice(&old), opt_slice(&new))
                    .await?;
                Ok(LogEntryResponse::CompareAndSwap(r))
            }
            LogEntry::SetKV(key, value) => {
                self.backing.set(key, opt_slice(value)).await?;
                Ok(LogEntryResponse::SetKV)
            }
        }
    }
}

#[openraft::async_trait::async_trait]
impl<T: Tree + 'static> RaftStorage<LogEntry, LogEntryResponse> for OpenRaftStore<T> {
    type SnapshotData = File;

    async fn save_hard_state(&self, state: &HardState) -> Result<()> {
        self.sled
            .insert("hard_state", crate::to_vec(state).unwrap())
            .storage_err(ErrorSubject::HardState, Write)?;
        Ok(())
    }

    async fn read_hard_state(&self) -> Result<Option<HardState>> {
        Ok(self
            .sled
            .get("hard_state")
            .storage_err(ErrorSubject::HardState, Read)?
            .map(|b| crate::from_slice(b).unwrap()))
    }

    async fn get_log_state(&self) -> Result<LogState> {
        let last_purged_log_id = self.state.lock().await.last_purged_log_id;

        let last_log_id = self
            .logs
            .iter()
            .next_back()
            .transpose()
            .storage_err(Logs, Read)?
            .map(|(_, b)| crate::from_slice::<Entry<LogEntry>>(b).unwrap().log_id);

        let last_log_id = core::cmp::max(last_purged_log_id, last_log_id);

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send + Sync>(
        &self,
        range: RB,
    ) -> Result<Vec<Entry<LogEntry>>> {
        let range = (
            range.start_bound().map(|i| self.log_key(*i)),
            range.end_bound().map(|i| self.log_key(*i)),
        );

        Ok(self
            .logs
            .range(range)
            .filter_map(|result| {
                let (_, bytes) = log_err!(result)?;
                log_err!(crate::from_slice(bytes))
            })
            .collect())
    }

    async fn append_to_log(&self, entries: &[&Entry<LogEntry>]) -> Result<()> {
        for &entry in entries {
            let index = self.log_key(entry.log_id.index);
            self.logs
                .insert(index, crate::to_vec(entry).unwrap())
                .storage_err(Log(entry.log_id), Write)?;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&self, id: LogId) -> Result<()> {
        for result in self.logs.range(self.log_key(id.index)..) {
            let (key, _) = result.storage_err(Logs, Delete)?;
            self.logs.remove(key).storage_err(Logs, Delete)?;
        }

        Ok(())
    }

    async fn purge_logs_upto(&self, id: LogId) -> Result<()> {
        for result in self.logs.range(..=(self.log_key(id.index))) {
            let (key, _) = result.storage_err(Logs, Delete)?;
            self.logs.remove(key).storage_err(Logs, Delete)?;
        }

        self.state.lock().await.last_purged_log_id = Some(id);

        Ok(())
    }

    async fn last_applied_state(&self) -> Result<(Option<LogId>, Option<EffectiveMembership>)> {
        let lock = self.state.lock().await;
        Ok((lock.last_applied_id, lock.membership.as_ref().cloned()))
    }

    async fn apply_to_state_machine(
        &self,
        entries: &[&Entry<LogEntry>],
    ) -> Result<Vec<LogEntryResponse>> {
        let mut result = Vec::new();

        for &entry in entries {
            match &entry.payload {
                EntryPayload::Blank => {
                    result.push(LogEntryResponse::RaftOp);
                }
                EntryPayload::Membership(membership) => {
                    let mut lock = self.state.lock().await;
                    lock.membership = Some(EffectiveMembership {
                        membership: membership.clone(),
                        log_id: entry.log_id,
                    });
                    result.push(LogEntryResponse::RaftOp);
                }

                EntryPayload::Normal(req) => {
                    let response = self.handle_request(req).await;
                    match response {
                        Ok(r) => result.push(r),
                        Err(e) => result.push(LogEntryResponse::Error(e.to_string())),
                    }
                }
            }

            self.state.lock().await.last_applied_id = Some(entry.log_id);
        }

        Ok(result)
    }

    async fn build_snapshot(&self) -> Result<Snapshot<File>> {
        log::warn!("Unimplemented: build_snapshot");
        unimplemented!("build_snapshot");
    }

    async fn begin_receiving_snapshot(&self) -> Result<Box<File>> {
        log::warn!("Unimplemented: begin_receiving_snapshot");
        unimplemented!("begin_receiving_snapshot");
    }

    async fn install_snapshot(
        &self,
        _meta: &SnapshotMeta,
        _snapshot: Box<File>,
    ) -> Result<StateMachineChanges> {
        unimplemented!("install_snapshot");
    }

    async fn get_current_snapshot(&self) -> Result<Option<Snapshot<File>>> {
        log::warn!("Unimplemented: get_current_snapshot");
        Ok(None)
    }
}

trait ResultExt<T, E> {
    fn storage_err(self, subject: ErrorSubject, verb: ErrorVerb) -> Result<T>;
}

impl<T, E> ResultExt<T, E> for std::result::Result<T, E>
where
    E: std::fmt::Display,
{
    fn storage_err(self, subject: ErrorSubject, verb: ErrorVerb) -> Result<T> {
        self.map_err(|e| StorageError::IO {
            source: StorageIOError::new(subject, verb, AnyError::error(e)),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "LogState")]
struct LogStateDef {
    last_log_id: Option<LogId>,
    last_purged_log_id: Option<LogId>,
}

#[derive(Deserialize)]
struct Helper(#[serde(with = "LogStateDef")] LogState);

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test]
    fn openraft_suite() {
        openraft::testing::Suite::test_all(|| async {
            let tempdir = tempfile::tempdir().unwrap();
            OpenRaftStore::new(
                sled::open(tempdir.path().join("sled")).unwrap(),
                MemoryTree::default(),
            )
            .unwrap()
        })
        .unwrap();
    }
}
