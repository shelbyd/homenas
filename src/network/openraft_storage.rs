// #![allow(unused)]

use crate::log_err;

use openraft::{ErrorSubject::*, ErrorVerb::*, HardState, Snapshot, *};
use serde::*;
use std::{ops::RangeBounds, option::Option::None, path::*};
use tokio::{fs::*, sync::Mutex};

type Result<T> = std::result::Result<T, StorageError>;

pub struct OpenRaftStore {
    sled: sled::Db,
    logs: sled::Tree,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    last_applied_id: Option<LogId>,
    membership: Option<EffectiveMembership>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum LogEntry {
    SetKV(String, Option<Vec<u8>>),
    CompareAndSwap(String, Option<Vec<u8>>, Option<Vec<u8>>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum LogEntryResponse {
    SetKV,
    CompareAndSwap(std::result::Result<(), ()>),
}

impl AppData for LogEntry {}
impl AppDataResponse for LogEntryResponse {}

impl OpenRaftStore {
    #[allow(unused)]
    pub fn new(meta_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = sled::open(meta_path.as_ref().join("raft_state"))?;
        Ok(Self {
            logs: db.open_tree("logs")?,
            sled: db,
            state: Mutex::new(State::default()),
        })
    }

    fn log_key(&self, index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }
}

#[openraft::async_trait::async_trait]
impl RaftStorage<LogEntry, LogEntryResponse> for OpenRaftStore {
    type SnapshotData = File;

    async fn save_hard_state(&self, state: &HardState) -> Result<()> {
        self.sled
            .insert("hard_state", crate::to_vec(state).unwrap())
            .storage(ErrorSubject::HardState, Write)?;
        Ok(())
    }

    async fn read_hard_state(&self) -> Result<Option<HardState>> {
        Ok(self
            .sled
            .get("hard_state")
            .storage(ErrorSubject::HardState, Read)?
            .map(|b| crate::from_slice(b).unwrap()))
    }

    async fn get_log_state(&self) -> Result<LogState> {
        let last_purged_log_id = self
            .sled
            .get("log_state/last_purged")
            .storage(Logs, Read)?
            .map(|b| crate::from_slice(b).unwrap());

        let last_log_id = self
            .sled
            .get("log_state/last_id")
            .storage(Logs, Read)?
            .map(|b| crate::from_slice::<LogId>(b).unwrap());

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
                .storage(Log(entry.log_id), Write)?;

            self.sled
                .insert("log_state/last_id", crate::to_vec(&entry.log_id).unwrap())
                .storage(Logs, Write)?;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&self, _id: LogId) -> Result<()> {
        unimplemented!("delete_conflict_logs_since");
    }

    async fn purge_logs_upto(&self, id: LogId) -> Result<()> {
        for result in self.logs.range(..=(self.log_key(id.index))) {
            let (key, _) = result.storage(Logs, Delete)?;
            self.logs.remove(key).storage(Logs, Delete)?;
        }

        self.sled
            .insert("log_state/last_purged", crate::to_vec(&id).unwrap())
            .storage(Logs, Write)?;

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
        for &entry in entries {
            match &entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Membership(membership) => {
                    let mut lock = self.state.lock().await;
                    lock.membership = Some(EffectiveMembership {
                        membership: membership.clone(),
                        log_id: entry.log_id,
                    });
                }

                unhandled => {
                    unimplemented!("unhandled: {:?}", unhandled);
                }
            }

            self.state.lock().await.last_applied_id = Some(entry.log_id);
        }

        Ok(Vec::new())
    }

    async fn build_snapshot(&self) -> Result<Snapshot<File>> {
        unimplemented!("build_snapshot");
    }

    async fn begin_receiving_snapshot(&self) -> Result<Box<File>> {
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
        unimplemented!("get_current_snapshot");
    }
}

trait ResultExt<T, E> {
    fn storage(self, subject: ErrorSubject, verb: ErrorVerb) -> Result<T>;
}

impl<T, E> ResultExt<T, E> for std::result::Result<T, E>
where
    E: std::fmt::Display,
{
    fn storage(self, subject: ErrorSubject, verb: ErrorVerb) -> Result<T> {
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
            OpenRaftStore::new(tempdir.path()).unwrap()
        })
        .unwrap();
    }
}
