use super::*;

use dashmap::{mapref::entry::Entry, *};
use futures::future::*;
use serde::de::DeserializeOwned;
use std::collections::*;
use tokio::sync::RwLock;
use tokio::time::*;

pub struct Consensus<T, TP> {
    node_id: NodeId,
    backing: T,
    transport: TP,

    active_leases: DashMap<String, (NodeId, Instant)>,

    config: Config,
    members: RwLock<BTreeSet<NodeId>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub enum Msg {
    AcquireLease(String, Duration),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
enum LeaseAcquisition {
    Ok,
    Leased(NodeId, Duration),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct Leased(NodeId, Duration);

pub struct Config {
    lease_duration: Duration,
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn request<R: DeserializeOwned>(&self, target: NodeId, msg: Msg) -> IoResult<R>;
}

impl<T, TP> Consensus<T, TP>
where
    TP: Transport,
{
    #[allow(dead_code)]
    pub fn new(node_id: NodeId, backing: T, transport: TP) -> Self {
        Consensus {
            node_id,
            backing,
            transport,

            active_leases: DashMap::default(),
            config: Config::default(),
            members: RwLock::new(BTreeSet::new()),
        }
    }

    #[allow(dead_code)]
    pub async fn set_members(&self, members: BTreeSet<NodeId>) {
        *self.members.write().await = members;
    }

    async fn lease(&self, key: &str) -> IoResult<()> {
        loop {
            if let Some((_, exp)) = self.current_lease(key) {
                sleep_until(exp).await;
            }

            let acq = self
                .try_majority(Msg::AcquireLease(
                    key.to_string(),
                    self.config.lease_duration,
                ))
                .await?;

            match acq {
                Ok(()) => {
                    self.active_leases.insert(
                        key.to_string(),
                        (self.node_id, Instant::now() + self.config.lease_duration),
                    );
                    return Ok(());
                }
                Err(Leased(to, dur)) => {
                    self.active_leases
                        .insert(key.to_string(), (to, Instant::now() + dur));
                    sleep(dur).await;
                }
            }
        }
    }

    fn current_lease(&self, key: &str) -> Option<(NodeId, Instant)> {
        let entry_locked = self.active_leases.get(key)?;
        let (node, exp) = entry_locked.value();

        if *exp <= Instant::now() {
            drop(entry_locked);
            self.active_leases.remove(key);
            None
        } else {
            Some((*node, *exp))
        }
    }

    async fn try_majority<R, E>(&self, msg: Msg) -> IoResult<Result<R, E>>
    where
        R: DeserializeOwned + Default,
        E: DeserializeOwned,
    {
        let members = self.members.read().await.clone();
        let successes_needed = (members.len() - 1) / 2;

        if successes_needed == 0 {
            return Ok(Ok(R::default()));
        }

        let mut successes: Vec<R> = Vec::new();
        let mut errors: Vec<E> = Vec::new();
        let mut failure: Option<IoError> = None;

        let mut futs = members
            .iter()
            .filter(|&id| *id != self.node_id)
            .map(|id| self.transport.request(*id, msg.clone()))
            .collect::<Vec<_>>();

        loop {
            let (done, _, f) = select_all(futs).await;
            futs = f;

            match done {
                Ok(Ok(r)) => successes.push(r),
                Ok(Err(e)) => errors.push(e),
                Err(err) => failure = Some(err),
            }

            if successes.len() >= successes_needed {
                return Ok(Ok(successes.remove(0)));
            }

            if futs.len() == 0 {
                break;
            }
        }

        errors
            .pop()
            .map(|e| Ok(Err(e)))
            .unwrap_or_else(|| Err(failure.unwrap()))
    }

    pub async fn on_message(&self, from: NodeId, message: Msg) -> IoResult<Option<Vec<u8>>> {
        match message {
            Msg::AcquireLease(key, dur) => {
                let result = match self.active_leases.entry(key) {
                    Entry::Occupied(entry) if entry.get().1 > Instant::now() => {
                        Err(Leased(entry.get().0, entry.get().1 - Instant::now()))
                    }
                    Entry::Occupied(mut can_replace) => {
                        can_replace.insert((from, Instant::now() + dur));
                        Ok(())
                    }
                    Entry::Vacant(v) => {
                        v.insert((from, Instant::now() + dur));
                        Ok(())
                    }
                };
                Ok(Some(crate::to_vec(&result).unwrap()))
            }
        }
    }
}

#[async_trait::async_trait]
impl<T: Tree, TP: Transport> Tree for Consensus<T, TP> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        self.backing.get(key).await
    }

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        self.lease(key).await?;
        self.backing.set(key, value).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError>> {
        self.lease(key).await?;
        self.backing.compare_and_swap(key, old, new).await
    }
}

#[async_trait::async_trait]
impl<P> Transport for P
where
    P: std::ops::Deref + Send + Sync,
    P::Target: Transport,
{
    async fn request<R: DeserializeOwned>(&self, target: NodeId, msg: Msg) -> IoResult<R> {
        (**self).request(target, msg).await
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            lease_duration: Duration::from_secs(1),
        }
    }
}
