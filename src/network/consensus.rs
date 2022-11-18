use super::*;

use dashmap::*;
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
    RefreshLease(String, Duration),
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
            if let Some((node, exp)) = self.current_lease(key) {
                if node == self.node_id {
                    let refresh = self
                        .try_majority::<(), _>(Msg::RefreshLease(
                            key.to_string(),
                            self.config.lease_duration,
                        ))
                        .await?;

                    if let Err(()) = refresh {
                        continue;
                    }

                    return Ok(());
                }

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
        let entry = self.active_leases.get(key)?;
        let (node, exp) = entry.value();

        if *exp <= Instant::now() {
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

        if members.len() <= 1 {
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

            if successes.len() > members.len() / 2 {
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

    async fn on_message(&self, from: NodeId, message: Msg) -> IoResult<Vec<u8>> {
        match message {
            Msg::AcquireLease(key, dur) => {
                todo!();
            }
            Msg::RefreshLease(key, dur) => {
                todo!();
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
        unimplemented!("compare_and_swap");
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     use dashmap::*;
//     use maplit::*;
//     use tokio::{sync::oneshot, time::*};
//
//     #[derive(Default)]
//     struct TestTransport {
//         messages: DashMap<Msg, oneshot::Sender<Vec<u8>>>,
//     }
//
//     #[async_trait::async_trait]
//     impl Transport for TestTransport {
//         async fn request<R: DeserializeOwned>(&self, msg: Msg) -> IoResult<R> {
//             let (tx, rx) = oneshot::channel();
//             self.messages.insert(msg, tx);
//
//             futures::future::pending::<()>().await;
//             todo!();
//         }
//     }
//
//     #[cfg(test)]
//     mod single_node {
//         use super::*;
//
//         #[tokio::test]
//         async fn single_node_reads_from_local() {
//             let mem = MemoryTree::default();
//             let subject = Consensus::new(10, &mem, TestTransport::default());
//
//             mem.set("foo", Some(&[1, 2, 3])).await.unwrap();
//
//             assert_eq!(subject.get("foo").await, Ok(Some(vec![1, 2, 3])));
//         }
//
//         #[tokio::test]
//         async fn set_sets_in_backing() {
//             let mem = MemoryTree::default();
//             let subject = Consensus::new(10, &mem, TestTransport::default());
//
//             let _ = subject.set("foo", Some(&[1, 2, 3])).await;
//
//             assert_eq!(mem.get("foo").await, Ok(Some(vec![1, 2, 3])));
//         }
//     }
//
//     #[cfg(test)]
//     mod three_nodes {
//         use super::*;
//
//         #[tokio::test]
//         async fn set_acquires_lease() {
//             let mem = MemoryTree::default();
//             let transport = TestTransport::default();
//
//             let subject = Consensus::new(10, &mem, &transport);
//             subject.set_members(btreeset! { 10, 11, 12 }).await;
//
//             let _ = timeout(
//                 Duration::from_millis(1),
//                 subject.set("foo", Some(&[1, 2, 3])),
//             )
//             .await;
//
//             assert!(transport
//                 .messages
//                 .contains_key(&Msg::TryAcquireLease("foo".to_string())));
//         }
//     }
// }
