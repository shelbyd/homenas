use super::*;

use dashmap::{mapref::entry::Entry, *};
use tokio::sync::{oneshot, RwLock};

pub struct ConsensusTree<T, TP> {
    node_id: NodeId,

    backing: T,
    transport: TP,

    active_locks: DashMap<String, Lock>,

    peers: RwLock<HashSet<NodeId>>,
}

struct Lock {
    node: NodeId,
    wakers: Vec<oneshot::Sender<()>>,
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn request<R: DeserializeOwned>(&self, peer: NodeId, message: Msg) -> IoResult<R>;
}

pub enum Msg {
    Set(String, Option<Vec<u8>>),
    Get(String),

    Lock(String),
    Unlock(String),
}

impl<T, TP> ConsensusTree<T, TP>
where
    T: Tree,
    TP: Transport,
{
    #[allow(dead_code)]
    pub fn new(node_id: NodeId, backing: T, transport: TP) -> ConsensusTree<T, TP> {
        ConsensusTree {
            node_id,
            backing,
            transport,

            active_locks: DashMap::default(),
            peers: RwLock::new(HashSet::new()),
        }
    }

    #[allow(dead_code)]
    pub async fn set_peers(&self, peers: HashSet<NodeId>) {
        *self.peers.write().await = peers.into_iter().filter(|&n| n != self.node_id).collect();
    }

    #[allow(dead_code)]
    pub async fn on_message(&self, from: NodeId, message: Msg) -> IoResult<Vec<u8>> {
        match message {
            Msg::Set(key, value) => {
                self.backing.set(&key, opt_slice(&value)).await?;
                Ok(crate::to_vec(&()).unwrap())
            }
            Msg::Get(key) => {
                let d = self.backing.get(&key).await?;
                Ok(crate::to_vec(&d).unwrap())
            }

            Msg::Lock(key) => {
                let result = match self.active_locks.entry(key) {
                    Entry::Occupied(_) => Err(()),
                    Entry::Vacant(v) => {
                        v.insert(Lock::new(from));
                        Ok(())
                    }
                };

                Ok(crate::to_vec(&result).unwrap())
            }
            Msg::Unlock(key) => {
                self.active_locks
                    .remove_if(&key, |_, lock| lock.node == from);
                Ok(crate::to_vec(&()).unwrap())
            }
        }
    }

    async fn lock(&self, key: &str) -> IoResult<()> {
        loop {
            let entry = self.active_locks.entry(key.to_string());
            match entry {
                Entry::Occupied(mut o) => {
                    let (tx, rx) = oneshot::channel();
                    o.get_mut().wakers.push(tx);
                    drop(o);

                    let _ = rx.await;
                }
                Entry::Vacant(v) => {
                    v.insert(Lock::new(self.node_id));

                    let mut acquired = true;
                    for peer in &*self.peers.read().await {
                        let result: Result<(), ()> = self
                            .transport
                            .request(*peer, Msg::Lock(key.to_string()))
                            .await?;
                        acquired = acquired && result.is_ok();
                    }

                    if acquired {
                        break;
                    }

                    self.unlock(key).await?;
                }
            }
        }

        Ok(())
    }

    async fn unlock(&self, key: &str) -> IoResult<()> {
        self.active_locks.remove(key);

        for peer in &*self.peers.read().await {
            self.transport
                .request(*peer, Msg::Unlock(key.to_string()))
                .await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: Tree, TP: Transport> Tree for ConsensusTree<T, TP> {
    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        if let Some(data) = self.backing.get(key).await? {
            return Ok(Some(data));
        }

        for peer in &*self.peers.read().await {
            if let Some(data) = self
                .transport
                .request(*peer, Msg::Get(key.to_string()))
                .await?
            {
                return Ok(Some(data));
            }
        }

        Ok(None)
    }

    async fn set(&self, key: &str, value: Option<&[u8]>) -> IoResult<()> {
        self.backing.set(key, value).await?;

        for peer in &*self.peers.read().await {
            self.transport
                .request(*peer, Msg::Set(key.to_string(), opt_vec(value)))
                .await?;
        }

        Ok(())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> IoResult<Result<(), CompareAndSwapError>> {
        self.lock(key).await?;
        log::info!("{}: Acquired lock on {key}", self.node_id);

        let result = self.backing.compare_and_swap(key, old, new).await?;
        if let Ok(()) = result {
            for peer in &*self.peers.read().await {
                self.transport
                    .request(*peer, Msg::Set(key.to_string(), opt_vec(new)))
                    .await?;
            }
        }

        self.unlock(key).await?;
        Ok(result)
    }
}

impl Lock {
    fn new(node: NodeId) -> Lock {
        Lock {
            node,
            wakers: Vec::new(),
        }
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        for waker in self.wakers.drain(..) {
            let _ = waker.send(());
        }
    }
}

#[async_trait::async_trait]
impl<P> Transport for P
where
    P: std::ops::Deref + Send + Sync,
    P::Target: Transport,
{
    async fn request<R: DeserializeOwned>(&self, peer: NodeId, msg: Msg) -> IoResult<R> {
        (**self).request(peer, msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::{Transport, *};

    use std::{collections::HashMap, sync::Weak};
    use tokio::time::*;

    type ConsensusTreeBound = ConsensusTree<MemoryTree, TestTransport>;

    #[derive(Default)]
    struct Cluster {
        nodes: HashMap<NodeId, Arc<ConsensusTreeBound>>,
        transport: Arc<TransportInner>,
    }

    struct TestTransport {
        node_id: NodeId,
        inner: Arc<TransportInner>,
    }

    #[derive(Default)]
    struct TransportInner {
        nodes: RwLock<HashMap<NodeId, Weak<ConsensusTreeBound>>>,
    }

    impl Cluster {
        async fn sized(count: usize) -> Self {
            let mut cluster = Cluster::default();
            for n in 0..count {
                cluster.add_node(n as u64).await;
            }
            cluster
        }

        async fn add_node(&mut self, node_id: NodeId) {
            let tree = Arc::new(ConsensusTree::new(
                node_id,
                MemoryTree::default(),
                TestTransport {
                    node_id,
                    inner: Arc::clone(&self.transport),
                },
            ));

            self.nodes.insert(node_id, Arc::clone(&tree));

            let peers: HashSet<_> = self.nodes.keys().cloned().collect();
            for node in self.nodes.values() {
                node.set_peers(peers.clone()).await;
            }

            self.transport
                .nodes
                .write()
                .await
                .insert(node_id, Arc::downgrade(&tree));
        }
    }

    #[async_trait::async_trait]
    impl Transport for TestTransport {
        async fn request<R: DeserializeOwned>(&self, peer: NodeId, message: Msg) -> IoResult<R> {
            assert_ne!(self.node_id, peer);

            tokio::time::sleep(tokio::time::Duration::from_millis(
                (rand::random::<f32>() * 2.) as u64 + 1,
            ))
            .await;

            let peer = self.inner.nodes.read().await[&peer]
                .upgrade()
                .ok_or(IoError::NotFound)?;
            let data = peer.on_message(self.node_id, message).await?;
            Ok(crate::from_slice(&data).unwrap())
        }
    }

    #[tokio::test]
    async fn single_node() {
        let cluster = Cluster::sized(1).await;

        cluster.nodes[&0]
            .set("foo", Some(&[1, 2, 3]))
            .await
            .unwrap();

        assert_eq!(cluster.nodes[&0].get("foo").await, Ok(Some(vec![1, 2, 3])));
        assert_eq!(
            cluster.nodes[&0].backing.get("foo").await,
            Ok(Some(vec![1, 2, 3]))
        );

        cluster.nodes[&0]
            .compare_and_swap("foo", Some(&[1, 2, 3]), Some(&[4, 5, 6]))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(cluster.nodes[&0].get("foo").await, Ok(Some(vec![4, 5, 6])));
    }

    #[tokio::test]
    async fn two_nodes_set_gets_on_other() {
        let cluster = Cluster::sized(2).await;

        cluster.nodes[&0]
            .set("foo", Some(&[1, 2, 3]))
            .await
            .unwrap();

        assert_eq!(cluster.nodes[&1].get("foo").await, Ok(Some(vec![1, 2, 3])));
    }

    #[tokio::test]
    async fn new_node_reads_set() {
        let mut cluster = Cluster::sized(2).await;

        cluster.nodes[&0]
            .set("foo", Some(&[1, 2, 3]))
            .await
            .unwrap();

        cluster.add_node(2).await;

        assert_eq!(cluster.nodes[&2].get("foo").await, Ok(Some(vec![1, 2, 3])));
    }

    #[test_log::test(tokio::test)]
    async fn compare_exchange_increments_atomically() {
        let cluster = Arc::new(Cluster::sized(3).await);

        let total: u32 = 20;

        let mut tasks = Vec::new();
        for i in 0..total {
            let cluster = Arc::clone(&cluster);
            tasks.push(tokio::task::spawn(async move {
                let node = i % cluster.nodes.len() as u32;
                let node = &cluster.nodes[&(node as u64)];

                update_typed(node, "count", |v| {
                    let next: u32 = v.unwrap_or_default() + 1;
                    Ok((Some(next), ()))
                })
                .await
                .unwrap();
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(cluster.nodes[&0].get_typed("count").await, Ok(Some(total)));
    }
}
