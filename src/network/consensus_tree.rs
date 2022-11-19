use super::*;

use dashmap::{mapref::entry::Entry, *};
use tokio::{sync::RwLock, time::*};

pub struct ConsensusTree<T, TP> {
    node_id: NodeId,

    backing: T,
    transport: TP,

    active_locks: DashMap<String, Lock>,
    acquiring: DashMap<String, NotifyOnDrop>,
    config: Config,

    peers: RwLock<HashSet<NodeId>>,
}

pub struct Config {
    unlock_after_idle: Duration,
}

struct Lock {
    node: NodeId,
    alive_at: Instant,
    notify_on_drop: NotifyOnDrop,
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn request<R: DeserializeOwned>(&self, peer: NodeId, message: Msg) -> IoResult<R>;
}

#[derive(Serialize, Deserialize, Debug)]
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
    pub fn new(node_id: NodeId, backing: T, transport: TP, config: Config) -> ConsensusTree<T, TP> {
        ConsensusTree {
            node_id,
            backing,
            transport,

            active_locks: Default::default(),
            acquiring: Default::default(),
            peers: Default::default(),
            config,
        }
    }

    #[allow(dead_code)]
    pub async fn set_peers(&self, peers: impl IntoIterator<Item = NodeId>) {
        *self.peers.write().await = peers.into_iter().filter(|&n| n != self.node_id).collect();
    }

    #[allow(dead_code)]
    pub async fn on_message(&self, from: NodeId, message: Msg) -> IoResult<Vec<u8>> {
        match message {
            Msg::Set(key, value) => {
                // TODO(shelbyd): Protect with lock?
                self.backing.set(&key, opt_slice(&value)).await?;
                Ok(crate::to_vec(&()).unwrap())
            }
            Msg::Get(key) => {
                let d = self.backing.get(&key).await?;
                Ok(crate::to_vec(&d).unwrap())
            }

            Msg::Lock(key) => {
                let result = match self.active_locks.entry(key) {
                    Entry::Occupied(mut o) if o.get().node == from => {
                        o.get_mut().alive_at = Instant::now();
                        Ok(())
                    }

                    Entry::Occupied(o)
                        if o.get().alive_at.elapsed() < self.config.unlock_after_idle =>
                    {
                        Err(())
                    }

                    Entry::Occupied(mut o) => {
                        o.insert(Lock::new(from));
                        Ok(())
                    }

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
            if let Some(mut notify_lock) = self.acquiring.get_mut(key) {
                let rx = notify_lock.push();
                drop(notify_lock);
                let _ = rx.await;
                continue;
            }

            let entry = self.active_locks.entry(key.to_string());
            match entry {
                Entry::Occupied(o)
                    if o.get().alive_at.elapsed() >= self.config.unlock_after_idle =>
                {
                    o.remove();
                }

                Entry::Occupied(mut o) if o.get().node == self.node_id => {
                    o.get_mut().alive_at = Instant::now();
                    drop(o);

                    if self.try_propagate_lock(key).await? {
                        return Ok(());
                    }
                }

                Entry::Vacant(v) => {
                    // TODO(shelbyd): Maybe some threading problems around here?
                    self.acquiring
                        .insert(key.to_string(), NotifyOnDrop::default());
                    v.insert(Lock::new(self.node_id));

                    let acquired = self.try_propagate_lock(key).await?;
                    let _notify = self.acquiring.remove(key);
                    if acquired {
                        return Ok(());
                    }
                }

                Entry::Occupied(mut o) => {
                    let unlocked_at = o.get().alive_at + self.config.unlock_after_idle;
                    let rx = o.get_mut().notify_on_drop.push();
                    drop(o);

                    tokio::select! {
                        _ = rx => {}
                        _ = sleep_until(unlocked_at) => {}
                    }
                }
            }
        }
    }

    async fn try_propagate_lock(&self, key: &str) -> IoResult<bool> {
        let peers = self.peers.read().await.clone();
        let futs = peers.into_iter().map(|peer| async move {
            self.transport
                .request(peer, Msg::Lock(key.to_string()))
                .await
        });
        let results: Vec<Result<(), ()>> = try_join_all(futs).await?;

        let successes = results.iter().filter(|r| r.is_ok()).count();
        let success = successes >= results.len() / 2;

        if !success {
            self.unlock(key).await?;
        }

        Ok(success)
    }

    async fn unlock(&self, key: &str) -> IoResult<()> {
        self.active_locks
            .remove_if(key, |_, l| l.node == self.node_id);

        let peers = self.peers.read().await.clone();
        let futs = peers.into_iter().map(|peer| async move {
            self.transport
                .request::<()>(peer, Msg::Unlock(key.to_string()))
                .await
        });
        join_all(futs).await.into_iter().collect()
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

        let result = self.backing.compare_and_swap(key, old, new).await?;
        if let Ok(()) = result {
            for peer in &*self.peers.read().await {
                self.transport
                    .request(*peer, Msg::Set(key.to_string(), opt_vec(new)))
                    .await?;
            }
        }

        // TODO(shelbyd): Why does explicit unlock here make tests fail?
        // self.unlock(key).await?;

        Ok(result)
    }
}

impl Lock {
    fn new(node: NodeId) -> Lock {
        Lock {
            node,
            alive_at: Instant::now(),
            notify_on_drop: NotifyOnDrop::default(),
        }
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            unlock_after_idle: Duration::from_millis(250),
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
                Config::default(),
            ));

            self.nodes.insert(node_id, Arc::clone(&tree));

            for node in self.nodes.values() {
                node.set_peers(self.nodes.keys().cloned()).await;
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

            let upgrade = self.inner.nodes.read().await[&peer].upgrade();
            let peer = upgrade.ok_or(IoError::NotFound)?;
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

    // TODO(shelbyd): This has failed sometimes?
    #[test_log::test(tokio::test)]
    async fn compare_exchange_increments_atomically() {
        let cluster = Arc::new(Cluster::sized(5).await);

        let total: u32 = 50;

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

    // TODO(shelbyd): Prevent stale read after disconnected from network.
}
