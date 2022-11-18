use super::{connections::Event as ConnectionEvent, *};

use ::futures::*;
use dashmap::*;
use serde::de::DeserializeOwned;
use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr},
};
use tokio::{
    net::*,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot, Mutex,
    },
    task::*,
    time::*,
};
use tokio_serde_cbor::Codec;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::*;

type RequestId = u64;

pub struct Cluster<N, R> {
    pub node_id: NodeId,

    connections: Connections<NodeId, SocketAddr, Message<N, R>, tokio_serde_cbor::Error>,

    pending_requests: DashMap<(NodeId, RequestId), oneshot::Sender<Vec<u8>>>,
    peers: Mutex<BTreeMap<NodeId, SocketAddr>>,

    handshake_tx: UnboundedSender<(SocketAddr, Option<TcpStream>)>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
enum Message<N, R> {
    Notification(N),
    Request(RequestId, R),
    Response(RequestId, Vec<u8>),

    Internal(Internal),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
enum Internal {
    KnownPeers(BTreeMap<NodeId, SocketAddr>),
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Event<N, R> {
    NewConnection(NodeId),
    Notification(NodeId, N),
    Request(NodeId, RequestId, R),
    Dropped(NodeId),
}

#[derive(Deserialize, Serialize)]
struct Handshake {
    my_id: NodeId,
    listening_on: u16,
}

struct HandshakeSuccess {
    stream: TcpStream,
    handshake: Handshake,
    peer_addr: SocketAddr,
}

impl<N, R> Cluster<N, R>
where
    N: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    #[allow(dead_code)]
    pub async fn new(
        node_id: NodeId,
        listen_on: u16,
        initial_peers: &[SocketAddr],
    ) -> anyhow::Result<Self> {
        let listen_addr = (Ipv4Addr::UNSPECIFIED, listen_on);
        let listener = TcpListener::bind(listen_addr).await?;

        let (handshake_tx, mut handshake_rx) = unbounded_channel();

        for peer in initial_peers {
            handshake_tx.send((*peer, None))?;
        }

        let handshake_new = handshake_tx.clone();
        spawn(async move {
            log::info!("{}: Starting listener", node_id);
            while let Some((socket, addr)) = log_err!(listener.accept().await) {
                log_err!(handshake_new.send((addr, Some(socket))));
            }
            log::error!("{}: Listener terminated", node_id);
        });

        let (conn_tx, conn_rx) = unbounded_channel();
        let connections = Connections::new(UnboundedReceiverStream::new(conn_rx));

        spawn(async move {
            while let Some((addr, socket)) = handshake_rx.recv().await {
                log::info!("{}: Trying handshake with {}", node_id, addr);
                let conn_tx = conn_tx.clone();
                spawn(async move {
                    let outgoing = Handshake {
                        my_id: node_id,
                        listening_on: listen_on,
                    };

                    let success = match log_err!(handshake(addr, socket, outgoing).await) {
                        None => return,
                        Some(ok) => ok,
                    };

                    let (send, recv) = Codec::new().framed(success.stream).split();
                    let recv = recv.filter_map(|result| async move { log_err!(result) });

                    let mut peer_addr = success.peer_addr;
                    peer_addr.set_port(success.handshake.listening_on);

                    log_err!(conn_tx.send((success.handshake.my_id, peer_addr, send, recv)));

                    log::info!(
                        "{}: Successfully connected to {}",
                        node_id,
                        success.handshake.my_id
                    );
                });
            }
        });

        Ok(Cluster {
            node_id,
            connections,
            pending_requests: DashMap::default(),
            peers: Mutex::default(),
            handshake_tx,
        })
    }

    #[allow(dead_code)]
    pub async fn next_event(&self) -> Event<N, R> {
        loop {
            return match self.connections.next_event().await {
                ConnectionEvent::NewConnection(id, addr) => {
                    let mut peers = self.peers.lock().await;
                    peers.insert(id, addr);
                    let peers = peers.clone();

                    log::info!("{}: Broadcasting new peers: {:?}", self.node_id, peers);
                    log_err!(self.broadcast_internal(Internal::KnownPeers(peers)).await);

                    Event::NewConnection(id)
                }
                ConnectionEvent::Dropped(id) => {
                    self.peers.lock().await.remove(&id);
                    Event::Dropped(id)
                }
                ConnectionEvent::Message(id, Message::Notification(n)) => {
                    Event::Notification(id, n)
                }
                ConnectionEvent::Message(id, Message::Request(req_id, r)) => {
                    Event::Request(id, req_id, r)
                }
                ConnectionEvent::Message(id, Message::Response(req_id, response)) => {
                    let (_, responder) = match self.pending_requests.remove(&(id, req_id)) {
                        Some(r) => r,
                        None => {
                            log::error!("Got unexpected response from peer {id}");
                            continue;
                        }
                    };
                    if let Err(_) = responder.send(response) {
                        log::error!("{id}: Failed to deliver response to request {req_id}",);
                    } else {
                        log::info!("{id}: Sent response to {req_id}");
                    }
                    continue;
                }
                ConnectionEvent::Message(_, Message::Internal(Internal::KnownPeers(peers))) => {
                    log::info!("{}: Received new peers: {:?}", self.node_id, peers);
                    let known_here = self.peers.lock().await;

                    for (id, addr) in peers {
                        if id != self.node_id && !known_here.contains_key(&id) {
                            log::info!("{}: Connecting to new peer {}", self.node_id, addr);
                            log_err!(self.handshake_tx.send((addr, None)));
                        }
                    }
                    continue;
                }
            };
        }
    }

    #[allow(dead_code)]
    pub async fn broadcast(&self, notification: N) -> anyhow::Result<()> {
        for id in self.connections.active() {
            self.connections
                .send_to(&id, Message::Notification(notification.clone()))
                .await?;
        }

        Ok(())
    }

    async fn broadcast_internal(&self, internal: Internal) -> anyhow::Result<()> {
        for id in self.connections.active() {
            self.connections
                .send_to(&id, Message::Internal(internal.clone()))
                .await?;
        }

        Ok(())
    }

    pub async fn request<Res>(&self, id: NodeId, req: R) -> anyhow::Result<Res>
    where
        Res: DeserializeOwned,
    {
        let (tx, rx) = oneshot::channel();

        let req_id = rand::random();
        log::info!("{}: Sending request {id}", self.node_id);
        self.pending_requests.insert((id, req_id), tx);

        self.connections
            .send_to(&id, Message::Request(req_id, req))
            .await?;

        let data = timeout(Duration::from_secs(5), rx).await??;
        let obj = crate::from_slice(&data)?;
        Ok(obj)
    }

    #[allow(dead_code)]
    pub async fn respond<Res>(&self, id: NodeId, request_id: u64, res: Res) -> anyhow::Result<()>
    where
        Res: Serialize,
    {
        self.respond_raw(id, request_id, crate::to_vec(&res)?).await
    }

    pub async fn respond_raw(
        &self,
        id: NodeId,
        request_id: u64,
        data: Vec<u8>,
    ) -> anyhow::Result<()> {
        self.connections
            .send_to(&id, Message::Response(request_id, data))
            .await?;
        Ok(())
    }

    pub fn peers(&self) -> impl Iterator<Item = NodeId> {
        self.connections.active().into_iter()
    }
}

async fn handshake(
    addr: SocketAddr,
    connection: Option<TcpStream>,
    outgoing: Handshake,
) -> anyhow::Result<HandshakeSuccess> {
    let connection = match connection {
        Some(c) => c,
        None => TcpStream::connect(addr).await?,
    };

    let mut connection = Codec::new().framed(connection);

    connection.send(outgoing).await?;

    let incoming = match connection.next().await {
        None => anyhow::bail!(
            "Remote connection {} closed without sending handshake",
            addr
        ),
        Some(hs) => hs?,
    };

    let raw = connection.into_inner();

    Ok(HandshakeSuccess {
        peer_addr: raw.peer_addr()?,
        stream: raw,
        handshake: incoming,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use serial_test::serial;
    use std::net::Ipv4Addr;
    use tokio::time::error::Elapsed;

    const TEN_MS: Duration = Duration::from_millis(10);

    fn localhost(port: u16) -> SocketAddr {
        (Ipv4Addr::LOCALHOST, port).into()
    }

    #[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
    enum TestNotification {
        String(String),
    }

    #[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
    enum TestRequest {
        Ping(String),
    }

    #[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
    struct Pong(String);

    type Cluster = super::Cluster<TestNotification, TestRequest>;
    type Event = super::Event<TestNotification, TestRequest>;

    async fn drain_events(cluster: &Cluster) {
        while let Ok(event) = timeout_event(cluster).await {
            log::info!("Received event: {:?}", event);
        }
    }

    async fn timeout_event(cluster: &Cluster) -> Result<Event, Elapsed> {
        timeout(TEN_MS, cluster.next_event()).await
    }

    async fn loop_events(cluster: Cluster) {
        while let Ok(_) = timeout(Duration::from_millis(100), cluster.next_event()).await {
            // Tests are too eager about polling first cluster without a sleep.
            sleep(Duration::from_micros(100)).await;
        }
    }

    #[tokio::test]
    #[serial]
    async fn connect_to_node() {
        let first = Cluster::new(42, 42000, &[]).await.unwrap();
        Cluster::new(43, 42001, &[localhost(42000)]).await.unwrap();

        assert_eq!(first.next_event().await, Event::NewConnection(43));
    }

    #[tokio::test]
    #[serial]
    async fn notification_goes_to_other() {
        let first = Cluster::new(42, 42000, &[]).await.unwrap();
        let second = Cluster::new(43, 42001, &[localhost(42000)]).await.unwrap();
        drain_events(&first).await;
        drain_events(&second).await;

        first
            .broadcast(TestNotification::String("foo".to_string()))
            .await
            .unwrap();

        assert_eq!(
            timeout_event(&second).await,
            Ok(Event::Notification(
                42,
                TestNotification::String("foo".to_string())
            ))
        );
    }

    #[tokio::test]
    #[serial]
    async fn request_goes_to_other() {
        let first = Cluster::new(42, 42000, &[]).await.unwrap();
        let second = Cluster::new(43, 42001, &[localhost(42000)]).await.unwrap();
        drain_events(&first).await;
        drain_events(&second).await;

        spawn(async move {
            first
                .request::<()>(43, TestRequest::Ping("foo".to_string()))
                .await
        });

        match timeout_event(&second).await {
            Ok(Event::Request(42, _, TestRequest::Ping(foo))) => {
                assert_eq!(&foo, "foo");
            }
            unexpected => panic!("Unexpected event: {:?}", unexpected),
        }
    }

    #[tokio::test]
    #[serial]
    async fn response_back_to_first() {
        let first = Arc::new(Cluster::new(42, 42000, &[]).await.unwrap());
        let second = Cluster::new(43, 42001, &[localhost(42000)]).await.unwrap();
        drain_events(&first).await;
        drain_events(&second).await;

        let clone = Arc::clone(&first);
        let request = spawn(async move {
            clone
                .request(43, TestRequest::Ping("foo".to_string()))
                .await
        });

        match timeout_event(&second).await {
            Ok(Event::Request(42, id, TestRequest::Ping(p))) => {
                second.respond(42, id, Pong(p)).await.unwrap();
            }
            unexpected => panic!("Unexpected event: {:?}", unexpected),
        }
        drain_events(&first).await;

        let joined: anyhow::Result<Pong> = timeout(TEN_MS, request).await.unwrap().unwrap();
        assert_eq!(joined.unwrap(), Pong("foo".to_string()));
    }

    #[tokio::test]
    #[serial]
    async fn disconnects_from_other() {
        let first = Cluster::new(42, 42000, &[]).await.unwrap();
        let second = Cluster::new(43, 42001, &[localhost(42000)]).await.unwrap();
        drain_events(&first).await;
        drain_events(&second).await;

        drop(second);

        assert_eq!(timeout_event(&first).await, Ok(Event::Dropped(43)));
    }

    #[tokio::test]
    #[serial]
    async fn connects_to_new_through_first() {
        let first_port = 42001;

        let first = Cluster::new(1, first_port, &[]).await.unwrap();
        spawn(loop_events(first));

        let second = Cluster::new(2, 42002, &[localhost(first_port)])
            .await
            .unwrap();
        spawn(loop_events(second));

        let last = Cluster::new(3, 42003, &[localhost(first_port)])
            .await
            .unwrap();

        assert_eq!(timeout_event(&last).await, Ok(Event::NewConnection(1)));
        assert_eq!(timeout_event(&last).await, Ok(Event::NewConnection(2)));
    }
}
