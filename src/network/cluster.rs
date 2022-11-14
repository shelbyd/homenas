use super::{connections::Event as ConnectionEvent, *};

use ::futures::*;
use dashmap::*;
use serde::{de::DeserializeOwned, *};
use std::net::{Ipv4Addr, SocketAddr};
use tokio::{
    net::*,
    sync::{mpsc::unbounded_channel, oneshot},
    task::*,
};
use tokio_serde_cbor::Codec;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::*;

type RequestId = u64;

pub struct Cluster<N, R> {
    connections: Connections<NodeId, Message<N, R>, tokio_serde_cbor::Error>,

    tasks: Vec<JoinHandle<()>>,
    pending_requests: DashMap<(NodeId, RequestId), oneshot::Sender<Vec<u8>>>,
}

#[derive(Deserialize, Serialize, Debug)]
enum Message<N, R> {
    Notification(N),
    Request(RequestId, R),
    Response(RequestId, Vec<u8>),
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

    // TODO(shelbyd): Add.
    // peers: BTreeMap<NodeId, SocketAddr>,
}

struct HandshakeSuccess {
    stream: TcpStream,
    handshake: Handshake,
}

impl<N, R> Cluster<N, R>
where
    N: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    #[allow(unused)]
    pub async fn new(
        node_id: NodeId,
        listen_on: u16,
        initial_peers: &[SocketAddr],
    ) -> Result<Self, std::io::Error> {
        let mut tasks = Vec::new();

        let listen_addr = (Ipv4Addr::UNSPECIFIED, listen_on);
        let listener = TcpListener::bind(listen_addr).await?;

        let (handshake_tx, mut handshake_rx) = unbounded_channel();

        for peer in initial_peers {
            handshake_tx.send((*peer, None));
        }

        spawn(async move {
            while let Some((socket, addr)) = log_err!(listener.accept().await) {
                handshake_tx.send((addr, Some(socket)));
            }
        });

        let (conn_tx, conn_rx) = unbounded_channel();
        let connections = Connections::new(UnboundedReceiverStream::new(conn_rx));

        spawn(async move {
            while let Some((addr, socket)) = handshake_rx.recv().await {
                let conn_tx = conn_tx.clone();
                spawn(async move {
                    let outgoing = Handshake { my_id: node_id };

                    let success = match log_err!(handshake(addr, socket, outgoing).await) {
                        None => return,
                        Some(ok) => ok,
                    };

                    let (send, recv) = Codec::new().framed(success.stream).split();
                    let recv = recv.filter_map(|result| async move { log_err!(result) });

                    conn_tx.send((success.handshake.my_id, send, recv));
                });
            }
        });

        Ok(Cluster {
            connections,
            tasks,
            pending_requests: DashMap::default(),
        })
    }

    #[allow(unused)]
    pub async fn next_event(&self) -> Event<N, R> {
        loop {
            return match self.connections.next_event().await {
                ConnectionEvent::NewConnection(id) => Event::NewConnection(id),
                ConnectionEvent::Message(id, Message::Notification(n)) => {
                    Event::Notification(id, n)
                }
                ConnectionEvent::Message(id, Message::Request(req_id, r)) => {
                    Event::Request(id, req_id, r)
                }
                ConnectionEvent::Message(id, Message::Response(req_id, response)) => {
                    let responder = match self.pending_requests.remove(&(id, req_id)) {
                        Some(r) => r,
                        None => {
                            log::error!("Got unexpected response from peer {}", id);
                            continue;
                        }
                    };
                    responder.1.send(response);
                    continue;
                }
                ConnectionEvent::Dropped(id) => Event::Dropped(id),
            };
        }
    }

    #[allow(unused)]
    pub async fn broadcast(&self, notification: N) -> anyhow::Result<()> {
        for id in self.connections.active() {
            self.connections
                .send_to(&id, Message::Notification(notification.clone()))
                .await?;
        }

        Ok(())
    }

    #[allow(unused)]
    pub async fn request<Res>(&self, id: NodeId, req: R) -> anyhow::Result<Res>
    where
        Res: DeserializeOwned,
    {
        let (tx, rx) = oneshot::channel();

        let req_id = rand::random();
        self.pending_requests.insert((id, req_id), tx);

        self.connections
            .send_to(&id, Message::Request(req_id, req))
            .await?;

        let data = rx.await?;
        let obj = crate::from_slice(&data)?;
        Ok(obj)
    }

    #[allow(unused)]
    pub async fn respond<Res>(&self, id: NodeId, request_id: u64, res: Res) -> anyhow::Result<()>
    where
        Res: Serialize,
    {
        self.connections
            .send_to(&id, Message::Response(request_id, crate::to_vec(&res)?))
            .await?;
        Ok(())
    }
}

impl<N, R> Drop for Cluster<N, R> {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
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

    Ok(HandshakeSuccess {
        stream: connection.into_inner(),
        handshake: incoming,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use serial_test::serial;
    use std::net::Ipv4Addr;
    use tokio::time::{error::Elapsed, *};

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
        while let Ok(_) = timeout_event(cluster).await {}
    }

    async fn timeout_event(cluster: &Cluster) -> Result<Event, Elapsed> {
        timeout(Duration::from_millis(10), cluster.next_event()).await
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
    #[ignore]
    async fn connects_to_new_through_first() {
        let first_port = 42000;

        let first = Cluster::new(42, first_port, &[]).await.unwrap();
        let second = Cluster::new(43, 42001, &[localhost(first_port)])
            .await
            .unwrap();
        let last = Cluster::new(44, 42002, &[localhost(first_port)])
            .await
            .unwrap();

        drain_events(&first).await;
        drain_events(&second).await;

        assert_eq!(timeout_event(&last).await, Ok(Event::NewConnection(42)));
        assert_eq!(timeout_event(&last).await, Ok(Event::NewConnection(44)));
    }
}
