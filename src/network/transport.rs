use super::*;

use anyhow::Result;
use dashmap::*;
use futures::{stream::*, SinkExt};
use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, ToSocketAddrs},
    sync::Arc,
};
use tokio::{
    net::*,
    sync::{mpsc, oneshot},
    time::*,
};
use tokio_serde_cbor::Codec;
use tokio_util::codec::*;

type MessageStream = Framed<TcpStream, Codec<Message, Message>>;
type SendHalf = SplitSink<MessageStream, Message>;
type Receive = SplitStream<MessageStream>;

type RequestId = u64;

type RaftConnectMessage = (Request, oneshot::Sender<Result<Response>>);
type AppSender = mpsc::Sender<RaftConnectMessage>;

type StringResult<T> = std::result::Result<T, String>;

pub struct Transport {
    peers: DashMap<NodeId, Peer>,
    node_id: NodeId,
    pending_requests: DashMap<(NodeId, RequestId), oneshot::Sender<StringResult<Response>>>,
    app_requests: AppSender,
}

struct Peer {
    node_id: NodeId,
    addr: SocketAddr,
    send: SendHalf,
}

#[derive(Serialize, Deserialize, Debug)]
enum Message {
    RequestNodeId,
    NodeId(NodeId),
    Request(u64, Request),
    Response(u64, StringResult<Response>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Raft(RaftResponse),
    Get(Option<Vec<u8>>),
    Write(LogEntryResponse),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftResponse {
    Vote(openraft::types::v070::VoteResponse),
    ChangeMembership,
    AppendEntries(AppendEntriesResponse),
}

enum Never {}

impl Transport {
    pub async fn create(
        node_id: NodeId,
        listen_on: u16,
        peers: &[SocketAddr],
        app_requests: AppSender,
    ) -> Result<Arc<Self>> {
        let transport = Arc::new(Transport {
            node_id,
            peers: DashMap::default(),
            pending_requests: DashMap::default(),
            app_requests,
        });

        let listener = Arc::clone(&transport);
        tokio::task::spawn(async move {
            if let Err(e) = listener.listener(listen_on).await {
                log::error!("Listener failed: {}", e);
            }
            log::info!("Listener task terminated");
        });

        for peer_addr in peers {
            let result = async {
                let stream = try_connect_for(Duration::from_secs(2), *peer_addr).await?;
                initial_handshake(node_id, stream, *peer_addr).await
            }
            .await;

            if let Some((peer, receive)) = log_err!(result) {
                Arc::clone(&transport).new_peer_connected(peer, receive);
            }
        }

        Ok(transport)
    }

    pub fn raft_members(&self) -> BTreeSet<NodeId> {
        self.peers
            .iter()
            .map(|entry| *entry.key())
            .chain([self.node_id])
            .collect()
    }

    async fn listener(self: Arc<Self>, port: u16) -> Result<Never> {
        let listen_addr = (Ipv4Addr::UNSPECIFIED, port)
            .to_socket_addrs()?
            .next()
            .unwrap();

        let listener = TcpListener::bind(listen_addr).await?;
        log::info!("Listening for peers on {}", listen_addr);

        loop {
            log::info!("Waiting for new incoming connection");
            if let Some((socket, addr)) = log_err!(listener.accept().await) {
                log::info!("Incoming connection from {}", addr);

                let clone = Arc::clone(&self);
                tokio::task::spawn(async move {
                    if let Some((peer, receive)) =
                        log_err!(initial_handshake(clone.node_id, socket, addr).await)
                    {
                        clone.new_peer_connected(peer, receive);
                    }
                });
            }
        }
    }

    fn new_peer_connected(self: Arc<Self>, peer: Peer, mut receive: Receive) {
        log::info!("Successfully connected to peer: {}", peer.addr);

        let addr = peer.addr;
        let peer_id = peer.node_id;
        self.peers.insert(peer.node_id, peer);

        tokio::task::spawn(async move {
            self.send_application_request(Request::Raft(RaftRequest::ChangeMembership(
                self.raft_members(),
            )))
            .await
            .ok();

            loop {
                let message = match log_err!(receive.try_next().await) {
                    Some(Some(m)) => m,
                    _ => break,
                };

                match message {
                    Message::RequestNodeId | Message::NodeId(_) => {
                        log::warn!("Got handshake message during normal operation");
                    }
                    Message::Request(id, req) => {
                        let resp = self.send_application_request(req).await.map_err(|e| {
                            log::error!("{}", e);
                            e.to_string()
                        });
                        log_err!(
                            self.send_message(peer_id, Message::Response(id, resp))
                                .await
                        );
                    }
                    Message::Response(id, resp) => {
                        if let Some((_, responder)) = self.pending_requests.remove(&(peer_id, id)) {
                            responder.send(resp).ok();
                        }
                    }
                }
            }

            log::warn!("Lost connection to peer: {}", addr);
            self.peers.remove(&peer_id);
            self.send_application_request(Request::Raft(RaftRequest::ChangeMembership(
                self.raft_members(),
            )))
            .await
            .ok();
        });
    }

    pub async fn request(&self, node_id: NodeId, request: Request) -> Result<Response> {
        let request_id = rand::random();

        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pending_requests.insert((node_id, request_id), sender);

        self.send_message(node_id, Message::Request(request_id, request))
            .await?;

        receiver.await?.map_err(|e| anyhow::anyhow!("{}", e))
    }

    async fn send_message(&self, node_id: NodeId, message: Message) -> Result<()> {
        let mut peer = match self.peers.get_mut(&node_id) {
            Some(p) => p,
            None => {
                anyhow::bail!("Attempting to send message to missing node: {}", node_id)
            }
        };

        peer.send.send(message).await?;
        Ok(())
    }

    async fn send_application_request(&self, request: Request) -> Result<Response> {
        let (tx, rx) = oneshot::channel();
        self.app_requests.send((request, tx)).await?;
        let resp = rx.await?;
        resp
    }
}

async fn try_connect_for(dur: Duration, addr: SocketAddr) -> Result<TcpStream> {
    log::info!(
        "Trying to connect to {} with timeout {}s",
        addr,
        dur.as_secs_f32()
    );

    let start = Instant::now();

    loop {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(_) if start.elapsed() < dur => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => {
                log::warn!("Failed to connect to peer {}", addr);
                return Err(e.into());
            }
        }
    }
}

async fn initial_handshake(
    my_node_id: NodeId,
    stream: TcpStream,
    addr: SocketAddr,
) -> Result<(Peer, Receive)> {
    log::info!("Attempting initial handshake with {}", addr);
    let do_connect = async {
        let codec = tokio_serde_cbor::Codec::<Message, Message>::new();
        let stream = codec.framed(stream);
        let (mut send, mut receive) = stream.split();

        send.send(Message::RequestNodeId).await?;

        while let Some(message) = receive.try_next().await? {
            match message {
                Message::RequestNodeId => {
                    send.send(Message::NodeId(my_node_id)).await?;
                }
                Message::NodeId(node_id) => {
                    return anyhow::Ok((
                        Peer {
                            node_id,
                            addr,
                            send,
                        },
                        receive,
                    ));
                }
                unexpected => {
                    anyhow::bail!(
                        "Unexpected message from peer {} during handshake: {:?}",
                        addr,
                        unexpected
                    );
                }
            }
        }

        anyhow::bail!("Stream closed before connection established");
    };

    Ok(timeout(Duration::from_secs(5), do_connect).await??)
}

#[openraft::async_trait::async_trait]
impl openraft::RaftNetwork<openraft_storage::LogEntry> for Transport {
    async fn send_append_entries(
        &self,
        target: u64,
        rpc: openraft::AppendEntriesRequest<openraft_storage::LogEntry>,
    ) -> Result<openraft::AppendEntriesResponse> {
        let resp = self
            .request(target, Request::Raft(RaftRequest::AppendEntries(rpc)))
            .await?;

        match resp {
            Response::Raft(RaftResponse::AppendEntries(r)) => Ok(r),
            unhandled => {
                anyhow::bail!("Unexpected response: {:?}", unhandled);
            }
        }
    }

    async fn send_install_snapshot(
        &self,
        _target: u64,
        _rpc: openraft::types::v070::InstallSnapshotRequest,
    ) -> Result<openraft::types::v070::InstallSnapshotResponse> {
        unimplemented!("send_install_snapshot");
    }

    async fn send_vote(
        &self,
        target: u64,
        rpc: openraft::types::v070::VoteRequest,
    ) -> Result<openraft::types::v070::VoteResponse> {
        let resp = self
            .request(target, Request::Raft(RaftRequest::Vote(rpc)))
            .await?;

        match resp {
            Response::Raft(RaftResponse::Vote(r)) => Ok(r),
            unhandled => {
                anyhow::bail!("Unexpected response: {:?}", unhandled);
            }
        }
    }
}
