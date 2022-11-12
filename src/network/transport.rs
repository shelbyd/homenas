use super::*;

use anyhow::Result;
use async_raft::async_trait::async_trait;
use dashmap::*;
use futures::{stream::*, SinkExt};
use serde::*;
use std::{
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

type RaftConnectMessage = (RaftRequest, oneshot::Sender<RaftResponse>);
type RaftSender = mpsc::Sender<RaftConnectMessage>;

type StringResult<T> = std::result::Result<T, String>;

pub struct Transport {
    peers: DashMap<NodeId, Peer>,
    node_id: NodeId,
    pending_requests: DashMap<(NodeId, RequestId), oneshot::Sender<StringResult<Response>>>,
    raft_requests: RaftSender,
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
pub enum Request {
    Raft(RaftRequest),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftRequest {
    Vote(async_raft::raft::VoteRequest),
    ChangeMembership(HashSet<NodeId>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Raft(RaftResponse),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftResponse {
    Vote(async_raft::raft::VoteResponse),
    ChangeMembership,
}

enum Never {}

impl Transport {
    pub async fn create(
        node_id: NodeId,
        listen_on: u16,
        peers: &[SocketAddr],
        raft_requests: RaftSender,
    ) -> Result<Arc<Self>> {
        let transport = Arc::new(Transport {
            node_id,
            peers: DashMap::default(),
            pending_requests: DashMap::default(),
            raft_requests,
        });

        let listener = Arc::clone(&transport);
        tokio::task::spawn(async move { listener.listener(listen_on).await.log_err() });

        for peer_addr in peers {
            let result = async {
                let stream = TcpStream::connect(peer_addr).await?;
                initial_handshake(node_id, stream, *peer_addr).await
            }
            .await;

            if let Some((peer, receive)) = result.log_err() {
                Arc::clone(&transport).new_peer_connected(peer, receive);
            }
        }

        Ok(transport)
    }

    pub fn raft_members(&self) -> HashSet<NodeId> {
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
        log::info!("Listening for peers on {}", listen_addr);

        let listener = TcpListener::bind(listen_addr).await?;
        loop {
            let (socket, addr) = listener.accept().await?;

            let clone = Arc::clone(&self);
            tokio::task::spawn(async move {
                if let Some((peer, receive)) = initial_handshake(clone.node_id, socket, addr)
                    .await
                    .log_err()
                {
                    clone.new_peer_connected(peer, receive);
                }
            });
        }
    }

    fn new_peer_connected(self: Arc<Self>, peer: Peer, mut receive: Receive) {
        log::info!("Successfully connected to peer: {}", peer.addr);

        let addr = peer.addr;
        let peer_id = peer.node_id;
        self.peers.insert(peer.node_id, peer);

        tokio::task::spawn(async move {
            self.send_raft_request(RaftRequest::ChangeMembership(self.raft_members()))
                .await
                .log_err();

            loop {
                let message = match receive.try_next().await.log_err() {
                    Some(Some(m)) => m,
                    _ => break,
                };

                match message {
                    Message::RequestNodeId | Message::NodeId(_) => {
                        log::warn!("Got handshake message during normal operation");
                    }
                    Message::Request(id, req) => {
                        let resp = self.handle_request(req).await.map_err(|e| {
                            log::error!("{}", e);
                            e.to_string()
                        });
                        self.send_message(peer_id, Message::Response(id, resp))
                            .await
                            .log_err();
                    }
                    Message::Response(id, resp) => {
                        if let Some((_, responder)) = self.pending_requests.remove(&(peer_id, id)) {
                            responder.send(resp).ok();
                        }
                    }
                }
            }
            log::warn!("Closing stream with peer: {}", addr);
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
        self.peers
            .get_mut(&node_id)
            .expect("called request on missing node id")
            .send
            .send(message)
            .await?;

        Ok(())
    }

    async fn handle_request(&self, request: Request) -> Result<Response> {
        match request {
            Request::Raft(req) => self.send_raft_request(req).await.map(Response::Raft),
        }
    }

    async fn send_raft_request(&self, request: RaftRequest) -> Result<RaftResponse> {
        let (tx, rx) = oneshot::channel();
        self.raft_requests.send((request, tx)).await?;
        let resp = rx.await?;
        Ok(resp)
    }
}

async fn initial_handshake(
    my_node_id: NodeId,
    stream: TcpStream,
    addr: SocketAddr,
) -> Result<(Peer, Receive)> {
    let do_connect = async {
        let codec = tokio_serde_cbor::Codec::<Message, Message>::new();
        let mut stream = codec.framed(stream);

        stream.send(Message::RequestNodeId).await?;

        while let Some(message) = stream.try_next().await? {
            match message {
                Message::RequestNodeId => {
                    stream.send(Message::NodeId(my_node_id)).await?;
                }
                Message::NodeId(node_id) => {
                    let (send, receive) = stream.split();
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

#[async_trait]
impl RaftNetwork<LogEntry> for Transport {
    async fn append_entries(
        &self,
        _: u64,
        _: AppendEntriesRequest<LogEntry>,
    ) -> Result<AppendEntriesResponse> {
        todo!("append_entries");
        Err(IoError::Unimplemented.into())
    }

    async fn install_snapshot(
        &self,
        _node_id: u64,
        _req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        log::error!("install_snapshot");
        Err(IoError::Unimplemented.into())
    }

    async fn vote(&self, node_id: u64, req: VoteRequest) -> Result<VoteResponse> {
        let resp = self
            .request(node_id, Request::Raft(RaftRequest::Vote(req)))
            .await?;

        match dbg!(resp) {
            Response::Raft(RaftResponse::Vote(r)) => Ok(r),
            unexpected => {
                anyhow::bail!("Unexpected response: {:?}", unexpected);
            }
        }
    }
}
