use super::*;

use crate::utils::*;

use anyhow::Result;
use async_raft::async_trait::async_trait;
use dashmap::*;
use futures::{future::*, stream::*, SinkExt};
use serde::*;
use std::{
    net::{Ipv4Addr, ToSocketAddrs},
    sync::Arc,
};
use tokio::{net::*, time::*};
use tokio_serde_cbor::Codec;
use tokio_util::codec::*;

type MessageStream = Framed<TcpStream, Codec<Message, Message>>;
type SendHalf = SplitSink<MessageStream, Message>;
type Receive = SplitStream<MessageStream>;

pub struct Transport {
    peers: DashMap<NodeId, Arc<Peer>>,
    node_id: NodeId,
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
}

enum Never {}

impl Transport {
    pub async fn create(
        node_id: NodeId,
        listen_on: u16,
        peers: &[SocketAddr],
    ) -> Result<Arc<Self>> {
        let transport = Arc::new(Transport {
            node_id,
            peers: DashMap::default(),
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

        let peer = Arc::new(peer);
        self.peers.insert(peer.node_id, Arc::clone(&peer));

        tokio::task::spawn(async move {
            loop {
                let message = match receive.try_next().await.log_err() {
                    Some(Some(m)) => m,
                    _ => break,
                };
            }
            log::error!("Closing stream with peer: {}", peer.addr);
        });
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
        log::error!("append_entries");
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

    async fn vote(&self, _node_id: u64, _req: VoteRequest) -> Result<VoteResponse> {
        log::error!("vote");
        Err(IoError::Unimplemented.into())
    }
}
