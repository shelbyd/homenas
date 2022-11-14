use super::*;

use ::futures::*;
use serde::*;
use std::net::{Ipv4Addr, SocketAddr};
use tokio::{net::*, sync::mpsc::unbounded_channel, task::*};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::*;

pub struct Cluster {
    #[allow(unused)]
    connections: Connections<NodeId, Message, tokio_serde_cbor::Error>,
}

#[derive(Deserialize, Serialize)]
enum Message {}

#[derive(Deserialize, Serialize)]
struct Handshake {
    my_id: u64,
    // TODO(shelbyd): Add.
    // peers: BTreeMap<u64, SocketAddr>,
}

struct HandshakeSuccess {
    stream: TcpStream,
    handshake: Handshake,
}

impl Cluster {
    #[allow(unused)]
    pub async fn new(
        node_id: NodeId,
        listen_on: u16,
        initial_peers: &[SocketAddr],
    ) -> Result<Cluster, std::io::Error> {
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

                    let codec = tokio_serde_cbor::Codec::<Message, Message>::new();

                    let (send, recv) = codec.framed(success.stream).split();
                    let recv = recv.filter_map(|result| async move { log_err!(result) });

                    conn_tx.send((success.handshake.my_id, send, recv));
                });
            }
        });

        Ok(Cluster { connections })
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

    let codec = tokio_serde_cbor::Codec::<Handshake, Handshake>::new();
    let mut connection = codec.framed(connection);

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
