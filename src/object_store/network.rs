use dashmap::{mapref::entry::Entry, DashMap};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::UdpSocket,
    sync::oneshot::{channel, Sender},
};

use crate::fs::{IoError, IoResult};

use super::*;
use response::*;

pub struct Network<O, P = NetworkPeer> {
    backing: O,
    peers: Vec<P>,
}

impl<O> Network<O>
where
    O: ObjectStore + Send + Sync + 'static,
{
    pub async fn new(backing: O, port: u16, peers: &[SocketAddr]) -> anyhow::Result<Arc<Self>> {
        let socket = UdpSocket::bind(("0.0.0.0", port)).await?;
        let dispatcher = Arc::new(Dispatcher {
            socket,
            outstanding_requests: Default::default(),
        });

        let peers = peers.iter().map(|&addr| NetworkPeer {
            addr,
            dispatcher: Arc::clone(&dispatcher),
        });

        let this = Arc::new(Self::new_inner(backing, peers.collect()));

        let this_for_task = Arc::clone(&this);
        tokio::spawn(async move {
            let mut buf = vec![0; 3 * 1024 * 1024];
            loop {
                let unhandled = match dispatcher.tick(&mut buf).await {
                    Ok(Some(m)) => m,
                    Ok(None) => continue,
                    Err(e) => {
                        log::error!("{}", e);
                        continue;
                    }
                };

                let clone = Arc::clone(&this_for_task);
                tokio::spawn(async move { clone.receive(unhandled).await });
            }
        });

        Ok(this)
    }
}

impl<O, P> Network<O, P> {
    fn new_inner(backing: O, peers: Vec<P>) -> Self {
        Self { backing, peers }
    }
}

impl<O, P> Network<O, P>
where
    O: ObjectStore + Send + Sync,
    P: Peer + Send + Sync,
{
    async fn receive(&self, message: Message) {
        match message {
            Message::Set(k, v) => self.backing.set(k, v).await,
            m => {
                log::warn!("Unhandled message: {:?}", m);
            },
        }
    }
}

#[async_trait::async_trait]
impl<O, P> ObjectStore for Network<O, P>
where
    O: ObjectStore + Send + Sync,
    P: Peer + Send + Sync,
{
    async fn set(&self, key: String, value: Vec<u8>) {
        for peer in &self.peers {
            peer.send(Message::Set(key.clone(), value.clone()));
        }
        self.backing.set(key, value).await;
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(v) = self.backing.get(key).await {
            return Some(v);
        }

        for peer in &self.peers {
            if let Ok(f) = peer.request::<Fetch>(Request::Fetch(key.to_string())).await {
                return Some(f.0);
            }
        }

        None
    }

    /// Update the value at the provided key. May retry until successful.
    async fn update<R, F>(&self, key: String, f: F) -> (Vec<u8>, R)
    where
        F: for<'v> FnMut(Option<&'v Vec<u8>>) -> (Vec<u8>, R) + Send,
    {
        let (bytes, r) = self.backing.update(key.clone(), f).await;

        for peer in &self.peers {
            peer.send(Message::Set(key.clone(), bytes.clone()));
        }

        (bytes, r)
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Message {
    Set(String, Vec<u8>),
    Request(u32, Request),
    Response(u32, Vec<u8>),
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Request {
    Fetch(String),
}

mod response {
    use super::*;

    #[derive(Serialize, Deserialize)]
    pub struct Fetch(pub Vec<u8>);
}

// TODO(shelbyd): Can make private?
#[async_trait::async_trait]
pub trait Peer {
    fn send(&self, message: Message);

    async fn request<R: DeserializeOwned>(&self, req: Request) -> IoResult<R>;
}

pub struct NetworkPeer {
    addr: SocketAddr,
    dispatcher: Arc<Dispatcher>,
}

#[async_trait::async_trait]
impl Peer for NetworkPeer {
    fn send(&self, _message: Message) {
        todo!()
    }

    async fn request<R: DeserializeOwned>(&self, req: Request) -> IoResult<R> {
        self.dispatcher.request(self.addr, req).await
    }
}

struct Dispatcher {
    socket: UdpSocket,
    outstanding_requests: DashMap<(SocketAddr, u32), Sender<Vec<u8>>>,
}

impl Dispatcher {
    async fn request<R: DeserializeOwned>(&self, addr: SocketAddr, req: Request) -> IoResult<R> {
        let (sender, receiver) = channel();
        let id = loop {
            let id = rand::random();
            match self.outstanding_requests.entry((addr, id)) {
                Entry::Vacant(v) => {
                    v.insert(sender);
                    break id;
                }
                _ => {}
            }
        };

        let req = serde_cbor::to_vec(&Message::Request(id, req)).map_err(|_| IoError::Parse)?;
        self.socket.send_to(&req, addr).await.map_err(|e| {
            log::error!("{}", e);
            IoError::Io
        })?;

        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                Err(IoError::Timeout)
            }
            data = receiver => {
                let data = data.map_err(|e| {
                    log::error!("{}", e);
                    IoError::Io
                })?;
                serde_cbor::from_slice(&data).map_err(|_| IoError::Parse)
            }
        }
    }

    async fn tick(&self, buf: &mut [u8]) -> anyhow::Result<Option<Message>> {
        let (amount, from) = self.socket.recv_from(buf).await?;
        let message = serde_cbor::from_slice(&buf[..amount])?;

        match message {
            Message::Response(id, data) => {
                if let Some((_, sender)) = self.outstanding_requests.remove(&(from, id)) {
                    let _disconnected = sender.send(data);
                    Ok(None)
                } else {
                    Ok(Some(Message::Response(id, data)))
                }
            }
            m => Ok(Some(m)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestPeer {
        inc: Mutex<Vec<Message>>,
        responder: Mutex<Option<Box<dyn Fn(Request) -> Option<Vec<u8>> + Send + Sync>>>,
    }

    impl TestPeer {
        fn contains(&self, message: &Message) -> bool {
            self.inc.lock().unwrap().contains(message)
        }

        fn receive(&self, message: Message) {}

        fn respond(&self, f: impl Fn(Request) -> Option<Vec<u8>> + Send + Sync + 'static) {
            *self.responder.lock().unwrap() = Some(Box::new(f));
        }

        fn ser(t: impl Serialize) -> Vec<u8> {
            serde_cbor::to_vec(&t).unwrap()
        }
    }

    #[async_trait::async_trait]
    impl<'a> Peer for &'a TestPeer {
        fn send(&self, message: Message) {
            self.inc.lock().unwrap().push(message)
        }

        async fn request<R: DeserializeOwned>(&self, req: Request) -> IoResult<R> {
            let lock = self.responder.lock().unwrap();
            if let Some(responder) = &*lock {
                if let Some(response) = (responder)(req) {
                    return Ok(serde_cbor::from_slice(&response).unwrap());
                }
            }

            Err(IoError::Unimplemented)
        }
    }

    #[tokio::test]
    async fn set_sends_to_peers() {
        let peer = TestPeer::default();

        let mem = Memory::default();
        let net = Network::new_inner(mem, vec![&peer]);

        net.set("foo".to_string(), b"bar".to_vec()).await;

        assert!(peer.contains(&Message::Set("foo".to_string(), b"bar".to_vec())));
    }

    #[tokio::test]
    async fn set_saves_in_backing() {
        let mem = Memory::default();
        let net = Network::<_, NetworkPeer>::new_inner(mem, vec![]);

        net.set("foo".to_string(), b"bar".to_vec()).await;

        assert_eq!(net.backing.get("foo").await, Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn set_from_peer_saves_in_backing() {
        let peer = TestPeer::default();

        let mem = Memory::default();
        let net = Network::new_inner(mem, vec![&peer]);

        net.receive(Message::Set("foo".to_string(), b"bar".to_vec()))
            .await;

        assert_eq!(net.backing.get("foo").await, Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn get_in_backing_returns_that() {
        let peer = TestPeer::default();

        let mem = Memory::default();
        let net = Network::new_inner(mem, vec![&peer]);

        net.backing.set("foo".to_string(), b"bar".to_vec()).await;

        assert_eq!(net.get("foo").await, Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn get_missing_queries_peers() {
        let peer = TestPeer::default();

        let mem = Memory::default();
        let net = Network::new_inner(mem, vec![&peer]);

        peer.respond(|m| match m {
            Request::Fetch(key) => {
                assert_eq!(&key, "foo");
                Some(TestPeer::ser(Fetch(b"bar".to_vec())))
            }

            message => {
                eprintln!("unrecognized message: {:?}", message);
                None
            }
        });

        assert_eq!(net.get("foo").await, Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn update_sets_on_peers() {
        let peer = TestPeer::default();

        let mem = Memory::default();
        let net = Network::new_inner(mem, vec![&peer]);

        net.update("foo".to_string(), |_| (b"bar".to_vec(), ()))
            .await;

        assert!(peer.contains(&Message::Set("foo".to_string(), b"bar".to_vec())));
    }
}
