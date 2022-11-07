use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};

use crate::fs::{IoError, IoResult};

use super::*;
use response::*;

type PeerId = u64;

pub struct Network<O, P = NetworkPeer> {
    backing: O,
    peers: HashMap<PeerId, P>,
}

impl<O> Network<O> {
    pub async fn new(_backing: O, _port: u16, _peers: &[SocketAddr]) -> anyhow::Result<Self> {
        unimplemented!("new");
    }
}

impl<O, P> Network<O, P> {
    fn new_inner(backing: O, peers: HashMap<PeerId, P>) -> Self {
        Self { backing, peers }
    }
}

impl<O, P> Network<O, P>
where
    O: ObjectStore + Send + Sync,
    P: Peer + Send + Sync,
{
    async fn receive(&self, from: PeerId, message: Message) {
        match message {
            Message::Set(k, v) => self.backing.set(k, v).await,
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
        for peer in self.peers.values() {
            peer.send(Message::Set(key.clone(), value.clone()));
        }
        self.backing.set(key, value).await;
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(v) = self.backing.get(key).await {
            return Some(v);
        }

        for peer in self.peers.values() {
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

        for peer in self.peers.values() {
            peer.send(Message::Set(key.clone(), bytes.clone()));
        }

        (bytes, r)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum Message {
    Set(String, Vec<u8>),
}

#[derive(PartialEq, Eq, Debug)]
enum Request {
    Fetch(String),
}

mod response {
    use super::*;

    #[derive(Serialize, Deserialize)]
    pub struct Fetch(pub Vec<u8>);
}

#[async_trait::async_trait]
trait Peer {
    fn send(&self, message: Message);
    async fn request<R: DeserializeOwned>(&self, req: Request) -> IoResult<R>;
}

pub struct NetworkPeer {}

#[async_trait::async_trait]
impl Peer for NetworkPeer {
    fn send(&self, _message: Message) {
        todo!()
    }

    async fn request<R: DeserializeOwned>(&self, req: Request) -> IoResult<R> {
        unimplemented!("request");
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
        let net = Network::new_inner(
            mem,
            maplit::hashmap! {
                1 => &peer,
            },
        );

        net.set("foo".to_string(), b"bar".to_vec()).await;

        assert!(peer.contains(&Message::Set("foo".to_string(), b"bar".to_vec())));
    }

    #[tokio::test]
    async fn set_saves_in_backing() {
        let mem = Memory::default();
        let net = Network::<_, NetworkPeer>::new_inner(mem, maplit::hashmap! {});

        net.set("foo".to_string(), b"bar".to_vec()).await;

        assert_eq!(net.backing.get("foo").await, Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn set_from_peer_saves_in_backing() {
        let peer = TestPeer::default();

        let mem = Memory::default();
        let net = Network::new_inner(
            mem,
            maplit::hashmap! {
                1 => &peer,
            },
        );

        net.receive(1, Message::Set("foo".to_string(), b"bar".to_vec()))
            .await;

        assert_eq!(net.backing.get("foo").await, Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn get_in_backing_returns_that() {
        let peer = TestPeer::default();

        let mem = Memory::default();
        let net = Network::new_inner(
            mem,
            maplit::hashmap! {
                1 => &peer,
            },
        );

        net.backing.set("foo".to_string(), b"bar".to_vec()).await;

        assert_eq!(net.get("foo").await, Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn get_missing_queries_peers() {
        let peer = TestPeer::default();

        let mem = Memory::default();
        let net = Network::new_inner(
            mem,
            maplit::hashmap! {
                1 => &peer,
            },
        );

        peer.respond(|m| match m {
            Request::Fetch(key) => {
                assert_eq!(&key, "foo");
                Some(TestPeer::ser(Fetch(b"bar".to_vec())))
            },

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
        let net = Network::new_inner(
            mem,
            maplit::hashmap! {
                1 => &peer,
            },
        );

        net.update("foo".to_string(), |_| (b"bar".to_vec(), ())).await;

        assert!(peer.contains(&Message::Set("foo".to_string(), b"bar".to_vec())));
    }
}
