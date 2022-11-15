use dashmap::*;
use futures::{future::*, *};
use std::{hash::Hash, pin::Pin, task::Poll};
use tokio::sync::Mutex;

// TODO(shelbyd): Move dyn to Cluster.
type MStream<M> = Pin<Box<dyn Stream<Item = M> + Send + Sync>>;
type MSink<M, E> = Pin<Box<dyn Sink<M, Error = E> + Send + Sync>>;

pub struct Connections<I: Send + Sync, D: Send + Sync, M, E> {
    new_connections:
        Mutex<Pin<Box<dyn Stream<Item = (I, D, MSink<M, E>, MStream<M>)> + Send + Sync>>>,
    receivers: DashMap<I, MStream<M>>,
    senders: DashMap<I, MSink<M, E>>,
    peer_data: DashMap<I, D>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event<I, D, M> {
    NewConnection(I, D),
    Dropped(I),
    Message(I, M),
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum SendError<I, E> {
    #[error("no sender {0}")]
    NoSender(I),

    #[error("inner: {0}")]
    Inner(#[from] E),
}

impl<I, D, M, E> Connections<I, D, M, E>
where
    I: Clone + Hash + Eq + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
    M: 'static,
    E: 'static,
{
    pub fn new<S, Inc, Out>(new_connections: S) -> Self
    where
        S: Stream<Item = (I, D, Out, Inc)> + Send + Sync + 'static,
        Out: Sink<M, Error = E> + Send + Sync + 'static,
        Inc: Stream<Item = M> + Send + Sync + 'static,
    {
        Connections {
            new_connections: Mutex::new(Box::pin(new_connections.map(
                |(id, data, sink, stream)| {
                    (
                        id,
                        data,
                        Box::pin(sink) as MSink<M, E>,
                        Box::pin(stream) as MStream<M>,
                    )
                },
            ))),
            senders: DashMap::default(),
            receivers: DashMap::default(),
            peer_data: DashMap::default(),
        }
    }

    pub async fn next_event(&self) -> Event<I, D, M> {
        let mut connections = self.new_connections.lock().await;

        let event = poll_fn(|cx| {
            match (*connections).as_mut().poll_next(cx) {
                Poll::Pending => {}
                Poll::Ready(None) => unreachable!("incoming connections closed unexpectedly"),
                Poll::Ready(Some((id, data, send, recv))) => {
                    self.receivers.insert(id.clone(), recv);
                    self.senders.insert(id.clone(), send);
                    self.peer_data.insert(id.clone(), data.clone());
                    return Poll::Ready(Event::NewConnection(id, data));
                }
            }

            for mut entry in self.receivers.iter_mut() {
                match entry.value_mut().as_mut().poll_next(cx) {
                    Poll::Pending => {}
                    Poll::Ready(None) => {
                        return Poll::Ready(Event::Dropped(entry.key().clone()));
                    }
                    Poll::Ready(Some(message)) => {
                        return Poll::Ready(Event::Message(entry.key().clone(), message))
                    }
                }
            }

            Poll::Pending
        })
        .await;

        if let Event::Dropped(id) = &event {
            self.receivers.remove(id);
        }

        event
    }

    pub async fn send_to(&self, id: &I, message: M) -> Result<(), SendError<I, E>> {
        let mut sender = self
            .senders
            .get_mut(id)
            .ok_or_else(|| SendError::NoSender(id.clone()))?;
        sender.send(message).await?;

        Ok(())
    }

    #[cfg(test)]
    fn insert<S, R>(&self, id: I, (data, send, recv): (D, S, R))
    where
        R: Stream<Item = M> + Send + Sync + 'static,
        S: Sink<M, Error = E> + Send + Sync + 'static,
    {
        self.receivers.insert(id.clone(), Box::pin(recv));
        self.senders.insert(id.clone(), Box::pin(send));
        self.peer_data.insert(id, data);
    }

    #[cfg(test)]
    fn no_connections() -> Self {
        Connections {
            new_connections: Mutex::new(Box::pin(futures::stream::pending())),
            senders: DashMap::default(),
            receivers: DashMap::default(),
            peer_data: DashMap::default(),
        }
    }

    pub fn active(&self) -> Vec<I> {
        self.senders
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{sink::drain, stream::pending};
    use std::sync::Arc;
    use tokio::{
        sync::mpsc::*,
        time::{timeout, Duration},
    };
    use tokio_stream::wrappers::*;
    use tokio_util::sync::PollSender;

    const TEN_MS: Duration = Duration::from_millis(10);

    #[tokio::test]
    async fn new_connection_emits_event() {
        let (connection_sink, connection_stream) = unbounded_channel();
        let subject = Connections::new(UnboundedReceiverStream::new(connection_stream));

        connection_sink
            .send(("foo", "bar", drain::<()>(), pending()))
            .unwrap();

        assert_eq!(
            timeout(TEN_MS, subject.next_event()).await,
            Ok(Event::NewConnection("foo", "bar"))
        );
    }

    #[tokio::test]
    async fn event_from_new_connection() {
        let subject = Connections::no_connections();

        let (message_sink, message_stream) = unbounded_channel();
        subject.insert(
            "foo",
            ((), drain(), UnboundedReceiverStream::new(message_stream)),
        );

        message_sink.send("a message").unwrap();
        assert_eq!(
            timeout(TEN_MS, subject.next_event()).await,
            Ok(Event::Message("foo", "a message"))
        );
    }

    #[tokio::test]
    async fn connection_dropped() {
        let subject = Connections::no_connections();

        let (message_sink, message_stream) = unbounded_channel::<()>();
        subject.insert(
            "foo",
            ((), drain(), UnboundedReceiverStream::new(message_stream)),
        );

        drop(message_sink);
        assert_eq!(
            timeout(TEN_MS, subject.next_event()).await,
            Ok(Event::Dropped("foo"))
        );
    }

    #[tokio::test]
    async fn does_not_poll_dropped_connection() {
        let subject = Connections::no_connections();

        let (message_sink, message_stream) = unbounded_channel::<()>();
        subject.insert(
            "foo",
            ((), drain(), UnboundedReceiverStream::new(message_stream)),
        );

        drop(message_sink);
        timeout(TEN_MS, subject.next_event()).await.unwrap();

        assert_ne!(
            timeout(TEN_MS, subject.next_event()).await,
            Ok(Event::Dropped("foo"))
        );
    }

    #[tokio::test]
    async fn sends_message() {
        let subject = Connections::no_connections();

        let (message_sink, mut message_stream) = channel(42);
        subject.insert("foo", ((), PollSender::new(message_sink), pending()));

        subject.send_to(&"foo", 123).await.unwrap();
        assert_eq!(
            timeout(Duration::from_millis(10), message_stream.recv()).await,
            Ok(Some(123))
        );
    }

    #[tokio::test]
    async fn sends_message_during_poll() {
        let subject = Connections::no_connections();

        let (message_sink, mut message_stream) = channel(42);
        subject.insert("foo", ((), PollSender::new(message_sink), pending()));

        let subject = Arc::new(subject);

        let clone = Arc::clone(&subject);
        tokio::task::spawn(async move { clone.next_event().await });

        subject.send_to(&"foo", 123).await.unwrap();
        assert_eq!(
            timeout(Duration::from_millis(10), message_stream.recv()).await,
            Ok(Some(123))
        );
    }
}
