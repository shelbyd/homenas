use dashmap::*;
use futures::{future::*, *};
use std::{hash::Hash, pin::Pin, task::Poll};
use tokio::sync::Mutex;

pub struct Connections<St, Cid, S, R> {
    new_connections: Mutex<Pin<Box<St>>>,
    receivers: DashMap<Cid, Pin<Box<R>>>,
    senders: DashMap<Cid, Pin<Box<S>>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event<Cid, M> {
    NewConnection(Cid),
    Dropped(Cid),
    Message(Cid, M),
}

impl<St, Cid, S, R, M> Connections<St, Cid, S, R>
where
    Cid: Clone + Hash + Eq,
    St: Stream<Item = (Cid, S, R)>,
    S: Sink<M>,
    R: Stream<Item = M>,
{
    #[allow(unused)]
    pub fn new(new_connections: St) -> Self {
        Connections {
            new_connections: Mutex::new(Box::pin(new_connections)),
            senders: DashMap::default(),
            receivers: DashMap::default(),
        }
    }

    #[allow(unused)]
    pub async fn next_event(&self) -> Event<Cid, M> {
        let mut connections = self.new_connections.lock().await;

        let event = poll_fn(|cx| {
            match (*connections).as_mut().poll_next(cx) {
                Poll::Pending => {}
                Poll::Ready(None) => unreachable!("incoming connections closed unexpectedly"),
                Poll::Ready(Some((id, send, recv))) => {
                    self.insert(id.clone(), (send, recv));
                    return Poll::Ready(Event::NewConnection(id));
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

    #[allow(unused)]
    pub async fn send_to(&self, id: &Cid, message: M) -> Result<(), S::Error> {
        self.senders
            .get_mut(id)
            .expect("called send_to on missing id")
            .send(message)
            .await
    }

    fn insert(&self, id: Cid, (send, recv): (S, R)) {
        self.receivers.insert(id.clone(), Box::pin(recv));
        self.senders.insert(id, Box::pin(send));
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
            .send(("foo", drain::<()>(), pending()))
            .unwrap();

        assert_eq!(
            timeout(TEN_MS, subject.next_event()).await,
            Ok(Event::NewConnection("foo"))
        );
    }

    #[tokio::test]
    async fn event_from_new_connection() {
        let subject = Connections::new(pending());

        let (message_sink, message_stream) = unbounded_channel();
        subject.insert(
            "foo",
            (drain(), UnboundedReceiverStream::new(message_stream)),
        );

        message_sink.send("a message").unwrap();
        assert_eq!(
            timeout(TEN_MS, subject.next_event()).await,
            Ok(Event::Message("foo", "a message"))
        );
    }

    #[tokio::test]
    async fn connection_dropped() {
        let subject = Connections::new(pending());

        let (message_sink, message_stream) = unbounded_channel::<()>();
        subject.insert(
            "foo",
            (drain(), UnboundedReceiverStream::new(message_stream)),
        );

        drop(message_sink);
        assert_eq!(
            timeout(TEN_MS, subject.next_event()).await,
            Ok(Event::Dropped("foo"))
        );
    }

    #[tokio::test]
    async fn does_not_poll_dropped_connection() {
        let subject = Connections::new(pending());

        let (message_sink, message_stream) = unbounded_channel::<()>();
        subject.insert(
            "foo",
            (drain(), UnboundedReceiverStream::new(message_stream)),
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
        let subject = Connections::new(pending());

        let (message_sink, mut message_stream) = channel(42);
        subject.insert("foo", (PollSender::new(message_sink), pending()));

        subject.send_to(&"foo", 123).await.unwrap();
        assert_eq!(
            timeout(Duration::from_millis(10), message_stream.recv()).await,
            Ok(Some(123))
        );
    }

    #[tokio::test]
    async fn sends_message_during_poll() {
        let subject = Connections::new(pending());

        let (message_sink, mut message_stream) = channel(42);
        subject.insert("foo", (PollSender::new(message_sink), pending()));

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
