use std::{
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};

use swap_buffer_queue::{
    buffer::{BufferIter, BufferSlice, VecBuffer},
    error::{DequeueError, EnqueueError, TryDequeueError, TryEnqueueError},
    SynchronizedNotifier, SynchronizedQueue,
};

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "capacity must be strictly positive");
    let channel = Arc::new(SynchronizedQueue::with_capacity(capacity));
    let iterator = None;
    (Sender(channel.clone()), Receiver { channel, iterator })
}

type Channel<T> = Arc<SynchronizedQueue<VecBuffer<T>>>;

#[derive(Debug, Clone)]
pub struct Sender<T>(Channel<T>);

impl<T> Sender<T> {
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self.0.try_enqueue(value) {
            Ok(()) => Ok(()),
            Err(TryEnqueueError::InsufficientCapacity(v)) => Err(TrySendError::Full(v)),
            Err(TryEnqueueError::Closed(v)) => Err(TrySendError::Disconnected(v)),
        }
    }

    pub fn send_timeout(&self, value: T, timeout: Duration) -> Result<(), TrySendError<T>> {
        match self.0.enqueue_timeout(value, timeout) {
            Ok(()) => Ok(()),
            Err(EnqueueError::InsufficientCapacity(v)) => Err(TrySendError::Full(v)),
            Err(EnqueueError::Closed(v)) => Err(TrySendError::Disconnected(v)),
        }
    }

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self.0.enqueue(value) {
            Ok(()) => Ok(()),
            Err(EnqueueError::InsufficientCapacity(_)) => unreachable!(),
            Err(EnqueueError::Closed(v)) => Err(SendError(v)),
        }
    }

    pub async fn send_async(&self, value: T) -> Result<(), SendError<T>> {
        match self.0.enqueue_async(value).await {
            Ok(()) => Ok(()),
            Err(EnqueueError::InsufficientCapacity(_)) => unreachable!(),
            Err(EnqueueError::Closed(v)) => Err(SendError(v)),
        }
    }

    pub fn downgrade(&self) -> WeakSender<T> {
        WeakSender(Arc::downgrade(&self.0))
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.0) == 1 {
            self.0.close();
        }
    }
}

pub struct WeakSender<T>(Weak<SynchronizedQueue<VecBuffer<T>>>);

impl<T> WeakSender<T> {
    pub fn upgrade(&self) -> Option<Sender<T>> {
        self.0.upgrade().map(Sender)
    }
}

#[derive(Debug)]
pub struct Receiver<T> {
    channel: Channel<T>,
    iterator: Option<BufferIter<Channel<T>, VecBuffer<T>, SynchronizedNotifier>>,
}

impl<T> Receiver<T> {
    pub fn capacity(&self) -> usize {
        self.channel.capacity()
    }

    pub fn len(&self) -> usize {
        self.channel.len()
    }

    pub fn is_empty(&self) -> bool {
        self.channel.is_empty()
    }

    pub fn close(&self) {
        self.channel.close();
    }

    pub fn is_closed(&self) -> bool {
        self.channel.is_closed()
            || (self.iterator.is_some() && Arc::strong_count(&self.channel) == 2)
    }

    fn drop_iter(&mut self) {
        if self.iterator.is_some() {
            self.iterator = None;
            if Arc::strong_count(&self.channel) == 1 {
                self.close();
            }
        }
    }

    fn next(&mut self) -> Option<T> {
        let value = self.iterator.as_mut().and_then(Iterator::next);
        if value.is_none() {
            self.drop_iter();
        }
        value
    }

    fn set_iter_and_next(
        &mut self,
        iterator: BufferIter<Channel<T>, VecBuffer<T>, SynchronizedNotifier>,
    ) -> T {
        self.iterator.replace(iterator);
        self.iterator
            .as_mut()
            .and_then(Iterator::next)
            .expect("dequeued iterator cannot be empty")
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(value) = self.next() {
            return Ok(value);
        }
        let iter = self
            .channel
            .try_dequeue()
            .map_err(try_recv_err)?
            .into_iter()
            .with_owned(self.channel.clone());
        Ok(self.set_iter_and_next(iter))
    }

    pub fn try_recv_slice(
        &mut self,
    ) -> Result<BufferSlice<VecBuffer<T>, SynchronizedNotifier>, TryRecvError> {
        self.drop_iter();
        self.channel.try_dequeue().map_err(try_recv_err)
    }

    pub fn recv(&mut self) -> Result<T, RecvError> {
        if let Some(value) = self.next() {
            return Ok(value);
        }
        let iter = self
            .channel
            .dequeue()
            .map_err(recv_err)?
            .into_iter()
            .with_owned(self.channel.clone());
        Ok(self.set_iter_and_next(iter))
    }

    pub fn recv_slice(
        &mut self,
    ) -> Result<BufferSlice<VecBuffer<T>, SynchronizedNotifier>, RecvError> {
        self.drop_iter();
        self.channel.dequeue().map_err(recv_err)
    }

    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, TryRecvError> {
        if let Some(value) = self.next() {
            return Ok(value);
        }
        let iter = self
            .channel
            .dequeue_timeout(timeout)
            .map_err(try_recv_err)?
            .into_iter()
            .with_owned(self.channel.clone());
        Ok(self.set_iter_and_next(iter))
    }

    pub fn recv_slice_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<BufferSlice<VecBuffer<T>, SynchronizedNotifier>, TryRecvError> {
        self.drop_iter();
        self.channel.dequeue_timeout(timeout).map_err(try_recv_err)
    }

    pub async fn recv_async(&mut self) -> Result<T, RecvError> {
        if let Some(value) = self.next() {
            return Ok(value);
        }
        let iter = self
            .channel
            .dequeue_async()
            .await
            .map_err(recv_err)?
            .into_iter()
            .with_owned(self.channel.clone());
        Ok(self.set_iter_and_next(iter))
    }

    pub async fn recv_slice_async(
        &mut self,
    ) -> Result<BufferSlice<VecBuffer<T>, SynchronizedNotifier>, RecvError> {
        self.drop_iter();
        self.channel.dequeue_async().await.map_err(recv_err)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close();
    }
}

fn try_recv_err(error: TryDequeueError) -> TryRecvError {
    match error {
        TryDequeueError::Empty => TryRecvError::Empty,
        TryDequeueError::Pending => TryRecvError::Pending,
        TryDequeueError::Closed => TryRecvError::Disconnected,
        _ => unreachable!(),
    }
}

fn recv_err(error: DequeueError) -> RecvError {
    match error {
        DequeueError::Closed => RecvError,
        _ => unreachable!(),
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum TrySendError<T> {
    Full(T),
    Disconnected(T),
}

impl<T> TrySendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(v) | Self::Disconnected(v) => v,
        }
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => write!(f, "TrySendError::Full(_)"),
            Self::Disconnected(_) => write!(f, "TrySendError::Disconnected(_)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let error = match self {
            Self::Full(_) => "channel is full",
            Self::Disconnected(_) => "channel is disconnected",
        };
        write!(f, "{error}")
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// Error returned by [`Sender::send`](crate::Sender::send)/
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct SendError<T>(pub T);

impl<T> SendError<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SendError(_)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "channel is disconnected")
    }
}

impl<T> std::error::Error for SendError<T> {}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TryRecvError {
    Empty,
    Pending,
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let error = match self {
            Self::Empty => "channel is empty",
            Self::Pending => "waiting for concurrent sending end",
            Self::Disconnected => "channel is disconnected",
        };
        write!(f, "{error}")
    }
}

impl std::error::Error for TryRecvError {}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "channel is disconnected")
    }
}

impl std::error::Error for RecvError {}
