//! Outbound write path for a single peer connection.
//!
//! A `PeerHandle` owns an MPSC channel feeding a background writer task.
//! `send` is non-blocking w.r.t. the network: it only enqueues the frame.

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::{self, error::TryRecvError};

use crate::runtime::NodeId;
use crate::wire::{encode_frame, WireMessage};

#[derive(Debug)]
pub enum SendError {
    /// The peer connection is gone.
    Closed,
    /// The target peer is not connected.
    PeerNotFound,
    /// The serialized payload would exceed the maximum frame size.
    PayloadTooLarge(usize),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Closed => write!(f, "peer connection is closed"),
            SendError::PeerNotFound => write!(f, "peer not connected"),
            SendError::PayloadTooLarge(n) => write!(f, "payload too large ({n} bytes)"),
        }
    }
}

impl std::error::Error for SendError {}

/// Outbound handle to a connected peer.
#[derive(Clone)]
pub struct PeerHandle {
    pub peer_id: NodeId,
    tx: mpsc::Sender<WireMessage>,
}

impl PeerHandle {
    /// Spawns the background writer task and returns the handle.
    pub fn spawn(peer_id: NodeId, write_half: OwnedWriteHalf, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        tokio::spawn(writer_loop(write_half, rx));
        PeerHandle { peer_id, tx }
    }

    /// Enqueues a message to be written to the socket.
    pub async fn send(&self, msg: WireMessage) -> Result<(), SendError> {
        self.tx.send(msg).await.map_err(|_| SendError::Closed)
    }

    /// Non-blocking enqueue; fails if the queue is full.
    pub fn try_send(&self, msg: WireMessage) -> Result<(), SendError> {
        self.tx.try_send(msg).map_err(|_| SendError::Closed)
    }

    /// Whether `other` refers to the same underlying connection as `self`.
    ///
    /// Used to tell a stale (superseded) connection apart from the current
    /// one for a given peer id, so its cleanup does not evict a fresher
    /// connection that replaced it in the peers map.
    pub fn is_same_connection(&self, other: &PeerHandle) -> bool {
        self.tx.same_channel(&other.tx)
    }
}

/// Drains the queue and writes framed messages to the socket.
async fn writer_loop(mut write_half: OwnedWriteHalf, mut rx: mpsc::Receiver<WireMessage>) {
    let mut frames = Vec::new();
    while let Some(msg) = rx.recv().await {
        match encode_frame(&msg) {
            Ok(frame) => frames.push(frame),
            Err(e) => {
                tracing::warn!(%e, "failed to encode frame, dropping");
                continue;
            }
        }
        // Coalesce as many queued messages as possible into one write syscall.
        loop {
            match rx.try_recv() {
                Ok(next) => match encode_frame(&next) {
                    Ok(frame) => frames.push(frame),
                    Err(e) => tracing::warn!(%e, "failed to encode frame, dropping"),
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        let mut done = true;
        for frame in frames.drain(..) {
            if write_half.write_all(&frame).await.is_err() {
                done = false;
                break;
            }
        }
        if !done {
            return;
        }
    }
}
