//! Multiplexed TCP transport.
//!
//! One `Transport` per node:
//! - listens for inbound connections (`listen`),
//! - dials outbound peers (`connect`),
//! - routes `Data` frames to actor mailboxes by destination actor name,
//! - keeps peers alive with heartbeats and disconnects silent ones.

mod peer;

pub use peer::{PeerHandle, SendError};

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch, RwLock};

use crate::runtime::NodeId;
use crate::wire::{FrameDecoder, MessageKind, WireMessage, PROTOCOL_VERSION};

/// Behaviour knobs of a `Transport`.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Interval between heartbeat pings to each peer.
    pub heartbeat_interval: Duration,
    /// A peer that sends nothing within this window is dropped.
    pub peer_timeout: Duration,
    /// Capacity of the outbound MPSC queue per peer.
    pub peer_queue_capacity: usize,
    /// Whether to automatically reconnect to peers that we dialed.
    pub reconnect: bool,
    /// Delay between reconnection attempts.
    pub reconnect_interval: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        TransportConfig {
            heartbeat_interval: Duration::from_secs(1),
            peer_timeout: Duration::from_secs(3),
            peer_queue_capacity: 256,
            reconnect: true,
            reconnect_interval: Duration::from_secs(1),
        }
    }
}

/// Events surfaced to the owning runtime (and to Python).
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    PeerConnected(NodeId),
    PeerDisconnected(NodeId),
    /// An `Error` frame was received from a peer.
    ErrorReceived { from: NodeId, detail: String },
}

/// Inbound mailbox: messages for a local actor land here.
pub type Mailbox = mpsc::Sender<WireMessage>;

/// Multiplexed transport owned by a node.
#[derive(Clone)]
pub struct Transport {
    local: NodeId,
    config: Arc<TransportConfig>,
    /// peer id -> outbound handle
    peers: Arc<RwLock<HashMap<NodeId, PeerHandle>>>,
    /// peers we intentionally dialed and want to stay connected to
    desired: Arc<RwLock<HashSet<NodeId>>>,
    /// dst actor name -> local mailbox
    routing: Arc<RwLock<HashMap<String, Mailbox>>>,
    /// system events
    events: broadcast::Sender<Event>,
    /// set to `true` on `shutdown()` to stop the accept loop
    stopped: watch::Sender<bool>,
    next_msg_id: Arc<AtomicU64>,
}

impl Transport {
    /// Binds a listener on `local` and starts the accept loop.
    pub async fn listen(local: NodeId, config: TransportConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind((local.host(), local.port())).await?;
        let bound = listener.local_addr()?;
        let (stopped_tx, stopped_rx) = watch::channel(false);
        let transport = Transport {
            local: local.with_port(bound.port()),
            config: Arc::new(config),
            peers: Arc::new(RwLock::new(HashMap::new())),
            desired: Arc::new(RwLock::new(HashSet::new())),
            routing: Arc::new(RwLock::new(HashMap::new())),
            events: broadcast::Sender::new(64),
            stopped: stopped_tx,
            next_msg_id: Arc::new(AtomicU64::new(1)),
        };
        tokio::spawn({
            let t = transport.clone();
            async move { t.accept_loop(listener, stopped_rx).await }
        });
        tokio::spawn({
            let t = transport.clone();
            let events = t.events.subscribe();
            async move { t.reconnect_supervisor(events).await }
        });
        Ok(transport)
    }

    /// Actual listening identity (port resolved when `local` used `:0`).
    pub fn local_id(&self) -> NodeId {
        self.local.clone()
    }

    /// Subscribes to system events.
    pub fn event_stream(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Registers the mailbox for a local actor.
    pub async fn register_actor(&self, name: String, mailbox: Mailbox) {
        self.routing.write().await.insert(name, mailbox);
    }

    /// Removes the mailbox for a local actor.
    pub async fn unregister_actor(&self, name: &str) {
        self.routing.write().await.remove(name);
    }

    /// Whether `peer` is currently connected.
    pub async fn has_peer(&self, peer: &NodeId) -> bool {
        self.peers.read().await.contains_key(peer)
    }

    /// Number of connected peers.
    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    /// Dials a peer, performs the handshake and starts tracking it.
    /// The peer is marked as *desired*, so it is reconnected automatically
    /// if the connection drops.
    pub async fn connect(&self, peer: NodeId) -> Result<(), std::io::Error> {
        if self.has_peer(&peer).await {
            return Ok(());
        }
        self.desired.write().await.insert(peer.clone());
        let stream = TcpStream::connect(peer.address()).await?;
        let (read_half, write_half) = stream.into_split();
        let handle = PeerHandle::spawn(peer.clone(), write_half, self.config.peer_queue_capacity);
        self.peers.write().await.insert(peer.clone(), handle.clone());

        // Announce ourselves; the remote registers us upon reading this frame.
        let hs = WireMessage::handshake(self.next_msg_id(), self.local.to_full());
        let _ = handle.send(hs).await;

        let _ = self.events.send(Event::PeerConnected(peer.clone()));
        let read_peer = peer.clone();
        let hb_peer = peer.clone();
        tokio::spawn({
            let t = self.clone();
            async move { t.read_loop(read_half, None, Some(read_peer)).await }
        });
        tokio::spawn({
            let t = self.clone();
            async move { t.heartbeat_loop(hb_peer).await }
        });
        Ok(())
    }

    /// Gracefully stops the transport: stops accepting, clears desired peers
    /// and drops all connections. Running tasks exit as a result.
    pub async fn shutdown(&self) {
        let _ = self.stopped.send(true);
        self.desired.write().await.clear();
        let mut guard = self.peers.write().await;
        let _ = std::mem::take(&mut *guard);
    }

    /// Sends a `Data` message to an actor on a remote node.
    pub async fn send_data(
        &self,
        peer: &NodeId,
        dst_actor: &str,
        payload: serde_json::Value,
        reply_to: Option<&str>,
        correlation_id: Option<u64>,
    ) -> Result<(), SendError> {
        let msg = WireMessage::data(
            self.next_msg_id(),
            self.local.to_full(),
            dst_actor,
            payload,
            reply_to.map(str::to_owned),
            correlation_id,
        );
        let handle = self.peers.read().await.get(peer).cloned().ok_or(SendError::PeerNotFound)?;
        handle.send(msg).await
    }

    /// Sends an `Error` frame back to a peer.
    async fn send_error(
        &self,
        peer: &NodeId,
        detail: String,
        correlation_id: Option<u64>,
    ) -> Result<(), SendError> {
        let payload = json!({ "error": detail });
        let msg = WireMessage {
            version: PROTOCOL_VERSION,
            msg_id: self.next_msg_id(),
            src: self.local.to_full(),
            dst_actor: String::new(),
            kind: MessageKind::Error,
            payload: serde_json::to_vec(&payload).expect("json serialization cannot fail"),
            reply_to: None,
            correlation_id,
        };
        let handle = self.peers.read().await.get(peer).cloned().ok_or(SendError::PeerNotFound)?;
        handle.send(msg).await
    }

    fn next_msg_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn accept_loop(&self, listener: TcpListener, mut stopped: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                changed = stopped.changed() => {
                    match changed {
                        Ok(_) => return,   // shutdown requested
                        Err(_) => return,  // sender dropped
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _addr)) => {
                            let t = self.clone();
                            tokio::spawn(async move { t.handle_inbound(stream).await });
                        }
                        Err(e) => {
                            tracing::error!(%e, "accept error");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_inbound(&self, stream: TcpStream) {
        let (read_half, write_half) = stream.into_split();
        self.clone().read_loop(read_half, Some(write_half), None).await;
    }

    /// Continuous read loop for one connection.
    ///
    /// * `write_half` — owned by the loop until the handshake arrives, at which
    ///   point it is handed to the writer task. `None` for outbound connections,
    ///   where the writer already exists.
    /// * `pre_known` — the peer id for outbound connections; the handshake is
    ///   only used to (re)confirm it.
    async fn read_loop(
        self,
        mut read_half: OwnedReadHalf,
        mut write_half: Option<OwnedWriteHalf>,
        pre_known: Option<NodeId>,
    ) {
        let mut decoder = FrameDecoder::new();
        let mut read_buf = [0u8; 4096];
        let mut registered = pre_known;

        loop {
            let read_result = tokio::time::timeout(self.config.peer_timeout, read_half.read(&mut read_buf)).await;
            let n = match read_result {
                Err(_elapsed) => break,
                Ok(Err(e)) => {
                    tracing::debug!(%e, "read error, dropping connection");
                    break;
                }
                Ok(Ok(0)) => break, // EOF
                Ok(Ok(n)) => n,
            };

            let frames = match decoder.decode(&read_buf[..n]) {
                Ok(frames) => frames,
                Err(e) => {
                    tracing::warn!(%e, "invalid frame, dropping connection");
                    break;
                }
            };

            for msg in frames {
                if msg.kind == MessageKind::Handshake {
                    if registered.is_none() {
                        let peer_id = match NodeId::parse(&msg.src) {
                            Ok(id) => id,
                            Err(e) => {
                                tracing::warn!(%e, "bad handshake source");
                                return;
                            }
                        };
                        let handle = match write_half.take() {
                            Some(w) => {
                                PeerHandle::spawn(peer_id.clone(), w, self.config.peer_queue_capacity)
                            }
                            None => {
                                tracing::warn!("handshake on outbound connection with no writer");
                                return;
                            }
                        };
                        self.peers.write().await.insert(peer_id.clone(), handle);
                        registered = Some(peer_id.clone());
                        let _ = self.events.send(Event::PeerConnected(peer_id.clone()));
                        tokio::spawn({
                            let t = self.clone();
                            async move { t.heartbeat_loop(peer_id).await }
                        });
                    }
                    continue;
                }

                let peer_id = match registered.as_ref() {
                    Some(id) => id.clone(),
                    None => {
                        tracing::warn!("data before handshake, dropping connection");
                        return;
                    }
                };
                self.route(peer_id, msg).await;
            }
        }

        if let Some(peer_id) = registered {
            self.peers.write().await.remove(&peer_id);
            let _ = self.events.send(Event::PeerDisconnected(peer_id));
        }
    }

    /// Subscribes to disconnect events and re-establishes desired peers.
    async fn reconnect_supervisor(&self, mut events: broadcast::Receiver<Event>) {
        while let Ok(ev) = events.recv().await {
            if let Event::PeerDisconnected(peer) = ev {
                if self.config.reconnect && self.desired.read().await.contains(&peer) {
                    let t = self.clone();
                    tokio::spawn(async move { t.reconnect_loop(peer).await });
                }
            }
        }
    }

    /// Retries `connect` until the desired peer is reachable again.
    async fn reconnect_loop(&self, peer: NodeId) {
        loop {
            tokio::time::sleep(self.config.reconnect_interval).await;
            if !self.desired.read().await.contains(&peer) {
                return;
            }
            if self.has_peer(&peer).await {
                return;
            }
            let attempt = tokio::time::timeout(
                self.config.reconnect_interval * 3,
                self.connect(peer.clone()),
            )
            .await;
            match attempt {
                Ok(Ok(())) => return,
                Ok(Err(e)) => tracing::debug!(%e, peer = %peer.to_full(), "reconnect failed"),
                Err(_) => tracing::debug!(peer = %peer.to_full(), "reconnect timed out"),
            }
        }
    }

    /// Routes a non-handshake frame received from `from`.
    async fn route(&self, from: NodeId, msg: WireMessage) {
        match msg.kind {
            MessageKind::Handshake => {}
            MessageKind::Heartbeat => {
                let handle = self.peers.read().await.get(&from).cloned();
                if let Some(h) = handle {
                    let reply = WireMessage::heartbeat(self.next_msg_id(), self.local.to_full());
                    let _ = h.send(reply).await;
                }
            }
            MessageKind::Data => {
                let mailbox = self.routing.read().await.get(&msg.dst_actor).cloned();
                match mailbox {
                    Some(mailbox) => {
                        let _ = mailbox.send(msg).await;
                    }                    None => {
                        let detail = format!("actor_not_found:{}", msg.dst_actor);
                        let _ = self.send_error(&from, detail, msg.correlation_id).await;
                    }
                }
            }
            MessageKind::Error => {
                let detail = msg
                    .payload_json()
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                let _ = self.events.send(Event::ErrorReceived { from, detail });
            }
        }
    }

    /// Periodic heartbeat sender for a peer. Exits when the peer is gone.
    async fn heartbeat_loop(&self, peer: NodeId) {
        loop {
            tokio::time::sleep(self.config.heartbeat_interval).await;
            let handle = self.peers.read().await.get(&peer).cloned();
            let Some(handle) = handle else { return };
            let hb = WireMessage::heartbeat(self.next_msg_id(), self.local.to_full());
            if handle.send(hb).await.is_err() {
                return;
            }
        }
    }
}
