//! `Node` binding: bridges the Rust `Transport` (on its own Tokio runtime) to
//! the Python event loop.
//!
//! Inbound `Data` messages are delivered by scheduling a callback on the
//! Python event loop with `loop.call_soon_threadsafe`, so actors keep running
//! inside Python's asyncio while all network I/O happens off the GIL.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::{broadcast, mpsc};

use crate::runtime::NodeId;
use crate::transport::{Event, Transport, TransportConfig};
use crate::wire::WireMessage;

use super::convert;

/// Python-side delivery target for one local actor.
struct Delivery {
    event_loop: Py<PyAny>,
    callback: Py<PyAny>,
}

/// A distributed node owned by Python, backed by a Rust runtime thread.
///
/// All methods take `&mut self` because the Tokio runtime is `!Sync`.
#[pyclass]
pub struct Node {
    local: NodeId,
    transport: Option<Transport>,
    runtime: Option<tokio::runtime::Runtime>,
    handle: Option<tokio::runtime::Handle>,
    reconnect_interval: Option<std::time::Duration>,
    reconnect_max_attempts: Option<u32>,
    cluster_secret: Option<Vec<u8>>,
}

impl Node {
    fn require(&self) -> PyResult<(Transport, tokio::runtime::Handle)> {
        match (&self.transport, &self.handle) {
            (Some(t), Some(h)) => Ok((t.clone(), h.clone())),
            _ => Err(PyRuntimeError::new_err("node has not been started")),
        }
    }
}

#[pymethods]
impl Node {
    /// Creates a node bound to `name@host:port`. Port `0` picks an ephemeral
    /// port; call `start()` before use.
    ///
    /// `cluster_secret`, when set, must match on every node this one talks
    /// to: a handshake that doesn't prove knowledge of the same secret is
    /// rejected before the connection is ever registered as a peer. See
    /// `wire::auth` (Rust) / the "Security" docs page for exactly what this
    /// does and doesn't protect against — it is not encryption.
    ///
    /// `reconnect_max_attempts` bounds how many times a dropped desired
    /// peer is retried before giving up (see `on_event`'s
    /// `"reconnect_gave_up"` and `forget_peer`) — pass `None` explicitly
    /// for the old retry-forever behaviour, which for a peer that's gone
    /// for good is an unbounded background task hammering `connect()`
    /// forever.
    #[new]
    #[pyo3(signature = (node_id, reconnect_interval=None, reconnect_max_attempts=30, cluster_secret=None))]
    fn new(
        node_id: &str,
        reconnect_interval: Option<f64>,
        reconnect_max_attempts: Option<u32>,
        cluster_secret: Option<&str>,
    ) -> PyResult<Self> {
        let local = NodeId::parse(node_id)
            .map_err(|e| PyValueError::new_err(format!("invalid node id: {e}")))?;
        Ok(Node {
            local,
            transport: None,
            runtime: None,
            handle: None,
            reconnect_interval: reconnect_interval.map(std::time::Duration::from_secs_f64),
            reconnect_max_attempts,
            cluster_secret: cluster_secret.map(|s| s.as_bytes().to_vec()),
        })
    }

    /// Starts the background Tokio runtime and binds the listener.
    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        // Best-effort: makes the crate's internal `tracing::warn!`/`debug!`
        // calls (frame decode failures, connection drops, ...) visible via
        // `RUST_LOG`, which nothing did before. `try_init` (rather than
        // `init`) only errors if a subscriber is already installed — by an
        // embedding application, or by an earlier `Node.start()` call in
        // this process — either of which should win, so the error is
        // intentionally ignored.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .try_init();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to start runtime: {e}")))?;

        let local = self.local.clone();
        let config = TransportConfig {
            reconnect_interval: self
                .reconnect_interval
                .unwrap_or(TransportConfig::default().reconnect_interval),
            reconnect_max_attempts: self.reconnect_max_attempts,
            cluster_secret: self.cluster_secret.clone(),
            ..Default::default()
        };
        let transport = py
            .detach(|| runtime.block_on(Transport::listen(local, config)))
            .map_err(|e| {
                PyRuntimeError::new_err(format!("failed to bind {}: {e}", self.local.to_full()))
            })?;

        self.local = transport.local_id();
        self.handle = Some(runtime.handle().clone());
        self.runtime = Some(runtime);
        self.transport = Some(transport);
        Ok(())
    }

    /// Shuts down the background runtime.
    fn stop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
        self.handle = None;
        self.transport = None;
    }

    /// Actual node id, with the resolved port.
    fn local_id(&mut self) -> String {
        self.local.to_full()
    }

    /// Asynchronously connects to a peer; returns before the connection is
    /// established. Poll `has_peer` to wait for it.
    fn connect_peer(&mut self, peer_id: &str) -> PyResult<()> {
        let peer = NodeId::parse(peer_id)
            .map_err(|e| PyValueError::new_err(format!("invalid peer id: {e}")))?;
        let (transport, handle) = self.require()?;
        handle.spawn(async move {
            let _ = transport.connect(peer).await;
        });
        Ok(())
    }

    /// Stops treating `peer_id` as desired (no further auto-reconnect
    /// attempts) and drops the connection now if one is currently open.
    /// Use this once you know you're done with a peer, instead of waiting
    /// for the automatic give-up after repeated failed reconnects.
    fn forget_peer(&mut self, py: Python<'_>, peer_id: &str) -> PyResult<()> {
        let peer = NodeId::parse(peer_id)
            .map_err(|e| PyValueError::new_err(format!("invalid peer id: {e}")))?;
        let (transport, handle) = self.require()?;
        py.detach(|| handle.block_on(transport.forget_peer(&peer)));
        Ok(())
    }

    /// Whether a peer is currently connected.
    fn has_peer(&mut self, py: Python<'_>, peer_id: &str) -> PyResult<bool> {
        let peer = NodeId::parse(peer_id)
            .map_err(|e| PyValueError::new_err(format!("invalid peer id: {e}")))?;
        let (transport, handle) = self.require()?;
        Ok(py.detach(|| handle.block_on(transport.has_peer(&peer))))
    }

    /// Number of connected peers.
    fn peer_count(&mut self, py: Python<'_>) -> PyResult<usize> {
        let (transport, handle) = self.require()?;
        Ok(py.detach(|| handle.block_on(transport.peer_count())))
    }

    /// Registers a local actor mailbox. Inbound messages for `name` are
    /// scheduled as `callback(payload, meta_dict)` on `event_loop` via
    /// `call_soon_threadsafe`. `meta_dict` has `"src"` (sender node id),
    /// `"reply_to"`, `"correlation_id"` and `"msg_id"` — whatever the
    /// sender passed to `send_data`, or `None`.
    ///
    /// Blocks (off the GIL) until the routing table update lands, so that by
    /// the time this call returns, remote sends to `name` are guaranteed to
    /// find the mailbox rather than racing a fire-and-forget task — this
    /// also fixes the reorder hazard between a rapid unregister/register pair
    /// (e.g. `Supervisor` restarting a crashed actor).
    fn register_actor(
        &mut self,
        py: Python<'_>,
        name: &str,
        event_loop: Py<PyAny>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        let (transport, handle) = self.require()?;
        let name = name.to_string();
        let (tx, rx) = mpsc::channel(256);
        let transport_register = transport.clone();
        py.detach(|| {
            handle.block_on(async move {
                transport_register.register_actor(name, tx).await;
            });
        });
        let delivery = Delivery {
            event_loop,
            callback,
        };
        handle.spawn(drain_loop(rx, delivery));
        Ok(())
    }

    /// Removes a local actor mailbox. See `register_actor` for why this
    /// blocks until the removal is applied.
    fn unregister_actor(&mut self, py: Python<'_>, name: &str) -> PyResult<()> {
        let (transport, handle) = self.require()?;
        let name = name.to_string();
        py.detach(|| {
            handle.block_on(async move {
                transport.unregister_actor(&name).await;
            });
        });
        Ok(())
    }

    /// Registers a handler for system events: peer connected/disconnected and
    /// errors reported by a peer (e.g. a send to an unknown remote actor).
    /// `callback(event_dict)` is scheduled on `event_loop` via
    /// `call_soon_threadsafe`, same delivery mechanism as actor messages.
    ///
    /// `event_dict` has a `"kind"` key (`"peer_connected"`,
    /// `"peer_disconnected"` or `"error"`), a `"peer"` key with the peer's
    /// full id, and — for `"error"` — a `"detail"` key.
    fn set_event_handler(&mut self, event_loop: Py<PyAny>, callback: Py<PyAny>) -> PyResult<()> {
        let (transport, handle) = self.require()?;
        let events = transport.event_stream();
        let delivery = Delivery {
            event_loop,
            callback,
        };
        handle.spawn(event_drain_loop(events, delivery));
        Ok(())
    }

    /// Sends a `Data` message to an actor on a connected peer.
    #[allow(clippy::too_many_arguments)]
    fn send_data(
        &mut self,
        py: Python<'_>,
        peer_id: &str,
        dst_actor: &str,
        payload: &Bound<'_, PyAny>,
        reply_to: Option<&str>,
        correlation_id: Option<u64>,
    ) -> PyResult<()> {
        let peer = NodeId::parse(peer_id)
            .map_err(|e| PyValueError::new_err(format!("invalid peer id: {e}")))?;
        let payload_bytes = convert::py_to_json_bytes(payload)?;
        let (transport, handle) = self.require()?;
        py.detach(|| {
            handle.block_on(transport.send_data_bytes(
                &peer,
                dst_actor,
                payload_bytes,
                reply_to,
                correlation_id,
            ))
        })
        .map_err(|e| PyValueError::new_err(format!("send failed: {e}")))?;
        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// Drains inbound messages for one actor and schedules them on the Python loop.
async fn drain_loop(mut rx: mpsc::Receiver<WireMessage>, delivery: Delivery) {
    while let Some(msg) = rx.recv().await {
        let delivered = Python::try_attach(|py| -> PyResult<()> {
            // Parses `msg.payload` straight into Python objects — see
            // `py::convert::bytes_to_py` for why this skips the
            // `serde_json::Value` step `payload_json()` used to require.
            let obj = convert::bytes_to_py(py, &msg.payload)?;
            let meta = PyDict::new(py);
            meta.set_item("src", &msg.src)?;
            meta.set_item("reply_to", &msg.reply_to)?;
            meta.set_item("correlation_id", msg.correlation_id)?;
            meta.set_item("msg_id", msg.msg_id)?;
            delivery.event_loop.call_method1(
                py,
                "call_soon_threadsafe",
                (&delivery.callback, obj, meta),
            )?;
            Ok(())
        });
        match delivered {
            Some(Ok(())) => {}
            Some(Err(e)) => tracing::warn!("undeliverable payload, dropping: {e}"),
            None => return, // interpreter is shutting down
        }
    }
}

/// Drains system events (peer connected/disconnected, errors) and schedules
/// them on the Python loop as plain dicts.
async fn event_drain_loop(mut events: broadcast::Receiver<Event>, delivery: Delivery) {
    loop {
        let event = match events.recv().await {
            Ok(ev) => ev,
            // A slow consumer missed some events; keep going with the rest
            // rather than dying, since events are advisory, not delivery-critical.
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return,
        };
        let delivered = Python::try_attach(|py| -> PyResult<()> {
            let dict = PyDict::new(py);
            match event {
                Event::PeerConnected(peer) => {
                    dict.set_item("kind", "peer_connected")?;
                    dict.set_item("peer", peer.to_full())?;
                }
                Event::PeerDisconnected(peer) => {
                    dict.set_item("kind", "peer_disconnected")?;
                    dict.set_item("peer", peer.to_full())?;
                }
                Event::ErrorReceived {
                    from,
                    detail,
                    correlation_id,
                } => {
                    dict.set_item("kind", "error")?;
                    dict.set_item("peer", from.to_full())?;
                    dict.set_item("detail", detail)?;
                    dict.set_item("correlation_id", correlation_id)?;
                }
                Event::ReconnectGaveUp(peer) => {
                    dict.set_item("kind", "reconnect_gave_up")?;
                    dict.set_item("peer", peer.to_full())?;
                }
            }
            delivery.event_loop.call_method1(
                py,
                "call_soon_threadsafe",
                (&delivery.callback, dict),
            )?;
            Ok(())
        });
        match delivered {
            Some(Ok(())) => {}
            Some(Err(e)) => tracing::warn!("failed to deliver event to python: {e}"),
            None => return, // interpreter is shutting down
        }
    }
}
