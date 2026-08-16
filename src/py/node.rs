//! `Node` binding: bridges the Rust `Transport` (on its own Tokio runtime) to
//! the Python event loop.
//!
//! Inbound `Data` messages are delivered by scheduling a callback on the
//! Python event loop with `loop.call_soon_threadsafe`, so actors keep running
//! inside Python's asyncio while all network I/O happens off the GIL.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::sync::mpsc;

use crate::runtime::NodeId;
use crate::transport::{Transport, TransportConfig};
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
    #[new]
    #[pyo3(signature = (node_id, reconnect_interval=None))]
    fn new(node_id: &str, reconnect_interval: Option<f64>) -> PyResult<Self> {
        let local = NodeId::parse(node_id)
            .map_err(|e| PyValueError::new_err(format!("invalid node id: {e}")))?;
        Ok(Node {
            local,
            transport: None,
            runtime: None,
            handle: None,
            reconnect_interval: reconnect_interval
                .map(std::time::Duration::from_secs_f64),
        })
    }

    /// Starts the background Tokio runtime and binds the listener.
    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to start runtime: {e}")))?;

        let local = self.local.clone();
        let config = match self.reconnect_interval {
            Some(interval) => TransportConfig {
                reconnect_interval: interval,
                ..Default::default()
            },
            None => TransportConfig::default(),
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
    /// scheduled as `callback(message_dict)` on `event_loop` via
    /// `call_soon_threadsafe`.
    fn register_actor(
        &mut self,
        name: &str,
        event_loop: Py<PyAny>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        let (transport, handle) = self.require()?;
        let name = name.to_string();
        let (tx, rx) = mpsc::channel(256);
        let transport_register = transport.clone();
        handle.spawn(async move {
            transport_register.register_actor(name, tx).await;
        });
        let delivery = Delivery { event_loop, callback };
        handle.spawn(drain_loop(rx, delivery));
        Ok(())
    }

    /// Removes a local actor mailbox.
    fn unregister_actor(&mut self, name: &str) -> PyResult<()> {
        let (transport, handle) = self.require()?;
        let name = name.to_string();
        handle.spawn(async move {
            transport.unregister_actor(&name).await;
        });
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
        let value = convert::py_to_json(payload)?;
        let (transport, handle) = self.require()?;
        py.detach(|| {
            handle.block_on(transport.send_data(&peer, dst_actor, value, reply_to, correlation_id))
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
        let payload = match msg.payload_json() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(%e, "undeliverable payload, dropping");
                continue;
            }
        };
        let delivered = Python::try_attach(|py| -> PyResult<()> {
            let obj = convert::json_to_py(py, &payload)?;
            delivery
                .event_loop
                .call_method1(py, "call_soon_threadsafe", (&delivery.callback, obj))?;
            Ok(())
        });
        match delivered {
            Some(Ok(())) => {}
            Some(Err(e)) => tracing::warn!("failed to deliver message to python: {e}"),
            None => return, // interpreter is shutting down
        }
    }
}
