"""Node: the local endpoint of the distributed runtime.

A `Node` wraps the native `_core.Node`: it owns the background Tokio runtime,
the listener and the peer connections. Python actors are delivered to through
`loop.call_soon_threadsafe` and processed by their own asyncio dispatcher task.
"""

import asyncio
import socket

import lapinbeam._core as _core

from . import codec
from .refs import RemoteRef

#: The most recently started node; used by `Supervisor` when no node is given.
_current_node = None


def get_current_node():
    return _current_node


class Node:
    """A distributed node identified by `name@host:port`."""

    def __init__(self, node_name, listen_port=None, reconnect_interval=1.0,
                 connect_timeout=5.0):
        self.node_id = self._build_id(node_name, listen_port)
        self._reconnect_interval = reconnect_interval
        self._connect_timeout = connect_timeout
        self._core = None
        self._mailboxes = {}
        self._stopped = None
        self._started = False
        self._event_listeners = []
        self._peer_waiters = {}

    @staticmethod
    def _build_id(node_name, listen_port):
        if "@" in node_name:
            return node_name
        host = socket.gethostname() or "127.0.0.1"
        port = listen_port or 0
        return f"{node_name}@{host}:{port}"

    @property
    def local_id(self):
        """Actual node id, with the resolved listening port."""
        if self._core is None:
            return self.node_id
        return self._core.local_id()

    async def start(self):
        """Starts the background runtime and binds the listener."""
        if self._started:
            return
        self._core = _core.Node(self.node_id, self._reconnect_interval)
        self._core.start()
        self.node_id = self._core.local_id()
        self._stopped = asyncio.Event()
        self._started = True
        loop = asyncio.get_running_loop()
        self._core.set_event_handler(loop, self._on_core_event)
        global _current_node
        _current_node = self

    async def stop(self):
        """Shuts down the background runtime."""
        if self._core is not None:
            self._core.stop()
            self._core = None
        self._mailboxes.clear()
        self._started = False
        for waiters in self._peer_waiters.values():
            for fut in waiters:
                if not fut.done():
                    fut.set_exception(ConnectionError("node stopped while connecting"))
        self._peer_waiters.clear()
        if self._stopped is not None:
            self._stopped.set()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    async def wait_until_stopped(self):
        """Blocks until `stop()` is called."""
        if self._stopped is None:
            return
        await self._stopped.wait()

    def on_event(self, callback):
        """Registers `callback(event: dict)` for system events.

        `event["kind"]` is one of `"peer_connected"`, `"peer_disconnected"`
        or `"error"` (a peer reported a delivery failure, e.g. sending to an
        unknown remote actor); `event["peer"]` is the peer's full id, and
        `event["detail"]` carries the error message for `"error"` events.
        Without a registered handler these events are otherwise invisible —
        message delivery is fire-and-forget.
        """
        self._event_listeners.append(callback)

    def _on_core_event(self, event):
        if event.get("kind") == "peer_connected":
            waiters = self._peer_waiters.get(event.get("peer"))
            if waiters:
                for fut in waiters:
                    if not fut.done():
                        fut.set_result(None)
        for callback in list(self._event_listeners):
            callback(event)

    async def connect_peer(self, peer_id):
        """Connects to a peer and waits until the handshake completes."""
        if self._core is None:
            raise RuntimeError("node has not been started")
        self._core.connect_peer(peer_id)
        if self._core.has_peer(peer_id):
            return
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._peer_waiters.setdefault(peer_id, []).append(fut)
        try:
            # Re-check after registering the waiter: closes the race where
            # the connection completes between the check above and this
            # point, since PeerConnected would otherwise fire with no one
            # listening yet.
            if self._core.has_peer(peer_id):
                return
            await asyncio.wait_for(fut, timeout=self._connect_timeout)
        except asyncio.TimeoutError:
            raise ConnectionError(f"failed to connect to peer {peer_id!r}") from None
        finally:
            waiters = self._peer_waiters.get(peer_id)
            if waiters and fut in waiters:
                waiters.remove(fut)
                if not waiters:
                    self._peer_waiters.pop(peer_id, None)

    def has_peer(self, peer_id):
        """Whether `peer_id` is currently connected."""
        if self._core is None:
            return False
        return self._core.has_peer(peer_id)

    def get_remote_actor(self, peer_id, actor_name):
        """Returns a reference to an actor on a remote node."""
        return RemoteRef(self, peer_id, actor_name)

    def register_actor(self, name, mailbox):
        """Registers the asyncio mailbox for a local actor."""
        if self._core is None:
            raise RuntimeError("node has not been started")
        self._mailboxes[name] = mailbox
        loop = asyncio.get_running_loop()
        callback = lambda msg: mailbox.put_nowait(codec.decode_payload(msg))  # noqa: E731
        self._core.register_actor(name, loop, callback)

    def unregister_actor(self, name):
        self._mailboxes.pop(name, None)
        if self._core is not None:
            self._core.unregister_actor(name)

    async def _send_local(self, name, msg):
        mailbox = self._mailboxes.get(name)
        if mailbox is None:
            raise ValueError(f"no local actor named {name!r}")
        await mailbox.put(msg)

    async def _send_remote(self, peer_id, actor_name, msg, reply_to=None, correlation_id=None):
        if self._core is None:
            raise RuntimeError("node has not been started")
        payload = codec.encode_payload(msg)
        self._core.send_data(peer_id, actor_name, payload, reply_to, correlation_id)

    def __repr__(self):
        return f"<Node {self.local_id!r}>"
