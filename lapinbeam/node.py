"""Node: the local endpoint of the distributed runtime.

A `Node` wraps the native `_core.Node`: it owns the background Tokio runtime,
the listener and the peer connections. Python actors are delivered to through
`loop.call_soon_threadsafe` and processed by their own asyncio dispatcher task.
"""

import asyncio
import socket

import lapinbeam._core as _core

from .refs import RemoteRef

#: The most recently started node; used by `Supervisor` when no node is given.
_current_node = None


def get_current_node():
    return _current_node


class Node:
    """A distributed node identified by `name@host:port`."""

    def __init__(self, node_name, listen_port=None):
        self.node_id = self._build_id(node_name, listen_port)
        self._core = None
        self._mailboxes = {}
        self._stopped = None
        self._started = False

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
        self._core = _core.Node(self.node_id)
        self._core.start()
        self.node_id = self._core.local_id()
        self._stopped = asyncio.Event()
        self._started = True
        global _current_node
        _current_node = self

    async def stop(self):
        """Shuts down the background runtime."""
        if self._core is not None:
            self._core.stop()
            self._core = None
        self._mailboxes.clear()
        self._started = False
        if self._stopped is not None:
            self._stopped.set()

    async def wait_until_stopped(self):
        """Blocks until `stop()` is called."""
        if self._stopped is None:
            return
        await self._stopped.wait()

    async def connect_peer(self, peer_id):
        """Connects to a peer and waits until the handshake completes."""
        if self._core is None:
            raise RuntimeError("node has not been started")
        self._core.connect_peer(peer_id)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5.0
        while not self._core.has_peer(peer_id):
            if loop.time() > deadline:
                raise ConnectionError(f"failed to connect to peer {peer_id!r}")
            await asyncio.sleep(0.01)

    def get_remote_actor(self, peer_id, actor_name):
        """Returns a reference to an actor on a remote node."""
        return RemoteRef(self, peer_id, actor_name)

    def register_actor(self, name, mailbox):
        """Registers the asyncio mailbox for a local actor."""
        if self._core is None:
            raise RuntimeError("node has not been started")
        self._mailboxes[name] = mailbox
        loop = asyncio.get_running_loop()
        callback = lambda msg: mailbox.put_nowait(msg)  # noqa: E731
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
        self._core.send_data(peer_id, actor_name, msg, reply_to, correlation_id)

    def __repr__(self):
        return f"<Node {self.local_id!r}>"
