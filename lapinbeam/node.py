"""Node: the local endpoint of the distributed runtime.

A `Node` wraps the native `_core.Node`: it owns the background Tokio runtime,
the listener and the peer connections. Python actors are delivered to through
`loop.call_soon_threadsafe` and processed by their own asyncio dispatcher task.
"""

import asyncio
import logging
import socket

import lapinbeam._core as _core

from . import codec
from .context import MessageMeta
from .refs import RemoteRef

_logger = logging.getLogger("lapinbeam")

#: The most recently started node; used by `Supervisor` when no node is given.
_current_node = None


def get_current_node():
    return _current_node


class Node:
    """A distributed node identified by `name@host:port`."""

    def __init__(self, node_name, listen_port=None, reconnect_interval=1.0,
                 connect_timeout=5.0, cluster_secret=None,
                 reconnect_max_attempts=30, heartbeat_interval=None,
                 peer_timeout=None, peer_queue_capacity=None,
                 mailbox_capacity=None):
        """`cluster_secret`, when set, must match on every node this one
        talks to: a handshake that doesn't prove knowledge of the same
        secret is rejected before ever being registered as a peer. This
        does not encrypt traffic — see docs/index.md's "Security" section
        for exactly what it does and doesn't protect against.

        `reconnect_max_attempts` bounds how many times a dropped desired
        peer is retried before giving up (see `on_event`'s
        `"reconnect_gave_up"` and `forget_peer`). Pass `None` explicitly
        for the old retry-forever behaviour — for a peer that's gone for
        good, that means an unbounded background task hammering
        `connect_peer` forever.

        `heartbeat_interval`/`peer_timeout` (seconds) control failure
        detection — a peer that sends nothing for `peer_timeout` is
        dropped. `None` keeps the defaults (1.0 / 3.0). `peer_queue_capacity`
        bounds the outbound queue kept per peer (default 256); a peer whose
        TCP write is congested can only have this many frames buffered
        before sends to it start failing.

        `mailbox_capacity` bounds how many undelivered messages an actor's
        mailbox can hold before new ones are dropped (with
        `on_event(kind="mailbox_full")`, and — for a dropped remote send —
        an `"error"` event back on the sender) instead of piling up
        forever. `None` (the default) keeps today's unbounded behaviour: an
        actor that can't keep up will have its mailbox grow without limit.
        """
        self.node_id = self._build_id(node_name, listen_port)
        self._reconnect_interval = reconnect_interval
        self._reconnect_max_attempts = reconnect_max_attempts
        self._connect_timeout = connect_timeout
        self._cluster_secret = cluster_secret
        self._heartbeat_interval = heartbeat_interval
        self._peer_timeout = peer_timeout
        self._peer_queue_capacity = peer_queue_capacity
        self.mailbox_capacity = mailbox_capacity
        self._core = None
        self._mailboxes = {}
        self._stopped = None
        self._started = False
        self._event_listeners = []
        self._peer_waiters = {}
        self._actor_tasks = set()

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
        self._core = _core.Node(
            self.node_id,
            reconnect_interval=self._reconnect_interval,
            reconnect_max_attempts=self._reconnect_max_attempts,
            cluster_secret=self._cluster_secret,
            heartbeat_interval=self._heartbeat_interval,
            peer_timeout=self._peer_timeout,
            peer_queue_capacity=self._peer_queue_capacity,
        )
        self._core.start()
        self.node_id = self._core.local_id()
        self._stopped = asyncio.Event()
        self._started = True
        loop = asyncio.get_running_loop()
        self._core.set_event_handler(loop, self._on_core_event)
        global _current_node
        _current_node = self

    async def stop(self):
        """Shuts down the background runtime.

        Also cancels every actor task spawned by any `Supervisor` on this
        node — without this, each one would be left running forever,
        permanently blocked reading from a mailbox nothing will ever fill
        again. To tear down only a specific `Supervisor`'s actors instead
        of the whole node, use `Supervisor.shutdown()`.
        """
        tasks = list(self._actor_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
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

    def _register_task(self, task):
        """Tracks a Supervisor-spawned actor task so `stop()` can cancel it.

        Self-pruning: an actor task that finishes on its own (a normal
        return, or `Supervisor` giving up on it) is dropped from tracking
        as soon as it's done, instead of accumulating here forever.
        """
        self._actor_tasks.add(task)
        task.add_done_callback(self._actor_tasks.discard)

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

        `event["kind"]` is one of:

        - `"peer_connected"` / `"peer_disconnected"` — `event["peer"]` is
          the peer's full id.
        - `"error"` — a peer reported a delivery failure (e.g. sending to
          an unknown remote actor). `event["peer"]` is the peer's full id,
          `event["detail"]` the error message, `event["correlation_id"]`
          echoes the `correlation_id` the failed `send()` was tagged with
          (`None` if it wasn't tagged).
        - `"decode_error"` — a message for a local actor failed to decode
          (e.g. a Pydantic `ValidationError`, or a dataclass missing a
          required field) and was dropped before ever reaching the actor's
          mailbox. `event["actor"]` is the actor name, `event["detail"]`
          describes the exception.
        - `"reconnect_gave_up"` — automatic reconnection to `event["peer"]`
          was abandoned after repeated failures; it's no longer retried.
          Call `connect_peer()` again to retry, or don't — either way, the
          peer is no longer tracked, so this isn't a leak left behind.
        - `"supervisor_gave_up"` — a `Supervisor` stopped restarting
          `event["actor"]` after too many crashes within its restart
          window; `event["detail"]` describes the last exception. The
          actor is no longer running and no further restarts will happen.
        - `"mailbox_full"` — a message for `event["actor"]` on *this* node
          was dropped because its mailbox was full (only possible if this
          `Node` was created with `mailbox_capacity` set — unbounded by
          default). If the dropped message came from a peer, that peer
          separately gets an `"error"` event for the same drop.

        Without a registered handler these are otherwise invisible —
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
            try:
                callback(event)
            except Exception:
                # This runs inline on whatever unrelated call raised the
                # event — an ordinary `ActorRef.send()` hitting a full
                # mailbox, or a `Supervisor` about to re-raise the real
                # crash reason after giving up on restarts. A broken
                # listener must not corrupt that caller's control flow (an
                # unexpected exception out of a "fire-and-forget" send, or
                # the real crash exception getting replaced by the
                # listener's own).
                _logger.exception("on_event listener raised for event %r", event)

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

    def forget_peer(self, peer_id):
        """Stops treating `peer_id` as desired and drops it now if connected.

        Call this once you know you're done with a peer, instead of
        waiting for automatic reconnection to give up on its own after
        repeated failures (see `on_event`'s `"reconnect_gave_up"`).
        """
        if self._core is None:
            raise RuntimeError("node has not been started")
        self._core.forget_peer(peer_id)

    def has_peer(self, peer_id):
        """Whether `peer_id` is currently connected."""
        if self._core is None:
            return False
        return self._core.has_peer(peer_id)

    def peer_count(self):
        """Number of currently connected peers."""
        if self._core is None:
            return 0
        return self._core.peer_count()

    def get_remote_actor(self, peer_id, actor_name):
        """Returns a reference to an actor on a remote node."""
        return RemoteRef(self, peer_id, actor_name)

    def register_actor(self, name, mailbox):
        """Registers the asyncio mailbox for a local actor.

        Raises `ValueError` if `name` is already registered to a *different*
        mailbox — actor names must be unique per node. Re-registering the
        same mailbox object (e.g. `Supervisor` re-registering across a
        restart, which reuses the crashed actor's mailbox) is not a
        collision and is allowed. Without this check, a second `spawn()`
        under a name already in use silently stole all future mail from the
        first actor, which kept running but could never be reached again.
        """
        if self._core is None:
            raise RuntimeError("node has not been started")
        existing = self._mailboxes.get(name)
        if existing is not None and existing is not mailbox:
            raise ValueError(f"actor name {name!r} is already registered on this node")
        self._mailboxes[name] = mailbox
        loop = asyncio.get_running_loop()

        def callback(payload, meta_dict):
            try:
                decoded = codec.decode_payload(payload)
            except Exception as exc:
                # A malformed/unvalidated payload (e.g. a Pydantic
                # ValidationError, or a dataclass missing a required field)
                # must not vanish into asyncio's default "Exception in
                # callback" log — surface it the same way any other
                # delivery failure is surfaced, so it's actually observable.
                self._on_core_event({
                    "kind": "decode_error",
                    "actor": name,
                    "detail": f"{type(exc).__name__}: {exc}",
                })
                return
            meta = MessageMeta(
                src=meta_dict["src"],
                reply_to=meta_dict["reply_to"],
                correlation_id=meta_dict["correlation_id"],
                msg_id=meta_dict["msg_id"],
                node=self,
            )
            try:
                mailbox.put_nowait((decoded, meta))
            except asyncio.QueueFull:
                # Only reachable if this Node was created with
                # mailbox_capacity set — unbounded by default. Dropping (not
                # blocking the caller) keeps this the same fire-and-forget
                # shape as every other send, and matches the drop the Rust
                # side already does if its own internal channel fills up
                # first. Every message reaching this callback is remote in
                # origin (local sends never go through it), so the sender
                # is always reachable to notify, same as any other
                # delivery failure — best-effort, never raises.
                self._on_core_event({"kind": "mailbox_full", "actor": name})
                self._core.notify_peer_error(
                    meta.src, f"mailbox_full:{name}", meta.correlation_id
                )

        self._core.register_actor(name, loop, callback)

    def unregister_actor(self, name):
        self._mailboxes.pop(name, None)
        if self._core is not None:
            self._core.unregister_actor(name)

    async def _send_local(self, name, msg, reply_to=None, correlation_id=None):
        mailbox = self._mailboxes.get(name)
        if mailbox is None:
            raise ValueError(f"no local actor named {name!r}")
        meta = MessageMeta(
            src=self.local_id,
            reply_to=reply_to,
            correlation_id=correlation_id,
            msg_id=None,
            node=self,
        )
        try:
            mailbox.put_nowait((msg, meta))
        except asyncio.QueueFull:
            self._on_core_event({"kind": "mailbox_full", "actor": name})

    async def _send_remote(self, peer_id, actor_name, msg, reply_to=None, correlation_id=None):
        if self._core is None:
            raise RuntimeError("node has not been started")
        payload = codec.encode_payload(msg)
        self._core.send_data(peer_id, actor_name, payload, reply_to, correlation_id)

    def __repr__(self):
        return f"<Node {self.local_id!r}>"
