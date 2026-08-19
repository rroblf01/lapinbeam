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
        # lapinbeam.links state — cheap to keep even if links are never
        # used. `_links`: local actor name -> set of (peer_id, name)
        # targets it's linked to (peer_id is None for a local target).
        # `_trap_exit`: local actor name -> whether it wants exits
        # delivered as an Exit message instead of being killed by them.
        # `_live_children`: local actor name -> the Supervisor's live
        # `_ActorChild` record, so a link-triggered exit can find (and
        # cancel) the actual running task — populated/cleared by
        # `Supervisor._run_actor_child` alongside `_mailboxes`.
        self._links = {}
        self._trap_exit = {}
        self._live_children = {}
        # lapinbeam.groups state — likewise cheap to keep unused.
        # `_groups`: group name -> set of (origin_node_id, actor_name)
        # members. `_group_leave_hook`, if `register_groups()` was called,
        # broadcasts a "leave" delta when `_clear_groups` drops a member —
        # kept as a plug point rather than importing `.groups` here, same
        # reasoning as `_deliver_exit`'s local import of `.links`.
        self._groups = {}
        self._group_leave_hook = None
        # lapinbeam.monitors state — likewise cheap to keep unused.
        # `_monitors`: ref -> (watcher_name, peer_id, target_name), owned
        # by the watcher's own node (peer_id is None for a local target).
        # `_monitor_watchers`: local target name -> {ref: (watcher_node,
        # watcher_name)}, owned by the *target's* node — this is the side
        # that decides when to fire a `Down`.
        self._monitors = {}
        self._monitor_watchers = {}
        # lapinbeam.registry state — likewise cheap to keep unused.
        # `_registry`: name -> (origin_node_id, actor_name), one owner per
        # name, converged the same way `_groups` is (delta + snapshot).
        self._registry = {}
        self._registry_release_hook = None

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
        - `"registry_conflict"` — only fires if `lapinbeam.registry` is in
          use: this node saw two different claims to the same
          `event["name"]` (`event["existing"]` kept, `event["incoming"]`
          rejected) — see `lapinbeam.registry`'s module docstring for why
          this can happen and what it doesn't guarantee.

        Without a registered handler these are otherwise invisible —
        message delivery is fire-and-forget.
        """
        self._event_listeners.append(callback)

    def _on_core_event(self, event):
        kind = event.get("kind")
        if kind == "peer_connected":
            waiters = self._peer_waiters.get(event.get("peer"))
            if waiters:
                for fut in waiters:
                    if not fut.done():
                        fut.set_result(None)
        elif kind == "peer_disconnected":
            # A linked actor's node dropped off the network — deliver a
            # "noconnection" exit to anything that had linked to one of
            # its actors, then forget those links (no auto-relink on
            # reconnect). `_on_core_event` itself must stay synchronous
            # (Rust calls it via call_soon_threadsafe), so the actual
            # delivery — which needs to await — runs as its own task.
            peer = event.get("peer")
            for name, targets in list(self._links.items()):
                gone = [t for t in targets if t[0] == peer]
                for target in gone:
                    targets.discard(target)
                    asyncio.create_task(
                        self._deliver_exit(f"{target[0]}/{target[1]}", name, "noconnection")
                    )
            # Symmetric cascade for monitors: any local actor watching one
            # on the peer that just dropped off the network gets a `Down`
            # with reason "noconnection", same as links' "noconnection".
            for target_name, watchers in list(self._monitor_watchers.items()):
                gone_refs = [
                    ref for ref, (watcher_node, _n) in watchers.items() if watcher_node == peer
                ]
                for ref in gone_refs:
                    watchers.pop(ref, None)
            for ref, (_watcher_name, peer_id, name) in list(self._monitors.items()):
                if peer_id == peer:
                    asyncio.create_task(self._deliver_down(ref, f"{peer_id}/{name}", "noconnection"))
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

    async def _deliver_exit(self, from_label, target_name, reason):
        """Delivers a `lapinbeam.links` exit signal to `target_name`: an
        ordinary `Exit` message if it called `trap_exit()`, otherwise a
        kill routed through its own Supervisor's normal crash/restart
        path (see `Supervisor._run_actor_child`'s `pending_exit` check)."""
        from .links import Exit

        if self._trap_exit.get(target_name):
            if target_name in self._mailboxes:
                await self._send_local(target_name, Exit(actor=from_label, reason=reason))
            return
        child = self._live_children.get(target_name)
        if child is not None and child.driver is not None and not child.driver.done():
            child.pending_exit = RuntimeError(f"linked actor {from_label!r} exited: {reason}")
            child.driver.cancel()

    def _clear_links(self, name):
        """Called by `Supervisor._run_actor_child` every time an actor's
        current generation ends, whether it's about to restart in place or
        gone for good — links don't survive a restart, so the next
        generation (if any) starts with a clean slate either way. Returns
        the targets it was linked to, so the caller can notify them if
        (and only if) this generation isn't coming back."""
        self._trap_exit.pop(name, None)
        return self._links.pop(name, None)

    def _clear_groups(self, name):
        """Called by `Supervisor._run_actor_child` every time an actor's
        current generation ends, restarting or not — unlike links, a group
        "leave" is broadcast unconditionally (not only on final death):
        membership is pid-scoped, so even a within-budget restart means
        this generation is no longer a member, full stop. A fresh
        generation that wants to stay a member re-`join_group()`s itself,
        typically from `__init__`."""
        target = (self.local_id, name)
        left = [group for group, members in self._groups.items() if target in members]
        for group in left:
            self._groups[group].discard(target)
            if self._group_leave_hook is not None:
                self._group_leave_hook(group, name)
        return left

    def _notify_links(self, actor_name, targets, reason):
        """Tells every target `actor_name` was linked to (captured via
        `_clear_links`) that it has exited for good."""
        if not targets:
            return
        for peer_id, target_name in targets:
            if peer_id is None:
                asyncio.create_task(self._deliver_exit(actor_name, target_name, reason))
            else:
                asyncio.create_task(
                    self._notify_remote_exit(peer_id, target_name, actor_name, reason)
                )

    async def _notify_remote_exit(self, peer_id, target_name, actor_name, reason):
        from .links import LINK_ACTOR

        remote = self.get_remote_actor(peer_id, LINK_ACTOR)
        try:
            await remote.send({
                "op": "exit", "to": target_name, "actor": actor_name,
                "reason": reason, "from_node": self.local_id,
            })
        except Exception:
            pass  # best-effort, same as any other fire-and-forget notification

    async def _deliver_down(self, ref, actor_label, reason):
        """Delivers a `lapinbeam.monitors` `Down` signal for `ref` — looks
        up (and forgets) the watcher `monitor()` recorded for it on *this*
        node, and delivers an ordinary message. A no-op if the watcher
        already doesn't exist (already died, or the ref was never ours)."""
        from .monitors import Down

        entry = self._monitors.pop(ref, None)
        if entry is None:
            return
        watcher_name = entry[0]
        if watcher_name in self._mailboxes:
            await self._send_local(watcher_name, Down(ref=ref, actor=actor_label, reason=reason))

    def _clear_monitors(self, name):
        """Called by `Supervisor._run_actor_child` every time a monitored
        actor's current generation ends, whether restarting or gone for
        good — same pid-scoping rationale as `_clear_links`. Returns the
        watchers to notify (`{ref: (watcher_node, watcher_name)}`) if (and
        only if) this generation isn't coming back."""
        return self._monitor_watchers.pop(name, None)

    def _notify_monitors(self, actor_name, watchers, reason):
        """Tells every watcher of `actor_name` (captured via
        `_clear_monitors`) that it has exited for good."""
        if not watchers:
            return
        for ref, (watcher_node, watcher_name) in watchers.items():
            if watcher_node is None:
                asyncio.create_task(self._deliver_down(ref, actor_name, reason))
            else:
                asyncio.create_task(
                    self._notify_remote_down(watcher_node, ref, actor_name, reason)
                )

    async def _notify_remote_down(self, peer_id, ref, actor_name, reason):
        from .monitors import MONITOR_ACTOR

        remote = self.get_remote_actor(peer_id, MONITOR_ACTOR)
        try:
            await remote.send({
                "op": "down", "ref": ref, "actor": actor_name,
                "reason": reason, "from_node": self.local_id,
            })
        except Exception:
            pass  # best-effort, same as any other fire-and-forget notification

    def _clear_registry(self, name):
        """Called by `Supervisor._run_actor_child` every time an actor's
        current generation ends, restarting or not — unlike links, a
        registry release is unconditional (same rationale as
        `_clear_groups`): a name is pid-scoped, so even a within-budget
        restart means this generation no longer owns it. A fresh
        generation that wants to keep the name calls `register_name()`
        again, typically from its first message handler."""
        released = [
            name_ for name_, (origin, actor_name) in self._registry.items()
            if origin == self.local_id and actor_name == name
        ]
        for name_ in released:
            del self._registry[name_]
            if self._registry_release_hook is not None:
                self._registry_release_hook(name_, name)
        return released

    def __repr__(self):
        return f"<Node {self.local_id!r}>"
