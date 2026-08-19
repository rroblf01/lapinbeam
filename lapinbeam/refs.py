"""References to local and remote actors."""

import asyncio
import itertools

_ask_ids = itertools.count(1)

#: Reserved envelope key `MessageMeta.reply_stream()`/`reply_final()` wrap
#: a payload in, and `ask_stream()` unwraps — see context.py. Shared here
#: (not in context.py) to avoid a Python import cycle: context.py already
#: imports `ActorRef`/`RemoteRef` from this module.
STREAM_KEY = "__lapinbeam_stream__"


async def _ask(ref, msg, timeout):
    """Shared `ask()` implementation for `ActorRef`/`RemoteRef`.

    Registers a one-shot hidden mailbox as the reply address, tags the
    send with a fresh `correlation_id`, and waits up to `timeout` seconds
    for a single reply — works the same regardless of whether `ref` is
    local or remote, since both already support `reply_to`/`correlation_id`.
    The mailbox is always cleaned up, whether a reply arrives, the wait
    times out, or `send()` itself raises.
    """
    node = ref._node
    n = next(_ask_ids)
    name = f"__lapinbeam_ask_{n}__"
    mailbox = asyncio.Queue(maxsize=1)
    node.register_actor(name, mailbox)
    try:
        await ref.send(msg, reply_to=name, correlation_id=n)
        reply, _meta = await asyncio.wait_for(mailbox.get(), timeout=timeout)
        return reply
    finally:
        node.unregister_actor(name)


async def _ask_stream(ref, msg, timeout):
    """Shared `ask_stream()` implementation for `ActorRef`/`RemoteRef`.

    Like `_ask`, but the hidden reply mailbox is unbounded (a burst of
    `reply_stream()` calls must never silently drop one waiting for the
    consumer to catch up, the way a bounded mailbox would) and it keeps
    yielding replies — one per `reply_stream()`/`reply_final()` call the
    handler makes — until one arrives tagged `"final"`, then stops and
    cleans up. `timeout` applies per item, not to the stream as a whole:
    the clock resets after every yield, so a handler that's still actively
    sending updates never times out just because the *total* stream is
    long.
    """
    node = ref._node
    n = next(_ask_ids)
    name = f"__lapinbeam_ask_{n}__"
    mailbox = asyncio.Queue()
    node.register_actor(name, mailbox)
    try:
        await ref.send(msg, reply_to=name, correlation_id=n)
        while True:
            reply, _meta = await asyncio.wait_for(mailbox.get(), timeout=timeout)
            if not (isinstance(reply, dict) and STREAM_KEY in reply):
                raise RuntimeError(
                    "ask_stream() received a reply not sent via "
                    "reply_stream()/reply_final() — did the handler use "
                    "current_message().reply() instead?"
                )
            yield reply["value"]
            if reply[STREAM_KEY] == "final":
                return
    finally:
        node.unregister_actor(name)


class ActorRef:
    """Reference to an actor living on the same node."""

    def __init__(self, node, name, child=None):
        self._node = node
        self.name = name
        # Internal: the Supervisor's live child record, if this ref was
        # constructed by `Supervisor.spawn()`/`_run_actor_child` — `None`
        # for a ref built for messaging purposes only (e.g.
        # `MessageMeta.reply()`), which has no watcher task to expose.
        self._child = child

    @property
    def task(self):
        """The task currently driving this actor, or `None` if this ref
        has no associated Supervisor child record.

        A live lookup, not a value frozen at construction time: under
        `one_for_all`/`rest_for_one`, a sibling swept into a group restart
        gets a *new* task object, so a `ref` obtained before that restart
        must still observe the current one — freezing `task` at spawn time
        would make `await ref.task` wrongly raise `CancelledError` for a
        healthy, successfully-restarted actor.
        """
        return self._child.task if self._child is not None else None

    async def send(self, msg, reply_to=None, correlation_id=None):
        """Fire-and-forget delivery of `msg` to the local actor.

        `reply_to`/`correlation_id`, if given, are available to the
        receiving handler via `lapinbeam.current_message()`.
        """
        await self._node._send_local(
            self.name, msg, reply_to=reply_to, correlation_id=correlation_id
        )

    async def ask(self, msg, timeout=5.0):
        """Sends `msg` and waits for a single correlated reply.

        The receiving handler must actually reply — either
        `current_message().reply(response)`, or a manual
        `send(response, correlation_id=current_message().correlation_id)`
        to the address in `current_message().reply_to`. Raises
        `TimeoutError` if nothing replies within `timeout` seconds (`None`
        waits indefinitely).
        """
        return await _ask(self, msg, timeout)

    def ask_stream(self, msg, timeout=5.0):
        """Sends `msg` and returns an async iterator over every reply the
        handler sends via `current_message().reply_stream()`, stopping
        right after the one sent via `reply_final()`.

        Use this instead of `ask()` when the receiving handler needs to
        report *progress*, not just a single final answer — e.g. a
        long-running job that reports each step as it completes. `timeout`
        applies per item (see `reply_stream()`'s docstring), not to the
        whole stream. Raises `TimeoutError` if nothing arrives within
        `timeout` seconds of the send, or between any two items.
        """
        return _ask_stream(self, msg, timeout)

    def __repr__(self):
        return f"<ActorRef {self.name!r} on {self._node.local_id!r}>"


class RemoteRef:
    """Reference to an actor living on another node."""

    def __init__(self, node, peer_id, actor_name):
        self._node = node
        self.peer_id = peer_id
        self.actor_name = actor_name

    async def send(self, msg, reply_to=None, correlation_id=None):
        """Fire-and-forget delivery of `msg` over the wire.

        `reply_to`/`correlation_id`, if given, are available to the
        receiving handler via `lapinbeam.current_message()`; `correlation_id`
        is also echoed back on the `"error"` event if delivery fails (e.g.
        the target actor doesn't exist on the remote node).
        """
        await self._node._send_remote(
            self.peer_id, self.actor_name, msg, reply_to=reply_to, correlation_id=correlation_id
        )

    async def ask(self, msg, timeout=5.0):
        """Sends `msg` and waits for a single correlated reply.

        The receiving handler must actually reply — either
        `current_message().reply(response)`, or a manual
        `send(response, correlation_id=current_message().correlation_id)`
        to the address in `current_message().reply_to`. Raises
        `TimeoutError` if nothing replies within `timeout` seconds (`None`
        waits indefinitely).
        """
        return await _ask(self, msg, timeout)

    def ask_stream(self, msg, timeout=5.0):
        """Sends `msg` and returns an async iterator over every reply the
        handler sends via `current_message().reply_stream()`, stopping
        right after the one sent via `reply_final()`.

        Use this instead of `ask()` when the receiving handler needs to
        report *progress*, not just a single final answer — e.g. a
        long-running job that reports each step as it completes. `timeout`
        applies per item (see `reply_stream()`'s docstring), not to the
        whole stream. Raises `TimeoutError` if nothing arrives within
        `timeout` seconds of the send, or between any two items.
        """
        return _ask_stream(self, msg, timeout)

    def __repr__(self):
        return f"<RemoteRef {self.actor_name!r} at {self.peer_id!r}>"


class SupervisorRef:
    """Reference to a nested `Supervisor` spawned via
    `Supervisor.spawn_supervisor()`."""

    def __init__(self, child):
        self._child = child

    @property
    def supervisor(self):
        """The current live nested `Supervisor` instance — replaced (not
        mutated) every time this subtree restarts, same rationale as
        `ActorRef.task`."""
        return self._child.supervisor

    @property
    def task(self):
        """The task currently running this subtree. `await ref.task`
        blocks until the whole subtree gives up, re-raising the original
        exception that caused it — not a generic wrapper — recursively
        through nested levels."""
        return self._child.task

    def __repr__(self):
        return f"<SupervisorRef {self._child.name!r}>"


class PoolRef:
    """Reference to a fixed worker pool spawned via
    `Supervisor.spawn_pool()`. Quacks like an `ActorRef`/`RemoteRef` for
    the one thing that matters — `send()`/`ask()`/`ask_stream()` all
    address the pool's single reserved dispatcher actor, which hands the
    message to whichever worker is free next, not to a specific one.
    `node.get_remote_actor(peer, pool.name)` lets another node reach the
    same pool exactly the same way.
    """

    def __init__(self, dispatcher_ref, worker_refs):
        self._dispatcher = dispatcher_ref
        self._workers = worker_refs

    @property
    def name(self):
        """The dispatcher's actor name — what a remote node addresses via
        `get_remote_actor(peer, pool.name)`."""
        return self._dispatcher.name

    @property
    def size(self):
        """Number of workers in the pool — fixed for its whole lifetime."""
        return len(self._workers)

    async def send(self, msg, reply_to=None, correlation_id=None):
        """Fire-and-forget delivery to whichever worker is free next."""
        await self._dispatcher.send(msg, reply_to=reply_to, correlation_id=correlation_id)

    async def ask(self, msg, timeout=5.0):
        """Like `ActorRef.ask()`, answered by whichever worker handles
        `msg`."""
        return await self._dispatcher.ask(msg, timeout=timeout)

    def ask_stream(self, msg, timeout=5.0):
        """Like `ActorRef.ask_stream()`, answered by whichever worker
        handles `msg`."""
        return self._dispatcher.ask_stream(msg, timeout=timeout)

    def __repr__(self):
        return f"<PoolRef {self.name!r} ({self.size} workers)>"
