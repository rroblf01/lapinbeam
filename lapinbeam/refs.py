"""References to local and remote actors."""

import asyncio
import itertools

_ask_ids = itertools.count(1)


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


class ActorRef:
    """Reference to an actor living on the same node."""

    def __init__(self, node, name, task=None):
        self._node = node
        self.name = name
        # Watcher task driving the actor (used by the Supervisor).
        self.task = task

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

    def __repr__(self):
        return f"<RemoteRef {self.actor_name!r} at {self.peer_id!r}>"
