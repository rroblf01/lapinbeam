"""References to local and remote actors."""


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

    def __repr__(self):
        return f"<RemoteRef {self.actor_name!r} at {self.peer_id!r}>"
