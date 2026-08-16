"""References to local and remote actors."""


class ActorRef:
    """Reference to an actor living on the same node."""

    def __init__(self, node, name, task=None):
        self._node = node
        self.name = name
        # Watcher task driving the actor (used by the Supervisor).
        self.task = task

    async def send(self, msg):
        """Fire-and-forget delivery of `msg` to the local actor."""
        await self._node._send_local(self.name, msg)

    def __repr__(self):
        return f"<ActorRef {self.name!r} on {self._node.local_id!r}>"


class RemoteRef:
    """Reference to an actor living on another node."""

    def __init__(self, node, peer_id, actor_name):
        self._node = node
        self.peer_id = peer_id
        self.actor_name = actor_name

    async def send(self, msg):
        """Fire-and-forget delivery of `msg` over the wire."""
        await self._node._send_remote(self.peer_id, self.actor_name, msg)

    def __repr__(self):
        return f"<RemoteRef {self.actor_name!r} at {self.peer_id!r}>"
