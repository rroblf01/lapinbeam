"""Metadata about the message an actor handler is currently processing."""

import contextvars
from typing import Any, NamedTuple, Optional

from .refs import ActorRef, RemoteRef


class MessageMeta(NamedTuple):
    """Metadata carried alongside a message delivered to an actor.

    `src` is the sending node's id — for a message sent by a local actor,
    that's this node's own id, since sender and receiver are the same node.
    `reply_to` and `correlation_id` are whatever the sender passed to
    `send()`, or `None` if it didn't. `msg_id` is a per-node monotonic id
    assigned by the transport to remote messages only; it's always `None`
    for local sends. `node` is the `Node` that received this message —
    used by `reply()`, but also handy if you need it directly.
    """

    src: Optional[str]
    reply_to: Optional[str]
    correlation_id: Optional[int]
    msg_id: Optional[int]
    node: Any

    async def reply(self, msg):
        """Sends `msg` back to whoever set `reply_to` on the message this
        describes, tagged with the same `correlation_id` so `ask()` (or a
        manual correlation check) can match it to the original send.

        Raises `RuntimeError` if `reply_to` is `None` — nothing sent it a
        return address, so there's nothing to reply to.
        """
        if self.reply_to is None:
            raise RuntimeError("message has no reply_to; nothing to reply to")
        if self.src == self.node.local_id:
            ref = ActorRef(self.node, self.reply_to)
        else:
            ref = RemoteRef(self.node, self.src, self.reply_to)
        await ref.send(msg, correlation_id=self.correlation_id)


#: Bound by `Supervisor._drive` for the duration of each handler call.
current = contextvars.ContextVar("lapinbeam_current_message", default=None)


def current_message() -> Optional[MessageMeta]:
    """Metadata for the message the currently running code is processing.

    Bound for the duration of each handler call by `Supervisor._drive`.
    Because this is a `contextvars.ContextVar`, a task created with
    `asyncio.create_task()` from *inside* a handler inherits whatever
    `current_message()` returns at the moment the task is created — and
    keeps returning that same, increasingly stale `MessageMeta` for as long
    as it runs, even after the handler that spawned it has moved on to a
    different message (or finished). Don't call `.reply()` from such a
    background task expecting it to still target the right message — read
    what you need from `current_message()` before creating the task and
    pass it in explicitly instead.

    Returns `None` outside of any handler's context — e.g. a task created
    at module scope, before any message has been dispatched.
    """
    return current.get()
