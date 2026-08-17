"""Metadata about the message an actor handler is currently processing."""

import contextvars
from typing import NamedTuple, Optional


class MessageMeta(NamedTuple):
    """Metadata carried alongside a message delivered to an actor.

    `src` is the sending node's id — for a message sent by a local actor,
    that's this node's own id, since sender and receiver are the same node.
    `reply_to` and `correlation_id` are whatever the sender passed to
    `send()`, or `None` if it didn't. `msg_id` is a per-node monotonic id
    assigned by the transport to remote messages only; it's always `None`
    for local sends.
    """

    src: Optional[str]
    reply_to: Optional[str]
    correlation_id: Optional[int]
    msg_id: Optional[int]


#: Bound by `Supervisor._drive` for the duration of each handler call.
current = contextvars.ContextVar("lapinbeam_current_message", default=None)


def current_message() -> Optional[MessageMeta]:
    """Metadata for the message the running actor handler is processing.

    Returns `None` outside of a handler call — e.g. from a background task
    an actor spawned itself, which runs outside the handler's context.
    """
    return current.get()
