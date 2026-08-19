"""In-process broadcast for live SSE updates. Postgres (`db.py`) is the
durable source of truth — this only exists so a client connected *right
now* sees new steps the instant they happen, instead of polling. A client
that reconnects gets its history from Postgres regardless of whether this
process has restarted, so nothing here needs to survive a restart."""

import asyncio

_subscribers = {}  # investigation_id -> set[asyncio.Queue]


def subscribe(investigation_id):
    queue = asyncio.Queue()
    _subscribers.setdefault(investigation_id, set()).add(queue)
    return queue


def unsubscribe(investigation_id, queue):
    subs = _subscribers.get(investigation_id)
    if subs is not None:
        subs.discard(queue)
        if not subs:
            _subscribers.pop(investigation_id, None)


def publish(investigation_id, event):
    for queue in _subscribers.get(investigation_id, ()):
        queue.put_nowait(event)
