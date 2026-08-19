"""One-way process monitoring — Erlang-style: `monitor(other)` watches
another actor/peer without linking to it. If the monitored actor exits
for good, the watcher receives a `Down` message; nothing happens to the
monitored actor, and nothing happens to the watcher if *it* dies first —
contrast with `lapinbeam.links`, whose default behavior kills both sides.
`monitor()` is the tool for "I want to know when X is gone" without
taking on any risk of being killed by it, or any obligation to kill it.

No wire protocol changes: cross-node monitors ride as ordinary `Data`
frames addressed to a reserved, well-known local actor name
(`__lapinbeam_monitor__`, registered per `Node` via `register_monitors()`
— the same trick `lapinbeam.discovery`/`links`/`groups` use for their own
well-known actors). A peer that hasn't called `register_monitors()` simply
has no such actor registered, so monitoring one of its actors fails with
an ordinary `actor_not_found` error instead of breaking the connection.

Monitors are pid-scoped like links: they fire only when the monitored
generation is gone for good (give-up/clean-return/explicit shutdown), not
on an ordinary in-place restart within budget — the same accepted
simplification `lapinbeam.links` makes, for the same reason (see its
module docstring): a supervisor-restarted actor is, conceptually, a new
generation the old monitor never asked to watch.

Known limitation, accepted rather than solved for something this small:
if the *watcher* itself restarts or dies without calling `demonitor()`
first, its outstanding monitors are not automatically cleaned up on the
monitored side — they simply sit there until the monitored actor
eventually exits for good (at which point the `Down` is delivered to a
watcher name that may no longer exist, and is silently dropped) or
`demonitor()` is called. For a monitor whose target rarely or never
exits, a watcher that creates many of them across repeated restarts
without ever demonitoring will accumulate unbounded bookkeeping — call
`demonitor()` when you're done, the same discipline `unlink()` already
asks for.
"""

import uuid
from dataclasses import dataclass

from .actor import actor
from .context import current_actor_ref
from .refs import RemoteRef

MONITOR_ACTOR = "__lapinbeam_monitor__"


@dataclass
class Down:
    """Delivered as an ordinary message — handle it with `@on(Down)` — to
    an actor that called `monitor()`, when the actor/peer it monitored
    exits for good."""

    ref: str
    actor: str
    reason: str


def _me():
    ref = current_actor_ref()
    if ref is None:
        raise RuntimeError(
            "monitor()/demonitor() must be called from inside a running actor"
        )
    return ref


def _target(other):
    """`(peer_id, name)` identifying `other` — `peer_id` is `None` for a
    local `ActorRef`."""
    if isinstance(other, RemoteRef):
        return (other.peer_id, other.actor_name)
    return (None, other.name)


async def monitor(other):
    """Monitors `other` (an `ActorRef` for a local actor, or a `RemoteRef`
    for one on another node) from the currently-running actor. Returns an
    opaque `ref` string — pass it to `demonitor()` to stop, and it's
    echoed back on the eventual `Down.ref` so a watcher juggling several
    monitors can tell them apart.

    Monitoring an actor that doesn't exist — locally, or on a remote peer
    that reports `actor_not_found` — delivers an immediate `Down` with
    reason `"noproc"`, the same signal a genuine death would produce.
    """
    me = _me()
    node = me._node
    ref = uuid.uuid4().hex
    peer_id, name = _target(other)
    node._monitors[ref] = (me.name, peer_id, name)
    if peer_id is None:
        node._monitor_watchers.setdefault(name, {})[ref] = (None, me.name)
        if name not in node._mailboxes:
            await node._deliver_down(ref, name, "noproc")
    else:
        remote = node.get_remote_actor(peer_id, MONITOR_ACTOR)
        await remote.send({
            "op": "monitor", "ref": ref, "watcher": me.name,
            "watcher_node": node.local_id, "target": name,
        })
    return ref


async def demonitor(ref):
    """Stops a monitor established with `monitor()`. Safe to call with an
    unknown or already-fired `ref`."""
    node = _me()._node
    entry = node._monitors.pop(ref, None)
    if entry is None:
        return
    _watcher_name, peer_id, name = entry
    if peer_id is None:
        watchers = node._monitor_watchers.get(name)
        if watchers is not None:
            watchers.pop(ref, None)
    else:
        remote = node.get_remote_actor(peer_id, MONITOR_ACTOR)
        try:
            await remote.send({"op": "demonitor", "ref": ref, "target": name})
        except Exception:
            pass  # best-effort, same as any other fire-and-forget notification


def register_monitors(node, sup):
    """Sets up this node to answer cross-node monitor requests and
    deliver `Down` signals. Only needed if you plan to `monitor()` (or be
    monitored by) an actor on a *different* node — purely local monitors
    work without it.

    Call once per node, alongside `register_links`/`register_groups` if
    you use those too.
    """
    return sup.spawn(_MonitorActor, node)


@actor(name=MONITOR_ACTOR)
class _MonitorActor:
    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        op = msg["op"]
        if op == "monitor":
            target = msg["target"]
            ref = msg["ref"]
            self.node._monitor_watchers.setdefault(target, {})[ref] = (
                msg["watcher_node"], msg["watcher"],
            )
            if target not in self.node._mailboxes:
                remote = self.node.get_remote_actor(msg["watcher_node"], MONITOR_ACTOR)
                await remote.send({
                    "op": "down", "ref": ref, "watcher": msg["watcher"],
                    "actor": target, "reason": "noproc", "from_node": self.node.local_id,
                })
        elif op == "demonitor":
            watchers = self.node._monitor_watchers.get(msg["target"])
            if watchers is not None:
                watchers.pop(msg["ref"], None)
        elif op == "down":
            label = f"{msg['from_node']}/{msg['actor']}"
            await self.node._deliver_down(msg["ref"], label, msg["reason"])
