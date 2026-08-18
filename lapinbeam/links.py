"""Bidirectional links between actors — Erlang-style: once two actors are
linked, if either one exits *for good* (its Supervisor gives up, it
returns cleanly, or it's explicitly shut down — not on an ordinary
in-place restart within budget), the other receives an exit signal too. By
default this kills the other, through the exact same crash/restart
machinery as any other failure; an actor that calls `trap_exit()` instead
receives the signal as an ordinary `Exit` message.

No wire protocol changes: cross-node links ride as ordinary `Data` frames
addressed to a reserved, well-known local actor name
(`__lapinbeam_link__`, registered per `Node` via `register_links()` — the
same trick `lapinbeam.discovery` uses for its own well-known actor). A
peer that hasn't called `register_links()` simply has no such actor
registered, so linking to one of its actors fails with an ordinary
`actor_not_found` error instead of breaking the connection — graceful,
per-peer degradation, not a forced simultaneous cluster upgrade.

Links are pid-scoped, not name-scoped: they do not survive their own
in-place restart (mirroring real Erlang, where a supervisor-restarted
process is a new pid and old links no longer apply) — call `link()` again
from `__init__` if a restarted actor should re-establish them.

Known asymmetry, accepted rather than solved for something this small:
when actor A restarts in place, only *A's* side of the link is cleared —
if A's partner B never itself restarts or re-links, B's own bookkeeping
still names A as linked. If B dies for good afterward, it will still try
to notify (and, if not trapping, kill) whatever is *currently* running
under A's name — even the fresh generation, which never asked to be
linked. Actors that care about this precisely should have *both* sides
re-`link()` after any restart, not just the side that actually restarted.
"""

from dataclasses import dataclass

from .actor import actor
from .context import current_actor_ref
from .refs import RemoteRef

LINK_ACTOR = "__lapinbeam_link__"


@dataclass
class Exit:
    """Delivered as an ordinary message — handle it with `@on(Exit)` — to
    an actor that called `trap_exit()`, instead of the default "kill me
    too" behavior, when a linked actor/peer exits."""

    actor: str
    reason: str


def _me():
    ref = current_actor_ref()
    if ref is None:
        raise RuntimeError(
            "link()/unlink()/trap_exit() must be called from inside a running actor"
        )
    return ref


def _target(other):
    """`(peer_id, name)` identifying `other` — `peer_id` is `None` for a
    local `ActorRef`."""
    if isinstance(other, RemoteRef):
        return (other.peer_id, other.actor_name)
    return (None, other.name)


async def link(other):
    """Links the currently-running actor to `other` (an `ActorRef` for a
    local actor, or a `RemoteRef` for one on another node).

    Linking to an actor that doesn't exist — locally, or on a remote peer
    that reports `actor_not_found` — delivers an immediate exit signal
    with reason `"noproc"`, the same signal a genuine death would produce.
    """
    me = _me()
    node = me._node
    node._links.setdefault(me.name, set()).add(_target(other))
    peer_id, name = _target(other)
    if peer_id is None:
        node._links.setdefault(name, set()).add((None, me.name))
        if name not in node._mailboxes:
            # `other` doesn't exist — deliver the exit signal to `me`
            # (the caller), not to the nonexistent target.
            await node._deliver_exit(name, me.name, "noproc")
    else:
        remote = node.get_remote_actor(peer_id, LINK_ACTOR)
        await remote.send({"op": "link", "from": me.name, "to": name, "from_node": node.local_id})


async def unlink(other):
    """Removes a link established with `link()`. Safe to call even if no
    such link exists."""
    me = _me()
    node = me._node
    node._links.get(me.name, set()).discard(_target(other))
    peer_id, name = _target(other)
    if peer_id is None:
        node._links.get(name, set()).discard((None, me.name))
    else:
        remote = node.get_remote_actor(peer_id, LINK_ACTOR)
        await remote.send({"op": "unlink", "from": me.name, "to": name, "from_node": node.local_id})


def trap_exit(enabled=True):
    """Makes the currently-running actor receive linked exits as an
    ordinary `Exit` message (handle with `@on(Exit)`) instead of being
    killed by them.

    Like links themselves, this does not survive the actor's own restart
    — call it again from `__init__` if it should stay in effect.
    """
    me = _me()
    me._node._trap_exit[me.name] = enabled


def register_links(node, sup):
    """Sets up this node to answer cross-node link requests and exit
    signals. Only needed if you plan to `link()` to (or be linked from) an
    actor on a *different* node — purely local links work without it.

    Call once per node, alongside `register_discovery` if you use both.
    """
    return sup.spawn(_LinkActor, node)


@actor(name=LINK_ACTOR)
class _LinkActor:
    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        op = msg["op"]
        from_node = msg.get("from_node")
        target = msg["to"]
        if op == "link":
            self.node._links.setdefault(target, set()).add((from_node, msg["from"]))
            if target not in self.node._mailboxes:
                remote = self.node.get_remote_actor(from_node, LINK_ACTOR)
                await remote.send({
                    "op": "exit", "to": msg["from"], "actor": target,
                    "reason": "noproc", "from_node": self.node.local_id,
                })
        elif op == "unlink":
            self.node._links.get(target, set()).discard((from_node, msg["from"]))
        elif op == "exit":
            from_label = f"{from_node}/{msg['actor']}"
            await self.node._deliver_exit(from_label, target, msg["reason"])
