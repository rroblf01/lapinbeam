"""Cluster-wide named process groups — a group name resolves to member
actors across every connected node, not just the local one.

Membership is pid-scoped, not name-scoped (same rationale as
`lapinbeam.links`): a restarted actor is dropped from every group it was
in and must explicitly rejoin (typically from `__init__`) if it should
stay a member across its own restarts.

No wire protocol changes: this rides as ordinary `Data` frames addressed
to a reserved, well-known local actor name (`__lapinbeam_groups__`,
registered per `Node` via `register_groups()` — the same trick
`lapinbeam.discovery` and `lapinbeam.links` use for their own well-known
actors).

What this deliberately does NOT do: continuous anti-entropy gossip beyond
a snapshot exchanged once per newly-connected peer (the same one-shot
convergence trade-off `lapinbeam.discovery`'s `join_via_seeds` already
documents and accepts) — two peers that join/leave a group in a very
tight race, right as a third node connects, could see it converge a beat
late rather than instantly.
"""

import asyncio

from .actor import actor
from .context import current_actor_ref
from .refs import ActorRef, RemoteRef

GROUPS_ACTOR = "__lapinbeam_groups__"


def _me(ref):
    if ref is not None:
        return ref
    me = current_actor_ref()
    if me is None:
        raise RuntimeError(
            "join_group()/leave_group() need an explicit ref= outside of a running actor"
        )
    return me


async def join_group(node, group, ref=None):
    """Adds `ref` (default: the currently-running actor) to `group`.

    Visible to every connected node via `members()`, including ones that
    join the cluster later (through a one-time snapshot exchange on
    connect — see the module docstring for what that does and doesn't
    guarantee).
    """
    me = _me(ref)
    member = (node.local_id, me.name)
    node._groups.setdefault(group, set()).add(member)
    _broadcast(node, group, member, "join")


async def leave_group(node, group, ref=None):
    """Removes `ref` (default: the currently-running actor) from `group`.
    Safe to call even if it isn't a member."""
    me = _me(ref)
    member = (node.local_id, me.name)
    node._groups.get(group, set()).discard(member)
    _broadcast(node, group, member, "leave")


def members(node, group):
    """A snapshot of `group`'s current members, as `ActorRef`s (local) or
    `RemoteRef`s (remote)."""
    result = []
    for origin, name in node._groups.get(group, set()):
        if origin == node.local_id:
            result.append(ActorRef(node, name))
        else:
            result.append(RemoteRef(node, origin, name))
    return result


def _broadcast(node, group, member, verb):
    peers = getattr(node, "_group_peers", None)
    if not peers:
        return  # register_groups() never called, or no peers yet — local-only
    for peer_id in list(peers):
        asyncio.create_task(_send_delta(node, peer_id, group, member, verb))


async def _send_delta(node, peer_id, group, member, verb):
    remote = node.get_remote_actor(peer_id, GROUPS_ACTOR)
    try:
        await remote.send({"op": "delta", "group": group, "member": list(member), "verb": verb})
    except Exception:
        pass  # best-effort, same as any other fire-and-forget notification


async def _send_snapshot(node, peer_id):
    remote = node.get_remote_actor(peer_id, GROUPS_ACTOR)
    snapshot = {
        group: [list(m) for m in members]
        for group, members in node._groups.items()
        if members
    }
    if not snapshot:
        return
    try:
        await remote.send({"op": "snapshot", "groups": snapshot})
    except Exception:
        pass


def _drop_peer_members(node, peer_id):
    for members in node._groups.values():
        stale = {m for m in members if m[0] == peer_id}
        members.difference_update(stale)


def register_groups(node, sup):
    """Sets up this node to answer cluster-wide group membership queries
    and keep them current. Only needed for groups whose members might live
    on a *different* node — purely local groups work without it.

    Call once per node, alongside `register_discovery`/`register_links` if
    you use those too.
    """
    node._group_peers = set()

    def _on_peer_event(event):
        kind = event["kind"]
        if kind == "peer_connected":
            node._group_peers.add(event["peer"])
            asyncio.create_task(_send_snapshot(node, event["peer"]))
        elif kind == "peer_disconnected":
            node._group_peers.discard(event["peer"])
            _drop_peer_members(node, event["peer"])

    node.on_event(_on_peer_event)
    node._group_leave_hook = lambda group, name: _broadcast(
        node, group, (node.local_id, name), "leave"
    )
    return sup.spawn(_GroupsActor, node)


@actor(name=GROUPS_ACTOR)
class _GroupsActor:
    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        op = msg["op"]
        if op == "delta":
            member = tuple(msg["member"])
            group = msg["group"]
            if msg["verb"] == "join":
                self.node._groups.setdefault(group, set()).add(member)
            else:
                self.node._groups.get(group, set()).discard(member)
        elif op == "snapshot":
            for group, members in msg["groups"].items():
                bucket = self.node._groups.setdefault(group, set())
                for m in members:
                    bucket.add(tuple(m))
