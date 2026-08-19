"""Cluster-wide unique name registration — Erlang's `:global` module:
`register_name(node, name)` claims `name` as the *one* owner across every
connected node, `whereis_name(node, name)` looks it up from anywhere, and
`unregister_name(node, name)` releases it. Unlike `lapinbeam.groups` (many
members per group), a name here has exactly one owner at a time —
`register_name()` raises `ValueError` if it's already claimed by a
*different* actor, the same "no silent collision" contract
`Supervisor.spawn()`/`Node.register_actor()` already enforce for local
actor names, just extended cluster-wide.

Membership is pid-scoped, same rationale as `lapinbeam.links`/`groups`: a
restarted actor releases every name it owned and must explicitly
re-`register_name()` (typically from its first message handler) to keep
it across its own restarts.

No wire protocol changes: this rides as ordinary `Data` frames addressed
to a reserved, well-known local actor name (`__lapinbeam_registry__`,
registered per `Node` via `register_registry()` — the same trick
`lapinbeam.discovery`/`links`/`groups`/`monitors` use for their own
well-known actors).

What this deliberately does NOT do: resolve a genuine split claim with
real distributed consensus. Two nodes that each believe `name` is free
and `register_name()` it within the same convergence race will each
succeed *locally* and broadcast a claim; whichever claim a given node's
`__lapinbeam_registry__` sees *first* wins there, and the other is
rejected with an `on_event(kind="registry_conflict")` instead of being
silently overwritten — but different nodes can end up seeing a different
order and so, briefly, disagree about who owns `name` (the same one-shot,
no-continuous-gossip trade-off `lapinbeam.discovery`'s `join_via_seeds`
and `lapinbeam.groups` already document and accept). For a cluster where
two actors genuinely might race to claim the same name, don't treat a
successful `register_name()` as a global guarantee until you've also
seen `whereis_name()` agree from more than one node.
"""

import asyncio

from .actor import actor
from .context import current_actor_ref
from .refs import ActorRef, RemoteRef

REGISTRY_ACTOR = "__lapinbeam_registry__"


def _me(ref):
    if ref is not None:
        return ref
    me = current_actor_ref()
    if me is None:
        raise RuntimeError(
            "register_name()/unregister_name() need an explicit ref= outside of a running actor"
        )
    return me


async def register_name(node, name, ref=None):
    """Claims `name` as the one cluster-wide owner, for `ref` (default:
    the currently-running actor). Raises `ValueError` if `name` is already
    claimed by a *different* actor — locally-known claims only; see the
    module docstring for what that does and doesn't guarantee against a
    genuine race with a claim made concurrently on another node."""
    me = _me(ref)
    owner = (node.local_id, me.name)
    existing = node._registry.get(name)
    if existing is not None and existing != owner:
        raise ValueError(f"name {name!r} is already registered to {existing!r}")
    node._registry[name] = owner
    _broadcast(node, name, owner, "claim")


async def unregister_name(node, name, ref=None):
    """Releases a name claimed with `register_name()`. Safe to call even
    if `ref` (default: the currently-running actor) doesn't currently own
    it — a no-op in that case, same as `leave_group()`."""
    me = _me(ref)
    owner = (node.local_id, me.name)
    if node._registry.get(name) == owner:
        del node._registry[name]
        _broadcast(node, name, owner, "release")


def whereis_name(node, name):
    """The current owner of `name`, as an `ActorRef` (local) or `RemoteRef`
    (remote) — or `None` if nobody currently owns it."""
    entry = node._registry.get(name)
    if entry is None:
        return None
    origin, actor_name = entry
    if origin == node.local_id:
        return ActorRef(node, actor_name)
    return RemoteRef(node, origin, actor_name)


def _broadcast(node, name, owner, verb):
    peers = getattr(node, "_registry_peers", None)
    if not peers:
        return  # register_registry() never called, or no peers yet — local-only
    for peer_id in list(peers):
        asyncio.create_task(_send_delta(node, peer_id, name, owner, verb))


async def _send_delta(node, peer_id, name, owner, verb):
    remote = node.get_remote_actor(peer_id, REGISTRY_ACTOR)
    try:
        await remote.send({"op": "delta", "name": name, "owner": list(owner), "verb": verb})
    except Exception:
        pass  # best-effort, same as any other fire-and-forget notification


async def _send_snapshot(node, peer_id):
    remote = node.get_remote_actor(peer_id, REGISTRY_ACTOR)
    snapshot = {name: list(owner) for name, owner in node._registry.items()}
    if not snapshot:
        return
    try:
        await remote.send({"op": "snapshot", "registry": snapshot})
    except Exception:
        pass


def _drop_peer_names(node, peer_id):
    stale = [name for name, (origin, _n) in node._registry.items() if origin == peer_id]
    for name in stale:
        del node._registry[name]


def register_registry(node, sup):
    """Sets up this node to answer cluster-wide name registration
    queries and keep them current. Only needed for names that might be
    claimed from a *different* node — purely local registration works
    without it.

    Call once per node, alongside `register_discovery`/`register_links`/
    `register_groups`/`register_monitors` if you use those too.
    """
    node._registry_peers = set()

    def _on_peer_event(event):
        kind = event["kind"]
        if kind == "peer_connected":
            node._registry_peers.add(event["peer"])
            asyncio.create_task(_send_snapshot(node, event["peer"]))
        elif kind == "peer_disconnected":
            node._registry_peers.discard(event["peer"])
            _drop_peer_names(node, event["peer"])

    node.on_event(_on_peer_event)
    node._registry_release_hook = lambda registry_name, actor_name: _broadcast(
        node, registry_name, (node.local_id, actor_name), "release"
    )
    return sup.spawn(_RegistryActor, node)


@actor(name=REGISTRY_ACTOR)
class _RegistryActor:
    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        op = msg["op"]
        if op == "delta":
            name = msg["name"]
            owner = tuple(msg["owner"])
            if msg["verb"] == "claim":
                existing = self.node._registry.get(name)
                if existing is not None and existing != owner:
                    self.node._on_core_event({
                        "kind": "registry_conflict", "name": name,
                        "existing": existing, "incoming": owner,
                    })
                    return
                self.node._registry[name] = owner
            else:
                if self.node._registry.get(name) == owner:
                    del self.node._registry[name]
        elif op == "snapshot":
            for name, owner in msg["registry"].items():
                owner = tuple(owner)
                existing = self.node._registry.get(name)
                if existing is not None and existing != owner:
                    self.node._on_core_event({
                        "kind": "registry_conflict", "name": name,
                        "existing": existing, "incoming": owner,
                    })
                    continue
                self.node._registry[name] = owner
