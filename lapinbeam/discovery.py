"""Lightweight seed-node discovery.

No external registry, no periodic background gossip — built entirely on
top of the rest of the public API (`Node`, `@actor`,
`ask()`/`current_message().reply()`, `on_event`). A node needs to know the
address of one (or a few) already-running "seed" nodes; everything else is
learned by asking whatever it connects to "who else do you know about?" and
connecting to the answer too, recursively, until a full pass turns up
nothing new. That turns "every node needs every other node's address" into
"every node needs one shared seed address" — see
`examples/seed_discovery/` for a runnable multi-node demo.

What this deliberately does NOT do (acceptable for something this small,
not for a real membership protocol):

- No re-discovery after the initial join. Two nodes that join at the exact
  same moment, before either has told a seed about itself, can both end up
  connected to the seed without being connected to each other. Call
  `join_via_seeds` again later (it's idempotent) to pick up stragglers —
  `examples/seed_discovery/node_app.py` does exactly that, a second time
  a couple of seconds after the first.
- No failure detection beyond what `Node`'s own `peer_timeout` already
  gives you, and no removal of a node from anyone's "known peers" beyond
  what a real `peer_disconnected` event reports.
"""

from .actor import actor
from .context import current_message

DISCOVERY_ACTOR = "discovery"


def register_discovery(node, sup):
    """Sets up this node to answer "who do you know about?" queries, and
    keeps the answer current by watching this node's own connect/disconnect
    events. Call once per node, before `join_via_seeds`.

    The known-peers set lives in this closure, outside the actor, on
    purpose: `on_event` has no way to unregister a listener, so if this
    were registered inside the actor's own `__init__` instead, a crash and
    restart of that actor would register a second, permanent listener
    bound to the discarded instance every time.
    """
    known = {node.local_id}

    def _on_event(event):
        if event["kind"] == "peer_connected":
            known.add(event["peer"])
        elif event["kind"] == "peer_disconnected":
            known.discard(event["peer"])

    node.on_event(_on_event)
    return sup.spawn(_DiscoveryActor, known)


@actor(name=DISCOVERY_ACTOR)
class _DiscoveryActor:
    def __init__(self, known):
        self.known = known

    async def receive(self, msg):
        if msg.get("type") == "WHO_DO_YOU_KNOW":
            await current_message().reply({"peers": sorted(self.known)})


async def join_via_seeds(node, seeds):
    """Connects to every seed, then transitively connects to (and asks)
    everything each newly-reached node reports knowing about, until a full
    pass finds nothing new. Returns the set of peers this call connected to
    or confirmed (not counting `node` itself).

    Idempotent and safe to call again later: peers already connected are
    skipped (`Node.connect_peer` is itself a no-op if already connected),
    so calling this a second time after a pause only picks up whatever is
    genuinely new since the first call.
    """
    # `visited` (peers already attempted, so the BFS terminates instead of
    # re-processing the same id forever) is deliberately a separate set
    # from `connected` (the actual return value): a peer that turned out
    # unreachable still needs to count as "visited" so it isn't retried in
    # every round, but must NOT be reported back as joined.
    connected = set()
    visited = {node.local_id}
    frontier = [s for s in seeds if s not in visited]

    while frontier:
        next_frontier = []
        for peer in frontier:
            if peer in visited:
                continue
            visited.add(peer)
            try:
                await node.connect_peer(peer)
                reply = await node.get_remote_actor(peer, DISCOVERY_ACTOR).ask(
                    {"type": "WHO_DO_YOU_KNOW"}, timeout=5.0
                )
            except (ConnectionError, TimeoutError):
                continue  # unreachable right now; the rest of the frontier still matters
            connected.add(peer)
            for candidate in reply["peers"]:
                if candidate not in visited:
                    next_frontier.append(candidate)
        frontier = next_frontier

    return connected
