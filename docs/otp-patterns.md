# OTP-inspired patterns

Beyond the core `Node`/`@actor`/`Supervisor` trio, lapinbeam borrows five
more patterns from Erlang/OTP: nested supervision trees, bidirectional
links, one-way monitors, cluster-wide process groups, and cluster-wide
name registration. All five are pure-Python additions on top of the same
public API — **zero Rust or wire-protocol changes** for any of them.
Cross-node traffic for links, monitors, groups, and the registry rides as
ordinary `Data` frames addressed to a reserved, well-known local actor
name (the same trick `lapinbeam.discovery` already uses for its own
seed-discovery actor) — a peer that hasn't opted in to a given feature
just answers with today's ordinary `actor_not_found` instead of breaking
the connection.

## Nested supervision trees and restart strategies

`Supervisor(strategy=...)` accepts three strategies, applied whenever any
child (an actor *or* a nested Supervisor) crashes:

- **`one_for_one`** (the default): restart only the crashed child. If it
  exhausts its restart budget, only *that* child gives up — unrelated
  siblings are never affected. This is what makes it safe to host many
  independent, unrelated actors under one `Supervisor` over its life (a
  worker-pool pattern in the loose sense of "many actors, one
  Supervisor" — for a fixed pool of *identical* workers sharing one work
  queue, see `Supervisor.spawn_pool()` in
  [Getting started](getting-started.md#concurrency-one-actor-handles-one-message-at-a-time)
  instead).
- **`one_for_all`**: a crash restarts *every* child this `Supervisor`
  manages, not just the one that failed.
- **`rest_for_one`**: a crash restarts the crashed child and every child
  spawned *after* it (spawn order matters).

For `one_for_all`/`rest_for_one`, exhausting the restart budget tears
down the whole subtree and this `Supervisor` itself is considered to have
given up.

`Supervisor.spawn_supervisor(name, build, *, strategy=, max_restarts=,
restart_window=)` spawns a **nested** `Supervisor` as a child of another
one — a real supervision tree, not just a flat pool of actors. `build`
receives the fresh nested `Supervisor` and populates it (spawn actors,
or nest even further); it runs again every time this subtree restarts,
since a used-up `Supervisor` can't be restarted in place the way an
actor's mailbox is reused.

```python
from lapinbeam import ActorRef, Node, Supervisor, SupervisorRef, actor


@actor(name="worker_a")
class WorkerA:
    async def receive(self, msg):
        if msg.get("crash"):
            raise RuntimeError("boom")


@actor(name="worker_b")
class WorkerB:
    async def receive(self, msg):
        pass


def build_pool(pool_sup):
    pool_sup.spawn(WorkerA)
    pool_sup.spawn(WorkerB)


async def main():
    async with Node("app@127.0.0.1:0") as node:
        sup = Supervisor(node=node)
        pool: SupervisorRef = sup.spawn_supervisor("pool", build_pool, strategy="one_for_all")
        await ActorRef(node, "worker_a").send({"crash": True})
        # one_for_all: worker_b also gets a fresh instance, even though
        # only worker_a raised.
```

`spawn_supervisor()` returns a `SupervisorRef` — `await ref.task` blocks
until that whole subtree gives up, re-raising the *original* exception
(not a generic wrapper), recursively through nested levels:

```python
try:
    await pool.task
except RuntimeError as exc:
    print("the whole pool gave up:", exc)
```

## Bidirectional links (`lapinbeam.links`)

`link(other)` ties the currently-running actor to `other` (an `ActorRef`
or a `RemoteRef`): if either side exits *for good* — its `Supervisor`
gives up, it returns cleanly, or it's explicitly shut down, **not** on an
ordinary in-place restart within budget — the other is killed too,
through its own `Supervisor`'s normal crash/restart path. An actor that
calls `trap_exit()` instead receives the signal as an ordinary `Exit`
message:

```python
from lapinbeam import Exit, Node, Supervisor, actor, on, link, trap_exit, register_links


@actor(name="watcher")
class Watcher:
    def __init__(self, node_ref):
        self.node = node_ref

    @on(Exit)
    async def on_exit(self, msg: Exit):
        print("linked actor exited:", msg.actor, msg.reason)

    @on(default=True)
    async def on_setup(self, msg):
        trap_exit(True)
        other = self.node.get_remote_actor("worker@worker:9101", "task_worker")
        await link(other)


async def main():
    node = Node("app@app:9100")
    await node.start()
    register_links(node, Supervisor(node=node))  # only needed for cross-node links
```

`unlink(other)` removes a link. Links are pid-scoped: they do not survive
their own in-place restart, so a restarted actor that still wants to be
linked must call `link()` again — typically from its first message
handler, since `__init__` can't `await`.

## One-way, non-lethal monitors (`lapinbeam.monitors`)

`monitor(other)` is the non-lethal counterpart to `link()`: the watcher
gets a `Down` message when the monitored actor/peer exits for good, but
**nothing happens to either side otherwise** — no kill in either
direction, no `trap_exit()` needed. It's the tool for "tell me when X is
gone" without the risk (or the obligation) `link()` carries:

```python
from lapinbeam import Down, actor, on, monitor, register_monitors


@actor(name="watcher")
class Watcher:
    def __init__(self, node_ref):
        self.node = node_ref

    @on(Down)
    async def on_down(self, msg: Down):
        print("monitored actor exited:", msg.actor, msg.reason)

    @on(default=True)
    async def on_setup(self, msg):
        other = self.node.get_remote_actor("worker@worker:9101", "task_worker")
        ref: str = await monitor(other)  # keep the ref if you'll demonitor() later
```

`demonitor(ref)` stops a monitor. Same pid-scoping as links, and the same
`register_monitors(node, sup)` setup call for the cross-node case.

## Cluster-wide process groups (`lapinbeam.groups`)

`join_group(node, group)` adds the currently-running actor to a named
group, visible from every connected node — not just the local one — via
`members(node, group)`, which returns a mix of `ActorRef` (local members)
and `RemoteRef` (remote ones):

```python
from lapinbeam import ActorRef, RemoteRef, actor, join_group, members, register_groups


@actor(name="worker")
class Worker:
    def __init__(self, node_ref):
        self.node = node_ref

    async def receive(self, msg):
        await join_group(self.node, "workers")


# From anywhere with a Node reference:
found: list[ActorRef | RemoteRef] = members(node, "workers")
```

`leave_group(node, group)` removes a member. Membership is pid-scoped: a
restarted actor is dropped from every group it was in and must explicitly
rejoin — same reasoning as links, and the same `register_groups(node,
sup)` setup call for cross-node visibility. Convergence for a
newly-connected peer is a one-time snapshot exchange, not continuous
gossip: two peers that join/leave in a very tight race, right as a third
node connects, could see it converge a beat late rather than instantly.

## Cluster-wide name registration (`lapinbeam.registry`)

`register_name(node, name)` claims `name` as the **one** owner across the
whole connected cluster — Erlang's `:global`. Unlike a group (many
members), a name has exactly one owner: `register_name()` raises
`ValueError` if it's already claimed by a different actor, the same
"no silent collision" contract `Supervisor.spawn()` already enforces for
local actor names:

```python
from lapinbeam import ActorRef, RemoteRef, actor, register_name, whereis_name, register_registry


@actor(name="worker")
class Worker:
    def __init__(self, node_ref):
        self.node = node_ref

    async def receive(self, msg):
        await register_name(self.node, "leader")


owner: ActorRef | RemoteRef | None = whereis_name(node, "leader")
```

`unregister_name(node, name)` releases a name; pid-scoped like group
membership. Convergence is delta-plus-snapshot, same trade-off as
groups — with one added wrinkle: two nodes that each race to claim the
same name *before* learning about each other will each succeed locally,
and the disagreement surfaces later as `on_event(kind="registry_conflict")`
rather than being silently resolved. This is deliberately **not** real
distributed consensus — see `lapinbeam/registry.py`'s module docstring for
the exact guarantees.

## See all five running across real containers

`examples/cluster_supervision/` runs a nested supervision tree, cross-node
links **and** monitors (the same crash delivers both an `Exit` and a
`Down`), a cluster-wide group, and a registered name across **three real
containers** — proof this all works over genuine TCP between separate
processes, not just localhost pytest:

```bash
cd examples/cluster_supervision
docker compose up --build
```
