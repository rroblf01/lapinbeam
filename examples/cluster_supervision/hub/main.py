"""The hub node: connects out to every worker, spawns a `Watcher` actor
under a *nested* supervision tree (`spawn_supervisor`), links it to each
worker's `task_worker` (cross-node `lapinbeam.links`), and polls the
cluster-wide "workers" group (cross-node `lapinbeam.groups`) — all three
new primitives, exercised across real containers, with zero wire protocol
changes.
"""

import asyncio
import os

from lapinbeam import (
    Exit,
    Node,
    Supervisor,
    actor,
    members,
    on,
    register_groups,
    register_links,
    link,
    trap_exit,
)

NODE_NAME = os.environ["NODE_NAME"]
WORKERS = [w.strip() for w in os.environ.get("WORKERS", "").split(",") if w.strip()]


@actor(name="watcher")
class Watcher:
    def __init__(self, node_ref, worker_ids):
        self.node = node_ref
        self.worker_ids = worker_ids

    @on(Exit)
    async def on_exit(self, msg):
        print(f"[hub] EXIT recibido: {msg.actor!r} salió con motivo {msg.reason!r}", flush=True)

    @on(default=True)
    async def on_setup(self, msg):
        trap_exit(True)
        for peer_id in self.worker_ids:
            other = self.node.get_remote_actor(peer_id, "task_worker")
            await link(other)
            print(f"[hub] enlazado (link) a task_worker en {peer_id}", flush=True)


async def main():
    node = Node(NODE_NAME, connect_timeout=30.0)
    await node.start()
    sup = Supervisor(node=node)
    register_links(node, sup)
    register_groups(node, sup)

    for worker in WORKERS:
        for _ in range(30):
            try:
                await node.connect_peer(worker)
                break
            except ConnectionError:
                await asyncio.sleep(1.0)
        else:
            raise RuntimeError(f"no se pudo conectar con {worker!r}")
    print(f"[hub] conectado a {len(WORKERS)} workers: {WORKERS}", flush=True)

    # Nested supervision tree: this hub's top-level Supervisor supervises
    # a second Supervisor ("watch_tree"), which in turn supervises the
    # Watcher actor itself — a real Supervisor-supervises-Supervisor tree,
    # not just a flat pool of actors.
    watcher_ref_holder = {}

    def build_watch_tree(watch_sup):
        watcher_ref_holder["ref"] = watch_sup.spawn(Watcher, node, WORKERS)

    sup.spawn_supervisor("watch_tree", build_watch_tree)
    await asyncio.sleep(1.0)
    await watcher_ref_holder["ref"].send({"setup": True})

    while True:
        await asyncio.sleep(3)
        found = members(node, "workers")
        labels = sorted(
            f"{m.peer_id}/{m.actor_name}" if hasattr(m, "peer_id") else f"{node.local_id}/{m.name}"
            for m in found
        )
        print(f"[hub] miembros actuales de 'workers': {labels}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
