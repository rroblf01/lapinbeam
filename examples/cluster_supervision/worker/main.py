"""A worker node: starts, joins the cluster-wide "workers" group, and
waits for the hub to dial in and link to its `task_worker` actor. Once
linked, it processes a few tasks and then deliberately fails for good —
demonstrating a cross-node `lapinbeam.links` exit signal delivered to the
hub, and cross-node `lapinbeam.groups` membership dropping, across a real
TCP connection between two containers, with zero wire protocol changes.

`max_restarts=0` on purpose: the very first crash is the final one, so
there's no intermediate in-place restart to clear the link the hub
registered (see `lapinbeam/links.py`'s module docstring on the pid-scoped,
restart-clears-links behavior — this keeps the demo to the common,
unambiguous case; `tests-python/test_links.py` covers the restart-doesn't-
propagate case directly).
"""

import asyncio
import os

from lapinbeam import Node, Supervisor, actor, join_group, register_groups, register_links

NODE_NAME = os.environ["NODE_NAME"]
CRASH_AFTER = int(os.environ.get("CRASH_AFTER", "3"))


@actor(name="task_worker")
class TaskWorker:
    def __init__(self, node_ref):
        self.node = node_ref
        self.count = 0

    async def receive(self, msg):
        if self.count == 0:
            await join_group(self.node, "workers")
            print(f"[{self.node.local_id}] unido al grupo 'workers'", flush=True)
        self.count += 1
        print(f"[{self.node.local_id}] procesando tarea #{self.count}", flush=True)
        if self.count > CRASH_AFTER:
            raise RuntimeError(f"fallo permanente tras {self.count} tareas")


async def main():
    node = Node(NODE_NAME, connect_timeout=30.0)
    await node.start()
    sup = Supervisor(node=node, max_restarts=0)
    register_links(node, sup)
    register_groups(node, sup)

    ref = sup.spawn(TaskWorker, node)
    print(f"[{node.local_id}] task_worker listo, esperando a que el hub se conecte", flush=True)

    # Give the hub plenty of time to come up, connect_peer() to us, and
    # link() to our task_worker before we start working (and eventually
    # failing for good) — see the module docstring for why the link has
    # to be in place before the one and only crash.
    await asyncio.sleep(8.0)
    for _ in range(10):
        try:
            await ref.send({})
        except ValueError:
            # task_worker gave up for good (max_restarts=0) — its mailbox
            # is gone. Nothing left to do here; the exit signal has
            # already reached the hub via the link.
            break
        await asyncio.sleep(1.0)

    print(f"[{node.local_id}] task_worker terminado, nodo sigue en pie", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
