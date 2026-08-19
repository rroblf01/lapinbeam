"""The worker node: a fixed pool of `MAX_PARALLEL` lapinbeam actors
(`Supervisor.spawn_pool()`, see order.py) that actually run orders. Never
dials out — `app` connects to *this* node and reaches the pool directly
via `get_remote_actor(peer, "order_pool")`.
"""

import asyncio
import os

from lapinbeam import Node, Supervisor

import db
from order import process_order

NODE_NAME = os.environ.get("NODE_NAME", "worker@127.0.0.1:9001")
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "200"))


async def main():
    await db.connect()
    node = Node(NODE_NAME)
    await node.start()
    sup = Supervisor(node=node)
    pool = await sup.spawn_pool(process_order, MAX_PARALLEL, name="order_pool")

    print(f"[worker] listo en {node.local_id}, pool {pool.name!r} de {pool.size} workers", flush=True)
    await node.wait_until_stopped()


if __name__ == "__main__":
    asyncio.run(main())
