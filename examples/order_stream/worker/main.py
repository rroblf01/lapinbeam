"""The worker node: a fixed pool of `MAX_PARALLEL` lapinbeam actors that
actually run investigations (see investigation.py). Never dials out —
`app` connects to *this* node, and pushes are sent back over that same
connection using the `reply_node` id carried in each dispatch message, so
this node needs no address for `app` configured anywhere.
"""

import asyncio
import os

from lapinbeam import Node, Supervisor

import db
from investigation import start_pool

NODE_NAME = os.environ.get("NODE_NAME", "worker@127.0.0.1:9001")
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "200"))


async def main():
    await db.connect()
    node = Node(NODE_NAME)
    await node.start()
    sup = Supervisor(node=node)
    await start_pool(node, sup, MAX_PARALLEL)

    print(f"[worker] listo en {node.local_id}, pool de {MAX_PARALLEL} workers", flush=True)
    await node.wait_until_stopped()


if __name__ == "__main__":
    asyncio.run(main())
