"""One script, reused by every node in docker-compose.yml — a node is
either a seed (no SEEDS env var) or a joiner (SEEDS points at one or more
already-running nodes) purely based on environment, not different code.
"""

import asyncio
import os

from lapinbeam import Node, Supervisor, join_via_seeds, register_discovery


async def main():
    node_name = os.environ["NODE_NAME"]
    seeds = [s.strip() for s in os.environ.get("SEEDS", "").split(",") if s.strip()]

    node = Node(node_name, connect_timeout=30.0)
    await node.start()
    sup = Supervisor(node=node)
    register_discovery(node, sup)

    if seeds:
        print(f"[{node.local_id}] uniéndome vía semillas: {seeds}", flush=True)
        found = await join_via_seeds(node, seeds)
        print(f"[{node.local_id}] ronda 1: descubiertos {len(found)} peers: {sorted(found)}", flush=True)

        # A second pass a moment later catches stragglers that joined the
        # same seed(s) in the brief race right after startup — see
        # lapinbeam.discovery's module docstring for exactly what this
        # does and doesn't guarantee.
        await asyncio.sleep(2.0)
        found |= await join_via_seeds(node, seeds + sorted(found))
        print(f"[{node.local_id}] ronda 2: total {len(found)} peers: {sorted(found)}", flush=True)
    else:
        print(f"[{node.local_id}] arrancando como semilla, sin seeds configuradas", flush=True)

    while True:
        await asyncio.sleep(5)
        print(f"[{node.local_id}] peers conectados ahora mismo: {node.peer_count()}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
