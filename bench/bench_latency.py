"""Round-trip latency benchmarks (p50/p95/p99).

Run with: uv run python bench/bench_latency.py
"""

import asyncio
import time

from lapinbeam import Node, Supervisor, actor

PENDING = {}


@actor(name="client")
class Client:
    async def receive(self, msg):
        ev = PENDING.pop(msg["id"], None)
        if ev is not None:
            ev.set()


@actor(name="echo")
class Echo:
    def __init__(self, node_ref, peer_id):
        self.node = node_ref
        self.peer_id = peer_id

    async def receive(self, msg):
        remote = self.node.get_remote_actor(self.peer_id, msg["reply_to"])
        await remote.send(msg)


def percentile(sorted_values, p):
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int(len(sorted_values) * p))
    return sorted_values[index]


async def measure(remote_ref, local_client, n):
    rtts = []
    for i in range(n):
        ev = asyncio.Event()
        PENDING[i] = ev
        t0 = time.perf_counter()
        await remote_ref.send({"id": i, "reply_to": "client"})
        await ev.wait()
        rtts.append((time.perf_counter() - t0) * 1000)
    rtts.sort()
    print(
        f"RTT p50={percentile(rtts, 0.5):.3f}ms "
        f"p95={percentile(rtts, 0.95):.3f}ms "
        f"p99={percentile(rtts, 0.99):.3f}ms"
    )
    return rtts


async def main():
    n = 2000

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a)
        sup_b = Supervisor(node=node_b)
        client_ref = sup_a.spawn(Client)
        sup_b.spawn(Echo, node_b, node_a.local_id)

        await node_a.connect_peer(node_b.local_id)
        echo = node_a.get_remote_actor(node_b.local_id, "echo")

        # Warmup
        for i in range(100):
            ev = asyncio.Event()
            PENDING[i] = ev
            await echo.send({"id": i, "reply_to": "client"})
            await ev.wait()

        print("local dispatch (send->receive):")
        await measure(client_ref, None, n)
        print("remote loopback TCP (send+ack):")
        await measure(echo, None, n)
    finally:
        await node_a.stop()
        await node_b.stop()


if __name__ == "__main__":
    asyncio.run(main())
