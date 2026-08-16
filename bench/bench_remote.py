"""Message-passing benchmarks for lapinbeam.

Compares:
  1. Pure asyncio.Queue put/get (baseline).
  2. lapinbeam local actor send (Python dispatch).
  3. lapinbeam remote send over loopback TCP (Rust transport).

Run with: uv run python bench/bench_remote.py
"""

import asyncio
import time

from lapinbeam import Node, Supervisor, actor


async def bench_asyncio_queue(n):
    queue = asyncio.Queue()
    done = asyncio.Event()

    async def consumer():
        while True:
            item = await queue.get()
            if item is None:
                done.set()
                return

    task = asyncio.create_task(consumer())
    start = time.perf_counter()
    for i in range(n):
        await queue.put(i)
    await queue.put(None)
    await done.wait()
    elapsed = time.perf_counter() - start
    task.cancel()
    return n / elapsed


@actor(name="echo")
class Echo:
    async def receive(self, msg):
        pass


async def bench_local_send(node, ref, n):
    for _ in range(2000):
        await ref.send({"n": 1})
    start = time.perf_counter()
    for _ in range(n):
        await ref.send({"n": 1})
    return n / (time.perf_counter() - start)


@actor(name="sink")
class Sink:
    def __init__(self):
        self.count = 0
        self.done = asyncio.Event()

    async def receive(self, msg):
        self.count += 1
        if self.count == 1:
            self.done.set()


async def bench_remote_send(node, remote, n):
    # First message primes the connection; measure delivery of the rest.
    for _ in range(100):
        await remote.send({"n": 1})
    await asyncio.sleep(0.2)
    start = time.perf_counter()
    for _ in range(n):
        await remote.send({"n": 1})
    elapsed = time.perf_counter() - start
    return n / elapsed


async def main():
    n_local = 50000
    n_remote = 2000

    asyncio_rate = await bench_asyncio_queue(n_local)
    print(f"asyncio.Queue put/get      : {asyncio_rate:>10.0f} msg/s")

    node = Node("node@127.0.0.1:0")
    await node.start()
    sup = Supervisor(node=node)
    ref = sup.spawn(Echo)
    local_rate = await bench_local_send(node, ref, n_local)
    print(f"lapinbeam local send       : {local_rate:>10.0f} msg/s")

    node_b = Node("node_b@127.0.0.1:0")
    await node_b.start()
    sup_b = Supervisor(node=node_b)
    sup_b.spawn(Sink)
    await node.connect_peer(node_b.local_id)
    remote = node.get_remote_actor(node_b.local_id, "sink")
    remote_rate = await bench_remote_send(node, remote, n_remote)
    print(f"lapinbeam remote (loopback): {remote_rate:>10.0f} msg/s")

    await node.stop()
    await node_b.stop()


if __name__ == "__main__":
    asyncio.run(main())
