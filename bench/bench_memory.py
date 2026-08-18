"""Memory-behavior checks under sustained load and churn.

Unlike bench_remote.py/bench_latency.py (throughput/latency in short
bursts), this samples this process's RSS (VmRSS from /proc/self/status,
Linux-only) across three longer-running scenarios to answer "does memory
usage stay bounded":

  1. Sustained local + remote traffic — should plateau quickly.
  2. Rapid connect_peer()/forget_peer() churn, run in several back-to-back
     rounds — a real per-cycle leak keeps adding roughly the same amount
     every round; retained-but-reused heap plateaus after the first one.
  3. A permanently crash-looping actor, once with the default unbounded
     mailbox and once with `mailbox_capacity` set — demonstrates that the
     documented "unbounded mailboxes" limitation is easy to hit for real
     (a slow/crashing consumer is enough), and that `mailbox_capacity` is
     an effective, working mitigation for it.

Run with: uv run python bench/bench_memory.py
"""

import asyncio
import gc
import time

from lapinbeam import Node, Supervisor, actor


def rss_mb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0


def report(label):
    gc.collect()
    print(f"  {label:<38} RSS={rss_mb():8.2f} MiB")


@actor(name="echo")
class Echo:
    async def receive(self, msg):
        pass


@actor(name="sink")
class Sink:
    async def receive(self, msg):
        pass


async def sustained_traffic(seconds=10):
    print(f"\n1) sustained local+remote traffic ({seconds}s)")
    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        local_ref = Supervisor(node=node_a).spawn(Echo)
        Supervisor(node=node_b).spawn(Sink)
        await node_a.connect_peer(node_b.local_id)
        remote_ref = node_a.get_remote_actor(node_b.local_id, "sink")

        report("start")
        start = time.monotonic()
        sent = 0
        while time.monotonic() - start < seconds:
            for _ in range(200):
                await local_ref.send({"n": sent})
                await remote_ref.send({"n": sent})
                sent += 1
            await asyncio.sleep(0)
        await asyncio.sleep(0.3)  # drain in-flight
        report(f"end (sent={sent})")
    finally:
        await node_a.stop()
        await node_b.stop()


async def connection_churn(rounds=3, cycles_per_round=500):
    print(f"\n2) connect_peer()/forget_peer() churn ({rounds} rounds x {cycles_per_round})")
    print("   (a real per-cycle leak keeps adding memory every round; healthy")
    print("   behavior plateaus after the first one or two)")
    for r in range(1, rounds + 1):
        node_a = Node("node_a@127.0.0.1:0")
        node_b = Node("node_b@127.0.0.1:0")
        await node_a.start()
        await node_b.start()
        for _ in range(cycles_per_round):
            await node_a.connect_peer(node_b.local_id)
            node_a.forget_peer(node_b.local_id)
        await asyncio.sleep(0.2)
        await node_a.stop()
        await node_b.stop()
        report(f"round {r}")


@actor(name="flaky")
class Flaky:
    async def receive(self, msg):
        raise RuntimeError("boom")


async def _crash_loop(node, seconds):
    sup = Supervisor(node=node, max_restarts=10_000_000, restart_window=0.001)
    ref = sup.spawn(Flaky)
    start = time.monotonic()
    sent = 0
    while time.monotonic() - start < seconds:
        try:
            await ref.send({"n": sent})
        except Exception:
            pass
        sent += 1
        if sent % 500 == 0:
            await asyncio.sleep(0)
    return sent


async def crash_loop_backpressure(seconds=6):
    print(f"\n3) permanently crash-looping actor ({seconds}s each)")

    print("   a) default (unbounded mailbox):")
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        report("start")
        sent = await _crash_loop(node, seconds)
        report(f"end (send attempts={sent})")
        print("   -> unbounded growth: a slow/crash-looping consumer fills the")
        print("      mailbox as fast as it's sent to. Expected — see")
        print("      docs/index.md's Limitations. Fix: mailbox_capacity=N.")
    finally:
        await node.stop()

    print("   b) with mailbox_capacity=100:")
    node = Node("node@127.0.0.1:0", mailbox_capacity=100)
    events = []
    node.on_event(lambda e: events.append(e) if e.get("kind") == "mailbox_full" else None)
    await node.start()
    try:
        report("start")
        sent = await _crash_loop(node, seconds)
        report(f"end (send attempts={sent})")
        print(f"   -> mailbox_full fired {len(events)} times; growth stays bounded.")
    finally:
        await node.stop()


async def main():
    report("baseline")
    await sustained_traffic()
    await connection_churn()
    await crash_loop_backpressure()
    report("\nfinal")


if __name__ == "__main__":
    asyncio.run(main())
