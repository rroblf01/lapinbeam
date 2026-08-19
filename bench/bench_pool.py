"""CPU/RAM behavior checks for Supervisor.spawn_pool() and its options
(queue_capacity, key, executor, map(), stop()).

Unlike bench_memory.py (general Node/actor memory behavior), this is
scoped to the pool primitive specifically, answering three questions:

  1. Does executor="process" actually buy real CPU parallelism for
     CPU-bound work, compared to a plain async pool or executor="thread"
     (both still serialized by the GIL)? Measured as wall-clock speedup,
     not just "it doesn't error" — see docs/getting-started.md's
     "This parallelism is for I/O-bound work, not CPU-bound work" warning.
  2. Does queue_capacity actually keep memory bounded under sustained
     overload, instead of growing without limit like the default
     unbounded queue?
  3. Does pool.stop() actually release everything — mailboxes, tasks,
     Supervisor bookkeeping, and (for executor= pools) OS threads/
     processes — so a server that creates/destroys many pools over its
     lifetime doesn't leak? Measured across repeated create/stop cycles,
     the same "does it plateau" check bench_memory.py's connection_churn
     scenario uses.

Also samples this process's CPU% at fine granularity (~0.15s, reading
/proc/self/stat directly rather than a coarser external tool) during a
sharded (key=) pool run, to check for the kind of short-lived spike that
only shows up at fine granularity — see the CPU investigation in
examples/sales_warehouse/README.md for why coarse sampling isn't enough.

Run with: uv run python bench/bench_pool.py
"""

import asyncio
import gc
import os
import time

from lapinbeam import Node, Supervisor, current_message

_CLK_TCK = os.sysconf("SC_CLK_TCK")


def rss_mb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0


def cpu_seconds():
    with open("/proc/self/stat") as f:
        fields = f.read().split()
    utime, stime = int(fields[13]), int(fields[14])
    return (utime + stime) / _CLK_TCK


def report(label):
    gc.collect()
    print(f"  {label:<42} RSS={rss_mb():8.2f} MiB")


def report_growth(label, start_rss, end_rss):
    print(f"  {label:<42} RSS growth: {end_rss - start_rss:+8.2f} MiB "
          f"({start_rss:.2f} -> {end_rss:.2f})")


def _cpu_square(msg):
    total = 0
    for i in range(msg["n"]):
        total += i * i
    return total


async def _run_cpu_pool(executor, n_workers, n_items, work_per_item):
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        if executor is None:
            # Plain async pool calling the same sync function directly —
            # the "I mistakenly used spawn_pool without executor=" case.
            async def handler(msg):
                result = _cpu_square(msg)
                await current_message().reply(result)

            pool = await sup.spawn_pool(handler, n_workers, name="cpu_pool")
        else:
            pool = await sup.spawn_pool(
                _cpu_square, n_workers, name="cpu_pool", executor=executor
            )
        start = time.perf_counter()
        results = await pool.map([{"n": work_per_item}] * n_items)
        elapsed = time.perf_counter() - start
        assert len(results) == n_items
        await pool.stop()
        return elapsed
    finally:
        await node.stop()


async def cpu_bound_speedup(n_workers=4, n_items=24, work_per_item=3_000_000):
    print(f"\n1) CPU-bound work: async pool vs executor= (n_workers={n_workers}, "
          f"n_items={n_items}, work_per_item={work_per_item})")
    baseline = await _run_cpu_pool(None, 1, n_items, work_per_item)
    print(f"   1 worker,  no executor (serial baseline): {baseline:6.2f}s")
    async_pool = await _run_cpu_pool(None, n_workers, n_items, work_per_item)
    print(f"   {n_workers} workers, no executor (GIL-bound):        {async_pool:6.2f}s "
          f"(speedup {baseline / async_pool:.2f}x)")
    thread_pool = await _run_cpu_pool("thread", n_workers, n_items, work_per_item)
    print(f"   {n_workers} workers, executor='thread' (GIL-bound):  {thread_pool:6.2f}s "
          f"(speedup {baseline / thread_pool:.2f}x)")
    process_pool = await _run_cpu_pool("process", n_workers, n_items, work_per_item)
    print(f"   {n_workers} workers, executor='process' (real cores): {process_pool:6.2f}s "
          f"(speedup {baseline / process_pool:.2f}x)")
    print("   -> only executor='process' should show speedup anywhere near "
          f"{n_workers}x; the other two stay near 1x regardless of worker count.")


async def queue_capacity_bounds_memory(seconds=6, capacity=200):
    print(f"\n2) queue_capacity under sustained overload ({seconds}s each)")

    async def slow_handler(msg):
        await asyncio.sleep(0.05)

    print("   a) default (unbounded queue), sender faster than workers can drain:")
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(slow_handler, 4, name="unbounded_pool")
        start_rss = rss_mb()
        start = time.monotonic()
        sent = 0
        while time.monotonic() - start < seconds:
            for _ in range(200):
                await pool.send({"n": sent})
                sent += 1
            await asyncio.sleep(0)
        report_growth(f"unbounded (sent={sent})", start_rss, rss_mb())
        print("   -> queue grows as fast as it's sent to; expected, this is exactly")
        print("      what queue_capacity exists to bound.")
    finally:
        await node.stop()

    gc.collect()
    await asyncio.sleep(0.2)  # let the previous scenario's freed queue settle
                              # before measuring this one's own growth

    print(f"   b) queue_capacity={capacity}, same overload:")
    events = []
    node = Node("node@127.0.0.1:0")
    node.on_event(lambda e: events.append(e) if e.get("kind") == "pool_queue_full" else None)
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(
            slow_handler, 4, name="bounded_pool", queue_capacity=capacity
        )
        start_rss = rss_mb()
        start = time.monotonic()
        sent = 0
        while time.monotonic() - start < seconds:
            for _ in range(200):
                await pool.send({"n": sent})
                sent += 1
            await asyncio.sleep(0)
        report_growth(f"bounded (send attempts={sent})", start_rss, rss_mb())
        print(f"   -> pool_queue_full fired {len(events)} times ({len(events)}/{sent} "
              "sends dropped); growth stays near zero regardless of send rate. A "
              "negative number here is scenario (a)'s freed queue settling during "
              "this run, not scenario (b)'s own memory use going down.")
    finally:
        await node.stop()


async def stop_repeated_create_destroy(cycles=40, n_workers=4):
    print(f"\n3) pool.stop() over {cycles} create/destroy cycles "
          f"(executor='thread', {n_workers} workers each)")
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        report("start")
        checkpoints = {cycles // 4, cycles // 2, cycles}
        for i in range(1, cycles + 1):
            pool = await sup.spawn_pool(
                _cpu_square, n_workers, name="churn_pool", executor="thread"
            )
            await pool.map([{"n": 10_000}] * n_workers)
            await pool.stop()
            if i in checkpoints:
                report(f"after {i} cycles")
        assert not sup._children, f"leaked child records: {sup._children!r}"
        assert "churn_pool" not in node._mailboxes, "leaked dispatcher mailbox"
        print("   -> RSS should plateau after the first few cycles, not keep "
              "climbing; 0 leaked Supervisor children or mailboxes confirmed.")
    finally:
        await node.stop()


async def sharded_pool_cpu_profile(seconds=4, n_workers=6, sample_interval=0.15):
    print(f"\n4) fine-grained CPU% during a sharded (key=) pool run ({seconds}s, "
          f"sampling every {sample_interval}s)")

    async def handler(msg):
        await asyncio.sleep(0.01)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(
            handler, n_workers, name="sharded_pool", key=lambda msg: msg["order_id"]
        )

        samples = []
        stop_event = asyncio.Event()

        async def sampler():
            prev_cpu, prev_t = cpu_seconds(), time.monotonic()
            while not stop_event.is_set():
                await asyncio.sleep(sample_interval)
                now_cpu, now_t = cpu_seconds(), time.monotonic()
                pct = 100 * (now_cpu - prev_cpu) / (now_t - prev_t)
                samples.append(pct)
                prev_cpu, prev_t = now_cpu, now_t

        sampler_task = asyncio.create_task(sampler())
        start = time.monotonic()
        sent = 0
        while time.monotonic() - start < seconds:
            for _ in range(50):
                await pool.send({"order_id": sent % 30, "n": sent})
                sent += 1
            await asyncio.sleep(0)
        await asyncio.sleep(0.3)
        stop_event.set()
        await sampler_task
        await pool.stop()
        print(f"   sent={sent}, samples={len(samples)}")
        print(f"   CPU%%: min={min(samples):5.1f}  max={max(samples):5.1f}  "
              f"avg={sum(samples) / len(samples):5.1f}")
        print("   -> no sample should spike far above the average for this kind "
              "of light I/O-bound load; a sustained near-100% sample would "
              "indicate the dispatcher/routing logic is doing unexpected work.")
    finally:
        await node.stop()


async def main():
    await cpu_bound_speedup()
    await queue_capacity_bounds_memory()
    await stop_repeated_create_destroy()
    await sharded_pool_cpu_profile()


if __name__ == "__main__":
    asyncio.run(main())
