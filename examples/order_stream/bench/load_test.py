"""Load generator for the order_stream example.

Fires N orders concurrently, follows each one's own SSE stream until it
finishes, and reports how long each one actually took — this is what
shows whether N orders really ran in parallel (all finishing around the
same time, close to a single order's own ~4-12s) or serialized behind
each other (finishing times spread out linearly with N).

Needs `httpx` (not stdlib) since it has to hold N concurrent streaming
connections open at once — see bench/pyproject.toml.
"""

import argparse
import asyncio
import json
import statistics
import time

import httpx


async def run_one(client, base_url):
    start = time.perf_counter()
    resp = await client.post(f"{base_url}/orders")
    resp.raise_for_status()
    order_id = resp.json()["id"]

    status = None
    async with client.stream("GET", f"{base_url}/orders/{order_id}/stream", timeout=None) as r:
        async for line in r.aiter_lines():
            if not line.startswith("data: "):
                continue
            payload = json.loads(line[len("data: "):])
            status = payload["status"]
            if status != "en_progreso":
                break
    elapsed = time.perf_counter() - start
    return order_id, status, elapsed


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("-n", "--count", type=int, default=200)
    args = parser.parse_args()

    # httpx's default pool (100 connections) is meant for short-lived
    # requests — here every order holds its SSE connection open for the
    # order's whole lifetime, so the pool must fit all of them at once or
    # later ones queue for a connection until the earlier ones' *entire*
    # orders finish, which can cascade into everything timing out. Not a
    # server-side limit — just this script's.
    limits = httpx.Limits(max_connections=args.count + 20, max_keepalive_connections=args.count + 20)
    async with httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(60.0)) as client:
        t0 = time.perf_counter()
        results = await asyncio.gather(
            *(run_one(client, args.base_url) for _ in range(args.count)),
            return_exceptions=True,
        )
        wall = time.perf_counter() - t0

    ok = [r for r in results if not isinstance(r, Exception) and r[1] == "completado"]
    failed = [r for r in results if isinstance(r, Exception) or r[1] != "completado"]
    times = [r[2] for r in ok]

    print(f"N={args.count}  wall-clock total={wall:.2f}s")
    print(f"  completados: {len(ok)}  fallidos: {len(failed)}")
    if times:
        times.sort()
        p50 = statistics.median(times)
        p90 = times[int(0.9 * (len(times) - 1))]
        print(f"  tiempo por pedido: min={min(times):.2f}s  p50={p50:.2f}s  "
              f"p90={p90:.2f}s  max={max(times):.2f}s")
    for r in failed[:5]:
        print("  fallo:", r)


if __name__ == "__main__":
    asyncio.run(main())
