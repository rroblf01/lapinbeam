"""Codec + JSON conversion path benchmark.

Measures three layers separately so a regression (or a future optimization)
can be attributed to the right place:
  1. `codec.encode_payload`/`decode_payload` — pure Python, the type-tagging
     walk in lapinbeam/codec.py.
  2. `_core.encode_payload`/`decode_payload` — the Rust PyAny<->JSON step.
  3. End-to-end over a real loopback connection (`Node.send_data` /
     receiving through an actor), the actual thing a user's throughput
     depends on.

Run with: uv run python bench/bench_codec.py
"""

import asyncio
import time

import lapinbeam._core as _core
from lapinbeam import Node, Supervisor, actor, codec

N = 20000

# A moderately-nested, realistic message: scalars, a nested dict, and a list
# of 20 nested dicts — not a toy {"a": 1}, not an absurd stress payload.
PAYLOAD = {
    "type": "TASK",
    "payload_id": 42,
    "reply_to": "ingestor",
    "metadata": {
        "created_at": "2026-08-16T12:00:00Z",
        "priority": 5,
        "tags": ["urgent", "batch", "retry"],
        "attempts": 0,
    },
    "items": [
        {"sku": f"SKU-{i}", "qty": i, "price": i * 1.5, "active": True}
        for i in range(20)
    ],
    "flags": {"a": True, "b": False, "c": None},
}


def bench(label, fn, n=N):
    # Warmup
    for _ in range(200):
        fn()
    start = time.perf_counter()
    for _ in range(n):
        fn()
    elapsed = time.perf_counter() - start
    print(f"{label:45s} {n / elapsed:>12.0f} ops/s   ({elapsed / n * 1e6:.2f} us/op)")


def bench_python_codec_encode():
    bench("codec.encode_payload (Python only)", lambda: codec.encode_payload(PAYLOAD))


def bench_python_codec_decode():
    encoded = codec.encode_payload(PAYLOAD)
    bench("codec.decode_payload (Python only)", lambda: codec.decode_payload(encoded))


def bench_rust_encode():
    tagged = codec.encode_payload(PAYLOAD)  # what actually reaches _core
    bench("_core.encode_payload (Rust only)", lambda: _core.encode_payload(tagged))


def bench_rust_decode():
    tagged = codec.encode_payload(PAYLOAD)
    raw_bytes = _core.encode_payload(tagged)
    bench("_core.decode_payload (Rust only)", lambda: _core.decode_payload(raw_bytes))


def bench_full_encode_pipeline():
    bench(
        "full encode (codec + _core, as _send_remote does)",
        lambda: _core.encode_payload(codec.encode_payload(PAYLOAD)),
    )


async def bench_end_to_end():
    n = 5000
    node = Node("bench@127.0.0.1:0")
    node_b = Node("bench_b@127.0.0.1:0")
    await node.start()
    await node_b.start()

    @actor(name="sink")
    class Sink:
        async def receive(self, msg):
            pass

    Supervisor(node=node_b).spawn(Sink)
    await node.connect_peer(node_b.local_id)
    remote = node.get_remote_actor(node_b.local_id, "sink")

    for _ in range(200):
        await remote.send(PAYLOAD)
    await asyncio.sleep(0.1)

    start = time.perf_counter()
    for _ in range(n):
        await remote.send(PAYLOAD)
    elapsed = time.perf_counter() - start
    print(f"{'end-to-end remote.send() (fire-and-forget)':45s} {n / elapsed:>12.0f} ops/s   ({elapsed / n * 1e6:.2f} us/op)")

    await node.stop()
    await node_b.stop()


if __name__ == "__main__":
    bench_python_codec_encode()
    bench_python_codec_decode()
    bench_rust_encode()
    bench_rust_decode()
    bench_full_encode_pipeline()
    asyncio.run(bench_end_to_end())
