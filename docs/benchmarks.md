# Benchmarks

These numbers come straight from the scripts in `bench/`, run on the
maintainer's machine (Python 3.14, loopback network). They are **not** a
formal, reproducible benchmark suite with confidence intervals — treat them
as an order-of-magnitude sanity check, and re-run them on your own hardware
before making a decision based on them:

```bash
uv run python bench/bench_remote.py   # throughput
uv run python bench/bench_latency.py  # RTT percentiles
```

## Throughput (`bench_remote.py`)

| Metric | Result |
| --- | --- |
| `asyncio.Queue` put/get (baseline, no lapinbeam) | ~1.6M msg/s |
| lapinbeam local actor send | ~1.2M msg/s |
| lapinbeam remote send (loopback TCP) | ~16K msg/s |

Methodology: `bench_asyncio_queue` measures a bare `asyncio.Queue` with one
producer and one consumer coroutine as the theoretical ceiling for
single-process message passing in Python. `bench_local_send` sends to a
spawned actor's `ActorRef` (2000 warmup sends, then timed). `bench_remote_send`
sends to a `RemoteRef` over an already-established loopback TCP connection
(100 warmup sends + 200ms settle, then timed) — this is fire-and-forget
throughput, not request/response.

The roughly 75x drop from local to remote send is expected: local sends are
zero-copy Python object references into an `asyncio.Queue`; remote sends pay
for JSON encoding, a bincode-framed write to a real (if loopback) TCP socket,
and the Rust-side per-peer outbound queue and writer task.

## Latency (`bench_latency.py`)

| Metric | Result |
| --- | --- |
| Local dispatch RTT (send → receive) | p50 0.007 ms |
| Remote loopback TCP RTT (send + ack) | p50 0.44 ms / p99 0.93 ms |

Methodology: 2000 timed round trips (100 warmup) per measurement.
"Local dispatch" times a send from outside any actor into a `Client` actor's
mailbox and back to an `asyncio.Event`, i.e. pure Python/asyncio scheduling
overhead with no network involved. "Remote loopback" times a full round trip
through the Rust transport: node A sends to an `Echo` actor on node B, which
replies to node A over the same multiplexed TCP connection — this is the
number to compare against a broker-mediated round trip (see
[lapinbeam vs. Celery + RabbitMQ](vs-celery-rabbitmq.md)).

## Reading these numbers correctly

- **Loopback only.** None of this measures real network latency between
  separate hosts — expect remote numbers to be dominated by actual RTT once
  nodes are on different machines, not by lapinbeam's own overhead.
  Loopback is what isolates the framework's cost from the network's.
  See [Examples](examples.md) for running nodes across real hosts.
- **Single peer, single actor.** These benchmarks don't exercise multiple
  concurrent peers, mailbox backpressure, or many actors multiplexed over
  one connection — they isolate the best-case cost of one send.
- **No comparison run against Celery+RabbitMQ in this repository.** The
  qualitative latency comparison on the [vs. Celery + RabbitMQ](vs-celery-rabbitmq.md)
  page (broker hop typically single-digit-to-tens of milliseconds) is a
  general characteristic of broker-mediated messaging, not a benchmark run
  against lapinbeam side by side under identical conditions. Take it as
  directional, not as a number to cite.
