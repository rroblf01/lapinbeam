# Benchmarks

These numbers come straight from the scripts in `bench/`, run on the
maintainer's machine (Python 3.14, loopback network). They are **not** a
formal, reproducible benchmark suite with confidence intervals — treat them
as an order-of-magnitude sanity check, and re-run them on your own hardware
before making a decision based on them:

```bash
uv run python bench/bench_remote.py   # throughput
uv run python bench/bench_latency.py  # RTT percentiles
uv run python bench/bench_codec.py    # codec + JSON conversion path, layer by layer
uv run python bench/bench_memory.py   # RSS under sustained load, connection churn, and mailbox backpressure
uv run python bench/bench_pool.py     # spawn_pool(): executor= CPU speedup, queue_capacity bounding, stop() cleanup
```

## Throughput (`bench_remote.py`)

| Metric | Result |
| --- | --- |
| `asyncio.Queue` put/get (baseline, no lapinbeam) | ~1.6M msg/s |
| lapinbeam local actor send | ~440K msg/s |
| lapinbeam remote send (loopback TCP) | ~20K msg/s |

Methodology: `bench_asyncio_queue` measures a bare `asyncio.Queue` with one
producer and one consumer coroutine as the theoretical ceiling for
single-process message passing in Python. `bench_local_send` sends to a
spawned actor's `ActorRef` (2000 warmup sends, then timed). `bench_remote_send`
sends to a `RemoteRef` over an already-established loopback TCP connection
(100 warmup sends + 200ms settle, then timed) — this is fire-and-forget
throughput, not request/response.

The roughly 20x drop from local to remote send is expected: local sends are
zero-copy Python object references into an `asyncio.Queue`; remote sends pay
for JSON encoding, a bincode-framed write to a real (if loopback) TCP socket,
and the Rust-side per-peer outbound queue and writer task. See "Codec"
below for where the remote path's own cost is spent.

## Codec (`bench_codec.py`)

Breaks the remote send path down layer by layer — where `full encode` and
`end-to-end` include everything above them in the table:

| Step | Result |
| --- | --- |
| `codec.encode_payload` (Python: dataclass/Pydantic tagging) | ~42K ops/s |
| `codec.decode_payload` (Python) | ~52K ops/s |
| `_core.encode_payload` (Rust: PyAny → JSON) | ~5K ops/s |
| `_core.decode_payload` (Rust: JSON → PyAny) | ~11K ops/s |
| Full encode (`codec` + `_core`, what `_send_remote` actually does) | ~4.5K ops/s |
| End-to-end `remote.send()` (fire-and-forget, real loopback connection) | ~2.3K ops/s |

Methodology: 20,000 timed operations per row (200 warmup), on a
moderately-nested payload — a few scalars, a nested dict, and a list of 20
nested dicts (see the script for the exact shape). This is heavier than
`bench_remote.py`'s trivial `{"n": 1}` message, which is why these
per-operation numbers are lower than the ~20K msg/s throughput figure
above — the two scripts aren't measuring the same payload, only the same
code path.

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

## Memory (`bench_memory.py`)

Unlike the throughput/latency/codec benchmarks above, this doesn't report a
single number to compare — it samples this process's RSS (`VmRSS` from
`/proc/self/status`, **Linux only**) across three longer-running scenarios
and prints whether it plateaus (healthy) or keeps climbing (a leak):

1. **Sustained local + remote traffic** — RSS should level off within the
   first few seconds and stay flat.
2. **Rapid `connect_peer()`/`forget_peer()` churn**, run as several
   back-to-back rounds — a real per-cycle leak keeps adding roughly the
   same amount every round; healthy behavior plateaus after the first one.
3. **A permanently crash-looping actor**, once with the default unbounded
   mailbox and once with `mailbox_capacity` set — demonstrates that the
   "unbounded mailboxes" limitation (see [Limitations](index.md#limitations))
   is trivial to hit for real (a slow or crash-looping consumer is enough)
   and that `mailbox_capacity` actually bounds it, firing
   `on_event(kind="mailbox_full")` instead of growing forever.

Absolute RSS values depend heavily on the machine and are not meaningful to
compare across runs or hardware — what matters is the *shape* of each
scenario's curve (flat vs. climbing), which is why this script prints a
running series of samples rather than a single before/after pair.

## Pool (`bench_pool.py`)

Scoped to `Supervisor.spawn_pool()` specifically, answering three
questions the general memory checks above don't:

1. **Does `executor="process"` actually buy real CPU parallelism** for
   CPU-bound work, compared to a plain async pool or `executor="thread"`
   (both still serialized by the GIL)? On the maintainer's 12-core
   machine, 4 workers: ~1x for the plain async pool and `executor="thread"`,
   ~3.4x for `executor="process"` — real, if not perfectly linear,
   speedup only when it's actually running in separate processes. See
   the "This parallelism is for I/O-bound work, not CPU-bound work"
   warning in [Getting started](getting-started.md#concurrency-one-actor-handles-one-message-at-a-time).
2. **Does `queue_capacity` actually keep memory bounded** under sustained
   overload? Without it, RSS grows as fast as messages are sent
   (hundreds of MiB over a few seconds); with it, growth stays flat and
   the excess is dropped and counted via `on_event(kind="pool_queue_full")`
   instead.
3. **Does `pool.stop()` actually release everything** — mailboxes, tasks,
   `Supervisor` bookkeeping, and (for `executor=` pools) OS
   threads/processes — across many create/destroy cycles? RSS should
   plateau after the first few cycles, the same "does it plateau" check
   as the Memory script's connection-churn scenario.

Also samples this process's CPU% at fine granularity (~0.15s, reading
`/proc/self/stat` directly) during a sharded (`key=`) pool run, to catch
the kind of short-lived spike that only shows up at fine granularity.

## Reading these numbers correctly

- **Loopback only.** None of this measures real network latency between
  separate hosts — expect remote numbers to be dominated by actual RTT once
  nodes are on different machines, not by lapinbeam's own overhead.
  Loopback is what isolates the framework's cost from the network's.
  See [Examples](examples.md) for running nodes across real hosts.
- **Single peer, single actor.** The throughput/latency/codec benchmarks
  don't exercise multiple concurrent peers or many actors multiplexed over
  one connection — they isolate the best-case cost of one send.
  `bench_memory.py` is the exception: it specifically exercises connection
  churn and mailbox backpressure.
- **No comparison run against Celery+RabbitMQ in this repository.** The
  qualitative latency comparison on the [vs. Celery + RabbitMQ](vs-celery-rabbitmq.md)
  page (broker hop typically single-digit-to-tens of milliseconds) is a
  general characteristic of broker-mediated messaging, not a benchmark run
  against lapinbeam side by side under identical conditions. Take it as
  directional, not as a number to cite.
