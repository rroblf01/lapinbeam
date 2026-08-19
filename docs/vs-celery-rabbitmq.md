# lapinbeam vs. Celery + RabbitMQ

These solve genuinely different problems, so "which is better" is the wrong
question — the right one is which shape your workload actually has. This
page is a technical comparison, not a sales pitch: Celery + RabbitMQ is
mature, production-proven infrastructure, and lapinbeam is a young, 1.0
library that has not been proven at that scale yet.

## The shape of the problem each one targets

**Celery + RabbitMQ** models **durable background jobs**: a task is a
function name plus arguments, put on a durable queue, picked up by any one of
a pool of worker processes, with automatic retries, rate limiting,
scheduling (`celery beat`), and dead-letter handling if it keeps failing.
The broker is the source of truth for what work exists — if every worker and
even the broker itself restarts, durable queues still redeliver unacked
messages.

**lapinbeam** models **direct, typed, actor-to-actor communication**: two
long-lived processes (or the same process, for local actors) exchange
messages over a connection they own, with no intermediary service. There is
no queue to inspect independently of the process holding the mailbox, and no
redelivery — if a node is unreachable when you send, the message is gone.

## Where lapinbeam has a real advantage

- **No broker to run.** Celery needs RabbitMQ (or Redis) as a separate,
  monitored service. lapinbeam's transport is a library import — two Python
  processes with `pip install lapinbeam` and the network is the whole
  infrastructure.
- **Latency.** A broker hop means: serialize → publish → broker persists/
  routes → consumer polls or gets pushed → deserialize → ack round-trip.
  lapinbeam talks directly over one multiplexed TCP connection per peer, no
  intermediary. See [Benchmarks](benchmarks.md): local dispatch is
  microseconds, and a remote loopback round trip (send + ack) is under half
  a millisecond p50. A Celery task round trip through RabbitMQ is typically
  single-digit-to-tens of milliseconds — the broker hop and worker pool
  polling are doing more work than the actual task logic for small jobs.
- **Typed payloads, not JSON blobs by convention.** `@dataclass`/Pydantic
  models round-trip as the exact type on the receiving end (see
  [Typed messages](typed-messages.md)), and `@on(Type)` dispatches on that
  type directly. Celery task arguments are positional/keyword arguments
  serialized by the configured serializer (JSON by default); nothing stops
  you from passing a Pydantic model's `.dict()` by hand, but the framework
  itself doesn't preserve or dispatch on the type.
- **Actor state.** An actor instance persists across messages (it's a Python
  object with `__init__`, held alive by `Supervisor` between sends). A
  Celery task is a stateless function invocation — any state has to live
  externally (a database, Redis, task chaining).

## Where Celery + RabbitMQ clearly wins

- **Durability.** RabbitMQ persists messages to disk (with durable
  queues/exchanges) and redelivers unacked ones after a crash. lapinbeam has
  **no persistence at all**: a bounded in-memory channel per actor, no disk
  spooling, no delivery guarantee beyond "the TCP connection was up and the
  mailbox had room." A message sent during a network partition is simply
  lost — see [Limitations](index.md#limitations).
- **Horizontal scaling of consumers.** Many Celery workers can consume from
  the same queue, load-balancing work automatically; add workers, throughput
  goes up, and you can add more worker *processes/machines* whenever you
  need more capacity. In lapinbeam, an actor name is one mailbox on one
  node; `Supervisor.spawn_pool()` gives you a fixed pool of workers
  sharing that mailbox's work *within one process* (including
  `executor="process"` for real CPU parallelism on that one machine's
  cores — see [Getting started](getting-started.md#concurrency-one-actor-handles-one-message-at-a-time))
  — but there's still no built-in way to fan the same logical work out
  across *multiple* processes or machines the way Celery's queue does.
- **Retries, rate limits, scheduling, workflows.** `celery beat` (cron-like
  scheduling), `retry(countdown=..., max_retries=...)`, rate limiting per
  task, chains/chords/groups for composing multi-step workflows — none of
  this exists in lapinbeam. `Supervisor` restarts a *crashed actor*, which is
  a different thing entirely from retrying a *failed unit of work*.
- **Operational maturity.** Flower for monitoring, a decade-plus of
  production usage, a large ecosystem of extensions and integration guides.
  lapinbeam is young: no dashboard, no dedicated ops tooling yet, and its
  wire protocol has no compatibility guarantees across versions — 1.0
  covers the Python API, not the wire format.

## Practical guidance

Reach for **Celery + RabbitMQ** for: sending emails, resizing images,
processing uploads, anything you'd be unhappy to lose on a crash, or a
pool of interchangeable workers that needs to span more processes or
machines than `Supervisor.spawn_pool()` (one process) can give you.

Reach for **lapinbeam** for: a real-time simulation or game server state
machine spread across processes, a cluster of stateful services that need to
call each other with sub-millisecond latency and typed payloads, or any case
where "the two processes are both up and directly reachable, and losing an
in-flight message during a crash is acceptable" — which is the same
trade-off Erlang/Elixir's `send` makes.

Nothing stops you from using both in the same system: Celery for durable
background work, lapinbeam for the low-latency actor mesh in front of it.
