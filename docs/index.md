# lapinbeam

**Real-time distributed systems framework for Python with a Rust core.**

lapinbeam brings an actor model inspired by Erlang/Elixir (BEAM) to Python:
`@actor`-decorated classes, a `Supervisor` that restarts crashed actors, and a
`Node` that gives you transparent references to actors running on other
machines. The networking layer — a multiplexed TCP transport with heartbeats,
framing, and automatic reconnection — is written in Rust (Tokio) and exposed
through PyO3, so all I/O happens off the GIL while your actors keep running as
plain `async def` coroutines.

!!! warning "Status: alpha"
    The MVP is two-node bidirectional message passing over a multiplexed TCP
    connection. See [Limitations](#limitations) below before betting
    production traffic on it.

## Why lapinbeam exists

Most Python systems reach for a task queue (Celery, RQ, Dramatiq) the moment
they need to run work outside the request/response cycle, and reach for a
message broker (RabbitMQ, Redis, Kafka) the moment two processes need to talk.
That combination is excellent for **durable background jobs** — but it adds
an extra service to run, and a hop through a broker, even for two processes
that just want to exchange a message and get an ack back in under a
millisecond.

lapinbeam targets that other case: processes that want direct, low-latency,
typed actor-to-actor messaging, with no broker to deploy, and no untyped JSON
blob in between. See [lapinbeam vs. Celery + RabbitMQ](vs-celery-rabbitmq.md)
for an honest comparison — they solve different problems and you may well
want both in the same system.

## Features

- `@actor`-decorated Python classes with `async def receive(msg)`, or typed
  dispatch via `@on(Type)` / `@on(default=True)` — see
  [Typed messages](typed-messages.md).
- `Supervisor` with restart strategies (`one_for_one`).
- `Node` with transparent remote actor references (`RemoteRef`) that look
  just like local ones (`ActorRef`).
- Multiplexed TCP transport (one socket per peer) with bincode framing.
- Heartbeat and connection watchdog in the Rust core; automatic reconnection
  of desired peers with backoff.
- System events (`Node.on_event`) for peer connect/disconnect and delivery
  errors — no silent message drops.
- Type-preserving payloads: `@dataclass` and Pydantic v2 models round-trip
  between nodes exactly as sent, via `lapinbeam.codec`.

## Security

lapinbeam's transport has **no authentication and no encryption** today: the
handshake a peer sends on connecting is simply *believed* — a `NodeId` is
whatever string the other end claims, with nothing to verify it — and all
traffic travels as plain, unencrypted TCP. Any process that can reach a
node's listening port can complete a handshake, claim to be any peer id, and
send it messages.

This is a deliberate, known trade-off for an early-stage project, not an
oversight — it's the same trust posture Erlang's distribution protocol has
defaulted to for decades (a shared cookie, not real authentication). It's
fine for a cluster of processes on a network boundary you already trust: a
private VPC, a single Docker Compose/Kubernetes network, a LAN. It is
**not** fine to expose a `Node`'s listening port directly to the open
internet. If you need that, put lapinbeam behind something that actually
authenticates and encrypts the link — a VPN, a WireGuard tunnel, an
mTLS-terminating proxy — rather than relying on the transport itself.

## Install

```bash
pip install lapinbeam
```

The wheel is built for `abi3 >= 3.11`, so a single artifact covers Python 3.11
through 3.14. Nothing else needs to be installed or run — no broker, no
external service.

Continue with [Getting started](getting-started.md).

## Limitations

- Payloads must be JSON-compatible (dict/list/str/int/float/bool/None) — or a
  `@dataclass`/Pydantic model, encoded through `lapinbeam.codec`. Ints are
  limited to `i64`/`u64`.
- Type preservation happens only on **remote** sends; local sends pass the
  object by reference (zero-copy).
- Actor names must be unique per node.
- No message persistence and no at-least-once delivery: a message in flight
  during a network partition is lost, not retried. See
  [lapinbeam vs. Celery + RabbitMQ](vs-celery-rabbitmq.md) for what that
  means in practice.
- Payloads larger than 16 MiB are rejected on the sender.

## License

MIT
