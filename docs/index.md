# lapinbeam

**Real-time distributed systems framework for Python with a Rust core.**

lapinbeam brings an actor model inspired by Erlang/Elixir (BEAM) to Python:
`@actor`-decorated classes, a `Supervisor` that restarts crashed actors, and a
`Node` that gives you transparent references to actors running on other
machines. The networking layer — a multiplexed TCP transport with heartbeats,
framing, and automatic reconnection — is written in Rust (Tokio) and exposed
through PyO3, so all I/O happens off the GIL while your actors keep running as
plain `async def` coroutines.

!!! info "Status: 1.0"
    The public API (`Node`, `Supervisor`, `actor`/`on`, `ActorRef`/
    `RemoteRef`, `codec`) is stable — a breaking change now requires a major
    version bump. It hasn't been run at production scale yet, so read
    [Limitations](#limitations) below before betting production traffic on
    it.

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
- `Supervisor` with restart strategies (`one_for_one`, `one_for_all`,
  `rest_for_one`).
- `Node` with transparent remote actor references (`RemoteRef`) that look
  just like local ones (`ActorRef`).
- Multiplexed TCP transport (one socket per peer) with bincode framing.
- Heartbeat and connection watchdog in the Rust core; automatic reconnection
  of desired peers with backoff.
- System events (`Node.on_event`) for peer connect/disconnect and delivery
  errors — no silent message drops.
- Type-preserving payloads: `@dataclass` and Pydantic v2 models round-trip
  between nodes exactly as sent, via `lapinbeam.codec`.
- Lightweight seed-node discovery (`lapinbeam.discovery`): a node needs the
  address of one already-running node to transitively connect to everything
  that node (and everything it knows about) is connected to — see
  [Examples](examples.md#node-discovery-via-a-seed-node).
- Nested supervision trees: `Supervisor.spawn_supervisor()` lets a
  `Supervisor` supervise another `Supervisor`.
- Bidirectional links (`lapinbeam.links`) and one-way, non-lethal monitors
  (`lapinbeam.monitors`), both local- and cross-node — no wire protocol
  changes for either.
- Cluster-wide named process groups (`lapinbeam.groups`) and cluster-wide
  unique name registration (`lapinbeam.registry`), both local- and
  cross-node — no wire protocol changes for either.

## Security

lapinbeam's transport has **no encryption**: all traffic travels as plain
TCP, readable by anyone who can observe the network path. It does have a
lightweight, opt-in **shared-secret handshake check**:

```python
node = Node("app@0.0.0.0:9001", cluster_secret="a-secret-only-your-cluster-knows")
```

`0.0.0.0` above is a stand-in you should generally replace: the host in
`node_name` is both the interface `Node` binds its listener to *and* the
identity it advertises in every handshake — there's no separate "bind on
every interface, but tell peers to reach me at this other address" setting.
`0.0.0.0` only makes sense when every peer that will ever see it is on the
*same* machine (all talking over loopback); across real hosts, a peer that
tries to dial a node back using its self-reported `0.0.0.0` identity is
dialing a non-address — on Linux this typically connects to `127.0.0.1`
instead (silently wrong on any other host), and on other platforms it can
fail outright. For a real multi-host cluster, put the node's actual
reachable host or DNS name (a LAN IP, a container's service name, etc.) in
`node_name` instead — e.g. `Node("app@node-a.internal:9001")`.

Every node in the cluster must be started with the *same* `cluster_secret`.
On connect, the dialer proves it knows the secret (a random nonce plus its
`HMAC-SHA256`); if the acceptor's secret doesn't produce the same proof, the
handshake is dropped before the connection is ever registered as a peer —
an arbitrary process reaching the port can no longer just claim a peer id
and be believed. Without `cluster_secret` (the default), behavior is
unchanged: any handshake is accepted, exactly as before.

This is deliberately the same trust model Erlang's distribution protocol
has used for decades (a shared cluster cookie) — and it has the same
limits, stated plainly:

- **One-directional.** The dialer proves itself to the acceptor; the
  acceptor does not prove itself back. A rogue process squatting on a
  peer's address before the real node starts isn't caught by this.
- **No encryption, no replay protection.** A network-position attacker who
  can already observe traffic can capture a valid handshake and replay it
  later. This closes "any random process can join," not "a passive
  attacker on the wire can never get in."
- **A mismatched secret is not reported by `connect_peer()` itself.** The
  dialer marks itself connected the instant it sends the handshake — before
  the acceptor has had a chance to check it — so `await
  node.connect_peer(...)` can return normally even when the secret is
  wrong. The acceptor drops its side moments later; the dialer only learns
  this afterward, from `has_peer()` turning `False` again or (after
  `reconnect_max_attempts`) an `on_event(kind="reconnect_gave_up")`, not
  from an exception out of `connect_peer()`.

It's fine for a cluster of processes on a network boundary you already
trust — a private VPC, a single Docker Compose/Kubernetes network, a LAN —
with `cluster_secret` raising the bar further within that boundary. It is
**not** fine to expose a `Node`'s listening port directly to the open
internet, secret or not. If you need that, put lapinbeam behind something
that actually authenticates and encrypts the link end to end — a VPN, a
WireGuard tunnel, an mTLS-terminating proxy — rather than relying on the
transport itself.

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
  limited to `i64`/`u64`. `__lb_type__` is a reserved payload key, used by
  the type-preserving codec.
- Type preservation happens only on **remote** sends; local sends pass the
  object by reference (zero-copy). A Pydantic field typed loosely (e.g.
  `Any`) won't get a nested `@dataclass` value reconstructed on decode — it
  comes back as a plain dict instead; a properly-typed field (e.g.
  `inner: Inner`) round-trips fine via Pydantic's own validation.
- Actor names must be unique per node — `Supervisor.spawn()` raises
  `ValueError` if the name is already registered to a different actor.
  Simultaneous dial (both nodes connecting to each other at once) is
  resolved deterministically — exactly one connection survives, not two.
- No message persistence and no at-least-once delivery: a message in flight
  during a network partition is lost, not retried. See
  [lapinbeam vs. Celery + RabbitMQ](vs-celery-rabbitmq.md) for what that
  means in practice.
- Payloads larger than 16 MiB are rejected on the sender.
- Actor mailboxes are unbounded by default: an actor that can't keep up with
  its inbound rate has its mailbox grow without limit instead of applying
  backpressure. Pass `Node(..., mailbox_capacity=N)` to cap it — a full
  mailbox then drops new messages instead, firing
  `on_event(kind="mailbox_full")` (and, for a dropped remote send, an
  `"error"` event back on the sender).

## License

MIT
