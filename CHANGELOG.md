# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/) —
while it stays in the `0.x` line (alpha), any release may include breaking
changes.

## [Unreleased]

### Performance

- **Payload encode/decode is significantly faster**, measured with the new
  `bench/bench_codec.py` (a moderately-nested realistic payload, before vs.
  after, averaged over multiple runs):
  - `codec.encode_payload` (pure Python): **~2.6x faster**. It no longer
    computes a registry-lookup tag for every JSON-native scalar (int, str,
    bool, ...), and no longer rebuilds a dict/list copy when nothing inside
    it actually needed type-tagging.
  - `_core.encode_payload` (Rust PyAny→JSON): **~87% faster**. Type dispatch
    used to probe via `.extract::<bool>()`/`.extract::<i64>()`, which are
    not simply "try and cheaply fail" — PyO3's `bool` extractor does a
    NumPy-interop fallback that looks up `type(obj).__module__` on every
    non-bool value, and its integer extractors call the C API directly on
    *any* object on Python 3.10+, which raises and clears a real Python
    exception for every non-int (a float, list, or dict). Dispatch now
    probes with `.cast::<T>()` (a cheap C-level type check that never
    raises) and only extracts once the concrete type is confirmed.
  - `_core.decode_payload` (Rust JSON→PyAny) and `encode_payload`/
    `decode_payload` no longer build an intermediate `serde_json::Value`
    tree — they serialize/deserialize directly against Python objects
    (`~30%` and `~2x` faster respectively for the Rust step alone).
  - End-to-end (`RemoteRef.send()` over a real loopback connection):
    **~24% faster**, confirmed with an alternating A/B comparison (not a
    single before/after pair) to rule out run-to-run system noise.

### Added

- **`Node(..., cluster_secret=...)`**: opt-in shared-secret handshake check.
  Every node in a cluster started with the same secret proves it via a
  random nonce + `HMAC-SHA256`; a handshake that doesn't match is dropped
  before ever being registered as a peer. One-directional and no
  encryption/replay-protection — see docs/index.md's "Security" section for
  the exact scope. Without `cluster_secret`, behavior is unchanged.
- **`on_event`'s `"decode_error"` kind**: a message that fails to decode
  (a Pydantic `ValidationError`, a dataclass constructed with the wrong
  fields, ...) now surfaces as an observable event with the actor name and
  the exception, instead of vanishing into asyncio's default
  "Exception in callback" log.
- Wire protocol version is now validated on receipt (previously written on
  every frame but never checked) — a mismatch drops the connection instead
  of risking misinterpreting frames from an incompatible lapinbeam version.
- Deterministic tiebreak for simultaneous dial: when both sides of a pair
  dial each other at once, the connection dialed by whichever `NodeId`
  sorts first (lexicographically) wins, so exactly one connection survives
  instead of leaking a wasted duplicate.
- `RUST_LOG`-aware `tracing_subscriber` installed in `Node.start()` — every
  internal `tracing::warn!`/`debug!` call was previously a complete no-op.
- **`Node(..., reconnect_max_attempts=30)`** and **`Node.forget_peer(peer_id)`**
  — see "Fixed" below for the leak this closes.
- **`lapinbeam.current_message()`**: returns a `MessageMeta(src, reply_to,
  correlation_id, msg_id)` describing the message the running handler is
  processing — `src` is always populated (the sending node's own id, for a
  message sent by a local actor), the rest are `None` unless the sender set
  them. `ActorRef.send`/`RemoteRef.send` gained matching `reply_to=`/
  `correlation_id=` keyword arguments. This finishes wire-level fields
  (`WireMessage.reply_to`/`correlation_id`) that already existed but were
  silently dropped before reaching Python — see "Fixed" below.
- `on_event`'s `"error"` events now include `event["correlation_id"]`,
  echoing the `correlation_id` of the send that failed.
- `on_event`'s new `"supervisor_gave_up"` kind: fires when a `Supervisor`
  stops restarting an actor after exhausting `max_restarts` — see "Fixed"
  below.
- `Node.peer_count()`: the number of currently connected peers. Existed in
  the Rust binding since the MVP but was never exposed on the Python `Node`.
- **`Node(..., heartbeat_interval=, peer_timeout=, peer_queue_capacity=)`**:
  these already existed as `TransportConfig` fields but were hardcoded
  (1s / 3s / 256) with no way to tune them from Python. `None` (the
  default for each) keeps exactly today's values.
- **`Node(..., mailbox_capacity=N)`**: caps how many undelivered messages
  an actor's mailbox can hold. `None` (the default) keeps today's
  unbounded behavior. Once set, a full mailbox drops new messages instead
  of growing forever — see "Fixed" below — firing
  `on_event(kind="mailbox_full")`, and for a dropped *remote* send, an
  `"error"` event on the sender too (`detail` starts with
  `"mailbox_full:"`, `correlation_id` echoed as usual).
- New `docker-compose.secure.yml` (two containers with a matching
  `cluster_secret`) and `docker-compose.restart.yml` (kills and restarts
  one container mid-stream, verifying the other reconnects and resumes
  delivery) E2E fixtures, each with a matching CI job — see "CI/CD" below.
- **`Supervisor.shutdown()`**: cancels every actor this `Supervisor`
  spawned and waits for them to stop, without affecting actors spawned by
  a different `Supervisor` on the same `Node`. See "Fixed" below for why
  this — and `Node.stop()` now doing the equivalent for every `Supervisor`
  on a node — matters.
- **`ActorRef.ask(msg, timeout=5.0)` / `RemoteRef.ask(msg, timeout=5.0)`**:
  request/response on top of `send()`'s existing `reply_to`/
  `correlation_id` — tags the send, waits for a single correlated reply
  (registering a one-shot hidden mailbox as the reply address, cleaned up
  afterwards either way), and raises `TimeoutError` if nothing replies in
  time. The receiving handler still has to reply explicitly — see
  `MessageMeta.reply()` next.
- **`MessageMeta.reply(msg)`**: shortcut for replying to whoever sent the
  message a handler is currently processing — `current_message().reply(x)`
  instead of manually building an `ActorRef`/`RemoteRef` from `meta.src`/
  `meta.reply_to` and tagging `correlation_id` by hand. Raises
  `RuntimeError` if `reply_to` is `None`. `MessageMeta` gained a `node`
  field (the `Node` that received the message) to make this possible.

### Fixed

- `Transport::shutdown()` no longer resurrects a connection whose handshake
  was still being processed concurrently with the shutdown call.
- **Unbounded reconnect leak.** A peer marked *desired* (by `connect_peer`)
  was never removed from that set except on full `shutdown()` — so a peer
  that goes away for good used to trigger an unconditional retry-forever
  loop (one `connect()` attempt every `reconnect_interval`, by default
  every second, forever) and permanently grow the desired-peers set for
  the life of the process. Reconnection now gives up after
  `reconnect_max_attempts` (default 30) consecutive failures, removes the
  peer from the desired set at that point, and fires
  `on_event(kind="reconnect_gave_up")`. `Node.forget_peer(peer_id)` gives
  an application that already knows it's done with a peer a way to clean
  up immediately instead of waiting for that to happen on its own.
- **Message metadata was write-only.** `WireMessage.reply_to`/
  `correlation_id` were accepted on send and, in the `correlation_id` case,
  even correctly threaded into the `Error` reply for an unknown-actor send
  — but discarded before ever reaching the receiving actor or the `"error"`
  event in Python, in two separate spots (`drain_loop` and
  `event_drain_loop` in `src/py/node.rs`). In practice this meant an actor
  had no built-in way to learn which node sent it a message. Both hops are
  now wired through — see `current_message()` and `on_event`'s
  `"correlation_id"` field above.
- **`Supervisor` could die silently on a crash in an actor's `__init__`.**
  Actor construction ran outside the `try`/`except` that drives restarts,
  so a bug in `__init__` — on the very first `spawn()`, or on any later
  restart attempt — killed the supervising task with nothing but asyncio's
  generic "exception was never retrieved" warning: no restart, no
  `on_event`, the actor simply stopped existing. Construction now goes
  through the same restart/backoff path as a crash in `receive`/`@on`, and
  exhausting restarts (for either kind of crash) now also fires
  `on_event(kind="supervisor_gave_up")` in addition to the pre-existing
  behavior of raising through `ActorRef.task`.
- **Unbounded mailbox growth under a slow or stuck actor**, found during
  the same audit as the reconnect leak above, in two places: (1) the
  per-actor `asyncio.Queue()` (Python) had no size limit at all; (2) the
  Rust-side channel between the transport and Python, when full, spawned
  a new task blocked on `.send().await` *per message* instead of applying
  any real limit — proven by the test that already relied on this
  (`slow_mailbox_does_not_block_other_traffic`). Fixed independently at
  both layers: the Rust channel now drops the message (and notifies, see
  `Event::MailboxFull` / `on_event`) instead of spawning an unbounded
  blocked task; the Python queue is boundable via the new
  `mailbox_capacity` (above), with the same drop-and-notify behavior once
  set. Both default to unchanged behavior unless configured.
- **Every actor task leaked on `Node.stop()`.** `Supervisor` had no
  `stop()`/`shutdown()` of any kind, and `Node.stop()` never touched the
  actor tasks it had spawned — each one was left running forever, blocked
  reading from a mailbox nothing would ever fill again (the mailbox itself
  was cleared, but the task waiting on it was not). `Node.stop()` now
  cancels every actor task from any `Supervisor` on that node; the new
  `Supervisor.shutdown()` does the same for just its own actors. As part
  of this, `Supervisor._watchers` — which tracked spawned tasks but never
  pruned finished ones, a real if narrower leak for a `Supervisor` that
  spawns many short-lived actors over its life (e.g. a worker-pool
  pattern) — now self-prunes via `add_done_callback` the moment a task
  ends, for any reason.

### Documentation

Audited every doc page (`docs/*.md`, `README.md`, `CLAUDE.md`) against the
actual current code rather than trusting existing prose — found and fixed:

- **README.md and CLAUDE.md both claimed simultaneous dial "creates two
  connections (no dedup yet)"** — stale; the deterministic tiebreak (see
  `[Unreleased] > Added` above) has resolved this to exactly one surviving
  connection for a while. Both corrected.
- **Stale local-send throughput figure.** `README.md` and
  `docs/benchmarks.md`/`.es.md` said "~1.2M msg/s" for `lapinbeam local
  send"; re-running `bench/bench_remote.py` (3x, for stability) measures
  ~440K msg/s consistently. Corrected in all three places, along with the
  dependent "~75x drop" comparison (now ~20x).
- **`bench/bench_codec.py` was never documented** on `docs/benchmarks.md`/
  `.es.md`, despite existing, being referenced in `README.md`/`CLAUDE.md`'s
  command lists, and being the entire subject of this changelog's
  "Performance" section above. Added a "Codec" section with real numbers
  from re-running it.
- **`docs/index.md`'s Limitations list was missing two items** that
  `README.md`'s equivalent list already had: `__lb_type__` being a reserved
  payload key, and a Pydantic-nesting caveat — synced into `index.md`/
  `.es.md`, and tightened based on actually testing the claim: a
  `@dataclass` nested in a **properly-typed** Pydantic field (e.g.
  `inner: Inner`) round-trips correctly via Pydantic's own validation; only
  a loosely-typed field (e.g. `Any`) loses the nested type on decode. The
  previous blanket "nested dataclass-in-Pydantic fields are not rebuilt"
  overstated the limitation.
- **`docs/examples.md`/`.es.md` didn't mention `docker-compose.secure.yml`
  or `docker-compose.restart.yml`** (both added earlier in this session)
  at all — added a section covering both, alongside the original
  `docker-compose.yml`.

### CI/CD

- `cargo fmt --check`, `cargo clippy -D warnings`, and `mypy lapinbeam/`
  gates (with a hand-written `lapinbeam/_core.pyi` stub for the compiled
  extension).
- Docs are now validated (`mkdocs build --strict`) on every push/PR, not
  only built as part of deploying to Pages on `main`.
- `cargo test` no longer runs once per Python version in the test matrix.
- The release pipeline (`publish.yml`) now builds proper `manylinux` wheels
  (a plain build was being rejected by PyPI) and smoke-tests the actual
  built wheel — installs it in a clean environment and runs the test suite
  against it — before publishing.
- **macOS and Windows**: `ci.yml` now also runs the test suite on
  `macos-latest` (Apple Silicon), `macos-15-intel`, and `windows-latest`;
  `publish.yml` builds and smoke-tests real wheels for all four platform
  targets (Linux manylinux, macOS x86_64/aarch64, Windows x64) before
  publishing, instead of only Linux. Not yet confirmed by an actual CI run.
- **Two new E2E jobs**, closing a gap where the only E2E coverage was a
  single happy-path run: `docker-e2e-secure` runs the same 100-ACK check
  as `docker-e2e` but with a matching `cluster_secret` on both containers
  (previously `cluster_secret` was only exercised in-process, never
  between two real separate processes); `docker-e2e-restart` kills and
  restarts node_b's container mid-stream and checks node_a's log climbs
  back up close to the full count afterward (not an exact match — message
  loss during the outage is expected, see Limitations — but a frozen
  count would mean reconnection never actually happened). Both verified
  locally with real `docker compose` runs (including a real mismatched-
  secret rejection) before being wired into CI, same as every other
  Docker-dependent claim in this changelog.

## [0.1.0] - 2026-08-16

Initial release.

### Added

- **Actor model**: `@actor`-decorated Python classes with `async def
  receive(msg)`, or typed dispatch per message type via `@on(Type)` /
  `@on(default=True)` as an alternative.
- **`Supervisor`**: spawns actors and restarts them on unhandled exceptions
  (`one_for_one` strategy, capped restarts within a rolling time window,
  exponential backoff).
- **`Node`**: the local endpoint of the cluster (`name@host:port`), usable
  as an `async with` context manager; transparent references to local
  (`ActorRef`) and remote (`RemoteRef`) actors that expose the same
  `await ref.send(msg)`.
- **System events**: `Node.on_event(callback)` surfaces peer
  connect/disconnect and delivery errors (e.g. sending to an unknown remote
  actor) instead of failing silently.
- **Transport core (Rust/Tokio)**: multiplexed TCP transport — one socket
  per peer, shared by every local actor — with bincode framing, a heartbeat
  and connection watchdog, and automatic reconnection of desired peers with
  backoff. All networking runs off the Python GIL.
- **Type-preserving payloads**: `@dataclass` and Pydantic v2 models
  round-trip between nodes as their exact original type via
  `lapinbeam.codec`; `register_codec` extends this to custom classes. Local
  (same-node) sends always pass the object by reference, with no encoding
  and no type restrictions.
- **Packaging**: single abi3 wheel covering Python 3.11–3.14.
- **Documentation**: English + Spanish docs (MkDocs + Material), covering
  getting started, typed messages, benchmarks, a comparison with Celery +
  RabbitMQ, and deployment examples (local, Docker Compose, real hosts).
- **CI/CD**: Rust + Python test matrix, Docker Compose two-node E2E check,
  wheel/sdist build, and a PyPI Trusted Publisher release workflow.

### Known limitations

- No message persistence or at-least-once delivery: a message in flight
  during a network partition is lost, not retried.
- Payloads must be JSON-compatible (or a `@dataclass`/Pydantic model via
  the codec); ints are limited to `i64`/`u64`, and payloads over 16 MiB are
  rejected on the sender.
- Actor names must be unique per node.
- No authentication on the transport handshake — suitable for a trusted
  LAN/cluster, not for running unmodified across the open internet.

See `ROADMAP.md` for what's planned before `1.0.0`.

[Unreleased]: https://github.com/rroblf01/lapinbeam/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rroblf01/lapinbeam/releases/tag/v0.1.0
