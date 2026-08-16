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

### Fixed

- `Transport::shutdown()` no longer resurrects a connection whose handshake
  was still being processed concurrently with the shutdown call.

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
