# lapinbeam

[![CI](https://github.com/rroblf01/lapinbeam/actions/workflows/ci.yml/badge.svg)](https://github.com/rroblf01/lapinbeam/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-online-blue)](https://rroblf01.github.io/lapinbeam/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://github.com/rroblf01/lapinbeam/blob/main/LICENSE)

Real-time distributed systems framework for Python with a Rust core.
An actor model inspired by Erlang/Elixir (BEAM), built with Rust (Tokio) exposed through PyO3.

Repository: <https://github.com/rroblf01/lapinbeam> · Docs: <https://rroblf01.github.io/lapinbeam/>

## Status

Alpha. MVP: two-node bidirectional message passing over a multiplexed TCP transport.

## Features

- `@actor` decorated Python classes with `async def receive(msg)`, or typed
  dispatch via `@on(Type)` / `@on(default=True)` (see below).
- `Supervisor` with restart strategies (`one_for_one`).
- `Node` with transparent remote actor references.
- Multiplexed TCP transport (one socket per peer) with bincode serialization.
- Heartbeat and connection watchdog in the Rust core.
- Automatic reconnection of desired peers with backoff.
- Type-preserving payloads: `@dataclass` and Pydantic v2 models round-trip
  between nodes via `lapinbeam.codec`.

## Install

```bash
pip install lapinbeam
```

The wheel is built for `abi3 >= 3.11`, so a single artifact covers Python 3.11 through 3.14.

## Quickstart (two nodes)

```bash
# terminal 1
NODE_NAME=node_a@127.0.0.1:9001 PEER=node_b@127.0.0.1:9002 uv run python examples/app_node_a.py
# terminal 2
NODE_NAME=node_b@127.0.0.1:9002 PEER=node_a@127.0.0.1:9001 uv run python examples/app_node_b.py
```

Or with Docker (validated end-to-end: 100/100 ACKs inside the compose network):

```bash
docker compose up --build
```

## Typed message dispatch

By default an actor implements a single `async def receive(self, msg)`. As an
alternative, use `@on(Type)` to dispatch by the message's real type — which
`lapinbeam.codec` already preserves for `@dataclass`/Pydantic payloads across
nodes — and `@on(default=True)` for a catch-all handler:

```python
from dataclasses import dataclass
from lapinbeam import actor, on


@dataclass
class Task:
    payload_id: int
    name: str


@actor(name="worker")
class Worker:
    @on(Task)
    async def handle_task(self, msg: Task):
        ...

    @on(default=True)
    async def handle_other(self, msg):
        print("unrecognized message:", msg)
```

An actor with any `@on` handler stops using `receive` entirely; a message
whose type has no dedicated handler and no `@on(default=True)` fallback
raises `TypeError` (crashing the actor, so `Supervisor` restarts it like any
other unhandled exception). Actors that only define `receive` are unaffected.

## Development

```bash
uv sync                       # create .venv, build the extension, install deps
uv run maturin develop        # fast rebuild of the Rust extension
uv run pytest                 # Python test suite
cargo test                    # Rust test suite
uv run python bench/bench_remote.py   # throughput benchmarks
uv run python bench/bench_latency.py  # RTT latency percentiles
```

Nothing is installed on the OS: everything lives in `.venv`.

## Documentation

Full docs (English + Spanish) live under `docs/` and build with MkDocs +
Material:

```bash
uv sync --group docs           # installs mkdocs, mkdocs-material, mkdocs-static-i18n
uv run mkdocs serve             # http://127.0.0.1:8000, live-reloads on edits
uv run mkdocs build --strict    # static site in site/ (gitignored)
```

Each page has an English file (e.g. `docs/getting-started.md`) and its
Spanish translation (`docs/getting-started.es.md`); `mkdocs-static-i18n`
serves the Spanish build under `/es/` with a language switcher.

## Benchmark snapshot

Measured on this machine (Python 3.14, loopback):

| Metric | Result |
| --- | --- |
| asyncio.Queue put/get | ~1.6M msg/s |
| lapinbeam local send | ~1.2M msg/s |
| lapinbeam remote (loopback TCP) throughput | ~16K msg/s |
| Local dispatch RTT | p50 0.007 ms |
| Remote loopback TCP RTT (send + ack) | p50 0.44 ms / p99 0.93 ms |

## Limitations

- Payloads must be JSON-compatible (dict/list/str/int/float/bool/None). Ints are
  limited to `i64`/`u64`; larger ints raise `TypeError`.
- `__lb_type__` is a reserved payload key (used by the type-preserving codecs).
- Type preservation happens only on **remote** sends; local sends pass the object
  by reference (zero-copy). Nested dataclass-in-Pydantic fields are not rebuilt.
- Actor names must be unique per node. If two nodes dial each other at the same
  time, two connections are created (deduplication not implemented yet).
- Payloads larger than 16 MiB are rejected on the sender.

## Publishing to PyPI

```bash
uv build                      # produce wheel (abi3) + sdist in dist/
uv publish                    # upload to PyPI (uses UV_PUBLISH_TOKEN)
```

CI (`./.github/workflows/ci.yml`) runs the test matrix on Python 3.11-3.14, a
Docker Compose end-to-end check, and builds the distributable artifacts.

## Project layout

```
src/           Rust core (_core extension module)
lapinbeam/     Pure-Python layer (@actor, Node, Supervisor, refs)
tests/         Rust integration tests
tests-python/  Python tests (pytest)
examples/      Two-node bidirectional demo
bench/         Latency benchmarks
```

## License

MIT
