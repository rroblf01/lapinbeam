# lapinbeam

Real-time distributed systems framework for Python with a Rust core.
An actor model inspired by Erlang/Elixir (BEAM), built with Rust (Tokio) exposed through PyO3.

## Status

Alpha. MVP: two-node bidirectional message passing over a multiplexed TCP transport.

## Features

- `@actor` decorated Python classes with `async def receive(msg)`.
- `Supervisor` with restart strategies (`one_for_one`).
- `Node` with transparent remote actor references.
- Multiplexed TCP transport (one socket per peer) with bincode serialization.
- Heartbeat and connection watchdog in the Rust core.

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

Or with Docker:

```bash
docker compose up --build
```

## Development

```bash
uv sync                       # create .venv, build the extension, install deps
uv run maturin develop        # fast rebuild of the Rust extension
uv run pytest                 # Python test suite
cargo test                    # Rust test suite
uv run python bench/bench_remote.py   # benchmarks
```

Nothing is installed on the OS: everything lives in `.venv`.

## Publishing to PyPI

```bash
uv build                      # produce wheel (abi3) + sdist in dist/
uv publish                    # upload to PyPI (uses UV_PUBLISH_TOKEN)
```

CI (`./.github/workflows/ci.yml`) runs the full test matrix on Python 3.11-3.14
and builds the distributable artifacts.

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
