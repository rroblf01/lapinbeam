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

## Development

```bash
uv sync                       # create .venv, build the extension, install deps
uv run maturin develop        # fast rebuild of the Rust extension
uv run pytest                 # Python test suite
cargo test                    # Rust test suite
```

Nothing is installed on the OS: everything lives in `.venv`.

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
