# Examples

The runnable versions of the two-node demo below live in `examples/`
(`app_node_a.py` / `app_node_b.py`). This page shows the same shape running
across different deployment targets, plus two extra patterns (crash recovery,
custom types) that don't fit on the [Getting started](getting-started.md)
page.

## Two local processes

The simplest case: both nodes on `127.0.0.1`, different ports.

```bash
# terminal 1
NODE_NAME=node_a@127.0.0.1:9001 PEER=node_b@127.0.0.1:9002 python examples/app_node_a.py
# terminal 2
NODE_NAME=node_b@127.0.0.1:9002 PEER=node_a@127.0.0.1:9001 python examples/app_node_b.py
```

## Docker Compose (two containers)

`docker-compose.yml` in the repository root runs the same two actors as two
containers on a bridge network, addressing each other by container name
instead of an IP:

```yaml
services:
  node_a:
    build: .
    command: python app_node_a.py
    environment:
      - NODE_NAME=node_a@node_a:9001
      - PEER=node_b@node_b:9002
    ports: ["9001:9001"]
    networks: [lapinbeam-net]

  node_b:
    build: .
    command: python app_node_b.py
    environment:
      - NODE_NAME=node_b@node_b:9002
      - PEER=node_a@node_a:9001
    ports: ["9002:9002"]
    networks: [lapinbeam-net]

networks:
  lapinbeam-net:
    driver: bridge
```

```bash
docker compose up --build
```

The only thing that changes versus two local processes is the host part of
`NODE_NAME`/`PEER`: Docker's embedded DNS resolves `node_a`/`node_b` to the
right container IP on the bridge network. The CI pipeline
(`.github/workflows/ci.yml`) runs exactly this compose file and asserts
node_a's logs show `Total: 100` ACKs before tearing it down.

## Real, separate hosts

See [Getting started](getting-started.md#two-nodes-talking-to-each-other)
for a diagram of exactly what "server A" and "server B" mean here — each is
its own OS process, and this section just changes their addresses from
loopback to real machines. Nothing about lapinbeam is loopback-specific —
`NodeId` is just
`name@host:port`, and `host` can be any address the other side can route to.
Running the two actors on two different machines only changes the
environment variables:

```bash
# machine at 10.0.0.1
NODE_NAME=node_a@10.0.0.1:9001 PEER=node_b@10.0.0.2:9002 python examples/app_node_a.py
# machine at 10.0.0.2
NODE_NAME=node_b@10.0.0.2:9002 PEER=node_a@10.0.0.1:9001 python examples/app_node_b.py
```

Two things to plan for once you leave loopback:

- **Firewall the listening port** (`9001`/`9002` above) between the hosts —
  `Node.start()` binds and accepts from anywhere by default.
- **Expect real network RTT to dominate.** The
  [loopback benchmarks](benchmarks.md) isolate lapinbeam's own overhead
  (sub-millisecond); across real hosts your latency floor is whatever the
  network between them gives you, plus that overhead on top.

## Recovering from a crash

`Supervisor` restarts an actor whose `receive` (or `@on` handler) raises,
using the `one_for_one` strategy: only the crashed actor is restarted, with
exponential backoff, up to `max_restarts` within `restart_window` seconds
before giving up and re-raising:

```python
import asyncio
from lapinbeam import Node, Supervisor, actor

# Kept outside the actor on purpose — see the note below.
attempts = {"n": 0}


@actor(name="flaky")
class Flaky:
    async def receive(self, msg):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError(f"transient failure #{attempts['n']}")
        print("succeeded on attempt", attempts["n"])


async def main():
    async with Node("app@127.0.0.1:0") as node:
        sup = Supervisor(strategy="one_for_one", node=node,
                          max_restarts=5, restart_window=10.0)
        ref = sup.spawn(Flaky)
        for _ in range(3):
            await ref.send({})
            # Give the restart (with backoff) time to finish before the next
            # send — a send while the actor is mid-restart briefly has no
            # mailbox to land in and raises ValueError, same as sending to
            # any other name that isn't registered yet.
            await asyncio.sleep(0.4)
```

!!! warning "Restarts create a fresh instance — state does not survive"
    Every restart runs `actor_cls(*args, **kwargs)` again, so anything stored
    on `self` (like `self.attempts`) resets to its initial value on every
    crash — only the registration (the mailbox and its name) survives, so
    senders never need to know a restart happened. That's why `attempts`
    lives outside the actor above: if it were `self.attempts`, it would
    never reach `3` no matter how many times you send, since each crash
    hands the next message to a brand new instance starting over. If an
    actor needs state to survive its own crashes, persist it externally
    (a database, Redis, or — as above — a plain object the actor closes
    over) rather than on `self`.

## Custom (non-dataclass, non-Pydantic) types

[Typed messages](typed-messages.md) covers dataclasses and Pydantic models,
which round-trip automatically. Anything else needs an explicit codec
registered on **both** ends of the cluster:

```python
from lapinbeam import register_codec

class Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

    def __repr__(self):
        return f"Point({self.x}, {self.y})"


register_codec(
    Point,
    encode=lambda p: {"x": p.x, "y": p.y},
    decode=lambda d: Point(d["x"], d["y"]),
)

# From here on, sending a Point works exactly like a dataclass:
# await remote.send(Point(1, 2))
```

Register the codec once, at import time, on every node that will either send
or receive `Point` instances — the tag lookup on decode needs the codec (or
the class itself, if it's a dataclass/Pydantic model) to already be
registered/importable.
