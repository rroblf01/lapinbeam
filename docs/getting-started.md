# Getting started

## Install

```bash
pip install lapinbeam
```

For local development against the source tree instead:

```bash
uv sync                       # create .venv, build the Rust extension, install deps
uv run maturin develop        # rebuild the extension after touching Rust code
uv run pytest                 # Python test suite
cargo test                    # Rust test suite
```

Nothing is installed system-wide: everything lives in `.venv`.

## Core concepts

| Concept | What it is |
| --- | --- |
| `Node` | The local endpoint of the cluster: `name@host:port`. Owns the background Tokio runtime, the listener, and peer connections. |
| `@actor` | Marks a class as an actor. `Supervisor.spawn` reads this metadata; it is not a runtime wrapper. |
| `Supervisor` | Spawns actors and restarts them on unhandled exceptions (`one_for_one` strategy, capped restarts with backoff). |
| `ActorRef` / `RemoteRef` | A handle to send messages to a local or remote actor. Both expose the same `await ref.send(msg)`. |

## A single actor

```python
import asyncio
from lapinbeam import Node, Supervisor, actor


@actor(name="echo")
class Echo:
    async def receive(self, msg):
        print("received:", msg)


async def main():
    node = Node("app@127.0.0.1:0")  # port 0 picks an ephemeral port
    await node.start()

    sup = Supervisor(strategy="one_for_one", node=node)
    echo = sup.spawn(Echo)

    await echo.send({"hello": "world"})
    await asyncio.sleep(0.1)  # let the mailbox drain before stopping
    await node.stop()


asyncio.run(main())
```

`Node` also works as an async context manager, which is the more idiomatic
shape for anything longer-lived:

```python
async with Node("app@127.0.0.1:0") as node:
    sup = Supervisor(node=node)
    ref = sup.spawn(Echo)
    await ref.send({"hello": "world"})
    await asyncio.sleep(0.1)
# node.stop() runs automatically on exit, even if the block raises.
```

## Two nodes talking to each other

This is the shape of the two-node demo in `examples/`: node A sends `TASK`
messages to a `processor` actor on node B; B replies with `ACK` to whichever
actor A named as `reply_to`.

```python
import asyncio
import os
from lapinbeam import Node, Supervisor, actor


@actor(name="ingestor")
class Ingestor:
    async def receive(self, msg):
        if msg.get("type") == "ACK":
            print("got ack for", msg["payload_id"])


@actor(name="processor")
class Processor:
    def __init__(self, node_ref, peer_id):
        self.node = node_ref
        self.peer_id = peer_id

    async def receive(self, msg):
        if msg.get("type") == "TASK":
            remote = self.node.get_remote_actor(self.peer_id, msg["reply_to"])
            await remote.send({"type": "ACK", "payload_id": msg["payload_id"]})


async def main():
    node_name = os.environ["NODE_NAME"]   # e.g. node_a@127.0.0.1:9001
    peer = os.environ["PEER"]             # e.g. node_b@127.0.0.1:9002

    node = Node(node_name)
    await node.start()
    sup = Supervisor(node=node)

    if "node_a" in node_name:
        sup.spawn(Ingestor)
        await node.connect_peer(peer)
        remote = node.get_remote_actor(peer, "processor")
        for i in range(100):
            await remote.send({"type": "TASK", "payload_id": i, "reply_to": "ingestor"})
            await asyncio.sleep(0.01)
    else:
        sup.spawn(Processor, node, peer)
        await node.wait_until_stopped()


asyncio.run(main())
```

Run it as two separate processes (see
[Examples](examples.md) for running this across containers or real hosts
instead of `127.0.0.1`):

```bash
# terminal 1
NODE_NAME=node_a@127.0.0.1:9001 PEER=node_b@127.0.0.1:9002 python app_node_a.py
# terminal 2
NODE_NAME=node_b@127.0.0.1:9002 PEER=node_a@127.0.0.1:9001 python app_node_b.py
```

## Observing the cluster

`connect_peer` already waits for the handshake to complete before returning,
but you rarely want to fly blind about connection state or delivery errors in
a long-running process — subscribe to system events:

```python
def on_event(event):
    if event["kind"] == "peer_disconnected":
        print("lost peer:", event["peer"])
    elif event["kind"] == "error":
        print("delivery error from", event["peer"], ":", event["detail"])

node.on_event(on_event)
```

`event["kind"]` is one of `"peer_connected"`, `"peer_disconnected"`, or
`"error"` — the last one fires when a peer reports a delivery failure, e.g. a
message sent to an actor name that does not exist on the remote node.

Next: [Typed messages](typed-messages.md) for sending real Python types
instead of dicts, or [Benchmarks](benchmarks.md) for what this costs you in
latency and throughput.
