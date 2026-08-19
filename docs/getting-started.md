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
from lapinbeam import ActorRef, Node, Supervisor, actor


@actor(name="echo")
class Echo:
    async def receive(self, msg):
        print("received:", msg)


async def main():
    node = Node("app@127.0.0.1:0")  # port 0 picks an ephemeral port
    await node.start()

    sup = Supervisor(strategy="one_for_one", node=node)
    echo: ActorRef = sup.spawn(Echo)

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

`node.stop()` also cancels every actor task spawned by any `Supervisor` on
that node — none are left running forever, blocked on a mailbox nothing
will ever fill again. To tear down only one `Supervisor`'s actors instead
of the whole node (e.g. multiple supervisors sharing one node), call
`await sup.shutdown()` directly.

## Two nodes talking to each other

This is the two-node demo in `examples/`, and the one thing worth being
extra explicit about: **a `Node` is one server/process**, not an in-memory
concept. Below are **two separate scripts** — `app_node_a.py` and
`app_node_b.py` — each with only the one actor that server needs. They are
not two branches of the same file: they are two files, meant to run as two
separate `python` processes, potentially on two separate machines.

```mermaid
sequenceDiagram
    box Server A (node_a@host:9001)
    participant Ingestor as Ingestor actor
    end
    box Server B (node_b@host:9002)
    participant Processor as Processor actor
    end

    Note over Ingestor,Processor: node.connect_peer() opens one TCP connection,<br/>shared by every actor on both servers

    Ingestor->>Processor: send(TASK, reply_to="ingestor")
    Note right of Processor: Processor.receive(msg) runs here, on server B
    Processor->>Ingestor: send(ACK) — routed to "ingestor" by name
    Note left of Ingestor: Ingestor.receive(msg) runs here, on server A
```

Server A sends `TASK` messages to the `processor` actor on server B; B
replies with `ACK` to whichever actor A named as `reply_to` — A doesn't hard-code
"reply to Ingestor", it just tells B who to answer, which is what makes the
same `Processor` code reusable no matter who calls it.

=== "app_node_a.py (server A)"

    ```python
    import asyncio
    import os
    from lapinbeam import Node, RemoteRef, Supervisor, actor


    # This actor exists only on server A. Its job is to receive the ACK
    # that server B sends back once it has processed a task.
    @actor(name="ingestor")
    class Ingestor:
        async def receive(self, msg):
            if msg.get("type") == "ACK":
                print("got ack for", msg["payload_id"])


    async def main():
        node_name = os.environ["NODE_NAME"]   # e.g. node_a@127.0.0.1:9001 (this server)
        peer = os.environ["PEER"]             # e.g. node_b@127.0.0.1:9002 (the other server)

        node = Node(node_name)
        await node.start()   # binds the listening socket for THIS server
        sup = Supervisor(node=node)
        sup.spawn(Ingestor)                                # register the actor that will receive ACKs

        await node.connect_peer(peer)                      # dial server B and wait for the TCP handshake
        remote: RemoteRef = node.get_remote_actor(peer, "processor")  # a handle to B's "processor" actor
        for i in range(100):
            # Every send() here actually crosses the network to server B.
            await remote.send({"type": "TASK", "payload_id": i, "reply_to": "ingestor"})
            await asyncio.sleep(0.01)


    asyncio.run(main())
    ```

=== "app_node_b.py (server B)"

    ```python
    import asyncio
    import os
    from lapinbeam import Node, RemoteRef, Supervisor, actor


    # This actor exists only on server B. It receives TASK messages sent
    # by server A, and replies with an ACK — not to a hard-coded address,
    # but to whichever actor name server A put in `reply_to`.
    @actor(name="processor")
    class Processor:
        def __init__(self, node_ref, peer_id):
            # `node_ref` is THIS process's own Node (server B's) — used to
            # send replies back out. `peer_id` is the OTHER server's id
            # (server A's); both servers already know each other's address
            # up front, from the NODE_NAME/PEER environment variables below.
            self.node = node_ref
            self.peer_id = peer_id

        async def receive(self, msg):
            if msg.get("type") == "TASK":
                # get_remote_actor() does NOT open a new connection — it
                # just builds a reference that reuses the one TCP
                # connection already established between the two servers.
                remote: RemoteRef = self.node.get_remote_actor(self.peer_id, msg["reply_to"])
                await remote.send({"type": "ACK", "payload_id": msg["payload_id"]})


    async def main():
        node_name = os.environ["NODE_NAME"]   # e.g. node_b@127.0.0.1:9002 (this server)
        peer = os.environ["PEER"]             # e.g. node_a@127.0.0.1:9001 (the other server)

        node = Node(node_name)
        await node.start()   # binds the listening socket for THIS server
        sup = Supervisor(node=node)
        sup.spawn(Processor, node, peer)   # register the actor that answers TASKs

        await node.wait_until_stopped()    # server B only reacts to incoming messages; it never dials out


    asyncio.run(main())
    ```

Note what's identical and what isn't: both scripts read `NODE_NAME`/`PEER`
from the environment the same way, but there's no branching logic anywhere
— each file only ever plays one role, matching `examples/app_node_a.py` and
`examples/app_node_b.py` exactly.

`Processor` above gets `peer_id` injected through its constructor because
that's the only way it can know which node to reply to. If a handler would
rather find that out from the message itself instead of relying on a
constructor argument, `lapinbeam.current_message()` returns it directly:

```python
from lapinbeam import MessageMeta, RemoteRef, current_message

async def receive(self, msg):
    if msg.get("type") == "TASK":
        meta: MessageMeta | None = current_message()  # who sent this, and to what?
        remote: RemoteRef = self.node.get_remote_actor(meta.src, meta.reply_to)
        await remote.send({"type": "ACK", "payload_id": msg["payload_id"]})
```

`current_message()` returns a `MessageMeta(src, reply_to, correlation_id,
msg_id, node)` — populated from whatever the sender passed to `send()` —
for as long as the handler coroutine that received `msg` is running, and
`None` outside of one (e.g. from a background task an actor spawned
itself). For a message sent by a local actor, `src` is this node's own id,
and `msg_id` is always `None` (it's a per-connection id the transport
assigns to remote messages only). `reply_to` and `correlation_id` are
`None` unless the sender set them: `await ref.send(msg, reply_to="ingestor",
correlation_id=7)`, on both `ActorRef` and `RemoteRef`.

Since replying to whoever sent a message — to `reply_to`, tagged with the
same `correlation_id`, whether they were local or remote — is common enough
to have its own shortcut, the snippet above can be written as:

```python
async def receive(self, msg):
    if msg.get("type") == "TASK":
        await current_message().reply({"type": "ACK", "payload_id": msg["payload_id"]})
```

`meta.reply(msg)` raises `RuntimeError` if `meta.reply_to` is `None` — there's
nothing sent it a return address to reply to.

## Request/response with `ask()`

`send()` is always fire-and-forget — nothing ties a reply back to the send
that provoked it unless you build that yourself. `ask()` does exactly that:
it tags the send with a fresh `correlation_id`, waits for a single reply,
and works the same on `ActorRef` and `RemoteRef`:

```python
reply: dict = await remote_processor.ask({"type": "TASK", "payload_id": 1})
```

The receiving handler still has to actually reply — `ask()` doesn't change
what a handler does, it only changes how the *caller* waits:

```python
@actor(name="processor")
class Processor:
    async def receive(self, msg):
        result = msg["payload_id"] * 2
        await current_message().reply({"type": "ACK", "result": result})
```

If nothing replies within `timeout` seconds (5 by default; `None` waits
forever), `ask()` raises `TimeoutError`. Under the hood it registers a
one-shot hidden mailbox as the reply address and cleans it up afterwards —
there's no persistent extra actor or resource left behind. See
[AI agents & MCP](ai-agents.md) for a worked example: dispatching MCP tool
calls to a worker node, and fanning a question out to several expert actors
concurrently.

Run them as two separate processes (see
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
def on_event(event: dict) -> None:
    if event["kind"] == "peer_disconnected":
        print("lost peer:", event["peer"])
    elif event["kind"] == "error":
        print("delivery error from", event["peer"], ":", event["detail"],
              "correlation_id:", event["correlation_id"])
    elif event["kind"] == "decode_error":
        print("bad message for", event["actor"], ":", event["detail"])
    elif event["kind"] == "reconnect_gave_up":
        print("giving up on peer:", event["peer"])
    elif event["kind"] == "supervisor_gave_up":
        print("actor stopped for good:", event["actor"], ":", event["detail"])
    elif event["kind"] == "mailbox_full":
        print("dropped a message for:", event["actor"])

node.on_event(on_event)
```

`event["kind"]` is one of `"peer_connected"`, `"peer_disconnected"`,
`"error"` (a peer reported a delivery failure, e.g. a message sent to an
actor name that does not exist on the remote node — `event["correlation_id"]`
echoes whatever the failed `send()` was tagged with, or `None`),
`"decode_error"` (a message for a local actor failed to decode — e.g. a
Pydantic `ValidationError` on a malformed payload — and was dropped before
ever reaching that actor's mailbox, instead of vanishing into an unrelated
asyncio log line), `"reconnect_gave_up"` (automatic reconnection to
`event["peer"]` was abandoned after `reconnect_max_attempts` failed
attempts — it's no longer retried or tracked, so it's not a leak left
behind; call `connect_peer()` again if you want to retry), or
`"supervisor_gave_up"` (a `Supervisor` stopped restarting `event["actor"]`
after too many crashes within its restart window — including a crash in the
actor's own `__init__`, not only its `receive`/`@on` handlers — and it is
no longer running), or `"mailbox_full"` (a message for `event["actor"]` was
dropped because its mailbox was full — only possible if that actor's
`Node` was created with `mailbox_capacity` set; unbounded by default, see
[Limitations](index.md#limitations)). If you already know you're done with
a peer, call `node.forget_peer(peer_id)` instead of waiting for that to
happen on its own.

## Tuning failure detection and backpressure

`Node(...)` takes a few more knobs beyond what's shown above, all optional
and all defaulting to today's behavior if left unset:

```python
node = Node(
    "app@127.0.0.1:0",
    heartbeat_interval=1.0,     # how often to ping each peer
    peer_timeout=3.0,           # drop a peer that's sent nothing for this long
    peer_queue_capacity=256,    # outbound frames buffered per peer
    mailbox_capacity=None,      # cap per-actor mailbox size; None = unbounded
)
```

`heartbeat_interval`/`peer_timeout` control how quickly a silently-dead peer
is noticed — shortening `peer_timeout` without also shortening
`heartbeat_interval` on *both* sides will false-positive on ordinary network
jitter. `peer_queue_capacity` bounds how many outbound frames can be queued
for a peer whose TCP write is congested. `mailbox_capacity` is the one
that changes default behavior in a real way once set — see
`"mailbox_full"` above.

Next: [Typed messages](typed-messages.md) for sending real Python types
instead of dicts, [OTP-inspired patterns](otp-patterns.md) for supervision
trees, links, monitors, groups, and cluster-wide name registration, or
[Benchmarks](benchmarks.md) for what this costs you in latency and
throughput.
