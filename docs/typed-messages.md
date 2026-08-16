# Typed messages

Plain dicts work everywhere in lapinbeam, but you rarely want `msg.get("type")
== "ACK"` sprinkled through actor code when Python already has real types.
Two independent pieces make this comfortable: **type-preserving payloads**
(dataclasses and Pydantic models survive the trip across nodes) and
**typed dispatch** (`@on(Type)`, an alternative to a single `receive` method).

## Type-preserving payloads

`lapinbeam.codec` wraps `@dataclass` instances and Pydantic v2 models in a
tagged envelope (`{"__lb_type__": "module.QualName", "data": {...}}`) before
they hit the JSON-only Rust transport, and rebuilds the exact type on the
receiving end:

```python
from dataclasses import dataclass
from lapinbeam import Node, Supervisor, actor


@dataclass
class Task:
    payload_id: int
    name: str


@actor(name="worker")
class Worker:
    async def receive(self, msg: Task):
        print(msg.payload_id, msg.name)  # msg is a real Task, not a dict


# Sending is transparent — no manual serialization step:
# await remote.send(Task(payload_id=1, name="build"))
```

This works the same for Pydantic v2 models. Custom classes (anything that
isn't a dataclass or a Pydantic model) need an explicit codec:

```python
from lapinbeam import register_codec

class Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

register_codec(
    Point,
    encode=lambda p: {"x": p.x, "y": p.y},
    decode=lambda d: Point(d["x"], d["y"]),
)
```

Both ends of the cluster must register the same codec — the type is looked
up by its `module.QualName` tag, so the class needs to be importable (or
registered) wherever `decode_payload` runs.

!!! note "Local sends are always zero-copy"
    Type preservation only matters for **remote** sends. A local
    `ActorRef.send(obj)` passes the exact Python object by reference — no
    encoding, no copy, and no restriction to JSON-compatible or codec-covered
    types. You can send *anything* between two actors on the same node.

## Typed dispatch with `@on`

Once messages carry real types, dispatching on them by hand still means a
chain of `isinstance`/`match` checks inside one `receive`. `@on(Type)` moves
that into the actor's declaration:

```python
from dataclasses import dataclass
from lapinbeam import actor, on


@dataclass
class Task:
    payload_id: int
    name: str


@dataclass
class Ack:
    result: int


@actor(name="worker")
class Worker:
    @on(Task)
    async def handle_task(self, msg: Task):
        ...

    @on(Ack)
    async def handle_ack(self, msg: Ack):
        ...

    @on(default=True)
    async def handle_other(self, msg):
        print("unrecognized message:", msg)
```

An actor with any `@on` handler stops using `receive` entirely — messages are
dispatched by `type(msg)` to the matching handler. `@on(default=True)` marks
a single catch-all handler for any type without a dedicated one (including
plain dicts); it is the simplest way to stay safe against message shapes you
didn't plan for. Without a default handler, an unmatched type raises
`TypeError`, which crashes the actor and lets `Supervisor` restart it — the
same "let it crash" philosophy the rest of the framework already follows for
unhandled exceptions in `receive`.

Actors that only define `receive` are completely unaffected — `@on` is
strictly additive, not a migration you're forced into.

### Or skip `@on` entirely: use `match`

Since the message already arrives with its real type, you don't need `@on`
at all to get typed dispatch — Python's structural pattern matching works
directly on dataclasses inside a single `receive`:

```python
async def receive(self, msg):
    match msg:
        case Task(payload_id=pid, name=name):
            ...
        case Ack(result=r):
            ...
        case _:
            print("unrecognized message:", msg)
```

This costs nothing (no framework feature involved) and gives you
destructuring in the same breath as the type check. Reach for `@on` instead
when you'd rather have one small method per message type — e.g. to unit-test
each handler in isolation, or once a single `match` block grows unwieldy.
