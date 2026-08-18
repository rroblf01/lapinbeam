import asyncio

import pytest
from pydantic import BaseModel

from helpers import wait_until
from lapinbeam import MessageMeta, Node, Supervisor, actor, current_message
from lapinbeam.codec import RESERVED


class Metric(BaseModel):
    name: str
    value: float


def test_actor_decorator_registers_name():
    @actor(name="custom")
    class Foo:
        pass

    @actor()
    class Bar:
        pass

    @actor
    class Baz:
        pass

    assert Foo.__lapinbeam_actor__["name"] == "custom"
    assert Bar.__lapinbeam_actor__["name"] == "Bar"
    assert Baz.__lapinbeam_actor__["name"] == "Baz"


async def test_local_actor_receives_message():
    received = []

    @actor(name="echo")
    class Echo:
        async def receive(self, msg):
            received.append(msg)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(strategy="one_for_one", node=node)
        ref = sup.spawn(Echo)
        await ref.send({"hello": 1})
        await wait_until(lambda: len(received) == 1)
        assert received[0] == {"hello": 1}
    finally:
        await node.stop()


async def test_local_send_to_unknown_actor_raises():
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        ref = node.get_remote_actor("ghost@127.0.0.1:1", "nope")
        with pytest.raises(ValueError):
            await ref.send({})
    finally:
        await node.stop()


async def test_supervisor_restarts_crashed_actor():
    instances = []
    crashes = {"n": 0}
    received = []

    @actor(name="flaky")
    class Flaky:
        def __init__(self):
            instances.append(self)

        async def receive(self, msg):
            crashes["n"] += 1
            if crashes["n"] < 3:
                raise RuntimeError("boom")
            received.append(msg)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(strategy="one_for_one", node=node)
        ref = sup.spawn(Flaky)
        for _ in range(3):
            await ref.send({"attempt": crashes["n"] + 1})
            await asyncio.sleep(0.4)
        await wait_until(lambda: len(received) == 1)
        assert received[0] == {"attempt": 3}
        assert len(instances) == 3
    finally:
        await node.stop()


async def test_supervisor_gives_up_after_max_restarts():
    @actor(name="always_crash")
    class AlwaysCrash:
        async def receive(self, msg):
            raise RuntimeError("nope")

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(strategy="one_for_one", node=node, max_restarts=2)
        ref = sup.spawn(AlwaysCrash)
        await ref.send({})
        await asyncio.sleep(0.4)
        await ref.send({})
        await asyncio.sleep(0.4)
        await ref.send({})
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(ref.task, timeout=5.0)
    finally:
        await node.stop()


async def test_remote_send_between_two_nodes():
    acks = []

    @actor(name="ingestor")
    class Ingestor:
        async def receive(self, msg):
            if msg.get("type") == "ACK":
                acks.append(msg)

    @actor(name="processor")
    class Processor:
        def __init__(self, node_ref, peer_id):
            self.node = node_ref
            self.peer_id = peer_id

        async def receive(self, msg):
            if msg.get("type") == "TASK":
                result = msg["payload_id"] * 2
                remote = self.node.get_remote_actor(self.peer_id, msg["reply_to"])
                await remote.send({"type": "ACK", "result": result})

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(strategy="one_for_one", node=node_a)
        sup_b = Supervisor(strategy="one_for_one", node=node_b)
        sup_a.spawn(Ingestor)
        sup_b.spawn(Processor, node_b, node_a.local_id)

        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "processor")
        for i in range(5):
            await remote.send({"type": "TASK", "payload_id": i, "reply_to": "ingestor"})

        await wait_until(lambda: len(acks) == 5)
        results = sorted(msg["result"] for msg in acks)
        assert results == [0, 2, 4, 6, 8]
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_node_as_async_context_manager():
    received = []

    @actor(name="echo")
    class Echo:
        async def receive(self, msg):
            received.append(msg)

    async with Node("node@127.0.0.1:0") as node:
        assert node.local_id.startswith("node@")
        ref = Supervisor(strategy="one_for_one", node=node).spawn(Echo)
        await ref.send({"x": 1})
        await wait_until(lambda: len(received) == 1)

    # __aexit__ must have stopped the node.
    assert node._core is None
    assert node._started is False


async def test_on_event_surfaces_actor_not_found_error():
    events = []

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        node_a.on_event(events.append)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "no_such_actor")
        await remote.send({"type": "TASK"}, correlation_id=99)
        await wait_until(lambda: any(e["kind"] == "error" for e in events))
        error = next(e for e in events if e["kind"] == "error")
        assert error["peer"] == node_b.local_id
        assert "actor_not_found" in error["detail"]
        assert error["correlation_id"] == 99
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_on_event_surfaces_peer_connected_and_disconnected():
    events = []

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    node_a.on_event(events.append)
    try:
        await node_a.connect_peer(node_b.local_id)
        await wait_until(lambda: any(e["kind"] == "peer_connected" for e in events))
        await node_b.stop()
        await wait_until(lambda: any(e["kind"] == "peer_disconnected" for e in events))
    finally:
        await node_a.stop()


async def test_on_event_surfaces_decode_error_instead_of_dropping_silently():
    events = []
    received = []

    @actor(name="sink")
    class Sink:
        async def receive(self, msg):
            received.append(msg)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        node_b.on_event(events.append)
        Supervisor(node=node_b).spawn(Sink)
        await node_a.connect_peer(node_b.local_id)

        # Tagged as Metric, but "value" can't be parsed as a float — fails
        # Pydantic validation on decode, on node_b's side.
        tag = f"{Metric.__module__}.{Metric.__qualname__}"
        bad_payload = {RESERVED: tag, "data": {"name": "latency", "value": "not-a-number"}}
        node_a._core.send_data(node_b.local_id, "sink", bad_payload, None, None)

        await wait_until(lambda: any(e["kind"] == "decode_error" for e in events))
        error = next(e for e in events if e["kind"] == "decode_error")
        assert error["actor"] == "sink"
        assert "ValidationError" in error["detail"]
        # The malformed message must never reach the actor's mailbox.
        assert received == []
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_auto_reconnect_after_peer_restart():
    acks = []

    @actor(name="ingestor")
    class Ingestor:
        async def receive(self, msg):
            acks.append(msg)

    @actor(name="processor")
    class Processor:
        def __init__(self, node_ref, peer_id):
            self.node = node_ref
            self.peer_id = peer_id

        async def receive(self, msg):
            remote = self.node.get_remote_actor(self.peer_id, msg["reply_to"])
            await remote.send({"ack": msg["n"]})

    node_a = Node("node_a@127.0.0.1:0", reconnect_interval=0.1)
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    node_b_id = node_b.local_id
    node_b_host = node_b_id.split("@")[1].rsplit(":", 1)[0]
    node_b_port = node_b_id.rsplit(":", 1)[1]
    try:
        Supervisor(strategy="one_for_one", node=node_a).spawn(Ingestor)
        Supervisor(strategy="one_for_one", node=node_b).spawn(
            Processor, node_b, node_a.local_id
        )
        await node_a.connect_peer(node_b_id)
        remote = node_a.get_remote_actor(node_b_id, "processor")
        await remote.send({"n": 1, "reply_to": "ingestor"})
        await wait_until(lambda: len(acks) == 1)

        # Node B dies...
        await node_b.stop()
        await asyncio.sleep(0.3)

        # ...and comes back on the same address.
        node_b = Node(f"node_b@{node_b_host}:{node_b_port}")
        await node_b.start()
        Supervisor(strategy="one_for_one", node=node_b).spawn(
            Processor, node_b, node_a.local_id
        )

        # Node A must auto-reconnect; the next message flows through.
        await wait_until(lambda: node_a.has_peer(node_b_id))
        await remote.send({"n": 2, "reply_to": "ingestor"})
        await wait_until(lambda: len(acks) == 2)
        assert [msg["ack"] for msg in acks] == [1, 2]
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_forget_peer_drops_connection_and_stops_reconnecting():
    node_a = Node("node_a@127.0.0.1:0", reconnect_interval=0.03)
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        await node_a.connect_peer(node_b.local_id)
        assert node_a.has_peer(node_b.local_id)

        node_a.forget_peer(node_b.local_id)
        assert not node_a.has_peer(node_b.local_id)

        # node_b is still alive and reachable — if node_a still considered
        # it desired, the fast reconnect_interval would reconnect almost
        # immediately. It must not, since we explicitly forgot it.
        await asyncio.sleep(0.2)
        assert not node_a.has_peer(node_b.local_id)
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_forget_peer_closes_the_socket_promptly_not_after_peer_timeout():
    # `forget_peer()` documents "drops that connection now". Before
    # `read_loop`/`heartbeat_loop` reacted to `PeerDisconnected` instead of
    # only polling on their own schedule, that wasn't true: the socket (and
    # its task) stayed alive until `peer_timeout` elapsed with no data —
    # under fast connect/forget churn, a real leaked fd per cycle, not a
    # cosmetic one. `peer_timeout`/`heartbeat_interval` are set far longer
    # than this test waits, so passing can only mean it closed promptly.
    events = []
    node_a = Node("node_a@127.0.0.1:0", heartbeat_interval=30.0, peer_timeout=30.0)
    node_b = Node("node_b@127.0.0.1:0", heartbeat_interval=30.0, peer_timeout=30.0)
    await node_a.start()
    await node_b.start()
    try:
        node_b.on_event(events.append)
        await node_a.connect_peer(node_b.local_id)
        await wait_until(lambda: node_b.has_peer(node_a.local_id))

        node_a.forget_peer(node_b.local_id)

        await wait_until(
            lambda: any(e["kind"] == "peer_disconnected" for e in events), timeout=3.0
        )
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_on_event_surfaces_reconnect_gave_up():
    events = []

    node_a = Node(
        "node_a@127.0.0.1:0",
        reconnect_interval=0.03,
        reconnect_max_attempts=3,
    )
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    node_a.on_event(events.append)
    try:
        await node_a.connect_peer(node_b.local_id)
        assert node_a.has_peer(node_b.local_id)

        # node_b goes away for good — every reconnect attempt after this
        # fails outright, so node_a must give up after 3 attempts instead
        # of retrying forever.
        await node_b.stop()

        await wait_until(lambda: any(e["kind"] == "reconnect_gave_up" for e in events))
        gave_up = next(e for e in events if e["kind"] == "reconnect_gave_up")
        assert gave_up["peer"] == node_b.local_id

        # Given up for good: waiting longer must not bring it back.
        await asyncio.sleep(0.2)
        assert not node_a.has_peer(node_b.local_id)
    finally:
        await node_a.stop()


def test_current_message_is_none_outside_a_handler():
    assert current_message() is None


async def test_local_send_exposes_current_message():
    seen = []

    @actor(name="observer")
    class Observer:
        async def receive(self, msg):
            seen.append(current_message())

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        ref = Supervisor(node=node).spawn(Observer)
        await ref.send({"hello": 1}, reply_to="somebody", correlation_id=7)
        await wait_until(lambda: len(seen) == 1)
        meta = seen[0]
        assert meta.src == node.local_id
        assert meta.reply_to == "somebody"
        assert meta.correlation_id == 7
        assert meta.msg_id is None
    finally:
        await node.stop()


async def test_remote_send_exposes_current_message():
    seen = []

    @actor(name="processor")
    class Processor:
        async def receive(self, msg):
            seen.append(current_message())

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        Supervisor(node=node_b).spawn(Processor)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "processor")
        await remote.send({"type": "TASK"}, reply_to="ingestor", correlation_id=42)
        await wait_until(lambda: len(seen) == 1)
        meta = seen[0]
        assert meta.src == node_a.local_id
        assert meta.reply_to == "ingestor"
        assert meta.correlation_id == 42
        assert isinstance(meta.msg_id, int)
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_node_peer_count():
    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        assert node_a.peer_count() == 0
        await node_a.connect_peer(node_b.local_id)
        assert node_a.peer_count() == 1
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_supervisor_restarts_actor_whose_constructor_crashes():
    attempts = {"n": 0}
    instances = []

    @actor(name="flaky_init")
    class FlakyInit:
        def __init__(self):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("boom in constructor")
            instances.append(self)

        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        Supervisor(node=node).spawn(FlakyInit)
        await wait_until(lambda: len(instances) == 1)
        assert attempts["n"] == 3
    finally:
        await node.stop()


async def test_on_event_surfaces_supervisor_gave_up():
    events = []

    @actor(name="always_crash_ctor")
    class AlwaysCrashInInit:
        def __init__(self):
            raise RuntimeError("nope")

        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    node.on_event(events.append)
    try:
        sup = Supervisor(node=node, max_restarts=1)
        ref = sup.spawn(AlwaysCrashInInit)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(ref.task, timeout=5.0)
        await wait_until(lambda: any(e["kind"] == "supervisor_gave_up" for e in events))
        gave_up = next(e for e in events if e["kind"] == "supervisor_gave_up")
        assert gave_up["actor"] == "always_crash_ctor"
        assert "nope" in gave_up["detail"]
        # The child record must not linger in _children forever — this is
        # what used to leak for a Supervisor that spawns many short-lived
        # actors over its life (e.g. a worker-pool pattern).
        assert not sup._children
    finally:
        await node.stop()


def test_node_mailbox_capacity_defaults_to_unbounded():
    assert Node("node@127.0.0.1:0").mailbox_capacity is None


async def test_local_mailbox_capacity_drops_and_fires_event():
    events = []
    started = asyncio.Event()
    release = asyncio.Event()

    @actor(name="stuck")
    class Stuck:
        async def receive(self, msg):
            started.set()
            await release.wait()

    node = Node("node@127.0.0.1:0", mailbox_capacity=2)
    await node.start()
    node.on_event(events.append)
    try:
        ref = Supervisor(node=node).spawn(Stuck)
        await ref.send({"n": 0})
        await started.wait()  # mailbox is now empty; "stuck" is blocked in receive()

        # Capacity is 2: the next two fit, the third must be dropped.
        await ref.send({"n": 1})
        await ref.send({"n": 2})
        await ref.send({"n": 3})

        await wait_until(lambda: any(e["kind"] == "mailbox_full" for e in events))
        full = next(e for e in events if e["kind"] == "mailbox_full")
        assert full["actor"] == "stuck"
    finally:
        release.set()
        await node.stop()


async def test_remote_mailbox_capacity_drops_and_notifies_both_sides():
    events_a = []
    events_b = []
    started = asyncio.Event()
    release = asyncio.Event()

    @actor(name="stuck")
    class Stuck:
        async def receive(self, msg):
            started.set()
            await release.wait()

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0", mailbox_capacity=1)
    await node_a.start()
    await node_b.start()
    node_a.on_event(events_a.append)
    node_b.on_event(events_b.append)
    try:
        Supervisor(node=node_b).spawn(Stuck)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "stuck")

        await remote.send({"n": 0})
        await started.wait()  # mailbox is now empty; "stuck" is blocked in receive()

        # Capacity is 1 and delivery is ordered (single TCP connection): the
        # next send fills it, the one after that must be dropped.
        await remote.send({"n": 1})
        await remote.send({"n": 2}, correlation_id=77)

        # The node whose actor overflowed sees it locally...
        await wait_until(lambda: any(e["kind"] == "mailbox_full" for e in events_b))
        full = next(e for e in events_b if e["kind"] == "mailbox_full")
        assert full["actor"] == "stuck"

        # ...and the sender is told its send failed, same as any other
        # delivery error, correlation_id and all.
        await wait_until(lambda: any(e["kind"] == "error" for e in events_a))
        error = next(e for e in events_a if e["kind"] == "error")
        assert "mailbox_full" in error["detail"]
        assert error["correlation_id"] == 77
    finally:
        release.set()
        await node_a.stop()
        await node_b.stop()


async def test_custom_peer_timeout_disconnects_silent_peer_faster():
    # Both nodes keep their default heartbeat_interval (1s) — each side only
    # hears from the other roughly once a second (a heartbeat, or the reply
    # it provokes). node_a's peer_timeout is far shorter than that gap, so
    # node_a must give up on node_b as "silent" well before its own next
    # heartbeat is even due, and well before the default 3s timeout would —
    # proving peer_timeout actually reaches the transport instead of being
    # ignored.
    node_a = Node("node_a@127.0.0.1:0", peer_timeout=0.15)
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        await node_a.connect_peer(node_b.local_id)
        assert node_a.has_peer(node_b.local_id)
        await wait_until(lambda: not node_a.has_peer(node_b.local_id))
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_custom_peer_queue_capacity_does_not_break_normal_delivery():
    received = []

    @actor(name="sink")
    class Sink:
        async def receive(self, msg):
            received.append(msg)

    node_a = Node("node_a@127.0.0.1:0", peer_queue_capacity=4)
    node_b = Node("node_b@127.0.0.1:0", peer_queue_capacity=4)
    await node_a.start()
    await node_b.start()
    try:
        Supervisor(node=node_b).spawn(Sink)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "sink")
        await remote.send({"ok": True})
        await wait_until(lambda: len(received) == 1)
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_node_stop_cancels_actor_tasks():
    @actor(name="stuck")
    class Stuck:
        async def receive(self, msg):
            await asyncio.sleep(1000)

    node = Node("node@127.0.0.1:0")
    await node.start()
    ref = Supervisor(node=node).spawn(Stuck)
    await ref.send({})
    await asyncio.sleep(0.05)  # let it actually start blocking in receive()
    await node.stop()
    assert ref.task.done()
    assert ref.task.cancelled()


async def test_supervisor_shutdown_cancels_only_its_own_actors():
    @actor(name="a")
    class A:
        async def receive(self, msg):
            await asyncio.sleep(1000)

    @actor(name="b")
    class B:
        async def receive(self, msg):
            await asyncio.sleep(1000)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup_a = Supervisor(node=node)
        sup_b = Supervisor(node=node)
        ref_a = sup_a.spawn(A)
        ref_b = sup_b.spawn(B)
        await sup_a.shutdown()
        assert ref_a.task.done() and ref_a.task.cancelled()
        assert not ref_b.task.done()
    finally:
        await node.stop()


async def test_local_ask_returns_correlated_reply():
    @actor(name="echoer")
    class Echoer:
        async def receive(self, msg):
            await current_message().reply({"echo": msg["n"]})

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        ref = Supervisor(node=node).spawn(Echoer)
        reply = await ref.ask({"n": 42})
        assert reply == {"echo": 42}
    finally:
        await node.stop()


async def test_remote_ask_returns_correlated_reply():
    @actor(name="echoer")
    class Echoer:
        async def receive(self, msg):
            await current_message().reply({"echo": msg["n"]})

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        Supervisor(node=node_b).spawn(Echoer)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "echoer")
        reply = await remote.ask({"n": 7})
        assert reply == {"echo": 7}
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_ask_times_out_if_nothing_replies():
    @actor(name="silent")
    class Silent:
        async def receive(self, msg):
            pass  # never replies

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        ref = Supervisor(node=node).spawn(Silent)
        with pytest.raises(TimeoutError):
            await ref.ask({"n": 1}, timeout=0.2)
        # The one-shot reply mailbox must not be left registered forever.
        assert not any(name.startswith("__lapinbeam_ask_") for name in node._mailboxes)
    finally:
        await node.stop()


async def test_message_meta_reply_without_reply_to_raises():
    meta = MessageMeta(src="x@127.0.0.1:1", reply_to=None, correlation_id=None, msg_id=None, node=None)
    with pytest.raises(RuntimeError):
        await meta.reply({"x": 1})


async def test_restart_does_not_drop_messages_already_queued():
    # A crash only consumes the one message that caused it (already
    # dequeued before the handler ran) — any messages sent right after it,
    # still sitting in the same mailbox when the crash happened, must
    # survive the restart instead of being discarded along with a
    # replaced, empty queue.
    crashes = {"n": 0}
    received = []

    @actor(name="flaky")
    class Flaky:
        async def receive(self, msg):
            crashes["n"] += 1
            if crashes["n"] == 1:
                raise RuntimeError("boom")
            received.append(msg)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        ref = Supervisor(node=node).spawn(Flaky)
        # All three land in the mailbox before the first one is even
        # dequeued — no sleep in between, unlike
        # test_supervisor_restarts_crashed_actor above.
        await ref.send({"n": 1})
        await ref.send({"n": 2})
        await ref.send({"n": 3})
        await wait_until(lambda: len(received) == 2)
        assert [msg["n"] for msg in received] == [2, 3]
    finally:
        await node.stop()


async def test_spawn_raises_on_duplicate_actor_name():
    received = []

    @actor(name="dup")
    class Dup:
        async def receive(self, msg):
            received.append(msg)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        first = sup.spawn(Dup)
        with pytest.raises(ValueError):
            sup.spawn(Dup)
        # The first actor must be unaffected by the failed second spawn.
        await first.send({"x": 1})
        await wait_until(lambda: len(received) == 1)
    finally:
        await node.stop()


async def test_current_message_in_background_task_is_a_frozen_snapshot():
    # `current_message()` is bound via a `contextvars.ContextVar`. A task
    # created with `asyncio.create_task()` from inside a handler copies the
    # ambient context at creation time, so it inherits that handler's
    # `current_message()` — and keeps returning that same, increasingly
    # stale value for as long as it runs, even once the actor has moved on
    # to a different message. This pins down that real (if surprising)
    # behavior — see `current_message()`'s docstring.
    bg_seen = []
    started = asyncio.Event()
    release = asyncio.Event()

    @actor(name="spawner")
    class Spawner:
        async def receive(self, msg):
            if msg.get("spawn_bg"):
                async def bg():
                    started.set()
                    await release.wait()
                    bg_seen.append(current_message())

                asyncio.create_task(bg())

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        ref = Supervisor(node=node).spawn(Spawner)
        await ref.send({"spawn_bg": True}, correlation_id=1)
        await asyncio.wait_for(started.wait(), timeout=2.0)

        # The actor moves on to a second, unrelated message while the
        # background task from the first message is still alive.
        await ref.send({"spawn_bg": False}, correlation_id=2)
        await asyncio.sleep(0.05)

        release.set()
        await wait_until(lambda: len(bg_seen) == 1)
        assert bg_seen[0].correlation_id == 1
    finally:
        await node.stop()


async def test_on_event_listener_exception_does_not_break_local_send():
    node = Node("node@127.0.0.1:0", mailbox_capacity=1)
    await node.start()
    try:
        node.on_event(lambda event: 1 / 0)

        @actor(name="stuck")
        class Stuck:
            async def receive(self, msg):
                await asyncio.sleep(5)

        ref = Supervisor(node=node).spawn(Stuck)
        await ref.send({"a": 1})
        await asyncio.sleep(0.05)  # let the handler dequeue it and block
        await ref.send({"b": 2})  # fills the one-slot mailbox
        # This one hits QueueFull, firing "mailbox_full" — the broken
        # listener above must not make this fire-and-forget send raise.
        await ref.send({"c": 3})
    finally:
        await node.stop()


async def test_on_event_listener_exception_does_not_mask_supervisor_give_up():
    @actor(name="always_crash")
    class AlwaysCrash:
        async def receive(self, msg):
            raise RuntimeError("real crash reason")

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        node.on_event(lambda event: 1 / 0)
        sup = Supervisor(node=node, max_restarts=1)
        ref = sup.spawn(AlwaysCrash)
        # First message: allowed restart. Second (already queued —
        # preserved across the restart, see the mailbox-reuse fix above):
        # exceeds max_restarts, so the actor gives up for good.
        await ref.send({})
        await ref.send({})
        # The broken listener must not replace the real crash exception
        # with its own when `supervisor_gave_up` fires.
        with pytest.raises(RuntimeError, match="real crash reason"):
            await asyncio.wait_for(ref.task, timeout=5.0)
    finally:
        await node.stop()


async def test_one_for_all_restarts_all_children_on_any_crash():
    a_instances = []
    b_instances = []

    @actor(name="a_one_for_all")
    class A:
        def __init__(self):
            a_instances.append(self)

        async def receive(self, msg):
            raise RuntimeError("boom")

    @actor(name="b_one_for_all")
    class B:
        def __init__(self):
            b_instances.append(self)

        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(strategy="one_for_all", node=node, max_restarts=3)
        ref_a = sup.spawn(A)
        sup.spawn(B)
        await ref_a.send({})
        # B never crashed itself — one_for_all must restart it anyway.
        await wait_until(lambda: len(a_instances) == 2)
        await wait_until(lambda: len(b_instances) == 2)
    finally:
        await node.stop()


async def test_rest_for_one_restarts_crashed_and_later_children():
    a_instances = []
    b_instances = []
    c_instances = []

    @actor(name="a_rest_for_one")
    class A:
        def __init__(self):
            a_instances.append(self)

        async def receive(self, msg):
            pass

    @actor(name="b_rest_for_one")
    class B:
        def __init__(self):
            b_instances.append(self)

        async def receive(self, msg):
            raise RuntimeError("boom")

    @actor(name="c_rest_for_one")
    class C:
        def __init__(self):
            c_instances.append(self)

        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(strategy="rest_for_one", node=node, max_restarts=3)
        sup.spawn(A)
        ref_b = sup.spawn(B)
        sup.spawn(C)
        await ref_b.send({})
        # B crashed and C was spawned after it — both must be rebuilt.
        await wait_until(lambda: len(b_instances) == 2)
        await wait_until(lambda: len(c_instances) == 2)
        # A was spawned before B — rest_for_one must leave it alone.
        await asyncio.sleep(0.1)
        assert len(a_instances) == 1
    finally:
        await node.stop()


async def test_one_for_one_give_up_does_not_affect_siblings():
    b_instances = []

    @actor(name="a_solo_giveup")
    class A:
        async def receive(self, msg):
            raise RuntimeError("a boom")

    @actor(name="b_solo")
    class B:
        def __init__(self):
            b_instances.append(self)

        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node, max_restarts=1)
        ref_a = sup.spawn(A)
        ref_b = sup.spawn(B)
        await ref_a.send({})
        await ref_a.send({})
        with pytest.raises(RuntimeError, match="a boom"):
            await asyncio.wait_for(ref_a.task, timeout=5.0)
        # B must be completely unaffected: same instance, still running —
        # one_for_one never lets siblings affect each other, even when one
        # of them gives up for good rather than merely restarting.
        assert len(b_instances) == 1
        assert not ref_b.task.done()
    finally:
        await node.stop()


async def test_spawn_supervisor_give_up_propagates_to_supervisor_ref():
    @actor(name="always_crash_nested")
    class AlwaysCrash:
        async def receive(self, msg):
            raise RuntimeError("nested boom")

    node = Node("node@127.0.0.1:0")
    await node.start()
    events = []
    node.on_event(events.append)
    refs = {}

    def build(child_sup):
        refs["actor"] = child_sup.spawn(AlwaysCrash)

    try:
        # max_restarts=0 on both levels: the nested actor's very first
        # crash exhausts the nested supervisor's budget immediately, and
        # that give-up exhausts the parent's own budget immediately too —
        # no rebuild attempt in between, so a single crash is enough to
        # observe the give-up propagate all the way to `sup_ref.task`.
        sup = Supervisor(node=node, max_restarts=0)
        sup_ref = sup.spawn_supervisor("worker_tree", build, max_restarts=0)
        await wait_until(lambda: "actor" in refs)
        await refs["actor"].send({})
        with pytest.raises(RuntimeError, match="nested boom"):
            await asyncio.wait_for(sup_ref.task, timeout=5.0)
        await wait_until(lambda: any(e["kind"] == "supervisor_gave_up" for e in events))
        # ActorRef.task still works correctly for an actor inside a nested
        # tree — also retrieves its exception, so asyncio doesn't warn
        # about it going unobserved (sup_ref.task and refs["actor"].task
        # are two distinct Task objects for the same underlying failure).
        with pytest.raises(RuntimeError, match="nested boom"):
            await refs["actor"].task
    finally:
        await node.stop()


async def test_parent_shutdown_cancels_nested_supervisor_grandchildren():
    @actor(name="grandchild")
    class Grandchild:
        async def receive(self, msg):
            await asyncio.sleep(1000)

    node = Node("node@127.0.0.1:0")
    await node.start()
    refs = {}

    def build(child_sup):
        refs["gc"] = child_sup.spawn(Grandchild)

    try:
        sup = Supervisor(node=node)
        sup_ref = sup.spawn_supervisor("subtree", build)
        await wait_until(lambda: "gc" in refs)
        await refs["gc"].send({})
        await asyncio.sleep(0.05)  # let it actually start blocking in receive()
        await sup.shutdown()
        assert refs["gc"].task.done()
        assert refs["gc"].task.cancelled()
        assert sup_ref.task.done()
        assert sup_ref.task.cancelled()
    finally:
        await node.stop()
