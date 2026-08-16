import asyncio

import pytest

from helpers import wait_until
from lapinbeam import Node, Supervisor, actor


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
        await remote.send({"type": "TASK"})
        await wait_until(lambda: any(e["kind"] == "error" for e in events))
        error = next(e for e in events if e["kind"] == "error")
        assert error["peer"] == node_b.local_id
        assert "actor_not_found" in error["detail"]
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
