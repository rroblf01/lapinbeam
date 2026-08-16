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
