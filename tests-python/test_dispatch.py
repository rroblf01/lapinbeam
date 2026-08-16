"""Typed message dispatch via `@on(Type)` / `@on(default=True)`."""

import asyncio
from dataclasses import dataclass

import pytest

from helpers import wait_until
from lapinbeam import Node, Supervisor, actor, on


@dataclass
class Task:
    payload_id: int
    name: str


@dataclass
class Ack:
    result: int


def test_on_requires_type_or_default():
    with pytest.raises(TypeError):
        on()


def test_on_type_and_default_are_mutually_exclusive():
    with pytest.raises(TypeError):
        on(Task, default=True)


def test_duplicate_handler_for_same_type_raises():
    with pytest.raises(TypeError):
        @actor(name="dup")
        class Dup:
            @on(Task)
            async def a(self, msg):
                pass

            @on(Task)
            async def b(self, msg):
                pass


def test_duplicate_default_handler_raises():
    with pytest.raises(TypeError):
        @actor(name="dup_default")
        class DupDefault:
            @on(default=True)
            async def a(self, msg):
                pass

            @on(default=True)
            async def b(self, msg):
                pass


async def test_on_dispatches_by_message_type():
    tasks_seen = []
    acks_seen = []

    @actor(name="typed")
    class Typed:
        @on(Task)
        async def handle_task(self, msg: Task):
            tasks_seen.append(msg)

        @on(Ack)
        async def handle_ack(self, msg: Ack):
            acks_seen.append(msg)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        ref = Supervisor(strategy="one_for_one", node=node).spawn(Typed)
        await ref.send(Task(payload_id=1, name="x"))
        await ref.send(Ack(result=42))
        await wait_until(lambda: len(tasks_seen) == 1 and len(acks_seen) == 1)
        assert tasks_seen[0] == Task(payload_id=1, name="x")
        assert acks_seen[0] == Ack(result=42)
    finally:
        await node.stop()


async def test_on_default_handles_unmatched_type():
    others_seen = []

    @actor(name="with_default")
    class WithDefault:
        @on(Task)
        async def handle_task(self, msg: Task):
            pass

        @on(default=True)
        async def handle_other(self, msg):
            others_seen.append(msg)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        ref = Supervisor(strategy="one_for_one", node=node).spawn(WithDefault)
        await ref.send({"plain": "dict"})
        await wait_until(lambda: len(others_seen) == 1)
        assert others_seen[0] == {"plain": "dict"}
    finally:
        await node.stop()


async def test_unmatched_type_without_default_crashes_actor():
    @actor(name="no_default")
    class NoDefault:
        @on(Task)
        async def handle_task(self, msg: Task):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(strategy="one_for_one", node=node, max_restarts=0)
        ref = sup.spawn(NoDefault)
        await ref.send(Ack(result=1))
        with pytest.raises(TypeError, match="Ack"):
            await asyncio.wait_for(ref.task, timeout=5.0)
    finally:
        await node.stop()


async def test_actor_without_on_still_uses_receive():
    received = []

    @actor(name="classic")
    class Classic:
        async def receive(self, msg):
            received.append(msg)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        ref = Supervisor(strategy="one_for_one", node=node).spawn(Classic)
        await ref.send(Task(payload_id=2, name="y"))
        await wait_until(lambda: len(received) == 1)
        assert received[0] == Task(payload_id=2, name="y")
    finally:
        await node.stop()


async def test_on_dispatch_across_two_nodes():
    tasks_seen = []

    @actor(name="remote_typed")
    class RemoteTyped:
        @on(Task)
        async def handle_task(self, msg: Task):
            tasks_seen.append(msg)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        Supervisor(strategy="one_for_one", node=node_b).spawn(RemoteTyped)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "remote_typed")
        await remote.send(Task(payload_id=9, name="remote"))
        await wait_until(lambda: len(tasks_seen) == 1)
        assert tasks_seen[0] == Task(payload_id=9, name="remote")
        assert type(tasks_seen[0]) is Task
    finally:
        await node_a.stop()
        await node_b.stop()
