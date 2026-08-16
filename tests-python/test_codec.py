import asyncio
from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from helpers import wait_until
from lapinbeam import Node, Supervisor, actor
from lapinbeam.codec import decode_payload, encode_payload, register_codec


@dataclass
class Task:
    payload_id: int
    name: str


@dataclass
class Envelope:
    task: Task
    tags: list


class Metric(BaseModel):
    name: str
    value: float


class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __eq__(self, other):
        return isinstance(other, Point) and (self.x, self.y) == (other.x, other.y)


def test_dataclass_roundtrip():
    task = Task(payload_id=7, name="spawn")
    enc = encode_payload(task)
    assert enc["__lb_type__"] == "test_codec.Task"
    dec = decode_payload(enc)
    assert dec == task
    assert type(dec) is Task


def test_nested_dataclass_roundtrip():
    env = Envelope(task=Task(1, "x"), tags=["a", "b"])
    dec = decode_payload(encode_payload(env))
    assert dec == env
    assert type(dec) is Envelope
    assert type(dec.task) is Task


def test_pydantic_roundtrip():
    m = Metric(name="latency", value=1.5)
    enc = encode_payload(m)
    assert enc["__lb_type__"] == "test_codec.Metric"
    dec = decode_payload(enc)
    assert dec == m
    assert type(dec) is Metric


def test_plain_dict_unaffected():
    msg = {"type": "ACK", "nested": [1, 2]}
    dec = decode_payload(encode_payload(msg))
    assert dec == msg
    assert "__lb_type__" not in dec


def test_custom_codec():
    register_codec(Point, lambda p: {"x": p.x, "y": p.y},
                   lambda d: Point(d["x"], d["y"]))
    dec = decode_payload(encode_payload(Point(1, 2)))
    assert dec == Point(1, 2)
    assert type(dec) is Point


def test_unknown_tag_raises():
    with pytest.raises(ValueError):
        decode_payload({"__lb_type__": "no.such.Cls", "data": {}})


async def test_dataclass_across_two_nodes():
    received = []

    @actor(name="recv")
    class Recv:
        async def receive(self, msg):
            received.append(msg)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        Supervisor(strategy="one_for_one", node=node_b).spawn(Recv)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "recv")
        await remote.send(Task(payload_id=3, name="worker"))
        await wait_until(lambda: len(received) == 1)
        assert received[0] == Task(payload_id=3, name="worker")
        assert type(received[0]) is Task
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_pydantic_across_two_nodes():
    received = []

    @actor(name="metric_sink")
    class MetricSink:
        async def receive(self, msg):
            received.append(msg)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        Supervisor(strategy="one_for_one", node=node_b).spawn(MetricSink)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "metric_sink")
        await remote.send(Metric(name="latency", value=2.5))
        await wait_until(lambda: len(received) == 1)
        assert received[0] == Metric(name="latency", value=2.5)
        assert type(received[0]) is Metric
    finally:
        await node_a.stop()
        await node_b.stop()
