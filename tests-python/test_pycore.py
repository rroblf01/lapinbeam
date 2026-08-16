import asyncio

import pytest

import lapinbeam._core as _core


async def wait_until(cond, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if cond():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met in time")


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        42,
        -7,
        3.5,
        "text",
        [1, "two", 3.0, None],
        {"nested": {"list": [1, 2], "ok": True}},
    ],
)
def test_payload_roundtrip(value):
    assert _core.decode_payload(_core.encode_payload(value)) == value


def test_payload_rejects_non_json():
    with pytest.raises(TypeError):
        _core.encode_payload(object())


async def test_two_nodes_message_flow():
    node_a = _core.Node("node_a@127.0.0.1:0")
    node_b = _core.Node("node_b@127.0.0.1:0")
    node_a.start()
    node_b.start()
    try:
        received = []
        loop = asyncio.get_running_loop()
        node_b.register_actor("processor", loop, lambda msg: received.append(msg))

        node_a.connect_peer(node_b.local_id())
        await wait_until(lambda: node_a.has_peer(node_b.local_id()))
        await wait_until(lambda: node_b.has_peer(node_a.local_id()))

        node_a.send_data(
            node_b.local_id(), "processor", {"type": "TASK", "payload_id": 7}, "ingestor", 1
        )
        await wait_until(lambda: len(received) == 1)
        assert received[0] == {"type": "TASK", "payload_id": 7}
    finally:
        node_a.stop()
        node_b.stop()


async def test_two_nodes_bidirectional():
    node_a = _core.Node("node_a@127.0.0.1:0")
    node_b = _core.Node("node_b@127.0.0.1:0")
    node_a.start()
    node_b.start()
    try:
        received_a = []
        received_b = []
        loop = asyncio.get_running_loop()
        node_a.register_actor("ingestor", loop, lambda msg: received_a.append(msg))
        node_b.register_actor("processor", loop, lambda msg: received_b.append(msg))

        node_a.connect_peer(node_b.local_id())
        await wait_until(lambda: node_a.has_peer(node_b.local_id()))

        node_a.send_data(node_b.local_id(), "processor", {"type": "TASK", "n": 3}, "ingestor", None)
        await wait_until(lambda: len(received_b) == 1)

        node_b.send_data(node_a.local_id(), "ingestor", {"type": "ACK"}, None, None)
        await wait_until(lambda: len(received_a) == 1)

        assert received_a[0] == {"type": "ACK"}
    finally:
        node_a.stop()
        node_b.stop()


async def test_send_data_unknown_peer_raises():
    node_a = _core.Node("node_a@127.0.0.1:0")
    node_a.start()
    try:
        with pytest.raises(ValueError):
            node_a.send_data("ghost@127.0.0.1:9999", "x", {}, None, None)
    finally:
        node_a.stop()


async def test_send_before_start_raises():
    node = _core.Node("node@127.0.0.1:0")
    with pytest.raises(RuntimeError):
        node.send_data("peer@127.0.0.1:1", "x", {}, None, None)
