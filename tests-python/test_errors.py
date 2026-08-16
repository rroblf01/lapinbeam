import asyncio
import socket

import pytest

from lapinbeam import Node


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def test_connect_refused_raises_connection_error():
    node = Node("node@127.0.0.1:0", connect_timeout=0.3)
    await node.start()
    try:
        with pytest.raises(ConnectionError):
            await node.connect_peer(f"ghost@127.0.0.1:{_free_port()}")
    finally:
        await node.stop()


async def test_oversized_payload_rejected():
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        big = {"data": "x" * (16 * 1024 * 1024)}
        with pytest.raises(ValueError, match="too large"):
            await node._send_remote("peer@127.0.0.1:1", "x", big)
    finally:
        await node.stop()
